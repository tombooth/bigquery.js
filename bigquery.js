(() => {

    const projectId = document.currentScript.dataset.projectId;
    const clientId = document.currentScript.dataset.clientId;
    const apiKey = document.currentScript.dataset.apiKey;

    let hasAuthenticated = false;

    function authenticate() {
        return new Promise((resolve, _) => {
            if (hasAuthenticated) {
                resolve();
            } else {
                let client = google.accounts.oauth2.initTokenClient({
                    client_id: clientId,
                    scope: 'https://www.googleapis.com/auth/bigquery.readonly https://www.googleapis.com/auth/cloud-platform.read-only',
                    callback: (_) => {
                        gapi.client.setApiKey(apiKey);
                        gapi.client.load('bigquery', 'v2', function () {
                            hasAuthenticated = true;
                            resolve();
                        });
                    },
                });

                client.requestAccessToken();
            }
        })
    }

    async function query(string) {
        let queryResponse = await gapi.client.bigquery.jobs.query({
            'projectId': projectId,
            'resource': {
                'query': string,
                'useLegacySql': false,
                'location': 'EU',
                'timeoutMs': 10000, // ms. it's the default, but be explicit
            }
        })

        let rows = queryResponse.result.rows;

        // if the job hasn't finished, then wait for it and use getQueryResults
        // to poll for completion
        if (!queryResponse.result.jobComplete) {
            let jobReference = queryResponse.result.jobReference;

            while (!queryResponse.result.jobComplete) {
                await new Promise(resolve => setTimeout(resolve, 1000));

                queryResponse = await gapi.client.bigquery.jobs.getQueryResults({
                    'projectId': projectId,
                    'jobId': jobReference.jobId,
                });
            }

            rows = queryResponse.result.rows;
        }

        // read all pages of results
        if (queryResponse.result.pageToken) {
            let pageToken = queryResponse.result.pageToken;

            while (pageToken) {
                let queryResults = await gapi.client.bigquery.jobs.getQueryResults({
                    'projectId': projectId,
                    'jobId': queryResponse.result.jobReference.jobId,
                    'pageToken': pageToken,
                });

                rows = rows.concat(queryResults.result.rows);

                pageToken = queryResults.result.pageToken;
            }
        }

        return {
            schema: queryResponse.result.schema,
            rows: rows,
        };
    }

    function renderTable({ schema, rows }) {
        let table = document.createElement("table");
        let thead = document.createElement("thead");
        let tbody = document.createElement("tbody");

        let headerRow = document.createElement("tr");
        for (let field of schema.fields) {
            let th = document.createElement("th");
            th.innerText = field.name;
            headerRow.appendChild(th);
        }
        thead.appendChild(headerRow);

        for (let row of rows) {
            let tr = document.createElement("tr");
            for (let cell of row.f) {
                let td = document.createElement("td");
                td.innerText = cell.v;
                tr.appendChild(td);
            }
            tbody.appendChild(tr);
        }

        table.appendChild(thead);
        table.appendChild(tbody);

        return table;
    }

    async function executeQuery(queryElem, resultsElem, evt) {
        evt.preventDefault();

        await authenticate()

        let queryString = queryElem.textContent;
        let results = await query(queryString);

        resultsElem.innerHTML = '';
        resultsElem.appendChild(renderTable(results));
    }

    function wrapBigQueryElement(elem) {
        let wrapperElem = document.createElement('div');
        let queryElem = elem.cloneNode(true);
        let executeButtonElem = document.createElement('button');
        let resultsElem = document.createElement('div');

        wrapperElem.classList.add('bigquery-wrapper');

        executeButtonElem.textContent = 'Run';
        executeButtonElem.addEventListener('click', executeQuery.bind(null, queryElem, resultsElem));

        wrapperElem.appendChild(queryElem);
        wrapperElem.appendChild(executeButtonElem);
        wrapperElem.appendChild(resultsElem);

        return wrapperElem;
    }

    function listBigQueryElements() {
        return Array.from(document.querySelectorAll('pre.bigquery'));
    }

    window.addEventListener('load', function () {
        gapi.load('client');

        for (let elem of listBigQueryElements()) {
            let wrappedElem = wrapBigQueryElement(elem);

            elem.parentNode.replaceChild(wrappedElem, elem);
        }
    });

})();