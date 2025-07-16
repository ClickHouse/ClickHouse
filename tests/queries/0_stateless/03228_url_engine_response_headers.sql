SELECT toTypeName(_headers)
FROM url('http://127.0.0.1:8123/?query=select+1&user=default', LineAsString, 's String');

SELECT
    *,
    mapFromString(_headers['X-ClickHouse-Summary'])['read_rows']
FROM url('http://127.0.0.1:8123/?query=select+1&user=default', LineAsString, 's String');
