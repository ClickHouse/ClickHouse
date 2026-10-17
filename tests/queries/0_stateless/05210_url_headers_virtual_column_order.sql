-- `_headers` is appended in requested-virtual-column order, so both orders below must work.
SELECT mapContains(_headers, 'X-ClickHouse-Query-Id'), isNull(_time)
FROM url('http://127.0.0.1:8123/?query=select+1&user=default', LineAsString, 's String');

SELECT isNull(_time), mapContains(_headers, 'X-ClickHouse-Query-Id')
FROM url('http://127.0.0.1:8123/?query=select+1&user=default', LineAsString, 's String');
