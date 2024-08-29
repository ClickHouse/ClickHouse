SELECT toTypeName(_headers)
FROM url('http://127.0.0.1:8123/?query=select+1&user=default', LineAsString, 's String');

SELECT
    *,
    mapFromString(_headers['X-ClickHouse-Summary'])['read_rows']
FROM url('http://127.0.0.1:8123/?query=select+1&user=default', LineAsString, 's String');

-- The real_time_microseconds is not available in the `X-ClickHouse-Progress` header (it is available in the `X-ClickHouse-Summary` header).
-- We need to wait until the query is finished to get the real_time_microseconds.
SELECT
    *,
    mapFromString(_headers['X-ClickHouse-Summary'])['read_rows'],
    toUInt64OrZero(mapFromString(_headers['X-ClickHouse-Summary'])['real_time_microseconds']) >= 0 ? 1 : 0
FROM url('http://127.0.0.1:8123/?query=SELECT%20uniq%28number%253%29%20FROM%20numbers%28100%29&user=default&wait_end_of_query=1', LineAsString, 's String');
