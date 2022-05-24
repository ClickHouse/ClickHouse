SELECT * FROM (

SELECT
    'http_requests_total' AS name,
    'counter' AS type,
    'Total number of HTTP requests' AS help,
    map('method', 'post', 'code', '200') AS labels,
    1027 AS value,
    1395066363000 :: Float64 AS timestamp
UNION ALL
SELECT
    'http_requests_total' AS name,
    'counter' AS type,
    '' AS help,
    map('method', 'post', 'code', '400') AS labels,
    3 AS value,
    1395066363000 :: Float64 AS timestamp
UNION ALL
SELECT
    'msdos_file_access_time_seconds' AS name,
    '' AS type,
    '' AS help,
    map('path', 'C:\\DIR\\FILE.TXT', 'error', 'Cannot find file:\n"FILE.TXT"') AS labels,
    1458255915 AS value,
    0 :: Float64 AS timestamp
UNION ALL
SELECT
    'metric_without_timestamp_and_labels' AS name,
    '' AS type,
    '' AS help,
    map() AS labels,
    12.47 AS value,
    0 :: Float64 AS timestamp
UNION ALL
SELECT
    'something_weird' AS name,
    '' AS type,
    '' AS help,
    map('problem', 'division by zero') AS labels,
    inf AS value,
    -3982045 :: Float64 AS timestamp
UNION ALL
SELECT
    'http_request_duration_seconds' AS name,
    'histogram' AS type,
    'A histogram of the request duration.' AS help,
    map('le', '0.05') AS labels,
    24054 AS value,
    0 :: Float64 AS timestamp
UNION ALL
SELECT
    'http_request_duration_seconds' AS name,
    'histogram' AS type,
    '' AS help,
    map('le', '0.1') AS labels,
    33444 AS value,
    0 :: Float64 AS timestamp
UNION ALL
SELECT
    'http_request_duration_seconds' AS name,
    'histogram' AS type,
    '' AS help,
    map('le', '0.2') AS labels,
    100392 AS value,
    0 :: Float64 AS timestamp
UNION ALL
SELECT
    'http_request_duration_seconds' AS name,
    'histogram' AS type,
    '' AS help,
    map('le', '0.5') AS labels,
    129389 AS value,
    0 :: Float64 AS timestamp
UNION ALL
SELECT
    'http_request_duration_seconds' AS name,
    'histogram' AS type,
    '' AS help,
    map('le', '1') AS labels,
    133988 AS value,
    0 :: Float64 AS timestamp
UNION ALL
SELECT
    'http_request_duration_seconds' AS name,
    'histogram' AS type,
    '' AS help,
    map('le', '+Inf') AS labels,
    144320 AS value,
    0 :: Float64 AS timestamp
UNION ALL
SELECT
    'http_request_duration_seconds' AS name,
    'histogram' AS type,
    '' AS help,
    map('sum', '') AS labels,
    53423 AS value,
    0 :: Float64 AS timestamp
UNION ALL
SELECT
    'rpc_duration_seconds' AS name,
    'summary' AS type,
    'A summary of the RPC duration in seconds.' AS help,
    map('quantile', '0.01') AS labels,
    3102 AS value,
    0 :: Float64 AS timestamp
UNION ALL
SELECT
    'rpc_duration_seconds' AS name,
    'summary' AS type,
    '' AS help,
    map('quantile', '0.05') AS labels,
    3272 AS value,
    0 :: Float64 AS timestamp
UNION ALL
SELECT
    'rpc_duration_seconds' AS name,
    'summary' AS type,
    '' AS help,
    map('quantile', '0.5') AS labels,
    4773 AS value,
    0 :: Float64 AS timestamp
UNION ALL
SELECT
    'rpc_duration_seconds' AS name,
    'summary' AS type,
    '' AS help,
    map('quantile', '0.9') AS labels,
    9001 AS value,
    0 :: Float64 AS timestamp
UNION ALL
SELECT
    'rpc_duration_seconds' AS name,
    'summary' AS type,
    '' AS help,
    map('quantile', '0.99') AS labels,
    76656 AS value,
    0 :: Float64 AS timestamp
UNION ALL
SELECT
    'rpc_duration_seconds' AS name,
    'summary' AS type,
    '' AS help,
    map('count', '') AS labels,
    2693 AS value,
    0 :: Float64 AS timestamp
UNION ALL
SELECT
    'rpc_duration_seconds' AS name,
    'summary' AS type,
    '' AS help,
    map('sum', '') AS labels,
    1.7560473e+07 AS value,
    0 :: Float64 AS timestamp

) ORDER BY name
FORMAT Prometheus;

SELECT
    'metric' || toString(number) AS name,
    number AS value,
    if(number % 2 == 0, 'info '  || toString(number), NULL) AS help,
    if(number % 3 == 0, 'counter', NULL) AS type,
    if(number == 2, 1395066363000, NULL) AS timestamp
FROM numbers(5)
FORMAT Prometheus;
