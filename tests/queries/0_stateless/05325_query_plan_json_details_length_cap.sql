-- Tags: no-old-analyzer

-- Each line of a step's `Details` is cut to `query_plan_max_step_description_length`, as the
-- one-line `Description` is.
--
-- A step renders its literals into `Details`, so a query carrying a large `IN` list produces a
-- correspondingly large line. Unlike `EXPLAIN`, which streams that text to the client, the capture
-- stores it on a `system.query_log` row, and it runs with the memory tracker blocked, so without a
-- cut the size of a row would answer to nothing.

SET log_query_plans = 1;
SET query_plan_max_step_description_length = 64;

-- The literal list renders into the filter step's `Details` far beyond 64 characters.
SELECT count() FROM numbers(10)
WHERE number IN (100000, 100001, 100002, 100003, 100004, 100005, 100006, 100007, 100008, 100009, 100010, 100011, 100012, 100013, 100014, 100015, 100016, 100017, 100018, 100019, 100020, 100021, 100022, 100023, 100024, 100025, 100026, 100027, 100028, 100029, 100030, 100031, 100032, 100033, 100034, 100035, 100036, 100037, 100038, 100039, 100040, 100041, 100042, 100043, 100044, 100045, 100046, 100047, 100048, 100049, 100050, 100051, 100052, 100053, 100054, 100055, 100056, 100057, 100058, 100059, 100060, 100061, 100062, 100063, 100064, 100065, 100066, 100067, 100068, 100069, 100070, 100071, 100072, 100073, 100074, 100075, 100076, 100077, 100078, 100079, 100080, 100081, 100082, 100083, 100084, 100085, 100086, 100087, 100088, 100089, 100090, 100091, 100092, 100093, 100094, 100095, 100096, 100097, 100098, 100099, 100100, 100101, 100102, 100103, 100104, 100105, 100106, 100107, 100108, 100109, 100110, 100111, 100112, 100113, 100114, 100115, 100116, 100117, 100118, 100119) AND '05325_capped' != ''
FORMAT Null;

SET log_query_plans = 0;
SYSTEM FLUSH LOGS query_log;

-- No line exceeds the cap, and at least one was long enough to reach it -- otherwise the query
-- would not be exercising the cut at all and the assertion above would hold vacuously.
SELECT
    'capped',
    max(length(detail)) <= 64,
    countIf(length(detail) = 64) > 0
FROM
(
    SELECT arrayJoin(arrayFlatten(arrayMap(
        node -> JSONExtract(node, 'Details', 'Array(String)'),
        JSONExtractArrayRaw(toJSONString(query_plan), 'Nodes')))) AS detail
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
      AND position(query, '05325_capped') > 0
);
