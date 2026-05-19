-- Compatibility setting `analyzer_compatibility_allow_non_aggregate_in_having`
-- mimics the legacy AST rewrite that moved non-aggregate AND-conjuncts from
-- HAVING to WHERE.

DROP TABLE IF EXISTS test_a1;
DROP TABLE IF EXISTS test_a2;
DROP TABLE IF EXISTS test_a3;
DROP TABLE IF EXISTS test_a4;
DROP TABLE IF EXISTS test_a5;
DROP TABLE IF EXISTS test_a6;

CREATE TABLE test_a1 (id UInt32, category String, service String, value UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO test_a1 VALUES (1,'a','svc1',10),(2,'a','svc2',20),(3,'b','svc1',30);

-- Default: error (standard SQL semantics).
SELECT category, sum(value) FROM test_a1 GROUP BY category HAVING service = 'svc1' SETTINGS enable_analyzer = 1; -- { serverError NOT_AN_AGGREGATE }

-- A.1 - plain column in HAVING.
SELECT 'A.1';
SELECT category, sum(value) AS total FROM test_a1 GROUP BY category HAVING service = 'svc1'
ORDER BY category
SETTINGS enable_analyzer = 1, analyzer_compatibility_allow_non_aggregate_in_having = 1;

-- A.2 - JOIN-aliased column in HAVING.
CREATE TABLE test_a2 (id UInt32, state String, created DateTime) ENGINE = MergeTree ORDER BY id;
INSERT INTO test_a2 VALUES
    (1, 'started',  '2024-01-01 00:00:00'),
    (1, 'finished', '2024-01-01 01:00:00'),
    (2, 'started',  '2024-01-02 00:00:00'),
    (2, 'finished', '2024-01-02 02:00:00');

SELECT 'A.2';
SELECT
    toDate(m_start.created) AS start_time,
    avg(dateDiff('second', m_start.created, m_end.created)) AS avg_runtime
FROM test_a2 AS m_start
INNER JOIN test_a2 AS m_end ON m_start.id = m_end.id
GROUP BY start_time
HAVING (m_start.state = 'started') AND (m_end.state = 'finished')
ORDER BY start_time
SETTINGS enable_analyzer = 1, analyzer_compatibility_allow_non_aggregate_in_having = 1;

-- A.3 - ARRAY JOIN-introduced column in HAVING.
CREATE TABLE test_a3 (
    asset_key String, site_id UInt32,
    vuln_ids Array(String), vuln_scores Array(Float32)
) ENGINE = MergeTree ORDER BY asset_key;
INSERT INTO test_a3 VALUES
    ('host1', 1, ['CVE-001','CVE-002'], [9.8, 3.0]),
    ('host2', 1, ['CVE-003'], [5.0]);

SELECT 'A.3';
SELECT groupUniqArray(cve) AS vulnerabilities, asset_key
FROM test_a3
LEFT ARRAY JOIN vuln_ids AS cve, vuln_scores AS severity
GROUP BY site_id, asset_key
HAVING severity > 5.0
ORDER BY asset_key
SETTINGS enable_analyzer = 1, analyzer_compatibility_allow_non_aggregate_in_having = 1;

-- A.4 - map element GROUP BY, plain column in HAVING.
CREATE TABLE test_a4 (
    service String, ts DateTime,
    tags Map(String, String), attributes Map(String, String)
) ENGINE = MergeTree ORDER BY ts;
INSERT INTO test_a4 VALUES
    ('web', '2024-01-01 00:00:00', {'company':'c1','app':'app1'}, {'bytes':'100'}),
    ('api', '2024-01-01 00:00:00', {'company':'c1','app':'app2'}, {'bytes':'200'}),
    ('web', '2024-01-01 00:00:00', {'company':'c2','app':'app1'}, {'bytes':'300'});

SELECT 'A.4';
SELECT tags['company'] AS idCompany, tags['app'] AS idApp,
       sum(toInt64(attributes['bytes'])) AS value
FROM test_a4
GROUP BY idCompany, idApp
HAVING (service = 'web') AND (value > 0)
ORDER BY idCompany, idApp
SETTINGS enable_analyzer = 1, analyzer_compatibility_allow_non_aggregate_in_having = 1;

-- A.5 - HAVING in CTE body.
CREATE TABLE test_a5 (
    ts DateTime, category String,
    service String, value UInt32
) ENGINE = MergeTree ORDER BY ts;
INSERT INTO test_a5 VALUES
    ('2024-01-01 00:00:00', 'a', 'svc1', 10),
    ('2024-01-01 00:00:00', 'a', 'svc2', 20),
    ('2024-01-01 00:00:00', 'b', 'svc1', 30);

SELECT 'A.5';
WITH filtered AS (
    SELECT toStartOfHour(ts) AS hour, category, count() AS cnt
    FROM test_a5
    GROUP BY hour, category
    HAVING (cnt > 0) AND (service = 'svc1')
)
SELECT * FROM filtered
ORDER BY hour, category
SETTINGS enable_analyzer = 1, analyzer_compatibility_allow_non_aggregate_in_having = 1;

-- A.6 - mixed aggregate alias and raw column.
CREATE TABLE test_a6 (
    session_id String, device_id String,
    ad_attempts UInt32, value UInt32
) ENGINE = MergeTree ORDER BY session_id;
INSERT INTO test_a6 VALUES
    ('s1', 'd1', 3, 10),
    ('s1', 'd1', 0, 20),
    ('s2', 'd2', 5, 30);

SELECT 'A.6';
SELECT session_id, device_id, sum(ad_attempts) AS total_attempts
FROM test_a6
GROUP BY session_id, device_id
HAVING (total_attempts > 0) AND (ad_attempts > 0)
ORDER BY session_id, device_id
SETTINGS enable_analyzer = 1, analyzer_compatibility_allow_non_aggregate_in_having = 1;

-- Result must match the legacy analyzer for the canonical case. The legacy AST-level
-- HAVING-to-WHERE rewrite lives in `PredicateExpressionsOptimizer` and is gated on
-- `enable_optimize_predicate_expression`; the flaky-check randomizer can disable it,
-- so pin it here to keep the parity assertion deterministic.
SELECT 'A.1 legacy parity';
SELECT category, sum(value) AS total FROM test_a1 GROUP BY category HAVING service = 'svc1'
ORDER BY category
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 1;

SELECT 'A.1 legacy parity';
SELECT category, sum(value) AS total FROM test_a1 GROUP BY category HAVING service = 'svc1'
ORDER BY category
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 1, group_by_use_nulls = 1;

SELECT 'A.1 legacy parity';
SELECT category, sum(value) AS total FROM test_a1 GROUP BY category HAVING service = 'svc1'
ORDER BY category
SETTINGS enable_analyzer = 1, analyzer_compatibility_allow_non_aggregate_in_having = 1, group_by_use_nulls = 1;

-- Setting enabled but without GROUP BY/aggregates: setting is a no-op.
SELECT 'no-aggregation no-op';
SELECT count() FROM test_a1 SETTINGS enable_analyzer = 1, analyzer_compatibility_allow_non_aggregate_in_having = 1;

-- WITH CUBE: setting must NOT bypass validation.
SELECT category, sum(value) FROM test_a1 GROUP BY category WITH CUBE HAVING service = 'svc1'
SETTINGS enable_analyzer = 1, analyzer_compatibility_allow_non_aggregate_in_having = 1; -- { serverError NOT_AN_AGGREGATE }

-- WITH ROLLUP: setting must NOT bypass validation.
SELECT category, sum(value) FROM test_a1 GROUP BY category WITH ROLLUP HAVING service = 'svc1'
SETTINGS enable_analyzer = 1, analyzer_compatibility_allow_non_aggregate_in_having = 1; -- { serverError NOT_AN_AGGREGATE }

-- WITH TOTALS: setting must NOT bypass validation.
SELECT category, sum(value) FROM test_a1 GROUP BY category WITH TOTALS HAVING service = 'svc1'
SETTINGS enable_analyzer = 1, analyzer_compatibility_allow_non_aggregate_in_having = 1; -- { serverError NOT_AN_AGGREGATE }

-- GROUPING SETS: setting must NOT bypass validation.
SELECT category, sum(value) FROM test_a1 GROUP BY GROUPING SETS ((category)) HAVING service = 'svc1'
SETTINGS enable_analyzer = 1, analyzer_compatibility_allow_non_aggregate_in_having = 1; -- { serverError NOT_AN_AGGREGATE }

-- WITH ROLLUP combined with group_by_use_nulls: the early WITH-ROLLUP guard wins, so still errors.
SELECT category, sum(value) FROM test_a1 GROUP BY category WITH ROLLUP HAVING service = 'svc1'
SETTINGS enable_analyzer = 1, analyzer_compatibility_allow_non_aggregate_in_having = 1, group_by_use_nulls = 1; -- { serverError NOT_AN_AGGREGATE }

-- A stateful function in any conjunct aborts the legacy rewrite; matches `tryMovePredicatesFromHavingToWhere` legacy behavior.
SELECT category, sum(value) FROM test_a1 GROUP BY category HAVING rowNumberInBlock() >= 0 AND service = 'svc1'
SETTINGS enable_analyzer = 1, analyzer_compatibility_allow_non_aggregate_in_having = 1; -- { serverError NOT_AN_AGGREGATE }

-- A window function in any conjunct aborts the legacy rewrite; legacy `tryMovePredicatesFromHavingToWhere` also `return false`s on window funcs. Window funcs in HAVING are invalid; validation surfaces its own error.
SELECT category, sum(value) FROM test_a1 GROUP BY category HAVING (count() OVER ()) > 0 AND service = 'svc1'
SETTINGS enable_analyzer = 1, analyzer_compatibility_allow_non_aggregate_in_having = 1; -- { serverError ILLEGAL_AGGREGATION }

-- Non-deterministic predicates stay in HAVING (stricter than legacy, which would have moved them to WHERE and silently changed per-group evaluation to per-row). Sibling deterministic conjuncts still move to WHERE.
SELECT 'A.7 non-deterministic conjunct stays in HAVING';
SELECT category, sum(value) AS total FROM test_a1 GROUP BY category HAVING ((rand() % 1) = 0) AND service = 'svc1'
ORDER BY category
SETTINGS enable_analyzer = 1, analyzer_compatibility_allow_non_aggregate_in_having = 1;

DROP TABLE test_a1;
DROP TABLE test_a2;
DROP TABLE test_a3;
DROP TABLE test_a4;
DROP TABLE test_a5;
DROP TABLE test_a6;
