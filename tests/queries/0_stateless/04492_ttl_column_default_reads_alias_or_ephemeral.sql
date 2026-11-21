-- A column TTL resets an expired value to its DEFAULT / MATERIALIZED expression as a read of a part without the column computes it:
-- ALIAS and EPHEMERAL references, lambda parameters named like table columns, and the expression's own aliases.

DROP TABLE IF EXISTS t_wide;
CREATE TABLE t_wide (d Date, k UInt32, q String, a String ALIAS concat(q, 'x'), w String DEFAULT a TTL d + INTERVAL 1 DAY,
    PROJECTION p (SELECT k, w ORDER BY k)) ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_wide (d, k, q, w) SELECT '2000-01-01', number, toString(number), 'stored' FROM numbers(100);
OPTIMIZE TABLE t_wide FINAL;
SELECT 'wide base', countIf(w = concat(q, 'x')), count() FROM t_wide SETTINGS optimize_use_projections = 0;
SELECT 'wide proj', countIf(w = concat(toString(k), 'x')), count() FROM t_wide SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
DROP TABLE t_wide;

DROP TABLE IF EXISTS t_compact;
CREATE TABLE t_compact (d Date, k UInt32, q String, a String ALIAS concat(q, 'x'), w String DEFAULT a TTL d + INTERVAL 1 DAY)
    ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 1000000000;
INSERT INTO t_compact (d, k, q, w) SELECT '2000-01-01', number, toString(number), 'stored' FROM numbers(100);
OPTIMIZE TABLE t_compact FINAL;
SELECT 'compact', countIf(w = concat(q, 'x')), count() FROM t_compact;
DROP TABLE t_compact;

DROP TABLE IF EXISTS t_partial;
CREATE TABLE t_partial (d Date, k UInt32, q String, a String ALIAS concat(q, 'x'), b String ALIAS upper(a),
    e String EPHEMERAL 'eph',
    w String DEFAULT a TTL d + INTERVAL 1 DAY,
    w2 String DEFAULT b TTL d + INTERVAL 1 DAY,
    w3 String DEFAULT concat(e, '-xx') TTL d + INTERVAL 1 DAY,
    w4 String DEFAULT arrayMap(z -> concat(z, a), ['1'])[1] TTL d + INTERVAL 1 DAY,
    a2 String ALIAS arrayMap(q -> concat(q, '!'), ['alias'])[1],
    w5 String DEFAULT a2 TTL d + INTERVAL 1 DAY,
    w6 String DEFAULT concat(a, arrayMap(q -> q, ['!'])[1]) TTL d + INTERVAL 1 DAY,
    m String MATERIALIZED concat(a, '!') TTL d + INTERVAL 1 DAY)
    ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_partial (d, k, q, e, w, w2, w3, w4, w5, w6) SELECT if(number % 2 = 0, '2000-01-01', '2100-01-01'), number, toString(number),
    'given', 'stored', 'stored', 'stored', 'stored', 'stored', 'stored' FROM numbers(100);
OPTIMIZE TABLE t_partial FINAL;
SELECT 'partial', d < '2050-01-01' AS expired, countIf(w = concat(q, 'x')), countIf(w2 = upper(concat(q, 'x'))),
    countIf(w3 = 'eph-xx'), countIf(w4 = concat('1', q, 'x')), countIf(w5 = 'alias!'), countIf(w6 = concat(q, 'x!')),
    countIf(m = concat(q, 'x!')),
    countIf(w = 'stored' AND w2 = 'stored' AND w3 = 'stored' AND w4 = 'stored' AND w5 = 'stored' AND w6 = 'stored'), count()
    FROM t_partial GROUP BY expired ORDER BY expired;
DROP TABLE t_partial;

DROP TABLE IF EXISTS t_recompute;
CREATE TABLE t_recompute (d Date, k UInt32, q String, w String DEFAULT concat(q, '-') TTL d + INTERVAL 1 DAY,
    c String ALIAS concat(w, '!'), r String MATERIALIZED c)
    ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_recompute (d, k, q, w) SELECT if(number % 2 = 0, '2000-01-01', '2100-01-01'), number, toString(number), 'stored' FROM numbers(100);
OPTIMIZE TABLE t_recompute FINAL;
SELECT 'recompute', d < '2050-01-01' AS expired, countIf(w = concat(q, '-') AND r = concat(q, '-!')), countIf(w = 'stored' AND r = 'stored!'), count()
    FROM t_recompute GROUP BY expired ORDER BY expired;
DROP TABLE t_recompute;

DROP TABLE IF EXISTS t_result_alias;
CREATE TABLE t_result_alias (d Date, k UInt32, q String, z Int64,
    w String DEFAULT (concat(q, 'D') AS w) TTL d + INTERVAL 1 DAY,
    x Int64 DEFAULT 5 TTL d + INTERVAL 1 DAY,
    m Int64 MATERIALIZED (x + z AS z))
    ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_result_alias (d, k, q, z, w, x) SELECT if(number % 2 = 0, '2000-01-01', '2100-01-01'), number, toString(number),
    100, 'stored', 10 FROM numbers(100);
OPTIMIZE TABLE t_result_alias FINAL;
SELECT 'result alias', d < '2050-01-01' AS expired, countIf(w = concat(q, 'D')), countIf(x = 5 AND m = 105),
    countIf(w = 'stored' AND x = 10 AND m = 110), count()
    FROM t_result_alias GROUP BY expired ORDER BY expired;
DROP TABLE t_result_alias;

DROP TABLE IF EXISTS t_recompute_capture;
CREATE TABLE t_recompute_capture (d Date, k UInt32, q String, w String DEFAULT 'D' TTL d + INTERVAL 1 DAY,
    b String ALIAS concat(q, w), a String ALIAS arrayMap(q -> concat(q, b), ['1'])[1], m String MATERIALIZED a,
    c String ALIAS concat((q AS z), z, w), m2 String MATERIALIZED arrayMap(z -> concat(z, c), ['1'])[1])
    ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_recompute_capture (d, k, q, w) SELECT if(number % 2 = 0, '2000-01-01', '2100-01-01'), number, toString(number),
    'stored' FROM numbers(100);
OPTIMIZE TABLE t_recompute_capture FINAL;
SELECT 'recompute capture', d < '2050-01-01' AS expired, countIf(w = 'D'), countIf(m = concat('1', q, w) AND m2 = concat('1', q, q, w)), count()
    FROM t_recompute_capture GROUP BY expired ORDER BY expired;
DROP TABLE t_recompute_capture;

DROP TABLE IF EXISTS t_capture;
CREATE TABLE t_capture (d Date, k0 UInt32, q String, `q.a` String, arr Array(Tuple(a String)),
    a String ALIAS concat(q, 'x'), b String ALIAS upper(a), `k.a` String ALIAS concat(q, 'x'), c String ALIAS concat(q.a, 'x'),
    w String DEFAULT arrayMap(q -> concat(q, a), ['1'])[1] TTL d + INTERVAL 1 DAY,
    w2 String DEFAULT arrayMap(q -> concat(q, b), ['1'])[1] TTL d + INTERVAL 1 DAY,
    w3 String DEFAULT arrayMap(k -> k.a, arr)[1] TTL d + INTERVAL 1 DAY,
    w4 String DEFAULT arrayMap(`q.a` -> concat(`q.a`, c), ['1'])[1] TTL d + INTERVAL 1 DAY)
    ENGINE = MergeTree ORDER BY k0 SETTINGS min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 1000000000;
INSERT INTO t_capture (d, k0, q, `q.a`, arr, w, w2, w3, w4) SELECT '2000-01-01', number, toString(number), concat('d', toString(number)),
    [tuple('field')], 'stored', 'stored', 'stored', 'stored' FROM numbers(10);
OPTIMIZE TABLE t_capture FINAL;
SELECT 'capture', countIf(w = concat('1', q, 'x')), countIf(w2 = concat('1', upper(concat(q, 'x')))), countIf(w3 = 'field'),
    countIf(w4 = concat('1', `q.a`, 'x')), count() FROM t_capture;
DROP TABLE t_capture;

DROP TABLE IF EXISTS t_lambda;
CREATE TABLE t_lambda (d Date, k UInt32, t Tuple(b String), n Nested(b String), `q.a.b` String, arr Array(Tuple(b String)),
    w String DEFAULT arrayMap(t -> concat(t.b, 'y'), arr)[1] TTL d + INTERVAL 1 DAY,
    w2 String DEFAULT arrayMap(n -> concat(n.b, 'y'), arr)[1] TTL d + INTERVAL 1 DAY,
    w3 String DEFAULT arrayMap(`q.a` -> concat(`q.a`.b, 'y'), arr)[1] TTL d + INTERVAL 1 DAY,
    x String DEFAULT 'r' TTL d + INTERVAL 1 DAY,
    m String MATERIALIZED arrayMap(n -> concat(n.b, x), arr)[1])
    ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 1000000000;
INSERT INTO t_lambda (d, k, t, `n.b`, `q.a.b`, arr, w, w2, w3, x) SELECT if(number % 2 = 0, '2000-01-01', '2100-01-01'), number,
    tuple('column'), ['nested'], 'q', [tuple('field')], 'stored', 'stored', 'stored', 's' FROM numbers(100);
OPTIMIZE TABLE t_lambda FINAL;
SELECT 'lambda', d < '2050-01-01' AS expired, countIf(w = 'fieldy' AND w2 = 'fieldy' AND w3 = 'fieldy'), countIf(x = 'r' AND m = 'fieldr'),
    countIf(w = 'stored' AND w2 = 'stored' AND w3 = 'stored' AND x = 's' AND m = 'fields'), count()
    FROM t_lambda GROUP BY expired ORDER BY expired;
DROP TABLE t_lambda;

DROP TABLE IF EXISTS t_lambda_wide;
CREATE TABLE t_lambda_wide (d Date, k UInt32, t Tuple(b String), arr Array(Tuple(b String)),
    w String DEFAULT arrayMap(t -> concat(t.b, 'y'), arr)[1] TTL d + INTERVAL 1 DAY, PROJECTION p (SELECT k, w ORDER BY k))
    ENGINE = MergeTree PARTITION BY intDiv(k, 50) ORDER BY k SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_lambda_wide SELECT if(number < 50 OR number % 2 = 0, '2000-01-01', '2100-01-01'), number, tuple('column'), [tuple('field')],
    'stored' FROM numbers(100);
OPTIMIZE TABLE t_lambda_wide FINAL;
SELECT 'lambda wide base', countIf(w = 'fieldy'), countIf(w = 'stored') FROM t_lambda_wide SETTINGS optimize_use_projections = 0;
SELECT 'lambda wide proj', countIf(w = 'fieldy'), countIf(w = 'stored') FROM t_lambda_wide
    SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
DROP TABLE t_lambda_wide;

DROP TABLE IF EXISTS t_local_alias;
CREATE TABLE t_local_alias (d Date, k UInt32, q String, a String ALIAS concat(q, 'x'), e String EPHEMERAL 'eph',
    w String DEFAULT arrayMap(z -> concat((z AS a), a), ['D'])[1] TTL d + INTERVAL 1 DAY,
    w2 String DEFAULT arrayMap(z -> concat((z AS a), (a AS r)), ['D'])[1] TTL d + INTERVAL 1 DAY,
    w3 String DEFAULT arrayMap(z -> concat((z AS e), e), ['D'])[1] TTL d + INTERVAL 1 DAY,
    w4 String DEFAULT concat((a AS r), r) TTL d + INTERVAL 1 DAY,
    m String MATERIALIZED arrayMap(z -> concat((z AS a), (a AS r), w4), ['Z'])[1])
    ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 1000000000;
INSERT INTO t_local_alias (d, k, q, e, w, w2, w3, w4) SELECT if(number % 2 = 0, '2000-01-01', '2100-01-01'), number, toString(number),
    'given', 'stored', 'stored', 'stored', 'stored' FROM numbers(100);
OPTIMIZE TABLE t_local_alias FINAL;
SELECT 'local alias', d < '2050-01-01' AS expired, countIf(w = 'DD' AND w2 = 'DD' AND w3 = 'DD'), countIf(w4 = concat(q, 'x', q, 'x')),
    countIf(m = concat('ZZ', w4)), countIf(w = 'stored' AND w2 = 'stored' AND w3 = 'stored' AND w4 = 'stored'), count()
    FROM t_local_alias GROUP BY expired ORDER BY expired;
DROP TABLE t_local_alias;

DROP TABLE IF EXISTS t_update;
CREATE TABLE t_update (d Date, k UInt32, q String, a String ALIAS concat(q, 'x'), w String DEFAULT a TTL d + INTERVAL 1 DAY)
    ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 1000000000;
INSERT INTO t_update (d, k, q, w) SELECT '2100-01-01', number, toString(number), 'stored' FROM numbers(100);
ALTER TABLE t_update UPDATE d = '2000-01-01' WHERE k % 2 = 0 SETTINGS mutations_sync = 2;
SELECT 'update', countIf(w = concat(q, 'x')), countIf(w = 'stored') FROM t_update;
DROP TABLE t_update;

DROP TABLE IF EXISTS t_materialize;
CREATE TABLE t_materialize (d Date, k UInt32, q String, a String ALIAS concat(q, 'x'), w String DEFAULT a TTL d + INTERVAL 1 DAY,
    t Tuple(b String), arr Array(Tuple(b String)), w2 String DEFAULT arrayMap(t -> concat(t.b, 'y'), arr)[1] TTL d + INTERVAL 1 DAY)
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 1000000000, materialize_ttl_recalculate_only = 0;
INSERT INTO t_materialize (d, k, q, w, t, arr, w2) SELECT if(number % 2 = 0, '2000-01-01', '2100-01-01'), number, toString(number), 'stored',
    tuple('column'), [tuple('field')], 'stored' FROM numbers(100);
ALTER TABLE t_materialize MATERIALIZE TTL SETTINGS mutations_sync = 2;
SELECT 'materialize ttl', countIf(w = concat(q, 'x') AND w2 = 'fieldy'), countIf(w = 'stored' AND w2 = 'stored') FROM t_materialize;
DROP TABLE t_materialize;
