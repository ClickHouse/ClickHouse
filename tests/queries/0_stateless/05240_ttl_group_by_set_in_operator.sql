-- A `GROUP BY ... SET` assignment whose `IN` needs a set built from a subquery or from a table read is
-- rejected at DDL time: nothing on the TTL path builds such a set, so the assignment could only fail on
-- every TTL merge of a part with expired rows.
-- The table on the right of `IN` is database-qualified because an unqualified name in a TTL expression is
-- resolved against `default` and not against the current database, independently of this check.
CREATE TABLE ttl_bad (d DateTime, v UInt64) ENGINE = MergeTree ORDER BY d
    TTL d + INTERVAL 1 SECOND GROUP BY d SET v = toUInt64(max(v) IN system.one); -- { serverError BAD_ARGUMENTS }
CREATE TABLE ttl_bad (d DateTime, v UInt64) ENGINE = MergeTree ORDER BY d
    TTL d + INTERVAL 1 SECOND GROUP BY d SET v = toUInt64(max(v) IN (SELECT 1)); -- { serverError BAD_ARGUMENTS }
CREATE TABLE ttl_bad (d DateTime, v UInt64) ENGINE = MergeTree ORDER BY d
    TTL d + INTERVAL 1 SECOND GROUP BY d SET v = toUInt64(max(v) GLOBAL IN (SELECT 1)); -- { serverError BAD_ARGUMENTS }
CREATE TABLE ttl_bad (d DateTime, v UInt64) ENGINE = MergeTree ORDER BY d
    TTL d + INTERVAL 1 SECOND GROUP BY d SET v = toUInt64(max(v) NOT IN (SELECT 1)); -- { serverError BAD_ARGUMENTS }
-- The `IN` may sit inside a lambda body, where the set is registered while the captured body is built.
CREATE TABLE ttl_bad (d DateTime, v UInt64) ENGINE = MergeTree ORDER BY d
    TTL d + INTERVAL 1 SECOND GROUP BY d SET v = toUInt64(arrayExists(x -> x IN system.one, [max(v)])); -- { serverError BAD_ARGUMENTS }
-- `EXISTS` becomes `in(1, (SELECT ... LIMIT 1))` during normalization, after any check on the written AST.
CREATE TABLE ttl_bad (d DateTime, v UInt64) ENGINE = MergeTree ORDER BY d
    TTL d + INTERVAL 1 SECOND GROUP BY d SET v = toUInt64(max(v)) + toUInt64(EXISTS (SELECT 1)); -- { serverError BAD_ARGUMENTS }
-- An `IN` under `indexHint` is rejected too: the hint's argument is analysed into a separate DAG that
-- shares the prepared-set register, while the executable node drops its arguments and returns a constant,
-- so the set is registered but never evaluated. The sibling key guards reject the same spelling.
CREATE TABLE ttl_bad (d DateTime, v UInt64) ENGINE = MergeTree ORDER BY d
    TTL d + INTERVAL 1 SECOND GROUP BY d SET v = toUInt64(max(v)) + toUInt64(indexHint(v IN system.one)); -- { serverError BAD_ARGUMENTS }

-- A literal set is complete at analysis time and keeps working. `r` is the oracle: `max(v)` is 109 in the
-- first group and 201 in the second, so an empty or unevaluated set could not produce the pair (1, 0).
CREATE TABLE ttl_lit (d DateTime, v UInt64, r UInt64) ENGINE = MergeTree ORDER BY d
    TTL d + INTERVAL 1 SECOND GROUP BY d SET v = max(v), r = toUInt64(max(v) IN (109, 200))
    SETTINGS merge_with_ttl_timeout = 100000;
INSERT INTO ttl_lit VALUES ('2020-01-01 00:00:00', 9, 0), ('2020-01-01 00:00:00', 109, 0),
                           ('2020-01-01 00:00:01', 1, 0), ('2020-01-01 00:00:01', 201, 0);
OPTIMIZE TABLE ttl_lit FINAL;
SELECT count() FROM ttl_lit;
SELECT v, r FROM ttl_lit ORDER BY d;

ALTER TABLE ttl_lit MODIFY TTL d + INTERVAL 1 SECOND GROUP BY d SET v = toUInt64(max(v) IN system.one); -- { serverError BAD_ARGUMENTS }

-- A scalar subquery is folded to a literal before the assignment's actions are built, so it needs no set of
-- its own and stays accepted, even when its body contains an `IN`.
CREATE TABLE ttl_scalar (d DateTime, v UInt64, r UInt64) ENGINE = MergeTree ORDER BY d
    TTL d + INTERVAL 1 SECOND GROUP BY d SET v = max(v), r = toUInt64(max(v)) + (SELECT 0 IN system.one)
    SETTINGS merge_with_ttl_timeout = 100000;
INSERT INTO ttl_scalar VALUES ('2020-01-01 00:00:00', 100, 0);
OPTIMIZE TABLE ttl_scalar FINAL;
SELECT v, r FROM ttl_scalar;

-- The TTL `DELETE WHERE` predicate is a different case and must keep working: its sets are collected by
-- `TTLTransform` and built by the merge and by the INSERT path.
CREATE TABLE ttl_del (d DateTime, v UInt64) ENGINE = MergeTree ORDER BY d
    TTL d + INTERVAL 1 SECOND DELETE WHERE v IN (SELECT 100)
    SETTINGS merge_with_ttl_timeout = 100000;
INSERT INTO ttl_del VALUES ('2020-01-01 00:00:00', 100), ('2020-01-01 00:00:00', 101);
OPTIMIZE TABLE ttl_del FINAL;
SELECT v FROM ttl_del;

-- A definition stored before this check existed still loads and still accepts writes, and fails only where
-- it would execute. `allow_suspicious_ttl_expressions` is the only way to produce one now.
SET allow_suspicious_ttl_expressions = 1;
CREATE TABLE ttl_stored (d DateTime, v UInt64, r UInt64) ENGINE = MergeTree ORDER BY d
    TTL d + INTERVAL 1 SECOND GROUP BY d SET r = toUInt64(max(v) IN system.one)
    SETTINGS merge_with_ttl_timeout = 100000;
CREATE TABLE ttl_stored_lambda (d DateTime, v UInt64, r UInt64) ENGINE = MergeTree ORDER BY d
    TTL d + INTERVAL 1 SECOND GROUP BY d SET r = toUInt64(arrayExists(x -> x IN system.one, [max(v)]))
    SETTINGS merge_with_ttl_timeout = 100000;
SET allow_suspicious_ttl_expressions = 0;

INSERT INTO ttl_stored VALUES ('2020-01-01 00:00:00', 100, 0);
SELECT count() FROM ttl_stored;
OPTIMIZE TABLE ttl_stored FINAL; -- { serverError BAD_ARGUMENTS }
DETACH TABLE ttl_stored;
ATTACH TABLE ttl_stored;
SELECT count() FROM ttl_stored;
OPTIMIZE TABLE ttl_stored FINAL; -- { serverError BAD_ARGUMENTS }
ALTER TABLE ttl_stored MODIFY TTL d + INTERVAL 1 SECOND GROUP BY d SET v = max(v), r = toUInt64(max(v) IN (100, 200));
OPTIMIZE TABLE ttl_stored FINAL;
SELECT v, r FROM ttl_stored;

INSERT INTO ttl_stored_lambda VALUES ('2020-01-01 00:00:00', 100, 0);
OPTIMIZE TABLE ttl_stored_lambda FINAL; -- { serverError BAD_ARGUMENTS }
DETACH TABLE ttl_stored_lambda;
ATTACH TABLE ttl_stored_lambda;
OPTIMIZE TABLE ttl_stored_lambda FINAL; -- { serverError BAD_ARGUMENTS }

-- A merge that processes TTL but aggregates nothing for the offending rule keeps succeeding: the part
-- predates the rule, so the merge is forced to recalculate TTLs, and no row is expired.
CREATE TABLE ttl_future (d DateTime, v UInt64) ENGINE = MergeTree ORDER BY d
    SETTINGS merge_with_ttl_timeout = 100000;
INSERT INTO ttl_future VALUES ('2099-01-01 00:00:00', 100);
SET allow_suspicious_ttl_expressions = 1, materialize_ttl_after_modify = 0;
ALTER TABLE ttl_future MODIFY TTL d + INTERVAL 1 SECOND GROUP BY d SET v = toUInt64(max(v) IN system.one);
SET allow_suspicious_ttl_expressions = 0, materialize_ttl_after_modify = 1;
OPTIMIZE TABLE ttl_future FINAL;
SELECT v FROM ttl_future;

DROP TABLE ttl_future;
DROP TABLE ttl_stored_lambda;
DROP TABLE ttl_stored;
DROP TABLE ttl_del;
DROP TABLE ttl_scalar;
DROP TABLE ttl_lit;
