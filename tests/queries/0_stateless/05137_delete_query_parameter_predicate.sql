DROP TABLE IF EXISTS t_delete_param;
CREATE TABLE t_delete_param (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_delete_param SELECT number FROM numbers(3);

SET param_none = 0;
SET param_all = 1;

-- The parameter is the whole predicate, so the `predicate` member points straight at the node
-- substitution replaces; both values are asserted so the substituted value itself is checked.
DELETE FROM t_delete_param WHERE {none:UInt8};
SELECT count() FROM t_delete_param;

DELETE FROM t_delete_param WHERE {all:UInt8};
SELECT count() FROM t_delete_param;

DELETE FROM t_delete_param WHERE {unset:UInt8}; -- { serverError UNKNOWN_QUERY_PARAMETER }

DROP TABLE t_delete_param;
