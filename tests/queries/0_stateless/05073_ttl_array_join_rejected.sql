-- https://github.com/ClickHouse/ClickHouse/issues/117014
-- `arrayJoin` changes the number of rows, while every consumer of a TTL expression indexes its result
-- column positionally against the block's rows. Such a TTL used to be accepted and then silently
-- deleted rows whose own TTL was far in the future, reading past the end of the column for empty
-- arrays. Not even `allow_suspicious_ttl_expressions` may allow it.

DROP TABLE IF EXISTS t_ttl_array_join;
CREATE TABLE t_ttl_array_join (k UInt32, arr Array(DateTime)) ENGINE = MergeTree ORDER BY k TTL arrayJoin(arr); -- { serverError BAD_TTL_EXPRESSION }
CREATE TABLE t_ttl_array_join (k UInt32, arr Array(DateTime)) ENGINE = MergeTree ORDER BY k TTL arrayJoin(arr) SETTINGS allow_suspicious_ttl_expressions = 1; -- { serverError BAD_TTL_EXPRESSION }
CREATE TABLE t_ttl_array_join (k UInt32, arr Array(DateTime)) ENGINE = MergeTree ORDER BY k TTL arrayJoin(arr) WHERE k > 0; -- { serverError BAD_TTL_EXPRESSION }
CREATE TABLE t_ttl_array_join (k UInt32, arr Array(DateTime)) ENGINE = MergeTree ORDER BY k TTL arr[1] WHERE arrayJoin(arr) > now(); -- { serverError BAD_TTL_EXPRESSION }

CREATE TABLE t_ttl_array_join (k UInt32, arr Array(DateTime), ts DateTime) ENGINE = MergeTree ORDER BY k;
ALTER TABLE t_ttl_array_join MODIFY TTL arrayJoin(arr); -- { serverError BAD_TTL_EXPRESSION }
ALTER TABLE t_ttl_array_join MODIFY TTL arrayJoin(arr) SETTINGS allow_suspicious_ttl_expressions = 1; -- { serverError BAD_TTL_EXPRESSION }

-- A column TTL is checked the same way.
ALTER TABLE t_ttl_array_join MODIFY COLUMN ts DateTime TTL arrayJoin(arr); -- { serverError BAD_TTL_EXPRESSION }

-- The `unnest` alias is the same function, so it is rejected the same way, whatever its spelling and
-- whether or not `normalize_function_names` canonicalized the name in the AST.
ALTER TABLE t_ttl_array_join MODIFY TTL unnest(arr); -- { serverError BAD_TTL_EXPRESSION }
ALTER TABLE t_ttl_array_join MODIFY TTL UNNEST(arr); -- { serverError BAD_TTL_EXPRESSION }

SET normalize_function_names = 0;
ALTER TABLE t_ttl_array_join MODIFY TTL unnest(arr); -- { serverError BAD_TTL_EXPRESSION }
ALTER TABLE t_ttl_array_join MODIFY TTL unnest(arr) SETTINGS allow_suspicious_ttl_expressions = 1; -- { serverError BAD_TTL_EXPRESSION }
ALTER TABLE t_ttl_array_join MODIFY TTL arr[1] WHERE unnest(arr) > now() SETTINGS allow_suspicious_ttl_expressions = 1; -- { serverError BAD_TTL_EXPRESSION }
ALTER TABLE t_ttl_array_join MODIFY COLUMN ts DateTime TTL unnest(arr) SETTINGS allow_suspicious_ttl_expressions = 1; -- { serverError BAD_TTL_EXPRESSION }
SET normalize_function_names = 1;

-- An ordinary TTL still works, and still expires only what it should.
SELECT 'ordinary ttl';
ALTER TABLE t_ttl_array_join MODIFY TTL arr[1] SETTINGS materialize_ttl_after_modify = 0;
INSERT INTO t_ttl_array_join VALUES (1, [now() - 3600, now() - 3600], now()), (2, [now() + 100000], now());
OPTIMIZE TABLE t_ttl_array_join FINAL;
SELECT k FROM t_ttl_array_join ORDER BY k;

DROP TABLE t_ttl_array_join;
