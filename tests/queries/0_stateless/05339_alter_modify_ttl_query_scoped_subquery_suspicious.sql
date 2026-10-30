-- `allow_suspicious_ttl_expressions` admits a TTL that depends on no column, but not one that the table cannot be loaded with.

DROP TABLE IF EXISTS t_ttl_suspicious;
SET ast_fuzzer_any_query = 0;

DROP TABLE IF EXISTS t_ttl_suspicious;
CREATE TABLE t_ttl_suspicious (d DateTime, x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_ttl_suspicious VALUES ('2100-01-01 00:00:00', 1);

ALTER TABLE t_ttl_suspicious MODIFY TTL toDateTime('2000-01-01 00:00:00') WHERE x < (SELECT count() FROM numbers(10)); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_ttl_suspicious MODIFY TTL toDateTime('2000-01-01 00:00:00') WHERE x < (SELECT count() FROM numbers(10)) SETTINGS allow_suspicious_ttl_expressions = 1; -- { serverError THERE_IS_NO_QUERY }

DETACH TABLE t_ttl_suspicious;
ATTACH TABLE t_ttl_suspicious;
SELECT count() FROM t_ttl_suspicious;

DROP TABLE t_ttl_suspicious;
