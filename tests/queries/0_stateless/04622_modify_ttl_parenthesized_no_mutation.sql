-- Restating the same TTL, differing only in the redundant top-level parentheses preserved
-- since #92340 (`TTL (expr)` vs `TTL expr`), must not schedule a `MATERIALIZE TTL` mutation.

DROP TABLE IF EXISTS t_ttl_parens;

CREATE TABLE t_ttl_parens (k UInt32, d DateTime, x UInt32 TTL (d + INTERVAL 2 YEAR))
ENGINE = MergeTree ORDER BY k
TTL (d + INTERVAL 10 YEAR);

INSERT INTO t_ttl_parens VALUES (1, now(), 1);

ALTER TABLE t_ttl_parens MODIFY TTL d + INTERVAL 10 YEAR;
ALTER TABLE t_ttl_parens MODIFY COLUMN x UInt32 TTL d + INTERVAL 2 YEAR;

SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_ttl_parens';

-- A genuinely different TTL must still schedule the mutation.
ALTER TABLE t_ttl_parens MODIFY TTL (d + INTERVAL 20 YEAR) SETTINGS mutations_sync = 1;

SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_ttl_parens' AND command LIKE '%MATERIALIZE TTL%';

DROP TABLE t_ttl_parens;
