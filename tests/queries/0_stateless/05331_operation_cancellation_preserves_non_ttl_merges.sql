-- Stopping TTL work must not cancel an ordinary merge through its thread group.
DROP TABLE IF EXISTS cancellation_non_ttl;
CREATE TABLE cancellation_non_ttl (k UInt64, d DateTime)
ENGINE = MergeTree ORDER BY k TTL d + INTERVAL 1 DAY;

SYSTEM STOP MERGES cancellation_non_ttl;
SYSTEM STOP TTL MERGES cancellation_non_ttl;
INSERT INTO cancellation_non_ttl VALUES (1, now());
INSERT INTO cancellation_non_ttl VALUES (2, now());

SYSTEM START MERGES cancellation_non_ttl;
OPTIMIZE TABLE cancellation_non_ttl FINAL;

SELECT count(), sum(k) FROM cancellation_non_ttl;
SELECT count() FROM system.parts
WHERE database = currentDatabase() AND table = 'cancellation_non_ttl' AND active;
DROP TABLE cancellation_non_ttl;
