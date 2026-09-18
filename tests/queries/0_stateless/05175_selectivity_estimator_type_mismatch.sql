-- The selectivity estimator tries to convert a string constant to the column type to estimate
-- selectivity. For a DateTime64 column and an ISO string with a trailing 'Z', the strict conversion
-- fails and `convertFieldToType` reports it as TYPE_MISMATCH. The estimator must tolerate that and
-- fall back to a default estimate instead of failing the whole query. The estimator is only built
-- when there are multiple conditions, hence the AND.

DROP TABLE IF EXISTS t_selectivity_type_mismatch;

CREATE TABLE t_selectivity_type_mismatch (id UInt32, t DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_selectivity_type_mismatch SELECT 1, toDateTime64('2026-07-16 12:30:45.123', 3, 'UTC');

SELECT count() FROM t_selectivity_type_mismatch WHERE t = '2026-07-16T12:30:45.123Z';
SELECT count() FROM t_selectivity_type_mismatch WHERE t = '2026-07-16T12:30:45.123Z' AND id = 1;

DROP TABLE t_selectivity_type_mismatch;
