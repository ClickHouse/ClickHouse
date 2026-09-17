-- An integer constant used as an exact bound for a `Time64` column is a number of seconds. Like for `DateTime64`,
-- `convertFieldToType` must treat a bound that overflows when scaled up to the column precision, or a `UInt64`
-- bound above the maximum of `Int64`, as "cannot convert" instead of throwing `DECIMAL_OVERFLOW` or wrapping
-- it around into a negative tick count that matches an unrelated row.

DROP TABLE IF EXISTS t_t64_nanoseconds;
CREATE TABLE t_t64_nanoseconds (t64 Time64(9)) ENGINE = MergeTree ORDER BY t64;
INSERT INTO t_t64_nanoseconds VALUES ('12:34:56');

SELECT count() FROM t_t64_nanoseconds WHERE t64 IN (9223372036854);
SELECT count() FROM t_t64_nanoseconds WHERE t64 IN (45296);

DROP TABLE t_t64_nanoseconds;

DROP TABLE IF EXISTS t_t64_wraparound;
CREATE TABLE t_t64_wraparound (t64 Time64(0)) ENGINE = MergeTree ORDER BY t64;
INSERT INTO t_t64_wraparound VALUES (-9223372036854775808);

SELECT count() FROM t_t64_wraparound WHERE t64 IN (9223372036854775808);
SELECT count() FROM t_t64_wraparound WHERE t64 IN (-9223372036854775808);

DROP TABLE t_t64_wraparound;
