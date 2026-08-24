DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (`s` String, `x` Array(UInt8), `k` UInt64) ENGINE = Join(ANY, LEFT, k);
CREATE TABLE t2 (`s` String, `x` Array(UInt8), `k` UInt64) ENGINE = Join(ANY, INNER, k);

SELECT joinGet('t1', '', number) FROM numbers(2); -- { serverError NO_SUCH_COLUMN_IN_TABLE }
SELECT joinGet('t2', 's', number) FROM numbers(2); -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }

DROP TABLE t1;
DROP TABLE t2;
