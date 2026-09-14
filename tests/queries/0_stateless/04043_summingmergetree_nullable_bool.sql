-- Verify that `Nullable(Bool)` columns are not summed by `SummingMergeTree`.
-- `DataTypeNullable::isSummable` delegates to the nested type; after PR #98976
-- `Bool.isSummable()` returns false, so `Nullable(Bool)` must also be skipped
-- by the summing algorithm and kept as a non-summed representative value.
-- Use `toUInt8` to distinguish the fixed result (1) from the broken result (2)
-- that would occur if the column were summed (1 + NULL + 1 = 2).

DROP TABLE IF EXISTS t_04043_nb;

CREATE TABLE t_04043_nb
(
    id    UInt32,
    count UInt64,
    flag  Nullable(Bool)
)
ENGINE = SummingMergeTree
ORDER BY id;

INSERT INTO t_04043_nb VALUES (1, 10, true), (1, 20, NULL), (1, 30, true);

OPTIMIZE TABLE t_04043_nb FINAL;

-- count must be summed (10+20+30=60); flag must NOT be summed (toUInt8 = 1)
SELECT id, count, toUInt8(flag) FROM t_04043_nb ORDER BY id;

DROP TABLE t_04043_nb;
