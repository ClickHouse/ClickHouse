-- The number of stride groups (dimension / stride) of a QBit is bounded by MAX_STRIDE_GROUPS (1024) so that a huge dimension
-- with a tiny stride cannot make the nested type materialize an unreasonable number of streams. This bound is enforced
-- end-to-end when the type is built (e.g. from a CREATE TABLE), not only in the type constructor.

DROP TABLE IF EXISTS qbit_max_groups;

-- Exactly MAX_STRIDE_GROUPS (1024) groups is allowed: 8192 dimensions with stride 8 -> 8192 / 8 = 1024 groups.
CREATE TABLE qbit_max_groups (id UInt32, vec QBit(Int8, 8192, 8)) ENGINE = Memory;
DROP TABLE qbit_max_groups;

-- One group over the limit is rejected: 8200 dimensions with stride 8 -> 8200 / 8 = 1025 groups.
CREATE TABLE qbit_max_groups (id UInt32, vec QBit(Int8, 8200, 8)) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }

-- A non-strided QBit is unaffected by the bound regardless of how large the dimension is (it always has element_size streams).
CREATE TABLE qbit_max_groups (id UInt32, vec QBit(Int8, 8200, 8200)) ENGINE = Memory;
DROP TABLE qbit_max_groups;
