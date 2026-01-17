-- Tags: no-fasttest, long
-- no-fasttest: 'countmin' sketches need a 3rd party library

-- Tests that DDL statements which create / drop / materialize statistics

SET mutations_sync = 1;

DROP TABLE IF EXISTS tab;

SET allow_experimental_statistics = 0;
-- Error case: Can't create statistics when allow_experimental_statistics = 0
CREATE TABLE tab (col Float64 STATISTICS(tdigest)) Engine = MergeTree() ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

SET allow_experimental_statistics = 1;

-- Error case: Unknown statistics types are rejected
CREATE TABLE tab (col Float64 STATISTICS(no_statistics_type)) Engine = MergeTree() ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

-- Error case: The same statistics type can't exist more than once on a column
CREATE TABLE tab (col Float64 STATISTICS(tdigest, tdigest)) Engine = MergeTree() ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

SET allow_suspicious_low_cardinality_types = 1;

-- Statistics can only be created on columns of specific data types (depending on the statistics kind), (*)

--   tdigest requires data_type.isValueRepresentedByInteger
--     These types work:
CREATE TABLE tab (col UInt8 STATISTICS(tdigest)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col UInt256 STATISTICS(tdigest)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col Float32 STATISTICS(tdigest)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col Decimal32(3) STATISTICS(tdigest)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col Date STATISTICS(tdigest)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col Date32 STATISTICS(tdigest)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col DateTime STATISTICS(tdigest)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col DateTime64 STATISTICS(tdigest)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col Enum('hello', 'world') STATISTICS(tdigest)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col Nullable(UInt8) STATISTICS(tdigest)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col LowCardinality(UInt8) STATISTICS(tdigest)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col LowCardinality(Nullable(UInt8)) STATISTICS(tdigest)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
--     These types don't work:
CREATE TABLE tab (col String STATISTICS(tdigest)) Engine = MergeTree() ORDER BY tuple(); -- { serverError ILLEGAL_STATISTICS }
CREATE TABLE tab (col FixedString(1) STATISTICS(tdigest)) Engine = MergeTree() ORDER BY tuple(); -- { serverError ILLEGAL_STATISTICS }
CREATE TABLE tab (col Array(Float64) STATISTICS(tdigest)) Engine = MergeTree() ORDER BY tuple(); -- { serverError ILLEGAL_STATISTICS }
CREATE TABLE tab (col Tuple(Float64, Float64) STATISTICS(tdigest)) Engine = MergeTree() ORDER BY tuple(); -- { serverError ILLEGAL_STATISTICS }
CREATE TABLE tab (col Map(UInt64, UInt64) STATISTICS(tdigest)) Engine = MergeTree() ORDER BY tuple(); -- { serverError ILLEGAL_STATISTICS }
CREATE TABLE tab (col UUID STATISTICS(tdigest)) Engine = MergeTree() ORDER BY tuple(); -- { serverError ILLEGAL_STATISTICS }
CREATE TABLE tab (col IPv4 STATISTICS(tdigest)) Engine = MergeTree() ORDER BY tuple(); -- { serverError ILLEGAL_STATISTICS }
CREATE TABLE tab (col IPv6 STATISTICS(tdigest)) Engine = MergeTree() ORDER BY tuple(); -- { serverError ILLEGAL_STATISTICS }

--   uniq requires data_type.isValueRepresentedByInteger or (Fixed)String
--     These types work:
CREATE TABLE tab (col UInt8 STATISTICS(uniq)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col UInt256 STATISTICS(uniq)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col Float32 STATISTICS(uniq)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col Decimal32(3) STATISTICS(uniq)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col Date STATISTICS(uniq)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col Date32 STATISTICS(uniq)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col DateTime STATISTICS(uniq)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col DateTime64 STATISTICS(uniq)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col Enum('hello', 'world') STATISTICS(uniq)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col IPv4 STATISTICS(uniq)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col Nullable(UInt8) STATISTICS(uniq)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col LowCardinality(UInt8) STATISTICS(uniq)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col LowCardinality(Nullable(UInt8)) STATISTICS(uniq)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col String STATISTICS(uniq)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col FixedString(1) STATISTICS(uniq)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
--     These types don't work:
CREATE TABLE tab (col Array(Float64) STATISTICS(uniq)) Engine = MergeTree() ORDER BY tuple(); -- { serverError ILLEGAL_STATISTICS }
CREATE TABLE tab (col Tuple(Float64, Float64) STATISTICS(uniq)) Engine = MergeTree() ORDER BY tuple(); -- { serverError ILLEGAL_STATISTICS }
CREATE TABLE tab (col Map(UInt64, UInt64) STATISTICS(uniq)) Engine = MergeTree() ORDER BY tuple(); -- { serverError ILLEGAL_STATISTICS }
CREATE TABLE tab (col UUID STATISTICS(uniq)) Engine = MergeTree() ORDER BY tuple(); -- { serverError ILLEGAL_STATISTICS }
CREATE TABLE tab (col IPv6 STATISTICS(uniq)) Engine = MergeTree() ORDER BY tuple(); -- { serverError ILLEGAL_STATISTICS }

--   countmin requires data_type.isValueRepresentedByInteger or data_type = (Fixed)String
--     These types work:
CREATE TABLE tab (col UInt8 STATISTICS(countmin)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col UInt256 STATISTICS(countmin)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col Float32 STATISTICS(countmin)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col Decimal32(3) STATISTICS(countmin)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col Date STATISTICS(countmin)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col Date32 STATISTICS(countmin)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col DateTime STATISTICS(countmin)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col DateTime64 STATISTICS(countmin)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col Enum('hello', 'world') STATISTICS(countmin)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col IPv4 STATISTICS(countmin)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col Nullable(UInt8) STATISTICS(countmin)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col LowCardinality(UInt8) STATISTICS(countmin)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col LowCardinality(Nullable(UInt8)) STATISTICS(countmin)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col String STATISTICS(countmin)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col FixedString(1) STATISTICS(countmin)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
--     These types don't work:
CREATE TABLE tab (col Array(Float64) STATISTICS(countmin)) Engine = MergeTree() ORDER BY tuple(); -- { serverError ILLEGAL_STATISTICS }
CREATE TABLE tab (col Tuple(Float64, Float64) STATISTICS(countmin)) Engine = MergeTree() ORDER BY tuple(); -- { serverError ILLEGAL_STATISTICS }
CREATE TABLE tab (col Map(UInt64, UInt64) STATISTICS(countmin)) Engine = MergeTree() ORDER BY tuple(); -- { serverError ILLEGAL_STATISTICS }
CREATE TABLE tab (col UUID STATISTICS(countmin)) Engine = MergeTree() ORDER BY tuple(); -- { serverError ILLEGAL_STATISTICS }
CREATE TABLE tab (col IPv6 STATISTICS(countmin)) Engine = MergeTree() ORDER BY tuple(); -- { serverError ILLEGAL_STATISTICS }

--   minmax requires data_type.isValueRepresentedByInteger
--     These types work:
CREATE TABLE tab (col UInt8 STATISTICS(minmax)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col UInt256 STATISTICS(minmax)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col Float32 STATISTICS(minmax)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col Decimal32(3) STATISTICS(minmax)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col Date STATISTICS(minmax)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col Date32 STATISTICS(minmax)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col DateTime STATISTICS(minmax)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col DateTime64 STATISTICS(minmax)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col Enum('hello', 'world') STATISTICS(minmax)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col IPv4 STATISTICS(minmax)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col Nullable(UInt8) STATISTICS(minmax)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col LowCardinality(UInt8) STATISTICS(minmax)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
CREATE TABLE tab (col LowCardinality(Nullable(UInt8)) STATISTICS(minmax)) Engine = MergeTree() ORDER BY tuple(); DROP TABLE tab;
--     These types don't work:
CREATE TABLE tab (col String STATISTICS(minmax)) Engine = MergeTree() ORDER BY tuple(); -- { serverError ILLEGAL_STATISTICS }
CREATE TABLE tab (col FixedString(1) STATISTICS(minmax)) Engine = MergeTree() ORDER BY tuple(); -- { serverError ILLEGAL_STATISTICS }
CREATE TABLE tab (col Array(Float64) STATISTICS(minmax)) Engine = MergeTree() ORDER BY tuple(); -- { serverError ILLEGAL_STATISTICS }
CREATE TABLE tab (col Tuple(Float64, Float64) STATISTICS(minmax)) Engine = MergeTree() ORDER BY tuple(); -- { serverError ILLEGAL_STATISTICS }
CREATE TABLE tab (col Map(UInt64, UInt64) STATISTICS(minmax)) Engine = MergeTree() ORDER BY tuple(); -- { serverError ILLEGAL_STATISTICS }
CREATE TABLE tab (col UUID STATISTICS(minmax)) Engine = MergeTree() ORDER BY tuple(); -- { serverError ILLEGAL_STATISTICS }
CREATE TABLE tab (col IPv6 STATISTICS(minmax)) Engine = MergeTree() ORDER BY tuple(); -- { serverError ILLEGAL_STATISTICS }

-- CREATE TABLE was easy, ALTER is more fun

CREATE TABLE tab
(
    f64           Float64,
    f64_tdigest   Float64 STATISTICS(tdigest),
    f32           Float32,
    s             String,
    a             Array(Float64)
)
Engine = MergeTree()
ORDER BY tuple();

-- Error case: Unknown statistics types are rejected
-- (relevant for ADD and MODIFY)
ALTER TABLE tab ADD STATISTICS f64 TYPE no_statistics_type; -- { serverError INCORRECT_QUERY }
ALTER TABLE tab ADD STATISTICS IF NOT EXISTS f64 TYPE no_statistics_type; -- { serverError INCORRECT_QUERY }
ALTER TABLE tab MODIFY STATISTICS f64 TYPE no_statistics_type; -- { serverError INCORRECT_QUERY }
-- for some reason, ALTER TABLE tab MODIFY STATISTICS IF EXISTS is not supported

-- Error case: The same statistics type can't exist more than once on a column
-- (relevant for ADD and MODIFY)
--   Create the same statistics object twice
ALTER TABLE tab ADD STATISTICS f64 TYPE tdigest, tdigest; -- { serverError INCORRECT_QUERY }
ALTER TABLE tab ADD STATISTICS IF NOT EXISTS f64 TYPE tdigest, tdigest; -- { serverError INCORRECT_QUERY }
ALTER TABLE tab MODIFY STATISTICS f64 TYPE tdigest, tdigest; -- { serverError INCORRECT_QUERY }
--   Create an statistics which exists already
ALTER TABLE tab ADD STATISTICS f64_tdigest TYPE tdigest; -- { serverError ILLEGAL_STATISTICS }
ALTER TABLE tab ADD STATISTICS IF NOT EXISTS f64_tdigest TYPE tdigest; -- no-op
ALTER TABLE tab MODIFY STATISTICS f64_tdigest TYPE tdigest; -- no-op

-- Error case: Column does not exist
-- (relevant for ADD, MODIFY, DROP, CLEAR, and MATERIALIZE)
-- Note that the results are unfortunately quite inconsistent ...
ALTER TABLE tab ADD STATISTICS no_such_column TYPE tdigest; -- { serverError ILLEGAL_STATISTICS }
ALTER TABLE tab ADD STATISTICS IF NOT EXISTS no_such_column TYPE tdigest; -- { serverError ILLEGAL_STATISTICS }
ALTER TABLE tab MODIFY STATISTICS no_such_column TYPE tdigest; -- { serverError ILLEGAL_STATISTICS }
ALTER TABLE tab DROP STATISTICS no_such_column; -- { serverError ILLEGAL_STATISTICS }
ALTER TABLE tab DROP STATISTICS IF EXISTS no_such_column; -- no-op
ALTER TABLE tab CLEAR STATISTICS no_such_column; -- { serverError ILLEGAL_STATISTICS }
ALTER TABLE tab CLEAR STATISTICS IF EXISTS no_such_column; -- no-op
ALTER TABLE tab MATERIALIZE STATISTICS no_such_column; -- { serverError ILLEGAL_STATISTICS }
ALTER TABLE tab MATERIALIZE STATISTICS IF EXISTS no_such_column; -- { serverError ILLEGAL_STATISTICS }

-- Error case: Column exists but has no statistics
-- (relevant for MODIFY, DROP, CLEAR, and MATERIALIZE)
-- Note that the results are unfortunately quite inconsistent ...
ALTER TABLE tab MODIFY STATISTICS s TYPE tdigest; -- { serverError ILLEGAL_STATISTICS }
ALTER TABLE tab DROP STATISTICS s; -- { serverError ILLEGAL_STATISTICS }
ALTER TABLE tab DROP STATISTICS IF EXISTS s; -- no-op
ALTER TABLE tab CLEAR STATISTICS s; -- { serverError ILLEGAL_STATISTICS }
ALTER TABLE tab CLEAR STATISTICS IF EXISTS s; -- no-op

-- We don't check systematically that that statistics can only be created via ALTER ADD STATISTICS on columns of specific data types (the
-- internal type validation code is tested already above, (*)). Only do a rudimentary check for each statistics type with a data type that
-- works and one that doesn't work.
--   tdigest
--     Works:
ALTER TABLE tab ADD STATISTICS f64 TYPE tdigest; ALTER TABLE tab DROP STATISTICS f64;
ALTER TABLE tab MODIFY STATISTICS f64 TYPE tdigest; ALTER TABLE tab DROP STATISTICS f64;
--     Doesn't work:
ALTER TABLE tab ADD STATISTICS a TYPE tdigest; -- { serverError ILLEGAL_STATISTICS }
ALTER TABLE tab MODIFY STATISTICS a TYPE tdigest; -- { serverError ILLEGAL_STATISTICS }
--   uniq
--     Works:
ALTER TABLE tab ADD STATISTICS f64 TYPE uniq; ALTER TABLE tab DROP STATISTICS f64;
ALTER TABLE tab MODIFY STATISTICS f64 TYPE countmin; ALTER TABLE tab DROP STATISTICS f64;
--     Doesn't work:
ALTER TABLE tab ADD STATISTICS a TYPE uniq; -- { serverError ILLEGAL_STATISTICS }
ALTER TABLE tab MODIFY STATISTICS a TYPE uniq; -- { serverError ILLEGAL_STATISTICS }
--   countmin
--     Works:
ALTER TABLE tab ADD STATISTICS f64 TYPE countmin; ALTER TABLE tab DROP STATISTICS f64;
ALTER TABLE tab MODIFY STATISTICS f64 TYPE countmin; ALTER TABLE tab DROP STATISTICS f64;
--     Doesn't work:
ALTER TABLE tab ADD STATISTICS a TYPE countmin; -- { serverError ILLEGAL_STATISTICS }
ALTER TABLE tab MODIFY STATISTICS a TYPE countmin; -- { serverError ILLEGAL_STATISTICS }
--   minmax
--     Works:
ALTER TABLE tab ADD STATISTICS f64 TYPE minmax; ALTER TABLE tab DROP STATISTICS f64;
ALTER TABLE tab MODIFY STATISTICS f64 TYPE minmax; ALTER TABLE tab DROP STATISTICS f64;
--     Doesn't work:
ALTER TABLE tab ADD STATISTICS a TYPE minmax; -- { serverError ILLEGAL_STATISTICS }
ALTER TABLE tab MODIFY STATISTICS a TYPE minmax; -- { serverError ILLEGAL_STATISTICS }
ALTER TABLE tab MODIFY COLUMN f64_tdigest UInt64;

-- Finally, do a full-circle test of a good case. Print table definition after each step.
-- Intentionally specifying _two_ columns and _two_ statistics types to have that also tested.
SHOW CREATE TABLE tab;
ALTER TABLE tab ADD STATISTICS f64, f32 TYPE tdigest, uniq;
SHOW CREATE TABLE tab;
ALTER TABLE tab MODIFY STATISTICS f64, f32 TYPE tdigest, uniq;
SHOW CREATE TABLE tab;
ALTER TABLE tab CLEAR STATISTICS f64, f32;
SHOW CREATE TABLE tab;
ALTER TABLE tab MATERIALIZE STATISTICS f64, f32;
SHOW CREATE TABLE tab;
ALTER TABLE tab DROP STATISTICS f64, f32;
SHOW CREATE TABLE tab;

DROP TABLE tab;
