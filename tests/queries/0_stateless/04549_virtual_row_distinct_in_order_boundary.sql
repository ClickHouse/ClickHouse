-- Tags: long
-- ^ Reliably reproducing the boundary violation needs several unmerged parts with many small
-- granules, so a single run can exceed the 180s flaky-check limit under sanitizer builds.

-- Regression test for the "Virtual row boundary violated in MergingSortedAlgorithm" logical error
-- (STID 2651-3359). ORDER BY builds a read-in-order virtual row for the sort-key prefix it needs
-- (here CounterID). distinct-in-order then widens the read to a longer prefix (CounterID, EventDate).
-- The virtual row must take index values for the whole widened prefix; previously the extra column
-- was default-filled and, in reverse order, announced a boundary (0) smaller than the real values,
-- tripping the assertion in debug builds and silently mis-ordering the merge in release builds.

DROP TABLE IF EXISTS t_virtual_row_distinct;

CREATE TABLE t_virtual_row_distinct (CounterID UInt32, EventDate UInt64, s String)
ENGINE = MergeTree ORDER BY (CounterID, EventDate)
SETTINGS index_granularity = 8;

-- Keep the parts unmerged so the cross-part in-order merge (which consumes the virtual row) is
-- exercised deterministically instead of being collapsed into a single part by a background merge.
SYSTEM STOP MERGES t_virtual_row_distinct;

-- Several unmerged parts with small granules so an in-order merge across parts is used.
INSERT INTO t_virtual_row_distinct SELECT number % 5, 16000 + (number % 1000), toString(number) FROM numbers(2000);
INSERT INTO t_virtual_row_distinct SELECT number % 5, 16000 + (number % 1000), toString(number) FROM numbers(2000, 2000);
INSERT INTO t_virtual_row_distinct SELECT number % 5, 16000 + (number % 1000), toString(number) FROM numbers(4000, 2000);
INSERT INTO t_virtual_row_distinct SELECT number % 5, 16000 + (number % 1000), toString(number) FROM numbers(6000, 2000);
INSERT INTO t_virtual_row_distinct SELECT number % 5, 16000 + (number % 1000), toString(number) FROM numbers(8000, 2000);
INSERT INTO t_virtual_row_distinct SELECT number % 5, 16000 + (number % 1000), toString(number) FROM numbers(10000, 2000);

SET optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, optimize_distinct_in_order = 1,
    read_in_order_two_level_merge_threshold = 3, max_threads = 2, max_block_size = 64;

-- Must not throw and must return the correct distinct set.
-- Reverse order is the case that previously tripped the boundary check.
SELECT count() FROM (SELECT DISTINCT CounterID, EventDate FROM t_virtual_row_distinct ORDER BY CounterID DESC);
SELECT count() FROM (SELECT DISTINCT CounterID, EventDate FROM t_virtual_row_distinct ORDER BY 1 ASC, CounterID DESC);

-- Result must match the unoptimized read.
SELECT
    (SELECT groupArray((CounterID, EventDate)) FROM (SELECT DISTINCT CounterID, EventDate FROM t_virtual_row_distinct ORDER BY CounterID DESC, EventDate))
  = (SELECT groupArray((CounterID, EventDate)) FROM (SELECT DISTINCT CounterID, EventDate FROM t_virtual_row_distinct ORDER BY CounterID DESC, EventDate SETTINGS optimize_read_in_order = 0, read_in_order_use_virtual_row = 0));

-- The virtual row optimization must stay enabled for the widened read.
SELECT count()
FROM (EXPLAIN actions = 1 SELECT DISTINCT CounterID, EventDate FROM t_virtual_row_distinct ORDER BY CounterID DESC)
WHERE explain ILIKE '%Virtual row conversions%';

-- A constant first ORDER BY key (the stress-test query fuzzer produces such shapes from
-- `ORDER BY 1 ASC, ...`) adds a constant output to the virtual row conversion, so a check based
-- on the conversion's output width miscounts the covered key prefix: the conversion announces
-- (const, CounterID) but nothing for EventDate, which the widened distinct-in-order read
-- default-filled, tripping the boundary check in reverse order (STID 2651-3359 in stress tests).
-- Must not throw and must return the correct distinct set.
SELECT
    (SELECT arraySort(groupArray((CounterID, EventDate))) FROM (SELECT DISTINCT CounterID, EventDate FROM t_virtual_row_distinct ORDER BY toIntervalMinute(1) > toIntervalWeek(59) ASC, CounterID DESC))
  = (SELECT arraySort(groupArray((CounterID, EventDate))) FROM (SELECT DISTINCT CounterID, EventDate FROM t_virtual_row_distinct ORDER BY toIntervalMinute(1) > toIntervalWeek(59) ASC, CounterID DESC SETTINGS optimize_read_in_order = 0, read_in_order_use_virtual_row = 0));

-- Per-block virtual rows (a truncated virtual row can then appear after every data chunk, not
-- only in `initialize`): the widened distinct-in-order read must stay correct and must not throw.
SELECT
    (SELECT groupArray((CounterID, EventDate)) FROM (SELECT DISTINCT CounterID, EventDate FROM t_virtual_row_distinct ORDER BY CounterID DESC, EventDate SETTINGS read_in_order_use_virtual_row_per_block = 1))
  = (SELECT groupArray((CounterID, EventDate)) FROM (SELECT DISTINCT CounterID, EventDate FROM t_virtual_row_distinct ORDER BY CounterID DESC, EventDate SETTINGS optimize_read_in_order = 0, read_in_order_use_virtual_row = 0));

-- LIMIT BY after ORDER BY: `optimizeLimitByInOrder` cannot reach this read (`findReadingStep`
-- does not look through `SortingStep`), so the read keeps the ORDER BY prefix and its virtual
-- row; the streaming `LIMIT BY` result must stay correct (a mis-ordered merge would emit
-- duplicate groups).
SELECT count()
FROM (EXPLAIN actions = 1 SELECT CounterID, EventDate FROM t_virtual_row_distinct ORDER BY CounterID DESC LIMIT 1 BY CounterID, EventDate)
WHERE explain ILIKE '%Virtual row conversions%';

SELECT
    (SELECT arraySort(groupArray((CounterID, EventDate))) FROM (SELECT CounterID, EventDate FROM t_virtual_row_distinct ORDER BY CounterID DESC LIMIT 1 BY CounterID, EventDate))
  = (SELECT arraySort(groupArray((CounterID, EventDate))) FROM (SELECT CounterID, EventDate FROM t_virtual_row_distinct ORDER BY CounterID DESC LIMIT 1 BY CounterID, EventDate SETTINGS optimize_read_in_order = 0, read_in_order_use_virtual_row = 0));

-- LIMIT BY driving the in-order read itself (`optimizeLimitByInOrder` widens the read to the
-- whole BY-columns key prefix through the same `requestReadingInOrder` hook as
-- distinct-in-order): must not throw and the group set must stay correct.
SELECT
    (SELECT arraySort(groupArray((CounterID, EventDate))) FROM (SELECT * FROM (SELECT CounterID, EventDate FROM t_virtual_row_distinct LIMIT 1 BY CounterID, EventDate) ORDER BY CounterID DESC))
  = (SELECT arraySort(groupArray((CounterID, EventDate))) FROM (SELECT * FROM (SELECT CounterID, EventDate FROM t_virtual_row_distinct LIMIT 1 BY CounterID, EventDate) ORDER BY CounterID DESC SETTINGS optimize_read_in_order = 0, read_in_order_use_virtual_row = 0));

DROP TABLE t_virtual_row_distinct;

-- Same widening on key (a, b): ORDER BY a builds a one-column virtual row for a, then
-- distinct-in-order widens the read prefix to (a, b). The in-order merges must announce the index
-- value of b instead of a default-filled 0 (which is the maximum in reverse order and previously
-- tripped the boundary check across parts).
DROP TABLE IF EXISTS t_virtual_row_widen;

CREATE TABLE t_virtual_row_widen (a UInt32, b UInt32)
ENGINE = MergeTree ORDER BY (a, b)
SETTINGS index_granularity = 8;

SYSTEM STOP MERGES t_virtual_row_widen;

INSERT INTO t_virtual_row_widen SELECT number % 10, 1 FROM numbers(2000);
INSERT INTO t_virtual_row_widen SELECT number % 10, 1 FROM numbers(2000, 2000);
INSERT INTO t_virtual_row_widen SELECT number % 10, 1 FROM numbers(4000, 2000);
INSERT INTO t_virtual_row_widen SELECT number % 10, 1 FROM numbers(6000, 2000);

-- Must not throw and must match the unoptimized read.
SELECT
    (SELECT groupArray((a, b)) FROM (SELECT DISTINCT a, b FROM t_virtual_row_widen WHERE b = 1 ORDER BY a DESC SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, optimize_distinct_in_order = 1, read_in_order_two_level_merge_threshold = 3, max_threads = 2, max_block_size = 64))
  = (SELECT groupArray((a, b)) FROM (SELECT DISTINCT a, b FROM t_virtual_row_widen WHERE b = 1 ORDER BY a DESC SETTINGS optimize_read_in_order = 0, read_in_order_use_virtual_row = 0));

DROP TABLE t_virtual_row_widen;

-- A fixed MIDDLE key (WHERE b = 1 ORDER BY a, c on key (a, b, c)) is skipped, and the index entry
-- at a mark boundary stops bounding the values of the later sort columns in the filtered stream.
-- Previously the columns after the skipped key were mapped onto wrong key columns: a wrong value
-- tripped "Virtual row boundary violated" (STID 2651-3359) and a wrong type (e.g. Nullable key)
-- tripped "Virtual row has different type" (STID 1637-309b). Now the virtual row covers only the
-- sort-description prefix before the skipped key and the merge compares it up to that prefix, so
-- the optimization stays enabled and the reads must not throw and stay correct.
DROP TABLE IF EXISTS t_virtual_row_fixed_key;

CREATE TABLE t_virtual_row_fixed_key (a UInt32, b UInt32, c UInt32)
ENGINE = MergeTree ORDER BY (a, b, c)
SETTINGS index_granularity = 8;

-- Keep the parts unmerged (here and in the sections below) so the multi-source in-order merge
-- that consumes the virtual row is exercised deterministically: a background merge collapsing
-- the table into one part would leave a single source and never compare the announced boundary.
SYSTEM STOP MERGES t_virtual_row_fixed_key;

INSERT INTO t_virtual_row_fixed_key SELECT number % 10, 1, number % 7 FROM numbers(2000);
INSERT INTO t_virtual_row_fixed_key SELECT number % 10, 1, number % 7 FROM numbers(2000, 2000);
INSERT INTO t_virtual_row_fixed_key SELECT number % 10, 1, number % 7 FROM numbers(4000, 2000);
INSERT INTO t_virtual_row_fixed_key SELECT number % 10, 1, number % 7 FROM numbers(6000, 2000);

-- Virtual row optimization must stay enabled for the skipped-middle-key read.
SELECT count()
FROM (EXPLAIN actions = 1 SELECT a, c FROM t_virtual_row_fixed_key WHERE b = 1 ORDER BY a, c
      SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 1)
WHERE explain ILIKE '%Virtual row conversions%';

-- Must not throw and must match the unoptimized read (previously STID 2651-3359).
SELECT
    (SELECT groupArray((a, c)) FROM (SELECT a, c FROM t_virtual_row_fixed_key WHERE b = 1 ORDER BY a, c SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 1))
  = (SELECT groupArray((a, c)) FROM (SELECT a, c FROM t_virtual_row_fixed_key WHERE b = 1 ORDER BY a, c SETTINGS optimize_read_in_order = 0, read_in_order_use_virtual_row = 0));

-- Reverse order relies on the same covered-prefix comparison.
SELECT
    (SELECT groupArray((a, c)) FROM (SELECT a, c FROM t_virtual_row_fixed_key WHERE b = 1 ORDER BY a DESC, c DESC SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 1))
  = (SELECT groupArray((a, c)) FROM (SELECT a, c FROM t_virtual_row_fixed_key WHERE b = 1 ORDER BY a DESC, c DESC SETTINGS optimize_read_in_order = 0, read_in_order_use_virtual_row = 0));

-- Per-block mode emits the truncated virtual row again after every data chunk, exercising the
-- covered-prefix comparison in `consume` and not only in `initialize`.
SELECT
    (SELECT groupArray((a, c)) FROM (SELECT a, c FROM t_virtual_row_fixed_key WHERE b = 1 ORDER BY a, c SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, read_in_order_use_virtual_row_per_block = 1))
  = (SELECT groupArray((a, c)) FROM (SELECT a, c FROM t_virtual_row_fixed_key WHERE b = 1 ORDER BY a, c SETTINGS optimize_read_in_order = 0, read_in_order_use_virtual_row = 0));

-- A fixed key that stays in ORDER BY is not supported for the virtual row (as before this fix):
-- the index value at the boundary may belong to a filtered-out row.
SELECT count()
FROM (EXPLAIN actions = 1 SELECT a, b, c FROM t_virtual_row_fixed_key WHERE b = 1 ORDER BY a, b, c
      SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 1)
WHERE explain ILIKE '%Virtual row conversions%';

SELECT
    (SELECT groupArray((a, b, c)) FROM (SELECT a, b, c FROM t_virtual_row_fixed_key WHERE b = 1 ORDER BY a, b, c SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 1))
  = (SELECT groupArray((a, b, c)) FROM (SELECT a, b, c FROM t_virtual_row_fixed_key WHERE b = 1 ORDER BY a, b, c SETTINGS optimize_read_in_order = 0, read_in_order_use_virtual_row = 0));

DROP TABLE t_virtual_row_fixed_key;

-- Same skipped-middle-key path but with a Nullable key column, which previously threw the type
-- mismatch (STID 1637-309b) instead of the boundary violation.
DROP TABLE IF EXISTS t_virtual_row_fixed_key_nullable;

CREATE TABLE t_virtual_row_fixed_key_nullable (a UInt32, b UInt32, c Nullable(UInt32))
ENGINE = MergeTree ORDER BY (a, b, c)
SETTINGS index_granularity = 8, allow_nullable_key = 1;

SYSTEM STOP MERGES t_virtual_row_fixed_key_nullable;

INSERT INTO t_virtual_row_fixed_key_nullable SELECT number % 10, 1, number % 7 FROM numbers(2000);
INSERT INTO t_virtual_row_fixed_key_nullable SELECT number % 10, 1, number % 7 FROM numbers(2000, 2000);
INSERT INTO t_virtual_row_fixed_key_nullable SELECT number % 10, 1, number % 7 FROM numbers(4000, 2000);
INSERT INTO t_virtual_row_fixed_key_nullable SELECT number % 10, 1, number % 7 FROM numbers(6000, 2000);

SELECT
    (SELECT groupArray((a, c)) FROM (SELECT a, c FROM t_virtual_row_fixed_key_nullable WHERE b = 1 ORDER BY a ASC NULLS FIRST, c ASC SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 1))
  = (SELECT groupArray((a, c)) FROM (SELECT a, c FROM t_virtual_row_fixed_key_nullable WHERE b = 1 ORDER BY a ASC NULLS FIRST, c ASC SETTINGS optimize_read_in_order = 0, read_in_order_use_virtual_row = 0));

DROP TABLE t_virtual_row_fixed_key_nullable;

-- A String column after the skipped key: the covered-prefix comparison does not depend on the
-- type having an extreme value, so the virtual row stays enabled in both directions and the
-- reads must stay correct.
DROP TABLE IF EXISTS t_virtual_row_fixed_key_string;

CREATE TABLE t_virtual_row_fixed_key_string (a UInt32, b UInt32, s String)
ENGINE = MergeTree ORDER BY (a, b, s)
SETTINGS index_granularity = 8;

SYSTEM STOP MERGES t_virtual_row_fixed_key_string;

INSERT INTO t_virtual_row_fixed_key_string SELECT number % 10, 1, toString(number % 7) FROM numbers(2000);
INSERT INTO t_virtual_row_fixed_key_string SELECT number % 10, 1, toString(number % 7) FROM numbers(2000, 2000);
INSERT INTO t_virtual_row_fixed_key_string SELECT number % 10, 1, toString(number % 7) FROM numbers(4000, 2000);
INSERT INTO t_virtual_row_fixed_key_string SELECT number % 10, 1, toString(number % 7) FROM numbers(6000, 2000);

SELECT count()
FROM (EXPLAIN actions = 1 SELECT a, s FROM t_virtual_row_fixed_key_string WHERE b = 1 ORDER BY a, s
      SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 1)
WHERE explain ILIKE '%Virtual row conversions%';

SELECT
    (SELECT groupArray((a, s)) FROM (SELECT a, s FROM t_virtual_row_fixed_key_string WHERE b = 1 ORDER BY a, s SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 1))
  = (SELECT groupArray((a, s)) FROM (SELECT a, s FROM t_virtual_row_fixed_key_string WHERE b = 1 ORDER BY a, s SETTINGS optimize_read_in_order = 0, read_in_order_use_virtual_row = 0));

SELECT count()
FROM (EXPLAIN actions = 1 SELECT a, s FROM t_virtual_row_fixed_key_string WHERE b = 1 ORDER BY a DESC, s DESC
      SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 1)
WHERE explain ILIKE '%Virtual row conversions%';

SELECT
    (SELECT groupArray((a, s)) FROM (SELECT a, s FROM t_virtual_row_fixed_key_string WHERE b = 1 ORDER BY a DESC, s DESC SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 1))
  = (SELECT groupArray((a, s)) FROM (SELECT a, s FROM t_virtual_row_fixed_key_string WHERE b = 1 ORDER BY a DESC, s DESC SETTINGS optimize_read_in_order = 0, read_in_order_use_virtual_row = 0));

DROP TABLE t_virtual_row_fixed_key_string;

-- A near-unique first key column makes every part keep only that column of the primary index in
-- memory (optimizeIndexColumns, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns).
-- The widened distinct-in-order read then requests index values for (a, b) while the loaded
-- index covers only (a): the virtual row must degrade to the loaded prefix instead of silently
-- disappearing, and the merges compare it on that prefix only.
DROP TABLE IF EXISTS t_virtual_row_truncated_index;

CREATE TABLE t_virtual_row_truncated_index (a UInt32, b UInt32)
ENGINE = MergeTree ORDER BY (a, b)
SETTINGS index_granularity = 8;

SYSTEM STOP MERGES t_virtual_row_truncated_index;

INSERT INTO t_virtual_row_truncated_index SELECT number, number % 3 FROM numbers(4000);
INSERT INTO t_virtual_row_truncated_index SELECT number, number % 3 FROM numbers(4000, 4000);
INSERT INTO t_virtual_row_truncated_index SELECT number, number % 3 FROM numbers(8000, 4000);
INSERT INTO t_virtual_row_truncated_index SELECT number, number % 3 FROM numbers(12000, 4000);

-- The virtual row must actually reach the pipeline (previously it was silently dropped for
-- every part whose loaded index is shorter than the widened prefix).
SELECT count() > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT a, b FROM t_virtual_row_truncated_index ORDER BY a DESC SETTINGS enable_parallel_replicas = 0)
WHERE explain ILIKE '%VirtualRow%';

SELECT
    (SELECT groupArray((a, b)) FROM (SELECT DISTINCT a, b FROM t_virtual_row_truncated_index ORDER BY a DESC))
  = (SELECT groupArray((a, b)) FROM (SELECT DISTINCT a, b FROM t_virtual_row_truncated_index ORDER BY a DESC SETTINGS optimize_read_in_order = 0, read_in_order_use_virtual_row = 0));

-- Per-block mode takes the same clamp in `MergeTreeSelectProcessor::buildVirtualRowFromIndex`.
SELECT
    (SELECT groupArray((a, b)) FROM (SELECT DISTINCT a, b FROM t_virtual_row_truncated_index ORDER BY a DESC SETTINGS read_in_order_use_virtual_row_per_block = 1))
  = (SELECT groupArray((a, b)) FROM (SELECT DISTINCT a, b FROM t_virtual_row_truncated_index ORDER BY a DESC SETTINGS optimize_read_in_order = 0, read_in_order_use_virtual_row = 0));

DROP TABLE t_virtual_row_truncated_index;

-- Widening past the primary key: `ORDER BY a` installs a virtual row for the prefix (a), which is
-- the whole primary key, and distinct-in-order then wants to widen the read to (a, b) — a prefix
-- no part can announce index values for. The virtual row cannot be revoked at that point (the
-- parent `SortingStep` is already converted with `apply_virtual_row_conversions`, and the
-- read-in-order-through-JOIN path may have been accepted only because virtual rows are emitted),
-- so the widening is rejected and the read keeps the ORDER BY prefix with its virtual row.
DROP TABLE IF EXISTS t_virtual_row_widen_past_pk;

CREATE TABLE t_virtual_row_widen_past_pk (a UInt32, b UInt32)
ENGINE = MergeTree PRIMARY KEY (a) ORDER BY (a, b)
SETTINGS index_granularity = 8;

SYSTEM STOP MERGES t_virtual_row_widen_past_pk;

INSERT INTO t_virtual_row_widen_past_pk SELECT number % 10, number % 7 FROM numbers(2000);
INSERT INTO t_virtual_row_widen_past_pk SELECT number % 10, number % 7 FROM numbers(2000, 2000);
INSERT INTO t_virtual_row_widen_past_pk SELECT number % 10, number % 7 FROM numbers(4000, 2000);
INSERT INTO t_virtual_row_widen_past_pk SELECT number % 10, number % 7 FROM numbers(6000, 2000);

-- The virtual row stays in the plan (the rejected widening must not leave the reader without the
-- conversion the already converted sort relies on).
SELECT count()
FROM (EXPLAIN actions = 1 SELECT DISTINCT a, b FROM t_virtual_row_widen_past_pk ORDER BY a DESC)
WHERE explain ILIKE '%Virtual row conversions%';

SELECT
    (SELECT arraySort(groupArray((a, b))) FROM (SELECT DISTINCT a, b FROM t_virtual_row_widen_past_pk ORDER BY a DESC))
  = (SELECT arraySort(groupArray((a, b))) FROM (SELECT DISTINCT a, b FROM t_virtual_row_widen_past_pk ORDER BY a DESC SETTINGS optimize_read_in_order = 0, read_in_order_use_virtual_row = 0));

-- Same for `LIMIT BY`, which widens the read through the same hook.
SELECT
    (SELECT arraySort(groupArray((a, b))) FROM (SELECT * FROM (SELECT a, b FROM t_virtual_row_widen_past_pk ORDER BY a DESC LIMIT 1 BY a, b)))
  = (SELECT arraySort(groupArray((a, b))) FROM (SELECT * FROM (SELECT a, b FROM t_virtual_row_widen_past_pk ORDER BY a DESC LIMIT 1 BY a, b SETTINGS optimize_read_in_order = 0, read_in_order_use_virtual_row = 0)));

DROP TABLE t_virtual_row_widen_past_pk;
