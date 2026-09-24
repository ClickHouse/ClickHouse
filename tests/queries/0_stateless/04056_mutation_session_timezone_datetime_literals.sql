-- Verify that ALTER DELETE/UPDATE with DateTime comparisons works correctly
-- when session_timezone differs from server timezone. String literals in the
-- mutation predicate must be interpreted in the session timezone, not the
-- server default. The fix wraps them with toDateTime('...', '<tz>') at ALTER
-- time so the background mutation thread evaluates them consistently.
--
-- Covered predicate shapes: a plain comparison in either operand order, UPDATE SET,
-- a literal list on the right of IN / NOT IN / nullIn (whether written with
-- parentheses or as an array), a table-qualified column name, DateTime64,
-- Nullable and LowCardinality columns, and lightweight DELETE. Columns that
-- declare their own timezone are deliberately left alone.

SET session_timezone = 'America/Denver'; -- UTC-7 (far from typical UTC server default)
SET mutations_sync = 2;

-- DateTime (no explicit timezone)
DROP TABLE IF EXISTS test_mutation_tz SYNC;
CREATE TABLE test_mutation_tz (id UInt32, time DateTime) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_mutation_tz VALUES (1, '2000-01-01 01:02:03'), (2, '2000-01-01 04:05:06');
SELECT 'before delete', id, time FROM test_mutation_tz ORDER BY id;

ALTER TABLE test_mutation_tz DELETE WHERE time >= '2000-01-01 02:00:00';
SELECT 'after delete', id, time FROM test_mutation_tz ORDER BY id;

-- DateTime64 (no explicit timezone)
DROP TABLE IF EXISTS test_mutation_tz64 SYNC;
CREATE TABLE test_mutation_tz64 (id UInt32, time DateTime64(3)) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_mutation_tz64 VALUES (1, '2000-01-01 01:02:03.123'), (2, '2000-01-01 04:05:06.456');
SELECT 'before delete dt64', id, time FROM test_mutation_tz64 ORDER BY id;

ALTER TABLE test_mutation_tz64 DELETE WHERE time >= '2000-01-01 02:00:00';
SELECT 'after delete dt64', id, time FROM test_mutation_tz64 ORDER BY id;

-- ALTER UPDATE with DateTime comparison in WHERE
DROP TABLE IF EXISTS test_mutation_tz_upd SYNC;
CREATE TABLE test_mutation_tz_upd (id UInt32, time DateTime, val String) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_mutation_tz_upd VALUES (1, '2000-01-01 01:02:03', 'old'), (2, '2000-01-01 04:05:06', 'old');
ALTER TABLE test_mutation_tz_upd UPDATE val = 'new' WHERE time >= '2000-01-01 02:00:00';
SELECT 'after update', id, val FROM test_mutation_tz_upd ORDER BY id;

-- Nullable(DateTime) — the wrapper must be unwrapped before the timezone check
DROP TABLE IF EXISTS test_mutation_tz_nullable SYNC;
CREATE TABLE test_mutation_tz_nullable (id UInt32, time Nullable(DateTime)) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_mutation_tz_nullable VALUES (1, '2000-01-01 01:02:03'), (2, '2000-01-01 04:05:06');
SELECT 'before delete nullable', id, time FROM test_mutation_tz_nullable ORDER BY id;

ALTER TABLE test_mutation_tz_nullable DELETE WHERE time >= '2000-01-01 02:00:00';
SELECT 'after delete nullable', id, time FROM test_mutation_tz_nullable ORDER BY id;

-- LowCardinality(DateTime) — same unwrapping logic
SET allow_suspicious_low_cardinality_types = 1;
DROP TABLE IF EXISTS test_mutation_tz_lc SYNC;
CREATE TABLE test_mutation_tz_lc (id UInt32, time LowCardinality(DateTime)) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_mutation_tz_lc VALUES (1, '2000-01-01 01:02:03'), (2, '2000-01-01 04:05:06');
SELECT 'before delete lc', id, time FROM test_mutation_tz_lc ORDER BY id;

ALTER TABLE test_mutation_tz_lc DELETE WHERE time >= '2000-01-01 02:00:00';
SELECT 'after delete lc', id, time FROM test_mutation_tz_lc ORDER BY id;

-- Nullable(DateTime64) — unwrapping for DateTime64
DROP TABLE IF EXISTS test_mutation_tz64_nullable SYNC;
CREATE TABLE test_mutation_tz64_nullable (id UInt32, time Nullable(DateTime64(3))) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_mutation_tz64_nullable VALUES (1, '2000-01-01 01:02:03.123'), (2, '2000-01-01 04:05:06.456');
SELECT 'before delete nullable dt64', id, time FROM test_mutation_tz64_nullable ORDER BY id;

ALTER TABLE test_mutation_tz64_nullable DELETE WHERE time >= '2000-01-01 02:00:00';
SELECT 'after delete nullable dt64', id, time FROM test_mutation_tz64_nullable ORDER BY id;

-- ALTER UPDATE SET DateTime column to a string literal — the literal must be
-- interpreted in session timezone, not server timezone
DROP TABLE IF EXISTS test_mutation_tz_set SYNC;
CREATE TABLE test_mutation_tz_set (id UInt32, time DateTime) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_mutation_tz_set VALUES (1, '2000-01-01 00:00:00');
ALTER TABLE test_mutation_tz_set UPDATE time = '2000-01-01 07:00:00' WHERE id = 1;
-- Verify UPDATE SET used session timezone: insert the same literal and compare unix timestamps
INSERT INTO test_mutation_tz_set VALUES (2, '2000-01-01 07:00:00');
SELECT 'update set dt', id, toUnixTimestamp(time) FROM test_mutation_tz_set ORDER BY id;

-- Same for DateTime64
DROP TABLE IF EXISTS test_mutation_tz64_set SYNC;
CREATE TABLE test_mutation_tz64_set (id UInt32, time DateTime64(3)) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_mutation_tz64_set VALUES (1, '2000-01-01 00:00:00.000');
ALTER TABLE test_mutation_tz64_set UPDATE time = '2000-01-01 07:00:00.123' WHERE id = 1;
INSERT INTO test_mutation_tz64_set VALUES (2, '2000-01-01 07:00:00.123');
SELECT 'update set dt64', id, toUnixTimestamp(time) FROM test_mutation_tz64_set ORDER BY id;

-- DateTime with explicit timezone should NOT be rewritten (timezone is already determined)
DROP TABLE IF EXISTS test_mutation_tz_explicit SYNC;
CREATE TABLE test_mutation_tz_explicit (id UInt32, time DateTime('UTC')) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_mutation_tz_explicit VALUES (1, '2000-01-01 01:02:03'), (2, '2000-01-01 04:05:06');
ALTER TABLE test_mutation_tz_explicit DELETE WHERE time >= '2000-01-01 02:00:00';
SELECT 'explicit tz', id, time FROM test_mutation_tz_explicit ORDER BY id;

DROP TABLE test_mutation_tz SYNC;
DROP TABLE test_mutation_tz64 SYNC;
DROP TABLE test_mutation_tz_upd SYNC;
DROP TABLE test_mutation_tz_nullable SYNC;
DROP TABLE test_mutation_tz_lc SYNC;
DROP TABLE test_mutation_tz64_nullable SYNC;
DROP TABLE test_mutation_tz_set SYNC;
DROP TABLE test_mutation_tz64_set SYNC;
DROP TABLE test_mutation_tz_explicit SYNC;

-- ===========================================================================
-- A literal list on the right of IN, and a qualified column name (issue #121991)
--
-- Fixture, per case: two rows holding the two absolute instants the literal
-- '2000-01-01 01:00:00' can denote -- id 1 reads it in the session timezone,
-- id 9 in UTC. Every assertion below therefore holds whatever timezone the
-- server runs in.
--
-- Oracle, per case: the mutation must affect exactly the rows the identical
-- SELECT matches, so 'selected' and the complement of 'survivors' agree.
-- ===========================================================================

SET session_timezone = 'America/Denver';
SET mutations_sync = 2;

-- IN with a single literal: the parser folds the list to one string literal.
DROP TABLE IF EXISTS mut_tz_in_one SYNC;
CREATE TABLE mut_tz_in_one (id UInt32, time DateTime) ENGINE = MergeTree ORDER BY id;
INSERT INTO mut_tz_in_one VALUES (1, toDateTime('2000-01-01 01:00:00')), (9, toDateTime('2000-01-01 01:00:00', 'UTC'));
SELECT 'in one selected', arraySort(groupArray(id)) FROM mut_tz_in_one WHERE time IN ('2000-01-01 01:00:00');
ALTER TABLE mut_tz_in_one DELETE WHERE time IN ('2000-01-01 01:00:00');
SELECT 'in one survivors', arraySort(groupArray(id)) FROM mut_tz_in_one;

-- IN with several literals: the parser folds the list to one tuple literal.
DROP TABLE IF EXISTS mut_tz_in_many SYNC;
CREATE TABLE mut_tz_in_many (id UInt32, time DateTime) ENGINE = MergeTree ORDER BY id;
INSERT INTO mut_tz_in_many VALUES (1, toDateTime('2000-01-01 01:00:00')), (9, toDateTime('2000-01-01 01:00:00', 'UTC'));
SELECT 'in many selected', arraySort(groupArray(id)) FROM mut_tz_in_many WHERE time IN ('2000-01-01 01:00:00', '2000-01-01 02:00:00');
ALTER TABLE mut_tz_in_many DELETE WHERE time IN ('2000-01-01 01:00:00', '2000-01-01 02:00:00');
SELECT 'in many survivors', arraySort(groupArray(id)) FROM mut_tz_in_many;

-- IN with an array literal.
DROP TABLE IF EXISTS mut_tz_in_array SYNC;
CREATE TABLE mut_tz_in_array (id UInt32, time DateTime) ENGINE = MergeTree ORDER BY id;
INSERT INTO mut_tz_in_array VALUES (1, toDateTime('2000-01-01 01:00:00')), (9, toDateTime('2000-01-01 01:00:00', 'UTC'));
SELECT 'in array selected', arraySort(groupArray(id)) FROM mut_tz_in_array WHERE time IN ['2000-01-01 01:00:00'];
ALTER TABLE mut_tz_in_array DELETE WHERE time IN ['2000-01-01 01:00:00'];
SELECT 'in array survivors', arraySort(groupArray(id)) FROM mut_tz_in_array;

-- NOT IN deletes the complement.
DROP TABLE IF EXISTS mut_tz_not_in SYNC;
CREATE TABLE mut_tz_not_in (id UInt32, time DateTime) ENGINE = MergeTree ORDER BY id;
INSERT INTO mut_tz_not_in VALUES (1, toDateTime('2000-01-01 01:00:00')), (9, toDateTime('2000-01-01 01:00:00', 'UTC'));
SELECT 'not in selected', arraySort(groupArray(id)) FROM mut_tz_not_in WHERE time NOT IN ('2000-01-01 01:00:00', '2000-01-01 02:00:00');
ALTER TABLE mut_tz_not_in DELETE WHERE time NOT IN ('2000-01-01 01:00:00', '2000-01-01 02:00:00');
SELECT 'not in survivors', arraySort(groupArray(id)) FROM mut_tz_not_in;

-- nullIn is one of the IN operators too.
DROP TABLE IF EXISTS mut_tz_null_in SYNC;
CREATE TABLE mut_tz_null_in (id UInt32, time DateTime) ENGINE = MergeTree ORDER BY id;
INSERT INTO mut_tz_null_in VALUES (1, toDateTime('2000-01-01 01:00:00')), (9, toDateTime('2000-01-01 01:00:00', 'UTC'));
SELECT 'null in selected', arraySort(groupArray(id)) FROM mut_tz_null_in WHERE nullIn(time, tuple('2000-01-01 01:00:00'));
ALTER TABLE mut_tz_null_in DELETE WHERE nullIn(time, tuple('2000-01-01 01:00:00'));
SELECT 'null in survivors', arraySort(groupArray(id)) FROM mut_tz_null_in;

-- The same predicate in an ALTER UPDATE.
DROP TABLE IF EXISTS mut_tz_in_update SYNC;
CREATE TABLE mut_tz_in_update (id UInt32, time DateTime, val String) ENGINE = MergeTree ORDER BY id;
INSERT INTO mut_tz_in_update VALUES (1, toDateTime('2000-01-01 01:00:00'), 'old'), (9, toDateTime('2000-01-01 01:00:00', 'UTC'), 'old');
SELECT 'in update selected', arraySort(groupArray(id)) FROM mut_tz_in_update WHERE time IN ('2000-01-01 01:00:00');
ALTER TABLE mut_tz_in_update UPDATE val = 'new' WHERE time IN ('2000-01-01 01:00:00');
SELECT 'in update changed', arraySort(groupArray(id)) FROM mut_tz_in_update WHERE val = 'new';

-- A table-qualified column name, comparison and IN.
DROP TABLE IF EXISTS mut_tz_qual SYNC;
CREATE TABLE mut_tz_qual (id UInt32, time DateTime) ENGINE = MergeTree ORDER BY id;
INSERT INTO mut_tz_qual VALUES (1, toDateTime('2000-01-01 01:00:00')), (9, toDateTime('2000-01-01 01:00:00', 'UTC'));
SELECT 'qualified selected', arraySort(groupArray(id)) FROM mut_tz_qual WHERE mut_tz_qual.time = '2000-01-01 01:00:00';
ALTER TABLE mut_tz_qual DELETE WHERE mut_tz_qual.time = '2000-01-01 01:00:00';
SELECT 'qualified survivors', arraySort(groupArray(id)) FROM mut_tz_qual;

DROP TABLE IF EXISTS mut_tz_qual_in SYNC;
CREATE TABLE mut_tz_qual_in (id UInt32, time DateTime) ENGINE = MergeTree ORDER BY id;
INSERT INTO mut_tz_qual_in VALUES (1, toDateTime('2000-01-01 01:00:00')), (9, toDateTime('2000-01-01 01:00:00', 'UTC'));
SELECT 'qualified in selected', arraySort(groupArray(id)) FROM mut_tz_qual_in WHERE mut_tz_qual_in.time IN ('2000-01-01 01:00:00');
ALTER TABLE mut_tz_qual_in DELETE WHERE mut_tz_qual_in.time IN ('2000-01-01 01:00:00');
SELECT 'qualified in survivors', arraySort(groupArray(id)) FROM mut_tz_qual_in;

-- DateTime64 takes the scale-aware conversion.
DROP TABLE IF EXISTS mut_tz_in_dt64 SYNC;
CREATE TABLE mut_tz_in_dt64 (id UInt32, time DateTime64(3)) ENGINE = MergeTree ORDER BY id;
INSERT INTO mut_tz_in_dt64 VALUES (1, toDateTime64('2000-01-01 01:00:00', 3)), (9, toDateTime64('2000-01-01 01:00:00', 3, 'UTC'));
SELECT 'in dt64 selected', arraySort(groupArray(id)) FROM mut_tz_in_dt64 WHERE time IN ('2000-01-01 01:00:00');
ALTER TABLE mut_tz_in_dt64 DELETE WHERE time IN ('2000-01-01 01:00:00');
SELECT 'in dt64 survivors', arraySort(groupArray(id)) FROM mut_tz_in_dt64;

-- Lightweight DELETE reaches the same rewrite.
DROP TABLE IF EXISTS mut_tz_in_lightweight SYNC;
CREATE TABLE mut_tz_in_lightweight (id UInt32, time DateTime) ENGINE = MergeTree ORDER BY id;
INSERT INTO mut_tz_in_lightweight VALUES (1, toDateTime('2000-01-01 01:00:00')), (9, toDateTime('2000-01-01 01:00:00', 'UTC'));
SELECT 'lightweight selected', arraySort(groupArray(id)) FROM mut_tz_in_lightweight WHERE time IN ('2000-01-01 01:00:00');
DELETE FROM mut_tz_in_lightweight WHERE time IN ('2000-01-01 01:00:00');
SELECT 'lightweight survivors', arraySort(groupArray(id)) FROM mut_tz_in_lightweight;

-- An empty list has no literal to rewrite: no error, and no row is affected.
DROP TABLE IF EXISTS mut_tz_in_empty SYNC;
CREATE TABLE mut_tz_in_empty (id UInt32, time DateTime) ENGINE = MergeTree ORDER BY id;
INSERT INTO mut_tz_in_empty VALUES (1, toDateTime('2000-01-01 01:00:00')), (9, toDateTime('2000-01-01 01:00:00', 'UTC'));
SELECT 'in empty selected', arraySort(groupArray(id)) FROM mut_tz_in_empty WHERE time IN [];
ALTER TABLE mut_tz_in_empty DELETE WHERE time IN [];
SELECT 'in empty survivors', arraySort(groupArray(id)) FROM mut_tz_in_empty;
SELECT 'in empty rewritten', countIf(command LIKE '%America/Denver%') FROM system.mutations
    WHERE database = currentDatabase() AND table = 'mut_tz_in_empty';

-- A column declaring its own timezone keeps it: the literal must not be rewritten.
DROP TABLE IF EXISTS mut_tz_in_explicit SYNC;
CREATE TABLE mut_tz_in_explicit (id UInt32, time DateTime('UTC')) ENGINE = MergeTree ORDER BY id;
INSERT INTO mut_tz_in_explicit VALUES (1, '2000-01-01 01:00:00'), (9, '2000-01-01 02:00:00');
SELECT 'in explicit selected', arraySort(groupArray(id)) FROM mut_tz_in_explicit WHERE time IN ('2000-01-01 01:00:00');
ALTER TABLE mut_tz_in_explicit DELETE WHERE time IN ('2000-01-01 01:00:00');
SELECT 'in explicit survivors', arraySort(groupArray(id)) FROM mut_tz_in_explicit;
SELECT 'in explicit rewritten', countIf(command LIKE '%America/Denver%') FROM system.mutations
    WHERE database = currentDatabase() AND table = 'mut_tz_in_explicit';

-- A dotted name that resolves to a subcolumn keeps that subcolumn's own timezone.
-- Here the Tuple column is named after the table, so stripping that qualifier would
-- reach the zone-less top-level `time` and give the literal the session timezone.
DROP TABLE IF EXISTS mut_tz_sub SYNC;
CREATE TABLE mut_tz_sub (id UInt32, time DateTime, mut_tz_sub Tuple(time DateTime('UTC'))) ENGINE = MergeTree ORDER BY id;
INSERT INTO mut_tz_sub VALUES (1, toDateTime('2000-01-01 01:00:00'), tuple(toDateTime('2000-01-01 01:00:00', 'UTC'))),
                             (9, toDateTime('2000-01-01 01:00:00', 'UTC'), tuple(toDateTime('2000-01-01 09:00:00', 'UTC')));
SELECT 'sub selected', arraySort(groupArray(id)) FROM mut_tz_sub WHERE mut_tz_sub.time IN ('2000-01-01 01:00:00');
ALTER TABLE mut_tz_sub DELETE WHERE mut_tz_sub.time IN ('2000-01-01 01:00:00');
SELECT 'sub survivors', arraySort(groupArray(id)) FROM mut_tz_sub;

DROP TABLE IF EXISTS mut_tz_sub_qual SYNC;
CREATE TABLE mut_tz_sub_qual (id UInt32, time DateTime, x Tuple(time DateTime('UTC'))) ENGINE = MergeTree ORDER BY id;
INSERT INTO mut_tz_sub_qual VALUES (1, toDateTime('2000-01-01 01:00:00'), tuple(toDateTime('2000-01-01 01:00:00', 'UTC'))),
                                  (9, toDateTime('2000-01-01 01:00:00', 'UTC'), tuple(toDateTime('2000-01-01 09:00:00', 'UTC')));
SELECT 'sub qual selected', arraySort(groupArray(id)) FROM mut_tz_sub_qual WHERE mut_tz_sub_qual.x.time = '2000-01-01 01:00:00';
ALTER TABLE mut_tz_sub_qual DELETE WHERE mut_tz_sub_qual.x.time IN ('2000-01-01 01:00:00');
SELECT 'sub qual survivors', arraySort(groupArray(id)) FROM mut_tz_sub_qual;

SELECT 'sub rewritten', countIf(command LIKE '%America/Denver%') FROM system.mutations
    WHERE database = currentDatabase() AND table IN ('mut_tz_sub', 'mut_tz_sub_qual');

-- A list holding both a string and a number has no common element type, so each
-- string is converted on its own. 946688400 is '2000-01-01 01:00:00' UTC, the
-- instant id 9 holds, so the mutation must delete both rows.
DROP TABLE IF EXISTS mut_tz_in_mixed SYNC;
CREATE TABLE mut_tz_in_mixed (id UInt32, time DateTime) ENGINE = MergeTree ORDER BY id;
INSERT INTO mut_tz_in_mixed VALUES (1, toDateTime('2000-01-01 01:00:00')), (9, toDateTime('2000-01-01 01:00:00', 'UTC'));
SELECT 'in mixed selected', arraySort(groupArray(id)) FROM mut_tz_in_mixed WHERE time IN ('2000-01-01 01:00:00', 946688400);
ALTER TABLE mut_tz_in_mixed DELETE WHERE time IN ('2000-01-01 01:00:00', 946688400);
SELECT 'in mixed survivors', arraySort(groupArray(id)) FROM mut_tz_in_mixed;

-- ---------------------------------------------------------------------------
-- A lambda parameter shadowing a DateTime column
--
-- A lambda keeps its parameters as plain identifiers, so a name bound by one is
-- the array element and not the column of that name. The literal list of an IN
-- is therefore left alone for such a name, while the rewrite must still reach a
-- real column named in the same body and must resume after the lambda ends. The
-- comparison arm resolves names as it did before this change, which is what keeps
-- an unzoned DateTime element agreeing with the identical SELECT.
-- ---------------------------------------------------------------------------

-- The parameter shadows the column: the elements are strings, so the literal list
-- must not be converted to the column's timezone.
DROP TABLE IF EXISTS mut_tz_lambda_in SYNC;
CREATE TABLE mut_tz_lambda_in (id UInt32, time DateTime, arr Array(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO mut_tz_lambda_in VALUES (1, toDateTime('2000-01-01 01:00:00'), ['2000-01-01 01:00:00']),
                                    (9, toDateTime('2000-01-01 01:00:00', 'UTC'), ['2000-01-01 09:00:00']);
SELECT 'lambda shadow in selected', arraySort(groupArray(id)) FROM mut_tz_lambda_in
    WHERE arrayExists(time -> time IN ('2000-01-01 01:00:00'), arr);
ALTER TABLE mut_tz_lambda_in DELETE WHERE arrayExists(time -> time IN ('2000-01-01 01:00:00'), arr);
SELECT 'lambda shadow in survivors', arraySort(groupArray(id)) FROM mut_tz_lambda_in;

SELECT 'lambda shadow rewritten', countIf(command LIKE '%America/Denver%') FROM system.mutations
    WHERE database = currentDatabase() AND table = 'mut_tz_lambda_in';

-- The same shadowed name written as an explicit tuple: that spelling resolves the name
-- as it did before this change, so an unzoned DateTime element is still converted.
DROP TABLE IF EXISTS mut_tz_lambda_tuple SYNC;
CREATE TABLE mut_tz_lambda_tuple (id UInt32, time DateTime, arr Array(DateTime)) ENGINE = MergeTree ORDER BY id;
INSERT INTO mut_tz_lambda_tuple VALUES (1, toDateTime('2000-01-01 01:00:00'), [toDateTime('2000-01-01 01:00:00')]),
                                       (9, toDateTime('2000-01-01 01:00:00', 'UTC'), [toDateTime('2000-01-01 01:00:00', 'UTC')]);
SELECT 'lambda tuple selected', arraySort(groupArray(id)) FROM mut_tz_lambda_tuple
    WHERE arrayExists(time -> time IN tuple('2000-01-01 01:00:00'), arr);
ALTER TABLE mut_tz_lambda_tuple DELETE WHERE arrayExists(time -> time IN tuple('2000-01-01 01:00:00'), arr);
SELECT 'lambda tuple survivors', arraySort(groupArray(id)) FROM mut_tz_lambda_tuple;
SELECT 'lambda tuple rewritten', countIf(command LIKE '%America/Denver%') FROM system.mutations
    WHERE database = currentDatabase() AND table = 'mut_tz_lambda_tuple';

-- The parameter is bound to an unzoned DateTime element, whose own reading of a
-- bare literal is the server timezone, so the comparison must still be converted
-- or the mutation stops agreeing with the identical SELECT.
DROP TABLE IF EXISTS mut_tz_lambda_dt SYNC;
CREATE TABLE mut_tz_lambda_dt (id UInt32, time DateTime, arr Array(DateTime)) ENGINE = MergeTree ORDER BY id;
INSERT INTO mut_tz_lambda_dt VALUES (1, toDateTime('2000-01-01 01:00:00'), [toDateTime('2000-01-01 01:00:00')]),
                                    (9, toDateTime('2000-01-01 01:00:00', 'UTC'), [toDateTime('2000-01-01 01:00:00', 'UTC')]);
SELECT 'lambda datetime array selected', arraySort(groupArray(id)) FROM mut_tz_lambda_dt
    WHERE arrayExists(time -> time = '2000-01-01 01:00:00', arr);
ALTER TABLE mut_tz_lambda_dt DELETE WHERE arrayExists(time -> time = '2000-01-01 01:00:00', arr);
SELECT 'lambda datetime array survivors', arraySort(groupArray(id)) FROM mut_tz_lambda_dt;
SELECT 'lambda datetime array rewritten', countIf(command LIKE '%America/Denver%') FROM system.mutations
    WHERE database = currentDatabase() AND table = 'mut_tz_lambda_dt';

-- A dotted name whose first part is the parameter is a field of that parameter, not the
-- table's subcolumn of the same spelling, so the string element keeps its own reading.
DROP TABLE IF EXISTS mut_tz_lambda_field SYNC;
CREATE TABLE mut_tz_lambda_field (id UInt32, x Tuple(time DateTime), arr Array(Tuple(time String))) ENGINE = MergeTree ORDER BY id;
INSERT INTO mut_tz_lambda_field VALUES (1, tuple(toDateTime('2000-01-01 01:00:00')), [tuple('2000-01-01 01:00:00')]),
                                       (9, tuple(toDateTime('2000-01-01 01:00:00')), [tuple('2000-01-01 09:00:00')]);
SELECT 'lambda field selected', arraySort(groupArray(id)) FROM mut_tz_lambda_field
    WHERE arrayExists(x -> x.time = '2000-01-01 01:00:00', arr);
ALTER TABLE mut_tz_lambda_field DELETE WHERE arrayExists(x -> x.time = '2000-01-01 01:00:00', arr);
SELECT 'lambda field survivors', arraySort(groupArray(id)) FROM mut_tz_lambda_field;
SELECT 'lambda field rewritten', countIf(command LIKE '%America/Denver%') FROM system.mutations
    WHERE database = currentDatabase() AND table = 'mut_tz_lambda_field';

-- The parameter is x, so the `time` in the same body is the column and its literal
-- list must still be rewritten: a guard that skipped lambda bodies outright would
-- trade one set of wrong rows for another.
DROP TABLE IF EXISTS mut_tz_lambda_column SYNC;
CREATE TABLE mut_tz_lambda_column (id UInt32, time DateTime, arr Array(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO mut_tz_lambda_column VALUES (1, toDateTime('2000-01-01 01:00:00'), ['k']),
                                        (9, toDateTime('2000-01-01 01:00:00', 'UTC'), ['k']);
SELECT 'lambda column selected', arraySort(groupArray(id)) FROM mut_tz_lambda_column
    WHERE arrayExists(x -> x = 'k' AND time IN ('2000-01-01 01:00:00'), arr);
ALTER TABLE mut_tz_lambda_column DELETE WHERE arrayExists(x -> x = 'k' AND time IN ('2000-01-01 01:00:00'), arr);
SELECT 'lambda column survivors', arraySort(groupArray(id)) FROM mut_tz_lambda_column;
SELECT 'lambda column rewritten', countIf(command LIKE '%America/Denver%') FROM system.mutations
    WHERE database = currentDatabase() AND table = 'mut_tz_lambda_column';

-- The bound name must stop binding when the lambda ends: the occurrence inside
-- it is left alone while the trailing top-level one is rewritten, so the stored
-- command carries exactly one converted literal.
DROP TABLE IF EXISTS mut_tz_lambda_pop SYNC;
CREATE TABLE mut_tz_lambda_pop (id UInt32, time DateTime, arr Array(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO mut_tz_lambda_pop VALUES (1, toDateTime('2000-01-01 01:00:00'), ['2000-01-01 01:00:00']),
                                     (9, toDateTime('2000-01-01 01:00:00', 'UTC'), ['2000-01-01 01:00:00']);
SELECT 'lambda scope pop selected', arraySort(groupArray(id)) FROM mut_tz_lambda_pop
    WHERE arrayExists(time -> time IN ('2000-01-01 01:00:00'), arr) AND time IN ('2000-01-01 01:00:00');
ALTER TABLE mut_tz_lambda_pop DELETE WHERE arrayExists(time -> time IN ('2000-01-01 01:00:00'), arr) AND time IN ('2000-01-01 01:00:00');
SELECT 'lambda scope pop survivors', arraySort(groupArray(id)) FROM mut_tz_lambda_pop;
SELECT 'lambda scope pop rewrites', sum(countMatches(command, 'America/Denver')) FROM system.mutations
    WHERE database = currentDatabase() AND table = 'mut_tz_lambda_pop';

-- One case under a second session timezone, so that the file stays discriminating
-- even for a server whose own timezone is America/Denver.
SET session_timezone = 'Asia/Tokyo';

DROP TABLE IF EXISTS mut_tz_in_tokyo SYNC;
CREATE TABLE mut_tz_in_tokyo (id UInt32, time DateTime) ENGINE = MergeTree ORDER BY id;
INSERT INTO mut_tz_in_tokyo VALUES (1, toDateTime('2000-01-01 01:00:00', 'America/Denver')),
                                   (5, toDateTime('2000-01-01 01:00:00')),
                                   (9, toDateTime('2000-01-01 01:00:00', 'UTC'));
SELECT 'tokyo selected', arraySort(groupArray(id)) FROM mut_tz_in_tokyo WHERE time IN ('2000-01-01 01:00:00');
ALTER TABLE mut_tz_in_tokyo DELETE WHERE time IN ('2000-01-01 01:00:00');
SELECT 'tokyo survivors', arraySort(groupArray(id)) FROM mut_tz_in_tokyo;

-- The heterogeneous list under that second session timezone too: the string
-- names the Tokyo reading and 946688400 the UTC one, so this pair stays
-- discriminating whatever timezone the server runs in.
DROP TABLE IF EXISTS mut_tz_in_tokyo_mixed SYNC;
CREATE TABLE mut_tz_in_tokyo_mixed (id UInt32, time DateTime) ENGINE = MergeTree ORDER BY id;
INSERT INTO mut_tz_in_tokyo_mixed VALUES (1, toDateTime('2000-01-01 01:00:00', 'America/Denver')),
                                         (5, toDateTime('2000-01-01 01:00:00')),
                                         (9, toDateTime('2000-01-01 01:00:00', 'UTC'));
SELECT 'tokyo mixed selected', arraySort(groupArray(id)) FROM mut_tz_in_tokyo_mixed WHERE time IN ('2000-01-01 01:00:00', 946688400);
ALTER TABLE mut_tz_in_tokyo_mixed DELETE WHERE time IN ('2000-01-01 01:00:00', 946688400);
SELECT 'tokyo mixed survivors', arraySort(groupArray(id)) FROM mut_tz_in_tokyo_mixed;

DROP TABLE mut_tz_in_one SYNC;
DROP TABLE mut_tz_in_many SYNC;
DROP TABLE mut_tz_in_array SYNC;
DROP TABLE mut_tz_not_in SYNC;
DROP TABLE mut_tz_null_in SYNC;
DROP TABLE mut_tz_in_update SYNC;
DROP TABLE mut_tz_qual SYNC;
DROP TABLE mut_tz_qual_in SYNC;
DROP TABLE mut_tz_in_dt64 SYNC;
DROP TABLE mut_tz_in_lightweight SYNC;
DROP TABLE mut_tz_in_empty SYNC;
DROP TABLE mut_tz_in_explicit SYNC;
DROP TABLE mut_tz_sub SYNC;
DROP TABLE mut_tz_sub_qual SYNC;
DROP TABLE mut_tz_in_mixed SYNC;
DROP TABLE mut_tz_lambda_in SYNC;
DROP TABLE mut_tz_lambda_tuple SYNC;
DROP TABLE mut_tz_lambda_dt SYNC;
DROP TABLE mut_tz_lambda_field SYNC;
DROP TABLE mut_tz_lambda_column SYNC;
DROP TABLE mut_tz_lambda_pop SYNC;
DROP TABLE mut_tz_in_tokyo SYNC;
DROP TABLE mut_tz_in_tokyo_mixed SYNC;
