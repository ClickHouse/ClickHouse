
-- Context: https://github.com/ClickHouse/ClickHouse/issues/42916

-- STRING WITH 10 CHARACTERS
-- SELECT version() AS v, hex(argMaxState('0123456789', number)) AS state FROM numbers(1) FORMAT CSV

CREATE TABLE argmaxstate_hex_small
(
    `v` String,
    `state` String
)
ENGINE = TinyLog;

INSERT into argmaxstate_hex_small VALUES ('22.8.5.29','0B0000003031323334353637383900010000000000000000'), ('22.8.6.71','0A00000030313233343536373839010000000000000000');

-- Assert that the current version will write the same as 22.8.5 (last known good 22.8 minor)
SELECT
    (SELECT hex(argMaxState('0123456789', number)) FROM numbers(1)) = state
FROM argmaxstate_hex_small
WHERE v = '22.8.5.29';

-- Assert that the current version can read correctly both the old and the regression states
SELECT
    v,
    length(finalizeAggregation(CAST(unhex(state) AS AggregateFunction(argMax, String, UInt64))))
FROM argmaxstate_hex_small;

-- STRING WITH 54 characters
-- SELECT version() AS v, hex(argMaxState('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz', number)) AS state FROM numbers(1) FORMAT CSV
CREATE TABLE argmaxstate_hex_large
(
    `v` String,
    `state` String
)
ENGINE = TinyLog;

INSERT into argmaxstate_hex_large VALUES ('22.8.5.29','350000004142434445464748494A4B4C4D4E4F505152535455565758595A6162636465666768696A6B6C6D6E6F707172737475767778797A00010000000000000000'), ('22.8.6.71','340000004142434445464748494A4B4C4D4E4F505152535455565758595A6162636465666768696A6B6C6D6E6F707172737475767778797A010000000000000000');

SELECT
    (SELECT hex(argMaxState('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz', number)) FROM numbers(1)) = state
FROM argmaxstate_hex_large
WHERE v = '22.8.5.29';

SELECT
    v,
    length(finalizeAggregation(CAST(unhex(state) AS AggregateFunction(argMax, String, UInt64))))
FROM argmaxstate_hex_large;

-- STRING WITH 0 characters
-- SELECT version() AS v, hex(argMaxState('', number)) AS state FROM numbers(1) FORMAT CSV
CREATE TABLE argmaxstate_hex_empty
(
    `v` String,
    `state` String
)
ENGINE = TinyLog;

INSERT into argmaxstate_hex_empty VALUES ('22.8.5.29','0100000000010000000000000000'), ('22.8.6.71','00000000010000000000000000');

SELECT
    (SELECT hex(argMaxState('', number)) FROM numbers(1)) = state
FROM argmaxstate_hex_empty
WHERE v = '22.8.5.29';

SELECT v, length(finalizeAggregation(CAST(unhex(state) AS AggregateFunction(argMax, String, UInt64))))
FROM argmaxstate_hex_empty;

-- Right in the border of small and large buffers
-- SELECT hex(argMaxState('0123456789012345678901234567890123456789012345' as a, number)) AS state, length(a) FROM numbers(1) FORMAT CSV
SELECT '46_OK', finalizeAggregation(CAST(unhex('2F0000003031323334353637383930313233343536373839303132333435363738393031323334353637383930313233343500010000000000000000'), 'AggregateFunction(argMax, String, UInt64)'));
SELECT '46_KO', finalizeAggregation(CAST(unhex('2E00000030313233343536373839303132333435363738393031323334353637383930313233343536373839303132333435010000000000000000'), 'AggregateFunction(argMax, String, UInt64)'));

-- SELECT hex(argMaxState('01234567890123456789012345678901234567890123456' as a, number)) AS state, length(a) FROM numbers(1) FORMAT CSV
SELECT '47_OK', finalizeAggregation(CAST(unhex('30000000303132333435363738393031323334353637383930313233343536373839303132333435363738393031323334353600010000000000000000'), 'AggregateFunction(argMax, String, UInt64)'));
SELECT '47_KO', finalizeAggregation(CAST(unhex('2F0000003031323334353637383930313233343536373839303132333435363738393031323334353637383930313233343536010000000000000000'), 'AggregateFunction(argMax, String, UInt64)'));

-- SELECT hex(argMaxState('012345678901234567890123456789012345678901234567' as a, number)) AS state, length(a) FROM numbers(1) FORMAT CSV
SELECT '48_OK', finalizeAggregation(CAST(unhex('3100000030313233343536373839303132333435363738393031323334353637383930313233343536373839303132333435363700010000000000000000'), 'AggregateFunction(argMax, String, UInt64)'));
SELECT '48_KO', finalizeAggregation(CAST(unhex('30000000303132333435363738393031323334353637383930313233343536373839303132333435363738393031323334353637010000000000000000'), 'AggregateFunction(argMax, String, UInt64)'));

-- Right in the allocation limit (power of 2)
-- SELECT hex(argMaxState('012345678901234567890123456789012345678901234567890123456789012' as a, number)) AS state, length(a) FROM numbers(1) FORMAT CSV
SELECT '63_OK', finalizeAggregation(CAST(unhex('4000000030313233343536373839303132333435363738393031323334353637383930313233343536373839303132333435363738393031323334353637383930313200010000000000000000'), 'AggregateFunction(argMax, String, UInt64)'));
SELECT '63_KO', finalizeAggregation(CAST(unhex('3F000000303132333435363738393031323334353637383930313233343536373839303132333435363738393031323334353637383930313233343536373839303132010000000000000000'), 'AggregateFunction(argMax, String, UInt64)'));
-- SELECT hex(argMaxState('0123456789012345678901234567890123456789012345678901234567890123' as a, number)) AS state, length(a) FROM numbers(1) FORMAT CSV
SELECT '64_OK', finalizeAggregation(CAST(unhex('410000003031323334353637383930313233343536373839303132333435363738393031323334353637383930313233343536373839303132333435363738393031323300010000000000000000'), 'AggregateFunction(argMax, String, UInt64)'));
SELECT '64_KO', finalizeAggregation(CAST(unhex('4000000030313233343536373839303132333435363738393031323334353637383930313233343536373839303132333435363738393031323334353637383930313233010000000000000000'), 'AggregateFunction(argMax, String, UInt64)'));

SELECT '-1', maxMerge(x), length(maxMerge(x)) from (select CAST(unhex('ffffffff') || randomString(100500), 'AggregateFunction(max, String)') as x);
SELECT '-2', maxMerge(x), length(maxMerge(x)) from (select CAST(unhex('fffffffe') || randomString(100500), 'AggregateFunction(max, String)') as x);
SELECT '-2^31', maxMerge(x), length(maxMerge(x)) from (select CAST(unhex('00000080') || randomString(100500), 'AggregateFunction(max, String)') as x);

SELECT '2^31-1', maxMerge(x) from (select CAST(unhex('ffffff7f') || randomString(100500), 'AggregateFunction(max, String)') as x); -- { serverError TOO_LARGE_STRING_SIZE }

SELECT '2^31-2', maxMerge(x) from (select CAST(unhex('feffff7f') || randomString(100500), 'AggregateFunction(max, String)') as x); -- { serverError CANNOT_READ_ALL_DATA }

SELECT '2^30', maxMerge(x) from (select CAST(unhex('00000040') || randomString(100500), 'AggregateFunction(max, String)') as x); -- { serverError CANNOT_READ_ALL_DATA }
SELECT '2^30+1', maxMerge(x) from (select CAST(unhex('01000040') || randomString(100500), 'AggregateFunction(max, String)') as x); -- { serverError CANNOT_READ_ALL_DATA }

SELECT '2^30-1', maxMerge(x) from (select CAST(unhex('ffffff3f') || randomString(100500), 'AggregateFunction(max, String)') as x); -- { serverError CANNOT_READ_ALL_DATA }
-- The following query works, but it's too long and consumes to much memory
-- SELECT '2^30-1', length(maxMerge(x)) from (select CAST(unhex('ffffff3f') || randomString(0x3FFFFFFF - 1) || 'x', 'AggregateFunction(max, String)') as x);
SELECT '1M without 0', length(maxMerge(x)) from (select CAST(unhex('00001000') || randomString(0x00100000 - 1) || 'x', 'AggregateFunction(max, String)') as x);
SELECT '1M with 0', length(maxMerge(x)) from (select CAST(unhex('00001000') || randomString(0x00100000 - 1) || '\0', 'AggregateFunction(max, String)') as x);

SELECT 'fuzz1', finalizeAggregation(CAST(unhex('3000000\0303132333435363738393031323334353637383930313233343536373839303132333435363738393031323334353600010000000000000000'), 'AggregateFunction(argMax, String, UInt64)')); -- { serverError INCORRECT_DATA }
SELECT 'fuzz2', finalizeAggregation(CAST(unhex('04000000' || '30313233' || '01' || 'ffffffffffffffff'), 'AggregateFunction(argMax, String, UInt64)')) as x, length(x);
SELECT 'fuzz3', finalizeAggregation(CAST(unhex('04000000' || '30313233' || '00' || 'ffffffffffffffff'), 'AggregateFunction(argMax, String, UInt64)')) as x, length(x); -- { serverError INCORRECT_DATA }
SELECT 'fuzz4', finalizeAggregation(CAST(unhex('04000000' || '30313233' || '00'), 'AggregateFunction(argMax, String, UInt64)')) as x, length(x); -- { serverError INCORRECT_DATA }
SELECT 'fuzz5', finalizeAggregation(CAST(unhex('0100000000000000000FFFFFFFF0'), 'AggregateFunction(argMax, UInt64, String)')); -- { serverError INCORRECT_DATA }


drop table if exists aggr;
create table aggr (n int, s AggregateFunction(max, String)) engine=MergeTree order by n;
insert into aggr select 1, maxState('');
insert into aggr select 2, maxState('\0');
insert into aggr select 3, maxState('\0\0\0\0');
insert into aggr select 4, maxState('abrac\0dabra\0');
select n, maxMerge(s) as x, length(x) from aggr group by n order by n;
select maxMerge(s) as x, length(x) from aggr;
drop table aggr;
