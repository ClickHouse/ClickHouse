-- Tags: no-fasttest
-- no-fasttest: uniqTheta requires building with libraries
-- Test uniq and uniqExact devirtualization for String, FixedString, and IPv6

-- Basic uniq tests
SELECT 'uniq String', uniq(number::String) FROM numbers(10);
SELECT 'uniq FixedString(1)', uniq(number::String::FixedString(1)) FROM numbers(10);
SELECT 'uniq FixedString(2)', uniq(number::String::FixedString(2)) FROM numbers(10);
SELECT 'uniq FixedString(8)', uniq(number::String::FixedString(8)) FROM numbers(10);
SELECT 'uniq FixedString(16)', uniq(number::String::FixedString(16)) FROM numbers(10);
SELECT 'uniq FixedString(32)', uniq(number::String::FixedString(32)) FROM numbers(10);
SELECT 'uniq IPv6', uniq(toIPv6(concat('2001:db8::', hex(number)))) FROM numbers(10);

-- Verify state format for uniq (single value - ensure devirtualization doesn't change state)
SELECT 'uniq String single state', hex(state) as hex_state, length(hex_state) as len, toTypeName(state) FROM (SELECT uniqState('test'::String) as state);
SELECT 'uniq FixedString single state', hex(state) as hex_state, length(hex_state) as len, toTypeName(state) FROM (SELECT uniqState('test'::FixedString(8)) as state);
SELECT 'uniq IPv6 single state', hex(state) as hex_state, length(hex_state) as len, toTypeName(state) FROM (SELECT uniqState('2001:0db8::'::IPv6) as state);

-- Basic uniqExact tests
SELECT 'uniqExact String', uniqExact(number::String) FROM numbers(10);
SELECT 'uniqExact FixedString(1)', uniqExact(number::String::FixedString(1)) FROM numbers(10);
SELECT 'uniqExact FixedString(2)', uniqExact(number::String::FixedString(2)) FROM numbers(10);
SELECT 'uniqExact FixedString(8)', uniqExact(number::String::FixedString(8)) FROM numbers(10);
SELECT 'uniqExact FixedString(16)', uniqExact(number::String::FixedString(16)) FROM numbers(10);
SELECT 'uniqExact FixedString(32)', uniqExact(number::String::FixedString(32)) FROM numbers(10);
SELECT 'uniqExact IPv6', uniqExact(toIPv6(concat('2001:db8::', hex(number)))) FROM numbers(10);

-- Verify state format for uniqExact (single value - ensure devirtualization doesn't change state)
SELECT 'uniqExact String single state', hex(state) as hex_state, length(hex_state) as len, toTypeName(state) FROM (SELECT uniqExactState('test'::String) as state);
SELECT 'uniqExact FixedString single state', hex(state) as hex_state, length(hex_state) as len, toTypeName(state) FROM (SELECT uniqExactState('test'::FixedString(8)) as state);
SELECT 'uniqExact IPv6 single state', hex(state) as hex_state, length(hex_state) as len, toTypeName(state) FROM (SELECT uniqExactState('2001:0db8::'::IPv6) as state);

-- uniqState tests with hex output and type checking
SELECT 'uniqState String', hex(uniqState(number::String) as state), toTypeName(state) FROM numbers(10);
SELECT 'uniqState FixedString(1)', hex(uniqState(number::String::FixedString(1)) as state), toTypeName(state) FROM numbers(10);
SELECT 'uniqState FixedString(2)', hex(uniqState(number::String::FixedString(2)) as state), toTypeName(state) FROM numbers(10);
SELECT 'uniqState FixedString(8)', hex(uniqState(number::String::FixedString(8)) as state), toTypeName(state) FROM numbers(10);
SELECT 'uniqState FixedString(16)', hex(uniqState(number::String::FixedString(16)) as state), toTypeName(state) FROM numbers(10);
SELECT 'uniqState FixedString(32)', hex(uniqState(number::String::FixedString(32)) as state), toTypeName(state) FROM numbers(10);
SELECT 'uniqState IPv6', hex(uniqState(toIPv6(concat('2001:db8::', hex(number)))) as state), toTypeName(state) FROM numbers(10);

-- uniqExactState tests with hex output and type checking
SELECT 'uniqExactState String', hex(uniqExactState(number::String) as state), toTypeName(state) FROM numbers(10);
SELECT 'uniqExactState FixedString(1)', hex(uniqExactState(number::String::FixedString(1)) as state), toTypeName(state) FROM numbers(10);
SELECT 'uniqExactState FixedString(2)', hex(uniqExactState(number::String::FixedString(2)) as state), toTypeName(state) FROM numbers(10);
SELECT 'uniqExactState FixedString(8)', hex(uniqExactState(number::String::FixedString(8)) as state), toTypeName(state) FROM numbers(10);
SELECT 'uniqExactState FixedString(16)', hex(uniqExactState(number::String::FixedString(16)) as state), toTypeName(state) FROM numbers(10);
SELECT 'uniqExactState FixedString(32)', hex(uniqExactState(number::String::FixedString(32)) as state), toTypeName(state) FROM numbers(10);
SELECT 'uniqExactState IPv6', hex(uniqExactState(toIPv6(concat('2001:db8::', hex(number)))) as state), toTypeName(state) FROM numbers(10);

-- Test merging states
SELECT 'uniqMerge String', uniqMerge(state) FROM (
    SELECT uniqState(number::String) as state FROM numbers(5)
    UNION ALL
    SELECT uniqState((number + 3)::String) as state FROM numbers(5)
);

SELECT 'uniqMerge FixedString(2)', uniqMerge(state) FROM (
    SELECT uniqState(number::String::FixedString(2)) as state FROM numbers(5)
    UNION ALL
    SELECT uniqState((number + 3)::String::FixedString(2)) as state FROM numbers(5)
);

SELECT 'uniqMerge IPv6', uniqMerge(state) FROM (
    SELECT uniqState(toIPv6(concat('2001:db8::', hex(number)))) as state FROM numbers(5)
    UNION ALL
    SELECT uniqState(toIPv6(concat('2001:db8::', hex(number + 3)))) as state FROM numbers(5)
);

SELECT 'uniqExactMerge String', uniqExactMerge(state) FROM (
    SELECT uniqExactState(number::String) as state FROM numbers(5)
    UNION ALL
    SELECT uniqExactState((number + 3)::String) as state FROM numbers(5)
);

SELECT 'uniqExactMerge FixedString(2)', uniqExactMerge(state) FROM (
    SELECT uniqExactState(number::String::FixedString(2)) as state FROM numbers(5)
    UNION ALL
    SELECT uniqExactState((number + 3)::String::FixedString(2)) as state FROM numbers(5)
);

SELECT 'uniqExactMerge IPv6', uniqExactMerge(state) FROM (
    SELECT uniqExactState(toIPv6(concat('2001:db8::', hex(number)))) as state FROM numbers(5)
    UNION ALL
    SELECT uniqExactState(toIPv6(concat('2001:db8::', hex(number + 3)))) as state FROM numbers(5)
);

-- Test with duplicate values to verify cardinality estimation
SELECT 'uniq String with duplicates', uniq(x) FROM (
    SELECT (number % 5)::String as x FROM numbers(100)
);

SELECT 'uniqExact String with duplicates', uniqExact(x) FROM (
    SELECT (number % 5)::String as x FROM numbers(100)
);

SELECT 'uniq FixedString with duplicates', uniq(x) FROM (
    SELECT (number % 5)::String::FixedString(8) as x FROM numbers(100)
);

SELECT 'uniqExact FixedString with duplicates', uniqExact(x) FROM (
    SELECT (number % 5)::String::FixedString(8) as x FROM numbers(100)
);

SELECT 'uniq IPv6 with duplicates', uniq(x) FROM (
    SELECT toIPv6(concat('2001:db8::', hex(number % 5))) as x FROM numbers(100)
);

SELECT 'uniqExact IPv6 with duplicates', uniqExact(x) FROM (
    SELECT toIPv6(concat('2001:db8::', hex(number % 5))) as x FROM numbers(100)
);

-- Test with empty strings and special cases
SELECT 'uniq empty String', uniq(x) FROM (
    SELECT ''::String as x FROM numbers(10)
);

SELECT 'uniqExact empty String', uniqExact(x) FROM (
    SELECT ''::String as x FROM numbers(10)
);

SELECT 'uniq empty FixedString', uniq(x) FROM (
    SELECT ''::FixedString(8) as x FROM numbers(10)
);

SELECT 'uniqExact empty FixedString', uniqExact(x) FROM (
    SELECT ''::FixedString(8) as x FROM numbers(10)
);

-- Test with larger dataset to ensure optimization works at scale
SELECT 'uniq String large', uniq(number::String) FROM numbers(10000);
SELECT 'uniqExact String large', uniqExact(number::String) FROM numbers(10000);
SELECT 'uniq FixedString(16) large', uniq(number::String::FixedString(16)) FROM numbers(10000);
SELECT 'uniqExact FixedString(16) large', uniqExact(number::String::FixedString(16)) FROM numbers(10000);

-- uniqHLL12 tests
SELECT 'uniqHLL12 String', uniqHLL12(number::String) FROM numbers(10);
SELECT 'uniqHLL12 FixedString(1)', uniqHLL12(number::String::FixedString(1)) FROM numbers(10);
SELECT 'uniqHLL12 FixedString(2)', uniqHLL12(number::String::FixedString(2)) FROM numbers(10);
SELECT 'uniqHLL12 FixedString(8)', uniqHLL12(number::String::FixedString(8)) FROM numbers(10);
SELECT 'uniqHLL12 FixedString(16)', uniqHLL12(number::String::FixedString(16)) FROM numbers(10);
SELECT 'uniqHLL12 IPv6', uniqHLL12(toIPv6(concat('2001:db8::', hex(number)))) FROM numbers(10);

-- Verify state format for uniqHLL12 (single value - ensure devirtualization doesn't change state)
SELECT 'uniqHLL12 String single state', hex(state) as hex_state, length(hex_state) as len, toTypeName(state) FROM (SELECT uniqHLL12State('test'::String) as state);
SELECT 'uniqHLL12 FixedString single state', hex(state) as hex_state, length(hex_state) as len, toTypeName(state) FROM (SELECT uniqHLL12State('test'::FixedString(8)) as state);
SELECT 'uniqHLL12 IPv6 single state', hex(state) as hex_state, length(hex_state) as len, toTypeName(state) FROM (SELECT uniqHLL12State('2001:0db8::'::IPv6) as state);

-- uniqHLL12 with duplicates
SELECT 'uniqHLL12 String with duplicates', uniqHLL12(x) FROM (
    SELECT (number % 5)::String as x FROM numbers(100)
);

SELECT 'uniqHLL12 FixedString with duplicates', uniqHLL12(x) FROM (
    SELECT (number % 5)::String::FixedString(8) as x FROM numbers(100)
);

SELECT 'uniqHLL12 IPv6 with duplicates', uniqHLL12(x) FROM (
    SELECT toIPv6(concat('2001:db8::', hex(number % 5))) as x FROM numbers(100)
);

-- uniqHLL12State tests
SELECT 'uniqHLL12State String', hex(uniqHLL12State(number::String) as state), toTypeName(state) FROM numbers(10);
SELECT 'uniqHLL12State FixedString(2)', hex(uniqHLL12State(number::String::FixedString(2)) as state), toTypeName(state) FROM numbers(10);
SELECT 'uniqHLL12State IPv6', hex(uniqHLL12State(toIPv6(concat('2001:db8::', hex(number)))) as state), toTypeName(state) FROM numbers(10);

-- uniqTheta tests
SELECT 'uniqTheta String', uniqTheta(number::String) FROM numbers(10);
SELECT 'uniqTheta FixedString(2)', uniqTheta(number::String::FixedString(2)) FROM numbers(10);
SELECT 'uniqTheta IPv6', uniqTheta(toIPv6(concat('2001:db8::', hex(number)))) FROM numbers(10);

-- uniqTheta with duplicates
SELECT 'uniqTheta String with duplicates', uniqTheta(x) FROM (
    SELECT (number % 5)::String as x FROM numbers(100)
);

SELECT 'uniqTheta FixedString with duplicates', uniqTheta(x) FROM (
    SELECT (number % 5)::String::FixedString(8) as x FROM numbers(100)
);

-- Verify state format for uniqTheta (single value - ensure devirtualization doesn't change state)
SELECT 'uniqTheta String single state', hex(state) as hex_state, length(hex_state) as len, toTypeName(state) FROM (SELECT uniqThetaState('test'::String) as state);
SELECT 'uniqTheta FixedString single state', hex(state) as hex_state, length(hex_state) as len, toTypeName(state) FROM (SELECT uniqThetaState('test'::FixedString(8)) as state);

-- uniqThetaState tests
SELECT 'uniqThetaState String', hex(uniqThetaState(number::String) as state), toTypeName(state) FROM numbers(10);
SELECT 'uniqThetaState FixedString(2)', hex(uniqThetaState(number::String::FixedString(2)) as state), toTypeName(state) FROM numbers(10);

-- Verify states can be finalized correctly (state -> value)
SELECT 'uniq finalize String', uniqMerge(state) FROM (SELECT uniqState('test'::String) as state);
SELECT 'uniq finalize FixedString', uniqMerge(state) FROM (SELECT uniqState('test'::FixedString(8)) as state);
SELECT 'uniq finalize IPv6', uniqMerge(state) FROM (SELECT uniqState('2001:0db8::'::IPv6) as state);
SELECT 'uniqExact finalize String', uniqExactMerge(state) FROM (SELECT uniqExactState('test'::String) as state);
SELECT 'uniqExact finalize FixedString', uniqExactMerge(state) FROM (SELECT uniqExactState('test'::FixedString(8)) as state);
SELECT 'uniqExact finalize IPv6', uniqExactMerge(state) FROM (SELECT uniqExactState('2001:0db8::'::IPv6) as state);
SELECT 'uniqHLL12 finalize String', uniqHLL12Merge(state) FROM (SELECT uniqHLL12State('test'::String) as state);
SELECT 'uniqHLL12 finalize FixedString', uniqHLL12Merge(state) FROM (SELECT uniqHLL12State('test'::FixedString(8)) as state);
SELECT 'uniqHLL12 finalize IPv6', uniqHLL12Merge(state) FROM (SELECT uniqHLL12State('2001:0db8::'::IPv6) as state);

-- uniqTheta variadic
SELECT 'uniqTheta variadic String+UInt', uniqTheta(number, number::String) FROM numbers(10);
SELECT 'uniqTheta variadic FixedString+UInt', uniqTheta(number, number::String::FixedString(4)) FROM numbers(10);

-- Variadic arguments tests (multiple arguments)
SELECT 'uniq variadic String+UInt', uniq(number, number::String) FROM numbers(10);
SELECT 'uniq variadic String+FixedString', uniq(number::String, number::String::FixedString(4)) FROM numbers(10);
SELECT 'uniq variadic FixedString+UInt', uniq(number, number::String::FixedString(4)) FROM numbers(10);
SELECT 'uniq variadic String+String', uniq(number::String, (number * 2)::String) FROM numbers(10);
SELECT 'uniq variadic FixedString+FixedString', uniq(number::String::FixedString(2), (number * 2)::String::FixedString(2)) FROM numbers(10);
SELECT 'uniq variadic IPv6+UInt', uniq(number, toIPv6(concat('2001:db8::', hex(number)))) FROM numbers(10);
SELECT 'uniq variadic String+IPv6', uniq(number::String, toIPv6(concat('2001:db8::', hex(number)))) FROM numbers(10);

-- uniqExact variadic arguments
SELECT 'uniqExact variadic String+UInt', uniqExact(number, number::String) FROM numbers(10);
SELECT 'uniqExact variadic String+FixedString', uniqExact(number::String, number::String::FixedString(4)) FROM numbers(10);
SELECT 'uniqExact variadic FixedString+UInt', uniqExact(number, number::String::FixedString(4)) FROM numbers(10);
SELECT 'uniqExact variadic String+String', uniqExact(number::String, (number * 2)::String) FROM numbers(10);
SELECT 'uniqExact variadic FixedString+FixedString', uniqExact(number::String::FixedString(2), (number * 2)::String::FixedString(2)) FROM numbers(10);
SELECT 'uniqExact variadic IPv6+UInt', uniqExact(number, toIPv6(concat('2001:db8::', hex(number)))) FROM numbers(10);
SELECT 'uniqExact variadic String+IPv6', uniqExact(number::String, toIPv6(concat('2001:db8::', hex(number)))) FROM numbers(10);

-- uniqHLL12 variadic arguments
SELECT 'uniqHLL12 variadic String+UInt', uniqHLL12(number, number::String) FROM numbers(10);
SELECT 'uniqHLL12 variadic FixedString+UInt', uniqHLL12(number, number::String::FixedString(4)) FROM numbers(10);
SELECT 'uniqHLL12 variadic String+IPv6', uniqHLL12(number::String, toIPv6(concat('2001:db8::', hex(number)))) FROM numbers(10);

-- Variadic with duplicates
SELECT 'uniq variadic with duplicates', uniq(x, y) FROM (
    SELECT (number % 5)::String as x, (number % 3)::String::FixedString(2) as y FROM numbers(100)
);

SELECT 'uniqExact variadic with duplicates', uniqExact(x, y) FROM (
    SELECT (number % 5)::String as x, (number % 3)::String::FixedString(2) as y FROM numbers(100)
);

-- Three arguments
SELECT 'uniq 3 args', uniq(number, number::String, number::String::FixedString(4)) FROM numbers(10);
SELECT 'uniqExact 3 args', uniqExact(number, number::String, number::String::FixedString(4)) FROM numbers(10);
