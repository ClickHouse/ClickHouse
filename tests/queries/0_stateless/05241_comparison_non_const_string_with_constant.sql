-- A non-constant `String` compared with a constant without a common type is rejected when the
-- comparison is resolved, so the error does not depend on whether an optimization removes it.
-- https://github.com/ClickHouse/ClickHouse/issues/121006

DROP TABLE IF EXISTS t_uuid;
CREATE TABLE t_uuid (c1 UUID) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_uuid SELECT reinterpretAsUUID(number) FROM numbers(10);

SELECT '-- rejected at every chain length';
SELECT count() FROM numbers(3) WHERE toString(number) != 1; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM numbers(3) WHERE toString(number) != 1 AND toString(number) != 2; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM numbers(3) WHERE toString(number) != 1 AND toString(number) != 2 AND toString(number) != 3; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM numbers(3) WHERE toString(number) = 1 OR toString(number) = 2 OR toString(number) = 3; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM numbers(3) WHERE 1 = toString(number); -- { serverError NO_COMMON_TYPE }
SELECT count() FROM numbers(3) WHERE toString(number) > 1 AND toString(number) > 2; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM numbers(3) WHERE toLowCardinality(toString(number)) = 1; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM numbers(3) WHERE toNullable(toString(number)) = 1; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM numbers(3) WHERE toFixedString(toString(number), 1) = 1; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM numbers(3) WHERE toString(number) = toDate('2020-01-01'); -- { serverError NO_COMMON_TYPE }
SELECT count() FROM numbers(3) WHERE toString(number) = [1]; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM numbers(3) WHERE toString(number) != 1 AND toString(number) != 2 AND toString(number) != 3 SETTINGS enable_analyzer = 0; -- { serverError NO_COMMON_TYPE }

SELECT toString(c1) AS c1, count() AS n FROM t_uuid
WHERE (c1 != toUUID('00000000-0000-0000-0000-000000000001'))
  AND (c1 != toUUID('00000000-0000-0000-0000-000000000002'))
  AND (c1 != toUUID('00000000-0000-0000-0000-000000000003'))
GROUP BY c1; -- { serverError NO_COMMON_TYPE }

-- The filter is constant false, so the comparison is never executed, but it is still rejected.
SELECT count() FROM t_uuid WHERE toString(c1) = 1 AND 0; -- { serverError NO_COMMON_TYPE }

SELECT '-- accepted';
SELECT count() FROM numbers(3) WHERE toString(number) != '1' AND toString(number) != '2' AND toString(number) != '3';
SELECT count() FROM numbers(3) WHERE number != '1' AND number != '2' AND number != '3';
SELECT count() FROM numbers(3) WHERE toString(number) = CAST('1', 'Enum8(\'1\' = 1, \'2\' = 2)');
SELECT count() FROM numbers(3) WHERE toFixedString(toString(number), 1) = '1';
SELECT count() FROM numbers(3) WHERE (toString(number), 1) = ('1', 1);
SELECT count() FROM numbers(3) WHERE (number, 1) = ('1', 1);
SELECT '1' = 1, 1 = '1';
SELECT toFixedString(IPv6StringToNum('::1'), 16) = toIPv6('::1') FROM numbers(1);
SELECT count() FROM t_uuid WHERE c1 != reinterpretAsUUID(1) AND c1 != reinterpretAsUUID(2) AND c1 != reinterpretAsUUID(3);
SELECT count() FROM t_uuid WHERE toString(c1) != toString(reinterpretAsUUID(1));

DROP TABLE t_uuid;
