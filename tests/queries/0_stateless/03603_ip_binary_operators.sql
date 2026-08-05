-- https://github.com/ClickHouse/ClickHouse/issues/71415
SELECT now() + CAST(toFixedString(materialize(toNullable('1')), 1), 'IPv6'); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT CAST('2000-01-01', 'Date32') - CAST(0, 'IPv4');  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 IPv4) ENGINE = Memory;
SELECT (t0.c0, t0.c0) * t0.c0 FROM t0; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
DROP TABLE IF EXISTS t0;

SELECT materialize('1')::IPv6 + '2000-01-01 00:00:00'::DateTime;  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

-- https://github.com/ClickHouse/ClickHouse/issues/83963
CREATE TABLE `02763_alias__fuzz_25` (`x` DateTime64(3), `y` IPv4, `z` Float32 ALIAS x + y) ENGINE = MergeTree ORDER BY x; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

-- Other non sensical operations
SELECT toIPv4('127.0.0.1') + 0.1; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT 0.1 + toIPv4('127.0.0.1'); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT toIPv6('127.0.0.1') + 0.1; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT 0.1 + toIPv6('127.0.0.1'); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

SELECT toIPv4('127.0.0.1') + 0.1::Float32; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT toIPv6('127.0.0.1') + 0.1::Float32; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT toIPv4('127.0.0.1') + 0.1::BFloat16; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT toIPv6('127.0.0.1') + 0.1::BFloat16; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

SELECT toIPv4('127.0.0.1') + INTERVAL 1 SECOND; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT toIPv6('127.0.0.1') + INTERVAL 1 SECOND; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

SELECT toIPv4('127.0.0.1') + toDecimal32(0.1, 2); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT toIPv6('127.0.0.1') + toDecimal32(0.1, 2); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
