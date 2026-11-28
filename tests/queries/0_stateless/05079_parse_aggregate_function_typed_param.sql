-- Coverage for src/Parsers/parseFieldFromCastedLiteral.cpp.
-- When ClickHouse parses an AggregateFunction type specification, each parameter
-- of the aggregate function is processed by parseFieldFromCastedLiteral, which
-- handles explicit type casts like `0.5::Decimal32(1)` or `1::Int64`.
-- This file covers every type dispatch branch (Decimal32/128/256, Bool, Float64,
-- String, Int64, UInt64, Int128, UInt128, Int256, UInt256, UUID, IPv4, IPv6)
-- plus the unsupported-type and non-literal error paths.
-- The round-trip type name (shown by system.columns) confirms each path was taken.

-- Decimal32 (line 74): float literal cast to Decimal32; hits the else-branch in
-- convertFieldTo<Decimal32> (non-String, non-Decimal source converted via text).
DROP TABLE IF EXISTS t_pfl_05079;
CREATE TABLE t_pfl_05079 (s AggregateFunction(quantile(0.5::Decimal32(1)), Float64)) ENGINE = Memory;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't_pfl_05079';
DROP TABLE t_pfl_05079;

-- Decimal128 (line 78)
CREATE TABLE t_pfl_05079 (s AggregateFunction(quantile(0.5::Decimal128(1)), Float64)) ENGINE = Memory;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't_pfl_05079';
DROP TABLE t_pfl_05079;

-- Decimal256 (line 80)
CREATE TABLE t_pfl_05079 (s AggregateFunction(quantile(0.5::Decimal256(1)), Float64)) ENGINE = Memory;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't_pfl_05079';
DROP TABLE t_pfl_05079;

-- Bool (line 83): UInt64 literal cast to Bool; hits the else-branch in convertFieldTo<bool>
-- (not already Bool, not String -> convert via FieldVisitorToString).
CREATE TABLE t_pfl_05079 (s AggregateFunction(quantile(0::Bool), Float64)) ENGINE = Memory;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't_pfl_05079';
DROP TABLE t_pfl_05079;

-- Float64 (line 85): Float64 literal cast to Float64; hits the same-type branch in
-- convertFieldTo<Float64> (src is already Float64, returned unchanged).
CREATE TABLE t_pfl_05079 (s AggregateFunction(quantile(0.5::Float64), Float64)) ENGINE = Memory;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't_pfl_05079';
DROP TABLE t_pfl_05079;

-- String (line 87): quantile does not accept a String parameter.
CREATE TABLE t_pfl_05079 (s AggregateFunction(quantile('0.5'::String), Float64))
ENGINE = Memory; -- { serverError CANNOT_CONVERT_TYPE }

-- Int64 (line 90): UInt64 literal cast to Int64; hits the else-branch in convertFieldTo<Int64>.
CREATE TABLE t_pfl_05079 (s AggregateFunction(quantile(1::Int64), Float64)) ENGINE = Memory;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't_pfl_05079';
DROP TABLE t_pfl_05079;

-- UInt64 (line 92): UInt64 literal cast to UInt64; hits the same-type branch.
CREATE TABLE t_pfl_05079 (s AggregateFunction(quantile(1::UInt64), Float64)) ENGINE = Memory;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't_pfl_05079';
DROP TABLE t_pfl_05079;

-- Int128 (line 94)
CREATE TABLE t_pfl_05079 (s AggregateFunction(quantile(1::Int128), Float64)) ENGINE = Memory;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't_pfl_05079';
DROP TABLE t_pfl_05079;

-- UInt128 (line 96)
CREATE TABLE t_pfl_05079 (s AggregateFunction(quantile(1::UInt128), Float64)) ENGINE = Memory;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't_pfl_05079';
DROP TABLE t_pfl_05079;

-- Int256 (line 98)
CREATE TABLE t_pfl_05079 (s AggregateFunction(quantile(1::Int256), Float64)) ENGINE = Memory;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't_pfl_05079';
DROP TABLE t_pfl_05079;

-- UInt256 (line 100)
CREATE TABLE t_pfl_05079 (s AggregateFunction(quantile(1::UInt256), Float64)) ENGINE = Memory;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't_pfl_05079';
DROP TABLE t_pfl_05079;

-- UUID (line 103): String literal cast to UUID; hits the String-branch in convertFieldTo<UUID>.
CREATE TABLE t_pfl_05079
(s AggregateFunction(quantile('00000000-0000-0000-0000-000000000000'::UUID), Float64))
ENGINE = Memory;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't_pfl_05079';
DROP TABLE t_pfl_05079;

-- IPv4 (line 105): String literal cast to IPv4.
CREATE TABLE t_pfl_05079 (s AggregateFunction(quantile('0.0.0.0'::IPv4), Float64)) ENGINE = Memory;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't_pfl_05079';
DROP TABLE t_pfl_05079;

-- IPv6 (line 107): String literal cast to IPv6.
CREATE TABLE t_pfl_05079 (s AggregateFunction(quantile('::0'::IPv6), Float64)) ENGINE = Memory;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't_pfl_05079';
DROP TABLE t_pfl_05079;

-- Unsupported type name (line 110): throws BAD_ARGUMENTS.
CREATE TABLE t_pfl_05079 (s AggregateFunction(quantile(0.5::BadType), Float64))
ENGINE = Memory; -- { serverError BAD_ARGUMENTS }

-- Non-literal parameter (lines 130-131): an identifier (not a literal or CAST expression)
-- triggers the "Expected a literal or a CAST of a literal" error.
CREATE TABLE t_pfl_05079 (s AggregateFunction(quantile(someColumn), Float64))
ENGINE = Memory; -- { serverError BAD_ARGUMENTS }
