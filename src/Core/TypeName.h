#pragma once

#include <Core/Types.h>

namespace DB
{
struct Array;
struct Tuple;
struct Map;
struct AggregateFunctionStateData;

template <class T> constexpr const char * TypeName = "";

template <> inline constexpr const char * TypeName<UInt8> = "UInt8";
template <> inline constexpr const char * TypeName<UInt16> = "UInt16";
template <> inline constexpr const char * TypeName<UInt32> = "UInt32";
template <> inline constexpr const char * TypeName<UInt64> = "UInt64";
template <> inline constexpr const char * TypeName<UInt128> = "UInt128";
template <> inline constexpr const char * TypeName<UInt256> = "UInt256";
template <> inline constexpr const char * TypeName<Int8> = "Int8";
template <> inline constexpr const char * TypeName<Int16> = "Int16";
template <> inline constexpr const char * TypeName<Int32> = "Int32";
template <> inline constexpr const char * TypeName<Int64> = "Int64";
template <> inline constexpr const char * TypeName<Int128> = "Int128";
template <> inline constexpr const char * TypeName<Int256> = "Int256";
template <> inline constexpr const char * TypeName<Float32> = "Float32";
template <> inline constexpr const char * TypeName<Float64> = "Float64";
template <> inline constexpr const char * TypeName<String> = "String";
template <> inline constexpr const char * TypeName<UUID> = "UUID";
template <> inline constexpr const char * TypeName<Decimal32> = "Decimal32";
template <> inline constexpr const char * TypeName<Decimal64> = "Decimal64";
template <> inline constexpr const char * TypeName<Decimal128> = "Decimal128";
template <> inline constexpr const char * TypeName<Decimal256> = "Decimal256";
template <> inline constexpr const char * TypeName<DateTime64> = "DateTime64";
template <> inline constexpr const char * TypeName<Array> = "Array";
template <> inline constexpr const char * TypeName<Tuple> = "Tuple";
template <> inline constexpr const char * TypeName<Map> = "Map";
template <> inline constexpr const char * TypeName<AggregateFunctionStateData> = "AggregateFunctionState";
}
