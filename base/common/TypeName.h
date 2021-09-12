#pragma once

#include "Decimal.h"
#include "UUID.h"

namespace DB
{
struct Array;
struct Tuple;
struct Map;
struct AggregateFunctionStateData;

template <class T> constexpr std::string_view TypeName;

template <> constexpr std::string_view TypeName<UInt8> = "UInt8";
template <> constexpr std::string_view TypeName<UInt16> = "UInt16";
template <> constexpr std::string_view TypeName<UInt32> = "UInt32";
template <> constexpr std::string_view TypeName<UInt64> = "UInt64";
template <> constexpr std::string_view TypeName<UInt128> = "UInt128";
template <> constexpr std::string_view TypeName<UInt256> = "UInt256";
template <> constexpr std::string_view TypeName<Int8> = "Int8";
template <> constexpr std::string_view TypeName<Int16> = "Int16";
template <> constexpr std::string_view TypeName<Int32> = "Int32";
template <> constexpr std::string_view TypeName<Int64> = "Int64";
template <> constexpr std::string_view TypeName<Int128> = "Int128";
template <> constexpr std::string_view TypeName<Int256> = "Int256";
template <> constexpr std::string_view TypeName<Float32> = "Float32";
template <> constexpr std::string_view TypeName<Float64> = "Float64";
template <> constexpr std::string_view TypeName<String> = "String";
template <> constexpr std::string_view TypeName<UUID> = "UUID";
template <> constexpr std::string_view TypeName<Decimal32> = "Decimal32";
template <> constexpr std::string_view TypeName<Decimal64> = "Decimal64";
template <> constexpr std::string_view TypeName<Decimal128> = "Decimal128";
template <> constexpr std::string_view TypeName<Decimal256> = "Decimal256";
template <> constexpr std::string_view TypeName<DateTime64> = "DateTime64";
template <> constexpr std::string_view TypeName<Array> = "Array";
template <> constexpr std::string_view TypeName<Tuple> = "Tuple";
template <> constexpr std::string_view TypeName<Map> = "Map";
template <> constexpr std::string_view TypeName<AggregateFunctionStateData> = "AggregateFunctionState";
}
