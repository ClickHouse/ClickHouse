#pragma once

#include "Decimal.h"
#include "UUID.h"

namespace DB
{
struct Array;
struct Tuple;
struct Map;
struct AggregateFunctionStateData;

/**
 * Obtain type string representation from real type if possible.
 * @example TypeName<UInt8> == "UInt8"
 */
template <class T> constexpr inline std::string_view TypeName;

#define TN_MAP(_A) \
    template <> constexpr inline std::string_view TypeName<_A> = #_A;

TN_MAP(UInt8)
TN_MAP(UInt16)
TN_MAP(UInt32)
TN_MAP(UInt64)
TN_MAP(UInt128)
TN_MAP(UInt256)
TN_MAP(Int8)
TN_MAP(Int16)
TN_MAP(Int32)
TN_MAP(Int64)
TN_MAP(Int128)
TN_MAP(Int256)
TN_MAP(Float32)
TN_MAP(Float64)
TN_MAP(String)
TN_MAP(UUID)
TN_MAP(Decimal32)
TN_MAP(Decimal64)
TN_MAP(Decimal128)
TN_MAP(Decimal256)
TN_MAP(DateTime64)
TN_MAP(Array)
TN_MAP(Tuple)
TN_MAP(Map)

/// Special case
template <> constexpr inline std::string_view TypeName<AggregateFunctionStateData> = "AggregateFunctionState";

#undef TN_MAP
}
