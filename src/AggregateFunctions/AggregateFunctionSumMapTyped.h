#pragma once

#include <AggregateFunctions/IAggregateFunction_fwd.h>
#include <Core/Field.h>
#include <Core/TypeId.h>
#include <DataTypes/IDataType_fwd.h>


/// The typed fast path of sumMap, minMap and maxMap, for numeric, decimal and string keys and numeric values.
/// Implemented in `AggregateFunctionSumMapTyped.cpp`; the generic path is in `AggregateFunctionSumMap.cpp`.

namespace DB
{

/// Minimum protocol revision that supports version 1 serialization format
/// (Decimal32/64 widened to Decimal128). Shared between the generic and typed paths.
inline constexpr auto SUMMAP_STATE_VERSION_1_MIN_REVISION = 54452;

enum class SumMapOp { Sum, Min, Max };

/// Compute the result DataType for the sumMap/minMap/maxMap family.
/// Shared between the generic (Field-based) and typed fast paths.
DataTypePtr createSumMapResultType(const DataTypePtr & keys_type, const DataTypes & values_types, bool overflow);

/// Helper to check if a key type is supported by the typed path
bool isSumMapTypedKeySupported(TypeIndex idx);

/// Helper to check if all value types are supported by the typed path
bool areSumMapTypedValuesSupported(const DataTypes & values_types);

/// Create a typed sumMap aggregate function, dispatching on key type.
/// Returns nullptr if the key type is not supported.
/// When keys_filter is non-empty, the function only accumulates keys present in the filter.
AggregateFunctionPtr createSumMapTyped(
    const DataTypePtr & keys_type,
    const DataTypes & values_types,
    const DataTypes & argument_types,
    bool overflow,
    bool tuple_argument,
    SumMapOp op,
    const Array & keys_filter = {});

}
