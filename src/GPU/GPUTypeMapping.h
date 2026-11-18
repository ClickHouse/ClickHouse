#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUTypes.cuh>

#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>

#include <optional>
#include <string_view>
#include <vector>

namespace DB::GPU
{

std::optional<GPUElementType> elementTypeOf(const IDataType & type);

GPUElementType elementTypeOrThrow(const IDataType & type);

/// The element type a column of a predicate on the device is compared as: that of `elementTypeOf`, and the integers that
/// `Date`, `Date32` and `DateTime` are stored as, which compare in the order of their values.
std::optional<GPUElementType> filterElementTypeOf(const IDataType & type);

std::optional<GPUCodec> codecOf(UInt8 method_byte);

GPUElementType sumResultTypeFor(GPUElementType element_type);

std::optional<GPUAggregationKind> aggregationOf(const String & aggregate_function_name);

String aggregationName(GPUAggregationKind aggregation);

/// The element type a key of a `GROUP BY` is grouped as: that of `elementTypeOf`, and the integer as wide as a
/// `FixedString` of 1, 2, 4 or 8 bytes, whose bytes are grouped by as they are.
std::optional<GPUElementType> keyElementTypeOf(const IDataType & type);

std::vector<GPUElementType> groupByKeysOrThrow(const DataTypes & key_types);

/// Whether the device aggregates the arguments into the results: without keys by reductions, with keys of `key_types`
/// by a `CudfGroupBy`. A `count` has no argument.
bool canAggregateOnDevice(
    const DataTypes & key_types,
    const DataTypes & argument_types,
    const DataTypes & result_types,
    const std::vector<GPUAggregationKind> & aggregations);

std::string_view rawValuesOf(const IColumn & column, size_t num_rows, size_t element_size);

HostColumnView resizeForElementType(IColumn & column, size_t num_rows, GPUElementType element_type);

}

#endif
