#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUTypes.h>

#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>

#include <optional>
#include <string_view>
#include <vector>

namespace DB::GPU
{

/// How a ClickHouse type looks to the device, when it looks like anything at all: a non-nullable
/// fixed-width numeric column is a run of values the device can read, and nothing else is.
std::optional<GPUElementType> elementTypeOf(const IDataType & type);

GPUElementType elementTypeOrThrow(const IDataType & type);

std::vector<GPUElementType> elementTypesOrThrow(const DataTypes & types);

std::vector<size_t> elementSizesOf(const std::vector<GPUElementType> & element_types);

/// What a `sum` over this column accumulates in: the device widens to eight bytes, as ClickHouse does.
GPUElementType sumResultTypeFor(GPUElementType element_type);

std::optional<GPUCodec> codecOf(UInt8 method_byte);

std::optional<GPUAggregationKind> aggregationOf(const String & aggregate_function_name);

String aggregationName(GPUAggregationKind aggregation);

bool canReduceOnDevice(const IDataType & argument_type, const IDataType & result_type, GPUAggregationKind aggregation);

GPUElementType reducibleElementTypeOrThrow(const IDataType & argument_type, const IDataType & result_type, GPUAggregationKind aggregation);

bool canGroupByReduceOnDevice(
    const DataTypes & key_types,
    const DataTypes & argument_types,
    const DataTypes & result_types,
    const std::vector<GPUAggregationKind> & aggregations);

/// The column's values as the device will read them, having checked that they are laid out as the
/// row count and element size say.
std::string_view rawValuesOf(const IColumn & column, size_t num_rows, size_t element_size);

/// Makes room in an empty column for `num_rows` values and answers where the device should write them.
HostColumnView resizeForElementType(IColumn & column, size_t num_rows, GPUElementType element_type);

}

#endif
