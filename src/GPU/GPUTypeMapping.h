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

std::vector<GPUElementType> elementTypesOrThrow(const DataTypes & types);

std::vector<size_t> elementSizesOf(const std::vector<GPUElementType> & element_types);

GPUElementType sumResultTypeFor(GPUElementType element_type);

std::optional<GPUCodec> codecOf(UInt8 method_byte);

std::optional<GPUAggregationKind> aggregationOf(const String & aggregate_function_name);

String aggregationName(GPUAggregationKind aggregation);

bool canReduceOnDevice(const IDataType & argument_type, const IDataType & result_type, GPUAggregationKind aggregation);

GPUElementType reducibleElementTypeOrThrow(const IDataType & argument_type, const IDataType & result_type, GPUAggregationKind aggregation);

/// Whether a `GROUP BY` on the device reduces a column of `argument_type`, or no column for a `count`, into `result_type`.
bool canGroupByOnDevice(const IDataType * argument_type, const IDataType & result_type, GPUAggregationKind aggregation);

GPUElementType groupByResultTypeOrThrow(const IDataType * argument_type, const IDataType & result_type, GPUAggregationKind aggregation);

bool isStringKey(const IDataType & type);

/// The element type a key of a `GROUP BY` is grouped as: that of `elementTypeOf`, and the integer as wide as a
/// `FixedString` of 1, 2, 4 or 8 bytes, whose bytes are grouped by as they are.
std::optional<GPUElementType> keyElementTypeOf(const IDataType & type);

std::vector<GPUElementType> groupByKeysOrThrow(const DataTypes & key_types);

bool canGroupByReduceOnDevice(
    const DataTypes & key_types,
    const DataTypes & argument_types,
    const DataTypes & result_types,
    const std::vector<GPUAggregationKind> & aggregations);

std::string_view rawValuesOf(const IColumn & column, size_t num_rows, size_t element_size);

HostColumnView resizeForElementType(IColumn & column, size_t num_rows, GPUElementType element_type);

}

#endif
