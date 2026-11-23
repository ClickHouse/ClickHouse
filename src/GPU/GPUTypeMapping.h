#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUTypes.cuh>

#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>

#include <optional>
#include <string_view>
#include <vector>

namespace DB
{
class ICompressionCodec;
}

namespace DB::GPU
{

std::optional<GPUElementType> elementTypeOf(const IDataType & type);

GPUElementType elementTypeOrThrow(const IDataType & type);

/// The element type a column of a predicate on the device is compared as: that of `elementTypeOf`, and the integers that
/// `Date`, `Date32` and `DateTime` are stored as, which compare in the order of their values.
std::optional<GPUElementType> filterElementTypeOf(const IDataType & type);

std::vector<GPUElementType> filterElementTypesOrThrow(const DataTypes & types);

std::vector<GPUElementType> elementTypesOrThrow(const DataTypes & types);

std::vector<size_t> elementSizesOf(const std::vector<GPUElementType> & element_types);

std::optional<GPUCodec> codecOf(UInt8 method_byte);

/// The codec the device expands the blocks of `codec` with, of those of `GPUCodec`. Nothing for any other.
std::optional<GPUCodec> deviceCodecOf(const ICompressionCodec & codec);

GPUElementType sumResultTypeFor(GPUElementType element_type);

std::optional<GPUAggregationKind> aggregationOf(const String & aggregate_function_name);

String aggregationName(GPUAggregationKind aggregation);

bool isStringKey(const IDataType & type);

/// Also a `FixedString` of 1, 2, 4 or 8 bytes, grouped by its bytes as an integer as wide.
std::optional<GPUElementType> keyElementTypeOf(const IDataType & type);

std::vector<GPUElementType> groupByKeysOrThrow(const DataTypes & key_types);

/// What the rows of a column that the device collects, rather than groups, are carried as: a `String` as one, a
/// `FixedString` as `keyElementTypeOf`, and any other as `filterElementTypeOf`, the bytes of its values as they are.
std::optional<GPUElementType> collectElementTypeOf(const IDataType & type);

bool canAggregateOnDevice(
    const DataTypes & key_types,
    const DataTypes & argument_types,
    const DataTypes & result_types,
    const std::vector<GPUAggregationKind> & aggregations);

std::string_view rawValuesOf(const IColumn & column, size_t num_rows, size_t element_size);

HostColumnView resizeForElementType(IColumn & column, size_t num_rows, GPUElementType element_type);

}

#endif
