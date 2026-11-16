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

std::string_view rawValuesOf(const IColumn & column, size_t num_rows, size_t element_size);

HostColumnView resizeForElementType(IColumn & column, size_t num_rows, GPUElementType element_type);

}

#endif
