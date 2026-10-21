#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUTypes.cuh>

#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>

#include <optional>
#include <span>

namespace DB::GPU
{

std::optional<GPUElementType> columnTypeOf(const IDataType & type);

GPUElementType columnTypeOrThrow(const IDataType & type);

/// What a column is sent to the device as: of `columnTypeOf`, and a `FixedString` key as an integer as wide, see
/// `keyElementTypeOf`.
GPUElementType uploadTypeOrThrow(const IDataType & type);

const DeviceFixedColumn & fixedOrThrow(const DeviceColumnView & view);

void copyDeviceToHost(std::span<const DeviceColumnView> from, std::span<IColumn * const> to, cuda::stream_ref stream);

}

#endif
