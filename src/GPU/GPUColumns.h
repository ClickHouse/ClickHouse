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

const DeviceFixedColumn & fixedOrThrow(const DeviceColumnView & view);

void copyDeviceToHost(std::span<const DeviceColumnView> from, std::span<IColumn * const> to, rmm::cuda_stream_view stream);

}

#endif
