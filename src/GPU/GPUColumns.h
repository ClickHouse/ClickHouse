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

/// The type of a column of `type` as the device takes it, if it takes it at all.
std::optional<GPUElementType> columnTypeOf(const IDataType & type);

GPUElementType columnTypeOrThrow(const IDataType & type);

/// The fixed-width values of `view`, having checked that it is of them and has no nulls.
const DeviceFixedColumn & fixedOrThrow(const DeviceColumnView & view);

/** Copies each column of `from` into the empty host column at the same position in `to`, on
  * `stream`, and returns once all of them are filled. The copies are queued together and the
  * stream is waited for once.
  */
void copyDeviceToHost(std::span<const DeviceColumnView> from, std::span<IColumn * const> to, rmm::cuda_stream_view stream);

}

#endif
