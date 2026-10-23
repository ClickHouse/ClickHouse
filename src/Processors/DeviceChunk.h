#pragma once

#include <Processors/Chunk.h>

#include <memory>

namespace DB
{

class Block;
class IProcessor;

/// The columns of a chunk that are on a device, a GPU, rather than in host memory.
class IDeviceColumns
{
public:
    virtual ~IDeviceColumns() = default;

    virtual size_t rows() const = 0;

    /// The columns in host memory, of the types of `header`, in its order.
    virtual Columns materialize(const Block & header) const = 0;
};

using DeviceColumnsPtr = std::shared_ptr<const IDeviceColumns>;

/** Marks a chunk whose columns are on a device: two steps that run there hand their rows over without a copy to host
  * memory and back. The chunk's own columns are constants of the defaults of its header, which keep the checks of the
  * ports on the numbers of columns and rows; the real ones are `columns`. A processor is handed such a chunk only when
  * it takes one, see `IProcessor::acceptsDeviceChunks`; any other is handed the columns materialized in host memory
  * when it pulls the chunk, so that a step that knows nothing of the device reads the rows as it would otherwise.
  */
class DeviceChunkInfo final : public ChunkInfoCloneable<DeviceChunkInfo>
{
public:
    explicit DeviceChunkInfo(DeviceColumnsPtr columns_) : columns(std::move(columns_)) { }
    DeviceChunkInfo(const DeviceChunkInfo & other) = default;

    DeviceColumnsPtr columns;
};

/// A chunk of the `columns` on a device, of the types of `header`.
Chunk makeDeviceChunk(DeviceColumnsPtr columns, const Block & header);

/// Has `chunk` carry the `columns` on a device instead of its own, of the types of `header`, and keeps its other infos.
void setDeviceColumns(Chunk & chunk, DeviceColumnsPtr columns, const Block & header);

/// The device columns a chunk carries, or nothing for a chunk in host memory.
DeviceColumnsPtr deviceColumnsOf(const Chunk & chunk);

/// Of a chunk that carries device columns, puts them in host memory instead, of the types of `header`, and drops the
/// info. Does nothing to a chunk in host memory.
void materializeDeviceChunk(Chunk & chunk, const Block & header);

/// What a port does for `processor` with a chunk it pulls: materializes device columns unless the processor takes them.
void materializeDeviceChunkFor(Chunk & chunk, const Block & header, const IProcessor * processor);

}
