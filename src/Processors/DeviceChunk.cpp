#include <Processors/DeviceChunk.h>

#include <Columns/ColumnConst.h>
#include <Core/Block.h>
#include <Processors/IProcessor.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

namespace ProfileEvents
{
    extern const Event DeviceChunksMaterialized;
    extern const Event DeviceChunksMaterializedMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void setDeviceColumns(Chunk & chunk, DeviceColumnsPtr columns, const Block & header)
{
    const size_t rows = columns->rows();
    Columns placeholders;
    placeholders.reserve(header.columns());
    for (const auto & column : header)
        placeholders.push_back(column.type->createColumnConstWithDefaultValue(rows));

    chunk.setColumns(std::move(placeholders), rows);
    if (!chunk.getChunkInfos().empty())
        chunk.getChunkInfos().extract<DeviceChunkInfo>();
    chunk.getChunkInfos().add(std::make_shared<DeviceChunkInfo>(std::move(columns)));
}

Chunk makeDeviceChunk(DeviceColumnsPtr columns, const Block & header)
{
    Chunk chunk;
    setDeviceColumns(chunk, std::move(columns), header);
    return chunk;
}

DeviceColumnsPtr deviceColumnsOf(const Chunk & chunk)
{
    if (chunk.getChunkInfos().empty())
        return nullptr;
    const auto info = chunk.getChunkInfos().get<DeviceChunkInfo>();
    return info ? info->columns : nullptr;
}

void materializeDeviceChunk(Chunk & chunk, const Block & header)
{
    if (chunk.getChunkInfos().empty())
        return;
    const auto info = chunk.getChunkInfos().extract<DeviceChunkInfo>();
    if (!info)
        return;

    ProfileEvents::increment(ProfileEvents::DeviceChunksMaterialized);
    Stopwatch watch;
    Columns columns = info->columns->materialize(header);
    ProfileEvents::increment(ProfileEvents::DeviceChunksMaterializedMicroseconds, watch.elapsedMicroseconds());
    if (columns.size() != header.columns())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "{} columns materialized from a device chunk of a header of {}", columns.size(), header.columns());
    const size_t rows = info->columns->rows();
    chunk.setColumns(std::move(columns), rows);
}

void materializeDeviceChunkFor(Chunk & chunk, const Block & header, const IProcessor * processor)
{
    if (processor && processor->acceptsDeviceChunks())
        return;
    materializeDeviceChunk(chunk, header);
}

}
