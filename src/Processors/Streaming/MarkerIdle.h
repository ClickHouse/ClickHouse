#pragma once

#include <Processors/Chunk.h>

#include <Core/Block.h>

namespace DB
{

struct IdleMarker : public ChunkInfoCloneable<IdleMarker>
{
};

inline Chunk makeIdleMarkerChunk(const Block & header)
{
    Chunk chunk(header.cloneEmptyColumns(), 0);
    chunk.getChunkInfos().add(std::make_shared<IdleMarker>());
    return chunk;
}

}
