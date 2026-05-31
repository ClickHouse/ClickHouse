#pragma once

#include <Processors/Chunk.h>

#include <Core/Field.h>
#include <Core/Block.h>

namespace DB
{

struct WatermarkMarker : public ChunkInfoCloneable<WatermarkMarker>
{
    Field watermark;
    explicit WatermarkMarker(Field watermark_) : watermark(std::move(watermark_)) {}
};

inline Chunk makeWatermarkMarkerChunk(const Block & header, Field watermark)
{
    Chunk chunk(header.cloneEmptyColumns(), 0);
    chunk.getChunkInfos().add(std::make_shared<WatermarkMarker>(std::move(watermark)));
    return chunk;
}

}
