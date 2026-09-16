#pragma once

#include <Processors/Chunk.h>

#include <Core/Field.h>
#include <Core/Block.h>

namespace DB
{

struct IdleMarker : public ChunkInfoCloneable<IdleMarker>
{
    static Chunk create(const Block & header);
};

struct WatermarkMarker : public ChunkInfoCloneable<WatermarkMarker>
{
    Field watermark;

    static Chunk create(const Block & header, Field watermark_);
};

bool isMarkerChunk(const Chunk & chunk);

}
