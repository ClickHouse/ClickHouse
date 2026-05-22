#pragma once

#include <Processors/Chunk.h>

#include <Core/Field.h>

namespace DB
{

struct WatermarkMarker : public ChunkInfoCloneable<WatermarkMarker>
{
    Field watermark;
    explicit WatermarkMarker(Field watermark_) : watermark(std::move(watermark_)) {}
};

}
