#include <Processors/Streaming/Markers.h>

namespace DB
{

Chunk IdleMarker::create(const Block & header)
{
    Chunk chunk(header.cloneEmptyColumns(), 0);
    chunk.getChunkInfos().add(std::make_shared<IdleMarker>());
    return chunk;
}

Chunk WatermarkMarker::create(const Block & header, Field watermark_)
{
    auto marker = std::make_shared<WatermarkMarker>();
    marker->watermark = std::move(watermark_);

    Chunk chunk(header.cloneEmptyColumns(), 0);
    chunk.getChunkInfos().add(std::move(marker));
    return chunk;
}

}
