#include <Processors/Streaming/RaiseWatermarksTransform.h>
#include <Processors/Streaming/Markers.h>
#include <Processors/Port.h>

#include <Core/Block.h>

#include <base/defines.h>

#include <algorithm>
#include <utility>

namespace DB
{

RaiseWatermarksTransform::RaiseWatermarksTransform(SharedHeader header, Field initial_watermark_)
    : IInflatingTransform(header, header)
    , watermark(std::move(initial_watermark_))
{
}

void RaiseWatermarksTransform::consume(Chunk chunk)
{
    if (auto marker = chunk.getChunkInfos().extract<WatermarkMarker>())
    {
        watermark = std::max(watermark, marker->watermark);
        marker->watermark = watermark;
        chunk.getChunkInfos().add(std::move(marker));
    }

    pending_chunk = std::move(chunk);
}

bool RaiseWatermarksTransform::canGenerate()
{
    return pending_chunk.has_value();
}

Chunk RaiseWatermarksTransform::generate()
{
    chassert(pending_chunk.has_value());
    return *std::exchange(pending_chunk, std::nullopt);
}

Chunk RaiseWatermarksTransform::getRemaining()
{
    if (watermark.isNull())
        return {};

    return WatermarkMarker::create(getOutputPort().getHeader(), watermark);
}

}
