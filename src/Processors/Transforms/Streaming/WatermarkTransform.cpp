#include <Processors/Transforms/Streaming/WatermarkTransform.h>


namespace DB
{
namespace ErrorCodes
{
extern const int INVALID_EMIT_MODE;
}

namespace Streaming
{
namespace
{
WatermarkStamperPtr initWatermark(const WatermarkStamperParams & params, Poco::Logger * log)
{
    assert(params.mode != WatermarkStamperParams::EmitMode::NONE);
    return std::make_unique<WatermarkStamper>(params, log);
}
}

WatermarkTransform::WatermarkTransform(const Block & header, WatermarkStamperParamsPtr params_, Poco::Logger * log)
    : ISimpleTransform(header, header, false), params(std::move(params_))
{
    watermark = initWatermark(*params, log);
    assert(watermark);
    watermark->preProcess(header);
}

void WatermarkTransform::transform(Chunk & chunk)
{
    chunk.clearWatermark();
    if (!chunk.avoidWatermark())
        watermark->process(chunk);
}

}
}
