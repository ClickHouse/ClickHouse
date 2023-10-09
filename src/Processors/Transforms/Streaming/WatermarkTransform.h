#pragma once

#include <Processors/Transforms/Streaming/WatermarkStamper.h>

#include <Processors/ISimpleTransform.h>

namespace DB
{
/**
 * WatermarkTransform projects watermark according to watermark strategies
 * by observing the events in its input.
 */

namespace Streaming
{
class WatermarkTransform final : public ISimpleTransform
{
public:
    WatermarkTransform(const Block & header, WatermarkStamperParamsPtr params_, Poco::Logger * log);

    ~WatermarkTransform() override = default;

    String getName() const override { return watermark->getName() + "Transform"; }

    // void checkpoint(CheckpointContextPtr ckpt_ctx) override;
    // void recover(CheckpointContextPtr ckpt_ctx) override;

private:
    void transform(Chunk & chunk) override;

private:
    WatermarkStamperParamsPtr params;
    WatermarkStamperPtr watermark;
};
}
}
