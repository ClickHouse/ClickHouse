#pragma once

#include <Processors/IInflatingTransform.h>

#include <Core/Field.h>

#include <optional>

namespace DB
{

/// Aggregates the watermark markers of a stream with a running maximum so the emitted watermarks never regress.
class RaiseWatermarksTransform final : public IInflatingTransform
{
public:
    RaiseWatermarksTransform(SharedHeader header, Field initial_watermark_);

    String getName() const override { return "RaiseWatermarks"; }

protected:
    void consume(Chunk chunk) override;
    bool canGenerate() override;
    Chunk generate() override;
    Chunk getRemaining() override;

private:
    Field watermark;
    std::optional<Chunk> pending_chunk;
};

}
