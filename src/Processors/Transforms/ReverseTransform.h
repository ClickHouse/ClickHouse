#pragma once
#include <Processors/ISimpleTransform.h>

namespace DB
{

/// Reverse rows in chunk.
class ReverseTransform final : public ISimpleTransform
{
public:
    explicit ReverseTransform(SharedHeader header) : ISimpleTransform(header, header, false) {}
    String getName() const override { return "ReverseTransform"; }

protected:
    void transform(Chunk & chunk) override;
};

}
