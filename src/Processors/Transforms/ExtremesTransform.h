#pragma once
#include <Processors/ISimpleTransform.h>

namespace DB
{

class ExtremesTransform final : public ISimpleTransform
{

public:
    explicit ExtremesTransform(SharedHeader header);

    String getName() const override { return "ExtremesTransform"; }

    OutputPort & getExtremesPort() { return outputs.back(); }

    Status prepare() override;
    void work() override;

protected:
    void transform(Chunk & chunk) override;

    bool finished_transform = false;
    Chunk extremes;

private:
    MutableColumns extremes_columns;

    /// Whether the column type is comparable: extremes of multiple chunks
    /// can be merged only for comparable types.
    std::vector<UInt8> is_comparable_column;
};

}

