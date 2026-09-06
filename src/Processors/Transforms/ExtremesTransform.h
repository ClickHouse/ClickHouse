#pragma once
#include <Processors/ISimpleTransform.h>

namespace DB
{

/// Merges the per-column min/max of `chunk` into `extremes_columns`, which is either empty (nothing
/// accumulated yet) or fully populated with size-2 columns at every index.
void accumulateExtremes(MutableColumns & extremes_columns, const Chunk & chunk);

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

