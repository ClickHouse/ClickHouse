#pragma once
#include <Processors/ISimpleTransform.h>

namespace DB
{

/// Converts columns-constants to full columns ("materializes" them).
class MaterializingTransform : public ISimpleTransform
{
public:
    explicit MaterializingTransform(const Block & header, bool remove_sparse_ = true);

    String getName() const override { return "MaterializingTransform"; }

protected:
    void transform(Chunk & chunk) override;
    bool remove_sparse;
};

}
