#pragma once
#include <Processors/ISimpleTransform.h>

namespace DB
{

class RemovingSparseTransform : public ISimpleTransform
{
public:
    explicit RemovingSparseTransform(const Block & header);

    String getName() const override { return "RemovingSparseTransform"; }

protected:
    void transform(Chunk & chunk) override;
};

}
