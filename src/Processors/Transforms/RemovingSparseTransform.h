#pragma once
#include <Processors/ISimpleTransform.h>

namespace DB
{

class RemovingSparseTransform final : public ISimpleTransform
{
public:
    explicit RemovingSparseTransform(SharedHeader header);

    String getName() const override { return "RemovingSparseTransform"; }

protected:
    void transform(Chunk & chunk) override;
};

}
