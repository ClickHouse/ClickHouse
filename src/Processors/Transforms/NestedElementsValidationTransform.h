#pragma once

#include <Processors/ISimpleTransform.h>

namespace DB
{

class NestedElementsValidationTransform : public ISimpleTransform
{
public:
    explicit NestedElementsValidationTransform(const Block & header);

    String getName() const override { return "NestedElementsValidationTransform"; }

protected:
    void transform(Chunk & chunk) override;
};

}

