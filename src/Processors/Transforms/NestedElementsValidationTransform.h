#pragma once

#include <Processors/ISimpleTransform.h>

namespace DB
{

class NestedElementsValidationTransform : public ISimpleTransform
{
public:
    explicit NestedElementsValidationTransform(SharedHeader header);

    String getName() const override { return "NestedElementsValidationTransform"; }

protected:
    void transform(Chunk & chunk) override;
};

}

