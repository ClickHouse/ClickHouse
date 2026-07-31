#pragma once

#include <Processors/ISimpleTransform.h>

namespace DB
{

/// On the WITH TOTALS stream only, overwrites the given output column positions with their column type
/// default. On the main stream it is an identity pass-through.
/// See OptimizeGroupByInjectiveFunctionsPass and #110715.
class DefaultTotalsColumnsTransform : public ISimpleTransform
{
public:
    DefaultTotalsColumnsTransform(SharedHeader header_, std::vector<size_t> positions_, bool on_totals_);

    String getName() const override { return "DefaultTotalsColumnsTransform"; }

    void transform(Chunk & chunk) override;

private:
    std::vector<size_t> positions;
    bool on_totals;
};

}
