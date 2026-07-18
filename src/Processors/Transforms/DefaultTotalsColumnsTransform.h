#pragma once

#include <Processors/ISimpleTransform.h>

namespace DB
{

/// On the WITH TOTALS stream only, overwrites the given output column positions with their column type
/// default. Used to correct the grand-total row after an injective GROUP BY key f(g) was unwrapped to g:
/// the totals row fills g with its type default, so the projection recomputes f(default) instead of the
/// required defaultOf(typeOf(f(g))). On the main stream this transform is an identity pass-through.
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
