#pragma once

#include <Processors/ISimpleTransform.h>

namespace DB
{

class FutureSet;
using FutureSetPtr = std::shared_ptr<FutureSet>;
class QueryPipelineBuilder;

/// Passes chunks through. When its output is first needed and one of `sets` is built and empty,
/// finishes without reading its input, which stops everything above it.
class EmptySetShortCircuitTransform final : public ISimpleTransform
{
public:
    EmptySetShortCircuitTransform(SharedHeader header_, std::vector<FutureSetPtr> sets_);

    String getName() const override { return "EmptySetShortCircuitTransform"; }

    Status prepare() override;

protected:
    void transform(Chunk &) override {}

private:
    std::vector<FutureSetPtr> sets;
    bool checked = false;
};

/// Adds `EmptySetShortCircuitTransform` to every main stream of `pipeline` (totals and extremes are not gated).
void addEmptySetShortCircuit(QueryPipelineBuilder & pipeline, const std::vector<FutureSetPtr> & sets);

}
