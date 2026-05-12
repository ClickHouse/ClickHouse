#pragma once

#include <optional>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

/** Implements SELECT ... SHUFFLE.
  *
  * Without a limit: merges all streams into one and applies a global
  * Fisher-Yates shuffle, returning every row in uniformly random order.
  *
  * With a limit (PR 3): uses reservoir sampling (Algorithm L) to return
  * at most limit rows in O(n) time and O(limit) space.
  */
class ShuffleStep : public ITransformingStep
{
public:
    explicit ShuffleStep(const SharedHeader & input_header, std::optional<size_t> limit_ = std::nullopt);

    String getName() const override { return "Shuffle"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

private:
    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }

    std::optional<size_t> limit;
};

}
