#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

/** Query plan step for ORDER BY col DEPENDS ON dep_col.
  * Merges all streams into one and applies Kahn's BFS topological sort.
  */
class TopologicalSortStep : public ITransformingStep
{
public:
    TopologicalSortStep(
        const SharedHeader & input_header,
        const String & key_column_name,
        const String & deps_column_name);

    String getName() const override { return "TopologicalSort"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

private:
    void updateOutputHeader() override { output_header = input_headers.front(); }

    String key_column_name;
    String deps_column_name;
};

}
