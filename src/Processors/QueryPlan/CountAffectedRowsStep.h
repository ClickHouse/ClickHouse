#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/Transforms/CountAffectedRowsTransform.h>


namespace DB
{

/// Counts the rows affected by each of several mutation commands.
class CountAffectedRowsStep : public ITransformingStep
{
public:
    explicit CountAffectedRowsStep(const SharedHeader & input_header_, String command_index_column_name_, AffectedRowsCounterPtr counter_);

    String getName() const override { return "CountAffectedRows"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }

    String command_index_column_name;
    AffectedRowsCounterPtr counter;
};

}
