#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

/// Executes LIMIT BY for specified columns. See LimitByTransform.
class LimitByStep : public ITransformingStep
{
public:
    explicit LimitByStep(
            const Header & input_header_,
            size_t group_length_, size_t group_offset_, const Names & columns_);

    String getName() const override { return "LimitBy"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

private:
    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }

    size_t group_length;
    size_t group_offset;
    Names columns;
};

}
