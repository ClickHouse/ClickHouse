#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

/// Executes LIMIT BY for specified columns. See LimitByTransform.
class LimitByStep : public ITransformingStep
{
public:
    explicit LimitByStep(
            const DataStream & input_stream_,
            size_t group_length_, size_t group_offset_, const Names & columns_);

    String getName() const override { return "LimitBy"; }

    void transformPipeline(QueryPipeline & pipeline) override;

    void describeActions(FormatSettings & settings) const override;

private:
    size_t group_length;
    size_t group_offset;
    Names columns;
};

}
