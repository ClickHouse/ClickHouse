#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

/// Implements LIMIT INRANGE operation. See LimitInRangeTransform.
class LimitInRangeStep : public ITransformingStep
{
public:
    LimitInRangeStep(
        const DataStream & input_stream_, String from_filter_column_name_, String to_filter_column_name_, bool remove_filter_column_);

    String getName() const override { return "Limit InRange"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    const String & getFromFilterColumnName() const { return from_filter_column_name; }
    const String & getToFilterColumnName() const { return to_filter_column_name; }
    bool removesFilterColumn() const { return remove_filter_column; }

private:
    void updateOutputStream() override;

    String from_filter_column_name;
    String to_filter_column_name;
    bool remove_filter_column;
};

}
