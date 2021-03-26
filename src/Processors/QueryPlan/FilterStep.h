#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

/// Implements WHERE, HAVING operations. See FilterTransform.
class FilterStep : public ITransformingStep
{
public:
    FilterStep(
        const DataStream & input_stream_,
        ExpressionActionsPtr expression_,
        String filter_column_name_,
        bool remove_filter_column_);

    String getName() const override { return "Filter"; }
    void transformPipeline(QueryPipeline & pipeline) override;

    void updateInputStream(DataStream input_stream, bool keep_header);

    void describeActions(FormatSettings & settings) const override;

    const ExpressionActionsPtr & getExpression() const { return expression; }
    const String & getFilterColumnName() const { return filter_column_name; }
    bool removesFilterColumn() const { return remove_filter_column; }

private:
    ExpressionActionsPtr expression;
    String filter_column_name;
    bool remove_filter_column;
};

}
