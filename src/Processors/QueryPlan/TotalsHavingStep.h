#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

enum class TotalsMode;

/// Execute HAVING and calculate totals. See TotalsHavingTransform.
class TotalsHavingStep : public ITransformingStep
{
public:
    TotalsHavingStep(
            const DataStream & input_stream_,
            bool overflow_row_,
            const ExpressionActionsPtr & expression_,
            const std::string & filter_column_,
            TotalsMode totals_mode_,
            double auto_include_threshold_,
            bool final_);

    String getName() const override { return "TotalsHaving"; }

    void transformPipeline(QueryPipeline & pipeline) override;

    void describeActions(FormatSettings & settings) const override;

private:
    bool overflow_row;
    ExpressionActionsPtr expression;
    String filter_column_name;
    TotalsMode totals_mode;
    double auto_include_threshold;
    bool final;
};

}

