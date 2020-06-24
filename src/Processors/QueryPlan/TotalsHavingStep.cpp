#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/Transforms/DistinctTransform.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/TotalsHavingTransform.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{

static ITransformingStep::DataStreamTraits getTraits()
{
    return ITransformingStep::DataStreamTraits
    {
            .preserves_distinct_columns = true
    };
}

TotalsHavingStep::TotalsHavingStep(
    const DataStream & input_stream_,
    bool overflow_row_,
    const ExpressionActionsPtr & expression_,
    const std::string & filter_column_,
    TotalsMode totals_mode_,
    double auto_include_threshold_,
    bool final_)
    : ITransformingStep(
            input_stream_,
            TotalsHavingTransform::transformHeader(input_stream_.header, expression_, final_),
            getTraits())
    , overflow_row(overflow_row_)
    , expression(expression_)
    , filter_column_name(filter_column_)
    , totals_mode(totals_mode_)
    , auto_include_threshold(auto_include_threshold_)
    , final(final_)
{
}

void TotalsHavingStep::transformPipeline(QueryPipeline & pipeline)
{
    auto totals_having = std::make_shared<TotalsHavingTransform>(
            pipeline.getHeader(), overflow_row, expression,
            filter_column_name, totals_mode, auto_include_threshold, final);

    pipeline.addTotalsHavingTransform(std::move(totals_having));
}

static String totalsModeToString(TotalsMode totals_mode, double auto_include_threshold)
{
    switch (totals_mode)
    {
        case TotalsMode::BEFORE_HAVING:
            return "before_having";
        case TotalsMode::AFTER_HAVING_INCLUSIVE:
            return "after_having_inclusive";
        case TotalsMode::AFTER_HAVING_EXCLUSIVE:
            return "after_having_exclusive";
        case TotalsMode::AFTER_HAVING_AUTO:
            return "after_having_auto threshold " + std::to_string(auto_include_threshold);
    }

    __builtin_unreachable();
}

Strings TotalsHavingStep::describeActions() const
{
    Strings res;
    res.emplace_back("Filter column: " + filter_column_name);
    res.emplace_back("Mode: " + totalsModeToString(totals_mode, auto_include_threshold));

    for (const auto & action : expression->getActions())
        res.emplace_back((res.size() == 2 ? "Actions: "
                                          : "         ") + action.toString());

    return res;
}

}
