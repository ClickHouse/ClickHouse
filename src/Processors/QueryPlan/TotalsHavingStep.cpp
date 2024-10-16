#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Transforms/DistinctTransform.h>
#include <Processors/Transforms/TotalsHavingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Interpreters/ExpressionActions.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
#include <Core/SettingsEnums.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

static ITransformingStep::Traits getTraits(bool has_filter)
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = !has_filter,
        }
    };
}

TotalsHavingStep::TotalsHavingStep(
    const Header & input_header_,
    const AggregateDescriptions & aggregates_,
    bool overflow_row_,
    std::optional<ActionsDAG> actions_dag_,
    const std::string & filter_column_,
    bool remove_filter_,
    TotalsMode totals_mode_,
    float auto_include_threshold_,
    bool final_)
    : ITransformingStep(
        input_header_,
        TotalsHavingTransform::transformHeader(
            input_header_,
            actions_dag_ ? &*actions_dag_ : nullptr,
            filter_column_,
            remove_filter_,
            final_,
            getAggregatesMask(input_header_, aggregates_)),
        getTraits(!filter_column_.empty()))
    , aggregates(aggregates_)
    , overflow_row(overflow_row_)
    , actions_dag(std::move(actions_dag_))
    , filter_column_name(filter_column_)
    , remove_filter(remove_filter_)
    , totals_mode(totals_mode_)
    , auto_include_threshold(auto_include_threshold_)
    , final(final_)
{
}

void TotalsHavingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    auto expression_actions = actions_dag ? std::make_shared<ExpressionActions>(std::move(*actions_dag), settings.getActionsSettings()) : nullptr;

    auto totals_having = std::make_shared<TotalsHavingTransform>(
        pipeline.getHeader(),
        getAggregatesMask(pipeline.getHeader(), aggregates),
        overflow_row,
        expression_actions,
        filter_column_name,
        remove_filter,
        totals_mode,
        auto_include_threshold,
        final);

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
}

void TotalsHavingStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    settings.out << prefix << "Filter column: " << filter_column_name;
    if (remove_filter)
        settings.out << " (removed)";
    settings.out << '\n';
    settings.out << prefix << "Mode: " << totalsModeToString(totals_mode, auto_include_threshold) << '\n';

    if (actions_dag)
    {
        bool first = true;
        if (actions_dag)
        {
            auto expression = std::make_shared<ExpressionActions>(actions_dag->clone());
            for (const auto & action : expression->getActions())
            {
                settings.out << prefix << (first ? "Actions: "
                                                : "         ");
                first = false;
                settings.out << action.toString() << '\n';
            }
        }
    }
}

void TotalsHavingStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Mode", totalsModeToString(totals_mode, auto_include_threshold));
    if (actions_dag)
    {
        map.add("Filter column", filter_column_name);
        if (actions_dag)
        {
            auto expression = std::make_shared<ExpressionActions>(actions_dag->clone());
            map.add("Expression", expression->toTree());
        }
    }
}

void TotalsHavingStep::updateOutputHeader()
{
    output_header =
        TotalsHavingTransform::transformHeader(
            input_headers.front(),
            getActions(),
            filter_column_name,
            remove_filter,
            final,
            getAggregatesMask(input_headers.front(), aggregates));
}

void TotalsHavingStep::serializeSettings(QueryPlanSerializationSettings & settings) const
{
    settings.totals_mode = totals_mode;
    settings.totals_auto_threshold = auto_include_threshold;
}

void TotalsHavingStep::serialize(Serialization & ctx) const
{
    UInt8 flags = 0;
    if (final)
        flags |= 1;
    if (overflow_row)
        flags |= 2;
    if (actions_dag)
        flags |= 4;
    if (actions_dag && remove_filter)
        flags |= 8;

    writeIntBinary(flags, ctx.out);

    serializeAggregateDescriptions(aggregates, ctx.out);

    if (actions_dag)
    {
        writeStringBinary(filter_column_name, ctx.out);
        actions_dag->serialize(ctx.out, ctx.registry);
    }
}

std::unique_ptr<IQueryPlanStep> TotalsHavingStep::deserialize(Deserialization & ctx)
{
    if (ctx.input_headers.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "TotalsHaving must have one input stream");

    UInt8 flags;
    readIntBinary(flags, ctx.in);

    bool final = bool(flags & 1);
    bool overflow_row = bool(flags & 2);
    bool has_actions_dag = bool(flags & 4);
    bool remove_filter_column = bool(flags & 8);

    AggregateDescriptions aggregates;
    deserializeAggregateDescriptions(aggregates, ctx.in);

    std::optional<ActionsDAG> actions_dag;
    String filter_column_name;
    if (has_actions_dag)
    {
        readStringBinary(filter_column_name, ctx.in);

        actions_dag = ActionsDAG::deserialize(ctx.in, ctx.registry, ctx.context);
    }

    return std::make_unique<TotalsHavingStep>(
        ctx.input_headers.front(),
        std::move(aggregates),
        overflow_row,
        std::move(actions_dag),
        std::move(filter_column_name),
        remove_filter_column,
        ctx.settings.totals_mode,
        ctx.settings.totals_auto_threshold,
        final);
}

void registerTotalsHavingStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("TotalsHaving", TotalsHavingStep::deserialize);
}

}
