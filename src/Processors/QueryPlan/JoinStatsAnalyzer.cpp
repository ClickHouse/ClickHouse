#include <Processors/QueryPlan/JoinStatsAnalyzer.h>
#include <Processors/QueryPlan/StepStatsAnalyzer.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Core/Joins.h>
#include <Common/typeid_cast.h>
#include <optional>
#include <string_view>
#include <type_traits>
#include <variant>

namespace DB
{

namespace
{

void swapReportSides(StepAnalysisReport & report)
{
    for (auto & group : report)
    {
        if (group.label == "left")
            group.label = "right";
        else if (group.label == "right")
            group.label = "left";
        else if (group.label == "spill")
        {
            for (auto & metric : group.metrics)
            {
                if (metric.name == "left spilled")
                    metric.name = "right spilled";
                else if (metric.name == "right spilled")
                    metric.name = "left spilled";
            }
        }
    }
}

UInt64 findQuantity(const MetricGroup & group, const std::string & name)
{
    for (const auto & metric : group.metrics)
        if (metric.name == name)
            if (const auto * quantity = std::get_if<UInt64>(&metric.value))
                return *quantity;
    return 0;
}

std::optional<UInt64> findMatched(const MetricGroup & group)
{
    for (const auto & metric : group.metrics)
        if (metric.name == "matched")
        {
            if (const auto * quantity = std::get_if<UInt64>(&metric.value))
                return *quantity;
            return std::nullopt;
        }
    return std::nullopt;
}

MetricGroup * findGroup(StepAnalysisReport & report, std::string_view label)
{
    for (auto & group : report)
        if (group.label == label)
            return &group;
    return nullptr;
}

/// Rows an outer join adds for rows of a preserved side that found no partner. Unknown when that
/// side has no matched count.
std::optional<UInt64> nullFilledRows(bool side_preserved, UInt64 input_rows, std::optional<UInt64> matched_rows)
{
    if (!side_preserved)
        return 0;
    if (!matched_rows)
        return {};
    return input_rows > *matched_rows ? input_rows - *matched_rows : 0;
}

/// Output rows produced by an actual match, per matched row of the side. The NULL-padded rows are
/// excluded, so a fanout above 1 means row multiplication and nothing else. The numerator is shared
/// by both sides, which gives `fanout_left * matched_left == fanout_right * matched_right`.
std::optional<double> fanoutForSide(
    bool is_left_table, JoinKind kind, JoinStrictness strictness,
    UInt64 input_rows, UInt64 matched_rows, UInt64 output_rows, std::optional<UInt64> matched_output_rows)
{
    if (strictness == JoinStrictness::Semi || strictness == JoinStrictness::Anti)
    {
        /// These emit one output row per row of a single side, so there is nothing to multiply.
        const bool this_side_emitted = is_left_table ? isLeft(kind) : isRight(kind);
        if (!this_side_emitted)
            return {};

        const UInt64 unmatched_rows = input_rows > matched_rows ? input_rows - matched_rows : 0;
        const UInt64 emitted_rows = strictness == JoinStrictness::Anti ? unmatched_rows : matched_rows;
        if (!emitted_rows)
            return {};

        return static_cast<double>(output_rows) / static_cast<double>(emitted_rows);
    }

    if (!matched_output_rows || !matched_rows)
        return {};

    return static_cast<double>(*matched_output_rows) / static_cast<double>(matched_rows);
}

void appendSideMetrics(
    MetricGroup & group, bool is_left_table, JoinKind kind, JoinStrictness strictness,
    UInt64 input_rows, std::optional<UInt64> matched_rows, UInt64 output_rows, std::optional<UInt64> matched_output_rows)
{
    if (!matched_rows)
    {
        group.metrics.emplace_back("match rate", std::string("not collected"), StepMetric::Format::Raw);
        group.metrics.emplace_back("fanout", std::string("not collected"), StepMetric::Format::Raw);
        return;
    }

    const double match_rate = input_rows ? 100.0 * static_cast<double>(*matched_rows) / static_cast<double>(input_rows) : 0.0;
    group.metrics.emplace_back("match rate", match_rate, StepMetric::Format::Percent);

    if (auto fanout = fanoutForSide(is_left_table, kind, strictness, input_rows, *matched_rows, output_rows, matched_output_rows))
        group.metrics.emplace_back("fanout", *fanout, StepMetric::Format::Ratio);
    else
        group.metrics.emplace_back("fanout", std::string("not collected"), StepMetric::Format::Raw);
}

void enrichJoinSides(StepAnalysisReport & report, UInt64 output_rows, JoinKind kind, JoinStrictness strictness)
{
    auto * left_group = findGroup(report, "left");
    auto * right_group = findGroup(report, "right");
    if (!left_group || !right_group)
        return;

    const UInt64 left_input_rows = findQuantity(*left_group, "rows");
    const UInt64 right_input_rows = findQuantity(*right_group, "rows");
    const auto left_matched_rows = findMatched(*left_group);
    const auto right_matched_rows = findMatched(*right_group);

    const bool one_sided = strictness == JoinStrictness::Semi || strictness == JoinStrictness::Anti;
    const auto left_null_filled_rows = nullFilledRows(isLeftOrFull(kind) && !one_sided, left_input_rows, left_matched_rows);
    const auto right_null_filled_rows = nullFilledRows(isRightOrFull(kind) && !one_sided, right_input_rows, right_matched_rows);

    std::optional<UInt64> matched_output_rows;
    if (left_null_filled_rows && right_null_filled_rows)
    {
        const UInt64 null_filled_rows = *left_null_filled_rows + *right_null_filled_rows;
        matched_output_rows = output_rows > null_filled_rows ? output_rows - null_filled_rows : 0;
    }

    appendSideMetrics(*left_group, true, kind, strictness, left_input_rows, left_matched_rows, output_rows, matched_output_rows);
    appendSideMetrics(*right_group, false, kind, strictness, right_input_rows, right_matched_rows, output_rows, matched_output_rows);
}

/// `sort time` is the time a merge join spent sorting the blocks of one side. Relate it to the
/// processor time of the corresponding stage to show whether sorting dominated that stage.
void annotateSortTimeShare(StepAnalysisReport & report, const StepStatsContext & context, std::string_view label, JoinStage stage)
{
    auto * group = findGroup(report, label);
    if (!group)
        return;

    const auto stage_stats_it = context.group_stats.find(static_cast<size_t>(stage));
    if (stage_stats_it == context.group_stats.end())
        return;

    const UInt64 stage_sum_elapsed_ns = stage_stats_it->second.sum_elapsed_ns;
    if (stage_sum_elapsed_ns == 0)
        return;

    for (auto & metric : group->metrics)
        if (metric.name == "sort time")
            if (const auto * sort_time_ns = std::get_if<UInt64>(&metric.value))
                metric.share_of_stage_time
                    = 100.0 * static_cast<double>(*sort_time_ns) / static_cast<double>(stage_sum_elapsed_ns);
}

void reshapeSpillGroup(MetricGroup & spill_group)
{
    bool spilled = false;
    for (const auto & metric : spill_group.metrics)
        spilled |= std::visit([](const auto & value) -> bool
        {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_arithmetic_v<T>)
                return value != T{0};
            else
                return !value.empty();
        }, metric.value);

    MetricList reshaped;
    reshaped.emplace_back("", std::string(spilled ? "yes" : "no"), StepMetric::Format::Raw);
    if (spilled)
        for (auto & metric : spill_group.metrics)
            reshaped.push_back(std::move(metric));

    spill_group.metrics = std::move(reshaped);
}

}

AnalyzedStepData analyzeJoinStep(const StepStatsContext & context, StepAnalysisReport report)
{
    const auto * join_step = typeid_cast<const JoinStep *>(context.step);
    if (!join_step)
        return buildAnalyzedStepData(context, std::move(report));

    if (join_step->swap_streams)
        swapReportSides(report);

    const auto & table_join = join_step->getJoin()->getTableJoin();
    enrichJoinSides(report, context.io.output_rows, table_join.kind(), table_join.strictness());

    annotateSortTimeShare(report, context, "build", JoinStage::Build);
    annotateSortTimeShare(report, context, "probe", JoinStage::Probe);

    for (auto & group : report)
        if (group.label == "spill")
            reshapeSpillGroup(group);

    return buildAnalyzedStepData(context, std::move(report));
}

}
