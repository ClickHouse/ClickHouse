#include <Processors/QueryPlan/JoinStatsAnalyzer.h>
#include <Processors/QueryPlan/StepStatsAnalyzer.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Core/Joins.h>
#include <Common/typeid_cast.h>
#include <algorithm>
#include <optional>
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
        if (group.key == MetricGroupKey::Left)
            group.key = MetricGroupKey::Right;
        else if (group.key == MetricGroupKey::Right)
            group.key = MetricGroupKey::Left;
        else if (group.key == MetricGroupKey::Spill)
        {
            for (auto & metric : group.metrics)
            {
                if (metric.key == MetricKey::LeftSpilled)
                    metric.key = MetricKey::RightSpilled;
                else if (metric.key == MetricKey::RightSpilled)
                    metric.key = MetricKey::LeftSpilled;
            }
        }
    }
}

struct JoinSideRows
{
    std::optional<UInt64> input_rows;
    std::optional<UInt64> matched_rows;
};

JoinSideRows readSideRows(const MetricGroup * group)
{
    if (!group)
        return {};
    return {findQuantity(*group, MetricKey::Rows), findQuantity(*group, MetricKey::Matched)};
}

std::optional<UInt64> unpairedOutputRows(const JoinSideRows & side, bool preserved_with_nulls)
{
    if (!preserved_with_nulls)
        return 0;
    if (!side.input_rows || !side.matched_rows)
        return std::nullopt;
    return *side.input_rows > *side.matched_rows ? *side.input_rows - *side.matched_rows : 0;
}

double matchRate(UInt64 matched_rows, UInt64 input_rows)
{
    if (input_rows == 0)
        return 0.0;
    return 100.0 * static_cast<double>(matched_rows) / static_cast<double>(input_rows);
}

std::optional<double> computeFanout(UInt64 matched_output_rows, UInt64 matched_rows)
{
    if (matched_rows == 0)
        return std::nullopt;
    return static_cast<double>(matched_output_rows) / static_cast<double>(matched_rows);
}

void appendSideMetrics(MetricGroup & group, const JoinSideRows & side, std::optional<UInt64> matched_output_rows)
{
    if (!side.input_rows || !side.matched_rows)
    {
        group.metrics.emplace_back(MetricKey::MatchRate, std::monostate{});
        group.metrics.emplace_back(MetricKey::Fanout, std::monostate{});
        return;
    }

    group.metrics.emplace_back(MetricKey::MatchRate, matchRate(*side.matched_rows, *side.input_rows));

    std::optional<double> fanout_value;
    if (matched_output_rows)
        fanout_value = computeFanout(*matched_output_rows, *side.matched_rows);

    if (fanout_value.has_value())
        group.metrics.emplace_back(MetricKey::Fanout, *fanout_value);
    else
        group.metrics.emplace_back(MetricKey::Fanout, std::monostate{});
}

void enrichJoinSides(StepAnalysisReport & report, UInt64 output_rows, JoinKind kind, JoinStrictness strictness)
{
    auto * left_group = findGroup(report, MetricGroupKey::Left);
    auto * right_group = findGroup(report, MetricGroupKey::Right);
    if (!left_group && !right_group)
        return;

    const JoinSideRows left_side = readSideRows(left_group);
    const JoinSideRows right_side = readSideRows(right_group);

    const bool left_side_preserved_with_nulls = isLeftOrFull(kind) && strictness != JoinStrictness::Semi;
    const bool right_side_preserved_with_nulls = isRightOrFull(kind) && strictness != JoinStrictness::Semi;

    const auto left_unpaired_rows = unpairedOutputRows(left_side, left_side_preserved_with_nulls);
    const auto right_unpaired_rows = unpairedOutputRows(right_side, right_side_preserved_with_nulls);

    std::optional<UInt64> matched_output_rows;
    if (left_unpaired_rows.has_value() && right_unpaired_rows.has_value())
    {
        const UInt64 unpaired_rows = *left_unpaired_rows + *right_unpaired_rows;
        matched_output_rows = output_rows > unpaired_rows ? output_rows - unpaired_rows : 0;
    }

    if (left_group)
        appendSideMetrics(*left_group, left_side, matched_output_rows);
    if (right_group)
        appendSideMetrics(*right_group, right_side, matched_output_rows);
}

/// sort time is the time a merge join spent sorting the blocks of one side. Relate it to the
/// processor time of the corresponding stage to show whether sorting dominated that stage.
void appendSortShare(StepAnalysisReport & report, const StepStatsContext & context, MetricGroupKey group_key, JoinStep::JoinStage stage)
{
    auto * group = findGroup(report, group_key);
    if (!group)
        return;

    const auto sort_time_ns = findQuantity(*group, MetricKey::SortTime);
    if (!sort_time_ns)
        return;

    const auto stage_stats_it = context.group_stats.find(static_cast<size_t>(stage));
    if (stage_stats_it == context.group_stats.end())
        return;

    const UInt64 stage_sum_elapsed_ns = stage_stats_it->second.sum_elapsed_ns;
    if (stage_sum_elapsed_ns == 0)
        return;

    const double share = 100.0 * static_cast<double>(*sort_time_ns) / static_cast<double>(stage_sum_elapsed_ns);
    group->metrics.emplace_back(MetricKey::SortShare, share);
}

void reshapeSpillGroup(MetricGroup & spill_group)
{
    bool spilled = false;
    for (const auto & metric : spill_group.metrics)
        spilled |= std::visit([](const auto & value) -> bool
        {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, std::monostate>)
                return false;
            else if constexpr (std::is_arithmetic_v<T>)
                return value != T{0};
            else
                return !value.empty();
        }, metric.value);

    MetricList reshaped;
    reshaped.emplace_back(MetricKey::Unnamed, std::string(spilled ? "yes" : "no"));
    if (spilled)
        for (auto & metric : spill_group.metrics)
            reshaped.push_back(std::move(metric));

    spill_group.metrics = std::move(reshaped);
}

void inlineGroupIntoStage(AnalyzedStepData & step_data, MetricGroupKey group_key, JoinStep::JoinStage stage)
{
    auto group_it = std::ranges::find(step_data.step_metric_groups, group_key, &MetricGroup::key);
    if (group_it == step_data.step_metric_groups.end())
        return;

    auto stage_it = std::ranges::find(step_data.stage_reports, static_cast<size_t>(stage), &AnalyzedStage::group_id);
    if (stage_it == step_data.stage_reports.end())
        return;

    for (auto & metric : group_it->metrics)
        stage_it->inline_metrics.push_back(std::move(metric));

    step_data.step_metric_groups.erase(group_it);
}

}

AnalyzedStepData analyzeJoinStep(const StepStatsContext & context, StepAnalysisReport report)
{
    const auto * join_step = typeid_cast<const JoinStep *>(context.step);
    const auto * filled_join_step = typeid_cast<const FilledJoinStep *>(context.step);
    if (!join_step && !filled_join_step)
        return buildAnalyzedStepData(context, std::move(report));

    /// Only JoinStep can swap its inputs; a filled join always keeps the storage on the right.
    bool swapped = join_step && join_step->swap_streams;
    const auto & join = join_step ? join_step->getJoin() : filled_join_step->getJoin();
    const auto & table_join = join->getTableJoin();

    const JoinKind logical_kind = swapped ? reverseJoinKind(table_join.kind()) : table_join.kind();

    if (swapped)
        swapReportSides(report);

    enrichJoinSides(report, context.io.output_rows, logical_kind, table_join.strictness());

    appendSortShare(report, context, MetricGroupKey::Build, JoinStep::JoinStage::Build);
    appendSortShare(report, context, MetricGroupKey::Probe, JoinStep::JoinStage::Probe);

    if (auto * spill_group = findGroup(report, MetricGroupKey::Spill))
        reshapeSpillGroup(*spill_group);

    AnalyzedStepData result = buildAnalyzedStepData(context, std::move(report));

    inlineGroupIntoStage(result, MetricGroupKey::Build, JoinStep::JoinStage::Build);
    inlineGroupIntoStage(result, MetricGroupKey::Probe, JoinStep::JoinStage::Probe);

    return result;
}

}
