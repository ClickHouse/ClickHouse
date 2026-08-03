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

MetricGroup * findGroup(StepAnalysisReport & report, std::string_view label)
{
    for (auto & group : report)
        if (group.label == label)
            return &group;
    return nullptr;
}

std::optional<UInt64> unpairedOutputRows(std::optional<UInt64> input_rows, std::optional<UInt64> matched_rows)
{
    if (!input_rows || !matched_rows)
        return std::nullopt;
    return *input_rows > *matched_rows ? *input_rows - *matched_rows : 0;
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

void appendSideMetrics(
    MetricGroup & group,
    std::optional<UInt64> input_rows,
    std::optional<UInt64> matched_rows,
    std::optional<UInt64> matched_output_rows)
{
    if (!input_rows || !matched_rows)
    {
        group.metrics.emplace_back("match rate", std::string("not collected"), StepMetric::Format::Raw);
        group.metrics.emplace_back("fanout", std::string("not collected"), StepMetric::Format::Raw);
        return;
    }

    group.metrics.emplace_back("match rate", matchRate(*matched_rows, *input_rows), StepMetric::Format::Percent);

    std::optional<double> fanout_value;
    if (matched_output_rows)
        fanout_value = computeFanout(*matched_output_rows, *matched_rows);

    if (fanout_value.has_value())
        group.metrics.emplace_back("fanout", *fanout_value, StepMetric::Format::Ratio);
    else
        group.metrics.emplace_back("fanout", std::string("not collected"), StepMetric::Format::Raw);
}

void enrichJoinSides(StepAnalysisReport & report, UInt64 output_rows, JoinKind kind, JoinStrictness strictness)
{
    auto * left_group = findGroup(report, "left");
    auto * right_group = findGroup(report, "right");
    if (!left_group || !right_group)
        return;

    const auto left_input_rows = findQuantity(*left_group, "rows");
    const auto right_input_rows = findQuantity(*right_group, "rows");
    const auto left_matched_rows = findQuantity(*left_group, "matched");
    const auto right_matched_rows = findQuantity(*right_group, "matched");

    const bool left_side_preserved_with_nulls = isLeftOrFull(kind) && strictness != JoinStrictness::Semi;
    const bool right_side_preserved_with_nulls = isRightOrFull(kind) && strictness != JoinStrictness::Semi;

    std::optional<UInt64> left_unpaired_rows = 0;
    if (left_side_preserved_with_nulls)
        left_unpaired_rows = unpairedOutputRows(left_input_rows, left_matched_rows);
        
    std::optional<UInt64> right_unpaired_rows = 0;
    if (right_side_preserved_with_nulls)
        right_unpaired_rows = unpairedOutputRows(right_input_rows, right_matched_rows);

    std::optional<UInt64> matched_output_rows;
    if (left_unpaired_rows.has_value() && right_unpaired_rows.has_value())
    {
        const UInt64 unpaired_rows = *left_unpaired_rows + *right_unpaired_rows;
        matched_output_rows = output_rows > unpaired_rows ? output_rows - unpaired_rows : 0;
    }

    appendSideMetrics(*left_group, left_input_rows, left_matched_rows, matched_output_rows);
    appendSideMetrics(*right_group, right_input_rows, right_matched_rows, matched_output_rows);
}

/// `sort time` is the time a merge join spent sorting the blocks of one side. Relate it to the
/// processor time of the corresponding stage to show whether sorting dominated that stage.
void appendSortShare(StepAnalysisReport & report, const StepStatsContext & context, std::string_view label, JoinStep::JoinStage stage)
{
    auto * group = findGroup(report, label);
    if (!group)
        return;

    const auto sort_time_ns = findQuantity(*group, "sort time");
    if (!sort_time_ns)
        return;

    const auto stage_stats_it = context.group_stats.find(static_cast<size_t>(stage));
    if (stage_stats_it == context.group_stats.end())
        return;

    const UInt64 stage_sum_elapsed_ns = stage_stats_it->second.sum_elapsed_ns;
    if (stage_sum_elapsed_ns == 0)
        return;

    const double share = 100.0 * static_cast<double>(*sort_time_ns) / static_cast<double>(stage_sum_elapsed_ns);
    group->metrics.emplace_back("sort share", share, StepMetric::Format::Percent);
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

    appendSortShare(report, context, "build", JoinStep::JoinStage::Build);
    appendSortShare(report, context, "probe", JoinStep::JoinStage::Probe);

    for (auto & group : report)
        if (group.label == "spill")
            reshapeSpillGroup(group);

    return buildAnalyzedStepData(context, std::move(report));
}

}
