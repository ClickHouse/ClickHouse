#include <Processors/QueryPlan/JoinStatsAnalyzer.h>
#include <Processors/QueryPlan/StepStatsAnalyzer.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Common/typeid_cast.h>
#include <algorithm>
#include <optional>
#include <type_traits>
#include <variant>
#include <fmt/format.h>

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

void enrichJoinSides(StepAnalysisReport & report, UInt64 output_rows)
{
    for (auto & group : report)
    {
        if (group.label != "left" && group.label != "right")
            continue;

        const UInt64 rows = findQuantity(group, "rows");
        const UInt64 matched = findQuantity(group, "matched");

        const double match_rate = rows ? 100.0 * static_cast<double>(matched) / static_cast<double>(rows) : 0.0;
        group.metrics.emplace_back("match rate", match_rate, StepMetric::Format::Percent);

        const double fanout = rows ? static_cast<double>(output_rows) / static_cast<double>(matched) : 0.0;
        group.metrics.emplace_back("fanout", fanout, StepMetric::Format::Ratio);
    }
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

void appendActualGroup(StepAnalysisReport & report, const StepStatsContext & context, const JoinStep & join_step, std::optional<double> actual_cost)
{
    const UInt64 output_rows = context.io.output_rows;
    const auto counters = extractJoinCounters(report);

    MetricGroup group;
    group.label = "actual";

    if (counters.left_rows && counters.right_rows && join_step.getJoin())
    {
        JoinKind kind = join_step.getJoin()->getTableJoin().kind();
        if (join_step.swap_streams)
            kind = reverseJoinKind(kind);

        const UInt64 matched_pairs = joinMatchedPairs(kind, counters, output_rows);
        group.metrics.emplace_back("matched pairs", matched_pairs, StepMetric::Format::Quantity);

        const double selectivity = static_cast<double>(matched_pairs)
            / (static_cast<double>(counters.left_rows) * static_cast<double>(counters.right_rows));
        group.metrics.emplace_back("selectivity (cartesian)", fmt::format("{:.4g}", selectivity), StepMetric::Format::Raw);
    }

    if (const auto estimated_rows = join_step.getEstimation().output_rows; estimated_rows && *estimated_rows && output_rows)
    {
        const double q_error = std::max(
            static_cast<double>(*estimated_rows) / static_cast<double>(output_rows),
            static_cast<double>(output_rows) / static_cast<double>(*estimated_rows));
        group.metrics.emplace_back("result rows q-error", q_error, StepMetric::Format::Ratio);
    }

    if (actual_cost)
        group.metrics.emplace_back("cost", *actual_cost, StepMetric::Format::Quantity);

    if (!group.metrics.empty())
        report.push_back(std::move(group));
}

}

JoinStepStatsAnalyzer::JoinStepStatsAnalyzer(const QueryPlan & plan, const JoinRuntimeReports & join_reports)
{
    CardinalityByJoinStep cardinality_by_join_step;
    for (const auto & [join_step, runtime] : join_reports)
    {
        if (!join_step->getJoin())
            continue;

        const auto counters = extractJoinCounters(runtime.report);
        if (!counters.left_rows || !counters.right_rows)
            continue;

        cardinality_by_join_step[join_step]
            = joinMatchedPairs(join_step->getJoin()->getTableJoin().kind(), counters, runtime.output_rows);
    }

    branch_costs = JoinBranchCosts(plan, cardinality_by_join_step);
}

AnalyzedStepData JoinStepStatsAnalyzer::analyze(const StepStatsContext & context, StepAnalysisReport report) const
{
    const auto * join_step = typeid_cast<const JoinStep *>(context.step);

    if (join_step && join_step->swap_streams)
        swapReportSides(report);

    enrichJoinSides(report, context.io.output_rows);

    if (join_step)
        appendActualGroup(report, context, *join_step, branch_costs.getBranchCost(join_step));

    for (auto & group : report)
        if (group.label == "spill")
            reshapeSpillGroup(group);

    return buildAnalyzedStepData(context, std::move(report));
}

JoinAnalysisCounters extractJoinCounters(const StepAnalysisReport & report)
{
    JoinAnalysisCounters counters;
    for (const auto & group : report)
    {
        if (group.label == "left")
        {
            counters.left_rows = findQuantity(group, "rows");
            counters.matched_left = findQuantity(group, "matched");
        }
        else if (group.label == "right")
        {
            counters.right_rows = findQuantity(group, "rows");
            counters.matched_right = findQuantity(group, "matched");
        }
    }
    return counters;
}

UInt64 joinMatchedPairs(JoinKind kind, const JoinAnalysisCounters & counters, UInt64 output_rows)
{
    const UInt64 unmatched_left = counters.left_rows > counters.matched_left ? counters.left_rows - counters.matched_left : 0;
    const UInt64 unmatched_right = counters.right_rows > counters.matched_right ? counters.right_rows - counters.matched_right : 0;
    const UInt64 padded = (isLeftOrFull(kind) ? unmatched_left : 0) + (isRightOrFull(kind) ? unmatched_right : 0);
    return output_rows > padded ? output_rows - padded : 0;
}

}
