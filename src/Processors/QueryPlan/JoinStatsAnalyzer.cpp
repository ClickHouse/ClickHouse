#include <Processors/QueryPlan/JoinStatsAnalyzer.h>
#include <Processors/QueryPlan/StepStatsAnalyzer.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
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

StepMetric quantityMetric(std::string_view name, UInt64 value)
{
    return {std::string(name), value, StepMetric::Format::Quantity};
}

StepMetric optionalQuantityMetric(std::string_view name, const std::optional<UInt64> & value)
{
    if (value)
        return {std::string(name), *value, StepMetric::Format::Quantity};
    return {std::string(name), String("missing stats"), StepMetric::Format::Raw};
}

StepMetric optionalCostMetric(std::string_view name, const std::optional<double> & value)
{
    if (value)
        return {std::string(name), *value, StepMetric::Format::Quantity};
    return {std::string(name), String("missing stats"), StepMetric::Format::Raw};
}

StepMetric optionalSelectivityMetric(std::string_view name, const std::optional<double> & value)
{
    if (value)
        return {std::string(name), fmt::format("{:.4g}", *value), StepMetric::Format::Raw};
    return {std::string(name), String("missing stats"), StepMetric::Format::Raw};
}

MetricList sideMetrics(const std::optional<UInt64> & estimated_rows, UInt64 actual_rows, UInt64 matched, UInt64 output_rows)
{
    MetricList metrics;
    metrics.emplace_back(optionalQuantityMetric("rows estimated", estimated_rows));
    metrics.emplace_back(quantityMetric("rows actual", actual_rows));
    metrics.emplace_back(quantityMetric("matched", matched));

    const double match_rate = actual_rows ? 100.0 * static_cast<double>(matched) / static_cast<double>(actual_rows) : 0.0;
    metrics.emplace_back("match rate", match_rate, StepMetric::Format::Percent);

    const double fanout = matched ? static_cast<double>(output_rows) / static_cast<double>(matched) : 0.0;
    metrics.emplace_back("fanout", fanout, StepMetric::Format::Ratio);
    return metrics;
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

std::optional<double> cartesianSelectivity(const JoinAnalysisCounters & counters, UInt64 matched_pairs)
{
    if (!counters.left_rows || !counters.right_rows)
        return std::nullopt;
    return static_cast<double>(matched_pairs) / (static_cast<double>(counters.left_rows) * static_cast<double>(counters.right_rows));
}

std::optional<double> resultRowsQError(const std::optional<UInt64> & estimated_rows, UInt64 actual_rows)
{
    if (!estimated_rows || !*estimated_rows || !actual_rows)
        return std::nullopt;
    return std::max(
        static_cast<double>(*estimated_rows) / static_cast<double>(actual_rows),
        static_cast<double>(actual_rows) / static_cast<double>(*estimated_rows));
}

UInt64 countMatchedPairs(const JoinStep & join_step, const JoinAnalysisCounters & counters, UInt64 output_rows)
{
    if (!join_step.getJoin())
        return 0;

    JoinKind kind = join_step.getJoin()->getTableJoin().kind();
    if (join_step.swap_streams)
        kind = reverseJoinKind(kind);
    return joinMatchedPairs(kind, counters, output_rows);
}

StepAnalysisReport buildComparisonReport(
    const StepStatsContext & context, const JoinStep & join_step, StepAnalysisReport runtime_groups, std::optional<double> actual_cost)
{
    const JoinEstimation & estimation = join_step.getEstimation();
    const UInt64 output_rows = context.io.output_rows;
    const auto counters = extractJoinCounters(runtime_groups);
    const UInt64 matched_pairs = countMatchedPairs(join_step, counters, output_rows);

    /// Turn the raw per-side groups from `getAnalysisReport` into the estimated/actual paired form in place.
    for (auto & group : runtime_groups)
    {
        if (group.label == "left")
            group.metrics = sideMetrics(estimation.left_rows, counters.left_rows, counters.matched_left, output_rows);
        else if (group.label == "right")
            group.metrics = sideMetrics(estimation.right_rows, counters.right_rows, counters.matched_right, output_rows);
    }

    MetricGroup cost;
    cost.label = "cost";
    cost.metrics.emplace_back(optionalCostMetric("estimated", estimation.cost));
    cost.metrics.emplace_back(optionalCostMetric("actual", actual_cost));

    MetricGroup selectivity;
    selectivity.label = "selectivity";
    selectivity.metrics.emplace_back(optionalSelectivityMetric("estimated (NDV)", estimation.selectivity));
    selectivity.metrics.emplace_back(optionalSelectivityMetric("actual (cartesian)", cartesianSelectivity(counters, matched_pairs)));

    MetricGroup output;
    output.label = "output rows";
    output.metrics.emplace_back(optionalQuantityMetric("estimated", estimation.output_rows));
    output.metrics.emplace_back(quantityMetric("actual", output_rows));
    if (const auto q_error = resultRowsQError(estimation.output_rows, output_rows))
        output.metrics.emplace_back("q-error", *q_error, StepMetric::Format::Ratio);

    StepAnalysisReport report;
    report.emplace_back(std::move(cost));
    report.emplace_back(std::move(selectivity));
    report.emplace_back(std::move(output));
    for (auto & group : runtime_groups)
        report.emplace_back(std::move(group));
    for (auto & group : QueryPlanFormat::collectJoinInputColumns(join_step))
        report.emplace_back(std::move(group));

    return report;
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

    if (join_step)
        report = buildComparisonReport(context, *join_step, std::move(report), branch_costs.getBranchCost(join_step));

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
