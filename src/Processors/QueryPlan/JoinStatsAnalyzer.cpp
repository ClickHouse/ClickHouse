#include <Processors/QueryPlan/JoinStatsAnalyzer.h>
#include <Processors/QueryPlan/StepStatsAnalyzer.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Common/typeid_cast.h>
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

}

AnalyzedStepData analyzeJoinStep(const StepStatsContext & context, StepAnalysisReport report)
{
    if (const auto * join_step = typeid_cast<const JoinStep *>(context.step); join_step && join_step->swap_streams)
        swapReportSides(report);

    enrichJoinSides(report, context.io.output_rows);

    for (auto & group : report)
        if (group.label == "spill")
            reshapeSpillGroup(group);

    return buildAnalyzedStepData(context, std::move(report));
}

}
