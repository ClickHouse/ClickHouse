#include <Processors/QueryPlan/StepStatsAnalyzer.h>
#include <Processors/QueryPlan/JoinStatsAnalyzer.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <algorithm>
#include <set>

namespace DB
{

namespace
{

MetricGroup makeIOGroup(const StepIOStats & io)
{
    MetricGroup io_group;
    io_group.label = "I/O";
    io_group.metrics.emplace_back("input rows", io.input_rows, StepMetric::Format::Quantity);
    io_group.metrics.emplace_back("output rows", io.output_rows, StepMetric::Format::Quantity);
    io_group.metrics.emplace_back("input bytes", io.input_bytes, StepMetric::Format::Bytes);
    io_group.metrics.emplace_back("output bytes", io.output_bytes, StepMetric::Format::Bytes);
    return io_group;
}

}

AnalyzedStages buildAnalyzedStages(const StepStatsContext & context)
{
    AnalyzedStages stages;
    for (size_t group : context.step->getStepGroups())
    {
        const auto group_it = context.group_stats.find(group);
        if (group_it == context.group_stats.end())
            continue;

        const StepGroupStats & timing = group_it->second;

        AnalyzedStage stage;
        stage.name = context.step->getStepGroupName(group);
        stage.wall_clock_time_ns = timing.wall_clock_time_ns;
        stage.total_num_processors = timing.total_num_processors;
        stage.share_of_query_time = context.execution_query_time_ns != 0
            ? 100.0 * static_cast<double>(timing.wall_clock_time_ns) / static_cast<double>(context.execution_query_time_ns)
            : 0.0;
        stage.parallelism = timing.wall_clock_time_ns != 0
            ? static_cast<double>(timing.sum_elapsed_ns) / static_cast<double>(timing.wall_clock_time_ns)
            : 0.0;
        stage.max_parallelism = std::min(context.max_num_threads_per_query, timing.total_num_processors);
        stage.processor_distribution.emplace_back("min", timing.min_elapsed_ns, StepMetric::Format::Time);
        stage.processor_distribution.emplace_back("median", timing.median_elapsed_ns, StepMetric::Format::Time);
        stage.processor_distribution.emplace_back("max", timing.max_elapsed_ns, StepMetric::Format::Time);
        stage.processor_distribution.emplace_back("sum", timing.sum_elapsed_ns, StepMetric::Format::Time);

        stages.push_back(std::move(stage));
    }
    return stages;
}

AnalyzedStepData buildAnalyzedStepData(const StepStatsContext & context, StepAnalysisReport report)
{
    AnalyzedStepData result;
    result.stage_reports = buildAnalyzedStages(context);

    std::set<std::string> stage_names;
    for (size_t group : context.step->getStepGroups())
    {
        std::string group_name = context.step->getStepGroupName(group);
        if (!group_name.empty())
            stage_names.insert(std::move(group_name));
    }

    for (auto & group : report)
    {
        if (stage_names.contains(group.label))
        {
            auto stage_it = std::ranges::find(result.stage_reports, group.label, &AnalyzedStage::name);
            if (stage_it != result.stage_reports.end())
                for (auto & metric : group.metrics)
                    stage_it->inline_metrics.push_back(std::move(metric));
            continue;
        }

        result.step_metric_groups.push_back(std::move(group));
    }

    result.step_metric_groups.push_back(makeIOGroup(context.io));

    result.label_stages = result.stage_reports.size() > 1
        || (!result.stage_reports.empty() && !result.stage_reports.front().name.empty());

    return result;
}

AnalyzedStepData analyzeDefaultStep(const StepStatsContext & context, StepAnalysisReport report)
{
    return buildAnalyzedStepData(context, std::move(report));
}

StepStatsAnalyzer getStepStatsAnalyzer(std::string_view step_name)
{
    if (step_name == "Join")
        return &analyzeJoinStep;
    return &analyzeDefaultStep;
}

}
