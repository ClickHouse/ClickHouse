#include <Processors/QueryPlan/StepStatsAnalyzer.h>
#include <Processors/QueryPlan/JoinStatsAnalyzer.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Common/typeid_cast.h>
#include <algorithm>

namespace DB
{

namespace
{

MetricGroup makeIOGroup(const StepIOStats & io)
{
    MetricList metrics;
    metrics.emplace_back(MetricKey::InputRows, io.input_rows);
    metrics.emplace_back(MetricKey::OutputRows, io.output_rows);
    metrics.emplace_back(MetricKey::InputBytes, io.input_bytes);
    metrics.emplace_back(MetricKey::OutputBytes, io.output_bytes);
    return {MetricGroupKey::IO, std::move(metrics)};
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
        stage.group_id = group;
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
        stage.processor_distribution.emplace_back(MetricKey::Min, timing.min_elapsed_ns);
        stage.processor_distribution.emplace_back(MetricKey::Median, timing.median_elapsed_ns);
        stage.processor_distribution.emplace_back(MetricKey::Max, timing.max_elapsed_ns);
        stage.processor_distribution.emplace_back(MetricKey::Sum, timing.sum_elapsed_ns);

        stages.push_back(std::move(stage));
    }
    return stages;
}

AnalyzedStepData buildAnalyzedStepData(const StepStatsContext & context, StepAnalysisReport report)
{
    AnalyzedStepData result;
    result.stage_reports = buildAnalyzedStages(context);
    result.step_metric_groups = std::move(report);
    result.step_metric_groups.push_back(makeIOGroup(context.io));

    result.label_stages = result.stage_reports.size() > 1
        || (!result.stage_reports.empty() && !result.stage_reports.front().name.empty());

    return result;
}

AnalyzedStepData analyzeDefaultStep(const StepStatsContext & context, StepAnalysisReport report)
{
    return buildAnalyzedStepData(context, std::move(report));
}

StepStatsAnalyzer getStepStatsAnalyzer(const IQueryPlanStep * step)
{
    if (typeid_cast<const JoinStep *>(step) || typeid_cast<const FilledJoinStep *>(step))
        return &analyzeJoinStep;
    return &analyzeDefaultStep;
}

}
