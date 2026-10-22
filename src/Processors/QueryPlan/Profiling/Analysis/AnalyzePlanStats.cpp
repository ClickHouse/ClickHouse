#include <iterator>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <variant>
#include <Processors/Port.h>
#include <Processors/QueryPlan/Profiling/Analysis/AnalyzePlanStats.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/Profiling/Analysis/JoinBranchCosts.h>
#include <Processors/QueryPlan/Profiling/Analysis/JoinStatsAnalyzer.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/Profiling/Metrics/StepAnalyzeInfo.h>
#include <Processors/QueryPlan/Profiling/Analysis/StepStatsAnalyzer.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Common/typeid_cast.h>
#include <Processors/QueryPlan/Profiling/Execution/StepProfiler.h>
#include <Processors/QueryPlan/Profiling/Execution/StepWallClock.h>
#include <base/defines.h>
#include <base/types.h>

namespace DB
{

namespace
{

/// Printed in place of a parallelism or concurrency value when there was no work to measure it on.
constexpr std::string_view unknown_value_text = "Unknown";

String formatStepMetricValue(const StepMetric & metric)
{
    if (std::holds_alternative<std::monostate>(metric.value))
        return String(missingValueText(metric.key));

    if (const auto * fraction = std::get_if<Fraction>(&metric.value))
        return fmt::format("{:.2f}/{:.0f}", fraction->numerator, fraction->denominator);

    const MetricFormat format = formatOf(metric.key);

    if (format == MetricFormat::Raw)
        return std::visit([](const auto & value) -> String
        {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, std::string>)
                return value;
            else if constexpr (std::is_arithmetic_v<T>)
                return fmt::format("{}", value);
            else
                return {};
        }, metric.value);

    const double numeric = std::visit([](const auto & value) -> double
    {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_arithmetic_v<T>)
            return static_cast<double>(value);
        else
            return 0.0;
    }, metric.value);

    switch (format)
    {
        case MetricFormat::Bytes:
            return formatReadableSizeWithDecimalSuffix(numeric);
        case MetricFormat::Quantity:
            return formatReadableQuantity(numeric);
        case MetricFormat::Time:
            return formatReadableTime(numeric);
        case MetricFormat::Percent:
            return fmt::format("{:.2f}%", numeric);
        case MetricFormat::Ratio:
            return fmt::format("{:.2f}", numeric);
        case MetricFormat::Selectivity:
            return fmt::format("{:.4g}", numeric);
        case MetricFormat::Fraction:
        case MetricFormat::Raw:
            return {};
    }
    return {};
}

void printMetricGroup(const MetricGroup & metric_group, WriteBuffer & out, const std::string & prefix)
{
    if (metric_group.metrics.empty())
        return;

    out << prefix << toString(metric_group.key) << ": ";

    bool first = true;
    for (const auto & metric : metric_group.metrics)
    {
        if (!first)
            out << " · ";
        first = false;
        const std::string_view name = toString(metric.key);
        if (name.empty())
            out << formatStepMetricValue(metric);
        else
            out << name << " " << formatStepMetricValue(metric);
    }
    out << "\n";
}

/// The group is built by makeIOGroup, so a missing metric means the two went out of sync
UInt64 getQuantity(const MetricGroup & metric_group, MetricKey key)
{
    const auto quantity = findQuantity(metric_group, key);
    chassert(quantity, "metric is missing from the I/O group");
    return quantity.value_or(0);
}

void printIOGroup(const MetricGroup & io_group, WriteBuffer & out, const std::string & prefix)
{
    const UInt64 input_rows = getQuantity(io_group, MetricKey::InputRows);
    const UInt64 output_rows = getQuantity(io_group, MetricKey::OutputRows);
    const UInt64 input_bytes = getQuantity(io_group, MetricKey::InputBytes);
    const UInt64 output_bytes = getQuantity(io_group, MetricKey::OutputBytes);

    const UInt8 precision_rows_in = input_rows < 1000 ? 0 : 2;
    const UInt8 precision_rows_out = output_rows < 1000 ? 0 : 2;

    out << prefix << "I/O: rows "
        << formatReadableQuantity(static_cast<double>(input_rows), precision_rows_in) << " → "
        << formatReadableQuantity(static_cast<double>(output_rows), precision_rows_out);

    if (input_rows != output_rows && input_rows != 0)
        out << fmt::format(" ({:.2f}%)", 100.0 * static_cast<double>(output_rows) / static_cast<double>(input_rows));

    if (input_bytes != 0 || output_bytes != 0)
    {
        const UInt8 precision_bytes_in = input_bytes < 1000 ? 0 : 2;
        const UInt8 precision_bytes_out = output_bytes < 1000 ? 0 : 2;
        out << " · " << formatReadableSizeWithDecimalSuffix(static_cast<double>(input_bytes), precision_bytes_in)
            << " → " << formatReadableSizeWithDecimalSuffix(static_cast<double>(output_bytes), precision_bytes_out);
    }
    out << "\n";
}

/// The group is built by `makeTimingReport`: a (time, share) pair for the step, then one for its branch.
void printTimeGroup(const MetricGroup & time_group, WriteBuffer & out, const std::string & prefix)
{
    if (!hasCollectedMetrics(time_group))
        return;

    chassert(time_group.metrics.size() == 4, "unexpected layout of the Time group");

    auto print_part = [&](std::string_view name, const StepMetric & time, const StepMetric & share)
    {
        out << name << " " << formatStepMetricValue(time);
        if (!std::holds_alternative<std::monostate>(share.value))
            out << " (" << formatStepMetricValue(share) << ")";
    };

    out << prefix << toString(time_group.key) << ": ";
    print_part("step", time_group.metrics[0], time_group.metrics[1]);
    out << " · ";
    print_part("branch", time_group.metrics[2], time_group.metrics[3]);
    out << "\n";
}

/// The group is built by `makeConcurrencyReport`: the step value, then the branch value.
void printConcurrencyGroup(const MetricGroup & concurrency_group, WriteBuffer & out, const std::string & prefix)
{
    /// Empty when work intervals were not collected, see `makeConcurrencyReport`.
    if (concurrency_group.metrics.empty())
        return;

    chassert(concurrency_group.metrics.size() == 2, "unexpected layout of the Concurrency group");

    /// An empty value means the step (or its subtree) did no work, so there is no concurrency to report.
    const auto format_part = [](const StepMetric & metric) -> String
    {
        if (std::holds_alternative<std::monostate>(metric.value))
            return String(unknown_value_text);
        return formatStepMetricValue(metric);
    };

    out << prefix << toString(concurrency_group.key) << ": "
        << "step " << format_part(concurrency_group.metrics[0]) << " · "
        << "branch " << format_part(concurrency_group.metrics[1]) << "\n";
}

void printStage(const AnalyzedStage & stage, bool label_stages, WriteBuffer & out, const std::string & prefix, bool processors_info)
{
    out << prefix << "  ";
    if (label_stages)
    {
        out << "Stage";
        if (!stage.name.empty())
            out << " (" << stage.name << ")";
        out << ": ";
    }
    out << "time " << formatReadableTime(static_cast<double>(stage.wall_clock_time_ns))
        << " (" << formatStepMetricValue({MetricKey::TimeShare, stage.share_of_query_time}) << ")" << " · parallelism "
        << (stage.wall_clock_time_ns ? fmt::format("{:.2f}/{}", stage.parallelism, stage.max_parallelism) : String(unknown_value_text));

    for (const auto & metric : stage.inline_metrics)
        out << " · " << toString(metric.key) << " " << formatStepMetricValue(metric);

    out << "\n";

    if (processors_info)
    {
        out << prefix << "    Time per processor (" << stage.total_num_processors << "): ";
        bool first = true;
        for (const auto & metric : stage.processor_distribution)
        {
            if (!first)
                out << " · ";
            first = false;
            out << toString(metric.key) << " " << formatStepMetricValue(metric);
        }
        out << "\n";
    }
}

}

AnalyzeStepsStats::AnalyzeStepsStats(
    const QueryPipeline & pipeline,
    PlanAnalysisSnapshot plan_snapshot_,
    StepProfiler & step_profiler,
    UInt64 execution_start_ns,
    UInt64 execution_query_time_ns_,
    const QueryPlan * plan_for_interval_timings)
: plan_snapshot(std::move(plan_snapshot_))
, max_num_threads_per_query(pipeline.getNumThreads())
, execution_query_time_ns(execution_query_time_ns_)
{
    const auto & processors = pipeline.getProcessors();

    collectIOStats(processors);
    const auto elapsed_per_step_group = collectTimingStats(step_profiler, processors);
    computeDistribution(elapsed_per_step_group);
    computeJoinBranchCosts();

    /// Work intervals are collected only when EXPLAIN ANALYZE requests the `time` setting, and only
    /// that caller passes a plan to lay them over.
    if (!plan_for_interval_timings)
        return;

    if (const auto work_intervals = step_profiler.extractWorkIntervals(execution_start_ns); !work_intervals.empty())
        interval_timings.emplace(work_intervals, *plan_for_interval_timings);
}

std::optional<ExecutionTimeBreakdown> AnalyzeStepsStats::executionTimeBreakdown() const
{
    if (!interval_timings)
        return std::nullopt;
    return interval_timings->executionTimeBreakdown(execution_query_time_ns);
}

void AnalyzeStepsStats::collectIOStats(const Processors & processors)
{
    auto crosses_step_boundary = [](const IProcessor & owner, const IProcessor & neighbour)
    {
        return owner.getStepUniqID() != neighbour.getStepUniqID();
    };

    for (const auto & proc : processors)
    {
        const auto & step_id = proc->getStepUniqID();

        if (step_id.empty())
            continue;

        auto & step_stats = stats_by_step[step_id];

        processors_by_step[step_id].push_back(proc.get());

        for (const auto & input_port : proc->getInputs())
        {
            if (!input_port.isConnected())
                continue;

            if (crosses_step_boundary(*proc, input_port.getOutputPort().getProcessor()))
            {
                const auto counters = proc->getPortDataCounters(input_port);
                step_stats.input_rows += counters.rows;
                step_stats.input_bytes += counters.bytes;
            }
        }

        for (const auto & output_port : proc->getOutputs())
        {
            if (!output_port.isConnected())
                continue;

            if (crosses_step_boundary(*proc, output_port.getInputPort().getProcessor()))
            {
                const auto counters = proc->getPortDataCounters(output_port);
                step_stats.output_rows += counters.rows;
                step_stats.output_bytes += counters.bytes;
            }
        }
    }
}

AnalyzeStepsStats::ElapsedTimesPerStepGroup AnalyzeStepsStats::collectTimingStats(const StepProfiler & step_profiler, const Processors & processors)
{
    ElapsedTimesPerStepGroup elapsed_per_step_group;

    for (const auto & proc : processors)
    {
        const auto & step_id = proc->getStepUniqID();

        if (step_id.empty())
            continue;

        const size_t group = proc->getQueryPlanStepGroup();
        const UInt64 group_elapsed = proc->getElapsedNs();
        if (group_elapsed == 0)
            continue;

        const auto step_group_key = std::make_pair(step_id, group);
        auto & group_stats = stats_by_step_group[step_group_key];
        group_stats.sum_elapsed_ns += group_elapsed;
        ++group_stats.total_num_processors;
        elapsed_per_step_group[step_group_key].insert(group_elapsed);

        if (group_stats.wall_clock_time_ns == 0)
            if (const auto * clock = step_profiler.findClockForStep(step_id, group))
                group_stats.wall_clock_time_ns = clock->getStepWallTime();
    }

    return elapsed_per_step_group;
}

void AnalyzeStepsStats::computeDistribution(const ElapsedTimesPerStepGroup & elapsed_per_step_group)
{
    /// Compute the per-processor elapsed time distribution for each (step, group).
    /// The multiset is already sorted, so min/max are its bounds and the median is the middle element.
    for (const auto & [step_group_key, elapsed] : elapsed_per_step_group)
    {
        if (elapsed.empty())
            continue;

        auto & group_stats = stats_by_step_group[step_group_key];
        group_stats.min_elapsed_ns = *elapsed.begin();
        group_stats.max_elapsed_ns = *elapsed.rbegin();

        const size_t count = elapsed.size();
        const auto middle = std::next(elapsed.begin(), count / 2);
        group_stats.median_elapsed_ns = (count % 2 == 1)
            ? *middle
            : (*std::prev(middle) + *middle) / 2;
    }
}

void AnalyzeStepsStats::computeJoinBranchCosts()
{
    CardinalityByJoinStep cardinality_by_join_step;

    for (const auto & node : plan_snapshot.join_nodes)
    {
        if (!node.is_join)
            continue;

        const auto analyzer_it = plan_snapshot.analyzers.find(node.id);
        if (analyzer_it == plan_snapshot.analyzers.end() || !analyzer_it->second.join_cardinality)
            continue;

        const auto processors_it = processors_by_step.find(node.id);
        if (processors_it == processors_by_step.end())
            continue;

        const StepAnalyzer & analyzer = analyzer_it->second;

        StepAnalysisReport report;
        if (analyzer.reporter)
            report = analyzer.reporter(processors_it->second);

        UInt64 output_rows = 0;
        if (const auto io_it = stats_by_step.find(node.id); io_it != stats_by_step.end())
            output_rows = io_it->second.output_rows;

        cardinality_by_join_step[node.id] = analyzer.join_cardinality(report, output_rows);

        join_raw_reports.emplace(node.id, std::move(report));
    }

    const JoinBranchCosts join_branch_costs(plan_snapshot.join_nodes, plan_snapshot.root_id, cardinality_by_join_step);
    for (auto & [step_id, report] : join_raw_reports)
    {
        MetricGroup cost_group{MetricGroupKey::Cost, {}};
        cost_group.metrics.emplace_back(MetricKey::Actual, optionalQuantity(join_branch_costs.getBranchCost(step_id)));
        report.push_back(std::move(cost_group));
    }
}

StepStatsContext AnalyzeStepsStats::makeContext(
    const String & step_id, const StepAnalyzer & analyzer, const IQueryPlanStep * step_for_interval_timings) const
{
    StepStatsContext context;
    context.execution_query_time_ns = execution_query_time_ns;
    context.max_num_threads_per_query = max_num_threads_per_query;
    context.groups = analyzer.groups;
    context.group_names = analyzer.group_names;

    if (const auto step_stats_it = stats_by_step.find(step_id); step_stats_it != stats_by_step.end())
        context.io = step_stats_it->second;

    for (size_t group : context.groups)
        if (const auto group_stats_it = stats_by_step_group.find(std::make_pair(step_id, group)); group_stats_it != stats_by_step_group.end())
            context.group_stats[group] = group_stats_it->second;

    /// The intervals are laid over the live plan, so only the caller that still has one gets them.
    if (interval_timings && step_for_interval_timings)
        context.time_and_conc_stats = interval_timings->findTiming(step_for_interval_timings);

    return context;
}

AnalyzedStepData AnalyzeStepsStats::analyzeStep(const IQueryPlanStep * step) const
{
    return analyzeStepImpl(step->getUniqID(), step);
}

AnalyzedStepData AnalyzeStepsStats::analyzeStep(const String & step_id) const
{
    return analyzeStepImpl(step_id, nullptr);
}

AnalyzedStepData AnalyzeStepsStats::analyzeStepImpl(const String & step_id, const IQueryPlanStep * step_for_interval_timings) const
{
    /// Every step analysed here comes from the walk that filled the snapshot, so it has an entry.
    /// One without would be shaped as a plain single-stage step and report none of its metrics.
    const auto analyzer_it = plan_snapshot.analyzers.find(step_id);
    chassert(analyzer_it != plan_snapshot.analyzers.end(), "a step is being analysed that the plan snapshot does not know");
    static const StepAnalyzer unknown_step;
    const StepAnalyzer & analyzer = analyzer_it != plan_snapshot.analyzers.end() ? analyzer_it->second : unknown_step;

    StepAnalysisReport raw_report;
    if (const auto report_it = join_raw_reports.find(step_id); report_it != join_raw_reports.end())
    {
        /// A join's report was already read, to accumulate the branch costs.
        raw_report = report_it->second;
    }
    else if (analyzer.reporter)
    {
        StepProcessors step_processors;
        if (const auto processors_it = processors_by_step.find(step_id); processors_it != processors_by_step.end())
            step_processors = processors_it->second;

        raw_report = analyzer.reporter(step_processors);
    }

    auto context_for_step = makeContext(step_id, analyzer, step_for_interval_timings);
    if (analyzer.finish)
        return analyzer.finish(context_for_step, std::move(raw_report));

    return analyzeDefaultStep(context_for_step, std::move(raw_report));
}

void AnalyzeStepsStats::renderStep(const AnalyzedStepData & step_data, WriteBuffer & out, const std::string & prefix, bool processors_info) const
{
    for (const auto & group : step_data.step_metric_groups)
    {
        if (group.key == MetricGroupKey::IO)
        {
            printIOGroup(group, out, prefix);
            continue;
        }

        if (group.key == MetricGroupKey::Time)
        {
            printTimeGroup(group, out, prefix);
            continue;
        }

        if (group.key == MetricGroupKey::Concurrency)
        {
            printConcurrencyGroup(group, out, prefix);
            continue;
        }

        printMetricGroup(group, out, prefix);
    }

    for (const auto & stage : step_data.stage_reports)
        printStage(stage, step_data.label_stages, out, prefix, processors_info);
}

void AnalyzeStepsStats::printStepStats(const IQueryPlanStep * step, WriteBuffer & out, const std::string & prefix, bool processors_info) const
{
    if (!step)
        return;

    renderStep(analyzeStep(step), out, prefix, processors_info);
}

};
