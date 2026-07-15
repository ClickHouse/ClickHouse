#include <algorithm>
#include <iterator>
#include <set>
#include <type_traits>
#include <unordered_map>
#include <variant>
#include <vector>
#include <Processors/Port.h>
#include <Processors/QueryPlan/AnalyzePlanStats.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/StepAnalyzeInfo.h>
#include <Processors/StepWallClock.h>
#include <Processors/StepWallClockRegistry.h>
#include <base/defines.h>
#include <base/types.h>

namespace DB
{

namespace
{

/// `Format::Time` is shown as an absolute duration plus its share of the stage time
/// (sum of elapsed time across the stage's processors), uniformly for any producing step.
String formatStepMetricValue(const StepMetric & metric, UInt64 stage_sum_elapsed_ns)
{
    if (metric.format == StepMetric::Format::Raw)
        return std::visit([](const auto & value) -> String
        {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, std::string>)
                return value;
            else
                return fmt::format("{}", value);
        }, metric.value);

    const double numeric = std::visit([](const auto & value) -> double
    {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_arithmetic_v<T>)
            return static_cast<double>(value);
        else
            return 0.0;
    }, metric.value);

    switch (metric.format)
    {
        case StepMetric::Format::Bytes:
            return formatReadableSizeWithDecimalSuffix(numeric);
        case StepMetric::Format::Quantity:
            return formatReadableQuantity(numeric);
        case StepMetric::Format::Time:
        {
            String result = formatReadableTime(numeric);
            if (stage_sum_elapsed_ns != 0)
                result += fmt::format(" ({:.1f}%)", 100.0 * numeric / static_cast<double>(stage_sum_elapsed_ns));
            return result;
        }
        case StepMetric::Format::Percent:
            return fmt::format("{:.2f}%", numeric);
        case StepMetric::Format::Ratio:
            return fmt::format("{:.2f}", numeric);
        case StepMetric::Format::Raw:
            return {};
    }
    return {};
}

void printMetricGroup(const MetricGroup & metric_group, WriteBuffer & out, const std::string & prefix)
{
    if (metric_group.metrics.empty())
        return;

    out << prefix;
    if (!metric_group.label.empty())
        out << metric_group.label << ": ";

    bool first = true;
    for (const auto & metric : metric_group.metrics)
    {
        if (!first)
            out << " · ";
        first = false;
        out << metric.name << " " << formatStepMetricValue(metric, 0);
    }
    out << "\n";
}

MetricGroup makeIOGroup(const StepStats & step_stats)
{
    MetricGroup io_group;
    io_group.label = "I/O";
    io_group.metrics.emplace_back("input rows", step_stats.input_rows, StepMetric::Format::Quantity);
    io_group.metrics.emplace_back("output rows", step_stats.output_rows, StepMetric::Format::Quantity);
    io_group.metrics.emplace_back("input bytes", step_stats.input_bytes, StepMetric::Format::Bytes);
    io_group.metrics.emplace_back("output bytes", step_stats.output_bytes, StepMetric::Format::Bytes);
    return io_group;
}

UInt64 findQuantity(const MetricGroup & metric_group, const std::string & name)
{
    for (const auto & metric : metric_group.metrics)
        if (metric.name == name)
            if (const auto * quantity = std::get_if<UInt64>(&metric.value))
                return *quantity;
    return 0;
}

void printIOGroup(const MetricGroup & io_group, WriteBuffer & out, const std::string & prefix)
{
    const UInt64 input_rows = findQuantity(io_group, "input rows");
    const UInt64 output_rows = findQuantity(io_group, "output rows");
    const UInt64 input_bytes = findQuantity(io_group, "input bytes");
    const UInt64 output_bytes = findQuantity(io_group, "output bytes");

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

void enrichJoinSides(StepAnalysisReport & report, UInt64 step_output_rows)
{
    for (auto & metric_group : report)
    {
        if (metric_group.label != "left" && metric_group.label != "right")
            continue;

        const UInt64 rows = findQuantity(metric_group, "rows");
        const UInt64 matched = findQuantity(metric_group, "matched");

        double selectivity_percent = rows ? 100.0 * static_cast<double>(matched) / static_cast<double>(rows) : 0.0;
        metric_group.metrics.emplace_back("selectivity", selectivity_percent, StepMetric::Format::Percent);

        const double fanout = rows ? static_cast<double>(step_output_rows) / static_cast<double>(matched) : 0.0;
        metric_group.metrics.emplace_back("fanout", fanout, StepMetric::Format::Ratio);
    }
}

}

AnalyzeStepsStats::AnalyzeStepsStats(const QueryPipeline & pipeline, UInt64 execution_query_time_ns_)
: max_num_threads_per_query(pipeline.getNumThreads())
, execution_query_time_ns(execution_query_time_ns_)
{
    const auto & processors = pipeline.getProcessors();

    collectIOStats(processors);
    const auto elapsed_per_step_group = collectTimingStats(pipeline, processors);
    computeDistribution(elapsed_per_step_group);
}

void AnalyzeStepsStats::collectIOStats(const Processors & processors)
{
    auto crosses_step_boundary = [](const IProcessor & owner, const IProcessor & neighbour)
    {
        return owner.getQueryPlanStep() != neighbour.getQueryPlanStep();
    };

    for (const auto & proc : processors)
    {
        const auto * step = proc->getQueryPlanStep();

        if (!step)
            continue;

        auto & step_stats = stats_by_step[step];

        const auto step_group_key = std::make_pair(step, proc->getQueryPlanStepGroup());
        processors_by_step_group[step_group_key].push_back(proc.get());

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

AnalyzeStepsStats::ElapsedTimesPerStepGroup AnalyzeStepsStats::collectTimingStats(const QueryPipeline & pipeline, const Processors & processors)
{
    ElapsedTimesPerStepGroup elapsed_per_step_group;

    for (const auto & proc : processors)
    {
        const auto * step = proc->getQueryPlanStep();

        if (!step)
            continue;

        const size_t group = proc->getQueryPlanStepGroup();
        const UInt64 group_elapsed = proc->getElapsedNs();
        if (group_elapsed == 0)
            continue;

        const auto step_group_key = std::make_pair(step, group);
        auto & group_stats = stats_by_step_group[step_group_key];
        group_stats.sum_elapsed_ns += group_elapsed;
        ++group_stats.total_num_processors;
        elapsed_per_step_group[step_group_key].insert(group_elapsed);

        if (group_stats.wall_clock_time_ns == 0)
        {
            if (const auto * registry = pipeline.getStepClocks())
                if (const auto * clock = registry->find(step, group))
                    group_stats.wall_clock_time_ns = clock->getStepWallTime();
        }
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

void AnalyzeStepsStats::printStepStats(const IQueryPlanStep * step, WriteBuffer & out, const std::string & prefix, bool processors_info) const
{
    if (!step)
        return ;

    StepStats step_stats;
    if (const auto step_it = stats_by_step.find(step); step_it != stats_by_step.end())
        step_stats = step_it->second;

    ProcessorsByGroup processors_by_group;
    for (size_t group : step->getStepGroups())
    {
        const auto group_it = processors_by_step_group.find(std::make_pair(step, group));
        if (group_it != processors_by_step_group.end())
            processors_by_group[group] = group_it->second;
    }

    StepAnalysisReport report = step->getAnalysisReport(processors_by_group);

    if (step->getName() == "Join")
        enrichJoinSides(report, step_stats.output_rows);

    /// Groups labelled by an execution stage name are phase-scoped: they are merged into that
    /// Stage line below. Everything else renders as a standalone line here.
    std::set<std::string> stage_names;
    for (size_t group : step->getStepGroups())
    {
        const std::string group_name = step->getStepGroupName(group);
        if (!group_name.empty())
            stage_names.insert(group_name);
    }

    std::unordered_map<std::string, MetricList> phase_metrics;

    for (auto & metric_group : report)
    {
        if (stage_names.contains(metric_group.label))
            phase_metrics.emplace(metric_group.label, std::move(metric_group.metrics));
        else
            printMetricGroup(metric_group, out, prefix);
    }

    /// Collect the stages (step groups) that actually have timing stats.
    std::vector<std::pair<size_t, const StepGroupStats *>> stages;
    for (size_t group : step->getStepGroups())
    {
        const auto group_it = stats_by_step_group.find(std::make_pair(step, group));
        if (group_it != stats_by_step_group.end())
            stages.emplace_back(group, &group_it->second);
    }

    printIOGroup(makeIOGroup(step_stats), out, prefix);

    /// A step that splits into several stages labels each one ("Stage (<name>): ..."); a single
    /// stage is also labeled when it carries a non-empty group name (e.g. a join's "probe").
    const bool label_stages
        = stages.size() > 1 || (!stages.empty() && !step->getStepGroupName(stages.front().first).empty());

    for (const auto & [group, group_stats_ptr] : stages)
    {
        const auto & group_stats = *group_stats_ptr;

        const double share_of_query_time = execution_query_time_ns != 0
            ? 100.0 * static_cast<double>(group_stats.wall_clock_time_ns) / static_cast<double>(execution_query_time_ns)
            : 0.0;
        const double parallelism = group_stats.wall_clock_time_ns
            ? static_cast<double>(group_stats.sum_elapsed_ns) / static_cast<double>(group_stats.wall_clock_time_ns)
            : 0.0;
        const UInt64 max_parallelism = std::min(max_num_threads_per_query, group_stats.total_num_processors);

        const std::string group_name = step->getStepGroupName(group);

        out << prefix << "  ";
        if (label_stages)
        {
            out << "Stage";
            if (!group_name.empty())
                out << " (" << group_name << ")";
            out << ": ";
        }
        out << "time " << formatReadableTime(static_cast<double>(group_stats.wall_clock_time_ns))
            << fmt::format(" ({:.1f}%)", share_of_query_time) << " · parallelism "
            << (group_stats.wall_clock_time_ns ? fmt::format("{:.2f}/{}", parallelism, max_parallelism) : "Unknown");

        if (const auto phase_it = phase_metrics.find(group_name); phase_it != phase_metrics.end())
            for (const auto & metric : phase_it->second)
                out << " · " << metric.name << " " << formatStepMetricValue(metric, group_stats.sum_elapsed_ns);

        out << "\n";

        if (processors_info)
            out << prefix << "    Time per processor (" << group_stats.total_num_processors << "): "
                << "min " << formatReadableTime(static_cast<double>(group_stats.min_elapsed_ns))
                << " · median " << formatReadableTime(static_cast<double>(group_stats.median_elapsed_ns))
                << " · max " << formatReadableTime(static_cast<double>(group_stats.max_elapsed_ns))
                << " · sum " << formatReadableTime(static_cast<double>(group_stats.sum_elapsed_ns)) << "\n";
    }
}

};
