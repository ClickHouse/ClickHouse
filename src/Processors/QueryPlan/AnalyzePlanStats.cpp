#include <iterator>
#include <type_traits>
#include <unordered_map>
#include <variant>
#include <Processors/Port.h>
#include <Processors/QueryPlan/AnalyzePlanStats.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/StepAnalyzeInfo.h>
#include <Processors/QueryPlan/StepStatsAnalyzer.h>
#include <Processors/StepWallClock.h>
#include <Processors/StepWallClockRegistry.h>
#include <base/defines.h>
#include <base/types.h>

namespace DB
{

namespace
{

String formatStepMetricValue(const StepMetric & metric)
{
    if (std::holds_alternative<std::monostate>(metric.value))
        return "not collected";

    const MetricFormat format = formatOf(metric.key);

    if (format == MetricFormat::Raw)
        return std::visit([](const auto & value) -> String
        {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, std::string>)
                return value;
            else if constexpr (std::is_same_v<T, std::monostate>)
                return {};
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
        << fmt::format(" ({:.1f}%)", stage.share_of_query_time) << " · parallelism "
        << (stage.wall_clock_time_ns ? fmt::format("{:.2f}/{}", stage.parallelism, stage.max_parallelism) : "Unknown");

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

        processors_by_step[step].push_back(proc.get());

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

StepStatsContext AnalyzeStepsStats::makeContext(const IQueryPlanStep * step) const
{
    StepStatsContext context;
    context.step = step;
    context.execution_query_time_ns = execution_query_time_ns;
    context.max_num_threads_per_query = max_num_threads_per_query;

    if (const auto step_stats_it = stats_by_step.find(step); step_stats_it != stats_by_step.end())
        context.io = step_stats_it->second;

    for (size_t group : step->getStepGroups())
        if (const auto group_stats_it = stats_by_step_group.find(std::make_pair(step, group)); group_stats_it != stats_by_step_group.end())
            context.group_stats[group] = group_stats_it->second;

    return context;
}

AnalyzedStepData AnalyzeStepsStats::analyzeStep(const IQueryPlanStep * step) const
{
    StepProcessors step_processors;
    if (const auto processors_it = processors_by_step.find(step); processors_it != processors_by_step.end())
        step_processors = processors_it->second;

    StepAnalysisReport raw_report = step->getAnalysisReport(step_processors);

    auto context_for_step = makeContext(step);
    StepStatsAnalyzer step_stats_generator = getStepStatsAnalyzer(step);

    /// Use the service of a generator, which takes the context (e.g. i/o, total time)
    /// some internal raw metrics, which are specific for each step,  that
    /// with the knowledge of the step will pre-process the metrics before printing
    return step_stats_generator(context_for_step, std::move(raw_report));
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
