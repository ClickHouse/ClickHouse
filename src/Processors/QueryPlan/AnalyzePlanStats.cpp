#include <algorithm>
#include <iterator>
#include <set>
#include <unordered_map>
#include <vector>
#include <Processors/Port.h>
#include <Processors/QueryPlan/AnalyzePlanStats.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/StepWallClock.h>
#include <Processors/StepWallClockRegistry.h>
#include <base/defines.h>
#include <base/types.h>

namespace DB
{

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

    const bool empty_io = (step_stats.input_bytes == 0 && step_stats.output_bytes == 0);

    UInt8 precision_rows_in = step_stats.input_rows < 1000 ? 0 : 2;
    UInt8 precision_rows_out = step_stats.output_rows < 1000 ? 0 : 2;
    UInt8 precision_bytes_in = step_stats.input_bytes < 1000 ? 0 : 2;
    UInt8 precision_bytes_out = step_stats.output_bytes < 1000 ? 0 : 2;

    out << prefix << "I/O: rows "
        << formatReadableQuantity(static_cast<double>(step_stats.input_rows), precision_rows_in) << " → "
        << formatReadableQuantity(static_cast<double>(step_stats.output_rows), precision_rows_out);

    if (step_stats.input_rows != step_stats.output_rows && step_stats.input_rows != 0)
    {
        const double selectivity = 100.0 * static_cast<double>(step_stats.output_rows) / static_cast<double>(step_stats.input_rows);
        out << fmt::format(" ({:.2f}%)", selectivity);
    }

    if (!empty_io)
        out << " · " << formatReadableSizeWithDecimalSuffix(static_cast<double>(step_stats.input_bytes), precision_bytes_in)
            << " → " << formatReadableSizeWithDecimalSuffix(static_cast<double>(step_stats.output_bytes), precision_bytes_out);

    out << "\n";

    /// Collect the stages (step groups) that actually have timing stats.
    std::vector<std::pair<size_t, const StepGroupStats *>> stages;
    for (size_t group : step->getStepGroups())
    {
        const auto group_it = stats_by_step_group.find(std::make_pair(step, group));
        if (group_it != stats_by_step_group.end())
            stages.emplace_back(group, &group_it->second);
    }

    /// A step that splits into several stages labels each one ("Stage (<name>): ..."); a step with a
    /// single stage just reports that stage's time directly, since there is nothing to disambiguate.
    const bool label_stages = stages.size() > 1;

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

        out << prefix << "  ";
        if (label_stages)
        {
            const std::string group_name = step->getStepGroupName(group);
            out << "Stage";
            if (!group_name.empty())
                out << " (" << group_name << ")";
            out << ": ";
        }
        out << "time " << formatReadableTime(static_cast<double>(group_stats.wall_clock_time_ns))
            << fmt::format(" ({:.1f}%)", share_of_query_time) << " · parallelism "
            << (group_stats.wall_clock_time_ns ? fmt::format("{:.2f}/{}", parallelism, max_parallelism) : "Unknown")
            << "\n";

        if (processors_info)
            out << prefix << "    Time per processor (" << group_stats.total_num_processors << "): "
                << "min " << formatReadableTime(static_cast<double>(group_stats.min_elapsed_ns))
                << " · median " << formatReadableTime(static_cast<double>(group_stats.median_elapsed_ns))
                << " · max " << formatReadableTime(static_cast<double>(group_stats.max_elapsed_ns))
                << " · sum " << formatReadableTime(static_cast<double>(group_stats.sum_elapsed_ns)) << "\n";
    }
}

};
