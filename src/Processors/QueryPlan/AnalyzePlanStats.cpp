#include <algorithm>
#include <iterator>
#include <set>
#include <unordered_map>
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

    /// Per-processor elapsed times collected per (step, group) to compute the distribution below.
    /// A multiset keeps the values sorted (no explicit sort needed) and, unlike a plain set,
    /// preserves duplicate timings so that the median stays statistically correct.
    using ElapsedTimes = std::multiset<UInt64>;
    using ElapsedTimesPerStep = std::unordered_map<StepAndGroup, ElapsedTimes, boost::hash<StepAndGroup>>;
    ElapsedTimesPerStep elapsed_per_step;

    auto crosses_step_boundary = [](const IProcessor & owner, const IProcessor & neighbour)
    {
        return owner.getQueryPlanStep() != neighbour.getQueryPlanStep();
    };

    for (const auto & proc : processors)
    {
        const auto * step_ptr = proc->getQueryPlanStep();

        if (!step_ptr)
            continue;

        auto & step_io_stats = step_io[step_ptr];

        for (const auto & input_port : proc->getInputs())
        {
            if (!input_port.isConnected())
                continue;

            if (crosses_step_boundary(*proc, input_port.getOutputPort().getProcessor()))
            {
                const auto counters = proc->getPortDataCounters(input_port);
                step_io_stats.input_rows += counters.rows;
                step_io_stats.input_bytes += counters.bytes;
            }
        }

        for (const auto & output_port : proc->getOutputs())
        {
            if (!output_port.isConnected())
                continue;

            if (crosses_step_boundary(*proc, output_port.getInputPort().getProcessor()))
            {
                const auto counters = proc->getPortDataCounters(output_port);
                step_io_stats.output_rows += counters.rows;
                step_io_stats.output_bytes += counters.bytes;
            }
        }

        for (size_t group = 0; group < IProcessor::MAX_STEP_GROUPS; ++group)
        {
            const UInt64 group_elapsed = proc->getElapsedNs(group);
            if (group_elapsed == 0)
                continue;

            const auto proc_key = std::make_pair(step_ptr, group);
            auto & proc_stats = steps_to_stats[proc_key];
            proc_stats.sum_elapsed_ns += group_elapsed;
            ++proc_stats.total_num_processors;
            elapsed_per_step[proc_key].insert(group_elapsed);

            if (proc_stats.wall_clock_time_ns == 0)
            {
                if (const auto * registry = pipeline.getStepClocks())
                    if (const auto * clock = registry->find(step_ptr, group))
                        proc_stats.wall_clock_time_ns = clock->getStepWallTime();
            }
        }
    }

    /// Compute the per-processor elapsed time distribution for each (step, group).
    /// The multiset is already sorted, so min/max are its bounds and the median is the middle element.
    for (const auto & [key, elapsed] : elapsed_per_step)
    {
        if (elapsed.empty())
            continue;

        auto & stats = steps_to_stats[key];
        stats.min_elapsed_ns = *elapsed.begin();
        stats.max_elapsed_ns = *elapsed.rbegin();

        const size_t n = elapsed.size();
        const auto middle = std::next(elapsed.begin(), n / 2);
        stats.median_elapsed_ns = (n % 2 == 1)
            ? *middle
            : (*std::prev(middle) + *middle) / 2;
    }
}

void AnalyzeStepsStats::printStepStats(const IQueryPlanStep * step, WriteBuffer & out, const std::string & prefix, bool processors_info) const
{
    if (!step)
        return ;

    StepIoStats io_stats;
    if (const auto io_it = step_io.find(step); io_it != step_io.end())
        io_stats = io_it->second;

    const bool empty_io = (io_stats.input_bytes == 0 && io_stats.output_bytes == 0);

    out << prefix << "Actual: rows "
        << formatReadableQuantity(static_cast<double>(io_stats.input_rows)) << " → "
        << formatReadableQuantity(static_cast<double>(io_stats.output_rows));

    if (io_stats.input_rows != io_stats.output_rows && io_stats.input_rows != 0)
    {
        const double selectivity = 100.0 * static_cast<double>(io_stats.output_rows) / static_cast<double>(io_stats.input_rows);
        out << fmt::format(" ({:.2f}%)", selectivity);
    }

    if (!empty_io)
        out << " · " << formatReadableSizeWithDecimalSuffix(static_cast<double>(io_stats.input_bytes))
            << " → " << formatReadableSizeWithDecimalSuffix(static_cast<double>(io_stats.output_bytes));

    out << "\n";

    for (size_t group : step->getStepGroups())
    {
        const auto group_it = steps_to_stats.find(std::make_pair(step, group));
        if (group_it == steps_to_stats.end())
            continue;

        const auto & group_stats = group_it->second;

        const double share_of_query_time = execution_query_time_ns != 0
            ? 100.0 * static_cast<double>(group_stats.wall_clock_time_ns) / static_cast<double>(execution_query_time_ns)
            : 0.0;
        const double parallelism = group_stats.wall_clock_time_ns
            ? static_cast<double>(group_stats.sum_elapsed_ns) / static_cast<double>(group_stats.wall_clock_time_ns)
            : 0.0;
        const UInt64 max_parallelism = std::min(max_num_threads_per_query, group_stats.total_num_processors);

        const std::string group_name = step->getStepGroupName(group);

        out << prefix << "  ";
        if (!group_name.empty())
            out << group_name << ": ";
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
