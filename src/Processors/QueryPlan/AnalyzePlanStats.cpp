#include <algorithm>
#include <iterator>
#include <set>
#include <unordered_map>
#include <Processors/Port.h>
#include <Processors/QueryPlan/AnalyzePlanStats.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/StepWallClock.h>
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

    for (const auto & proc : processors)
    {
        const auto * step_ptr = proc->getQueryPlanStep();

        if (!step_ptr)
            continue;

        const auto key = std::make_pair(step_ptr, proc->getQueryPlanStepGroup());
        auto & stats = steps_to_stats[key];

        auto is_same_step = [](const IProcessor & lhs, const IProcessor & rhs) -> bool
        {
            return lhs.getStepUniqID() == rhs.getStepUniqID()
            && lhs.getQueryPlanStepGroup() == rhs.getQueryPlanStepGroup();
        };

        auto input_is_boundary = [&](const InputPort & in_port)
        {
            return in_port.isConnected() && !is_same_step(*proc, in_port.getOutputPort().getProcessor());
        };

        auto output_is_boundary = [&](const OutputPort & out_port)
        {
            return out_port.isConnected() && !is_same_step(*proc, out_port.getInputPort().getProcessor());
        };

        for (const auto & in_port : proc->getInputs())
        {
            if (input_is_boundary(in_port))
            {
                auto port_stats = proc->getPortDataCounters(in_port);
                stats.input_rows += port_stats.rows;
                stats.input_bytes += port_stats.bytes;
            }
        }

        for (const auto & out_port : proc->getOutputs())
        {
            if (output_is_boundary(out_port))
            {
                auto port_stats = proc->getPortDataCounters(out_port);
                stats.output_rows += port_stats.rows;
                stats.output_bytes += port_stats.bytes;
            }
        }

        stats.sum_elapsed_ns += proc->getElapsedNs();
        ++stats.total_num_processors;
        elapsed_per_step[key].insert(proc->getElapsedNs());

        if (stats.wall_clock_time_ns == 0)
        {
            chassert(proc->getStepWallClock().get());
            stats.wall_clock_time_ns = proc->getStepWallClock()->getStepWallTime();
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

    for (auto it = steps_to_stats.lower_bound(std::make_pair(step, 0ul)); it != steps_to_stats.end() && it->first.first == step; ++it)
    {
        size_t group = it->first.second;
        auto key = std::make_pair(step, group);
        const auto it_stats = steps_to_stats.find(key);
        if (it_stats == steps_to_stats.end())
            return;

        const auto & stats = it_stats->second;
        std::string rows_input = formatReadableQuantity(static_cast<double>(stats.input_rows));
        std::string rows_output = formatReadableQuantity(static_cast<double>(stats.output_rows));
        bool empty_io = (stats.input_bytes == stats.output_bytes && stats.input_bytes == 0);
        bool print_selectivity = stats.input_rows != stats.output_rows;
        std::string in_bytes = empty_io ? "" : formatReadableSizeWithDecimalSuffix(static_cast<double>(stats.input_bytes));
        std::string out_bytes = empty_io ? "" : formatReadableSizeWithDecimalSuffix(static_cast<double>(stats.output_bytes));
        std::string time = formatReadableTime(static_cast<double>(stats.wall_clock_time_ns));
        const double step_time_per_total_time_ns = execution_query_time_ns != 0
                ? 100.0 * static_cast<double>(stats.wall_clock_time_ns) / static_cast<double>(execution_query_time_ns)
                : 0.0;

        std::string percentage_of_step_time = fmt::format(" ({:.1f}%)", step_time_per_total_time_ns);

        std::string group_name = step->getStepGroupName(group);

        out << prefix << "Actual";
        out << (group_name.empty()? ": " : fmt::format(" ({}): ", group_name));
        out << "rows " << rows_input << " → " << rows_output;

        if (print_selectivity && stats.input_rows != 0)
        {
            const double selectivity = 100.0 * static_cast<double>(stats.output_rows) / static_cast<double>(stats.input_rows);
            out << fmt::format(" ({:.2f}%)", selectivity);
        }


        out << " · ";
        out << "time " << time << percentage_of_step_time;
        std::string info_about_io = !empty_io ? " · " + in_bytes + " → " + out_bytes : "";
        out << info_about_io;
        double parallelism = stats.wall_clock_time_ns
        ? static_cast<double>(stats.sum_elapsed_ns) / static_cast<double>(stats.wall_clock_time_ns)
        : 0.0;
        std::string parallelism_string = stats.wall_clock_time_ns ? fmt::format("{:.2f}", parallelism) : "Unknown";
        UInt64 max_parallelism_per_step = std::min(max_num_threads_per_query, stats.total_num_processors);
        std::string max_parallelism_per_step_string = stats.wall_clock_time_ns ? fmt::format("/{}", max_parallelism_per_step) : "";
        out << " · parallelism " << parallelism_string << max_parallelism_per_step_string << "\n";

        if (processors_info)
        {
            out << prefix << "Time per processor (" << stats.total_num_processors << "): "
                << "min " << formatReadableTime(static_cast<double>(stats.min_elapsed_ns))
                << " · median " << formatReadableTime(static_cast<double>(stats.median_elapsed_ns))
                << " · max " << formatReadableTime(static_cast<double>(stats.max_elapsed_ns))
                << " · sum " << formatReadableTime(static_cast<double>(stats.sum_elapsed_ns)) << "\n";
        }
    }
}

};
