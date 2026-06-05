#include <algorithm>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <Processors/Port.h>
#include <Processors/QueryPlan/AnalyzePlanStats.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <base/defines.h>
#include <base/types.h>

namespace DB
{

AnalyzeStepsStats::AnalyzeStepsStats(const QueryPipeline & pipeline, UInt64 execution_query_time_ns_)
: max_num_threads_per_query(pipeline.getNumThreads())
, execution_query_time_ns(execution_query_time_ns_)
{

    steps_to_stats.reserve(64);

    const auto & processors = pipeline.getProcessors();

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

        if (stats.wall_clock_time_ns == 0)
        {
            chassert(proc->getStepWallClock().get());
            stats.wall_clock_time_ns = proc->getStepWallClock()->getStepWallTime();
        }
    }
}

void AnalyzeStepsStats::printStepStats(const IQueryPlanStep * step, WriteBuffer & out, const std::string & prefix) const
{
    if (!step)
        return ;

    for (size_t group = 0; ;group++)
    {
        auto key = std::make_pair(step, group);
        const auto it = steps_to_stats.find(key);
        if (it == steps_to_stats.end())
            return;  
        
        const auto & stats = it->second;
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
        out << (group_name.empty()? ": " : std::format(" ({}): ", group_name));
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
    }
}

};
