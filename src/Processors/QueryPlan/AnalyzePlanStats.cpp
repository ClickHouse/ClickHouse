#include <algorithm>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <Processors/Port.h>
#include <Processors/QueryPlan/AnalyzePlanStats.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <base/defines.h>

namespace DB
{

AnalyzeStepsStats::AnalyzeStepsStats(const QueryPipeline & pipeline)
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

        if (stats.wall_clock_time == 0)
        {
            chassert(proc->getStepWallClock().get());
            stats.wall_clock_time = proc->getStepWallClock()->getStepWallTime();
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
        std::string time = formatReadableTime(static_cast<double>(stats.wall_clock_time));

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
        out << "time " << time;
        std::string info_about_io = !empty_io ? " · " + in_bytes + " → " + out_bytes : "";
        out << info_about_io;
        double parallelism = stats.wall_clock_time
        ? static_cast<double>(stats.sum_elapsed_ns) / static_cast<double>(stats.wall_clock_time)
        : 0.0; 
        std::string parallelism_string = stats.wall_clock_time ? fmt::format("{:.2f}", parallelism) : "Unknown";
        out << " · parallelism (l) " << parallelism_string << "\n";
    }
}

};
