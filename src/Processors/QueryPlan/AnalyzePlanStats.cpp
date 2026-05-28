#include <algorithm>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <Processors/Port.h>
#include <Processors/QueryPlan/AnalyzePlanStats.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

AnalyzeStepsStats::AnalyzeStepsStats(const QueryPipeline & pipeline)
{
    // using SetOfPorts = std::unordered_set<const Port *>;
    // using StepToPorts = std::unordered_map<StepAndGroup, SetOfPorts>;

    // /// To count exact number of rows and bytes per step input/output 
    // /// we need to count only the ones on the boundary with another step 
    // StepToPorts step_to_in_ports;
    // StepToPorts step_to_out_ports;

    // step_to_in_ports.reserve(64);
    // step_to_out_ports.reserve(64);
    steps_to_stats.reserve(64);

    const auto & processors = pipeline.getProcessors();

    // for(const auto & proc : processors)
    // {
    //     const auto * step_ptr = proc->getQueryPlanStep();

    //     if (!step_ptr)
    //         continue; 

    //     const auto key = std::make_pair(step_ptr, 0/* proc->getQueryPlanStepGroup() */);
    //     steps_to_stats.try_emplace(key);
    //     auto & set_of_inputs = step_to_in_ports.try_emplace(key).first->second;
    //     auto & set_of_outputs = step_to_out_ports.try_emplace(key).first->second;

    //     for (const auto & in_port : proc->getInputs())
    //         set_of_inputs.insert(&in_port);

    //     for (const auto & out_port : proc->getOutputs())
    //         set_of_outputs.insert(&out_port);
    // } 

    for(const auto & proc : processors)
    {
        const auto * step_ptr = proc->getQueryPlanStep();
        
        if (!step_ptr)
            continue; 

        const auto key = std::make_pair(step_ptr, 0/* proc->getQueryPlanStepGroup() */);
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
    }
}

void AnalyzeStepsStats::printStepStats(const IQueryPlanStep * step, WriteBuffer & out, const std::string & prefix) const
{
    if (!step)
        return ;

    auto key = std::make_pair(step, 0);
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
    std::string time = formatReadableTime(static_cast<double>(stats.sum_elapsed_ns));

    out << prefix << "Actual: " << "rows " << rows_input << " → " << rows_output;

    if (print_selectivity && stats.input_rows != 0)
    {
        const double selectivity = 100.0 * static_cast<double>(stats.output_rows) / static_cast<double>(stats.input_rows);
        out << fmt::format(" ({:.2f}%)", selectivity);
    }

    out << " · ";
    out << "time " << time;
    std::string info_about_io = !empty_io ? " · " + in_bytes + " → " + out_bytes : "";
    out << info_about_io << "\n";
}

};
