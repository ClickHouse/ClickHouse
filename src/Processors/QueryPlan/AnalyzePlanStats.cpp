#include <Processors/QueryPlan/AnalyzePlanStats.h>

namespace DB
{

AnalyzeStepsStats::AnalyzeStepsStats(const QueryPipeline & pipeline)
{
    const auto & processors = pipeline.getProcessors();

    for(const auto & proc : processors)
    {
        const auto * step_ptr = proc->getQueryPlanStep();
        
        if (!step_ptr)
            continue; 

        auto key = std::make_pair(step_ptr, 0/* proc->getQueryPlanStepGroup() */);
        auto & stats = steps_to_stats[key];
        stats.sum_elapsed_ns += proc->getElapsedNs();

        auto processor_data_stats = proc->getProcessorDataStats();

        stats.input_rows += processor_data_stats.input_rows;
        stats.input_bytes += processor_data_stats.input_bytes;
        stats.output_rows += processor_data_stats.output_rows;
        stats.output_bytes += processor_data_stats.output_bytes;
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
    out << prefix << "Total time: " << stats.sum_elapsed_ns / 1000UL << " µs\n";
    out << prefix << "Input rows: " <<  stats.input_rows << "\n";
    out << prefix << "Output rows: " <<  stats.output_rows << "\n";
    out << prefix << "Input bytes: " <<  stats.input_bytes << "\n";
    out << prefix << "Output bytes: " <<  stats.output_bytes << "\n";
}


};
