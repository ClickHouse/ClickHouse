#pragma once

#include <IO/Operators.h>
#include <Processors/IProcessor.h>

namespace DB
{

/** Print pipeline in "dot" format for GraphViz.
  * You can render it with:
  *  dot -T png < pipeline.dot > pipeline.png
  */
template <typename Processors, typename Statuses>
void printPipeline(const Processors & processors, const Statuses & statuses, WriteBuffer & out, bool with_profile = false)
{
    out << "digraph\n{\n";
    out << "  rankdir=\"LR\";\n";
    out << "  { node [shape = rect]\n";

    std::unordered_map<const void *, std::size_t> pointer_to_id;
    auto get_proc_id = [&pointer_to_id](const IProcessor & proc) -> std::size_t
    {
        auto [it, inserted] = pointer_to_id.try_emplace(&proc, pointer_to_id.size());
        return it->second;
    };


    auto statuses_iter = statuses.begin();

    /// Nodes // TODO quoting and escaping
    for (const auto & processor : processors)
    {
        const auto & description = processor->getDescription();
        out << "    n" << get_proc_id(*processor) << "[label=\"" << processor->getUniqID() << (description.empty() ? "" : ":")
            << description;

        if (statuses_iter != statuses.end())
        {
            out << " (" << IProcessor::statusToName(*statuses_iter) << ")";
            ++statuses_iter;
        }

        if (with_profile)
        {
            out << "\\nExecution time: " << processor->getElapsedNs()/1000.0 << " us"
                << "\\nInput wait time: " << processor->getInputWaitElapsedNs()/1000.0 << " us"
                << "\\nOutput wait time: " << processor->getOutputWaitElapsedNs()/1000.0 << " us"
                << "\\nInput rows: " << processor->getProcessorDataStats().input_rows
                << "\\nInput bytes: " << processor->getProcessorDataStats().input_bytes
                << "\\nOutput rows: " << processor->getProcessorDataStats().output_rows
                << "\\nOutput bytes: " << processor->getProcessorDataStats().output_bytes;
        }

        out << "\"];\n";
    }

    /// Print the outputs which are not in `processors`
    for (const auto & proc : processors)
    {
        for (const auto & port : proc->getOutputs())
        {
            if (!port.isConnected())
                continue;
            const IProcessor & next = port.getInputPort().getProcessor();
            auto [it, inserted] = pointer_to_id.try_emplace(&next, pointer_to_id.size());
            if (!inserted)
                continue;

            auto next_proc_id = it->second;
            const auto & description = next.getDescription();
            out << "    n" << next_proc_id ///
                << "[label=\"" << next.getUniqID() ///
                << ":(output)" ///
                << (description.empty() ? "" : ":") << description ///
                << "\"];\n";
        }
    }

    out << "  }\n";

    /// Edges
    for (const auto & processor : processors)
    {
        auto current_proc_id = get_proc_id(*processor);
        for (const auto & port : processor->getOutputs())
        {
            if (!port.isConnected())
                continue;

            const IProcessor & next = port.getInputPort().getProcessor();
            out << "  n" << current_proc_id << " -> n" << get_proc_id(next) << ";\n";
        }
    }
    out << "}\n";
}

template <typename Processors>
void printPipeline(const Processors & processors, WriteBuffer & out, bool with_profile = false)
{
    printPipeline(processors, std::vector<IProcessor::Status>(), out, with_profile);
}

/// Prints pipeline in compact representation.
/// Group processors by it's name, QueryPlanStep and QueryPlanStepGroup.
/// If QueryPlanStep wasn't set for processor, representation may be not correct.
/// If with_header is set, prints block header for each edge.
void printPipelineCompact(const Processors & processors, WriteBuffer & out, bool with_header);
}
