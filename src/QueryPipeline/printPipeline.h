#pragma once

#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
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

    UnorderedMapWithMemoryTracking<const void *, std::size_t> pointer_to_id;
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
            out << "\\nExecution time: " << static_cast<double>(processor->getElapsedNs()) / 1000.0 << " us"
                << "\\nInput wait time: " << static_cast<double>(processor->getInputWaitElapsedNs()) / 1000.0 << " us"
                << "\\nOutput wait time: " << static_cast<double>(processor->getOutputWaitElapsedNs()) / 1000.0 << " us"
                << "\\nInput rows: " << processor->getProcessorDataStats().input_rows
                << "\\nInput bytes: " << processor->getProcessorDataStats().input_bytes
                << "\\nOutput rows: " << processor->getProcessorDataStats().output_rows
                << "\\nOutput bytes: " << processor->getProcessorDataStats().output_bytes;
        }

        out << "\"];\n";
    }

    out << "  }\n";

    /// Map each input port's shared connection id to the id of the processor that owns it.
    /// Connected ports share one state, so an output port can find its peer through this map
    /// without dereferencing the peer processor, which may already be destroyed (e.g. removed
    /// during pipeline teardown while a survivor still references it through a port).
    UnorderedMapWithMemoryTracking<const void *, std::size_t> input_connection_to_id;
    for (const auto & processor : processors)
    {
        auto proc_id = get_proc_id(*processor);
        for (const auto & port : processor->getInputs())
            if (port.isConnected())
                input_connection_to_id.try_emplace(port.getConnectionId(), proc_id);
    }

    /// Edges
    for (const auto & processor : processors)
    {
        auto current_proc_id = get_proc_id(*processor);
        for (const auto & port : processor->getOutputs())
        {
            if (!port.isConnected())
                continue;

            /// Only draw the edge if the peer input port belongs to a processor in this set.
            auto it = input_connection_to_id.find(port.getConnectionId());
            if (it == input_connection_to_id.end())
                continue;

            out << "  n" << current_proc_id << " -> n" << it->second << ";\n";
        }
    }
    out << "}\n";
}

template <typename Processors>
void printPipeline(const Processors & processors, WriteBuffer & out, bool with_profile = false)
{
    printPipeline(processors, VectorWithMemoryTracking<IProcessor::Status>(), out, with_profile);
}

/// Prints pipeline in compact representation.
/// Group processors by it's name, QueryPlanStep and QueryPlanStepGroup.
/// If QueryPlanStep wasn't set for processor, representation may be not correct.
/// If with_header is set, prints block header for each edge.
void printPipelineCompact(const Processors & processors, WriteBuffer & out, bool with_header);
}
