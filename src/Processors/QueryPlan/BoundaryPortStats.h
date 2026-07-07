#pragma once

#include <Processors/IProcessor.h>
#include <Processors/Port.h>

namespace DB
{

template <typename GetSeparationKey, typename OnBoundaryPort>
void forEachBoundaryInputPort(
    const IProcessor & processor,
    GetSeparationKey && get_separation_key,
    OnBoundaryPort && on_boundary_port)
{
    const auto own_key = get_separation_key(processor);
    for (const auto & input_port : processor.getInputs())
    {
        if (!input_port.isConnected())
            continue;

        if (get_separation_key(input_port.getOutputPort().getProcessor()) != own_key)
            on_boundary_port(processor.getPortDataCounters(input_port));
    }
}

template <typename GetSeparationKey, typename OnBoundaryPort>
void forEachBoundaryOutputPort(
    const IProcessor & processor,
    GetSeparationKey && get_separation_key,
    OnBoundaryPort && on_boundary_port)
{
    const auto own_key = get_separation_key(processor);
    for (const auto & output_port : processor.getOutputs())
    {
        if (!output_port.isConnected())
            continue;

        if (get_separation_key(output_port.getInputPort().getProcessor()) != own_key)
            on_boundary_port(processor.getPortDataCounters(output_port));
    }
}

}
