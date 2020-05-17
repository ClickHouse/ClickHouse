#pragma once

#include <Processors/IProcessor.h>


namespace DB
{

/// Look for first Ready or Async processor by depth-first search in needed input ports and full output ports.
/// NOTE: Pipeline must not have cycles.
//template <typename Visit>
//void traverse(IProcessor & processor, Visit && visit)
//{
//    IProcessor::Status status = visit(processor);
//
//    if (status == IProcessor::Status::Ready || status == IProcessor::Status::Async)
//        return;
//
//    if (status == IProcessor::Status::NeedData)
//        for (auto & input : processor.getInputs())
//            if (input.isNeeded() && !input.hasData())
//                traverse(input.getOutputPort().getProcessor(), std::forward<Visit>(visit));
//
//    if (status == IProcessor::Status::PortFull)
//        for (auto & output : processor.getOutputs())
//            if (output.hasData())
//                traverse(output.getInputPort().getProcessor(), std::forward<Visit>(visit));
//}

}
