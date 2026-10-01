#pragma once

#include <list>
#include <memory>

namespace DB
{

class InputPort;
class OutputPort;
using InputPorts = std::list<InputPort>; // STYLE_CHECK_ALLOW_STD_CONTAINERS
using OutputPorts = std::list<OutputPort>; // STYLE_CHECK_ALLOW_STD_CONTAINERS

class IProcessor;
using ProcessorPtr = std::shared_ptr<IProcessor>;
using Processors = std::list<ProcessorPtr>; // STYLE_CHECK_ALLOW_STD_CONTAINERS

}
