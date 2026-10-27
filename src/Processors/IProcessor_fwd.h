#pragma once

#include <list>
#include <memory>

namespace DB
{

class InputPort;
class OutputPort;
using InputPorts = std::list<InputPort>;
using OutputPorts = std::list<OutputPort>;

class IProcessor;
using ProcessorPtr = std::shared_ptr<IProcessor>;
using Processors = std::list<ProcessorPtr>;

}
