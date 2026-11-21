#pragma once

#include <Common/ListWithMemoryTracking.h>
#include <memory>

namespace DB
{

class InputPort;
class OutputPort;
using InputPorts = ListWithMemoryTracking<InputPort>;
using OutputPorts = ListWithMemoryTracking<OutputPort>;

class IProcessor;
using ProcessorPtr = std::shared_ptr<IProcessor>;
using Processors = ListWithMemoryTracking<ProcessorPtr>;

}
