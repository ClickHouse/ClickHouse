#pragma once

#include <Processors/IProcessor.h>

#include <vector>

namespace DB::Runtime::V2
{

Processors chain(std::vector<ProcessorPtr> stages);
Processors fanIn(std::vector<ProcessorPtr> sources, ProcessorPtr sink);
Processors fanOut(ProcessorPtr source, std::vector<ProcessorPtr> sinks);

}
