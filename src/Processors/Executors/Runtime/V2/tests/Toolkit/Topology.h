#pragma once

#include <Processors/IProcessor.h>

#include <vector>

namespace DB::Runtime::V2
{

void chain(const std::vector<ProcessorPtr> & stages);
ProcessorPtr fanIn(const std::vector<ProcessorPtr> & sources, const ProcessorPtr & sink);
ProcessorPtr fanOut(const ProcessorPtr & source, const std::vector<ProcessorPtr> & sinks);

}
