#pragma once

#include <Processors/IProcessor.h>
#include <Common/VectorWithMemoryTracking.h>


namespace DB::Runtime::V2
{

void chain(const VectorWithMemoryTracking<ProcessorPtr> & stages);
ProcessorPtr fanIn(const VectorWithMemoryTracking<ProcessorPtr> & sources, const ProcessorPtr & sink);
ProcessorPtr fanOut(const ProcessorPtr & source, const VectorWithMemoryTracking<ProcessorPtr> & sinks);

}
