#pragma once

#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Common/VectorWithMemoryTracking.h>

#include <functional>

namespace DB::Runtime::V2
{

struct Action
{
    enum class Kind : UInt8
    {
        Work,
        Async,
        Expand,
        Finish,
    };

    Kind kind = Kind::Work;
    std::function<void(IProcessor &)> on_work;
    std::function<int(IProcessor &)> on_async;
    std::function<IProcessor::PipelineUpdate(IProcessor &)> on_expand;
    std::function<void(IProcessor &)> on_finish;
};

Action work(std::function<void(IProcessor &)> on_work);
Action emit(UInt64 value);
VectorWithMemoryTracking<Action> emits(UInt64 first_value, size_t count);

Action async(std::function<int(IProcessor &)> on_async);
Action fd(int descriptor);

Action expand(std::function<IProcessor::PipelineUpdate(IProcessor &)> on_expand);
Action addInput(ProcessorPtr new_input);
Action addOutput(ProcessorPtr new_output);

Action finish(std::function<void(IProcessor &)> on_finish);
Action stop();

/// Scripted processors

SharedHeader makeHeader();
Chunk makeChunk(UInt64 value);

ProcessorPtr makeSource(String name, VectorWithMemoryTracking<Action> actions);
ProcessorPtr makeTransform(String name, VectorWithMemoryTracking<Action> actions);
ProcessorPtr makeSink(String name, VectorWithMemoryTracking<Action> actions);

VectorWithMemoryTracking<UInt64> received(const IProcessor & sink);

}
