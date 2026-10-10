#pragma once

#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>

#include <functional>
#include <vector>

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

ProcessorPtr makeSource(String name, std::vector<Action> actions);
ProcessorPtr makeTransform(String name, std::vector<Action> actions);
ProcessorPtr makeSink(String name, std::vector<Action> actions);

std::vector<UInt64> received(const IProcessor & sink);

}
