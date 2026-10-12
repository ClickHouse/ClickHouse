#include <Processors/Executors/Runtime/V2/tests/Toolkit/Scripted.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/ExclusiveScope.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Port.h>
#include <Common/DequeWithMemoryTracking.h>

#include <algorithm>
#include <optional>
#include <stdexcept>
#include <utility>

namespace DB::Runtime::V2
{

namespace
{

UInt64 valueOf(const Chunk & chunk)
{
    return chunk.getColumns().front()->getUInt(0);
}

InputPorts makeInputs(size_t count)
{
    InputPorts inputs;
    for (size_t index = 0; index < count; ++index)
        inputs.emplace_back(makeHeader());
    return inputs;
}

OutputPorts makeOutputs(size_t count)
{
    OutputPorts outputs;
    for (size_t index = 0; index < count; ++index)
        outputs.emplace_back(makeHeader());
    return outputs;
}

class Scripted : public IProcessor
{
    friend Action V2::emit(UInt64 value);
    friend Action V2::addInput(ProcessorPtr new_input);
    friend Action V2::addOutput(ProcessorPtr new_output);
    friend VectorWithMemoryTracking<UInt64> V2::received(const IProcessor & sink);

    Status finishAll()
    {
        actions.clear();
        for (auto & output : outputs)
            output.finish();
        for (auto & input : inputs)
            input.close();
        return Status::Finished;
    }

public:
    Scripted(String name_, size_t inputs_count, size_t outputs_count, VectorWithMemoryTracking<Action> actions_)
        : IProcessor(makeInputs(inputs_count), makeOutputs(outputs_count))
        , name(std::move(name_))
        , actions(std::make_move_iterator(actions_.begin()), std::make_move_iterator(actions_.end()))
        , scope(name)
    {
    }

    String getName() const override
    {
        return name;
    }

    Status prepare() override
    {
        auto section = scope.enter("prepare");

        if (!outputs.empty() && std::ranges::all_of(outputs, [](const OutputPort & output) { return output.isFinished(); }))
            return finishAll();

        if (pending)
        {
            auto output = std::ranges::find_if(outputs, [](const OutputPort & port) { return port.canPush(); });
            if (output == outputs.end())
                return Status::PortFull;

            output->push(std::move(*pending));
            pending.reset();
        }

        if (!actions.empty())
        {
            switch (actions.front().kind)
            {
                case Action::Kind::Work:
                    break;
                case Action::Kind::Async:
                    return Status::Async;
                case Action::Kind::Expand:
                    return Status::UpdatePipeline;
                case Action::Kind::Finish:
                    if (actions.front().on_finish)
                        actions.front().on_finish(*this);
                    return finishAll();
            }
        }

        if (inputs.empty())
            return actions.empty() ? finishAll() : Status::Ready;

        for (auto & input : inputs)
        {
            if (input.isFinished())
                continue;

            input.setNeeded();
            if (!input.hasData())
                continue;

            current = input.pull();
            if (outputs.empty())
                received_values.push_back(valueOf(*current));
            return Status::Ready;
        }

        if (!std::ranges::all_of(inputs, [](const InputPort & input) { return input.isFinished(); }))
            return Status::NeedData;

        return finishAll();
    }

    void work() override
    {
        auto section = scope.enter("work");

        if (!actions.empty() && actions.front().kind == Action::Kind::Work)
        {
            Action action = std::move(actions.front());
            actions.pop_front();
            if (action.on_work)
                action.on_work(*this);
        }

        auto processed = std::exchange(current, std::nullopt);
        if (!outputs.empty())
            pending = std::move(processed);
    }

    int schedule() override
    {
        auto section = scope.enter("schedule");
        return actions.front().on_async(*this);
    }

    void onAsyncJobReady() override
    {
        auto section = scope.enter("onAsyncJobReady");
        actions.pop_front();
    }

    PipelineUpdate updatePipeline() override
    {
        auto section = scope.enter("updatePipeline");
        Action action = std::move(actions.front());
        actions.pop_front();
        return action.on_expand(*this);
    }

private:
    const String name;
    DequeWithMemoryTracking<Action> actions;
    std::optional<Chunk> current;
    std::optional<Chunk> pending;
    VectorWithMemoryTracking<UInt64> received_values;
    Wyrd::ExclusiveScope scope;
};

}

Action work(std::function<void(IProcessor &)> on_work)
{
    Action action;
    action.on_work = std::move(on_work);
    return action;
}

Action emit(UInt64 value)
{
    return work([value](IProcessor & processor) { dynamic_cast<Scripted &>(processor).current = makeChunk(value); });
}

VectorWithMemoryTracking<Action> emits(UInt64 first_value, size_t count)
{
    VectorWithMemoryTracking<Action> actions;
    for (size_t index = 0; index < count; ++index)
        actions.push_back(emit(first_value + index));
    return actions;
}

Action async(std::function<int(IProcessor &)> on_async)
{
    Action action;
    action.kind = Action::Kind::Async;
    action.on_async = std::move(on_async);
    return action;
}

Action fd(int descriptor)
{
    return async([descriptor](IProcessor &) { return descriptor; });
}

Action expand(std::function<IProcessor::PipelineUpdate(IProcessor &)> on_expand)
{
    Action action;
    action.kind = Action::Kind::Expand;
    action.on_expand = std::move(on_expand);
    return action;
}

Action addInput(ProcessorPtr new_input)
{
    return expand([new_input](IProcessor & processor)
    {
        auto & scripted = dynamic_cast<Scripted &>(processor);
        connect(new_input->getOutputs().front(), scripted.inputs.emplace_back(Block(*makeHeader()), &scripted));
        IProcessor::PipelineUpdate update;
        update.to_add.push_back(new_input);
        return update;
    });
}

Action addOutput(ProcessorPtr new_output)
{
    return expand([new_output](IProcessor & processor)
    {
        auto & scripted = dynamic_cast<Scripted &>(processor);
        connect(scripted.outputs.emplace_back(Block(*makeHeader()), &scripted), new_output->getInputs().front());
        IProcessor::PipelineUpdate update;
        update.to_add.push_back(new_output);
        return update;
    });
}

Action finish(std::function<void(IProcessor &)> on_finish)
{
    Action action;
    action.kind = Action::Kind::Finish;
    action.on_finish = std::move(on_finish);
    return action;
}

Action stop()
{
    return finish([](IProcessor &) {});
}

SharedHeader makeHeader()
{
    return std::make_shared<const Block>(Block{ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "value")});
}

Chunk makeChunk(UInt64 value)
{
    Columns columns;
    columns.emplace_back(ColumnUInt64::create(1, value));
    return Chunk(std::move(columns), 1);
}

ProcessorPtr makeSource(String name, VectorWithMemoryTracking<Action> actions)
{
    return std::make_shared<Scripted>(std::move(name), 0, 1, std::move(actions));
}

ProcessorPtr makeTransform(String name, VectorWithMemoryTracking<Action> actions)
{
    return std::make_shared<Scripted>(std::move(name), 1, 1, std::move(actions));
}

ProcessorPtr makeSink(String name, VectorWithMemoryTracking<Action> actions)
{
    return std::make_shared<Scripted>(std::move(name), 1, 0, std::move(actions));
}

VectorWithMemoryTracking<UInt64> received(const IProcessor & sink)
{
    return dynamic_cast<const Scripted &>(sink).received_values;
}

}
