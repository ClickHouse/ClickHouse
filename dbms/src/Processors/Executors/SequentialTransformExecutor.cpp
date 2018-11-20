#include <Processors/Executors/SequentialTransformExecutor.h>
#include <Processors/ISink.h>
#include <Processors/ISource.h>
#include <Processors/printPipeline.h>

namespace DB
{

namespace
{

class SequentialTransformSource : public IProcessor
{
public:

    explicit SequentialTransformSource(Block header) : IProcessor({}, {std::move(header)}), output(outputs.front()) {}

    String getName() const override { return "SequentialTransformSource"; }

    Status prepare() override
    {
        if (output.hasData())
            return Status::PortFull;

        if (!output.isNeeded())
            return Status::Unneeded;

        if (!input_block)
            return Status::NeedData;

        return Status::Ready;
    }

    void work() override
    {
        output.push(std::move(input_block));
    }

    Block input_block;

protected:
    OutputPort & output;
};

class SequentialTransformSink : public ISink
{
public:
    explicit SequentialTransformSink(Block header) : ISink(std::move(header)) {}

    void consume(Block block) override { output_block = std::move(block); }
    String getName() const override { return "SequentialTransformSink"; }

    Block output_block;
};

}

SequentialTransformExecutor::SequentialTransformExecutor(Processors processors, InputPort & input, OutputPort & output)
{
    auto source = std::make_shared<SequentialTransformSource>(input.getHeader());
    auto sink = std::make_shared<SequentialTransformSink>(output.getHeader());

    connect(source->getOutputs().front(), input);
    connect(output, sink->getInputs().front());

    input_block = &source->input_block;
    output_block = &sink->output_block;

    processors.emplace_back(std::move(source));
    processors.emplace_back(std::move(sink));

    executor = std::make_shared<SequentialPipelineExecutor>(processors);
}

void SequentialTransformExecutor::execute(Block & block)
{
    std::lock_guard<std::mutex> guard(mutex);

    // printPipeline(dynamic_cast<const SequentialPipelineExecutor *>(executor.get())->getProcessors());

    *input_block = std::move(block);
    output_block->clear();

    while (!(*output_block))
    {
        auto status = executor->prepare();

        if (status == IProcessor::Status::Ready)
            executor->work();
        else if (status == IProcessor::Status::Async)
        {
            EventCounter watch;
            executor->schedule(watch);
            watch.wait();
        }
        else
            throw Exception("Unexpected status for SequentialTransformExecutor: " + std::to_string(int(status)),
                            ErrorCodes::LOGICAL_ERROR);
    }

    block = std::move(*output_block);
}

}
