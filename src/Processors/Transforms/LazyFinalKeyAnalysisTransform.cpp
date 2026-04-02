#include <Interpreters/Set.h>
#include <Processors/Port.h>
#include <Processors/Transforms/LazyFinalKeyAnalysisTransform.h>

namespace DB
{

LazyFinalKeyAnalysisTransform::LazyFinalKeyAnalysisTransform(FutureSetPtr future_set_)
    : IProcessor(InputPorts{InputPort(Block())}, OutputPorts{OutputPort(Block())})
    , future_set(std::move(future_set_))
{
}

IProcessor::Status LazyFinalKeyAnalysisTransform::prepare()
{
    auto & input = inputs.front();
    auto & output = outputs.front();

    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
        return Status::NeedData;

    if (input.isFinished())
    {
        auto set = future_set->get();
        if (set && !set->isTruncated())
            output.push(Chunk());

        output.finish();
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    /// Discard any signal chunks.
    input.pull();
    return Status::NeedData;
}

}
