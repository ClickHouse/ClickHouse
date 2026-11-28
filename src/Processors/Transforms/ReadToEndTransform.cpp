#include <Processors/Transforms/ReadToEndTransform.h>

#include <Processors/Port.h>

namespace DB
{

ReadToEndTransform::ReadToEndTransform(SharedHeader header, std::shared_ptr<StreamingCursor> cursor)
    : IProcessor({header}, {header})
    , pending_report(std::move(cursor))
{
}

IProcessor::Status ReadToEndTransform::prepare()
{
    auto & input = inputs.front();
    auto & output = outputs.front();

    if (output.isFinished())
    {
        /// Nobody needs the data anymore, but the source has to reach its end. A finished input may still have
        /// the last chunk, so it is checked again after the chunk is discarded. The chunk may be buffered in an input
        /// that is not needed (see the back-pressure below), so the pull must not require it; it is needed again below.
        if (input.hasData())
            input.pull(/*set_not_needed=*/ true);

        if (input.isFinished())
        {
            pending_report.finish();
            return Status::Finished;
        }

        input.setNeeded();
        return Status::NeedData;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    if (input.hasData())
    {
        output.push(input.pull(/*set_not_needed=*/ true));
        return Status::PortFull;
    }

    if (input.isFinished())
    {
        pending_report.finish();
        output.finish();
        return Status::Finished;
    }

    input.setNeeded();
    return Status::NeedData;
}

void ReadToEndTransform::onCancel() noexcept
{
    /// The query fails, so the cursor does not matter.
    pending_report.finish();
}

}
