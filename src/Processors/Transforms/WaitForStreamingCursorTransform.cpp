#include <Processors/Transforms/WaitForStreamingCursorTransform.h>

#include <Interpreters/Context.h>
#include <Processors/Port.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

WaitForStreamingCursorTransform::WaitForStreamingCursorTransform(SharedHeader header, std::shared_ptr<StreamingCursor> cursor_)
    : IProcessor({header}, {header})
    , cursor(std::move(cursor_))
{
}

IProcessor::Status WaitForStreamingCursorTransform::prepare()
{
    auto & input = inputs.front();
    auto & output = outputs.front();

    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    if (input.isFinished())
    {
        if (cursor->hasPendingReports())
            return Status::Async;

        output.finish();
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    output.push(input.pull(/*set_not_needed=*/true));
    return Status::PortFull;
}

void WaitForStreamingCursorTransform::work()
{
    /// Nothing to do: the descriptor of `schedule` became readable, so `prepare` checks the pending reads again.
}

int WaitForStreamingCursorTransform::schedule()
{
#if defined(OS_LINUX) || defined(OS_DARWIN)
    return cursor->pendingReportsFD();
#else
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Waiting for the streaming cursor is not supported on this platform");
#endif
}

}
