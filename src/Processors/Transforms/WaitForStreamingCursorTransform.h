#pragma once

#include <Processors/IProcessor.h>

namespace DB
{

struct StreamingCursor;

/// Passes the rows of an `INSERT SELECT` for an incremental refresh through, and finishes after its input only once every
/// read of the query reported its streaming cursor, see `StreamingCursor::PendingReport`. A target that commits the cursor
/// along with its data does it when its input is finished, so it gets the cursor after all the rows that the query
/// consumed. It waits without taking a thread, as the reads may still need the threads of the query to report.
class WaitForStreamingCursorTransform final : public IProcessor
{
public:
    WaitForStreamingCursorTransform(SharedHeader header, std::shared_ptr<StreamingCursor> cursor_);

    String getName() const override { return "WaitForStreamingCursorTransform"; }

    Status prepare() override;
    void work() override;
    int schedule() override;

private:
    const std::shared_ptr<StreamingCursor> cursor;
};

}
