#pragma once

#include <Interpreters/Context.h>
#include <Processors/IProcessor.h>

namespace DB
{

/// Passes the chunks through, and when its output is closed, keeps reading and discarding its input until it finishes.
///
/// A processor closes its input when it does not need more data for the result (e.g. `DISTINCT` by constants, or
/// `LIMIT BY` without keys), which finishes the source before it has read everything. This transform is put right after
/// a streaming source that reports its cursor only when it reaches the end of its bounded stream, for an incremental
/// refresh of a materialized view (see `IStorage::reportsStreamingCursor`): the rows that the query did not need are
/// still consumed by the refresh, and the next refresh must not get them again.
///
/// The read stays pending in the holder of the cursor until the source finishes (see `StreamingCursor::PendingReport`),
/// as the query may finish its output before that.
class ReadToEndTransform final : public IProcessor
{
public:
    ReadToEndTransform(SharedHeader header, std::shared_ptr<StreamingCursor> cursor);

    String getName() const override { return "ReadToEnd"; }

    Status prepare() override;
    void onCancel() noexcept override;

private:
    StreamingCursor::PendingReport pending_report;
};

}
