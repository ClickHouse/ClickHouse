#include <Processors/Transforms/CountingTransform.h>

#include <IO/Progress.h>
#include <Interpreters/ProcessList.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadStatus.h>


namespace ProfileEvents
{
    extern const Event InsertedRows;
    extern const Event InsertedBytes;
}


namespace DB
{

void CountingTransform::onConsume(Chunk chunk)
{
    auto written_bytes = chunk.bytes();

    if (quota)
        quota->usedForQuery(normalized_query_hash, QuotaType::WRITTEN_BYTES, written_bytes);

    Progress local_progress{WriteProgress(chunk.getNumRows(), written_bytes)};

    /// When these rows are already accounted by an outer pipeline (a distributed INSERT counting
    /// the block before dispatching it to local shards), the nested transform must not re-charge
    /// the global InsertedRows / InsertedBytes profile events either, otherwise
    /// system.query_log ProfileEvent_InsertedRows / _InsertedBytes get doubled for the local write.
    if (count_profile_events)
    {
        ProfileEvents::increment(ProfileEvents::InsertedRows, local_progress.written_rows);
        ProfileEvents::increment(ProfileEvents::InsertedBytes, written_bytes);
    }

    if (process_elem)
        process_elem->updateProgressOut(local_progress);

    if (progress_callback)
        progress_callback(local_progress);

    cur_chunk = std::move(chunk);
}

}
