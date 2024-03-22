#include <iostream>

#include <Interpreters/ProcessList.h>
#include <Processors/Transforms/CountingTransform.h>
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
    if (quota)
        quota->used(QuotaType::WRITTEN_BYTES, chunk.bytes());

    Progress local_progress{WriteProgress(chunk.getNumRows(), chunk.bytes())};
    progress.incrementPiecewiseAtomically(local_progress);

    if (thread_status)
    {
        thread_status->performance_counters.increment(ProfileEvents::InsertedRows, local_progress.written_rows);
        thread_status->performance_counters.increment(ProfileEvents::InsertedBytes, local_progress.written_bytes);
        thread_status->progress_out.incrementPiecewiseAtomically(local_progress);
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::InsertedRows, local_progress.written_rows);
        ProfileEvents::increment(ProfileEvents::InsertedBytes, local_progress.written_bytes);
    }

    if (process_elem)
        process_elem->updateProgressOut(local_progress);

    if (progress_callback)
        progress_callback(local_progress);

    cur_chunk = std::move(chunk);
}

}
