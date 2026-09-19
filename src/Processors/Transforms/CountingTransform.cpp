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
        quota->used(QuotaType::WRITTEN_BYTES, written_bytes);

    Progress local_progress{WriteProgress(chunk.getNumRows(), written_bytes)};

    ProfileEvents::increment(ProfileEvents::InsertedRows, local_progress.written_rows);
    ProfileEvents::increment(ProfileEvents::InsertedBytes, written_bytes);

    if (process_elem)
        process_elem->updateProgressOut(local_progress);

    if (progress_callback)
        progress_callback(local_progress);

    cur_chunk = std::move(chunk);
}

}
