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

void CountingTransform::transform(Chunk & chunk)
{
    Progress local_progress(chunk.getNumRows(), chunk.bytes(), 0);
    progress.incrementPiecewiseAtomically(local_progress);

    //std::cerr << "============ counting adding progress for " << static_cast<const void *>(thread_status) << ' ' << chunk.getNumRows() << " rows\n";

    if (thread_status)
    {
        thread_status->performance_counters.increment(ProfileEvents::InsertedRows, local_progress.read_rows);
        thread_status->performance_counters.increment(ProfileEvents::InsertedBytes, local_progress.read_bytes);
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::InsertedRows, local_progress.read_rows);
        ProfileEvents::increment(ProfileEvents::InsertedBytes, local_progress.read_bytes);
    }

    if (process_elem)
        process_elem->updateProgressOut(local_progress);

    if (progress_callback)
        progress_callback(local_progress);
}

}
