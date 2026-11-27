#pragma once

#include <base/defines.h>
#include <base/types.h>

#include <algorithm>
#include <mutex>


namespace DB
{

/// Limit on the number of files in one batch of `S3Queue`/`AzureQueue`, shared by all streaming tasks of a table.
///
/// When an insert fails, the batch size is halved, so that the file which causes the failure
/// eventually ends up alone in a batch (of size 1), and only then its retry count is reduced.
/// A successful batch restores the full batch size, but only if no insert failed since this batch started:
/// with `parallel_inserts` there are many tasks, and a success which started earlier must not clear
/// a reduction made by a newer failure, even if that reduction happens to have the same value.
/// So every failure increments a generation, and a success resets the limit only if the generation is unchanged.
class ObjectStorageQueueBatchSizeOverride
{
public:
    struct Snapshot
    {
        /// 0 means no override.
        size_t max_files = 0;
        UInt64 generation = 0;
    };

    Snapshot get() const
    {
        std::lock_guard lock(mutex);
        return {max_files, generation};
    }

    /// `default_max_files` is the configured limit used when there is no override (0 means no limit),
    /// `processed_files` is the size of the failed batch, used when there is no limit at all.
    void halveAfterFailure(size_t default_max_files, size_t processed_files)
    {
        std::lock_guard lock(mutex);
        size_t current = max_files ? max_files : default_max_files;
        if (!current)
            current = processed_files;
        max_files = std::max<size_t>(current / 2, 1);
        ++generation;
    }

    /// `started_with` is the snapshot taken when the successful batch started.
    void resetAfterSuccess(const Snapshot & started_with)
    {
        std::lock_guard lock(mutex);
        if (generation == started_with.generation)
            max_files = 0;
    }

    void reset()
    {
        std::lock_guard lock(mutex);
        max_files = 0;
        ++generation;
    }

private:
    mutable std::mutex mutex;
    size_t max_files TSA_GUARDED_BY(mutex) = 0;
    UInt64 generation TSA_GUARDED_BY(mutex) = 0;
};

}
