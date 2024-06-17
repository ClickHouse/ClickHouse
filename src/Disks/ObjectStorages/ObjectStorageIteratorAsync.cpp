#include <Disks/ObjectStorages/ObjectStorageIteratorAsync.h>

#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void IObjectStorageIteratorAsync::nextBatch()
{
    std::lock_guard lock(mutex);
    if (!is_finished)
    {
        if (!is_initialized)
        {
            outcome_future = scheduleBatch();
            is_initialized = true;
        }

         BatchAndHasNext next_batch = outcome_future.get();
         current_batch = std::move(next_batch.batch);
         accumulated_size.fetch_add(current_batch.size(), std::memory_order_relaxed);
         current_batch_iterator = current_batch.begin();
         if (next_batch.has_next)
             outcome_future = scheduleBatch();
         else
             is_finished = true;
    }
    else
    {
        current_batch.clear();
        current_batch_iterator = current_batch.begin();
    }
}

void IObjectStorageIteratorAsync::next()
{
    std::lock_guard lock(mutex);

    if (current_batch_iterator != current_batch.end())
    {
        ++current_batch_iterator;
    }
    else if (!is_finished)
    {
        if (outcome_future.valid())
        {
            BatchAndHasNext next_batch = outcome_future.get();
            current_batch = std::move(next_batch.batch);
            accumulated_size.fetch_add(current_batch.size(), std::memory_order_relaxed);
            current_batch_iterator = current_batch.begin();
            if (next_batch.has_next)
                outcome_future = scheduleBatch();
            else
                is_finished = true;
        }
    }
}

std::future<IObjectStorageIteratorAsync::BatchAndHasNext> IObjectStorageIteratorAsync::scheduleBatch()
{
    return list_objects_scheduler([this]
    {
        BatchAndHasNext result;
        result.has_next = getBatchAndCheckNext(result.batch);
        return result;
    }, Priority{});
}


bool IObjectStorageIteratorAsync::isValid()
{
    if (!is_initialized)
        nextBatch();

    std::lock_guard lock(mutex);
    return current_batch_iterator != current_batch.end();
}

RelativePathWithMetadata IObjectStorageIteratorAsync::current()
{
    if (!isValid())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to access invalid iterator");

    std::lock_guard lock(mutex);
    return *current_batch_iterator;
}


RelativePathsWithMetadata IObjectStorageIteratorAsync::currentBatch()
{
    if (!isValid())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to access invalid iterator");

    std::lock_guard lock(mutex);
    return current_batch;
}

std::optional<RelativePathsWithMetadata> IObjectStorageIteratorAsync::getCurrrentBatchAndScheduleNext()
{
    std::lock_guard lock(mutex);
    if (!is_initialized)
        nextBatch();

    if (current_batch_iterator != current_batch.end())
    {
        auto temp_current_batch = current_batch;
        nextBatch();
        return temp_current_batch;
    }

    return std::nullopt;
}

size_t IObjectStorageIteratorAsync::getAccumulatedSize() const
{
    return accumulated_size.load(std::memory_order_relaxed);
}

}
