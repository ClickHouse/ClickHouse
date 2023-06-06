#include <Disks/ObjectStorages/ObjectStorageIteratorAsync.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
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


bool IObjectStorageIteratorAsync::isValid() const
{
    return current_batch_iterator != current_batch.end();
}

RelativePathWithMetadata IObjectStorageIteratorAsync::current() const
{
    if (!isValid())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to access invalid iterator");

    return *current_batch_iterator;
}

size_t IObjectStorageIteratorAsync::getAccumulatedSize() const
{
    return accumulated_size.load(std::memory_order_relaxed);
}

}
