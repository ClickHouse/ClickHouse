#include <Disks/ObjectStorages/ObjectStorageIteratorAsync.h>

#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

IObjectStorageIteratorAsync::IObjectStorageIteratorAsync(
    CurrentMetrics::Metric threads_metric,
    CurrentMetrics::Metric threads_active_metric,
    CurrentMetrics::Metric threads_scheduled_metric,
    const std::string & thread_name)
    : list_objects_pool(threads_metric, threads_active_metric, threads_scheduled_metric, 1)
    , list_objects_scheduler(threadPoolCallbackRunner<BatchAndHasNext>(list_objects_pool, thread_name))
{
}

void IObjectStorageIteratorAsync::nextBatch()
{
    std::lock_guard lock(mutex);
    if (is_finished)
    {
        current_batch.clear();
        current_batch_iterator = current_batch.begin();
    }
    else
    {
        if (!is_initialized)
        {
            outcome_future = scheduleBatch();
            is_initialized = true;
        }

        chassert(outcome_future.valid());
        BatchAndHasNext result;
        try
        {
            result = outcome_future.get();
        }
        catch (...)
        {
            is_finished = true;
            throw;
        }

        current_batch = std::move(result.batch);
        current_batch_iterator = current_batch.begin();

        accumulated_size.fetch_add(current_batch.size(), std::memory_order_relaxed);

        if (result.has_next)
            outcome_future = scheduleBatch();
        else
            is_finished = true;
    }
}

void IObjectStorageIteratorAsync::next()
{
    std::lock_guard lock(mutex);

    if (current_batch_iterator == current_batch.end())
        nextBatch();
    else
        ++current_batch_iterator;
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

RelativePathWithMetadataPtr IObjectStorageIteratorAsync::current()
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

std::optional<RelativePathsWithMetadata> IObjectStorageIteratorAsync::getCurrentBatchAndScheduleNext()
{
    std::lock_guard lock(mutex);
    if (!is_initialized)
        nextBatch();

    if (current_batch_iterator == current_batch.end())
    {
        return std::nullopt;
    }

    auto temp_current_batch = std::move(current_batch);
    nextBatch();
    return temp_current_batch;
}

size_t IObjectStorageIteratorAsync::getAccumulatedSize() const
{
    return accumulated_size.load(std::memory_order_relaxed);
}

}
