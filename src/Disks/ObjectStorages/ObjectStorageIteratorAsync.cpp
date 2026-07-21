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
    , list_objects_scheduler(threadPoolCallbackRunnerUnsafe<BatchAndHasNext>(list_objects_pool, thread_name))
{
}

IObjectStorageIteratorAsync::~IObjectStorageIteratorAsync()
{
    if (!deactivated)
        deactivate();
}

void IObjectStorageIteratorAsync::deactivate()
{
    list_objects_pool.wait();
    deactivated = true;
}

void IObjectStorageIteratorAsync::nextBatch()
{
    std::lock_guard lock(mutex);

    if (!has_next_batch)
    {
        current_batch.clear();
        current_batch_iterator = current_batch.begin();
        is_finished = true;
        return;
    }

    if (!is_initialized)
    {
        outcome_future = scheduleBatch();
        is_initialized = true;
    }

    try
    {
        chassert(outcome_future.valid());
        BatchAndHasNext result = outcome_future.get();

        current_batch = std::move(result.batch);
        current_batch_iterator = current_batch.begin();

        if (current_batch.empty())
        {
            is_finished = true;
            has_next_batch = false;
        }
        else
        {
            accumulated_size.fetch_add(current_batch.size(), std::memory_order_relaxed);

            has_next_batch = result.has_next;
            if (has_next_batch)
                outcome_future = scheduleBatch();
        }
    }
    catch (...)
    {
        has_next_batch = false;
        throw;
    }
}

void IObjectStorageIteratorAsync::next()
{
    std::lock_guard lock(mutex);

    if (is_finished)
        return;

    ++current_batch_iterator;
    if (current_batch_iterator == current_batch.end())
        nextBatch();
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
    std::lock_guard lock(mutex);

    if (!is_initialized)
        nextBatch();

    return !is_finished;
}

RelativePathWithMetadataPtr IObjectStorageIteratorAsync::current()
{
    std::lock_guard lock(mutex);

    if (!isValid())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to access invalid iterator");

    return *current_batch_iterator;
}


RelativePathsWithMetadata IObjectStorageIteratorAsync::currentBatch()
{
    std::lock_guard lock(mutex);

    if (!isValid())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to access invalid iterator");

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
