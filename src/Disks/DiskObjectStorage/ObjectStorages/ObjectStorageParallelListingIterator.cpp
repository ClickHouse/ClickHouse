#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageParallelListingIterator.h>

#include <Common/CurrentMetrics.h>
#include <Common/setThreadName.h>

#include <algorithm>


namespace CurrentMetrics
{
    extern const Metric ObjectStorageParallelListingThreads;
    extern const Metric ObjectStorageParallelListingThreadsActive;
    extern const Metric ObjectStorageParallelListingThreadsScheduled;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ObjectStorageParallelListingIterator::ObjectStorageParallelListingIterator(
    std::string root_prefix_,
    size_t num_threads_,
    size_t max_buffered_keys_,
    ListLevelFunction list_level_,
    std::function<bool(const std::string & common_prefix)> should_descend_)
    : num_threads(std::max<size_t>(num_threads_, 1))
    , max_buffered_objects(std::max<size_t>(max_buffered_keys_, 1))
    , list_level(std::move(list_level_))
    , should_descend(std::move(should_descend_))
    , pool(
          CurrentMetrics::ObjectStorageParallelListingThreads,
          CurrentMetrics::ObjectStorageParallelListingThreadsActive,
          CurrentMetrics::ObjectStorageParallelListingThreadsScheduled,
          /* max_threads */ num_threads,
          /* max_free_threads */ 0,
          /* queue_size */ num_threads)
{
    /// Seed the walk with the root prefix. Workers are started lazily on the first batch request.
    prefixes_to_list.push_back(std::move(root_prefix_));
    outstanding_prefixes = 1;
}

ObjectStorageParallelListingIterator::~ObjectStorageParallelListingIterator()
{
    {
        std::lock_guard lock(mutex);
        stop = true;
    }
    work_available.notify_all();
    result_available.notify_all();
    space_available.notify_all();
    pool.wait();
}

void ObjectStorageParallelListingIterator::ensureStarted(std::unique_lock<std::mutex> &)
{
    if (started)
        return;
    started = true;
    for (size_t i = 0; i < num_threads; ++i)
        pool.scheduleOrThrowOnError([this] { worker(); });
}

void ObjectStorageParallelListingIterator::worker()
{
    setThreadName(ThreadName::S3_LIST_POOL);

    while (true)
    {
        std::string prefix_to_list;
        {
            std::unique_lock lock(mutex);
            work_available.wait(lock, [this] { return !prefixes_to_list.empty() || finished || stop; });
            if (stop || finished)
                return;
            prefix_to_list = std::move(prefixes_to_list.front());
            prefixes_to_list.pop_front();
        }

        if (!listPrefix(prefix_to_list))
            return; /// An exception was stored and `finished` was set; stop the walk.

        std::unique_lock lock(mutex);
        chassert(outstanding_prefixes > 0);
        if (--outstanding_prefixes == 0)
        {
            finished = true;
            lock.unlock();
            work_available.notify_all();
            result_available.notify_all();
            space_available.notify_all();
            return;
        }
    }
}

bool ObjectStorageParallelListingIterator::listPrefix(const std::string & prefix_to_list)
{
    std::string continuation_token;
    bool is_truncated = true;

    try
    {
        while (is_truncated)
        {
            {
                std::unique_lock lock(mutex);
                /// Backpressure: do not run ahead of the consumer indefinitely.
                space_available.wait(lock, [this] { return buffered_objects < max_buffered_objects || finished || stop; });
                if (stop || finished)
                    return true;
            }

            auto result = list_level(prefix_to_list, continuation_token);

            /// Decide which sub-directories to descend into without holding the lock
            /// (the predicate may do non-trivial regexp matching).
            std::vector<std::string> children;
            children.reserve(result.common_prefixes.size());
            for (auto & common_prefix : result.common_prefixes)
            {
                if (should_descend(common_prefix))
                    children.push_back(std::move(common_prefix));
            }

            {
                std::unique_lock lock(mutex);
                if (stop || finished)
                    return true;

                if (!children.empty())
                {
                    outstanding_prefixes += children.size();
                    for (auto & child : children)
                        prefixes_to_list.push_back(std::move(child));
                    work_available.notify_all();
                }

                if (!result.objects.empty())
                {
                    buffered_objects += result.objects.size();
                    ready_batches.push_back(std::move(result.objects));
                    result_available.notify_one();
                }

                continuation_token = result.next_continuation_token;
                is_truncated = result.is_truncated;
            }
        }
        return true;
    }
    catch (...)
    {
        {
            std::lock_guard lock(mutex);
            if (!first_exception)
                first_exception = std::current_exception();
            finished = true;
        }
        work_available.notify_all();
        result_available.notify_all();
        space_available.notify_all();
        return false;
    }
}

std::optional<RelativePathsWithMetadata> ObjectStorageParallelListingIterator::popBatch(std::unique_lock<std::mutex> & lock)
{
    result_available.wait(lock, [this] { return !ready_batches.empty() || finished || stop; });

    if (first_exception)
        std::rethrow_exception(first_exception);

    if (ready_batches.empty())
        return std::nullopt;

    auto batch = std::move(ready_batches.front());
    ready_batches.pop_front();
    buffered_objects -= batch.size();
    accumulated_size.fetch_add(batch.size(), std::memory_order_relaxed);
    space_available.notify_all();
    return batch;
}

void ObjectStorageParallelListingIterator::advanceLocked(std::unique_lock<std::mutex> & lock)
{
    ensureStarted(lock);
    auto batch = popBatch(lock);
    if (batch)
    {
        current_batch = std::move(*batch);
        current_batch_iterator = current_batch.begin();
    }
    else
    {
        current_batch.clear();
        current_batch_iterator = current_batch.begin();
        consumer_finished = true;
    }
    is_initialized = true;
}

void ObjectStorageParallelListingIterator::next()
{
    std::unique_lock lock(mutex);
    if (!is_initialized)
    {
        advanceLocked(lock);
        return;
    }
    if (consumer_finished)
        return;
    ++current_batch_iterator;
    if (current_batch_iterator == current_batch.end())
        advanceLocked(lock);
}

void ObjectStorageParallelListingIterator::nextBatch()
{
    std::unique_lock lock(mutex);
    advanceLocked(lock);
}

bool ObjectStorageParallelListingIterator::isValid()
{
    std::unique_lock lock(mutex);
    if (!is_initialized)
        advanceLocked(lock);
    return !consumer_finished;
}

RelativePathWithMetadataPtr ObjectStorageParallelListingIterator::current()
{
    std::unique_lock lock(mutex);
    if (!is_initialized)
        advanceLocked(lock);
    if (consumer_finished || current_batch_iterator == current_batch.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to access invalid iterator");
    return *current_batch_iterator;
}

RelativePathsWithMetadata ObjectStorageParallelListingIterator::currentBatch()
{
    std::unique_lock lock(mutex);
    if (!is_initialized)
        advanceLocked(lock);
    return current_batch;
}

std::optional<RelativePathsWithMetadata> ObjectStorageParallelListingIterator::getCurrentBatchAndScheduleNext()
{
    std::unique_lock lock(mutex);
    if (!is_initialized)
        advanceLocked(lock);

    if (current_batch_iterator == current_batch.end())
        return std::nullopt;

    auto batch = std::move(current_batch);
    advanceLocked(lock);
    return batch;
}

size_t ObjectStorageParallelListingIterator::getAccumulatedSize() const
{
    return accumulated_size.load(std::memory_order_relaxed);
}

}
