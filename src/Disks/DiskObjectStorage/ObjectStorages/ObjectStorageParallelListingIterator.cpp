#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageParallelListingIterator.h>

#include <Common/CurrentMetrics.h>
#include <Common/setThreadName.h>

#include <algorithm>
#include <array>


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

namespace
{
    /// Number of times a flat directory may be recursively split so that the leaf ranges are roughly
    /// `num_threads`-many (each split fans out by the key alphabet, typically ~16 for hex/UUID names).
    size_t flatSplitBudget(size_t num_threads)
    {
        size_t budget = 1;
        size_t ranges = 16;
        while (ranges < num_threads && budget < 4)
        {
            ranges *= 16;
            ++budget;
        }
        return budget;
    }
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
    ListRange root;
    root.prefix = std::move(root_prefix_);
    root.split_pos = root.prefix.size();
    root.split_budget = flatSplitBudget(num_threads);
    root.use_delimiter = true;
    ranges_to_list.push_back(std::move(root));
    outstanding_ranges = 1;
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

void ObjectStorageParallelListingIterator::enqueueLocked(
    std::vector<ListRange> & new_ranges, RelativePathsWithMetadata & batch, std::unique_lock<std::mutex> &)
{
    if (!new_ranges.empty())
    {
        outstanding_ranges += new_ranges.size();
        for (auto & r : new_ranges)
            ranges_to_list.push_back(std::move(r));
        work_available.notify_all();
    }
    if (!batch.empty())
    {
        buffered_objects += batch.size();
        ready_batches.push_back(std::move(batch));
        result_available.notify_one();
    }
}

void ObjectStorageParallelListingIterator::worker()
{
    setThreadName(ThreadName::S3_LIST_POOL);

    while (true)
    {
        ListRange range;
        {
            std::unique_lock lock(mutex);
            work_available.wait(lock, [this] { return !ranges_to_list.empty() || finished || stop; });
            if (stop || finished)
                return;
            range = std::move(ranges_to_list.front());
            ranges_to_list.pop_front();
        }

        if (!listRange(range))
            return; /// An exception was stored and `finished` was set; stop the walk.

        std::unique_lock lock(mutex);
        chassert(outstanding_ranges > 0);
        if (--outstanding_ranges == 0)
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

std::vector<ObjectStorageParallelListingIterator::ListRange> ObjectStorageParallelListingIterator::splitFlatRange(
    const ListRange & range, const std::string & last_key, const RelativePathsWithMetadata & sample) const
{
    std::vector<ListRange> result;

    const size_t pos = range.split_pos;
    if (pos >= last_key.size())
        return result; /// Cannot form a boundary at this position; caller paginates instead.

    /// Bytes shared by all keys up to the split position (taken from the last listed key).
    const std::string base = last_key.substr(0, pos);

    /// The byte alphabet observed in the sampled keys, from the split position onwards. We split the
    /// keyspace at the bytes of this alphabet; because the resulting sub-ranges are contiguous they
    /// cover the whole interval regardless of which bytes actually occur (no gaps).
    std::array<bool, 256> seen{};
    for (const auto & object : sample)
    {
        const std::string & key = object->getPath();
        for (size_t i = pos; i < key.size(); ++i)
            seen[static_cast<unsigned char>(key[i])] = true;
    }

    std::vector<std::string> boundaries;
    for (int v = 0; v < 256; ++v)
    {
        if (!seen[v])
            continue;
        std::string boundary = base;
        boundary.push_back(static_cast<char>(static_cast<unsigned char>(v)));
        /// Keep only boundaries strictly inside the remaining interval (last_key, range.end].
        if (boundary > last_key && (range.end.empty() || boundary < range.end))
            boundaries.push_back(std::move(boundary));
    }
    /// `boundaries` are distinct (distinct bytes appended to a fixed base) and built in byte order.

    if (boundaries.empty())
        return result; /// Nothing to split into; caller paginates the remainder.

    /// Tile (last_key, range.end] into contiguous half-open-then-closed sub-ranges. `end` is inclusive,
    /// `start_after` is exclusive, so a key equal to a boundary lands in exactly one sub-range.
    std::string prev = last_key;
    result.reserve(boundaries.size() + 1);
    for (auto & boundary : boundaries)
    {
        ListRange sub{range.prefix, prev, boundary, pos + 1, range.split_budget - 1, /* use_delimiter */ false};
        result.push_back(std::move(sub));
        prev = std::move(boundary);
    }
    result.push_back(ListRange{range.prefix, prev, range.end, pos + 1, range.split_budget - 1, /* use_delimiter */ false});
    return result;
}

bool ObjectStorageParallelListingIterator::listRange(const ListRange & range)
{
    const std::string delimiter = range.use_delimiter ? "/" : "";
    std::string continuation_token;
    std::string last_key = range.start_after;
    bool first = true;

    try
    {
        while (true)
        {
            {
                std::unique_lock lock(mutex);
                space_available.wait(lock, [this] { return buffered_objects < max_buffered_objects || finished || stop; });
                if (stop || finished)
                    return true;
            }

            auto result = list_level(range.prefix, delimiter, first ? range.start_after : std::string{}, continuation_token);
            first = false;

            std::vector<ListRange> new_ranges;
            for (auto & common_prefix : result.common_prefixes)
            {
                if (!range.end.empty() && common_prefix > range.end)
                    continue;
                if (should_descend(common_prefix))
                {
                    ListRange child;
                    child.split_pos = common_prefix.size();
                    child.split_budget = range.split_budget;
                    child.use_delimiter = true;
                    child.prefix = std::move(common_prefix);
                    new_ranges.push_back(std::move(child));
                }
            }

            RelativePathsWithMetadata batch;
            bool reached_end = false;
            for (auto & object : result.objects)
            {
                if (!range.end.empty() && object->getPath() > range.end)
                {
                    reached_end = true;
                    break;
                }
                last_key = object->getPath();
                batch.push_back(std::move(object));
            }

            const bool had_common_prefixes = !result.common_prefixes.empty();

            /// Decide what to do next before publishing (so the outstanding-range count stays consistent).
            std::vector<ListRange> follow_up = std::move(new_ranges);
            bool keep_paginating = false;

            if (!reached_end && result.is_truncated)
            {
                continuation_token = result.next_continuation_token;
                if (had_common_prefixes || range.split_budget == 0)
                {
                    keep_paginating = true;
                }
                else
                {
                    auto split = splitFlatRange(range, last_key, batch.empty() ? result.objects : batch);
                    if (split.empty())
                        keep_paginating = true; /// Could not split; fall back to serial pagination.
                    else
                        for (auto & s : split)
                            follow_up.push_back(std::move(s));
                }
            }

            {
                std::unique_lock lock(mutex);
                if (stop || finished)
                    return true;
                enqueueLocked(follow_up, batch, lock);
            }

            if (reached_end || !result.is_truncated || !keep_paginating)
                return true;
        }
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
