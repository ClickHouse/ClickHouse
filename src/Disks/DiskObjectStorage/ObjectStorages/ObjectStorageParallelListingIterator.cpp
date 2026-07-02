#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageParallelListingIterator.h>

#include <Common/CurrentMetrics.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/setThreadName.h>

#include <algorithm>
#include <array>
#include <chrono>


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
    /// A flat directory is split a single level: its keyspace is fanned out by the byte alphabet
    /// observed in the first page (typically ~16 for hex/UUID names), and each resulting sub-range is
    /// paginated serially in parallel with the others. Recursive (multi-level) splitting is deliberately
    /// avoided: for keys that share a long common prefix (e.g. `pageviews-YYYYMMDD-HH`) the alphabet does
    /// not divide them, so deeper levels only issue empty boundary probes (pure overhead) while one
    /// sub-range still holds all the keys. A single level captures the win for uniformly distributed keys
    /// without ever being slower than serial for clustered keys.
    constexpr size_t FLAT_SPLIT_BUDGET = 1;

    /// Query cancellation does not notify our condition variables, so the consumer waits in bounded
    /// slices and re-checks `check_cancellation` on each timeout instead of blocking indefinitely.
    constexpr std::chrono::milliseconds CANCELLATION_POLL_INTERVAL{100};
}

ObjectStorageParallelListingIterator::ObjectStorageParallelListingIterator(
    std::string root_prefix_,
    size_t num_threads_,
    size_t max_buffered_keys_,
    ListLevelFunction list_level_,
    std::function<bool(const std::string & common_prefix)> should_descend_,
    bool allow_keyspace_split_,
    std::function<void()> check_cancellation_)
    : num_threads(std::max<size_t>(num_threads_, 1))
    , max_buffered_objects(std::max<size_t>(max_buffered_keys_, 1))
    , list_level(std::move(list_level_))
    , should_descend(std::move(should_descend_))
    , check_cancellation(std::move(check_cancellation_))
    , thread_group(getCurrentThreadGroup())
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
    /// A zero budget disables flat keyspace splitting (and the `StartAfter`/empty-delimiter requests it
    /// issues), so a flat directory is paginated serially; the hierarchical delimiter walk is unaffected.
    root.split_budget = allow_keyspace_split_ ? FLAT_SPLIT_BUDGET : 0;
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
    /// Inherit the owning query's thread group (if any) so listing memory is accounted to the query and
    /// in-flight listing requests observe its cancellation, like the serial listing path.
    ThreadGroupSwitcher thread_group_switcher(thread_group, ThreadName::S3_LIST_POOL);
    /// Set the name explicitly as well: the switcher sets no name when there is no thread group (tests).
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

    /// Bound the number of sub-ranges (hence the number of possibly-empty boundary probes) to a small
    /// multiple of the worker count; sample evenly across the sorted boundaries if the alphabet is large.
    const size_t max_boundaries = std::max<size_t>(num_threads * 4, 32);
    if (boundaries.size() > max_boundaries)
    {
        std::vector<std::string> sampled;
        sampled.reserve(max_boundaries);
        for (size_t i = 0; i < max_boundaries; ++i)
            sampled.push_back(boundaries[i * boundaries.size() / max_boundaries]);
        sampled.erase(std::unique(sampled.begin(), sampled.end()), sampled.end());
        boundaries = std::move(sampled);
    }

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

bool ObjectStorageParallelListingIterator::splitWouldHelp(
    const ListRange & range, const std::string & delimiter, const std::string & last_key) const
{
    if (range.split_pos >= last_key.size())
        return false;
    const auto bucket_byte = static_cast<unsigned char>(last_key[range.split_pos]);
    if (bucket_byte == 0xff)
        return false; /// No higher bucket: everything left is in the current bucket.

    /// The smallest key of the *next* bucket; listing after it returns keys whose byte at the split
    /// position is greater than the current one (i.e. any data beyond the current bucket).
    std::string probe = last_key.substr(0, range.split_pos);
    probe.push_back(static_cast<char>(static_cast<unsigned char>(bucket_byte + 1)));

    auto result = list_level(range.prefix, delimiter, probe, std::string{});
    if (!result.objects.empty() && (range.end.empty() || result.objects.front()->getPath() <= range.end))
        return true;
    if (!result.common_prefixes.empty() && (range.end.empty() || result.common_prefixes.front() <= range.end))
        return true;
    return false;
}

bool ObjectStorageParallelListingIterator::listRange(const ListRange & range)
{
    const std::string delimiter = range.use_delimiter ? "/" : "";
    std::string continuation_token;
    std::string last_key = range.start_after;
    bool first = true;
    /// Whether a flat keyspace split may still be attempted. We attempt it at most once per range:
    /// once we decide a flat range cannot be usefully split, we paginate it serially without probing
    /// again on every page (which would otherwise issue one wasted probe request per page).
    bool may_split = range.split_budget > 0;

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
                if (had_common_prefixes || !may_split)
                {
                    keep_paginating = true;
                }
                else if (splitWouldHelp(range, delimiter, last_key))
                {
                    auto split = splitFlatRange(range, last_key, batch.empty() ? result.objects : batch);
                    if (split.empty())
                    {
                        keep_paginating = true; /// Could not split; fall back to serial pagination.
                        may_split = false;
                    }
                    else
                        for (auto & s : split)
                            follow_up.push_back(std::move(s));
                }
                else
                {
                    /// Keys share a common prefix; splitting would only waste requests. Paginate serially
                    /// and do not probe again on subsequent pages of this range.
                    keep_paginating = true;
                    may_split = false;
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
    /// Wait in bounded slices rather than indefinitely: query cancellation (a `KILL` or a timeout) does
    /// not notify our condition variables, so on each timeout `check_cancellation` throws the proper
    /// exception if the query was cancelled. Without this the consumer (e.g. the query's main thread in
    /// `getPathSample`) could block forever when the producers stall, ignoring cancellation entirely.
    while (!result_available.wait_for(lock, CANCELLATION_POLL_INTERVAL, [this] { return !ready_batches.empty() || finished || stop; }))
    {
        if (!check_cancellation)
            continue;
        try
        {
            check_cancellation();
        }
        catch (...)
        {
            /// Stop the workers right away so the destructor's `pool.wait()` does not have to wait for a
            /// worker that is about to issue another listing request.
            stop = true;
            lock.unlock();
            work_available.notify_all();
            space_available.notify_all();
            throw;
        }
    }

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
