#pragma once

#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageIterator.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Common/ThreadPool.h>
#include <Common/ThreadGroupSwitcher.h>

#include <atomic>
#include <condition_variable>
#include <deque>
#include <exception>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <vector>


namespace DB
{

/// Lists an object storage prefix in parallel.
///
/// Two sources of parallelism, both expressed as a tree of "ranges" walked by a pool of workers:
///
///  1. Hierarchical layouts: a prefix listed with the '/' delimiter exposes sub-"directories"
///     (common prefixes), which are walked concurrently; subtrees that cannot contain a matching key
///     are pruned via `should_descend`.
///
///  2. Big flat directories: a prefix that is truncated but has no '/' sub-directories is split by
///     keyspace. Its remaining range is tiled into contiguous `(start_after, end)` sub-ranges whose
///     boundaries are derived from the byte alphabet observed in the listed page, and each sub-range
///     is listed concurrently. Because the sub-ranges are *contiguous* they tile the interval with no
///     gaps — a key whose byte is not in the sampled alphabet still falls into the range that brackets
///     it — so the split is complete regardless of the key distribution or character set.
///
/// The order in which keys are produced is unspecified (it depends on thread scheduling).
///
/// The iterator is storage-agnostic: it drives a caller-provided `list_level` callback (one delimited
/// page of one prefix per call, optionally resuming after a key) and a `should_descend` callback.
class ObjectStorageParallelListingIterator final : public IObjectStorageIterator
{
public:
    /// Lists a single delimited page. `start_after` (honored only on the first call of a listing, i.e.
    /// when `continuation_token` is empty) resumes strictly after that key; `continuation_token`
    /// resumes pagination. Returns objects + common prefixes + truncation/next-token.
    using ListLevelFunction = std::function<ObjectStorageListResult(
        const std::string & prefix, const std::string & delimiter, const std::string & start_after, const std::string & continuation_token)>;

    /// `should_descend(common_prefix)` decides whether a discovered sub-"directory" might contain a key
    /// of interest. It may be called concurrently. Returning `true` when unsure is safe.
    /// `max_buffered_keys` softly bounds how many keys may be buffered ahead of the consumer.
    /// `allow_keyspace_split` enables splitting a big flat directory by keyspace (issuing `start_after`
    /// requests with an empty delimiter). Set it to false for storages that do not support `StartAfter`
    /// or a non-'/' delimiter (e.g. S3 Express / directory buckets); such flat ranges are then paginated
    /// serially, while the hierarchical delimiter walk stays parallel.
    /// `check_cancellation`, if set, must throw when the query owning this listing was cancelled; it is
    /// polled while the consumer waits for a batch (a `KILL`/timeout does not notify our condition
    /// variables), so the listing fails fast instead of hanging. Empty in non-query contexts (tests).
    ObjectStorageParallelListingIterator(
        std::string root_prefix_,
        size_t num_threads_,
        size_t max_buffered_keys_,
        ListLevelFunction list_level_,
        std::function<bool(const std::string & common_prefix)> should_descend_,
        bool allow_keyspace_split_ = true,
        std::function<void()> check_cancellation_ = {});

    ~ObjectStorageParallelListingIterator() override;

    void next() override;
    void nextBatch() override;
    bool isValid() override;
    RelativePathWithMetadataPtr current() override;
    RelativePathsWithMetadata currentBatch() override;
    std::optional<RelativePathsWithMetadata> getCurrentBatchAndScheduleNext() override;
    size_t getAccumulatedSize() const override;

private:
    /// A half-open keyspace range `(start_after, end)` of keys under `prefix` left to list.
    struct ListRange
    {
        std::string prefix;        /// S3 Prefix.
        std::string start_after;   /// Exclusive lower bound; empty = from the beginning of `prefix`.
        std::string end;           /// Inclusive upper bound key; empty = unbounded.
        size_t split_pos = 0;      /// Byte position to split at if this range needs a flat (keyspace) split.
        size_t split_budget = 0;   /// How many more times this branch may flat-split (0 = paginate serially).
        bool use_delimiter = true; /// List with the '/' delimiter (discover sub-dirs) vs. raw keyspace range.
    };

    void worker();
    /// Lists one range completely (paginating, splitting flat sub-trees). Returns false if an exception
    /// was stored and the walk must stop.
    bool listRange(const ListRange & range);
    /// Tiles `(last_key, range.end)` into contiguous sub-ranges using boundaries derived from the byte
    /// alphabet of `sample`. Returns empty if the range cannot be usefully split (caller paginates).
    std::vector<ListRange> splitFlatRange(const ListRange & range, const std::string & last_key, const RelativePathsWithMetadata & sample) const;
    /// One cheap probe: is there any key beyond the bucket `last_key` sits in (within the range)?
    /// If not, the keys share a common prefix and splitting cannot parallelize them — so the caller
    /// paginates serially instead of issuing a fan of empty boundary probes.
    bool splitWouldHelp(const ListRange & range, const std::string & delimiter, const std::string & last_key) const;

    std::optional<RelativePathsWithMetadata> popBatch(std::unique_lock<std::mutex> & lock);
    void ensureStarted(std::unique_lock<std::mutex> & lock);
    void advanceLocked(std::unique_lock<std::mutex> & lock);
    /// Enqueue ranges and emit a batch atomically; updates the outstanding-range counter. Requires lock.
    void enqueueLocked(std::vector<ListRange> & new_ranges, RelativePathsWithMetadata & batch, std::unique_lock<std::mutex> & lock);

    const size_t num_threads;
    const size_t max_buffered_objects;
    const ListLevelFunction list_level;
    const std::function<bool(const std::string & common_prefix)> should_descend;
    /// Throws (the proper `TIMEOUT_EXCEEDED` / `QUERY_WAS_CANCELLED`) when the owning query was
    /// cancelled; empty in non-query contexts. Polled by the consumer while waiting for a batch.
    const std::function<void()> check_cancellation;
    /// The owning query's thread group, captured at construction (on the consumer thread). Workers
    /// attach to it so listing memory is accounted to the query and in-flight requests observe its
    /// cancellation, mirroring the serial listing path. Null in non-query contexts.
    const ThreadGroupPtr thread_group;

    mutable std::mutex mutex;
    std::condition_variable work_available;
    std::condition_variable result_available;
    std::condition_variable space_available;

    std::deque<ListRange> ranges_to_list;
    size_t outstanding_ranges = 0;

    std::deque<RelativePathsWithMetadata> ready_batches;
    size_t buffered_objects = 0;

    bool started = false;
    bool finished = false;
    bool stop = false;
    std::exception_ptr first_exception;

    bool is_initialized = false;
    bool consumer_finished = false;
    RelativePathsWithMetadata current_batch;
    RelativePathsWithMetadata::iterator current_batch_iterator;
    std::atomic<size_t> accumulated_size = 0;

    ThreadPool pool;
};

}
