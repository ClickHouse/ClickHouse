#pragma once

#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageIterator.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Common/ThreadPool.h>

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

/// Lists an object storage prefix in parallel by walking the "directory" tree formed by a delimiter.
///
/// Instead of paginating a single prefix in one serial stream (which is bound by the per-request
/// latency of `ListObjectsV2`), several worker threads list different sub-"directories" (common
/// prefixes) concurrently. Each worker takes a prefix from a shared work queue, lists exactly one
/// level (grouping by the delimiter), enqueues the discovered sub-directories for further listing,
/// and pushes the keys it finds to the consumer. Sub-directories that cannot contain a matching key
/// are skipped via the `should_descend` predicate, so whole subtrees are pruned instead of listed.
///
/// The order in which keys are produced is unspecified (it depends on thread scheduling).
///
/// The iterator is storage-agnostic: it drives a caller-provided `list_level` callback (which lists a
/// single delimited level of one prefix, one page per call) and a `should_descend` callback (which
/// decides which discovered sub-directories are worth listing). Both must be thread-safe.
class ObjectStorageParallelListingIterator final : public IObjectStorageIterator
{
public:
    /// Lists a single delimited level of `prefix`, one page per call. The first call for a prefix is
    /// made with an empty `continuation_token`; if the returned result `is_truncated`, it is called
    /// again with the returned `next_continuation_token`, and so on.
    using ListLevelFunction
        = std::function<ObjectStorageListResult(const std::string & prefix, const std::string & continuation_token)>;

    /// `should_descend(common_prefix)` decides whether a discovered sub-"directory" (which always
    /// ends with the listing delimiter) might contain a key the caller is interested in. It may be
    /// called concurrently from several worker threads. Returning `true` when unsure is safe (the
    /// caller is expected to filter keys afterwards); it only costs extra listing requests.
    ///
    /// `max_buffered_keys` bounds (softly) how many keys may be buffered ahead of the consumer.
    ObjectStorageParallelListingIterator(
        std::string root_prefix_,
        size_t num_threads_,
        size_t max_buffered_keys_,
        ListLevelFunction list_level_,
        std::function<bool(const std::string & common_prefix)> should_descend_);

    ~ObjectStorageParallelListingIterator() override;

    void next() override;
    void nextBatch() override;
    bool isValid() override;
    RelativePathWithMetadataPtr current() override;
    RelativePathsWithMetadata currentBatch() override;
    std::optional<RelativePathsWithMetadata> getCurrentBatchAndScheduleNext() override;
    size_t getAccumulatedSize() const override;

private:
    /// Worker-thread main loop: pull prefixes from the work queue and list them.
    void worker();
    /// List a single prefix completely (paginating one level), pushing keys to the result queue and
    /// child common prefixes to the work queue. Returns false if an exception was stored and the walk
    /// must stop.
    bool listPrefix(const std::string & prefix);

    /// Pops the next ready batch, blocking until one is available or the walk is finished.
    /// Returns nullopt only when the walk is finished and no more batches will arrive.
    /// Rethrows the first worker exception, if any. Requires `lock` to be held on `mutex`.
    std::optional<RelativePathsWithMetadata> popBatch(std::unique_lock<std::mutex> & lock);

    /// Lazily starts the worker threads on first use. Requires `lock` to be held on `mutex`.
    void ensureStarted(std::unique_lock<std::mutex> & lock);

    /// Moves the consumer cursor to the next batch (blocking via `popBatch`). Requires `lock` held.
    void advanceLocked(std::unique_lock<std::mutex> & lock);

    const size_t num_threads;
    /// Soft upper bound on the number of keys buffered in `ready_batches` (backpressure).
    const size_t max_buffered_objects;
    const ListLevelFunction list_level;
    const std::function<bool(const std::string & common_prefix)> should_descend;

    mutable std::mutex mutex;
    std::condition_variable work_available; /// A prefix is available to list, or the walk finished.
    std::condition_variable result_available; /// A batch of keys is available, or the walk finished.
    std::condition_variable space_available; /// Buffered keys dropped below the limit (backpressure).

    std::deque<std::string> prefixes_to_list;
    /// Number of prefixes that have been enqueued but not yet fully listed. When it reaches zero the
    /// walk is complete. Guarded by `mutex`.
    size_t outstanding_prefixes = 0;

    std::deque<RelativePathsWithMetadata> ready_batches;
    size_t buffered_objects = 0;

    bool started = false;
    bool finished = false; /// The walk produced everything (or failed) and no more batches will arrive.
    bool stop = false; /// Destructor requested shutdown.
    std::exception_ptr first_exception;

    /// Consumer-side cursor over the batch handed out by the iterator interface.
    bool is_initialized = false;
    bool consumer_finished = false;
    RelativePathsWithMetadata current_batch;
    RelativePathsWithMetadata::iterator current_batch_iterator;
    std::atomic<size_t> accumulated_size = 0;

    ThreadPool pool;
};

}
