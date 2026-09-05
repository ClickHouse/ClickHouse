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
///  2. Big flat directories: a prefix that is truncated but has no '/' sub-directories on its first page
///     is split by keyspace. Its remaining range is tiled into contiguous `(start_after, end)` sub-ranges
///     whose boundaries are derived from the byte alphabet observed in the listed page, and each sub-range
///     is listed concurrently. Because the sub-ranges are *contiguous* they tile the interval with no
///     gaps — a key whose byte is not in the sampled alphabet still falls into the range that brackets
///     it — so the split is complete regardless of the key distribution or character set. Each sub-range
///     is still listed *with* the '/' delimiter, so a sub-directory that only a later page would have
///     revealed (a mixed prefix that merely looked flat on its first page) still surfaces as a common
///     prefix and is pruned via `should_descend`, rather than being scanned recursively.
///
/// The order in which keys are produced is unspecified (it depends on thread scheduling).
///
/// Worker threads are started on demand rather than all at once: the pool grows only as ranges appear
/// and never past `num_threads`. A flat directory that never fans out lists on a single worker, so a
/// large `s3_list_object_parallelism` does not reserve the whole clamped worker count up front (which,
/// with the global-pool-backed `ThreadPool`, would otherwise be able to starve the server for a tiny
/// listing); a wide tree still grows the pool up to `num_threads` as sub-directories are discovered.
///
/// The walk is depth-first with a bounded frontier. Each worker keeps the sub-directories it discovers on
/// its own private stack and descends into them, handing off only the shallowest ones to a small shared
/// queue (capped at `max_pending_ranges`) for idle workers to steal. This bounds the number of pending
/// ranges to `O(num_threads * tree_depth * per-level_fan-out)` rather than the breadth-first
/// `O(total_directories)`: a wide hierarchical layout (e.g. `year=*/month=*/day=*/file.csv`) whose upper
/// pages return only common prefixes — and so never trip the buffered-object backpressure, since they
/// produce no leaf objects for the consumer to drain — no longer lets the frontier grow with the total
/// directory count and burn per-query memory unrelated to `s3_list_object_keys_size`. A single directory
/// that itself spans many pages of common prefixes is not paginated in place either: once a page has
/// produced child ranges, the parent's *continuation* is re-enqueued as its own range (walked depth-first
/// / stealable like any other) so a worker descends into a child before collecting the next page of
/// siblings, keeping the frontier to at most one page of siblings per active parent regardless of how many
/// sub-directories the parent has. The frontier is bounded without ever blocking a producer on it: because
/// the pending ranges are consumed only by the same worker pool (unlike buffered objects, which an external
/// consumer drains), blocking a producer on the frontier size could deadlock, so overflow is carried
/// depth-first instead of waited on.
///
/// On top of the depth-first shape there is a hard byte budget (`max_pending_range_bytes`) on the *total*
/// pending ranges across the shared queue and every worker's private frontier. Depth-first walking alone
/// bounds the frontier to `O(num_threads * tree_depth * page_size)` ranges, which at the extreme settings
/// (`s3_list_object_parallelism` and `s3_list_object_keys_size` both at 1000) is still about a million
/// pending ranges per level — hundreds of MB of per-query memory. The budget is enforced without ever
/// blocking a producer (which could deadlock, see above) and without dropping work: when materializing a
/// page's fan-out would exceed it, the worker keeps only the ranges that fit (at least one, so the walk
/// always progresses) and represents the rest as a single range — for a hierarchical page, a "resume"
/// range that re-lists the parent strictly after the last kept child (`StartAfter`), re-discovering the
/// unkept siblings later; for a keyspace split, the contiguous tail slices merged into one. The extra
/// re-listing requests are paid only while the budget is exhausted, so workloads whose frontier fits the
/// budget (any realistic parallelism) see no change, while pathological fan-outs are capped at the budget
/// instead of growing with `num_threads * page_size`.
///
/// The listed objects buffered ahead of the consumer (`ready_batches`) are bounded the same way, by a hard
/// byte budget (`max_buffered_object_bytes`) in addition to the caller's count cap — see
/// `DEFAULT_MAX_BUFFERED_OBJECT_BYTES`. Here blocking the producers is safe (the consumer drains the buffer
/// independently of the worker pool), so workers simply wait for space before listing another page.
///
/// The iterator is storage-agnostic: it drives a caller-provided `list_level` callback (one delimited
/// page of one prefix per call, optionally resuming after a key) and a `should_descend` callback.
class ObjectStorageParallelListingIterator final : public IObjectStorageIterator
{
public:
    /// Hard budget on the total bytes of pending (created but not yet picked up) ranges across the shared
    /// queue and all private frontiers; see the class comment. Sized so that any realistic configuration
    /// never trims (a range is typically well under a KiB, so this holds tens of thousands of them) while
    /// the pathological `num_threads * page_size` fan-out is capped here instead of reaching hundreds of MB.
    static constexpr size_t DEFAULT_MAX_PENDING_RANGE_BYTES = 32 * 1024 * 1024;

    /// Hard budget on the total bytes of listed objects buffered ahead of the consumer (`ready_batches`).
    /// The count cap (`max_buffered_keys`) passed by the caller scales with the user-set parallelism and
    /// page size, so at extreme settings it alone would admit millions of buffered `RelativePathWithMetadata`
    /// entries; this byte budget bounds the buffered-object memory independently of those settings. Unlike
    /// pending ranges, buffered objects are drained by the external consumer, so workers can safely *block*
    /// on this budget (no deadlock): a worker publishes its already-listed page (the budget is checked
    /// before listing the next one), which keeps the overshoot to at most one in-flight page per worker.
    static constexpr size_t DEFAULT_MAX_BUFFERED_OBJECT_BYTES = 32 * 1024 * 1024;

    /// Lists a single delimited page. `start_after` (honored only on the first call of a listing, i.e.
    /// when `continuation_token` is empty) resumes strictly after that key; `continuation_token`
    /// resumes pagination. Returns objects + common prefixes + truncation/next-token.
    using ListLevelFunction = std::function<ObjectStorageListResult(
        const std::string & prefix, const std::string & delimiter, const std::string & start_after, const std::string & continuation_token)>;

    /// A lightweight existence-only probe used by the flat keyspace split (`splitWouldHelp`): it only needs
    /// to know whether *any* key exists past a boundary, so it must not fetch per-object metadata that the
    /// probe discards (on `S3` the caller wires this to a `with_tags=false`, single-key `ListObjectsV2`), and
    /// its result is only inspected at `.front()`. Separate from `list_level` so that enabling
    /// `s3_list_object_parallelism` never turns a `_tags` scan into a fan of redundant `GetObjectTagging`
    /// requests for pages that the probe throws away.
    using ProbeLevelFunction = ListLevelFunction;

    /// `should_descend(common_prefix)` decides whether a discovered sub-"directory" might contain a key
    /// of interest. It may be called concurrently. Returning `true` when unsure is safe.
    /// `max_buffered_keys` softly bounds how many keys may be buffered ahead of the consumer.
    /// `allow_keyspace_split` enables splitting a big flat directory by keyspace (issuing `start_after`
    /// requests, still with the '/' delimiter). Set it to false for storages that do not support
    /// `StartAfter` (e.g. S3 Express / directory buckets); such flat ranges are then paginated serially,
    /// while the hierarchical delimiter walk stays parallel.
    /// `check_cancellation`, if set, must throw when the query owning this listing was cancelled; it is
    /// polled while the consumer waits for a batch (a `KILL`/timeout does not notify our condition
    /// variables), so the listing fails fast instead of hanging. Empty in non-query contexts (tests).
    /// `max_pending_range_bytes` is the hard pending-range byte budget described in the class comment;
    /// overridable only so tests can shrink it to force trimming on small fixtures.
    /// `max_buffered_object_bytes` is the hard byte budget on buffered listed objects (see
    /// `DEFAULT_MAX_BUFFERED_OBJECT_BYTES`); overridable only so tests can shrink it.
    /// `allow_start_after` states whether the storage accepts a `start_after` at all (`StartAfter` of
    /// `ListObjectsV2`; see `IObjectStorage::supportsStartAfterListing`). When false, no request issued by
    /// this iterator carries one: flat keyspace splitting is off (as with `allow_keyspace_split = false`) and
    /// the pending-range budget trim resumes a hierarchical parent by re-listing it from the beginning with
    /// the '/' delimiter and locally discarding what the kept child ranges already cover, instead of
    /// resuming after the last kept child by key.
    /// `root_range_end`, when non-empty, is an inclusive upper bound on the keys of interest under
    /// `root_prefix`: pagination of the root level stops once its listed entries sort past it, so entries
    /// beyond the bound are neither fetched page by page nor emitted. Used when `root_prefix` is *wider*
    /// than the glob's fixed prefix (see `chooseDelimitedListingStartPrefix`) to stop the walk's own level
    /// at the end of the fixed prefix's key region instead of paging through every later loose object.
    /// Sub-"directories" discovered within the bound are walked unbounded — their keys share the
    /// discovered prefix and therefore sort within the bound already.
    /// `is_marker_only_prefix(common_prefix)`, when set, tells that the only key of interest under a
    /// discovered "directory" is its own directory-marker object (the key equal to the prefix). Such a
    /// range is listed for exactly one page — the marker sorts before every other key under the prefix, so
    /// the first page either returns it or proves it absent — and the page's other keys are dropped. Used
    /// for trailing-slash globs (`root/*/`), whose matching keys are the markers themselves; see
    /// `makeIsMarkerOnlyPrefixPredicate`. Without it a marker-less layout would paginate (and keyspace-split)
    /// every subtree in full only to prove the markers are absent.
    ObjectStorageParallelListingIterator(
        std::string root_prefix_,
        size_t num_threads_,
        size_t max_buffered_keys_,
        ListLevelFunction list_level_,
        ProbeLevelFunction probe_level_,
        std::function<bool(const std::string & common_prefix)> should_descend_,
        bool allow_keyspace_split_ = true,
        std::function<void()> check_cancellation_ = {},
        size_t max_pending_range_bytes_ = DEFAULT_MAX_PENDING_RANGE_BYTES,
        size_t max_buffered_object_bytes_ = DEFAULT_MAX_BUFFERED_OBJECT_BYTES,
        bool allow_start_after_ = true,
        std::string root_range_end_ = {},
        std::function<bool(const std::string & common_prefix)> is_marker_only_prefix_ = {});

    ~ObjectStorageParallelListingIterator() override;

    void next() override;
    void nextBatch() override;
    bool isValid() override;
    RelativePathWithMetadataPtr current() override;
    RelativePathsWithMetadata currentBatch() override;
    std::optional<RelativePathsWithMetadata> getCurrentBatchAndScheduleNext() override;
    size_t getAccumulatedSize() const override;

    /// For tests/observability: the high-water mark of the number of pending (created but not yet fully
    /// listed) ranges. Depth-first walking keeps this bounded by the active parallelism and the tree depth
    /// rather than the total number of directories, which is what stops a wide hierarchical layout from
    /// growing the listing frontier — and thus per-query memory — without limit.
    size_t getPeakOutstandingRanges() const;

    /// For tests/observability: the high-water mark of the total bytes of pending ranges (across the shared
    /// queue and all private frontiers). Stays within `max_pending_range_bytes` plus a small overshoot — the
    /// minimum kept for progress, a couple of ranges per tree level of each worker's active depth-first
    /// path — regardless of the width of the layout being listed.
    size_t getPeakPendingRangeBytes() const;

    /// For tests/observability: the high-water mark of the total bytes of listed objects buffered ahead of
    /// the consumer. Stays within `max_buffered_object_bytes` plus at most one in-flight page per worker
    /// (the budget is checked before a worker lists its next page, never blocking an already-listed one).
    size_t getPeakBufferedObjectBytes() const;

    /// Approximate heap + struct footprint of one buffered batch of listed objects, the unit of the
    /// buffered-object byte budget. Charges the `ObjectMetadata` payload too (`etag`, `tags`,
    /// `attributes`): S3 listings buffer an `etag` for every object, and a `_tags` scan fills
    /// `metadata.tags`, either of which can dwarf the path itself. Public for tests.
    static size_t batchBytes(const RelativePathsWithMetadata & batch);

private:
    /// A half-open keyspace range `(start_after, end)` of keys under `prefix` left to list.
    struct ListRange
    {
        std::string prefix;        /// S3 Prefix.
        std::string start_after;   /// Exclusive lower bound; empty = from the beginning of `prefix`.
        std::string end;           /// Inclusive upper bound key; empty = unbounded.
        size_t split_pos = 0;      /// Byte position to split at if this range needs a flat (keyspace) split.
        size_t split_budget = 0;   /// How many more times this branch may flat-split (0 = paginate serially).
        bool use_delimiter = true; /// List with the '/' delimiter to discover (and prune) sub-directories.
                                   /// Kept true even for keyspace-split slices, so a mixed prefix that
                                   /// only looked flat on its first page is never scanned recursively.
        std::string continuation_token; /// Resumes a hierarchical parent whose pages of common prefixes are
                                   /// walked as separate ranges (so a directory with more immediate
                                   /// sub-directories than fit in one page does not buffer one child range
                                   /// per sub-directory before descending). Empty for a fresh range; when
                                   /// set, it resumes pagination and `start_after` is not re-applied.
        bool skip_prefixes_not_after = false; /// Set on a budget-trim "resume" range, whose `start_after` is
                                   /// exactly a common prefix whose subtree was kept as its own range. Real
                                   /// S3 returns only `CommonPrefixes` greater than `StartAfter` and skips
                                   /// that group by itself; an S3-compatible storage treating `StartAfter`
                                   /// as a plain key filter re-emits the equal common prefix (keys under the
                                   /// subtree are all greater than `start_after`), and this flag skips it to
                                   /// not list that subtree twice. The listing is correct under both
                                   /// semantics. Never set on keyspace-split slices, whose `start_after` is
                                   /// a plain key boundary (its byte past the shared base comes from keys of
                                   /// a delimited page, so it never falls inside a sub-directory group).
        bool marker_only = false;  /// The only key of interest under `prefix` is its own directory-marker
                                   /// object (the key equal to `prefix`), which sorts before every other
                                   /// key under it. Such a range is listed for a single page and is never
                                   /// paginated or keyspace-split: after that page the marker is either
                                   /// found or proven absent, and every other key of the subtree is
                                   /// irrelevant. See `makeIsMarkerOnlyPrefixPredicate`.
        bool resume_by_relisting = false; /// Set on a budget-trim "resume" range when the storage rejects
                                   /// `StartAfter` (S3 Express / directory buckets, see
                                   /// `IObjectStorage::supportsStartAfterListing`): the range is then listed
                                   /// from the *beginning* of `prefix` with the '/' delimiter only, and
                                   /// everything not after `start_after` is discarded locally (common
                                   /// prefixes by `skip_prefixes_not_after`, objects by the same bound). The
                                   /// result is identical to a `StartAfter` resume, at the cost of re-listing
                                   /// the pages before the resume point.
    };

    void worker();
    /// The body of `worker`: takes ranges (from the shared queue or the private `local_frontier`) and lists
    /// them until the walk completes or is stopped. Any exception it lets escape — not only from `listRange`,
    /// e.g. from `donateLocked` failing to schedule a new worker — is stored by `worker` and finishes the
    /// iterator, so the consumer rethrows it instead of blocking forever on the dropped ranges.
    void workerLoop(std::deque<ListRange> & local_frontier);
    /// Lists one range completely (paginating, splitting flat sub-trees). Sub-directories it discovers are
    /// pushed onto this worker's private `local_frontier` (from which they are walked depth-first and, for
    /// the shallowest of them, donated to the shared queue). Returns false if an exception was stored and
    /// the walk must stop.
    bool listRange(const ListRange & range, std::deque<ListRange> & local_frontier);
    /// Tiles `(last_key, range.end)` into contiguous sub-ranges using boundaries derived from the byte
    /// alphabet of `sample`. Returns empty if the range cannot be usefully split (caller paginates).
    std::vector<ListRange> splitFlatRange(const ListRange & range, const std::string & last_key, const RelativePathsWithMetadata & sample) const;
    /// One cheap probe: is there any key beyond the bucket `last_key` sits in (within the range)?
    /// If not, the keys share a common prefix and splitting cannot parallelize them — so the caller
    /// paginates serially instead of issuing a fan of empty boundary probes.
    bool splitWouldHelp(const ListRange & range, const std::string & delimiter, const std::string & last_key) const;

    std::optional<RelativePathsWithMetadata> popBatch(std::unique_lock<std::mutex> & lock);
    void ensureStarted(std::unique_lock<std::mutex> & lock);
    /// Start just enough additional workers (up to `num_threads`) so that every currently queued range
    /// has an idle or freshly started worker to serve it. Called whenever ranges are enqueued, so the
    /// pool grows on demand instead of eagerly reserving `num_threads` workers up front. Requires lock.
    void maybeSpawnWorkers(std::unique_lock<std::mutex> & lock);
    void advanceLocked(std::unique_lock<std::mutex> & lock);
    /// Account newly discovered ranges (updating the outstanding-range counter) onto `local_frontier`, emit
    /// a batch, and donate the shallowest local ranges to the shared queue. Requires lock.
    void enqueueLocked(
        std::vector<ListRange> & new_ranges,
        RelativePathsWithMetadata & batch,
        std::deque<ListRange> & local_frontier,
        std::unique_lock<std::mutex> & lock);
    /// Move the shallowest ranges of a worker's `local_frontier` into the shared queue (bounded by
    /// `max_pending_ranges`) so idle workers can steal them, keeping the walk depth-first. Requires lock.
    void donateLocked(std::deque<ListRange> & local_frontier, std::unique_lock<std::mutex> & lock);
    /// Enforce the hard pending-range byte budget on a page's fan-out before it is materialized: if adding
    /// `follow_up` would exceed `max_pending_range_bytes`, keep only the leading ranges that fit (at least
    /// one, so the walk progresses) and represent the rest as a single range — a `StartAfter` resume of
    /// `range` after the last kept child for a hierarchical page (`hierarchical`; also subsumes the parent's
    /// pending continuation, and drops the already-covered tail of `batch`), or the merged contiguous tail
    /// slice for a keyspace split. Requires lock (it reads `pending_range_bytes`).
    void trimToBudgetLocked(
        const ListRange & range,
        bool hierarchical,
        std::vector<ListRange> & follow_up,
        RelativePathsWithMetadata & batch,
        std::unique_lock<std::mutex> & lock) const;
    /// Approximate heap + struct footprint of one pending range, the unit of the byte budget.
    static size_t rangeBytes(const ListRange & range);

    const size_t num_threads;
    const size_t max_buffered_objects;
    /// Hard byte budget on buffered listed objects (`ready_batches`); see the public constant.
    const size_t max_buffered_object_bytes;
    /// Cap on the shared pending-range queue. Discovered ranges beyond it stay on the discovering worker's
    /// private frontier (walked depth-first), so the shared queue never grows without limit; it is sized to
    /// keep every worker fed with stealable work while staying small.
    const size_t max_pending_ranges;
    /// Hard budget on the total bytes of pending ranges (shared queue + all private frontiers); see the
    /// class comment and `trimToBudgetLocked`.
    const size_t max_pending_range_bytes;
    /// Whether the storage accepts `start_after` (`StartAfter` of `ListObjectsV2`); see the constructor.
    const bool allow_start_after;
    const ListLevelFunction list_level;
    /// Tags-free existence probe for the flat keyspace split; see `ProbeLevelFunction`.
    const ProbeLevelFunction probe_level;
    const std::function<bool(const std::string & common_prefix)> should_descend;
    /// Marks a discovered "directory" whose only key of interest is its own directory marker; see the
    /// constructor. Empty when no such directory exists for the glob being listed.
    const std::function<bool(const std::string & common_prefix)> is_marker_only_prefix;
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

    /// The shared, capped frontier of ranges available for any idle worker to pick up (workers also keep a
    /// larger private depth-first frontier of their own). Bounded by `max_pending_ranges`.
    std::deque<ListRange> ranges_to_list;
    /// Ranges created but not yet fully listed, across the shared queue, every worker's private frontier and
    /// the ranges being listed right now. Reaches zero exactly when the whole walk is done.
    size_t outstanding_ranges = 0;
    /// High-water mark of `outstanding_ranges`; a proxy for the peak pending-range memory. See
    /// `getPeakOutstandingRanges`.
    size_t peak_outstanding_ranges = 0;
    /// Total bytes of the pending ranges currently stored in the shared queue and the private frontiers
    /// (a range being listed right now is not counted: there is at most one per worker). Kept within
    /// `max_pending_range_bytes` by `trimToBudgetLocked`, and its high-water mark, for tests.
    size_t pending_range_bytes = 0;
    size_t peak_pending_range_bytes = 0;

    std::deque<RelativePathsWithMetadata> ready_batches;
    size_t buffered_objects = 0;
    /// Total approximate bytes of the batches in `ready_batches`, kept within `max_buffered_object_bytes`
    /// by worker backpressure, and its high-water mark, for tests.
    size_t buffered_object_bytes = 0;
    size_t peak_buffered_object_bytes = 0;

    bool started = false;
    bool finished = false;
    bool stop = false;
    /// Workers scheduled on the pool so far (grows on demand up to `num_threads`).
    size_t scheduled_workers = 0;
    /// Workers currently blocked waiting for a range (i.e. immediately available to serve a new range).
    size_t idle_workers = 0;
    std::exception_ptr first_exception;

    bool is_initialized = false;
    bool consumer_finished = false;
    /// Set once `getCurrentBatchAndScheduleNext` has handed the current batch to the caller: the next batch
    /// is fetched lazily, only when the consumer actually asks for it, rather than eagerly before the
    /// current one is returned. The workers keep filling `ready_batches` in the background regardless, so
    /// this preserves the batching API's overlap of listing with reading (see `getCurrentBatchAndScheduleNext`).
    bool advance_pending = false;
    RelativePathsWithMetadata current_batch;
    RelativePathsWithMetadata::iterator current_batch_iterator;
    std::atomic<size_t> accumulated_size = 0;

    ThreadPool pool;
};

}
