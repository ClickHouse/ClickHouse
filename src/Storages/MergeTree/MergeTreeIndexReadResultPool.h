#pragma once

#include <Common/SharedMutex.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/MergeTree/ConditionTemplate.h>
#include <Storages/MergeTree/IMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/VectorSimilarityIndexCache.h>
#include <Storages/MergeTree/MergeTreeIndexMinMax.h>
#include <Storages/MergeTree/KeyCondition.h>

#include <atomic>
#include <functional>
#include <mutex>
#include <roaring/roaring.hh>

namespace DB
{

class IMergeTreeDataPart;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;

struct SkipIndexReadInput;

/// Granule pruning by the JOIN runtime filters that were not ready when the index result of a part was built.
/// A granule only changes from kept to dropped, so a reader that sees a stale flag reads the granule, which is
/// correct, only slower.
struct LateRuntimeFilterPruning
{
    LateRuntimeFilterPruning(
        MergeTreeDataPartInfoForReaderPtr part_info_,
        MarkRanges ranges_,
        RangesInDataPartReadHints read_hints_,
        size_t part_starting_offset_in_query_,
        StorageMetadataPtr metadata_snapshot_,
        NameSet all_updated_columns_,
        size_t num_marks,
        size_t num_pending_);

    bool isDropped(size_t mark) const { return dropped[mark].load(std::memory_order_relaxed); }

    /// The part and the granules left by the indexes and the filters that were ready.
    const MergeTreeDataPartInfoForReaderPtr part_info;
    const MarkRanges ranges;
    const RangesInDataPartReadHints read_hints;
    const size_t part_starting_offset_in_query;
    const StorageMetadataPtr metadata_snapshot;
    const NameSet all_updated_columns;

    std::unique_ptr<std::atomic<bool>[]> dropped;
    /// The number of filters that were not ready at the last pruning.
    std::atomic<size_t> num_pending;
    /// Held by the one task that prunes again.
    std::mutex mutex;
};

struct SkipIndexReadResult
{
    std::vector<bool> granules_selected; /// granules selected by skip index(es) at read time
    std::shared_ptr<MergeTreeIndexBulkGranulesMinMax> min_max_index_for_top_k;
    TopKThresholdTrackerPtr threshold_tracker;

    /// The primary index of the part, for skipping granules by the running top-K threshold through the
    /// primary key: `top_k_primary_key_column_position` is the position of the threshold column among the
    /// key columns. Null when the threshold column is not a primary key column.
    std::shared_ptr<const Columns> primary_index_for_top_k;
    size_t top_k_primary_key_column_position = 0;

    /// Whether every row of the granule lies beyond the running top-K threshold according to the primary index.
    bool isGranuleBeyondTopKThreshold(size_t mark) const;

    /// Whether granules may be dropped by something other than the primary-key top-K check above: skip indexes
    /// (the `minmax` top-K one included) or JOIN runtime filters.
    bool may_skip_granules_besides_top_k_primary_key = true;
    /// Pre-computed index granules for indexes created for the whole part.
    IndexGranulesMap index_granules;
    /// Set when some JOIN runtime filters were not ready when this result was built.
    std::shared_ptr<LateRuntimeFilterPruning> late_runtime_filters;
};

using SkipIndexReadResultPtr = std::shared_ptr<SkipIndexReadResult>;

class MergeTreeSkipIndexReader
{
public:
    /// Builds a predicate into the DAG to prune granules at read time, or nullptr for none.
    using DynamicPredicateBuilder = std::function<const ActionsDAG::Node *(ActionsDAG &)>;

    /// The number of filters of the predicate that are not ready yet and so are left out of it.
    using DynamicPredicatePendingCounter = std::function<size_t()>;

    /// Whether a dynamic skip index should be applied at read time.
    using DynamicSkipIndexFilter = std::function<bool(const IMergeTreeIndex &)>;

    MergeTreeSkipIndexReader(
        UsefulSkipIndexes skip_indexes_,
        ConditionTemplate<KeyCondition>::Ptr key_condition_rpn_template_,
        bool use_for_disjunctions_,
        MarkCachePtr mark_cache_,
        UncompressedCachePtr uncompressed_cache_,
        VectorSimilarityIndexCachePtr vector_similarity_index_cache_,
        MergeTreeReaderSettings reader_settings_,
        DynamicPredicateBuilder dynamic_predicate_builder_,
        DynamicPredicatePendingCounter dynamic_predicate_pending_counter_,
        bool prune_primary_key_,
        MergeTreeIndices dynamic_skip_indexes_,
        DynamicSkipIndexFilter dynamic_skip_index_filter_,
        ContextPtr context_,
        LoggerPtr log_);

    SkipIndexReadResultPtr read(
        const MergeTreeDataPartInfoForReaderPtr & part_info,
        const SkipIndexReadInput & input,
        const StorageMetadataPtr & metadata_snapshot,
        const NameSet & all_updated_columns);

    /// Whether `read` prunes by JOIN runtime filters. It prunes with the filters that are ready, fail-open;
    /// `pruneByLateRuntimeFilters` prunes the result again with the filters that became ready later.
    bool hasRuntimeFilters() const;

    /// Prunes the granules of `result` with the JOIN runtime filters that became ready since it was built or last
    /// pruned. Does nothing if there are none, or if another thread is already doing it.
    void pruneByLateRuntimeFilters(const SkipIndexReadResult & result) const;

    /// Lets `canSkipMark` skip granules by the primary key against a top-K threshold that becomes known
    /// while reading (see `TopKThresholdTracker`): the threshold column is the primary key column at
    /// `primary_key_column_position`.
    void setTopKPrimaryKeyPruning(size_t primary_key_column_position, TopKThresholdTrackerPtr tracker);

    void cancel() noexcept { is_cancelled = true; }

private:
    UsefulSkipIndexes skip_indexes;
    ConditionTemplate<KeyCondition>::Ptr key_condition_rpn_template;
    bool use_for_disjunctions;
    MarkCachePtr mark_cache;
    UncompressedCachePtr uncompressed_cache;
    VectorSimilarityIndexCachePtr vector_similarity_index_cache;
    MergeTreeReaderSettings reader_settings;

    /// Prunes `ranges` with the dynamic predicate; `num_pending` is the number of its filters that were not ready.
    MarkRanges pruneByDynamicPredicate(
        const MergeTreeDataPartInfoForReaderPtr & part_info,
        MarkRanges ranges,
        const RangesInDataPartReadHints & read_hints,
        size_t part_starting_offset_in_query,
        const StorageMetadataPtr & metadata_snapshot,
        const NameSet & all_updated_columns,
        size_t & num_pending) const;

    DynamicPredicateBuilder dynamic_predicate_builder;
    DynamicPredicatePendingCounter dynamic_predicate_pending_counter;
    bool prune_primary_key = false;
    MergeTreeIndices dynamic_skip_indexes;
    DynamicSkipIndexFilter dynamic_skip_index_filter;

    std::optional<size_t> top_k_primary_key_column_position;
    TopKThresholdTrackerPtr top_k_threshold_tracker;
    ContextPtr context;
    LoggerPtr log;

    std::atomic_bool is_cancelled = false;
};

using MergeTreeSkipIndexReaderPtr = std::shared_ptr<MergeTreeSkipIndexReader>;

struct ProjectionIndexBitmap;
using ProjectionIndexBitmapPtr = std::shared_ptr<ProjectionIndexBitmap>;
using ProjectionIndexBitmaps = std::vector<ProjectionIndexBitmapPtr>;

/// A bitmap wrapper for either 32-bit or 64-bit Roaring bitmaps, used in projection index processing.
struct ProjectionIndexBitmap
{
    enum class BitmapType
    {
        Bitmap32, // For roaring_bitmap_t (32-bit)
        Bitmap64 // For roaring64_bitmap_t (64-bit)
    };

    union BitmapUnion
    {
        roaring::api::roaring_bitmap_t * bitmap32;
        roaring::api::roaring64_bitmap_t * bitmap64;
        BitmapUnion() : bitmap32(nullptr) {}
    };

    BitmapType type;
    BitmapUnion data;

    explicit ProjectionIndexBitmap(BitmapType bitmap_type);
    ~ProjectionIndexBitmap();

    // Non-copyable
    ProjectionIndexBitmap(const ProjectionIndexBitmap &) = delete;
    ProjectionIndexBitmap & operator=(const ProjectionIndexBitmap &) = delete;

    static ProjectionIndexBitmapPtr create32();
    static ProjectionIndexBitmapPtr create64();

    void intersectWith(const ProjectionIndexBitmap & other);
    size_t cardinality() const;
    bool empty() const;

    template <typename Offset>
    bool contains(std::type_identity_t<Offset> value);

    template <typename Offset>
    void add(std::type_identity_t<Offset> value);

    template <typename Offset>
    void addBulk(const std::type_identity_t<Offset> * values, size_t size);

    /// Checks whether the bitmap has no bits set in the range [begin, end).
    bool rangeAllZero(size_t begin, size_t end) const;

    /// Appends a `PaddedPODArray<UInt8>` in the range [starting_row, starting_row + num_rows)
    /// with `1`s at positions corresponding to values present in the bitmap.
    ///
    /// For example, if the bitmap contains values {101, 103} and `starting_row = 100`, `num_rows = 5`,
    /// the resulting buffer segment (of size `num_rows`) will be: [0, 1, 0, 1, 0].
    ///
    /// The method resizes the `filter` array by appending `num_rows` zero-initialized bytes to it,
    /// and writes `1` to positions where a row number is found in the bitmap.
    ///
    /// Returns true if at least one value in the bitmap falls within the specified range.
    bool appendToFilter(PaddedPODArray<UInt8> & filter, size_t starting_row, size_t num_rows) const;
};

class MergeTreeSelectProcessor;
using MergeTreeSelectProcessorPtr = std::unique_ptr<MergeTreeSelectProcessor>;
class MergeTreeReadPoolProjectionIndex;

class SingleProjectionIndexReader
{
public:
    SingleProjectionIndexReader(
        std::shared_ptr<MergeTreeReadPoolProjectionIndex> pool,
        PrewhereInfoPtr prewhere_info,
        const ExpressionActionsSettings & actions_settings,
        const MergeTreeReaderSettings & reader_settings);

    ProjectionIndexBitmapPtr read(const RangesInDataPart & ranges);

    void cancel() noexcept;

private:
    std::shared_ptr<MergeTreeReadPoolProjectionIndex> projection_index_read_pool;
    MergeTreeSelectProcessorPtr processor;
};

using ProjectionIndexReaderByName = std::unordered_map<String, SingleProjectionIndexReader>;

class MergeTreeProjectionIndexReader
{
public:
    explicit MergeTreeProjectionIndexReader(ProjectionIndexReaderByName projection_index_readers_);

    ProjectionIndexBitmapPtr read(const RangesInDataParts & projection_parts);

    void cancel() noexcept;

private:
    ProjectionIndexReaderByName projection_index_readers;
};

using MergeTreeProjectionIndexReaderPtr = std::shared_ptr<MergeTreeProjectionIndexReader>;

class MergeTreeIndexGranularity;

struct MergeTreeIndexReadResult
{
    SkipIndexReadResultPtr skip_index_read_result;
    ProjectionIndexBitmapPtr projection_index_read_result;

    /// Whether index read result is useful and marks can be skipped.
    bool canSkipAnyMark() const;

    /// Like `canSkipAnyMark`, but leaving out the granules skipped by the primary key against a top-K threshold.
    bool canSkipAnyMarkBesidesTopKPrimaryKey() const;

    /// Whether all rows of the granule are filtered out by the present index read results.
    bool canSkipMark(size_t mark, const MergeTreeIndexGranularity & index_granularity) const;
};

using MergeTreeIndexReadResultPtr = std::shared_ptr<MergeTreeIndexReadResult>;

/// A pool structure for reading and building filters from MergeTree indexes. This pool coordinates the parallel
/// execution of index reading tasks, and it is responsible for building and sharing the result across MergeTree read
/// tasks from the same data part.
///
/// Key Responsibilities:
/// - Serves as the unified access point for reading index data in a multi-threaded environment.
/// - Lazily builds the MergeTreeIndexReadResult for a part on-demand, ensuring that only one thread performs
///   the construction, while others wait for the result via shared futures.
/// - Stores granule filters in a shared registry keyed by data part identity (raw pointer),
///   allowing concurrent tasks to reuse the result efficiently.
/// - Handles cleanup of granule filters once the last read task for a part completes, releasing associated resources.
class MergeTreeIndexReadResultPool
{
public:
    MergeTreeIndexReadResultPool(
        MergeTreeSkipIndexReaderPtr skip_index_reader_, MergeTreeProjectionIndexReaderPtr projection_index_reader_);

    /// Holds a shared future to a lazily built MergeTreeIndexReadResult.
    /// This enables concurrent consumers to wait on a single computation.
    struct IndexReadResultEntry
    {
        std::shared_ptr<std::promise<MergeTreeIndexReadResultPtr>> promise;
        std::shared_future<MergeTreeIndexReadResultPtr> future;

        IndexReadResultEntry()
            : promise(std::make_shared<std::promise<MergeTreeIndexReadResultPtr>>())
            , future(promise->get_future().share())
        {
        }
    };

    /// Lazily constructs and caches the MergeTreeIndexReadResult for a given data part. If it is already being built by
    /// another thread, waits for its result. Throws if the builder fails.
    ///
    /// This map is keyed by `part_index_in_query`, which is unique and stable for the query.
    MergeTreeIndexReadResultPtr getOrBuildIndexReadResult(
        size_t part_index,
        const MergeTreeDataPartInfoForReaderPtr & part_info,
        const SkipIndexReadInput & input,
        const RangesInDataParts & projection_parts,
        const StorageMetadataPtr & metadata_snapshot,
        const NameSet & all_updated_columns);

    /// Prunes a result of `getOrBuildIndexReadResult` again with the JOIN runtime filters that became ready later.
    void pruneByLateRuntimeFilters(const MergeTreeIndexReadResult & result) const;

    /// Cleans up the cached MergeTreeIndexReadResult for a given part if it exists.
    /// Should be called when the last task for the part has finished.
    void clear(size_t part_index);

    /// Whether index read results may include a skip index part (for any part of the query).
    bool hasSkipIndexReader() const { return skip_index_reader != nullptr; }

    /// Whether index read results depend on JOIN runtime filters (see MergeTreeSkipIndexReader::hasRuntimeFilters).
    bool hasRuntimeFilters() const { return skip_index_reader && skip_index_reader->hasRuntimeFilters(); }

    void cancel() noexcept;

private:
    MergeTreeSkipIndexReaderPtr skip_index_reader;
    MergeTreeProjectionIndexReaderPtr projection_index_reader;

    /// Stores MergeTreeIndexReadResult instances per part to avoid redundant construction.
    std::unordered_map<size_t, IndexReadResultEntry> index_read_result_registry;
    SharedMutex index_read_result_registry_mutex;
};

using MergeTreeIndexReadResultPoolPtr = std::shared_ptr<MergeTreeIndexReadResultPool>;

}
