#pragma once

#include <Common/SharedMutex.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/MergeTree/VectorSimilarityIndexCache.h>

#include <roaring.hh>

namespace DB
{

class IMergeTreeDataPart;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;

using SkipIndexReadResult = std::vector<bool>;
using SkipIndexReadResultPtr = std::shared_ptr<SkipIndexReadResult>;

class MergeTreeSkipIndexReader
{
public:
    MergeTreeSkipIndexReader(
        UsefulSkipIndexes skip_indexes_,
        MarkCachePtr mark_cache_,
        UncompressedCachePtr uncompressed_cache_,
        VectorSimilarityIndexCachePtr vector_similarity_index_cache_,
        MergeTreeReaderSettings reader_settings_,
        LoggerPtr log_);

    SkipIndexReadResultPtr read(const RangesInDataPart & part);

    void cancel() noexcept { is_cancelled = true; }

private:
    UsefulSkipIndexes skip_indexes;
    MarkCachePtr mark_cache;
    UncompressedCachePtr uncompressed_cache;
    VectorSimilarityIndexCachePtr vector_similarity_index_cache;
    MergeTreeReaderSettings reader_settings;
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

struct MergeTreeIndexReadResult
{
    SkipIndexReadResultPtr skip_index_read_result;
    ProjectionIndexBitmapPtr projection_index_read_result;
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
    /// This map uses raw pointer of data part as key because it is unique and stable for the lifetime of the part.
    MergeTreeIndexReadResultPtr getOrBuildIndexReadResult(const RangesInDataPart & part, const RangesInDataParts & projection_parts);

    /// Cleans up the cached MergeTreeIndexReadResult for a given part if it exists.
    /// Should be called when the last task for the part has finished.
    void clear(const DataPartPtr & part);

    void cancel() noexcept;

private:
    MergeTreeSkipIndexReaderPtr skip_index_reader;
    MergeTreeProjectionIndexReaderPtr projection_index_reader;

    /// Stores MergeTreeIndexReadResult instances per part to avoid redundant construction.
    std::unordered_map<const IMergeTreeDataPart *, IndexReadResultEntry> index_read_result_registry;
    SharedMutex index_read_result_registry_mutex;
};

using MergeTreeIndexReadResultPoolPtr = std::shared_ptr<MergeTreeIndexReadResultPool>;

}
