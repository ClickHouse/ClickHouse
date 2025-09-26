#pragma once

#include <Common/SharedMutex.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/MergeTree/VectorSimilarityIndexCache.h>

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

struct MergeTreeIndexReadResult
{
    SkipIndexReadResultPtr skip_index_read_result;

    /// TODO(ab): Add projection index read result.
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
    explicit MergeTreeIndexReadResultPool(MergeTreeSkipIndexReaderPtr skip_index_reader_);

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
    MergeTreeIndexReadResultPtr getOrBuildIndexReadResult(const RangesInDataPart & part);

    /// Cleans up the cached MergeTreeIndexReadResult for a given part if it exists.
    /// Should be called when the last task for the part has finished.
    void clear(const DataPartPtr & part);

    void cancel() noexcept;

private:
    MergeTreeSkipIndexReaderPtr skip_index_reader;

    /// TODO(ab): Add projection index reader.

    /// Stores MergeTreeIndexReadResult instances per part to avoid redundant construction.
    std::unordered_map<const IMergeTreeDataPart *, IndexReadResultEntry> index_read_result_registry;
    SharedMutex index_read_result_registry_mutex;
};

using MergeTreeIndexReadResultPoolPtr = std::shared_ptr<MergeTreeIndexReadResultPool>;

}
