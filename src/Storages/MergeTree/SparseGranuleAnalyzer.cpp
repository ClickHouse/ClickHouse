#include <Storages/MergeTree/SparseGranuleAnalyzer.h>

#include <Columns/ColumnSparse.h>
#include <Core/Settings.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <IO/SharedThreadPools.h>
#include <Interpreters/Cache/QueryConditionCache.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/StorageSnapshot.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/threadPoolCallbackRunner.h>

#include <future>
#include <vector>


namespace DB
{

namespace Setting
{
    extern const SettingsBool use_query_condition_cache;
    extern const SettingsMaxThreads max_threads;
}

namespace
{

/// Cache keys: the verdict is a function of (part, column), not of the specific
/// predicate, so the two bitmaps can be shared by every predicate of the matching
/// class on the same column. Hashing a synthetic domain string with the column name
/// keeps these keys disjoint from any real `WHERE` predicate hash.
constexpr std::string_view CACHE_DOMAIN_DEFAULTS = "__sparse_offsets_defaults__";
constexpr std::string_view CACHE_DOMAIN_NON_DEFAULTS = "__sparse_offsets_non_defaults__";

UInt64 syntheticConditionHash(std::string_view domain, const String & column_name)
{
    SipHash hash;
    hash.update(domain.data(), domain.size());
    hash.update(column_name.data(), column_name.size());
    return hash.get64();
}

void writeBitmapToCache(
    QueryConditionCache & cache,
    const UUID & table_uuid,
    const String & part_name,
    UInt64 condition_hash,
    std::string_view condition_label,
    const std::vector<bool> & granule_bitmap,
    size_t marks_count,
    bool has_final_mark)
{
    /// `QueryConditionCache::write` takes mark ranges that have NO matches; collapse
    /// the contiguous `true` runs of `granule_bitmap` into that shape.
    MarkRanges no_match_ranges;
    size_t i = 0;
    while (i < granule_bitmap.size())
    {
        if (!granule_bitmap[i])
        {
            ++i;
            continue;
        }
        size_t j = i + 1;
        while (j < granule_bitmap.size() && granule_bitmap[j])
            ++j;
        no_match_ranges.emplace_back(i, j);
        i = j;
    }

    /// Always write, even when no granule is prunable: subsequent queries need to see
    /// "analyzed, nothing to prune" so they don't re-run the analyzer.
    cache.write(
        table_uuid, part_name, condition_hash, String(condition_label),
        no_match_ranges, marks_count, has_final_mark);
}

}

std::optional<SparseGranuleAnalysis>
analyzeSparseColumnGranules(
    const DataPartPtr & part,
    const String & column_name,
    const MarkRanges & ranges,
    const MergeTreeData & storage,
    const StorageSnapshotPtr & storage_snapshot,
    const ContextPtr & query_context,
    SparseOffsetsShare * offsets_share,
    LoggerPtr log,
    const std::atomic<bool> * is_cancelled)
{
    auto check_cancelled = [&] { return is_cancelled && is_cancelled->load(std::memory_order_acquire); };
    if (check_cancelled())
        return std::nullopt;

    /// Without a sparse offsets stream there is no cheap classification to make.
    const auto & infos = part->getSerializationInfos();
    auto it = infos.find(column_name);
    if (it == infos.end()
        || !ISerialization::hasKind(it->second->getKindStack(), ISerialization::Kind::SPARSE))
        return std::nullopt;

    const size_t total_marks = part->index_granularity->getMarksCountWithoutFinal();
    /// `QueryConditionCache` expects `marks_count` to *include* the final-mark sentinel,
    /// and `has_final_mark` then flips `matching_marks[marks_count-1]` to false. We
    /// never classify the sentinel ourselves, so always read/write only `[0, total_marks)`.
    const size_t cache_marks_count = part->index_granularity->getMarksCount();
    const bool has_final_mark = part->index_granularity->hasFinalMark();
    const UUID table_uuid = storage.getStorageID().uuid;
    const String cache_part_name = part->getNameWithParent();

    /// Setting is per-query, so it must come from `query_context`, not from the storage's
    /// startup context.
    auto cache = query_context->getSettingsRef()[Setting::use_query_condition_cache]
        ? query_context->getQueryConditionCache()
        : nullptr;
    if (cache && table_uuid != UUIDHelpers::Nil)
    {
        const UInt64 hash_defaults = syntheticConditionHash(CACHE_DOMAIN_DEFAULTS, column_name);
        const UInt64 hash_non_defaults = syntheticConditionHash(CACHE_DOMAIN_NON_DEFAULTS, column_name);
        auto cached_defaults = cache->read(table_uuid, cache_part_name, hash_defaults);
        auto cached_non_defaults = cache->read(table_uuid, cache_part_name, hash_non_defaults);
        if (cached_defaults && cached_non_defaults
            && cached_defaults->size() >= cache_marks_count
            && cached_non_defaults->size() >= cache_marks_count)
        {
            SparseGranuleAnalysis analysis;
            analysis.granule_has_only_defaults.resize(total_marks);
            analysis.granule_has_only_non_defaults.resize(total_marks);
            /// Cache bit `false` means "no matches for the predicate", which for our
            /// `Defaults` domain translates back to "all-default" (and symmetrically
            /// for `NonDefaults`).
            for (size_t g = 0; g < total_marks; ++g)
            {
                analysis.granule_has_only_defaults[g] = !cached_defaults->at(g);
                analysis.granule_has_only_non_defaults[g] = !cached_non_defaults->at(g);
            }
            return analysis;
        }
    }

    const auto & metadata_snapshot = storage_snapshot->metadata;
    auto column_in_storage = metadata_snapshot->getColumns().tryGetColumn(GetColumnsOptions::AllPhysical, column_name);
    if (!column_in_storage)
        return std::nullopt;

    NamesAndTypesList cols;
    cols.push_back(*column_in_storage);

    auto alter_conversions = std::make_shared<AlterConversions>();
    auto part_info = std::make_shared<LoadedMergeTreeDataPartInfoForReader>(part, alter_conversions);

    /// Split the input ranges across roughly `max_threads` chunks so the parallel pass
    /// on the global IO thread pool finishes in a single round instead of stair-stepping
    /// through multiple rounds when chunks > cores. Clamp to a [MIN, MAX] band so very
    /// small parts don't dispatch microscopic chunks (per-chunk reader setup would
    /// dominate) and very large parts don't create huge chunks that under-utilise cores.
    constexpr size_t MIN_ANALYZER_CHUNK_MARKS = 256;
    constexpr size_t MAX_ANALYZER_CHUNK_MARKS = 8192;

    size_t total_marks_in_ranges = 0;
    for (const auto & range : ranges)
        if (range.begin < range.end)
            total_marks_in_ranges += range.end - range.begin;

    const size_t target_chunks
        = std::max<size_t>(1, static_cast<UInt64>(query_context->getSettingsRef()[Setting::max_threads]));
    const size_t chunk_marks = total_marks_in_ranges == 0
        ? 0
        : std::clamp(
            (total_marks_in_ranges + target_chunks - 1) / target_chunks,
            MIN_ANALYZER_CHUNK_MARKS,
            MAX_ANALYZER_CHUNK_MARKS);

    std::vector<MarkRange> chunks;
    for (const auto & range : ranges)
    {
        if (range.begin >= range.end)
            continue;
        size_t cursor = range.begin;
        while (cursor < range.end)
        {
            const size_t end = std::min(cursor + chunk_marks, range.end);
            chunks.push_back(MarkRange{cursor, end});
            cursor = end;
        }
    }

    struct ChunkResult
    {
        MarkRange range;
        size_t rows_in_chunk = 0;
        /// The chunk's offsets column with positions in `[0, rows_in_chunk)`. After all
        /// chunks complete, these get concatenated per original `MarkRange` (with each
        /// chunk's positions shifted by the cumulative row count of preceding chunks in
        /// the same range) and stored as a single entry in the share. Storing per-chunk
        /// would cause `slice()` to miss when a scan call straddles a chunk boundary,
        /// and the disk fallback would read with stale state.
        ColumnPtr offsets;
        std::vector<char> has_only_defaults;
        std::vector<char> has_only_non_defaults;
        bool ok = false;
    };

    const auto storage_settings_ptr = storage.getSettings();
    auto mark_cache_keepalive = storage.getContext()->getMarkCache();
    auto * mark_cache_raw = mark_cache_keepalive.get();

    auto process_chunk = [part, cols, storage_snapshot, storage_settings_ptr, mark_cache_keepalive, mark_cache_raw, part_info, log, column_name, is_cancelled](const MarkRange & chunk) -> ChunkResult
    {
        ChunkResult r;
        r.range = chunk;
        r.has_only_defaults.assign(chunk.end - chunk.begin, 0);
        r.has_only_non_defaults.assign(chunk.end - chunk.begin, 0);

        if (is_cancelled && is_cancelled->load(std::memory_order_acquire))
            return r;

        auto chunk_reader = createMergeTreeReader(
            part_info,
            cols,
            storage_snapshot,
            storage_settings_ptr,
            MarkRanges{chunk},
            /*virtual_fields=*/{},
            /*uncompressed_cache=*/nullptr,
            mark_cache_raw,
            /*deserialization_prefixes_cache=*/nullptr,
            MergeTreeReaderSettings::createFromSettings(),
            /*avg_value_size_hints=*/{},
            /*profile_callback=*/{});

        /// Analyzer only inspects the offsets column for non-default counts per granule;
        /// reading the values stream is pure overhead. The reader is discarded right
        /// after one `readRows`, so the un-advanced nested-values state is harmless.
        chunk_reader->setOnlyReadSparseOffsets(true);

        const size_t rows_in_chunk = part->index_granularity->getRowsCountInRange(chunk);
        r.rows_in_chunk = rows_in_chunk;

        Columns result(1);
        try
        {
            const size_t rows_read = chunk_reader->readRows(
                chunk.begin, chunk.end, /*continue_reading=*/false, rows_in_chunk, /*rows_offset=*/0, result);
            if (rows_read != rows_in_chunk)
            {
                LOG_DEBUG(log, "Short read on range [{}, {}) of part {} ({} rows instead of {}); skipping sparsity classification for this part",
                    chunk.begin, chunk.end, part->name, rows_read, rows_in_chunk);
                return r;
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, fmt::format(
                "Failed to read sparse offsets for column {} of part {}; skipping sparsity classification for this part",
                column_name, part->name));
            return r;
        }

        const auto * sparse = result[0] ? typeid_cast<const ColumnSparse *>(result[0].get()) : nullptr;
        if (!sparse)
        {
            LOG_DEBUG(log, "Sparse-encoded column {} read as dense for part {}; skipping sparsity classification",
                column_name, part->name);
            return r;
        }

        const auto & offsets_column = assert_cast<const ColumnUInt64 &>(sparse->getOffsetsColumn());
        const auto & offsets_data = offsets_column.getData();
        r.offsets = sparse->getOffsetsPtr();

        size_t offset_idx = 0;
        size_t cursor_row = 0;
        for (size_t mark = chunk.begin; mark < chunk.end; ++mark)
        {
            const size_t rows_in_granule = part->index_granularity->getMarkRows(mark);
            const size_t end_row = cursor_row + rows_in_granule;
            size_t non_defaults = 0;
            while (offset_idx < offsets_data.size() && offsets_data[offset_idx] < end_row)
            {
                ++non_defaults;
                ++offset_idx;
            }
            if (non_defaults == 0)
                r.has_only_defaults[mark - chunk.begin] = 1;
            else if (non_defaults == rows_in_granule)
                r.has_only_non_defaults[mark - chunk.begin] = 1;
            cursor_row = end_row;
        }

        r.ok = true;
        return r;
    };

    std::vector<ChunkResult> results(chunks.size());

    if (chunks.size() == 1)
    {
        results[0] = process_chunk(chunks[0]);
    }
    else
    {
        ThreadPoolCallbackRunnerLocal<void> runner(getIOThreadPool().get(), ThreadName::MERGETREE_READ);
        for (size_t i = 0; i < chunks.size(); ++i)
        {
            runner.enqueueAndKeepTrack(
                [i, chunk = chunks[i], process_chunk, &results] { results[i] = process_chunk(chunk); });
        }
        runner.waitForAllToFinishAndRethrowFirstError();
    }

    for (const auto & r : results)
    {
        if (!r.ok)
            return std::nullopt;
    }

    /// Store each chunk as its own entry in the share. Scan `readRows` calls are
    /// `max_block_size` rows wide (typically a small multiple of one mark) while
    /// analyzer chunks span hundreds of marks, so a scan window almost always lands
    /// fully within one chunk and the slicer's fast path returns it without further
    /// work. The slicer handles the rare straddle by stitching adjacent chunks into a
    /// fresh column on its own; the disk-fallback path is not safe here because the
    /// scan never seeked the disk stream (all previous calls were cache hits) and
    /// `DeserializeStateSparse` would be stale.
    if (offsets_share)
    {
        for (size_t j = 0; j < chunks.size(); ++j)
        {
            if (!results[j].offsets)
                continue;
            offsets_share->insert(
                cache_part_name,
                column_name,
                chunks[j],
                part->index_granularity->getMarkStartingRow(chunks[j].begin),
                results[j].rows_in_chunk,
                results[j].offsets);
        }
    }

    SparseGranuleAnalysis analysis;
    analysis.granule_has_only_defaults.assign(total_marks, false);
    analysis.granule_has_only_non_defaults.assign(total_marks, false);
    for (const auto & r : results)
    {
        for (size_t i = 0; i < r.has_only_defaults.size(); ++i)
        {
            if (r.has_only_defaults[i])
                analysis.granule_has_only_defaults[r.range.begin + i] = true;
            if (r.has_only_non_defaults[i])
                analysis.granule_has_only_non_defaults[r.range.begin + i] = true;
        }
    }

    /// Persist for the next query: the verdict depends only on the column's data,
    /// so any predicate of the matching class on the same column will hit this.
    if (cache && table_uuid != UUIDHelpers::Nil)
    {
        writeBitmapToCache(*cache, table_uuid, cache_part_name,
            syntheticConditionHash(CACHE_DOMAIN_DEFAULTS, column_name),
            CACHE_DOMAIN_DEFAULTS,
            analysis.granule_has_only_defaults, cache_marks_count, has_final_mark);
        writeBitmapToCache(*cache, table_uuid, cache_part_name,
            syntheticConditionHash(CACHE_DOMAIN_NON_DEFAULTS, column_name),
            CACHE_DOMAIN_NON_DEFAULTS,
            analysis.granule_has_only_non_defaults, cache_marks_count, has_final_mark);
    }

    return analysis;
}


MergeTreeSparsityReader::MergeTreeSparsityReader(
    std::vector<RecognisedSparsityPredicate> predicates_,
    const MergeTreeData & data_,
    StorageSnapshotPtr storage_snapshot_,
    ContextPtr query_context_,
    LoggerPtr log_)
    : predicates(std::move(predicates_))
    , data(data_)
    , storage_snapshot(std::move(storage_snapshot_))
    , query_context(std::move(query_context_))
    , log(std::move(log_))
{
}

SparsityReadResultPtr MergeTreeSparsityReader::read(const RangesInDataPart & part)
{
    if (is_cancelled.load(std::memory_order_acquire))
        return nullptr;

    auto result = std::make_shared<SparsityReadResult>();
    const size_t total_marks = part.data_part->index_granularity->getMarksCountWithoutFinal();
    result->granules_selected.assign(total_marks, true);

    bool any_predicate_used = false;
    for (const auto & predicate : predicates)
    {
        if (is_cancelled.load(std::memory_order_acquire))
            return nullptr;

        auto analysis = analyzeSparseColumnGranules(
            part.data_part, predicate.column_name, part.ranges, data, storage_snapshot, query_context, offsets_share.get(), log, &is_cancelled);
        if (!analysis)
            continue;
        any_predicate_used = true;

        for (const auto & range : part.ranges)
        {
            for (size_t mark = range.begin; mark < range.end; ++mark)
            {
                const bool drop = (predicate.predicate_class == SparsityPredicateClass::MatchesNonDefault)
                    ? analysis->granule_has_only_defaults[mark]
                    : analysis->granule_has_only_non_defaults[mark];
                if (drop)
                    result->granules_selected[mark] = false;
            }
        }
    }

    if (!any_predicate_used)
        return nullptr;
    return result;
}

}
