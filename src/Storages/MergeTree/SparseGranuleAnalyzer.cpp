#include <Storages/MergeTree/SparseGranuleAnalyzer.h>

#include <Columns/ColumnSparse.h>
#include <Core/Settings.h>
#include <DataTypes/Serializations/ISerialization.h>
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
#include <Common/ThreadPool.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/threadPoolCallbackRunner.h>

#include <vector>


namespace DB
{

namespace Setting
{
    extern const SettingsBool use_query_condition_cache;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Cache keys: the verdict is a function of (part, column), not of the specific
/// predicate, so the two bitmaps can be shared by every predicate of the matching
/// class on the same column. Hashing a synthetic domain string with the column name
/// keeps these keys disjoint from any real `WHERE` predicate hash.
constexpr std::string_view CACHE_DOMAIN_DEFAULTS = "__sparse_offsets_defaults__";
constexpr std::string_view CACHE_DOMAIN_NON_DEFAULTS = "__sparse_offsets_non_defaults__";

/// Floor for the per-chunk mark count: smaller chunks would have their per-chunk
/// reader setup dominate the actual read work.
constexpr size_t MIN_ANALYZER_CHUNK_MARKS = 256;

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

/// Per-chunk independent work item.
struct ChunkResult
{
    MarkRange range{0, 0};
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

/// Everything `processChunk` and `finalizeAnalysis` need; carries the per-(part, column)
/// state between the prepare/process/finalize phases of the analyzer.
struct AnalysisPlan
{
    DataPartPtr part;
    String column_name;
    MarkRanges ranges;
    NamesAndTypesList cols;
    StorageSnapshotPtr storage_snapshot;
    MergeTreeSettingsPtr storage_settings;
    /// `MergeTreeReaderSettings` has a private default constructor; initialize through
    /// its factory so `AnalysisPlan` remains default-constructible.
    MergeTreeReaderSettings reader_settings = MergeTreeReaderSettings::createFromSettings();
    std::shared_ptr<MarkCache> mark_cache_keepalive;
    MarkCache * mark_cache_raw = nullptr;
    DataPartInfoForReaderPtr part_info;
    LoggerPtr log;
    const std::atomic<bool> * is_cancelled = nullptr;

    /// Set by `buildChunks` between prepare and process.
    std::vector<MarkRange> chunks;

    size_t total_marks = 0;
    size_t cache_marks_count = 0;
    bool has_final_mark = false;
    UUID table_uuid = UUIDHelpers::Nil;
    String cache_part_name;
    QueryConditionCachePtr cache;
    SparseOffsetsShare * offsets_share = nullptr;
};

struct PreparedAnalysis
{
    bool sparse = false;
    std::optional<SparseGranuleAnalysis> cached;
    std::optional<AnalysisPlan> plan;
};

PreparedAnalysis preparePlan(
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
    PreparedAnalysis prep;

    if (is_cancelled && is_cancelled->load(std::memory_order_acquire))
        return prep;

    /// Without a sparse offsets stream there is no cheap classification to make.
    const auto & infos = part->getSerializationInfos();
    auto it = infos.find(column_name);
    if (it == infos.end()
        || !ISerialization::hasKind(it->second->getKindStack(), ISerialization::Kind::SPARSE))
        return prep;

    prep.sparse = true;

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
            prep.cached = std::move(analysis);
            return prep;
        }
    }

    const auto & metadata_snapshot = storage_snapshot->metadata;
    auto column_in_storage = metadata_snapshot->getColumns().tryGetColumn(GetColumnsOptions::AllPhysical, column_name);
    if (!column_in_storage)
    {
        /// Can't read the column; treat as not analyzable.
        prep.sparse = false;
        return prep;
    }

    AnalysisPlan plan;
    plan.part = part;
    plan.column_name = column_name;
    plan.ranges = ranges;
    plan.cols.push_back(*column_in_storage);
    plan.storage_snapshot = storage_snapshot;
    plan.storage_settings = storage.getSettings();
    /// Respect the query's read settings (filesystem cache, throttlers, remote/local
    /// read method, prefetch knobs). The offsets read here is part of the user's
    /// query, so it must go through the same read pipeline as the regular scan.
    plan.reader_settings = MergeTreeReaderSettings::createFromSettings(query_context->getReadSettings());
    plan.mark_cache_keepalive = storage.getContext()->getMarkCache();
    plan.mark_cache_raw = plan.mark_cache_keepalive.get();
    plan.part_info = std::make_shared<LoadedMergeTreeDataPartInfoForReader>(part, std::make_shared<AlterConversions>());
    plan.log = log;
    plan.is_cancelled = is_cancelled;

    plan.total_marks = total_marks;
    plan.cache_marks_count = cache_marks_count;
    plan.has_final_mark = has_final_mark;
    plan.table_uuid = table_uuid;
    plan.cache_part_name = cache_part_name;
    plan.cache = std::move(cache);
    plan.offsets_share = offsets_share;

    prep.plan = std::move(plan);
    return prep;
}

/// Split `ranges` into roughly `target_chunks` pieces, each at least
/// `MIN_ANALYZER_CHUNK_MARKS` marks (a single piece for tiny inputs).
std::vector<MarkRange> buildChunks(const MarkRanges & ranges, size_t target_chunks)
{
    size_t total_marks_in_ranges = 0;
    for (const auto & range : ranges)
        if (range.begin < range.end)
            total_marks_in_ranges += range.end - range.begin;

    if (total_marks_in_ranges == 0)
        return {};

    const size_t target = std::max<size_t>(1, target_chunks);
    const size_t chunk_marks = std::max<size_t>(MIN_ANALYZER_CHUNK_MARKS, (total_marks_in_ranges + target - 1) / target);

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
    return chunks;
}

ChunkResult processChunk(const AnalysisPlan & plan, const MarkRange & chunk)
{
    ChunkResult r;
    r.range = chunk;
    r.has_only_defaults.assign(chunk.end - chunk.begin, 0);
    r.has_only_non_defaults.assign(chunk.end - chunk.begin, 0);

    if (plan.is_cancelled && plan.is_cancelled->load(std::memory_order_acquire))
        return r;

    auto chunk_reader = createMergeTreeReader(
        plan.part_info,
        plan.cols,
        plan.storage_snapshot,
        plan.storage_settings,
        MarkRanges{chunk},
        /*virtual_fields=*/{},
        /*uncompressed_cache=*/nullptr,
        plan.mark_cache_raw,
        /*deserialization_prefixes_cache=*/nullptr,
        plan.reader_settings,
        /*avg_value_size_hints=*/{},
        /*profile_callback=*/{});

    /// Analyzer only inspects the offsets column for non-default counts per granule;
    /// reading the values stream is pure overhead. The reader is discarded right
    /// after one `readRows`, so the un-advanced nested-values state is harmless.
    chunk_reader->setOnlyReadSparseOffsets(true);

    const size_t rows_in_chunk = plan.part->index_granularity->getRowsCountInRange(chunk);
    r.rows_in_chunk = rows_in_chunk;

    Columns result(1);
    const size_t rows_read = chunk_reader->readRows(
        chunk.begin, chunk.end, /*continue_reading=*/false, rows_in_chunk, /*rows_offset=*/0, result);
    if (rows_read != rows_in_chunk)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Short read on range [{}, {}) of part {} ({} rows instead of {}) while analyzing column {}",
            chunk.begin, chunk.end, plan.part->name, rows_read, rows_in_chunk, plan.column_name);

    const auto * sparse = result[0] ? typeid_cast<const ColumnSparse *>(result[0].get()) : nullptr;
    if (!sparse)
    {
        LOG_DEBUG(plan.log, "Sparse-encoded column {} read as dense for part {}; skipping sparsity classification",
            plan.column_name, plan.part->name);
        return r;
    }

    const auto & offsets_column = assert_cast<const ColumnUInt64 &>(sparse->getOffsetsColumn());
    const auto & offsets_data = offsets_column.getData();
    r.offsets = sparse->getOffsetsPtr();

    size_t offset_idx = 0;
    size_t cursor_row = 0;
    for (size_t mark = chunk.begin; mark < chunk.end; ++mark)
    {
        const size_t rows_in_granule = plan.part->index_granularity->getMarkRows(mark);
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
}

std::optional<SparseGranuleAnalysis>
finalizeAnalysis(const AnalysisPlan & plan, const std::vector<ChunkResult> & chunk_results)
{
    for (const auto & r : chunk_results)
        if (!r.ok)
            return std::nullopt;

    /// Store each chunk as its own entry in the share. Scan `readRows` calls are
    /// `max_block_size` rows wide (typically a small multiple of one mark) while
    /// analyzer chunks span hundreds of marks, so a scan window almost always lands
    /// fully within one chunk and the slicer's fast path returns it without further
    /// work. The slicer handles the rare straddle by stitching adjacent chunks into a
    /// fresh column on its own; the disk-fallback path is not safe here because the
    /// scan never seeked the disk stream (all previous calls were cache hits) and
    /// `DeserializeStateSparse` would be stale.
    /// Only wide parts consume the share today. Compact parts pack all columns and
    /// substreams contiguously in one file and the top-level column read path in
    /// `MergeTreeReaderCompactSingleBuffer` passes a null SubstreamsCache, so a
    /// cached offsets hit would not advance the underlying buffer and the next
    /// substream read would land at the wrong file offset. Seeding for compact parts
    /// would also need a cache that survives across `readRows` calls (the wide
    /// reader carries one as a member; the compact reader recreates a local cache
    /// each iteration). Until those gaps are addressed, skip storing for compact.
    if (plan.offsets_share && plan.part->getType() == MergeTreeDataPartType::Wide)
    {
        for (size_t j = 0; j < plan.chunks.size(); ++j)
        {
            if (!chunk_results[j].offsets)
                continue;
            plan.offsets_share->insert(
                plan.cache_part_name,
                plan.column_name,
                plan.chunks[j],
                plan.part->index_granularity->getMarkStartingRow(plan.chunks[j].begin),
                chunk_results[j].rows_in_chunk,
                chunk_results[j].offsets);
        }
    }

    SparseGranuleAnalysis analysis;
    analysis.granule_has_only_defaults.assign(plan.total_marks, false);
    analysis.granule_has_only_non_defaults.assign(plan.total_marks, false);
    for (const auto & r : chunk_results)
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
    if (plan.cache && plan.table_uuid != UUIDHelpers::Nil)
    {
        writeBitmapToCache(*plan.cache, plan.table_uuid, plan.cache_part_name,
            syntheticConditionHash(CACHE_DOMAIN_DEFAULTS, plan.column_name),
            CACHE_DOMAIN_DEFAULTS,
            analysis.granule_has_only_defaults, plan.cache_marks_count, plan.has_final_mark);
        writeBitmapToCache(*plan.cache, plan.table_uuid, plan.cache_part_name,
            syntheticConditionHash(CACHE_DOMAIN_NON_DEFAULTS, plan.column_name),
            CACHE_DOMAIN_NON_DEFAULTS,
            analysis.granule_has_only_non_defaults, plan.cache_marks_count, plan.has_final_mark);
    }

    return analysis;
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
    auto prep = preparePlan(part, column_name, ranges, storage, storage_snapshot,
                            query_context, offsets_share, log, is_cancelled);
    if (!prep.sparse)
        return std::nullopt;
    if (prep.cached)
        return std::move(prep.cached);

    auto & plan = *prep.plan;
    plan.chunks = buildChunks(plan.ranges, /*target_chunks=*/ 1);

    std::vector<ChunkResult> chunk_results(plan.chunks.size());
    for (size_t i = 0; i < plan.chunks.size(); ++i)
        chunk_results[i] = processChunk(plan, plan.chunks[i]);

    return finalizeAnalysis(plan, chunk_results);
}

std::vector<std::vector<std::optional<SparseGranuleAnalysis>>>
analyzeSparseColumnGranulesBatched(
    const RangesInDataParts & parts,
    const std::vector<String> & columns,
    const MergeTreeData & storage,
    const StorageSnapshotPtr & storage_snapshot,
    const ContextPtr & query_context,
    SparseOffsetsShare * offsets_share,
    LoggerPtr log,
    ThreadPool & pool)
{
    std::vector<std::vector<std::optional<SparseGranuleAnalysis>>> results(parts.size());
    for (auto & per_part : results)
        per_part.assign(columns.size(), std::nullopt);

    if (parts.empty() || columns.empty())
        return results;

    /// Phase 1: prepare each (part, column). Cache hits fill the result slot
    /// immediately; not-sparse leaves `nullopt`; pending ones become active units.
    struct ActiveUnit
    {
        size_t part_idx = 0;
        size_t col_idx = 0;
        AnalysisPlan plan;
        std::vector<ChunkResult> chunk_results;
    };
    std::vector<ActiveUnit> units;
    units.reserve(parts.size() * columns.size());

    for (size_t p = 0; p < parts.size(); ++p)
    {
        for (size_t c = 0; c < columns.size(); ++c)
        {
            auto prep = preparePlan(
                parts[p].data_part, columns[c], parts[p].ranges,
                storage, storage_snapshot, query_context,
                offsets_share, log, /*is_cancelled=*/ nullptr);
            if (!prep.sparse)
                continue;
            if (prep.cached)
            {
                results[p][c] = std::move(prep.cached);
                continue;
            }
            ActiveUnit unit;
            unit.part_idx = p;
            unit.col_idx = c;
            unit.plan = std::move(*prep.plan);
            units.push_back(std::move(unit));
        }
    }

    if (units.empty())
        return results;

    /// Phase 2: split each active unit into enough chunks to fill the pool. With
    /// many units one chunk per unit is enough; with few units we give each unit
    /// extra chunks so all pool workers stay busy.
    const size_t pool_size = std::max<size_t>(1, pool.getMaxThreads());
    const size_t target_chunks_per_unit
        = std::max<size_t>(1, (pool_size + units.size() - 1) / units.size());

    for (auto & unit : units)
    {
        unit.plan.chunks = buildChunks(unit.plan.ranges, target_chunks_per_unit);
        unit.chunk_results.assign(unit.plan.chunks.size(), ChunkResult{});
    }

    /// Phase 3: dispatch every `(unit, chunk)` as a leaf task. No nested fanout,
    /// no shared mutable state (each task writes its own slot), no waits inside
    /// tasks: pool workers can never end up blocked on each other.
    {
        ThreadPoolCallbackRunnerLocal<void> runner(pool, ThreadName::MERGETREE_SPARSE_GRANULE_ANALYSIS);
        for (auto & unit : units)
        {
            const size_t n_chunks = unit.plan.chunks.size();
            for (size_t k = 0; k < n_chunks; ++k)
            {
                runner.enqueueAndKeepTrack(
                    [k, &unit]
                    {
                        unit.chunk_results[k] = processChunk(unit.plan, unit.plan.chunks[k]);
                    });
            }
        }
        runner.waitForAllToFinishAndRethrowFirstError();
    }

    /// Phase 4: aggregate per unit. Sequential, single-threaded.
    for (auto & unit : units)
        results[unit.part_idx][unit.col_idx] = finalizeAnalysis(unit.plan, unit.chunk_results);

    return results;
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

    /// Analysis is a function of `(part, column)` only; predicates differ only in
    /// `predicate_class`. Cache so unsatisfiable conjuncts like `x = 0 AND x != 0`
    /// (or `x IS NULL AND x IS NOT NULL`) do not double-read offsets and seed the
    /// `SparseOffsetsShare` twice for the same column.
    std::unordered_map<String, std::optional<SparseGranuleAnalysis>> analyses;
    bool any_predicate_used = false;
    for (const auto & predicate : predicates)
    {
        if (is_cancelled.load(std::memory_order_acquire))
            return nullptr;

        auto [it, inserted] = analyses.try_emplace(predicate.column_name, std::nullopt);
        if (inserted)
            it->second = analyzeSparseColumnGranules(
                part.data_part, predicate.column_name, part.ranges, data, storage_snapshot, query_context,
                offsets_share.get(), log, &is_cancelled);
        const auto & analysis = it->second;
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
