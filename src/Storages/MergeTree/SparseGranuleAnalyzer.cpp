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
#include <Common/logger_useful.h>


namespace DB
{

namespace Setting
{
    extern const SettingsBool use_query_condition_cache;
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
    LoggerPtr log)
{
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

    /// Setting is per-query, so it must come from `query_context`, not from the storage's
    /// startup context.
    auto cache = query_context->getSettingsRef()[Setting::use_query_condition_cache]
        ? query_context->getQueryConditionCache()
        : nullptr;
    if (cache && table_uuid != UUIDHelpers::Nil)
    {
        const UInt64 hash_defaults = syntheticConditionHash(CACHE_DOMAIN_DEFAULTS, column_name);
        const UInt64 hash_non_defaults = syntheticConditionHash(CACHE_DOMAIN_NON_DEFAULTS, column_name);
        auto cached_defaults = cache->read(table_uuid, part->name, hash_defaults);
        auto cached_non_defaults = cache->read(table_uuid, part->name, hash_non_defaults);
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

    auto reader = createMergeTreeReader(
        part_info,
        cols,
        storage_snapshot,
        storage.getSettings(),
        ranges,
        /*virtual_fields=*/{},
        /*uncompressed_cache=*/nullptr,
        storage.getContext()->getMarkCache().get(),
        /*deserialization_prefixes_cache=*/nullptr,
        MergeTreeReaderSettings::createFromSettings(),
        /*avg_value_size_hints=*/{},
        /*profile_callback=*/{});

    /// Read each `MarkRange` in one `readRows` call. Each call has fixed per-invocation
    /// overhead (mark seek, decompression block setup); doing it granule-by-granule was
    /// ~100x slower on 100M-row parts. After reading, we bucket the offsets array into
    /// per-granule non-default counts with a single linear sweep.
    SparseGranuleAnalysis analysis;
    analysis.granule_has_only_defaults.assign(total_marks, false);
    analysis.granule_has_only_non_defaults.assign(total_marks, false);

    for (const auto & range : ranges)
    {
        if (range.begin >= range.end)
            continue;

        const size_t rows_in_range = part->index_granularity->getRowsCountInRange(range);

        Columns result(1);
        try
        {
            const size_t rows_read = reader->readRows(
                range.begin, range.end, /*continue_reading=*/false, rows_in_range, /*rows_offset=*/0, result);
            if (rows_read != rows_in_range)
            {
                LOG_DEBUG(log, "Short read on range [{}, {}) of part {} ({} rows instead of {}); skipping sparsity classification for this part",
                    range.begin, range.end, part->name, rows_read, rows_in_range);
                return std::nullopt;
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, fmt::format(
                "Failed to read sparse offsets for column {} of part {}; skipping sparsity classification for this part",
                column_name, part->name));
            return std::nullopt;
        }

        const auto * sparse = result[0] ? typeid_cast<const ColumnSparse *>(result[0].get()) : nullptr;
        if (!sparse)
        {
            /// The reader returned a dense column (e.g. the column was materialised as dense
            /// during this read), so the offsets stream needed for cheap classification is
            /// not available.
            LOG_DEBUG(log, "Sparse-encoded column {} read as dense for part {}; skipping sparsity classification",
                column_name, part->name);
            return std::nullopt;
        }

        /// Two-pointer walk: offsets are sorted absolute positions within the read
        /// sequence; advance the offset cursor over each granule's row range.
        const auto & offsets_column = assert_cast<const ColumnUInt64 &>(sparse->getOffsetsColumn());
        const auto & offsets_data = offsets_column.getData();

        if (offsets_share)
        {
            /// Persist the decompressed offsets so the data-scan reader can reuse them
            /// rather than re-reading the substream from disk.
            offsets_share->insert(
                part->name,
                column_name,
                range,
                /*start_row_in_part=*/part->index_granularity->getMarkStartingRow(range.begin),
                /*total_rows=*/rows_in_range,
                sparse->getOffsetsPtr());
        }

        size_t offset_idx = 0;
        size_t cursor_row = 0;

        for (size_t mark = range.begin; mark < range.end; ++mark)
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
                analysis.granule_has_only_defaults[mark] = true;
            else if (non_defaults == rows_in_granule)
                analysis.granule_has_only_non_defaults[mark] = true;

            cursor_row = end_row;
        }
    }

    /// Persist for the next query: the verdict depends only on the column's data,
    /// so any predicate of the matching class on the same column will hit this.
    if (cache && table_uuid != UUIDHelpers::Nil)
    {
        writeBitmapToCache(*cache, table_uuid, part->name,
            syntheticConditionHash(CACHE_DOMAIN_DEFAULTS, column_name),
            CACHE_DOMAIN_DEFAULTS,
            analysis.granule_has_only_defaults, cache_marks_count, has_final_mark);
        writeBitmapToCache(*cache, table_uuid, part->name,
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
    auto result = std::make_shared<SparsityReadResult>();
    const size_t total_marks = part.data_part->index_granularity->getMarksCountWithoutFinal();
    result->granules_selected.assign(total_marks, true);

    bool any_predicate_used = false;
    for (const auto & predicate : predicates)
    {
        auto analysis = analyzeSparseColumnGranules(
            part.data_part, predicate.column_name, part.ranges, data, storage_snapshot, query_context, offsets_share.get(), log);
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
