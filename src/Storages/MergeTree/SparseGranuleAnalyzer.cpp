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

/// Synthetic condition hashes that key Phase B's per-(part, column) verdict in the
/// `QueryConditionCache`. The bitmap stored for `Defaults` says "granule G has only
/// default values for this column" -- usable for any `MatchesNonDefault` predicate on
/// the column. The `NonDefaults` bitmap is the symmetric statement and feeds
/// `MatchesDefault` predicates.
constexpr std::string_view CACHE_DOMAIN_DEFAULTS = "__phaseB_offsets_defaults__";
constexpr std::string_view CACHE_DOMAIN_NON_DEFAULTS = "__phaseB_offsets_non_defaults__";

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
    /// Translate `granule_bitmap[g] == true` (this granule has no matches for the
    /// corresponding predicate class) into the dense `MarkRanges` shape that
    /// `QueryConditionCache::write` expects.
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

    /// Always call `write` -- even when `no_match_ranges` is empty -- so the cache
    /// records "this (part, column, class) was analyzed; no prunable granules" and
    /// the next query short-circuits to the cache instead of re-running the analyzer.
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
    LoggerPtr log)
{
    /// Phase B only applies when this column is sparse-encoded on this part. Otherwise the
    /// data has no offsets stream and we can't compute per-granule defaults cheaply (and
    /// Phase A or a full scan handle the dense case).
    const auto & infos = part->getSerializationInfos();
    auto it = infos.find(column_name);
    if (it == infos.end()
        || !ISerialization::hasKind(it->second->getKindStack(), ISerialization::Kind::SPARSE))
        return std::nullopt;

    const size_t total_marks = part->index_granularity->getMarksCountWithoutFinal();
    /// `QueryConditionCache` follows the convention that `marks_count` *includes* the
    /// final-mark sentinel, and `has_final_mark` then flips `matching_marks[marks_count-1]`
    /// to `false`. Phase B never analyses the sentinel granule, so we pass the full
    /// `getMarksCount()` here and only read/write positions in `[0, total_marks)`.
    const size_t cache_marks_count = part->index_granularity->getMarksCount();
    const bool has_final_mark = part->index_granularity->hasFinalMark();
    const UUID table_uuid = storage.getStorageID().uuid;

    /// Phase B's verdict for a (part, column) is independent of the specific predicate
    /// being asked about -- a granule is "all-default" or "all-non-default" purely as
    /// a function of the column's data. Cache both bitmaps under synthetic keys so
    /// repeated queries (with any classifiable predicate on the same column) hit it
    /// without re-running the analyzer or its per-part reader setup. Gated on the
    /// same `use_query_condition_cache` setting as the regular cache path; the setting
    /// is per-query so it must come from `query_context`, not the storage's context.
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
            for (size_t g = 0; g < total_marks; ++g)
            {
                /// In `QueryConditionCache`, `false` means "no matches for this predicate".
                /// For our `Defaults` domain that translates to "granule G is all-default".
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

    /// Read each `MarkRange` in a single `readRows` call rather than granule-by-granule.
    /// Each `readRows` has fixed dispatch + setup cost (mark seek, decompression block
    /// setup, etc.); with adaptive granularity that's ~12K granules per 100M-row part,
    /// and a per-granule read pattern is ~100x slower than reading the whole range at
    /// once. After the read, we bucket the `ColumnSparse` offsets array into per-granule
    /// counts with a single linear sweep.
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
                LOG_DEBUG(log, "Short read on range [{}, {}) of part {} ({} rows instead of {}); skipping Phase B for this part",
                    range.begin, range.end, part->name, rows_read, rows_in_range);
                return std::nullopt;
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, fmt::format(
                "Failed to read sparse offsets for column {} of part {}; skipping Phase B for this part",
                column_name, part->name));
            return std::nullopt;
        }

        const auto * sparse = result[0] ? typeid_cast<const ColumnSparse *>(result[0].get()) : nullptr;
        if (!sparse)
        {
            /// Reader returned a dense column (e.g. because the column was materialised
            /// as dense during the read). Without sparse offsets we can't classify.
            LOG_DEBUG(log, "Sparse-encoded column {} read as dense for part {}; skipping Phase B",
                column_name, part->name);
            return std::nullopt;
        }

        /// Bucket the offsets array (sorted, absolute positions within this read sequence)
        /// into per-granule non-default counts via a two-pointer walk.
        const auto & offsets_column = assert_cast<const ColumnUInt64 &>(sparse->getOffsetsColumn());
        const auto & offsets_data = offsets_column.getData();

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

    /// Cache the verdict so the next query on this part doesn't re-do the analyzer's
    /// per-part reader setup. Phase B's verdict is column-keyed, not predicate-keyed,
    /// so any classifiable predicate on the same column shares this entry.
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
            part.data_part, predicate.column_name, part.ranges, data, storage_snapshot, query_context, log);
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
