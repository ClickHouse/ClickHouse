#include <Storages/MergeTree/SparseGranuleAnalyzer.h>

#include <Columns/ColumnSparse.h>
#include <DataTypes/Serializations/ISerialization.h>
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
#include <Common/logger_useful.h>


namespace DB
{

std::optional<SparseGranuleAnalysis>
analyzeSparseColumnGranules(
    const DataPartPtr & part,
    const String & column_name,
    const MarkRanges & ranges,
    const MergeTreeData & storage,
    const StorageSnapshotPtr & storage_snapshot,
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

    /// Read the column granule-by-granule. Looking at the resulting `ColumnSparse`'s
    /// offsets after each step tells us how many non-default values landed in that
    /// granule. We deliberately read one granule per iteration (rather than the whole
    /// part in one call) so that we can stop as soon as we have an answer per mark
    /// without materialising the entire column in memory.
    const size_t total_marks = part->index_granularity->getMarksCountWithoutFinal();
    SparseGranuleAnalysis analysis;
    analysis.granule_has_only_defaults.assign(total_marks, false);
    analysis.granule_has_only_non_defaults.assign(total_marks, false);

    Columns result(1);

    for (const auto & range : ranges)
    {
        /// `continue_reading=false` on the first mark of each range triggers a seek;
        /// after that we read consecutive marks within the range. Resetting between
        /// ranges matters because PK/skip-index/cache trimming can leave us with
        /// non-adjacent ranges (e.g. [0,1) and [2,3)).
        bool continue_reading = false;

        for (size_t mark = range.begin; mark < range.end; ++mark)
        {
            const size_t rows_in_granule = part->index_granularity->getMarkRows(mark);
            if (rows_in_granule == 0)
                continue;

            /// Capture the offsets count before this read so we can diff afterwards.
            size_t offsets_before = 0;
            if (result[0])
            {
                if (const auto * sparse = typeid_cast<const ColumnSparse *>(result[0].get()))
                    offsets_before = sparse->getOffsetsColumn().size();
            }

            try
            {
                const size_t rows_read = reader->readRows(
                    mark, range.end, continue_reading, rows_in_granule, /*rows_offset=*/0, result);
                continue_reading = true;
                if (rows_read != rows_in_granule)
                {
                    LOG_DEBUG(log, "Short read at mark {} of part {} ({} rows instead of {}); giving up Phase B for this part",
                        mark, part->name, rows_read, rows_in_granule);
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
                /// as dense during the read). Without sparse offsets we can't classify;
                /// skip Phase B for this part.
                LOG_DEBUG(log, "Sparse-encoded column {} read as dense for part {}; skipping Phase B",
                    column_name, part->name);
                return std::nullopt;
            }

            const size_t offsets_after = sparse->getOffsetsColumn().size();
            const size_t non_defaults_in_granule = offsets_after - offsets_before;

            if (non_defaults_in_granule == 0)
                analysis.granule_has_only_defaults[mark] = true;
            else if (non_defaults_in_granule == rows_in_granule)
                analysis.granule_has_only_non_defaults[mark] = true;
        }
    }

    return analysis;
}


MergeTreeSparsityReader::MergeTreeSparsityReader(
    std::vector<RecognisedSparsityPredicate> predicates_,
    const MergeTreeData & data_,
    StorageSnapshotPtr storage_snapshot_,
    LoggerPtr log_)
    : predicates(std::move(predicates_))
    , data(data_)
    , storage_snapshot(std::move(storage_snapshot_))
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
            part.data_part, predicate.column_name, part.ranges, data, storage_snapshot, log);
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
