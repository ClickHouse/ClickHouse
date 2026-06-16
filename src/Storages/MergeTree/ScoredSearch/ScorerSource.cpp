#include <roaring/roaring.hh>
#include <Storages/MergeTree/ScoredSearch/ScorerSource.h>

#include <algorithm>
#include <Access/ContextAccess.h>
#include <Columns/ColumnsNumber.h>
#include <Common/ProfileEvents.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/ScoredSearch/IScorer.h>

namespace ProfileEvents
{
    extern const Event ScoredSearchPerPartRowsReturned;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

ScorerSource::ScorerSource(
    SharedHeader output_header_,
    std::shared_ptr<RowScorer> scorer_,
    ScorerSharedPartsPtr shared_parts_,
    MergeTreeData::MutationsSnapshotPtr mutations_snapshot_,
    StorageMetadataPtr source_metadata_,
    ContextPtr context_)
    : ISource(output_header_)
    , output_header(std::move(output_header_))
    , scorer(std::move(scorer_))
    , shared_parts(std::move(shared_parts_))
    , mutations_snapshot(std::move(mutations_snapshot_))
    , source_metadata(std::move(source_metadata_))
    , context(std::move(context_))
{
    if (!scorer)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ScorerSource requires a non-null RowScorer");

    if (!shared_parts)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ScorerSource requires a non-null ScorerSharedParts");
}

void ScorerSource::checkCanUseIndexes(const RangesInDataPart & part_ranges) const
{
    auto scorer_indexes = scorer->getIndexes();
    if (scorer_indexes.empty())
        return;

    auto alter_conversions = MergeTreeData::getAlterConversionsForPart(part_ranges.data_part, mutations_snapshot, context
#if CLICKHOUSE_CLOUD
        , context->getAccess()->getEnabledMaskingPolicies()
#endif
    );

    const auto & updated_columns = alter_conversions->getAllUpdatedColumns();

    for (const auto & index : scorer_indexes)
    {
        if (auto can_use = MergeTreeDataSelectExecutor::canUseIndex(index, source_metadata, updated_columns); !can_use)
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Cannot run a scored search on part {}: {}. Wait for the pending mutations of the source table to finish",
                part_ranges.data_part->name, can_use.error().text);
        }
    }
}

void ScorerSource::mergePartResult(RowScorer::ScoreResult part_result, UInt64 part_starting_offset)
{
    /// Part-local row ids become global row indexes.
    /// The addition of a constant keeps the within-part order,
    /// so `part_result` stays sorted by the merge comparator.
    for (auto & [row_id, score] : part_result)
        row_id += part_starting_offset;

    const ScoreDirection direction = scorer->getSortDirection();

    auto less = [direction](const auto & lhs, const auto & rhs)
    {
        if (lhs.second == rhs.second)
            return lhs.first < rhs.first;

        return direction == ScoreDirection::Ascending ? lhs.second < rhs.second : lhs.second > rhs.second;
    };

    RowScorer::ScoreResult merged;
    merged.reserve(top_rows.size() + part_result.size());
    std::merge(top_rows.begin(), top_rows.end(), part_result.begin(), part_result.end(), std::back_inserter(merged), less);

    if (merged.size() > scorer->getTopK())
        merged.resize(scorer->getTopK());

    top_rows = std::move(merged);
}

Chunk ScorerSource::generate()
{
    if (generated)
        return {};

    /// Claim parts until one actually needs scoring; score at most one part
    /// per call, so the executor checks cancellation between parts.
    while (true)
    {
        const size_t part_idx = shared_parts->next_part_index.fetch_add(1, std::memory_order_relaxed);
        if (part_idx >= shared_parts->parts.size())
            break;

        const auto & part_ranges = shared_parts->parts[part_idx];
        std::shared_ptr<roaring::Roaring> prefilter;

        if (shared_parts->bitmaps)
            prefilter = (*shared_parts->bitmaps)[part_ranges.part_index_in_query];

        /// The prefilter exists but no row of this part survived the WHERE clause.
        if (prefilter && prefilter->isEmpty())
            continue;

        checkCanUseIndexes(part_ranges);

        auto score_result = scorer->scorePart(part_ranges.data_part, prefilter.get(), context);
        ProfileEvents::increment(ProfileEvents::ScoredSearchPerPartRowsReturned, score_result.size());

        mergePartResult(std::move(score_result), part_ranges.part_starting_offset_in_query);
        return Chunk(output_header->cloneEmptyColumns(), 0);
    }

    generated = true;

    const size_t num_rows = top_rows.size();
    auto score_col = ColumnFloat32::create();
    auto row_index_col = ColumnUInt64::create();
    auto & score_data = score_col->getData();
    auto & row_index_data = row_index_col->getData();
    score_data.reserve(num_rows);
    row_index_data.reserve(num_rows);

    for (const auto & [global_row_index, score] : top_rows)
    {
        score_data.push_back(score);
        row_index_data.push_back(global_row_index);
    }

    MutableColumns output_columns(output_header->columns());
    output_columns[output_header->getPositionByName("_score")] = std::move(score_col);
    output_columns[output_header->getPositionByName("__global_row_index")] = std::move(row_index_col);

    return Chunk(std::move(output_columns), num_rows);
}

}
