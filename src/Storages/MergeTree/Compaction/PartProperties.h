#pragma once

#include <Storages/MergeTree/MergeTreeDataPartType.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>

#include <Disks/IStoragePolicy.h>

#include <Core/UUID.h>

#include <map>
#include <optional>
#include <set>
#include <span>
#include <ctime>

namespace DB
{

class IMergeTreeDataPart;
using MergeTreeDataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

struct PartProperties
{
    const std::string name;
    const MergeTreePartInfo info;
    const std::set<std::string> projection_names = {};

    /// The projections among `projection_names` whose projection part lacks a column the current projection
    /// metadata expects (after an `ALTER` of an `ALIAS` column it selects). A merge rebuilds such a projection
    /// from the parent part instead of merging the projection parts, see
    /// `MergeTask::ExecuteAndFinalizeHorizontalPart::prepareProjectionsToMergeAndRebuild`.
    const std::set<std::string> projections_missing_columns = {};

    /// The projections of the current metadata whose projection part the part has on disk but not loaded, typically
    /// a projection dropped and added again while the part was detached. A merge in which every source part has a
    /// projection either loaded or unloaded, and at least one has it unloaded, rebuilds the projection from the
    /// parent part, see `MergeTask::ExecuteAndFinalizeHorizontalPart::prepareProjectionsToMergeAndRebuild`.
    const std::set<std::string> unloaded_projection_names = {};

    /// The size of every projection part among `projection_names`. A merge that keeps a projection every source
    /// part has merges the projection parts in a child merge of its own, which runs after the parent merge and
    /// chooses its algorithm by these sizes, see `MergeTask::MergeProjectionsStage::prepareProjections`.
    /// The child merge predicts the format of its result from the levels and the types of the projection parts,
    /// not of the parent parts, see `FutureMergedMutatedPart::assign`: a merged projection part has the level
    /// of `MergeListElement::FAKE_RESULT_PART_FOR_PROJECTION`, which is zero.
    struct ProjectionPartSize
    {
        const size_t rows = 0;
        const size_t size_uncompressed = 0;
        const UInt32 level = 0;
        const MergeTreeDataPartType part_type = {};
    };
    const std::map<std::string, ProjectionPartSize> projection_sizes = {};

    /// The physical columns of the table that the part does not have and that have no default expression. A
    /// column absent from every source part of a merge is expired by the merge: it is not gathered, which
    /// may turn a vertical merge into a horizontal one, and a merge that expires a column of a projection
    /// rebuilds the projection from the parent part, see `MergeTask::ExecuteAndFinalizeHorizontalPart::prepare`
    /// and `MergeTask::ExecuteAndFinalizeHorizontalPart::prepareProjectionsToMergeAndRebuild`.
    const std::set<std::string> absent_columns = {};

    const bool all_ttl_calculated_if_any = false;
    const bool is_in_volume_where_merges_avoid = false;

    /// Size of data part in bytes.
    const size_t size = 0;

    /// Uncompressed size of the columns of the part in bytes, as used by `MergeTask` and
    /// `MergeTreeData::choosePartFormat` to choose the merge algorithm and the format of the result.
    const size_t size_uncompressed = 0;

    /// The format of the part. A merge never produces a part of a more compact type than any of its source parts:
    /// `FutureMergedMutatedPart::assign` takes the minimum of the type chosen by the thresholds and the types of the
    /// source parts, so merging existing `Wide` parts keeps the result `Wide`. Unknown when not filled.
    const MergeTreeDataPartType part_type = {};

    /// How old this data part in seconds.
    const time_t age = 0;

    /// Number of rows in part.
    const size_t rows = 0;

    /// Whether the part carries a lightweight delete mask. A merge that removes expired values cannot run
    /// vertically over such parts, see `MergeTask::canVerticalTTLDelete`.
    const bool has_lightweight_delete = false;

    /// Information about different TTLs for part. Used by Part/Row Delete Merge Selectors.
    struct GeneralTTLInfo
    {
        const bool has_any_non_finished_ttls;
        const bool has_any_non_finished_row_ttls;
        const bool has_any_non_finished_column_ttls;
        const time_t part_min_ttl;
        const time_t part_max_ttl;
        /// The earliest due time among the unfinished column TTLs, as opposed to `part_min_ttl`,
        /// which is the minimum across the row TTLs as well.
        const time_t column_min_ttl;
        /// The earliest due time among the `GROUP BY` TTLs, finished or not. A merge treats the finished ones as
        /// unfinished again (see `MergeTreeDataPartTTLInfos::update`), so they are not in `part_min_ttl` of the
        /// part but are in that of the merged part.
        const time_t group_by_min_ttl;
        /// The table has an unconditional rows TTL, and it has expired for every row of the part.
        const bool rows_ttl_expired;
    };
    const std::optional<GeneralTTLInfo> general_ttl_info = std::nullopt;

    /// Information about recompression TTL for part. Used by Recompress Merge Selector.
    struct RecompressTTLInfo
    {
        const bool will_change_codec;
        const time_t next_recompress_ttl;
    };
    const std::optional<RecompressTTLInfo> recompression_ttl_info = std::nullopt;
};

using PartsRange = std::vector<PartProperties>;
using PartsRanges = std::vector<PartsRange>;
using PartsRangeView = std::span<const PartProperties>;

PartProperties buildPartProperties(
    const MergeTreeDataPartPtr & part,
    const StorageMetadataPtr & metadata_snapshot,
    const StoragePolicyPtr & storage_policy,
    time_t current_time);

}
