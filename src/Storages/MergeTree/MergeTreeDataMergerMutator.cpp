#include "MergeTreeDataMergerMutator.h"

#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/MergedColumnOnlyOutputStream.h>
#include <Storages/MergeTree/SimpleMergeSelector.h>
#include <Storages/MergeTree/AllMergeSelector.h>
#include <Storages/MergeTree/TTLMergeSelector.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/StorageFromMergeTreeDataPart.h>
#include <DataStreams/TTLBlockInputStream.h>
#include <DataStreams/DistinctSortedBlockInputStream.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <DataStreams/ColumnGathererStream.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Merges/CollapsingSortedTransform.h>
#include <Processors/Merges/SummingSortedTransform.h>
#include <Processors/Merges/ReplacingSortedTransform.h>
#include <Processors/Merges/GraphiteRollupSortedTransform.h>
#include <Processors/Merges/AggregatingSortedTransform.h>
#include <Processors/Merges/VersionedCollapsingTransform.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/Executors/PipelineExecutingBlockInputStream.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/Context.h>
#include <Common/interpolate.h>
#include <Common/typeid_cast.h>
#include <Common/escapeForFileName.h>
#include <Parsers/queryToString.h>

#include <cmath>
#include <ctime>
#include <numeric>

#include <boost/algorithm/string/replace.hpp>


namespace ProfileEvents
{
    extern const Event MergedRows;
    extern const Event MergedUncompressedBytes;
    extern const Event MergesTimeMilliseconds;
    extern const Event Merge;
}

namespace CurrentMetrics
{
    extern const Metric BackgroundPoolTask;
    extern const Metric PartMutation;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int DIRECTORY_ALREADY_EXISTS;
    extern const int LOGICAL_ERROR;
    extern const int ABORTED;
}

/// Do not start to merge parts, if free space is less than sum size of parts times specified coefficient.
/// This value is chosen to not allow big merges to eat all free space. Thus allowing small merges to proceed.
static const double DISK_USAGE_COEFFICIENT_TO_SELECT = 2;

/// To do merge, reserve amount of space equals to sum size of parts times specified coefficient.
/// Must be strictly less than DISK_USAGE_COEFFICIENT_TO_SELECT,
///  because between selecting parts to merge and doing merge, amount of free space could have decreased.
static const double DISK_USAGE_COEFFICIENT_TO_RESERVE = 1.1;

void FutureMergedMutatedPart::assign(MergeTreeData::DataPartsVector parts_)
{
    if (parts_.empty())
        return;

    size_t sum_rows = 0;
    size_t sum_bytes_uncompressed = 0;
    MergeTreeDataPartType future_part_type = MergeTreeDataPartType::UNKNOWN;
    for (const auto & part : parts_)
    {
        sum_rows += part->rows_count;
        sum_bytes_uncompressed += part->getTotalColumnsSize().data_uncompressed;
        future_part_type = std::min(future_part_type, part->getType());
    }

    auto chosen_type = parts_.front()->storage.choosePartTypeOnDisk(sum_bytes_uncompressed, sum_rows);
    future_part_type = std::min(future_part_type, chosen_type);
    assign(std::move(parts_), future_part_type);
}

void FutureMergedMutatedPart::assign(MergeTreeData::DataPartsVector parts_, MergeTreeDataPartType future_part_type)
{
    if (parts_.empty())
        return;

    for (const MergeTreeData::DataPartPtr & part : parts_)
    {
        const MergeTreeData::DataPartPtr & first_part = parts_.front();

        if (part->partition.value != first_part->partition.value)
            throw Exception(
                "Attempting to merge parts " + first_part->name + " and " + part->name + " that are in different partitions",
                ErrorCodes::LOGICAL_ERROR);
    }

    parts = std::move(parts_);

    UInt32 max_level = 0;
    Int64 max_mutation = 0;
    for (const auto & part : parts)
    {
        max_level = std::max(max_level, part->info.level);
        max_mutation = std::max(max_mutation, part->info.mutation);
    }

    type = future_part_type;
    part_info.partition_id = parts.front()->info.partition_id;
    part_info.min_block = parts.front()->info.min_block;
    part_info.max_block = parts.back()->info.max_block;
    part_info.level = max_level + 1;
    part_info.mutation = max_mutation;

    if (parts.front()->storage.format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        DayNum min_date = DayNum(std::numeric_limits<UInt16>::max());
        DayNum max_date = DayNum(std::numeric_limits<UInt16>::min());
        for (const auto & part : parts)
        {
            /// NOTE: getting min and max dates from part names (instead of part data) because we want
            /// the merged part name be determined only by source part names.
            /// It is simpler this way when the real min and max dates for the block range can change
            /// (e.g. after an ALTER DELETE command).
            DayNum part_min_date;
            DayNum part_max_date;
            MergeTreePartInfo::parseMinMaxDatesFromPartName(part->name, part_min_date, part_max_date);
            min_date = std::min(min_date, part_min_date);
            max_date = std::max(max_date, part_max_date);
        }

        name = part_info.getPartNameV0(min_date, max_date);
    }
    else
        name = part_info.getPartName();
}

void FutureMergedMutatedPart::updatePath(const MergeTreeData & storage, const ReservationPtr & reservation)
{
    path = storage.getFullPathOnDisk(reservation->getDisk()) + name + "/";
}

MergeTreeDataMergerMutator::MergeTreeDataMergerMutator(MergeTreeData & data_, size_t background_pool_size_)
    : data(data_), background_pool_size(background_pool_size_), log(&Poco::Logger::get(data.getLogName() + " (MergerMutator)"))
{
}


UInt64 MergeTreeDataMergerMutator::getMaxSourcePartsSizeForMerge() const
{
    size_t busy_threads_in_pool = CurrentMetrics::values[CurrentMetrics::BackgroundPoolTask].load(std::memory_order_relaxed);

    return getMaxSourcePartsSizeForMerge(background_pool_size, busy_threads_in_pool);
}


UInt64 MergeTreeDataMergerMutator::getMaxSourcePartsSizeForMerge(size_t pool_size, size_t pool_used) const
{
    if (pool_used > pool_size)
        throw Exception("Logical error: invalid arguments passed to getMaxSourcePartsSize: pool_used > pool_size", ErrorCodes::LOGICAL_ERROR);

    size_t free_entries = pool_size - pool_used;
    const auto data_settings = data.getSettings();

    /// Always allow maximum size if one or less pool entries is busy.
    /// One entry is probably the entry where this function is executed.
    /// This will protect from bad settings.
    UInt64 max_size = 0;
    if (pool_used <= 1 || free_entries >= data_settings->number_of_free_entries_in_pool_to_lower_max_size_of_merge)
        max_size = data_settings->max_bytes_to_merge_at_max_space_in_pool;
    else
        max_size = interpolateExponential(
            data_settings->max_bytes_to_merge_at_min_space_in_pool,
            data_settings->max_bytes_to_merge_at_max_space_in_pool,
            static_cast<double>(free_entries) / data_settings->number_of_free_entries_in_pool_to_lower_max_size_of_merge);

    return std::min(max_size, static_cast<UInt64>(data.getStoragePolicy()->getMaxUnreservedFreeSpace() / DISK_USAGE_COEFFICIENT_TO_SELECT));
}


UInt64 MergeTreeDataMergerMutator::getMaxSourcePartSizeForMutation() const
{
    const auto data_settings = data.getSettings();
    size_t busy_threads_in_pool = CurrentMetrics::values[CurrentMetrics::BackgroundPoolTask].load(std::memory_order_relaxed);

    /// DataPart can be store only at one disk. Get maximum reservable free space at all disks.
    UInt64 disk_space = data.getStoragePolicy()->getMaxUnreservedFreeSpace();

    /// Allow mutations only if there are enough threads, leave free threads for merges else
    if (busy_threads_in_pool <= 1
        || background_pool_size - busy_threads_in_pool >= data_settings->number_of_free_entries_in_pool_to_execute_mutation)
        return static_cast<UInt64>(disk_space / DISK_USAGE_COEFFICIENT_TO_RESERVE);

    return 0;
}

SelectPartsDecision MergeTreeDataMergerMutator::selectPartsToMerge(
    FutureMergedMutatedPart & future_part,
    bool aggressive,
    size_t max_total_size_to_merge,
    const AllowedMergingPredicate & can_merge_callback,
    bool merge_with_ttl_allowed,
    String * out_disable_reason)
{
    MergeTreeData::DataPartsVector data_parts = data.getDataPartsVector();
    const auto data_settings = data.getSettings();
    auto metadata_snapshot = data.getInMemoryMetadataPtr();

    if (data_parts.empty())
    {
        if (out_disable_reason)
            *out_disable_reason = "There are no parts in the table";
        return SelectPartsDecision::CANNOT_SELECT;
    }

    time_t current_time = std::time(nullptr);

    IMergeSelector::PartsRanges parts_ranges;

    StoragePolicyPtr storage_policy = data.getStoragePolicy();
    /// Volumes with stopped merges are extremely rare situation.
    /// Check it once and don't check each part (this is bad for performance).
    bool has_volumes_with_disabled_merges = storage_policy->hasAnyVolumeWithDisabledMerges();

    const String * prev_partition_id = nullptr;
    /// Previous part only in boundaries of partition frame
    const MergeTreeData::DataPartPtr * prev_part = nullptr;

    size_t parts_selected_precondition = 0;
    for (const MergeTreeData::DataPartPtr & part : data_parts)
    {
        const String & partition_id = part->info.partition_id;

        if (!prev_partition_id || partition_id != *prev_partition_id)
        {
            if (parts_ranges.empty() || !parts_ranges.back().empty())
                parts_ranges.emplace_back();

            /// New partition frame.
            prev_partition_id = &partition_id;
            prev_part = nullptr;
        }

        /// Check predicate only for the first part in each range.
        if (!prev_part)
        {
            /* Parts can be merged with themselves for TTL needs for example.
            * So we have to check if this part is currently being inserted with quorum and so on and so forth.
            * Obviously we have to check it manually only for the first part
            * of each partition because it will be automatically checked for a pair of parts. */
            if (!can_merge_callback(nullptr, part, nullptr))
                continue;

        }
        else
        {
            /// If we cannot merge with previous part we had to start new parts
            /// interval (in the same partition)
            if (!can_merge_callback(*prev_part, part, nullptr))
            {
                /// Starting new interval in the same partition
                assert(!parts_ranges.back().empty());
                parts_ranges.emplace_back();

                /// Now we have no previous part, but it affects only logging
                prev_part = nullptr;
            }
        }

        IMergeSelector::Part part_info;
        part_info.size = part->getBytesOnDisk();
        part_info.age = current_time - part->modification_time;
        part_info.level = part->info.level;
        part_info.data = &part;
        part_info.ttl_infos = &part->ttl_infos;
        part_info.compression_codec_desc = part->default_codec->getFullCodecDesc();
        part_info.shall_participate_in_merges = has_volumes_with_disabled_merges ? part->shallParticipateInMerges(storage_policy) : true;

        ++parts_selected_precondition;

        parts_ranges.back().emplace_back(part_info);

        /// Check for consistency of data parts. If assertion is failed, it requires immediate investigation.
        if (prev_part && part->info.partition_id == (*prev_part)->info.partition_id
            && part->info.min_block <= (*prev_part)->info.max_block)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} intersects previous part {}", part->name, (*prev_part)->name);
        }

        prev_part = &part;
    }

    if (parts_selected_precondition == 0)
    {
        if (out_disable_reason)
            *out_disable_reason = "No parts satisfy preconditions for merge";
        return SelectPartsDecision::CANNOT_SELECT;
    }

    IMergeSelector::PartsRange parts_to_merge;

    if (metadata_snapshot->hasAnyTTL() && merge_with_ttl_allowed && !ttl_merges_blocker.isCancelled())
    {
        /// TTL delete is preferred to recompression
        TTLDeleteMergeSelector delete_ttl_selector(
                next_delete_ttl_merge_times_by_partition,
                current_time,
                data_settings->merge_with_ttl_timeout,
                data_settings->ttl_only_drop_parts);

        parts_to_merge = delete_ttl_selector.select(parts_ranges, max_total_size_to_merge);
        if (!parts_to_merge.empty())
        {
            future_part.merge_type = MergeType::TTL_DELETE;
        }
        else if (metadata_snapshot->hasAnyRecompressionTTL())
        {
            TTLRecompressMergeSelector recompress_ttl_selector(
                    next_recompress_ttl_merge_times_by_partition,
                    current_time,
                    data_settings->merge_with_recompression_ttl_timeout,
                    metadata_snapshot->getRecompressionTTLs());

            parts_to_merge = recompress_ttl_selector.select(parts_ranges, max_total_size_to_merge);
            if (!parts_to_merge.empty())
                future_part.merge_type = MergeType::TTL_RECOMPRESS;
        }
    }

    if (parts_to_merge.empty())
    {
        SimpleMergeSelector::Settings merge_settings;
        if (aggressive)
            merge_settings.base = 1;

        parts_to_merge = SimpleMergeSelector(merge_settings)
                            .select(parts_ranges, max_total_size_to_merge);

        /// Do not allow to "merge" part with itself for regular merges, unless it is a TTL-merge where it is ok to remove some values with expired ttl
        if (parts_to_merge.size() == 1)
            throw Exception("Logical error: merge selector returned only one part to merge", ErrorCodes::LOGICAL_ERROR);

        if (parts_to_merge.empty())
        {
            if (out_disable_reason)
                *out_disable_reason = "There is no need to merge parts according to merge selector algorithm";
            return SelectPartsDecision::CANNOT_SELECT;
        }
    }

    MergeTreeData::DataPartsVector parts;
    parts.reserve(parts_to_merge.size());
    for (IMergeSelector::Part & part_info : parts_to_merge)
    {
        const MergeTreeData::DataPartPtr & part = *static_cast<const MergeTreeData::DataPartPtr *>(part_info.data);
        parts.push_back(part);
    }

    LOG_DEBUG(log, "Selected {} parts from {} to {}", parts.size(), parts.front()->name, parts.back()->name);
    future_part.assign(std::move(parts));
    return SelectPartsDecision::SELECTED;
}

SelectPartsDecision MergeTreeDataMergerMutator::selectAllPartsToMergeWithinPartition(
    FutureMergedMutatedPart & future_part,
    UInt64 & available_disk_space,
    const AllowedMergingPredicate & can_merge,
    const String & partition_id,
    bool final,
    const StorageMetadataPtr & metadata_snapshot,
    String * out_disable_reason,
    bool optimize_skip_merged_partitions)
{
    MergeTreeData::DataPartsVector parts = selectAllPartsFromPartition(partition_id);

    if (parts.empty())
        return SelectPartsDecision::CANNOT_SELECT;

    if (!final && parts.size() == 1)
    {
        if (out_disable_reason)
            *out_disable_reason = "There is only one part inside partition";
        return SelectPartsDecision::CANNOT_SELECT;
    }

    /// If final, optimize_skip_merged_partitions is true and we have only one part in partition with level > 0
    /// than we don't select it to merge. But if there are some expired TTL then merge is needed
    if (final && optimize_skip_merged_partitions && parts.size() == 1 && parts[0]->info.level > 0 &&
        (!metadata_snapshot->hasAnyTTL() || parts[0]->checkAllTTLCalculated(metadata_snapshot)))
    {
        return SelectPartsDecision::NOTHING_TO_MERGE;
    }

    auto it = parts.begin();
    auto prev_it = it;

    UInt64 sum_bytes = 0;
    while (it != parts.end())
    {
        /// For the case of one part, we check that it can be merged "with itself".
        if ((it != parts.begin() || parts.size() == 1) && !can_merge(*prev_it, *it, out_disable_reason))
        {
            return SelectPartsDecision::CANNOT_SELECT;
        }

        sum_bytes += (*it)->getBytesOnDisk();

        prev_it = it;
        ++it;
    }

    /// Enough disk space to cover the new merge with a margin.
    auto required_disk_space = sum_bytes * DISK_USAGE_COEFFICIENT_TO_SELECT;
    if (available_disk_space <= required_disk_space)
    {
        time_t now = time(nullptr);
        if (now - disk_space_warning_time > 3600)
        {
            disk_space_warning_time = now;
            LOG_WARNING(log,
                "Won't merge parts from {} to {} because not enough free space: {} free and unreserved"
                ", {} required now (+{}% on overhead); suppressing similar warnings for the next hour",
                parts.front()->name,
                (*prev_it)->name,
                ReadableSize(available_disk_space),
                ReadableSize(sum_bytes),
                static_cast<int>((DISK_USAGE_COEFFICIENT_TO_SELECT - 1.0) * 100));
        }

        if (out_disable_reason)
            *out_disable_reason = fmt::format("Insufficient available disk space, required {}", ReadableSize(required_disk_space));

        return SelectPartsDecision::CANNOT_SELECT;
    }

    LOG_DEBUG(log, "Selected {} parts from {} to {}", parts.size(), parts.front()->name, parts.back()->name);
    future_part.assign(std::move(parts));

    available_disk_space -= required_disk_space;
    return SelectPartsDecision::SELECTED;
}


MergeTreeData::DataPartsVector MergeTreeDataMergerMutator::selectAllPartsFromPartition(const String & partition_id)
{
    MergeTreeData::DataPartsVector parts_from_partition;

    MergeTreeData::DataParts data_parts = data.getDataParts();

    for (const auto & current_part : data_parts)
    {
        if (current_part->info.partition_id != partition_id)
            continue;

        parts_from_partition.push_back(current_part);
    }

    return parts_from_partition;
}


/// PK columns are sorted and merged, ordinary columns are gathered using info from merge step
static void extractMergingAndGatheringColumns(
    const NamesAndTypesList & storage_columns,
    const ExpressionActionsPtr & sorting_key_expr,
    const IndicesDescription & indexes,
    const ProjectionsDescription & projections,
    const MergeTreeData::MergingParams & merging_params,
    NamesAndTypesList & gathering_columns, Names & gathering_column_names,
    NamesAndTypesList & merging_columns, Names & merging_column_names)
{
    Names sort_key_columns_vec = sorting_key_expr->getRequiredColumns();
    std::set<String> key_columns(sort_key_columns_vec.cbegin(), sort_key_columns_vec.cend());
    for (const auto & index : indexes)
    {
        Names index_columns_vec = index.expression->getRequiredColumns();
        std::copy(index_columns_vec.cbegin(), index_columns_vec.cend(),
                  std::inserter(key_columns, key_columns.end()));
    }

    for (const auto & projection : projections)
    {
        Names projection_columns_vec = projection.required_columns;
        std::copy(projection_columns_vec.cbegin(), projection_columns_vec.cend(),
                  std::inserter(key_columns, key_columns.end()));
    }

    /// Force sign column for Collapsing mode
    if (merging_params.mode == MergeTreeData::MergingParams::Collapsing)
        key_columns.emplace(merging_params.sign_column);

    /// Force version column for Replacing mode
    if (merging_params.mode == MergeTreeData::MergingParams::Replacing)
        key_columns.emplace(merging_params.version_column);

    /// Force sign column for VersionedCollapsing mode. Version is already in primary key.
    if (merging_params.mode == MergeTreeData::MergingParams::VersionedCollapsing)
        key_columns.emplace(merging_params.sign_column);

    /// Force to merge at least one column in case of empty key
    if (key_columns.empty())
        key_columns.emplace(storage_columns.front().name);

    /// TODO: also force "summing" and "aggregating" columns to make Horizontal merge only for such columns

    for (const auto & column : storage_columns)
    {
        if (key_columns.count(column.name))
        {
            merging_columns.emplace_back(column);
            merging_column_names.emplace_back(column.name);
        }
        else
        {
            gathering_columns.emplace_back(column);
            gathering_column_names.emplace_back(column.name);
        }
    }
}

/* Allow to compute more accurate progress statistics */
class ColumnSizeEstimator
{
    MergeTreeData::DataPart::ColumnToSize map;
public:

    /// Stores approximate size of columns in bytes
    /// Exact values are not required since it used for relative values estimation (progress).
    size_t sum_total = 0;
    size_t sum_index_columns = 0;
    size_t sum_ordinary_columns = 0;

    ColumnSizeEstimator(const MergeTreeData::DataPart::ColumnToSize & map_, const Names & key_columns, const Names & ordinary_columns)
        : map(map_)
    {
        for (const auto & name : key_columns)
            if (!map.count(name)) map[name] = 0;
        for (const auto & name : ordinary_columns)
            if (!map.count(name)) map[name] = 0;

        for (const auto & name : key_columns)
            sum_index_columns += map.at(name);

        for (const auto & name : ordinary_columns)
            sum_ordinary_columns += map.at(name);

        sum_total = std::max(static_cast<decltype(sum_index_columns)>(1), sum_index_columns + sum_ordinary_columns);
    }

    Float64 columnWeight(const String & column) const
    {
        return static_cast<Float64>(map.at(column)) / sum_total;
    }

    Float64 keyColumnsWeight() const
    {
        return static_cast<Float64>(sum_index_columns) / sum_total;
    }
};

/** Progress callback.
  * What it should update:
  * - approximate progress
  * - amount of read rows
  * - various metrics
  * - time elapsed for current merge.
  */

/// Auxiliary struct that for each merge stage stores its current progress.
/// A stage is: the horizontal stage + a stage for each gathered column (if we are doing a
/// Vertical merge) or a mutation of a single part. During a single stage all rows are read.
struct MergeStageProgress
{
    explicit MergeStageProgress(Float64 weight_)
        : is_first(true) , weight(weight_)
    {
    }

    MergeStageProgress(Float64 initial_progress_, Float64 weight_)
        : initial_progress(initial_progress_), is_first(false), weight(weight_)
    {
    }

    Float64 initial_progress = 0.0;
    bool is_first;
    Float64 weight;

    UInt64 total_rows = 0;
    UInt64 rows_read = 0;
};

class MergeProgressCallback
{
public:
    MergeProgressCallback(
        MergeList::Entry & merge_entry_, UInt64 & watch_prev_elapsed_, MergeStageProgress & stage_)
        : merge_entry(merge_entry_)
        , watch_prev_elapsed(watch_prev_elapsed_)
        , stage(stage_)
    {
        updateWatch();
    }

    MergeList::Entry & merge_entry;
    UInt64 & watch_prev_elapsed;
    MergeStageProgress & stage;

    void updateWatch()
    {
        UInt64 watch_curr_elapsed = merge_entry->watch.elapsed();
        ProfileEvents::increment(ProfileEvents::MergesTimeMilliseconds, (watch_curr_elapsed - watch_prev_elapsed) / 1000000);
        watch_prev_elapsed = watch_curr_elapsed;
    }

    void operator() (const Progress & value)
    {
        ProfileEvents::increment(ProfileEvents::MergedUncompressedBytes, value.read_bytes);
        if (stage.is_first)
        {
            ProfileEvents::increment(ProfileEvents::MergedRows, value.read_rows);
            ProfileEvents::increment(ProfileEvents::Merge);
        }
        updateWatch();

        merge_entry->bytes_read_uncompressed += value.read_bytes;
        if (stage.is_first)
            merge_entry->rows_read += value.read_rows;

        stage.total_rows += value.total_rows_to_read;
        stage.rows_read += value.read_rows;
        if (stage.total_rows > 0)
        {
            merge_entry->progress.store(
                stage.initial_progress + stage.weight * stage.rows_read / stage.total_rows,
                std::memory_order_relaxed);
        }
    }
};

static bool needSyncPart(size_t input_rows, size_t input_bytes, const MergeTreeSettings & settings)
{
    return ((settings.min_rows_to_fsync_after_merge && input_rows >= settings.min_rows_to_fsync_after_merge)
        || (settings.min_compressed_bytes_to_fsync_after_merge && input_bytes >= settings.min_compressed_bytes_to_fsync_after_merge));
}


/// parts should be sorted.
MergeTreeData::MutableDataPartPtr MergeTreeDataMergerMutator::mergePartsToTemporaryPart(
    const FutureMergedMutatedPart & future_part,
    const StorageMetadataPtr & metadata_snapshot,
    MergeList::Entry & merge_entry,
    TableLockHolder & holder,
    time_t time_of_merge,
    ContextPtr context,
    const ReservationPtr & space_reservation,
    bool deduplicate,
    const Names & deduplicate_by_columns,
    const MergeTreeData::MergingParams & merging_params,
    const IMergeTreeDataPart * parent_part,
    const String & prefix)
{
    const String tmp_prefix = parent_part ? prefix : "tmp_merge_";

    if (merges_blocker.isCancelled())
        throw Exception("Cancelled merging parts", ErrorCodes::ABORTED);

    /// We don't want to perform merge assigned with TTL as normal merge, so
    /// throw exception
    if (isTTLMergeType(future_part.merge_type) && ttl_merges_blocker.isCancelled())
        throw Exception("Cancelled merging parts with TTL", ErrorCodes::ABORTED);

    const MergeTreeData::DataPartsVector & parts = future_part.parts;

    LOG_DEBUG(log, "Merging {} parts: from {} to {} into {}", parts.size(), parts.front()->name, parts.back()->name, future_part.type.toString());
    if (deduplicate)
    {
        if (deduplicate_by_columns.empty())
            LOG_DEBUG(log, "DEDUPLICATE BY all columns");
        else
            LOG_DEBUG(log, "DEDUPLICATE BY ('{}')", fmt::join(deduplicate_by_columns, "', '"));
    }

    auto disk = space_reservation->getDisk();
    String part_path = data.relative_data_path;
    String new_part_tmp_path = part_path + tmp_prefix + future_part.name + (parent_part ? ".proj" : "") + "/";
    if (disk->exists(new_part_tmp_path))
        throw Exception("Directory " + fullPath(disk, new_part_tmp_path) + " already exists", ErrorCodes::DIRECTORY_ALREADY_EXISTS);


    Names all_column_names = metadata_snapshot->getColumns().getNamesOfPhysical();
    NamesAndTypesList storage_columns = metadata_snapshot->getColumns().getAllPhysical();
    const auto data_settings = data.getSettings();

    NamesAndTypesList gathering_columns;
    NamesAndTypesList merging_columns;
    Names gathering_column_names, merging_column_names;
    extractMergingAndGatheringColumns(
        storage_columns,
        metadata_snapshot->getSortingKey().expression,
        metadata_snapshot->getSecondaryIndices(),
        metadata_snapshot->getProjections(),
        merging_params,
        gathering_columns,
        gathering_column_names,
        merging_columns,
        merging_column_names);

    auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + future_part.name, disk, 0);
    MergeTreeData::MutableDataPartPtr new_data_part = data.createPart(
        future_part.name,
        future_part.type,
        future_part.part_info,
        single_disk_volume,
        tmp_prefix + future_part.name + (parent_part ? ".proj" : ""),
        parent_part);

    new_data_part->uuid = future_part.uuid;
    new_data_part->setColumns(storage_columns);
    new_data_part->partition.assign(future_part.getPartition());
    new_data_part->is_temp = parent_part == nullptr;

    bool need_remove_expired_values = false;
    bool force_ttl = false;
    for (const auto & part : parts)
    {
        new_data_part->ttl_infos.update(part->ttl_infos);
        if (metadata_snapshot->hasAnyTTL() && !part->checkAllTTLCalculated(metadata_snapshot))
        {
            LOG_INFO(log, "Some TTL values were not calculated for part {}. Will calculate them forcefully during merge.", part->name);
            need_remove_expired_values = true;
            force_ttl = true;
        }
    }

    const auto & part_min_ttl = new_data_part->ttl_infos.part_min_ttl;
    if (part_min_ttl && part_min_ttl <= time_of_merge)
        need_remove_expired_values = true;

    if (need_remove_expired_values && ttl_merges_blocker.isCancelled())
    {
        LOG_INFO(log, "Part {} has values with expired TTL, but merges with TTL are cancelled.", new_data_part->name);
        need_remove_expired_values = false;
    }

    size_t sum_input_rows_upper_bound = merge_entry->total_rows_count;
    size_t sum_compressed_bytes_upper_bound = merge_entry->total_size_bytes_compressed;
    MergeAlgorithm chosen_merge_algorithm = chooseMergeAlgorithm(
        parts, sum_input_rows_upper_bound, gathering_columns, deduplicate, need_remove_expired_values, merging_params);
    merge_entry->merge_algorithm.store(chosen_merge_algorithm, std::memory_order_relaxed);

    LOG_DEBUG(log, "Selected MergeAlgorithm: {}", toString(chosen_merge_algorithm));

    /// Note: this is done before creating input streams, because otherwise data.data_parts_mutex
    /// (which is locked in data.getTotalActiveSizeInBytes())
    /// (which is locked in shared mode when input streams are created) and when inserting new data
    /// the order is reverse. This annoys TSan even though one lock is locked in shared mode and thus
    /// deadlock is impossible.
    auto compression_codec = data.getCompressionCodecForPart(merge_entry->total_size_bytes_compressed, new_data_part->ttl_infos, time_of_merge);

    auto tmp_disk = context->getTemporaryVolume()->getDisk();
    String rows_sources_file_path;
    std::unique_ptr<WriteBufferFromFileBase> rows_sources_uncompressed_write_buf;
    std::unique_ptr<WriteBuffer> rows_sources_write_buf;
    std::optional<ColumnSizeEstimator> column_sizes;

    SyncGuardPtr sync_guard;

    if (chosen_merge_algorithm == MergeAlgorithm::Vertical)
    {
        tmp_disk->createDirectories(new_part_tmp_path);
        rows_sources_file_path = new_part_tmp_path + "rows_sources";
        rows_sources_uncompressed_write_buf = tmp_disk->writeFile(rows_sources_file_path);
        rows_sources_write_buf = std::make_unique<CompressedWriteBuffer>(*rows_sources_uncompressed_write_buf);

        MergeTreeData::DataPart::ColumnToSize merged_column_to_size;
        for (const MergeTreeData::DataPartPtr & part : parts)
            part->accumulateColumnSizes(merged_column_to_size);

        column_sizes = ColumnSizeEstimator(merged_column_to_size, merging_column_names, gathering_column_names);

        if (data.getSettings()->fsync_part_directory)
            sync_guard = disk->getDirectorySyncGuard(new_part_tmp_path);
    }
    else
    {
        merging_columns = storage_columns;
        merging_column_names = all_column_names;
        gathering_columns.clear();
        gathering_column_names.clear();
    }

    /** Read from all parts, merge and write into a new one.
      * In passing, we calculate expression for sorting.
      */
    Pipes pipes;
    UInt64 watch_prev_elapsed = 0;

    /// We count total amount of bytes in parts
    /// and use direct_io + aio if there is more than min_merge_bytes_to_use_direct_io
    bool read_with_direct_io = false;
    if (data_settings->min_merge_bytes_to_use_direct_io != 0)
    {
        size_t total_size = 0;
        for (const auto & part : parts)
        {
            total_size += part->getBytesOnDisk();
            if (total_size >= data_settings->min_merge_bytes_to_use_direct_io)
            {
                LOG_DEBUG(log, "Will merge parts reading files in O_DIRECT");
                read_with_direct_io = true;

                break;
            }
        }
    }

    MergeStageProgress horizontal_stage_progress(
        column_sizes ? column_sizes->keyColumnsWeight() : 1.0);

    for (const auto & part : parts)
    {
        auto input = std::make_unique<MergeTreeSequentialSource>(
            data, metadata_snapshot, part, merging_column_names, read_with_direct_io, true);

        input->setProgressCallback(
            MergeProgressCallback(merge_entry, watch_prev_elapsed, horizontal_stage_progress));

        Pipe pipe(std::move(input));

        if (metadata_snapshot->hasSortingKey())
        {
            pipe.addSimpleTransform([&metadata_snapshot](const Block & header)
            {
                return std::make_shared<ExpressionTransform>(header, metadata_snapshot->getSortingKey().expression);
            });
        }

        pipes.emplace_back(std::move(pipe));
    }

    Names sort_columns = metadata_snapshot->getSortingKeyColumns();
    SortDescription sort_description;
    size_t sort_columns_size = sort_columns.size();
    sort_description.reserve(sort_columns_size);

    Names partition_key_columns = metadata_snapshot->getPartitionKey().column_names;

    Block header = pipes.at(0).getHeader();
    for (size_t i = 0; i < sort_columns_size; ++i)
        sort_description.emplace_back(header.getPositionByName(sort_columns[i]), 1, 1);

    /// The order of the streams is important: when the key is matched, the elements go in the order of the source stream number.
    /// In the merged part, the lines with the same key must be in the ascending order of the identifier of original part,
    ///  that is going in insertion order.
    ProcessorPtr merged_transform;

    /// If merge is vertical we cannot calculate it
    bool blocks_are_granules_size = (chosen_merge_algorithm == MergeAlgorithm::Vertical);

    UInt64 merge_block_size = data_settings->merge_max_block_size;
    switch (merging_params.mode)
    {
        case MergeTreeData::MergingParams::Ordinary:
            merged_transform = std::make_unique<MergingSortedTransform>(
                header, pipes.size(), sort_description, merge_block_size, 0, rows_sources_write_buf.get(), true, blocks_are_granules_size);
            break;

        case MergeTreeData::MergingParams::Collapsing:
            merged_transform = std::make_unique<CollapsingSortedTransform>(
                header, pipes.size(), sort_description, merging_params.sign_column, false,
                merge_block_size, rows_sources_write_buf.get(), blocks_are_granules_size);
            break;

        case MergeTreeData::MergingParams::Summing:
            merged_transform = std::make_unique<SummingSortedTransform>(
                header, pipes.size(), sort_description, merging_params.columns_to_sum, partition_key_columns, merge_block_size);
            break;

        case MergeTreeData::MergingParams::Aggregating:
            merged_transform = std::make_unique<AggregatingSortedTransform>(
                header, pipes.size(), sort_description, merge_block_size);
            break;

        case MergeTreeData::MergingParams::Replacing:
            merged_transform = std::make_unique<ReplacingSortedTransform>(
                header, pipes.size(), sort_description, merging_params.version_column,
                merge_block_size, rows_sources_write_buf.get(), blocks_are_granules_size);
            break;

        case MergeTreeData::MergingParams::Graphite:
            merged_transform = std::make_unique<GraphiteRollupSortedTransform>(
                header, pipes.size(), sort_description, merge_block_size,
                merging_params.graphite_params, time_of_merge);
            break;

        case MergeTreeData::MergingParams::VersionedCollapsing:
            merged_transform = std::make_unique<VersionedCollapsingTransform>(
                header, pipes.size(), sort_description, merging_params.sign_column,
                merge_block_size, rows_sources_write_buf.get(), blocks_are_granules_size);
            break;
    }

    QueryPipeline pipeline;
    pipeline.init(Pipe::unitePipes(std::move(pipes)));
    pipeline.addTransform(std::move(merged_transform));
    pipeline.setMaxThreads(1);
    BlockInputStreamPtr merged_stream = std::make_shared<PipelineExecutingBlockInputStream>(std::move(pipeline));

    if (deduplicate)
        merged_stream = std::make_shared<DistinctSortedBlockInputStream>(merged_stream, sort_description, SizeLimits(), 0 /*limit_hint*/, deduplicate_by_columns);

    if (need_remove_expired_values)
        merged_stream = std::make_shared<TTLBlockInputStream>(merged_stream, data, metadata_snapshot, new_data_part, time_of_merge, force_ttl);

    if (metadata_snapshot->hasSecondaryIndices())
    {
        const auto & indices = metadata_snapshot->getSecondaryIndices();
        merged_stream = std::make_shared<ExpressionBlockInputStream>(
            merged_stream, indices.getSingleExpressionForIndices(metadata_snapshot->getColumns(), data.getContext()));
        merged_stream = std::make_shared<MaterializingBlockInputStream>(merged_stream);
    }

    const auto & index_factory = MergeTreeIndexFactory::instance();
    MergedBlockOutputStream to{
        new_data_part,
        metadata_snapshot,
        merging_columns,
        index_factory.getMany(metadata_snapshot->getSecondaryIndices()),
        compression_codec,
        blocks_are_granules_size};

    merged_stream->readPrefix();
    to.writePrefix();

    size_t rows_written = 0;
    const size_t initial_reservation = space_reservation ? space_reservation->getSize() : 0;

    auto is_cancelled = [&]() { return merges_blocker.isCancelled()
        || (need_remove_expired_values && ttl_merges_blocker.isCancelled()); };

    Block block;
    while (!is_cancelled() && (block = merged_stream->read()))
    {
        rows_written += block.rows();

        to.write(block);

        merge_entry->rows_written = merged_stream->getProfileInfo().rows;
        merge_entry->bytes_written_uncompressed = merged_stream->getProfileInfo().bytes;

        /// Reservation updates is not performed yet, during the merge it may lead to higher free space requirements
        if (space_reservation && sum_input_rows_upper_bound)
        {
            /// The same progress from merge_entry could be used for both algorithms (it should be more accurate)
            /// But now we are using inaccurate row-based estimation in Horizontal case for backward compatibility
            Float64 progress = (chosen_merge_algorithm == MergeAlgorithm::Horizontal)
                ? std::min(1., 1. * rows_written / sum_input_rows_upper_bound)
                : std::min(1., merge_entry->progress.load(std::memory_order_relaxed));

            space_reservation->update(static_cast<size_t>((1. - progress) * initial_reservation));
        }
    }

    merged_stream->readSuffix();
    merged_stream.reset();

    if (merges_blocker.isCancelled())
        throw Exception("Cancelled merging parts", ErrorCodes::ABORTED);

    if (need_remove_expired_values && ttl_merges_blocker.isCancelled())
        throw Exception("Cancelled merging parts with expired TTL", ErrorCodes::ABORTED);

    bool need_sync = needSyncPart(sum_input_rows_upper_bound, sum_compressed_bytes_upper_bound, *data_settings);
    MergeTreeData::DataPart::Checksums checksums_gathered_columns;

    /// Gather ordinary columns
    if (chosen_merge_algorithm == MergeAlgorithm::Vertical)
    {
        size_t sum_input_rows_exact = merge_entry->rows_read;
        merge_entry->columns_written = merging_column_names.size();
        merge_entry->progress.store(column_sizes->keyColumnsWeight(), std::memory_order_relaxed);

        BlockInputStreams column_part_streams(parts.size());

        auto it_name_and_type = gathering_columns.cbegin();

        rows_sources_write_buf->next();
        rows_sources_uncompressed_write_buf->next();
        /// Ensure data has written to disk.
        rows_sources_uncompressed_write_buf->finalize();

        size_t rows_sources_count = rows_sources_write_buf->count();
        /// In special case, when there is only one source part, and no rows were skipped, we may have
        /// skipped writing rows_sources file. Otherwise rows_sources_count must be equal to the total
        /// number of input rows.
        if ((rows_sources_count > 0 || parts.size() > 1) && sum_input_rows_exact != rows_sources_count)
            throw Exception("Number of rows in source parts (" + toString(sum_input_rows_exact)
                + ") differs from number of bytes written to rows_sources file (" + toString(rows_sources_count)
                + "). It is a bug.", ErrorCodes::LOGICAL_ERROR);

        CompressedReadBufferFromFile rows_sources_read_buf(tmp_disk->readFile(rows_sources_file_path));
        IMergedBlockOutputStream::WrittenOffsetColumns written_offset_columns;

        for (size_t column_num = 0, gathering_column_names_size = gathering_column_names.size();
            column_num < gathering_column_names_size;
            ++column_num, ++it_name_and_type)
        {
            const String & column_name = it_name_and_type->name;
            Names column_names{column_name};
            Float64 progress_before = merge_entry->progress.load(std::memory_order_relaxed);

            MergeStageProgress column_progress(progress_before, column_sizes->columnWeight(column_name));
            for (size_t part_num = 0; part_num < parts.size(); ++part_num)
            {
                auto column_part_source = std::make_shared<MergeTreeSequentialSource>(
                    data, metadata_snapshot, parts[part_num], column_names, read_with_direct_io, true);

                column_part_source->setProgressCallback(
                    MergeProgressCallback(merge_entry, watch_prev_elapsed, column_progress));

                QueryPipeline column_part_pipeline;
                column_part_pipeline.init(Pipe(std::move(column_part_source)));
                column_part_pipeline.setMaxThreads(1);

                column_part_streams[part_num] =
                        std::make_shared<PipelineExecutingBlockInputStream>(std::move(column_part_pipeline));
            }

            rows_sources_read_buf.seek(0, 0);
            ColumnGathererStream column_gathered_stream(column_name, column_part_streams, rows_sources_read_buf);

            MergedColumnOnlyOutputStream column_to(
                new_data_part,
                metadata_snapshot,
                column_gathered_stream.getHeader(),
                compression_codec,
                /// we don't need to recalc indices here
                /// because all of them were already recalculated and written
                /// as key part of vertical merge
                std::vector<MergeTreeIndexPtr>{},
                &written_offset_columns,
                to.getIndexGranularity());

            size_t column_elems_written = 0;

            column_to.writePrefix();
            while (!merges_blocker.isCancelled() && (block = column_gathered_stream.read()))
            {
                column_elems_written += block.rows();
                column_to.write(block);
            }

            if (merges_blocker.isCancelled())
                throw Exception("Cancelled merging parts", ErrorCodes::ABORTED);

            column_gathered_stream.readSuffix();
            auto changed_checksums = column_to.writeSuffixAndGetChecksums(new_data_part, checksums_gathered_columns, need_sync);
            checksums_gathered_columns.add(std::move(changed_checksums));

            if (rows_written != column_elems_written)
            {
                throw Exception("Written " + toString(column_elems_written) + " elements of column " + column_name +
                                ", but " + toString(rows_written) + " rows of PK columns", ErrorCodes::LOGICAL_ERROR);
            }

            /// NOTE: 'progress' is modified by single thread, but it may be concurrently read from MergeListElement::getInfo() (StorageSystemMerges).

            merge_entry->columns_written += 1;
            merge_entry->bytes_written_uncompressed += column_gathered_stream.getProfileInfo().bytes;
            merge_entry->progress.store(progress_before + column_sizes->columnWeight(column_name), std::memory_order_relaxed);
        }

        tmp_disk->removeFile(rows_sources_file_path);
    }

    for (const auto & part : parts)
        new_data_part->minmax_idx.merge(part->minmax_idx);

    /// Print overall profiling info. NOTE: it may duplicates previous messages
    {
        double elapsed_seconds = merge_entry->watch.elapsedSeconds();
        LOG_DEBUG(log,
            "Merge sorted {} rows, containing {} columns ({} merged, {} gathered) in {} sec., {} rows/sec., {}/sec.",
            merge_entry->rows_read,
            all_column_names.size(),
            merging_column_names.size(),
            gathering_column_names.size(),
            elapsed_seconds,
            merge_entry->rows_read / elapsed_seconds,
            ReadableSize(merge_entry->bytes_read_uncompressed / elapsed_seconds));
    }

    for (const auto & projection : metadata_snapshot->getProjections())
    {
        MergeTreeData::DataPartsVector projection_parts;
        for (const auto & part : parts)
        {
            auto it = part->getProjectionParts().find(projection.name);
            if (it != part->getProjectionParts().end())
                projection_parts.push_back(it->second);
        }
        if (projection_parts.size() < parts.size())
        {
            LOG_DEBUG(log, "Projection {} is not merged because some parts don't have it", projection.name);
            continue;
        }

        LOG_DEBUG(
            log,
            "Selected {} projection_parts from {} to {}",
            projection_parts.size(),
            projection_parts.front()->name,
            projection_parts.back()->name);

        FutureMergedMutatedPart projection_future_part;
        projection_future_part.assign(std::move(projection_parts));
        projection_future_part.name = projection.name;
        projection_future_part.path = future_part.path + "/" + projection.name + ".proj/";
        projection_future_part.part_info = {"all", 0, 0, 0};

        MergeTreeData::MergingParams projection_merging_params;
        projection_merging_params.mode = MergeTreeData::MergingParams::Ordinary;
        if (projection.type == ProjectionDescription::Type::Aggregate)
            projection_merging_params.mode = MergeTreeData::MergingParams::Aggregating;

        // TODO Should we use a new merge_entry for projection?
        auto merged_projection_part = mergePartsToTemporaryPart(
            projection_future_part,
            projection.metadata,
            merge_entry,
            holder,
            time_of_merge,
            context,
            space_reservation,
            deduplicate,
            deduplicate_by_columns,
            projection_merging_params,
            new_data_part.get());
        new_data_part->addProjectionPart(projection.name, std::move(merged_projection_part));
    }

    if (chosen_merge_algorithm != MergeAlgorithm::Vertical)
        to.writeSuffixAndFinalizePart(new_data_part, need_sync);
    else
        to.writeSuffixAndFinalizePart(new_data_part, need_sync, &storage_columns, &checksums_gathered_columns);

    return new_data_part;
}


MergeTreeData::MutableDataPartPtr MergeTreeDataMergerMutator::mutatePartToTemporaryPart(
    const FutureMergedMutatedPart & future_part,
    const StorageMetadataPtr & metadata_snapshot,
    const MutationCommands & commands,
    MergeListEntry & merge_entry,
    time_t time_of_mutation,
    ContextPtr context,
    const ReservationPtr & space_reservation,
    TableLockHolder & holder)
{
    checkOperationIsNotCanceled(merge_entry);

    if (future_part.parts.size() != 1)
        throw Exception("Trying to mutate " + toString(future_part.parts.size()) + " parts, not one. "
            "This is a bug.", ErrorCodes::LOGICAL_ERROR);

    CurrentMetrics::Increment num_mutations{CurrentMetrics::PartMutation};
    const auto & source_part = future_part.parts[0];
    auto storage_from_source_part = StorageFromMergeTreeDataPart::create(source_part);

    auto context_for_reading = Context::createCopy(context);
    context_for_reading->setSetting("max_streams_to_max_threads_ratio", 1);
    context_for_reading->setSetting("max_threads", 1);
    /// Allow mutations to work when force_index_by_date or force_primary_key is on.
    context_for_reading->setSetting("force_index_by_date", Field(0));
    context_for_reading->setSetting("force_primary_key", Field(0));

    MutationCommands commands_for_part;
    for (const auto & command : commands)
    {
        if (command.partition == nullptr || future_part.parts[0]->info.partition_id == data.getPartitionIDFromQuery(
                command.partition, context_for_reading))
            commands_for_part.emplace_back(command);
    }

    if (source_part->isStoredOnDisk() && !isStorageTouchedByMutations(
        storage_from_source_part, metadata_snapshot, commands_for_part, Context::createCopy(context_for_reading)))
    {
        LOG_TRACE(log, "Part {} doesn't change up to mutation version {}", source_part->name, future_part.part_info.mutation);
        return data.cloneAndLoadDataPartOnSameDisk(source_part, "tmp_clone_", future_part.part_info, metadata_snapshot);
    }
    else
    {
        LOG_TRACE(log, "Mutating part {} to mutation version {}", source_part->name, future_part.part_info.mutation);
    }

    BlockInputStreamPtr in = nullptr;
    Block updated_header;
    std::unique_ptr<MutationsInterpreter> interpreter;

    const auto data_settings = data.getSettings();
    MutationCommands for_interpreter;
    MutationCommands for_file_renames;

    splitMutationCommands(source_part, commands_for_part, for_interpreter, for_file_renames);

    UInt64 watch_prev_elapsed = 0;
    MergeStageProgress stage_progress(1.0);

    NamesAndTypesList storage_columns = metadata_snapshot->getColumns().getAllPhysical();
    NameSet materialized_indices;
    NameSet materialized_projections;
    MutationsInterpreter::MutationKind::MutationKindEnum mutation_kind
        = MutationsInterpreter::MutationKind::MutationKindEnum::MUTATE_UNKNOWN;

    if (!for_interpreter.empty())
    {
        interpreter = std::make_unique<MutationsInterpreter>(
            storage_from_source_part, metadata_snapshot, for_interpreter, context_for_reading, true);
        materialized_indices = interpreter->grabMaterializedIndices();
        materialized_projections = interpreter->grabMaterializedProjections();
        mutation_kind = interpreter->getMutationKind();
        in = interpreter->execute();
        updated_header = interpreter->getUpdatedHeader();
        in->setProgressCallback(MergeProgressCallback(merge_entry, watch_prev_elapsed, stage_progress));
    }

    auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + future_part.name, space_reservation->getDisk(), 0);
    auto new_data_part = data.createPart(
        future_part.name, future_part.type, future_part.part_info, single_disk_volume, "tmp_mut_" + future_part.name);

    new_data_part->uuid = future_part.uuid;
    new_data_part->is_temp = true;
    new_data_part->ttl_infos = source_part->ttl_infos;

    /// It shouldn't be changed by mutation.
    new_data_part->index_granularity_info = source_part->index_granularity_info;
    new_data_part->setColumns(getColumnsForNewDataPart(source_part, updated_header, storage_columns, for_file_renames));
    new_data_part->partition.assign(source_part->partition);

    auto disk = new_data_part->volume->getDisk();
    String new_part_tmp_path = new_data_part->getFullRelativePath();

    SyncGuardPtr sync_guard;
    if (data.getSettings()->fsync_part_directory)
        sync_guard = disk->getDirectorySyncGuard(new_part_tmp_path);

    /// Don't change granularity type while mutating subset of columns
    auto mrk_extension = source_part->index_granularity_info.is_adaptive ? getAdaptiveMrkExtension(new_data_part->getType())
                                                                         : getNonAdaptiveMrkExtension();
    bool need_sync = needSyncPart(source_part->rows_count, source_part->getBytesOnDisk(), *data_settings);
    bool need_remove_expired_values = false;

    if (in && shouldExecuteTTL(metadata_snapshot, in->getHeader().getNamesAndTypesList().getNames(), commands_for_part))
        need_remove_expired_values = true;

    /// All columns from part are changed and may be some more that were missing before in part
    /// TODO We can materialize compact part without copying data
    if (!isWidePart(source_part)
        || (mutation_kind == MutationsInterpreter::MutationKind::MUTATE_OTHER && interpreter && interpreter->isAffectingAllColumns()))
    {
        disk->createDirectories(new_part_tmp_path);

        /// Note: this is done before creating input streams, because otherwise data.data_parts_mutex
        /// (which is locked in data.getTotalActiveSizeInBytes())
        /// (which is locked in shared mode when input streams are created) and when inserting new data
        /// the order is reverse. This annoys TSan even though one lock is locked in shared mode and thus
        /// deadlock is impossible.
        auto compression_codec = data.getCompressionCodecForPart(source_part->getBytesOnDisk(), source_part->ttl_infos, time_of_mutation);

        auto part_indices = getIndicesForNewDataPart(metadata_snapshot->getSecondaryIndices(), for_file_renames);
        auto part_projections = getProjectionsForNewDataPart(metadata_snapshot->getProjections(), for_file_renames);

        mutateAllPartColumns(
            new_data_part,
            metadata_snapshot,
            part_indices,
            part_projections,
            in,
            time_of_mutation,
            compression_codec,
            merge_entry,
            need_remove_expired_values,
            need_sync,
            space_reservation,
            holder,
            context);

        /// no finalization required, because mutateAllPartColumns use
        /// MergedBlockOutputStream which finilaze all part fields itself
    }
    else /// TODO: check that we modify only non-key columns in this case.
    {
        /// We will modify only some of the columns. Other columns and key values can be copied as-is.
        NameSet updated_columns;
        if (mutation_kind != MutationsInterpreter::MutationKind::MUTATE_INDEX_PROJECTION)
        {
            for (const auto & name_type : updated_header.getNamesAndTypesList())
                updated_columns.emplace(name_type.name);
        }

        auto indices_to_recalc = getIndicesToRecalculate(
            in, updated_columns, metadata_snapshot, context, materialized_indices, source_part);
        auto projections_to_recalc = getProjectionsToRecalculate(
            updated_columns, metadata_snapshot, materialized_projections, source_part);

        NameSet files_to_skip = collectFilesToSkip(
            source_part,
            mutation_kind == MutationsInterpreter::MutationKind::MUTATE_INDEX_PROJECTION ? Block{} : updated_header,
            indices_to_recalc,
            mrk_extension,
            projections_to_recalc);
        NameToNameVector files_to_rename = collectFilesForRenames(source_part, for_file_renames, mrk_extension);

        if (indices_to_recalc.empty() && projections_to_recalc.empty() && mutation_kind != MutationsInterpreter::MutationKind::MUTATE_OTHER
            && files_to_rename.empty())
        {
            LOG_TRACE(
                log, "Part {} doesn't change up to mutation version {} (optimized)", source_part->name, future_part.part_info.mutation);
            return data.cloneAndLoadDataPartOnSameDisk(source_part, "tmp_clone_", future_part.part_info, metadata_snapshot);
        }

        if (need_remove_expired_values)
            files_to_skip.insert("ttl.txt");

        disk->createDirectories(new_part_tmp_path);

        /// Create hardlinks for unchanged files
        for (auto it = disk->iterateDirectory(source_part->getFullRelativePath()); it->isValid(); it->next())
        {
            if (files_to_skip.count(it->name()))
                continue;

            String destination = new_part_tmp_path;
            String file_name = it->name();
            auto rename_it = std::find_if(files_to_rename.begin(), files_to_rename.end(), [&file_name](const auto & rename_pair) { return rename_pair.first == file_name; });
            if (rename_it != files_to_rename.end())
            {
                if (rename_it->second.empty())
                    continue;
                destination += rename_it->second;
            }
            else
            {
                destination += it->name();
            }

            if (!disk->isDirectory(it->path()))
                disk->createHardLink(it->path(), destination);
            else if (!startsWith("tmp_", it->name())) // ignore projection tmp merge dir
            {
                // it's a projection part directory
                disk->createDirectories(destination);
                for (auto p_it = disk->iterateDirectory(it->path()); p_it->isValid(); p_it->next())
                {
                    String p_destination = destination + "/";
                    String p_file_name = p_it->name();
                    p_destination += p_it->name();
                    disk->createHardLink(p_it->path(), p_destination);
                }
            }
        }

        merge_entry->columns_written = storage_columns.size() - updated_header.columns();

        new_data_part->checksums = source_part->checksums;

        auto compression_codec = source_part->default_codec;

        if (in)
        {
            mutateSomePartColumns(
                source_part,
                metadata_snapshot,
                indices_to_recalc,
                projections_to_recalc,
                // If it's an index/projection materialization, we don't write any data columns, thus empty header is used
                mutation_kind == MutationsInterpreter::MutationKind::MUTATE_INDEX_PROJECTION ? Block{} : updated_header,
                new_data_part,
                in,
                time_of_mutation,
                compression_codec,
                merge_entry,
                need_remove_expired_values,
                need_sync,
                space_reservation,
                holder,
                context);
        }

        for (const auto & [rename_from, rename_to] : files_to_rename)
        {
            if (rename_to.empty() && new_data_part->checksums.files.count(rename_from))
            {
                new_data_part->checksums.files.erase(rename_from);
            }
            else if (new_data_part->checksums.files.count(rename_from))
            {
                new_data_part->checksums.files[rename_to] = new_data_part->checksums.files[rename_from];

                new_data_part->checksums.files.erase(rename_from);
            }
        }

        finalizeMutatedPart(source_part, new_data_part, need_remove_expired_values, compression_codec);
    }

    return new_data_part;
}


MergeAlgorithm MergeTreeDataMergerMutator::chooseMergeAlgorithm(
    const MergeTreeData::DataPartsVector & parts,
    size_t sum_rows_upper_bound,
    const NamesAndTypesList & gathering_columns,
    bool deduplicate,
    bool need_remove_expired_values,
    const MergeTreeData::MergingParams & merging_params) const
{
    const auto data_settings = data.getSettings();

    if (deduplicate)
        return MergeAlgorithm::Horizontal;
    if (data_settings->enable_vertical_merge_algorithm == 0)
        return MergeAlgorithm::Horizontal;
    if (need_remove_expired_values)
        return MergeAlgorithm::Horizontal;

    for (const auto & part : parts)
        if (!part->supportsVerticalMerge())
            return MergeAlgorithm::Horizontal;

    bool is_supported_storage =
        merging_params.mode == MergeTreeData::MergingParams::Ordinary ||
        merging_params.mode == MergeTreeData::MergingParams::Collapsing ||
        merging_params.mode == MergeTreeData::MergingParams::Replacing ||
        merging_params.mode == MergeTreeData::MergingParams::VersionedCollapsing;

    bool enough_ordinary_cols = gathering_columns.size() >= data_settings->vertical_merge_algorithm_min_columns_to_activate;

    bool enough_total_rows = sum_rows_upper_bound >= data_settings->vertical_merge_algorithm_min_rows_to_activate;

    bool no_parts_overflow = parts.size() <= RowSourcePart::MAX_PARTS;

    auto merge_alg = (is_supported_storage && enough_total_rows && enough_ordinary_cols && no_parts_overflow) ?
                        MergeAlgorithm::Vertical : MergeAlgorithm::Horizontal;

    return merge_alg;
}


MergeTreeData::DataPartPtr MergeTreeDataMergerMutator::renameMergedTemporaryPart(
    MergeTreeData::MutableDataPartPtr & new_data_part,
    const MergeTreeData::DataPartsVector & parts,
    MergeTreeData::Transaction * out_transaction)
{
    /// Rename new part, add to the set and remove original parts.
    auto replaced_parts = data.renameTempPartAndReplace(new_data_part, nullptr, out_transaction);

    /// Let's check that all original parts have been deleted and only them.
    if (replaced_parts.size() != parts.size())
    {
        /** This is normal, although this happens rarely.
         *
         * The situation - was replaced 0 parts instead of N can be, for example, in the following case
         * - we had A part, but there was no B and C parts;
         * - A, B -> AB was in the queue, but it has not been done, because there is no B part;
         * - AB, C -> ABC was in the queue, but it has not been done, because there are no AB and C parts;
         * - we have completed the task of downloading a B part;
         * - we started to make A, B -> AB merge, since all parts appeared;
         * - we decided to download ABC part from another replica, since it was impossible to make merge AB, C -> ABC;
         * - ABC part appeared. When it was added, old A, B, C parts were deleted;
         * - AB merge finished. AB part was added. But this is an obsolete part. The log will contain the message `Obsolete part added`,
         *   then we get here.
         *
         * When M > N parts could be replaced?
         * - new block was added in ReplicatedMergeTreeBlockOutputStream;
         * - it was added to working dataset in memory and renamed on filesystem;
         * - but ZooKeeper transaction that adds it to reference dataset in ZK failed;
         * - and it is failed due to connection loss, so we don't rollback working dataset in memory,
         *   because we don't know if the part was added to ZK or not
         *   (see ReplicatedMergeTreeBlockOutputStream)
         * - then method selectPartsToMerge selects a range and sees, that EphemeralLock for the block in this part is unlocked,
         *   and so it is possible to merge a range skipping this part.
         *   (NOTE: Merging with part that is not in ZK is not possible, see checks in 'createLogEntryToMergeParts'.)
         * - and after merge, this part will be removed in addition to parts that was merged.
         */
        LOG_WARNING(log, "Unexpected number of parts removed when adding {}: {} instead of {}", new_data_part->name, replaced_parts.size(), parts.size());
    }
    else
    {
        for (size_t i = 0; i < parts.size(); ++i)
            if (parts[i]->name != replaced_parts[i]->name)
                throw Exception("Unexpected part removed when adding " + new_data_part->name + ": " + replaced_parts[i]->name
                    + " instead of " + parts[i]->name, ErrorCodes::LOGICAL_ERROR);
    }

    LOG_TRACE(log, "Merged {} parts: from {} to {}", parts.size(), parts.front()->name, parts.back()->name);
    return new_data_part;
}


size_t MergeTreeDataMergerMutator::estimateNeededDiskSpace(const MergeTreeData::DataPartsVector & source_parts)
{
    size_t res = 0;
    for (const MergeTreeData::DataPartPtr & part : source_parts)
        res += part->getBytesOnDisk();

    return static_cast<size_t>(res * DISK_USAGE_COEFFICIENT_TO_RESERVE);
}

void MergeTreeDataMergerMutator::splitMutationCommands(
    MergeTreeData::DataPartPtr part,
    const MutationCommands & commands,
    MutationCommands & for_interpreter,
    MutationCommands & for_file_renames)
{
    ColumnsDescription part_columns(part->getColumns());

    if (!isWidePart(part))
    {
        NameSet mutated_columns;
        for (const auto & command : commands)
        {
            if (command.type == MutationCommand::Type::MATERIALIZE_INDEX
                || command.type == MutationCommand::Type::MATERIALIZE_PROJECTION
                || command.type == MutationCommand::Type::MATERIALIZE_TTL
                || command.type == MutationCommand::Type::DELETE
                || command.type == MutationCommand::Type::UPDATE)
            {
                for_interpreter.push_back(command);
                for (const auto & [column_name, expr] : command.column_to_update_expression)
                    mutated_columns.emplace(column_name);
            }
            else if (command.type == MutationCommand::Type::DROP_INDEX || command.type == MutationCommand::Type::DROP_PROJECTION)
            {
                for_file_renames.push_back(command);
            }
            else if (part_columns.has(command.column_name))
            {
                if (command.type == MutationCommand::Type::DROP_COLUMN)
                {
                    mutated_columns.emplace(command.column_name);
                }
                else if (command.type == MutationCommand::Type::RENAME_COLUMN)
                {
                    for_interpreter.push_back(
                    {
                        .type = MutationCommand::Type::READ_COLUMN,
                        .column_name = command.rename_to,
                    });
                    mutated_columns.emplace(command.column_name);
                    part_columns.rename(command.column_name, command.rename_to);
                }
            }
        }
        /// If it's compact part, then we don't need to actually remove files
        /// from disk we just don't read dropped columns
        for (const auto & column : part->getColumns())
        {
            if (!mutated_columns.count(column.name))
                for_interpreter.emplace_back(
                    MutationCommand{.type = MutationCommand::Type::READ_COLUMN, .column_name = column.name, .data_type = column.type});
        }
    }
    else
    {
        for (const auto & command : commands)
        {
            if (command.type == MutationCommand::Type::MATERIALIZE_INDEX
                || command.type == MutationCommand::Type::MATERIALIZE_PROJECTION
                || command.type == MutationCommand::Type::MATERIALIZE_TTL
                || command.type == MutationCommand::Type::DELETE
                || command.type == MutationCommand::Type::UPDATE)
            {
                for_interpreter.push_back(command);
            }
            else if (command.type == MutationCommand::Type::DROP_INDEX || command.type == MutationCommand::Type::DROP_PROJECTION)
            {
                for_file_renames.push_back(command);
            }
            /// If we don't have this column in source part, than we don't need
            /// to materialize it
            else if (part_columns.has(command.column_name))
            {
                if (command.type == MutationCommand::Type::READ_COLUMN)
                {
                    for_interpreter.push_back(command);
                }
                else if (command.type == MutationCommand::Type::RENAME_COLUMN)
                {
                    part_columns.rename(command.column_name, command.rename_to);
                    for_file_renames.push_back(command);
                }
                else
                {
                    for_file_renames.push_back(command);
                }
            }
        }
    }
}


NameToNameVector MergeTreeDataMergerMutator::collectFilesForRenames(
    MergeTreeData::DataPartPtr source_part, const MutationCommands & commands_for_removes, const String & mrk_extension)
{
    /// Collect counts for shared streams of different columns. As an example, Nested columns have shared stream with array sizes.
    std::map<String, size_t> stream_counts;
    for (const NameAndTypePair & column : source_part->getColumns())
    {
        auto serialization = source_part->getSerializationForColumn(column);
        serialization->enumerateStreams(
            [&](const ISerialization::SubstreamPath & substream_path)
            {
                ++stream_counts[ISerialization::getFileNameForStream(column, substream_path)];
            },
            {});
    }

    NameToNameVector rename_vector;
    /// Remove old data
    for (const auto & command : commands_for_removes)
    {
        if (command.type == MutationCommand::Type::DROP_INDEX)
        {
            if (source_part->checksums.has(INDEX_FILE_PREFIX + command.column_name + ".idx"))
            {
                rename_vector.emplace_back(INDEX_FILE_PREFIX + command.column_name + ".idx", "");
                rename_vector.emplace_back(INDEX_FILE_PREFIX + command.column_name + mrk_extension, "");
            }
        }
        else if (command.type == MutationCommand::Type::DROP_PROJECTION)
        {
            if (source_part->checksums.has(command.column_name + ".proj"))
                rename_vector.emplace_back(command.column_name + ".proj", "");
        }
        else if (command.type == MutationCommand::Type::DROP_COLUMN)
        {
            ISerialization::StreamCallback callback = [&](const ISerialization::SubstreamPath & substream_path)
            {
                String stream_name = ISerialization::getFileNameForStream({command.column_name, command.data_type}, substream_path);
                /// Delete files if they are no longer shared with another column.
                if (--stream_counts[stream_name] == 0)
                {
                    rename_vector.emplace_back(stream_name + ".bin", "");
                    rename_vector.emplace_back(stream_name + mrk_extension, "");
                }
            };

            auto column = source_part->getColumns().tryGetByName(command.column_name);
            if (column)
            {
                auto serialization = source_part->getSerializationForColumn(*column);
                serialization->enumerateStreams(callback);
            }
        }
        else if (command.type == MutationCommand::Type::RENAME_COLUMN)
        {
            String escaped_name_from = escapeForFileName(command.column_name);
            String escaped_name_to = escapeForFileName(command.rename_to);

            ISerialization::StreamCallback callback = [&](const ISerialization::SubstreamPath & substream_path)
            {
                String stream_from = ISerialization::getFileNameForStream({command.column_name, command.data_type}, substream_path);

                String stream_to = boost::replace_first_copy(stream_from, escaped_name_from, escaped_name_to);

                if (stream_from != stream_to)
                {
                    rename_vector.emplace_back(stream_from + ".bin", stream_to + ".bin");
                    rename_vector.emplace_back(stream_from + mrk_extension, stream_to + mrk_extension);
                }
            };

            auto column = source_part->getColumns().tryGetByName(command.column_name);
            if (column)
            {
                auto serialization = source_part->getSerializationForColumn(*column);
                serialization->enumerateStreams(callback);
            }
        }
    }

    return rename_vector;
}

NameSet MergeTreeDataMergerMutator::collectFilesToSkip(
    const MergeTreeDataPartPtr & source_part,
    const Block & updated_header,
    const std::set<MergeTreeIndexPtr> & indices_to_recalc,
    const String & mrk_extension,
    const std::set<MergeTreeProjectionPtr> & projections_to_recalc)
{
    NameSet files_to_skip = source_part->getFileNamesWithoutChecksums();

    /// Skip updated files
    for (const auto & entry : updated_header)
    {
        ISerialization::StreamCallback callback = [&](const ISerialization::SubstreamPath & substream_path)
        {
            String stream_name = ISerialization::getFileNameForStream({entry.name, entry.type}, substream_path);
            files_to_skip.insert(stream_name + ".bin");
            files_to_skip.insert(stream_name + mrk_extension);
        };

        auto serialization = source_part->getSerializationForColumn({entry.name, entry.type});
        serialization->enumerateStreams(callback);
    }
    for (const auto & index : indices_to_recalc)
    {
        files_to_skip.insert(index->getFileName() + ".idx");
        files_to_skip.insert(index->getFileName() + mrk_extension);
    }
    for (const auto & projection : projections_to_recalc)
    {
        files_to_skip.insert(projection->getDirectoryName());
    }

    return files_to_skip;
}


NamesAndTypesList MergeTreeDataMergerMutator::getColumnsForNewDataPart(
    MergeTreeData::DataPartPtr source_part,
    const Block & updated_header,
    NamesAndTypesList storage_columns,
    const MutationCommands & commands_for_removes)
{
    /// In compact parts we read all columns, because they all stored in a
    /// single file
    if (!isWidePart(source_part))
        return updated_header.getNamesAndTypesList();

    NameSet removed_columns;
    NameToNameMap renamed_columns_to_from;
    /// All commands are validated in AlterCommand so we don't care about order
    for (const auto & command : commands_for_removes)
    {
        if (command.type == MutationCommand::DROP_COLUMN)
            removed_columns.insert(command.column_name);
        if (command.type == MutationCommand::RENAME_COLUMN)
            renamed_columns_to_from.emplace(command.rename_to, command.column_name);
    }
    Names source_column_names = source_part->getColumns().getNames();
    NameSet source_columns_name_set(source_column_names.begin(), source_column_names.end());
    for (auto it = storage_columns.begin(); it != storage_columns.end();)
    {
        if (updated_header.has(it->name))
        {
            auto updated_type = updated_header.getByName(it->name).type;
            if (updated_type != it->type)
                it->type = updated_type;
            ++it;
        }
        else
        {
            if (!source_columns_name_set.count(it->name))
            {
                /// Source part doesn't have column but some other column
                /// was renamed to it's name.
                auto renamed_it = renamed_columns_to_from.find(it->name);
                if (renamed_it != renamed_columns_to_from.end()
                    && source_columns_name_set.count(renamed_it->second))
                    ++it;
                else
                    it = storage_columns.erase(it);
            }
            else
            {
                bool was_renamed = false;
                bool was_removed = removed_columns.count(it->name);

                /// Check that this column was renamed to some other name
                for (const auto & [rename_to, rename_from] : renamed_columns_to_from)
                {
                    if (rename_from == it->name)
                    {
                        was_renamed = true;
                        break;
                    }
                }

                /// If we want to rename this column to some other name, than it
                /// should it's previous version should be dropped or removed
                if (renamed_columns_to_from.count(it->name) && !was_renamed && !was_removed)
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Incorrect mutation commands, trying to rename column {} to {}, but part {} already has column {}", renamed_columns_to_from[it->name], it->name, source_part->name, it->name);


                /// Column was renamed and no other column renamed to it's name
                /// or column is dropped.
                if (!renamed_columns_to_from.count(it->name) && (was_renamed || was_removed))
                    it = storage_columns.erase(it);
                else
                    ++it;
            }
        }
    }

    return storage_columns;
}

MergeTreeIndices MergeTreeDataMergerMutator::getIndicesForNewDataPart(
    const IndicesDescription & all_indices,
    const MutationCommands & commands_for_removes)
{
    NameSet removed_indices;
    for (const auto & command : commands_for_removes)
        if (command.type == MutationCommand::DROP_INDEX)
            removed_indices.insert(command.column_name);

    MergeTreeIndices new_indices;
    for (const auto & index : all_indices)
        if (!removed_indices.count(index.name))
            new_indices.push_back(MergeTreeIndexFactory::instance().get(index));

    return new_indices;
}

MergeTreeProjections MergeTreeDataMergerMutator::getProjectionsForNewDataPart(
    const ProjectionsDescription & all_projections,
    const MutationCommands & commands_for_removes)
{
    NameSet removed_projections;
    for (const auto & command : commands_for_removes)
        if (command.type == MutationCommand::DROP_PROJECTION)
            removed_projections.insert(command.column_name);

    MergeTreeProjections new_projections;
    for (const auto & projection : all_projections)
        if (!removed_projections.count(projection.name))
            new_projections.push_back(MergeTreeProjectionFactory::instance().get(projection));

    return new_projections;
}

std::set<MergeTreeIndexPtr> MergeTreeDataMergerMutator::getIndicesToRecalculate(
    BlockInputStreamPtr & input_stream,
    const NameSet & updated_columns,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr context,
    const NameSet & materialized_indices,
    const MergeTreeData::DataPartPtr & source_part)
{
    /// Checks if columns used in skipping indexes modified.
    const auto & index_factory = MergeTreeIndexFactory::instance();
    std::set<MergeTreeIndexPtr> indices_to_recalc;
    ASTPtr indices_recalc_expr_list = std::make_shared<ASTExpressionList>();
    const auto & indices = metadata_snapshot->getSecondaryIndices();

    for (size_t i = 0; i < indices.size(); ++i)
    {
        const auto & index = indices[i];

        // If we ask to materialize and it already exists
        if (!source_part->checksums.has(INDEX_FILE_PREFIX + index.name + ".idx") && materialized_indices.count(index.name))
        {
            if (indices_to_recalc.insert(index_factory.get(index)).second)
            {
                ASTPtr expr_list = index.expression_list_ast->clone();
                for (const auto & expr : expr_list->children)
                    indices_recalc_expr_list->children.push_back(expr->clone());
            }
        }
        // If some dependent columns gets mutated
        else
        {
            bool mutate = false;
            const auto & index_cols = index.expression->getRequiredColumns();
            for (const auto & col : index_cols)
            {
                if (updated_columns.count(col))
                {
                    mutate = true;
                    break;
                }
            }
            if (mutate && indices_to_recalc.insert(index_factory.get(index)).second)
            {
                ASTPtr expr_list = index.expression_list_ast->clone();
                for (const auto & expr : expr_list->children)
                    indices_recalc_expr_list->children.push_back(expr->clone());
            }
        }
    }

    if (!indices_to_recalc.empty() && input_stream)
    {
        auto indices_recalc_syntax = TreeRewriter(context).analyze(indices_recalc_expr_list, input_stream->getHeader().getNamesAndTypesList());
        auto indices_recalc_expr = ExpressionAnalyzer(
                indices_recalc_expr_list,
                indices_recalc_syntax, context).getActions(false);

        /// We can update only one column, but some skip idx expression may depend on several
        /// columns (c1 + c2 * c3). It works because this stream was created with help of
        /// MutationsInterpreter which knows about skip indices and stream 'in' already has
        /// all required columns.
        /// TODO move this logic to single place.
        input_stream = std::make_shared<MaterializingBlockInputStream>(
            std::make_shared<ExpressionBlockInputStream>(input_stream, indices_recalc_expr));
    }
    return indices_to_recalc;
}

std::set<MergeTreeProjectionPtr> MergeTreeDataMergerMutator::getProjectionsToRecalculate(
    const NameSet & updated_columns,
    const StorageMetadataPtr & metadata_snapshot,
    const NameSet & materialized_projections,
    const MergeTreeData::DataPartPtr & source_part)
{
    /// Checks if columns used in projections modified.
    const auto & projection_factory = MergeTreeProjectionFactory::instance();
    std::set<MergeTreeProjectionPtr> projections_to_recalc;
    for (const auto & projection : metadata_snapshot->getProjections())
    {
        // If we ask to materialize and it doesn't exist
        if (!source_part->checksums.has(projection.name + ".proj") && materialized_projections.count(projection.name))
        {
            projections_to_recalc.insert(projection_factory.get(projection));
        }
        else
        {
            // If some dependent columns gets mutated
            bool mutate = false;
            const auto & projection_cols = projection.required_columns;
            for (const auto & col : projection_cols)
            {
                if (updated_columns.count(col))
                {
                    mutate = true;
                    break;
                }
            }
            if (mutate)
                projections_to_recalc.insert(projection_factory.get(projection));
        }
    }
    return projections_to_recalc;
}

bool MergeTreeDataMergerMutator::shouldExecuteTTL(const StorageMetadataPtr & metadata_snapshot, const Names & columns, const MutationCommands & commands)
{
    if (!metadata_snapshot->hasAnyTTL())
        return false;

    for (const auto & command : commands)
        if (command.type == MutationCommand::MATERIALIZE_TTL)
            return true;

    auto dependencies = metadata_snapshot->getColumnDependencies(NameSet(columns.begin(), columns.end()));
    for (const auto & dependency : dependencies)
        if (dependency.kind == ColumnDependency::TTL_EXPRESSION || dependency.kind == ColumnDependency::TTL_TARGET)
            return true;

    return false;
}

// 1. get projection pipeline and a sink to write parts
// 2. build an executor that can write block to the input stream (actually we can write through it to generate as many parts as possible)
// 3. finalize the pipeline so that all parts are merged into one part
void MergeTreeDataMergerMutator::writeWithProjections(
    MergeTreeData::MutableDataPartPtr new_data_part,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeProjections & projections_to_build,
    BlockInputStreamPtr mutating_stream,
    IMergedBlockOutputStream & out,
    time_t time_of_mutation,
    MergeListEntry & merge_entry,
    const ReservationPtr & space_reservation,
    TableLockHolder & holder,
    ContextPtr context,
    IMergeTreeDataPart::MinMaxIndex * minmax_idx)
{
    size_t block_num = 0;
    std::map<String, MergeTreeData::MutableDataPartsVector> projection_parts;
    Block block;
    std::vector<SquashingTransform> projection_squashes;
    for (size_t i = 0, size = projections_to_build.size(); i < size; ++i)
    {
        projection_squashes.emplace_back(65536, 65536 * 256);
    }
    while (checkOperationIsNotCanceled(merge_entry) && (block = mutating_stream->read()))
    {
        if (minmax_idx)
            minmax_idx->update(block, data.getMinMaxColumnsNames(metadata_snapshot->getPartitionKey()));

        out.write(block);

        for (size_t i = 0, size = projections_to_build.size(); i < size; ++i)
        {
            const auto & projection = projections_to_build[i]->projection;
            auto in = InterpreterSelectQuery(
                          projection.query_ast,
                          context,
                          Pipe(std::make_shared<SourceFromSingleChunk>(block, Chunk(block.getColumns(), block.rows()))),
                          SelectQueryOptions{
                              projection.type == ProjectionDescription::Type::Normal ? QueryProcessingStage::FetchColumns : QueryProcessingStage::WithMergeableState})
                          .execute()
                          .getInputStream();
            in = std::make_shared<SquashingBlockInputStream>(in, block.rows(), std::numeric_limits<UInt64>::max());
            in->readPrefix();
            auto & projection_squash = projection_squashes[i];
            auto projection_block = projection_squash.add(in->read());
            if (in->read())
                throw Exception("Projection cannot increase the number of rows in a block", ErrorCodes::LOGICAL_ERROR);
            in->readSuffix();
            if (projection_block)
            {
                projection_parts[projection.name].emplace_back(
                    MergeTreeDataWriter::writeTempProjectionPart(data, log, projection_block, projection, new_data_part.get(), ++block_num));
            }
        }

        merge_entry->rows_written += block.rows();
        merge_entry->bytes_written_uncompressed += block.bytes();
    }

    // Write the last block
    for (size_t i = 0, size = projections_to_build.size(); i < size; ++i)
    {
        const auto & projection = projections_to_build[i]->projection;
        auto & projection_squash = projection_squashes[i];
        auto projection_block = projection_squash.add({});
        if (projection_block)
        {
            projection_parts[projection.name].emplace_back(
                MergeTreeDataWriter::writeTempProjectionPart(data, log, projection_block, projection, new_data_part.get(), ++block_num));
        }
    }

    const auto & projections = metadata_snapshot->projections;

    for (auto && [name, parts] : projection_parts)
    {
        LOG_DEBUG(log, "Selected {} projection_parts from {} to {}", parts.size(), parts.front()->name, parts.back()->name);

        const auto & projection = projections.get(name);

        std::map<size_t, MergeTreeData::MutableDataPartsVector> level_parts;
        size_t current_level = 0;
        size_t next_level = 1;
        level_parts[current_level] = std::move(parts);
        size_t max_parts_to_merge_in_one_level = 10;
        for (;;)
        {
            auto & current_level_parts = level_parts[current_level];
            auto & next_level_parts = level_parts[next_level];

            MergeTreeData::MutableDataPartsVector selected_parts;
            while (selected_parts.size() < max_parts_to_merge_in_one_level && !current_level_parts.empty())
            {
                selected_parts.push_back(std::move(current_level_parts.back()));
                current_level_parts.pop_back();
            }

            if (selected_parts.empty())
            {
                if (next_level_parts.empty())
                {
                    LOG_WARNING(log, "There is no projection parts merged");
                    break;
                }
                current_level = next_level;
                ++next_level;
            }
            else if (selected_parts.size() == 1)
            {
                if (next_level_parts.empty())
                {
                    LOG_DEBUG(log, "Merged a projection part in level {}", current_level);
                    selected_parts[0]->renameTo(projection.name + ".proj", true);
                    selected_parts[0]->name = projection.name;
                    selected_parts[0]->is_temp = false;
                    new_data_part->addProjectionPart(name, std::move(selected_parts[0]));
                    break;
                }
                else
                {
                    LOG_DEBUG(log, "Forwarded part {} in level {} to next level", selected_parts[0]->name, current_level);
                    next_level_parts.push_back(std::move(selected_parts[0]));
                }
            }
            else if (selected_parts.size() > 1)
            {
                // Generate a unique part name
                ++block_num;
                FutureMergedMutatedPart projection_future_part;
                MergeTreeData::DataPartsVector const_selected_parts(
                    std::make_move_iterator(selected_parts.begin()), std::make_move_iterator(selected_parts.end()));
                projection_future_part.assign(std::move(const_selected_parts));
                projection_future_part.name = fmt::format("{}_{}", projection.name, ++block_num);
                projection_future_part.part_info = {"all", 0, 0, 0};

                MergeTreeData::MergingParams projection_merging_params;
                projection_merging_params.mode = MergeTreeData::MergingParams::Ordinary;
                if (projection.type == ProjectionDescription::Type::Aggregate)
                    projection_merging_params.mode = MergeTreeData::MergingParams::Aggregating;

                LOG_DEBUG(log, "Merged {} parts in level {} to {}", selected_parts.size(), current_level, projection_future_part.name);
                next_level_parts.push_back(mergePartsToTemporaryPart(
                    projection_future_part,
                    projection.metadata,
                    merge_entry,
                    holder,
                    time_of_mutation,
                    context,
                    space_reservation,
                    false, // TODO Do we need deduplicate for projections
                    {},
                    projection_merging_params,
                    new_data_part.get(),
                    "tmp_merge_"));

                next_level_parts.back()->is_temp = true;
            }
        }
    }
}

void MergeTreeDataMergerMutator::mutateAllPartColumns(
    MergeTreeData::MutableDataPartPtr new_data_part,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeIndices & skip_indices,
    const MergeTreeProjections & projections_to_build,
    BlockInputStreamPtr mutating_stream,
    time_t time_of_mutation,
    const CompressionCodecPtr & compression_codec,
    MergeListEntry & merge_entry,
    bool need_remove_expired_values,
    bool need_sync,
    const ReservationPtr & space_reservation,
    TableLockHolder & holder,
    ContextPtr context)
{
    if (mutating_stream == nullptr)
        throw Exception("Cannot mutate part columns with uninitialized mutations stream. It's a bug", ErrorCodes::LOGICAL_ERROR);

    if (metadata_snapshot->hasPrimaryKey() || metadata_snapshot->hasSecondaryIndices())
        mutating_stream = std::make_shared<MaterializingBlockInputStream>(
            std::make_shared<ExpressionBlockInputStream>(mutating_stream, data.getPrimaryKeyAndSkipIndicesExpression(metadata_snapshot)));

    if (need_remove_expired_values)
        mutating_stream = std::make_shared<TTLBlockInputStream>(mutating_stream, data, metadata_snapshot, new_data_part, time_of_mutation, true);

    IMergeTreeDataPart::MinMaxIndex minmax_idx;

    MergedBlockOutputStream out{
        new_data_part,
        metadata_snapshot,
        new_data_part->getColumns(),
        skip_indices,
        compression_codec};

    mutating_stream->readPrefix();
    out.writePrefix();

    writeWithProjections(
        new_data_part,
        metadata_snapshot,
        projections_to_build,
        mutating_stream,
        out,
        time_of_mutation,
        merge_entry,
        space_reservation,
        holder,
        context,
        &minmax_idx);

    new_data_part->minmax_idx = std::move(minmax_idx);
    mutating_stream->readSuffix();
    out.writeSuffixAndFinalizePart(new_data_part, need_sync);
}

void MergeTreeDataMergerMutator::mutateSomePartColumns(
    const MergeTreeDataPartPtr & source_part,
    const StorageMetadataPtr & metadata_snapshot,
    const std::set<MergeTreeIndexPtr> & indices_to_recalc,
    const std::set<MergeTreeProjectionPtr> & projections_to_recalc,
    const Block & mutation_header,
    MergeTreeData::MutableDataPartPtr new_data_part,
    BlockInputStreamPtr mutating_stream,
    time_t time_of_mutation,
    const CompressionCodecPtr & compression_codec,
    MergeListEntry & merge_entry,
    bool need_remove_expired_values,
    bool need_sync,
    const ReservationPtr & space_reservation,
    TableLockHolder & holder,
    ContextPtr context)
{
    if (mutating_stream == nullptr)
        throw Exception("Cannot mutate part columns with uninitialized mutations stream. It's a bug", ErrorCodes::LOGICAL_ERROR);

    if (need_remove_expired_values)
        mutating_stream = std::make_shared<TTLBlockInputStream>(mutating_stream, data, metadata_snapshot, new_data_part, time_of_mutation, true);

    IMergedBlockOutputStream::WrittenOffsetColumns unused_written_offsets;
    MergedColumnOnlyOutputStream out(
        new_data_part,
        metadata_snapshot,
        mutation_header,
        compression_codec,
        std::vector<MergeTreeIndexPtr>(indices_to_recalc.begin(), indices_to_recalc.end()),
        nullptr,
        source_part->index_granularity,
        &source_part->index_granularity_info
    );

    mutating_stream->readPrefix();
    out.writePrefix();

    std::vector<MergeTreeProjectionPtr> projections_to_build(projections_to_recalc.begin(), projections_to_recalc.end());
    writeWithProjections(
        new_data_part,
        metadata_snapshot,
        projections_to_build,
        mutating_stream,
        out,
        time_of_mutation,
        merge_entry,
        space_reservation,
        holder,
        context);

    mutating_stream->readSuffix();

    auto changed_checksums = out.writeSuffixAndGetChecksums(new_data_part, new_data_part->checksums, need_sync);

    new_data_part->checksums.add(std::move(changed_checksums));
}

void MergeTreeDataMergerMutator::finalizeMutatedPart(
    const MergeTreeDataPartPtr & source_part,
    MergeTreeData::MutableDataPartPtr new_data_part,
    bool need_remove_expired_values,
    const CompressionCodecPtr & codec)
{
    auto disk = new_data_part->volume->getDisk();

    if (new_data_part->uuid != UUIDHelpers::Nil)
    {
        auto out = disk->writeFile(new_data_part->getFullRelativePath() + IMergeTreeDataPart::UUID_FILE_NAME, 4096);
        HashingWriteBuffer out_hashing(*out);
        writeUUIDText(new_data_part->uuid, out_hashing);
        new_data_part->checksums.files[IMergeTreeDataPart::UUID_FILE_NAME].file_size = out_hashing.count();
        new_data_part->checksums.files[IMergeTreeDataPart::UUID_FILE_NAME].file_hash = out_hashing.getHash();
    }

    if (need_remove_expired_values)
    {
        /// Write a file with ttl infos in json format.
        auto out_ttl = disk->writeFile(new_data_part->getFullRelativePath() + "ttl.txt", 4096);
        HashingWriteBuffer out_hashing(*out_ttl);
        new_data_part->ttl_infos.write(out_hashing);
        new_data_part->checksums.files["ttl.txt"].file_size = out_hashing.count();
        new_data_part->checksums.files["ttl.txt"].file_hash = out_hashing.getHash();
    }

    {
        /// Write file with checksums.
        auto out_checksums = disk->writeFile(new_data_part->getFullRelativePath() + "checksums.txt", 4096);
        new_data_part->checksums.write(*out_checksums);
    } /// close fd

    {
        auto out = disk->writeFile(new_data_part->getFullRelativePath() + IMergeTreeDataPart::DEFAULT_COMPRESSION_CODEC_FILE_NAME, 4096);
        DB::writeText(queryToString(codec->getFullCodecDesc()), *out);
    }

    {
        /// Write a file with a description of columns.
        auto out_columns = disk->writeFile(new_data_part->getFullRelativePath() + "columns.txt", 4096);
        new_data_part->getColumns().writeText(*out_columns);
    } /// close fd

    new_data_part->rows_count = source_part->rows_count;
    new_data_part->index_granularity = source_part->index_granularity;
    new_data_part->index = source_part->index;
    new_data_part->minmax_idx = source_part->minmax_idx;
    new_data_part->modification_time = time(nullptr);
    new_data_part->loadProjections(false, false);
    new_data_part->setBytesOnDisk(
        MergeTreeData::DataPart::calculateTotalSizeOnDisk(new_data_part->volume->getDisk(), new_data_part->getFullRelativePath()));
    new_data_part->default_codec = codec;
    new_data_part->calculateColumnsSizesOnDisk();
    new_data_part->storage.lockSharedData(*new_data_part);
}

bool MergeTreeDataMergerMutator::checkOperationIsNotCanceled(const MergeListEntry & merge_entry) const
{
    if (merges_blocker.isCancelled() || merge_entry->is_cancelled)
        throw Exception("Cancelled mutating parts", ErrorCodes::ABORTED);

    return true;
}

}
