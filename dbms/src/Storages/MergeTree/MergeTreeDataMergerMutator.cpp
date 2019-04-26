#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <Storages/MergeTree/MergeTreeSequentialBlockInputStream.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/DiskSpaceMonitor.h>
#include <Storages/MergeTree/SimpleMergeSelector.h>
#include <Storages/MergeTree/AllMergeSelector.h>
#include <Storages/MergeTree/TTLMergeSelector.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/StorageFromMergeTreeDataPart.h>
#include <Storages/MergeTree/BackgroundProcessingPool.h>
#include <DataStreams/TTLBlockInputStream.h>
#include <DataStreams/DistinctSortedBlockInputStream.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/MergingSortedBlockInputStream.h>
#include <DataStreams/CollapsingSortedBlockInputStream.h>
#include <DataStreams/SummingSortedBlockInputStream.h>
#include <DataStreams/ReplacingSortedBlockInputStream.h>
#include <DataStreams/GraphiteRollupSortedBlockInputStream.h>
#include <DataStreams/AggregatingSortedBlockInputStream.h>
#include <DataStreams/VersionedCollapsingSortedBlockInputStream.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/ColumnGathererStream.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Common/SimpleIncrement.h>
#include <Common/interpolate.h>
#include <Common/typeid_cast.h>
#include <Common/localBackup.h>
#include <Common/createHardLink.h>

#include <Poco/File.h>
#include <Poco/DirectoryIterator.h>

#include <cmath>
#include <numeric>
#include <iomanip>


namespace ProfileEvents
{
    extern const Event MergedRows;
    extern const Event MergedUncompressedBytes;
    extern const Event MergesTimeMilliseconds;
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
    extern const int ABORTED;
}


using MergeAlgorithm = MergeTreeDataMergerMutator::MergeAlgorithm;


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
    for (const auto & part : parts)
        max_level = std::max(max_level, part->info.level);

    part_info.partition_id = parts.front()->info.partition_id;
    part_info.min_block = parts.front()->info.min_block;
    part_info.max_block = parts.back()->info.max_block;
    part_info.level = max_level + 1;
    part_info.mutation = parts.front()->info.mutation;

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

MergeTreeDataMergerMutator::MergeTreeDataMergerMutator(MergeTreeData & data_, const BackgroundProcessingPool & pool_)
    : data(data_), pool(pool_), log(&Logger::get(data.getLogName() + " (MergerMutator)"))
{
}


UInt64 MergeTreeDataMergerMutator::getMaxSourcePartsSize()
{
    size_t total_threads_in_pool = pool.getNumberOfThreads();
    size_t busy_threads_in_pool = CurrentMetrics::values[CurrentMetrics::BackgroundPoolTask].load(std::memory_order_relaxed);

    return getMaxSourcePartsSize(total_threads_in_pool, busy_threads_in_pool == 0 ? 0 : busy_threads_in_pool - 1); /// 1 is current thread
}


UInt64 MergeTreeDataMergerMutator::getMaxSourcePartsSize(size_t pool_size, size_t pool_used)
{
    if (pool_used > pool_size)
        throw Exception("Logical error: invalid arguments passed to getMaxSourcePartsSize: pool_used > pool_size", ErrorCodes::LOGICAL_ERROR);

    size_t free_entries = pool_size - pool_used;

    UInt64 max_size = 0;
    if (free_entries >= data.settings.number_of_free_entries_in_pool_to_lower_max_size_of_merge)
        max_size = data.settings.max_bytes_to_merge_at_max_space_in_pool;
    else
        max_size = interpolateExponential(
            data.settings.max_bytes_to_merge_at_min_space_in_pool,
            data.settings.max_bytes_to_merge_at_max_space_in_pool,
            static_cast<double>(free_entries) / data.settings.number_of_free_entries_in_pool_to_lower_max_size_of_merge);

    return std::min(max_size, static_cast<UInt64>(DiskSpaceMonitor::getUnreservedFreeSpace(data.full_path) / DISK_USAGE_COEFFICIENT_TO_SELECT));
}


bool MergeTreeDataMergerMutator::selectPartsToMerge(
    FutureMergedMutatedPart & future_part,
    bool aggressive,
    size_t max_total_size_to_merge,
    const AllowedMergingPredicate & can_merge_callback,
    String * out_disable_reason)
{
    MergeTreeData::DataPartsVector data_parts = data.getDataPartsVector();

    if (data_parts.empty())
    {
        if (out_disable_reason)
            *out_disable_reason = "There are no parts in the table";
        return false;
    }

    time_t current_time = time(nullptr);

    IMergeSelector::Partitions partitions;

    const String * prev_partition_id = nullptr;
    const MergeTreeData::DataPartPtr * prev_part = nullptr;
    bool has_part_with_expired_ttl = false;
    for (const MergeTreeData::DataPartPtr & part : data_parts)
    {
        const String & partition_id = part->info.partition_id;
        if (!prev_partition_id || partition_id != *prev_partition_id || (prev_part && !can_merge_callback(*prev_part, part, nullptr)))
        {
            if (partitions.empty() || !partitions.back().empty())
                partitions.emplace_back();
            prev_partition_id = &partition_id;
        }

        IMergeSelector::Part part_info;
        part_info.size = part->bytes_on_disk;
        part_info.age = current_time - part->modification_time;
        part_info.level = part->info.level;
        part_info.data = &part;
        part_info.min_ttl = part->ttl_infos.part_min_ttl;

        if (part_info.min_ttl && part_info.min_ttl <= current_time)
            has_part_with_expired_ttl = true;

        partitions.back().emplace_back(part_info);

        /// Check for consistency of data parts. If assertion is failed, it requires immediate investigation.
        if (prev_part && part->info.partition_id == (*prev_part)->info.partition_id
            && part->info.min_block <= (*prev_part)->info.max_block)
        {
            LOG_ERROR(log, "Part " << part->name << " intersects previous part " << (*prev_part)->name);
        }

        prev_part = &part;
    }

    std::unique_ptr<IMergeSelector> merge_selector;

    SimpleMergeSelector::Settings merge_settings;
    if (aggressive)
        merge_settings.base = 1;

    bool can_merge_with_ttl =
        (current_time - last_merge_with_ttl > data.settings.merge_with_ttl_timeout);

    /// NOTE Could allow selection of different merge strategy.
    if (can_merge_with_ttl && has_part_with_expired_ttl)
    {
        merge_selector = std::make_unique<TTLMergeSelector>(current_time);
        last_merge_with_ttl = current_time;
    }
    else
        merge_selector = std::make_unique<SimpleMergeSelector>(merge_settings);

    IMergeSelector::PartsInPartition parts_to_merge = merge_selector->select(
        partitions,
        max_total_size_to_merge);

    if (parts_to_merge.empty())
    {
        if (out_disable_reason)
            *out_disable_reason = "There are no need to merge parts according to merge selector algorithm";
        return false;
    }

    /// Allow to "merge" part with itself if we need remove some values with expired ttl
    if (parts_to_merge.size() == 1 && !has_part_with_expired_ttl)
        throw Exception("Logical error: merge selector returned only one part to merge", ErrorCodes::LOGICAL_ERROR);

    MergeTreeData::DataPartsVector parts;
    parts.reserve(parts_to_merge.size());
    for (IMergeSelector::Part & part_info : parts_to_merge)
    {
        const MergeTreeData::DataPartPtr & part = *static_cast<const MergeTreeData::DataPartPtr *>(part_info.data);
        parts.push_back(part);
    }

    LOG_DEBUG(log, "Selected " << parts.size() << " parts from " << parts.front()->name << " to " << parts.back()->name);
    future_part.assign(std::move(parts));
    return true;
}


bool MergeTreeDataMergerMutator::selectAllPartsToMergeWithinPartition(
    FutureMergedMutatedPart & future_part,
    UInt64 & available_disk_space,
    const AllowedMergingPredicate & can_merge,
    const String & partition_id,
    bool final,
    String * out_disable_reason)
{
    MergeTreeData::DataPartsVector parts = selectAllPartsFromPartition(partition_id);

    if (parts.empty())
        return false;

    if (!final && parts.size() == 1)
    {
        if (out_disable_reason)
            *out_disable_reason = "There is only one part inside partition";
        return false;
    }

    auto it = parts.begin();
    auto prev_it = it;

    UInt64 sum_bytes = 0;
    while (it != parts.end())
    {
        /// For the case of one part, we check that it can be merged "with itself".
        if ((it != parts.begin() || parts.size() == 1) && !can_merge(*prev_it, *it, out_disable_reason))
        {
            return false;
        }

        sum_bytes += (*it)->bytes_on_disk;

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
            LOG_WARNING(log, "Won't merge parts from " << parts.front()->name << " to " << (*prev_it)->name
                << " because not enough free space: "
                << formatReadableSizeWithBinarySuffix(available_disk_space) << " free and unreserved "
                << "(" << formatReadableSizeWithBinarySuffix(DiskSpaceMonitor::getReservedSpace()) << " reserved in "
                << DiskSpaceMonitor::getReservationCount() << " chunks), "
                << formatReadableSizeWithBinarySuffix(sum_bytes)
                << " required now (+" << static_cast<int>((DISK_USAGE_COEFFICIENT_TO_SELECT - 1.0) * 100)
                << "% on overhead); suppressing similar warnings for the next hour");
        }

        if (out_disable_reason)
            *out_disable_reason = "Insufficient available disk space, required " +
                formatReadableSizeWithDecimalSuffix(required_disk_space);

        return false;
    }

    LOG_DEBUG(log, "Selected " << parts.size() << " parts from " << parts.front()->name << " to " << parts.back()->name);
    future_part.assign(std::move(parts));
    available_disk_space -= required_disk_space;
    return true;
}


MergeTreeData::DataPartsVector MergeTreeDataMergerMutator::selectAllPartsFromPartition(const String & partition_id)
{
    MergeTreeData::DataPartsVector parts_from_partition;

    MergeTreeData::DataParts data_parts = data.getDataParts();

    for (MergeTreeData::DataParts::iterator it = data_parts.cbegin(); it != data_parts.cend(); ++it)
    {
        const MergeTreeData::DataPartPtr & current_part = *it;
        if (current_part->info.partition_id != partition_id)
            continue;

        parts_from_partition.push_back(current_part);
    }

    return parts_from_partition;
}


/// PK columns are sorted and merged, ordinary columns are gathered using info from merge step
static void extractMergingAndGatheringColumns(
    const NamesAndTypesList & all_columns,
    const ExpressionActionsPtr & sorting_key_expr,
    const MergeTreeIndices & indexes,
    const MergeTreeData::MergingParams & merging_params,
    NamesAndTypesList & gathering_columns, Names & gathering_column_names,
    NamesAndTypesList & merging_columns, Names & merging_column_names)
{
    Names sort_key_columns_vec = sorting_key_expr->getRequiredColumns();
    std::set<String> key_columns(sort_key_columns_vec.cbegin(), sort_key_columns_vec.cend());
    for (const auto & index : indexes)
    {
        Names index_columns_vec = index->expr->getRequiredColumns();
        std::copy(index_columns_vec.cbegin(), index_columns_vec.cend(),
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
        key_columns.emplace(all_columns.front().name);

    /// TODO: also force "summing" and "aggregating" columns to make Horizontal merge only for such columns

    for (const auto & column : all_columns)
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

/// Auxilliary struct that for each merge stage stores its current progress.
/// A stage is: the horizontal stage + a stage for each gathered column (if we are doing a
/// Vertical merge) or a mutation of a single part. During a single stage all rows are read.
struct MergeStageProgress
{
    MergeStageProgress(Float64 weight_)
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
        ProfileEvents::increment(ProfileEvents::MergedUncompressedBytes, value.bytes);
        if (stage.is_first)
            ProfileEvents::increment(ProfileEvents::MergedRows, value.rows);
        updateWatch();

        merge_entry->bytes_read_uncompressed += value.bytes;
        if (stage.is_first)
            merge_entry->rows_read += value.rows;

        stage.total_rows += value.total_rows;
        stage.rows_read += value.rows;
        if (stage.total_rows > 0)
        {
            merge_entry->progress.store(
                stage.initial_progress + stage.weight * stage.rows_read / stage.total_rows,
                std::memory_order_relaxed);
        }
    }
};

/// parts should be sorted.
MergeTreeData::MutableDataPartPtr MergeTreeDataMergerMutator::mergePartsToTemporaryPart(
    const FutureMergedMutatedPart & future_part, MergeList::Entry & merge_entry,
    time_t time_of_merge, DiskSpaceMonitor::Reservation * disk_reservation, bool deduplicate)
{
    static const String TMP_PREFIX = "tmp_merge_";

    if (actions_blocker.isCancelled())
        throw Exception("Cancelled merging parts", ErrorCodes::ABORTED);

    const MergeTreeData::DataPartsVector & parts = future_part.parts;

    LOG_DEBUG(log, "Merging " << parts.size() << " parts: from "
              << parts.front()->name << " to " << parts.back()->name
              << " into " << TMP_PREFIX + future_part.name);

    String new_part_tmp_path = data.getFullPath() + TMP_PREFIX + future_part.name + "/";
    if (Poco::File(new_part_tmp_path).exists())
        throw Exception("Directory " + new_part_tmp_path + " already exists", ErrorCodes::DIRECTORY_ALREADY_EXISTS);

    MergeTreeData::DataPart::ColumnToSize merged_column_to_size;
    for (const MergeTreeData::DataPartPtr & part : parts)
        part->accumulateColumnSizes(merged_column_to_size);

    Names all_column_names = data.getColumns().getNamesOfPhysical();
    NamesAndTypesList all_columns = data.getColumns().getAllPhysical();

    NamesAndTypesList gathering_columns, merging_columns;
    Names gathering_column_names, merging_column_names;
    extractMergingAndGatheringColumns(
        all_columns, data.sorting_key_expr, data.skip_indices,
        data.merging_params, gathering_columns, gathering_column_names, merging_columns, merging_column_names);

    MergeTreeData::MutableDataPartPtr new_data_part = std::make_shared<MergeTreeData::DataPart>(
            data, future_part.name, future_part.part_info);
    new_data_part->partition.assign(future_part.getPartition());
    new_data_part->relative_path = TMP_PREFIX + future_part.name;
    new_data_part->is_temp = true;

    size_t sum_input_rows_upper_bound = merge_entry->total_rows_count;

    bool need_remove_expired_values = false;
    for (const MergeTreeData::DataPartPtr & part : parts)
        new_data_part->ttl_infos.update(part->ttl_infos);

    const auto & part_min_ttl = new_data_part->ttl_infos.part_min_ttl;
    if (part_min_ttl && part_min_ttl <= time_of_merge)
        need_remove_expired_values = true;


    MergeAlgorithm merge_alg = chooseMergeAlgorithm(parts, sum_input_rows_upper_bound, gathering_columns, deduplicate, need_remove_expired_values);

    LOG_DEBUG(log, "Selected MergeAlgorithm: " << ((merge_alg == MergeAlgorithm::Vertical) ? "Vertical" : "Horizontal"));

    /// Note: this is done before creating input streams, because otherwise data.data_parts_mutex
    /// (which is locked in data.getTotalActiveSizeInBytes()) is locked after part->columns_lock
    /// (which is locked in shared mode when input streams are created) and when inserting new data
    /// the order is reverse. This annoys TSan even though one lock is locked in shared mode and thus
    /// deadlock is impossible.
    auto compression_codec = data.global_context.chooseCompressionCodec(
        merge_entry->total_size_bytes_compressed,
        static_cast<double> (merge_entry->total_size_bytes_compressed) / data.getTotalActiveSizeInBytes());

    String rows_sources_file_path;
    std::unique_ptr<WriteBuffer> rows_sources_uncompressed_write_buf;
    std::unique_ptr<WriteBuffer> rows_sources_write_buf;

    if (merge_alg == MergeAlgorithm::Vertical)
    {
        Poco::File(new_part_tmp_path).createDirectories();
        rows_sources_file_path = new_part_tmp_path + "rows_sources";
        rows_sources_uncompressed_write_buf = std::make_unique<WriteBufferFromFile>(rows_sources_file_path);
        rows_sources_write_buf = std::make_unique<CompressedWriteBuffer>(*rows_sources_uncompressed_write_buf);
    }
    else
    {
        merging_columns = all_columns;
        merging_column_names = all_column_names;
        gathering_columns.clear();
        gathering_column_names.clear();
    }

    ColumnSizeEstimator column_sizes(merged_column_to_size, merging_column_names, gathering_column_names);

    /** Read from all parts, merge and write into a new one.
      * In passing, we calculate expression for sorting.
      */
    BlockInputStreams src_streams;
    UInt64 watch_prev_elapsed = 0;

    /// We count total amount of bytes in parts
    /// and use direct_io + aio if there is more than min_merge_bytes_to_use_direct_io
    bool read_with_direct_io = false;
    if (data.settings.min_merge_bytes_to_use_direct_io != 0)
    {
        size_t total_size = 0;
        for (const auto & part : parts)
        {
            total_size += part->bytes_on_disk;
            if (total_size >= data.settings.min_merge_bytes_to_use_direct_io)
            {
                LOG_DEBUG(log, "Will merge parts reading files in O_DIRECT");
                read_with_direct_io = true;

                break;
            }
        }
    }

    MergeStageProgress horizontal_stage_progress(
        merge_alg == MergeAlgorithm::Horizontal ? 1.0 : column_sizes.keyColumnsWeight());

    for (const auto & part : parts)
    {
        auto input = std::make_unique<MergeTreeSequentialBlockInputStream>(
            data, part, merging_column_names, read_with_direct_io, true);

        input->setProgressCallback(
            MergeProgressCallback(merge_entry, watch_prev_elapsed, horizontal_stage_progress));

        BlockInputStreamPtr stream = std::move(input);
        if (data.hasPrimaryKey() || data.hasSkipIndices())
            stream = std::make_shared<MaterializingBlockInputStream>(
                    std::make_shared<ExpressionBlockInputStream>(stream, data.sorting_key_and_skip_indices_expr));

        src_streams.emplace_back(stream);
    }

    Names sort_columns = data.sorting_key_columns;
    SortDescription sort_description;
    size_t sort_columns_size = sort_columns.size();
    sort_description.reserve(sort_columns_size);

    Block header = src_streams.at(0)->getHeader();
    for (size_t i = 0; i < sort_columns_size; ++i)
        sort_description.emplace_back(header.getPositionByName(sort_columns[i]), 1, 1);

    /// The order of the streams is important: when the key is matched, the elements go in the order of the source stream number.
    /// In the merged part, the lines with the same key must be in the ascending order of the identifier of original part,
    ///  that is going in insertion order.
    std::shared_ptr<IBlockInputStream> merged_stream;

    /// If merge is vertical we cannot calculate it
    bool blocks_are_granules_size = (merge_alg == MergeAlgorithm::Vertical);

    switch (data.merging_params.mode)
    {
        case MergeTreeData::MergingParams::Ordinary:
            merged_stream = std::make_unique<MergingSortedBlockInputStream>(
                src_streams, sort_description, DEFAULT_MERGE_BLOCK_SIZE, 0, rows_sources_write_buf.get(), true, blocks_are_granules_size);
            break;

        case MergeTreeData::MergingParams::Collapsing:
            merged_stream = std::make_unique<CollapsingSortedBlockInputStream>(
                src_streams, sort_description, data.merging_params.sign_column, DEFAULT_MERGE_BLOCK_SIZE, rows_sources_write_buf.get(), blocks_are_granules_size);
            break;

        case MergeTreeData::MergingParams::Summing:
            merged_stream = std::make_unique<SummingSortedBlockInputStream>(
                src_streams, sort_description, data.merging_params.columns_to_sum, DEFAULT_MERGE_BLOCK_SIZE);
            break;

        case MergeTreeData::MergingParams::Aggregating:
            merged_stream = std::make_unique<AggregatingSortedBlockInputStream>(
                src_streams, sort_description, DEFAULT_MERGE_BLOCK_SIZE);
            break;

        case MergeTreeData::MergingParams::Replacing:
            merged_stream = std::make_unique<ReplacingSortedBlockInputStream>(
                src_streams, sort_description, data.merging_params.version_column, DEFAULT_MERGE_BLOCK_SIZE, rows_sources_write_buf.get(), blocks_are_granules_size);
            break;

        case MergeTreeData::MergingParams::Graphite:
            merged_stream = std::make_unique<GraphiteRollupSortedBlockInputStream>(
                src_streams, sort_description, DEFAULT_MERGE_BLOCK_SIZE,
                data.merging_params.graphite_params, time_of_merge);
            break;

        case MergeTreeData::MergingParams::VersionedCollapsing:
            merged_stream = std::make_unique<VersionedCollapsingSortedBlockInputStream>(
                src_streams, sort_description, data.merging_params.sign_column, DEFAULT_MERGE_BLOCK_SIZE, rows_sources_write_buf.get(), blocks_are_granules_size);
            break;
    }

    if (deduplicate)
        merged_stream = std::make_shared<DistinctSortedBlockInputStream>(merged_stream, SizeLimits(), 0 /*limit_hint*/, Names());

    if (need_remove_expired_values)
        merged_stream = std::make_shared<TTLBlockInputStream>(merged_stream, data, new_data_part, time_of_merge);

    MergedBlockOutputStream to{
        data,
        new_part_tmp_path,
        merging_columns,
        compression_codec,
        merged_column_to_size,
        data.settings.min_merge_bytes_to_use_direct_io,
        blocks_are_granules_size};

    merged_stream->readPrefix();
    to.writePrefix();

    size_t rows_written = 0;
    const size_t initial_reservation = disk_reservation ? disk_reservation->getSize() : 0;

    Block block;
    while (!actions_blocker.isCancelled() && (block = merged_stream->read()))
    {
        rows_written += block.rows();

        to.write(block);

        merge_entry->rows_written = merged_stream->getProfileInfo().rows;
        merge_entry->bytes_written_uncompressed = merged_stream->getProfileInfo().bytes;

        /// Reservation updates is not performed yet, during the merge it may lead to higher free space requirements
        if (disk_reservation && sum_input_rows_upper_bound)
        {
            /// The same progress from merge_entry could be used for both algorithms (it should be more accurate)
            /// But now we are using inaccurate row-based estimation in Horizontal case for backward compatibility
            Float64 progress = (merge_alg == MergeAlgorithm::Horizontal)
                ? std::min(1., 1. * rows_written / sum_input_rows_upper_bound)
                : std::min(1., merge_entry->progress.load(std::memory_order_relaxed));

            disk_reservation->update(static_cast<size_t>((1. - progress) * initial_reservation));
        }
    }
    merged_stream->readSuffix();
    merged_stream.reset();

    if (actions_blocker.isCancelled())
        throw Exception("Cancelled merging parts", ErrorCodes::ABORTED);

    MergeTreeData::DataPart::Checksums checksums_gathered_columns;

    /// Gather ordinary columns
    if (merge_alg == MergeAlgorithm::Vertical)
    {
        size_t sum_input_rows_exact = merge_entry->rows_read;
        merge_entry->columns_written = merging_column_names.size();
        merge_entry->progress.store(column_sizes.keyColumnsWeight(), std::memory_order_relaxed);

        BlockInputStreams column_part_streams(parts.size());

        auto it_name_and_type = gathering_columns.cbegin();

        rows_sources_write_buf->next();
        rows_sources_uncompressed_write_buf->next();

        size_t rows_sources_count = rows_sources_write_buf->count();
        /// In special case, when there is only one source part, and no rows were skipped, we may have
        /// skipped writing rows_sources file. Otherwise rows_sources_count must be equal to the total
        /// number of input rows.
        if ((rows_sources_count > 0 || parts.size() > 1) && sum_input_rows_exact != rows_sources_count)
            throw Exception("Number of rows in source parts (" + toString(sum_input_rows_exact)
                + ") differs from number of bytes written to rows_sources file (" + toString(rows_sources_count)
                + "). It is a bug.", ErrorCodes::LOGICAL_ERROR);

        CompressedReadBufferFromFile rows_sources_read_buf(rows_sources_file_path, 0, 0);
        IMergedBlockOutputStream::WrittenOffsetColumns written_offset_columns;

        for (size_t column_num = 0, gathering_column_names_size = gathering_column_names.size();
            column_num < gathering_column_names_size;
            ++column_num, ++it_name_and_type)
        {
            const String & column_name = it_name_and_type->name;
            Names column_names{column_name};
            Float64 progress_before = merge_entry->progress.load(std::memory_order_relaxed);

            MergeStageProgress column_progress(progress_before, column_sizes.columnWeight(column_name));
            for (size_t part_num = 0; part_num < parts.size(); ++part_num)
            {
                auto column_part_stream = std::make_shared<MergeTreeSequentialBlockInputStream>(
                    data, parts[part_num], column_names, read_with_direct_io, true);

                column_part_stream->setProgressCallback(
                    MergeProgressCallback(merge_entry, watch_prev_elapsed, column_progress));

                column_part_streams[part_num] = std::move(column_part_stream);
            }

            rows_sources_read_buf.seek(0, 0);
            ColumnGathererStream column_gathered_stream(column_name, column_part_streams, rows_sources_read_buf);
            MergedColumnOnlyOutputStream column_to(
                data,
                column_gathered_stream.getHeader(),
                new_part_tmp_path,
                false,
                compression_codec,
                false,
                written_offset_columns,
                to.getIndexGranularity()
            );
            size_t column_elems_written = 0;

            column_to.writePrefix();
            while (!actions_blocker.isCancelled() && (block = column_gathered_stream.read()))
            {
                column_elems_written += block.rows();
                column_to.write(block);
            }

            if (actions_blocker.isCancelled())
                throw Exception("Cancelled merging parts", ErrorCodes::ABORTED);

            column_gathered_stream.readSuffix();
            checksums_gathered_columns.add(column_to.writeSuffixAndGetChecksums());

            if (rows_written != column_elems_written)
            {
                throw Exception("Written " + toString(column_elems_written) + " elements of column " + column_name +
                                ", but " + toString(rows_written) + " rows of PK columns", ErrorCodes::LOGICAL_ERROR);
            }

            /// NOTE: 'progress' is modified by single thread, but it may be concurrently read from MergeListElement::getInfo() (StorageSystemMerges).

            merge_entry->columns_written += 1;
            merge_entry->bytes_written_uncompressed += column_gathered_stream.getProfileInfo().bytes;
            merge_entry->progress.store(progress_before + column_sizes.columnWeight(column_name), std::memory_order_relaxed);
        }

        Poco::File(rows_sources_file_path).remove();
    }

    for (const auto & part : parts)
        new_data_part->minmax_idx.merge(part->minmax_idx);

    /// Print overall profiling info. NOTE: it may duplicates previous messages
    {
        double elapsed_seconds = merge_entry->watch.elapsedSeconds();
        LOG_DEBUG(log, std::fixed << std::setprecision(2)
            << "Merge sorted " << merge_entry->rows_read << " rows"
            << ", containing " << all_column_names.size() << " columns"
            << " (" << merging_column_names.size() << " merged, " << gathering_column_names.size() << " gathered)"
            << " in " << elapsed_seconds << " sec., "
            << merge_entry->rows_read / elapsed_seconds << " rows/sec., "
            << merge_entry->bytes_read_uncompressed / 1000000.0 / elapsed_seconds << " MB/sec.");
    }

    if (merge_alg != MergeAlgorithm::Vertical)
        to.writeSuffixAndFinalizePart(new_data_part);
    else
        to.writeSuffixAndFinalizePart(new_data_part, &all_columns, &checksums_gathered_columns);

    return new_data_part;
}


MergeTreeData::MutableDataPartPtr MergeTreeDataMergerMutator::mutatePartToTemporaryPart(
    const FutureMergedMutatedPart & future_part,
    const std::vector<MutationCommand> & commands,
    MergeListEntry & merge_entry,
    const Context & context)
{
    auto check_not_cancelled = [&]()
    {
        if (actions_blocker.isCancelled() || merge_entry->is_cancelled)
            throw Exception("Cancelled mutating parts", ErrorCodes::ABORTED);

        return true;
    };

    check_not_cancelled();

    if (future_part.parts.size() != 1)
        throw Exception("Trying to mutate " + toString(future_part.parts.size()) + " parts, not one. "
            "This is a bug.", ErrorCodes::LOGICAL_ERROR);

    CurrentMetrics::Increment num_mutations{CurrentMetrics::PartMutation};

    const auto & source_part = future_part.parts[0];
    auto storage_from_source_part = StorageFromMergeTreeDataPart::create(source_part);

    auto context_for_reading = context;
    context_for_reading.getSettingsRef().merge_tree_uniform_read_distribution = 0;
    context_for_reading.getSettingsRef().max_threads = 1;

    MutationsInterpreter mutations_interpreter(storage_from_source_part, commands, context_for_reading);

    if (!mutations_interpreter.isStorageTouchedByMutations())
    {
        LOG_TRACE(log, "Part " << source_part->name << " doesn't change up to mutation version " << future_part.part_info.mutation);
        return data.cloneAndLoadDataPart(source_part, "tmp_clone_", future_part.part_info);
    }
    else
        LOG_TRACE(log, "Mutating part " << source_part->name << " to mutation version " << future_part.part_info.mutation);

    MergeTreeData::MutableDataPartPtr new_data_part = std::make_shared<MergeTreeData::DataPart>(
        data, future_part.name, future_part.part_info);
    new_data_part->relative_path = "tmp_mut_" + future_part.name;
    new_data_part->is_temp = true;
    new_data_part->ttl_infos = source_part->ttl_infos;

    String new_part_tmp_path = new_data_part->getFullPath();

    /// Note: this is done before creating input streams, because otherwise data.data_parts_mutex
    /// (which is locked in data.getTotalActiveSizeInBytes()) is locked after part->columns_lock
    /// (which is locked in shared mode when input streams are created) and when inserting new data
    /// the order is reverse. This annoys TSan even though one lock is locked in shared mode and thus
    /// deadlock is impossible.
    auto compression_codec = context.chooseCompressionCodec(
        source_part->bytes_on_disk,
        static_cast<double>(source_part->bytes_on_disk) / data.getTotalActiveSizeInBytes());

    Poco::File(new_part_tmp_path).createDirectories();

    auto in = mutations_interpreter.execute();
    NamesAndTypesList all_columns = data.getColumns().getAllPhysical();

    Block in_header = in->getHeader();

    UInt64 watch_prev_elapsed = 0;
    MergeStageProgress stage_progress(1.0);
    in->setProgressCallback(MergeProgressCallback(merge_entry, watch_prev_elapsed, stage_progress));

    if (in_header.columns() == all_columns.size())
    {
        /// All columns are modified, proceed to write a new part from scratch.
        if (data.hasPrimaryKey() || data.hasSkipIndices())
            in = std::make_shared<MaterializingBlockInputStream>(
                std::make_shared<ExpressionBlockInputStream>(in, data.primary_key_and_skip_indices_expr));

        MergeTreeDataPart::MinMaxIndex minmax_idx;

        MergedBlockOutputStream out(data, new_part_tmp_path, all_columns, compression_codec);

        in->readPrefix();
        out.writePrefix();

        Block block;
        while (check_not_cancelled() && (block = in->read()))
        {
            minmax_idx.update(block, data.minmax_idx_columns);
            out.write(block);

            merge_entry->rows_written += block.rows();
            merge_entry->bytes_written_uncompressed += block.bytes();
        }

        new_data_part->partition.assign(source_part->partition);
        new_data_part->minmax_idx = std::move(minmax_idx);

        in->readSuffix();
        out.writeSuffixAndFinalizePart(new_data_part);
    }
    else
    {
        /// We will modify only some of the columns. Other columns and key values can be copied as-is.
        /// TODO: check that we modify only non-key columns in this case.

        /// Checks if columns used in skipping indexes modified/
        for (const auto & col : in_header.getNames())
        {
            for (const auto & index : data.skip_indices)
            {
                const auto & index_cols = index->expr->getRequiredColumns();
                auto it = find(cbegin(index_cols), cend(index_cols), col);
                if (it != cend(index_cols))
                    throw Exception("You can not modify columns used in index. Index name: '"
                                    + index->name
                                    + "' bad column: '" + *it + "'", ErrorCodes::ILLEGAL_COLUMN);
            }
        }

        NameSet files_to_skip = {"checksums.txt", "columns.txt"};
        for (const auto & entry : in_header)
        {
            IDataType::StreamCallback callback = [&](const IDataType::SubstreamPath & substream_path)
            {
                String stream_name = IDataType::getFileNameForStream(entry.name, substream_path);
                files_to_skip.insert(stream_name + ".bin");
                files_to_skip.insert(stream_name + data.index_granularity_info.marks_file_extension);
            };

            IDataType::SubstreamPath stream_path;
            entry.type->enumerateStreams(callback, stream_path);
        }

        Poco::DirectoryIterator dir_end;
        for (Poco::DirectoryIterator dir_it(source_part->getFullPath()); dir_it != dir_end; ++dir_it)
        {
            if (files_to_skip.count(dir_it.name()))
                continue;

            Poco::Path destination(new_part_tmp_path);
            destination.append(dir_it.name());

            createHardLink(dir_it.path().toString(), destination.toString());
        }

        merge_entry->columns_written = all_columns.size() - in_header.columns();

        IMergedBlockOutputStream::WrittenOffsetColumns unused_written_offsets;
        MergedColumnOnlyOutputStream out(
            data,
            in_header,
            new_part_tmp_path,
            /* sync = */ false,
            compression_codec,
            /* skip_offsets = */ false,
            unused_written_offsets,
            source_part->index_granularity
        );

        in->readPrefix();
        out.writePrefix();

        Block block;
        while (check_not_cancelled() && (block = in->read()))
        {
            out.write(block);

            merge_entry->rows_written += block.rows();
            merge_entry->bytes_written_uncompressed += block.bytes();
        }

        in->readSuffix();
        auto changed_checksums = out.writeSuffixAndGetChecksums();

        new_data_part->checksums = source_part->checksums;
        new_data_part->checksums.add(std::move(changed_checksums));
        {
            /// Write file with checksums.
            WriteBufferFromFile out_checksums(new_part_tmp_path + "checksums.txt", 4096);
            new_data_part->checksums.write(out_checksums);
        }

        /// Write the columns list of the resulting part in the same order as all_columns.
        new_data_part->columns = all_columns;
        Names source_column_names = source_part->columns.getNames();
        NameSet source_columns_name_set(source_column_names.begin(), source_column_names.end());
        for (auto it = new_data_part->columns.begin(); it != new_data_part->columns.end();)
        {
            if (source_columns_name_set.count(it->name) || in_header.has(it->name))
                ++it;
            else
                it = new_data_part->columns.erase(it);
        }
        {
            /// Write a file with a description of columns.
            WriteBufferFromFile out_columns(new_part_tmp_path + "columns.txt", 4096);
            new_data_part->columns.writeText(out_columns);
        }

        new_data_part->rows_count = source_part->rows_count;
        new_data_part->index_granularity = source_part->index_granularity;
        new_data_part->index = source_part->index;
        new_data_part->partition.assign(source_part->partition);
        new_data_part->minmax_idx = source_part->minmax_idx;
        new_data_part->modification_time = time(nullptr);
        new_data_part->bytes_on_disk = MergeTreeData::DataPart::calculateTotalSizeOnDisk(new_data_part->getFullPath());
    }

    return new_data_part;
}


MergeTreeDataMergerMutator::MergeAlgorithm MergeTreeDataMergerMutator::chooseMergeAlgorithm(
    const MergeTreeData::DataPartsVector & parts, size_t sum_rows_upper_bound,
    const NamesAndTypesList & gathering_columns, bool deduplicate, bool need_remove_expired_values) const
{
    if (deduplicate)
        return MergeAlgorithm::Horizontal;
    if (data.settings.enable_vertical_merge_algorithm == 0)
        return MergeAlgorithm::Horizontal;
    if (need_remove_expired_values)
        return MergeAlgorithm::Horizontal;

    bool is_supported_storage =
        data.merging_params.mode == MergeTreeData::MergingParams::Ordinary ||
        data.merging_params.mode == MergeTreeData::MergingParams::Collapsing ||
        data.merging_params.mode == MergeTreeData::MergingParams::Replacing ||
        data.merging_params.mode == MergeTreeData::MergingParams::VersionedCollapsing;

    bool enough_ordinary_cols = gathering_columns.size() >= data.settings.vertical_merge_algorithm_min_columns_to_activate;

    bool enough_total_rows = sum_rows_upper_bound >= data.settings.vertical_merge_algorithm_min_rows_to_activate;

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
        LOG_WARNING(log, "Unexpected number of parts removed when adding " << new_data_part->name << ": " << replaced_parts.size()
            << " instead of " << parts.size());
    }
    else
    {
        for (size_t i = 0; i < parts.size(); ++i)
            if (parts[i]->name != replaced_parts[i]->name)
                throw Exception("Unexpected part removed when adding " + new_data_part->name + ": " + replaced_parts[i]->name
                    + " instead of " + parts[i]->name, ErrorCodes::LOGICAL_ERROR);
    }

    LOG_TRACE(log, "Merged " << parts.size() << " parts: from " << parts.front()->name << " to " << parts.back()->name);
    return new_data_part;
}

size_t MergeTreeDataMergerMutator::estimateNeededDiskSpace(const MergeTreeData::DataPartsVector & source_parts)
{
    size_t res = 0;
    for (const MergeTreeData::DataPartPtr & part : source_parts)
        res += part->bytes_on_disk;

    return static_cast<size_t>(res * DISK_USAGE_COEFFICIENT_TO_RESERVE);
}

}
