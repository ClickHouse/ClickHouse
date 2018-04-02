#include <Storages/MergeTree/MergeTreeDataMerger.h>
#include <Storages/MergeTree/MergeTreeBlockInputStream.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/DiskSpaceMonitor.h>
#include <Storages/MergeTree/SimpleMergeSelector.h>
#include <Storages/MergeTree/AllMergeSelector.h>
#include <Storages/MergeTree/MergeList.h>
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
#include <IO/CompressedWriteBuffer.h>
#include <IO/CompressedReadBufferFromFile.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeArray.h>
#include <Storages/MergeTree/BackgroundProcessingPool.h>
#include <Common/SimpleIncrement.h>
#include <Common/interpolate.h>
#include <Common/typeid_cast.h>

#include <Poco/File.h>

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
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
}


using MergeAlgorithm = MergeTreeDataMerger::MergeAlgorithm;


/// Do not start to merge parts, if free space is less than sum size of parts times specified coefficient.
/// This value is chosen to not allow big merges to eat all free space. Thus allowing small merges to proceed.
static const double DISK_USAGE_COEFFICIENT_TO_SELECT = 2;

/// To do merge, reserve amount of space equals to sum size of parts times specified coefficient.
/// Must be strictly less than DISK_USAGE_COEFFICIENT_TO_SELECT,
///  because between selecting parts to merge and doing merge, amount of free space could have decreased.
static const double DISK_USAGE_COEFFICIENT_TO_RESERVE = 1.1;


void MergeTreeDataMerger::FuturePart::assign(MergeTreeData::DataPartsVector parts_)
{
    if (parts_.empty())
        return;

    for (size_t i = 0; i < parts_.size(); ++i)
    {
        if (parts_[i]->partition.value != parts_[0]->partition.value)
            throw Exception(
                "Attempting to merge parts " + parts_[i]->name + " and " + parts_[0]->name + " that are in different partitions",
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

    if (parts.front()->storage.format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        DayNum_t min_date = DayNum_t(std::numeric_limits<UInt16>::max());
        DayNum_t max_date = DayNum_t(std::numeric_limits<UInt16>::min());
        for (const auto & part : parts)
        {
            min_date = std::min(min_date, part->getMinDate());
            max_date = std::max(max_date, part->getMaxDate());
        }

        name = part_info.getPartNameV0(min_date, max_date);
    }
    else
        name = part_info.getPartName();
}

MergeTreeDataMerger::MergeTreeDataMerger(MergeTreeData & data_, const BackgroundProcessingPool & pool_)
    : data(data_), pool(pool_), log(&Logger::get(data.getLogName() + " (Merger)"))
{
}

void MergeTreeDataMerger::setCancellationHook(CancellationHook cancellation_hook_)
{
    cancellation_hook = cancellation_hook_;
}


size_t MergeTreeDataMerger::getMaxPartsSizeForMerge()
{
    size_t total_threads_in_pool = pool.getNumberOfThreads();
    size_t busy_threads_in_pool = CurrentMetrics::values[CurrentMetrics::BackgroundPoolTask].load(std::memory_order_relaxed);

    return getMaxPartsSizeForMerge(total_threads_in_pool, busy_threads_in_pool == 0 ? 0 : busy_threads_in_pool - 1); /// 1 is current thread
}


size_t MergeTreeDataMerger::getMaxPartsSizeForMerge(size_t pool_size, size_t pool_used)
{
    if (pool_used > pool_size)
        throw Exception("Logical error: invalid arguments passed to getMaxPartsSizeForMerge: pool_used > pool_size", ErrorCodes::LOGICAL_ERROR);

    size_t free_entries = pool_size - pool_used;

    size_t max_size = 0;
    if (free_entries >= data.settings.number_of_free_entries_in_pool_to_lower_max_size_of_merge)
        max_size = data.settings.max_bytes_to_merge_at_max_space_in_pool;
    else
        max_size = interpolateExponential(
            data.settings.max_bytes_to_merge_at_min_space_in_pool,
            data.settings.max_bytes_to_merge_at_max_space_in_pool,
            static_cast<double>(free_entries) / data.settings.number_of_free_entries_in_pool_to_lower_max_size_of_merge);

    return std::min(max_size, static_cast<size_t>(DiskSpaceMonitor::getUnreservedFreeSpace(data.full_path) / DISK_USAGE_COEFFICIENT_TO_SELECT));
}


bool MergeTreeDataMerger::selectPartsToMerge(
    FuturePart & future_part,
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

    /// NOTE Could allow selection of different merge strategy.
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

    if (parts_to_merge.size() == 1)
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


bool MergeTreeDataMerger::selectAllPartsToMergeWithinPartition(
    FuturePart & future_part,
    size_t available_disk_space,
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

    size_t sum_bytes = 0;
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
    return true;
}


MergeTreeData::DataPartsVector MergeTreeDataMerger::selectAllPartsFromPartition(const String & partition_id)
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
static void extractMergingAndGatheringColumns(const NamesAndTypesList & all_columns,
    const ExpressionActionsPtr & primary_key_expressions, const ExpressionActionsPtr & secondary_key_expressions,
    const MergeTreeData::MergingParams & merging_params,
    NamesAndTypesList & gathering_columns, Names & gathering_column_names,
    NamesAndTypesList & merging_columns, Names & merging_column_names
)
{
    Names primary_key_columns_vec = primary_key_expressions->getRequiredColumns();
    std::set<String> key_columns(primary_key_columns_vec.cbegin(), primary_key_columns_vec.cend());
    if (secondary_key_expressions)
    {
        Names secondary_key_columns_vec = secondary_key_expressions->getRequiredColumns();
        key_columns.insert(secondary_key_columns_vec.begin(), secondary_key_columns_vec.end());
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

    for (auto & column : all_columns)
    {
        auto it = std::find(key_columns.cbegin(), key_columns.cend(), column.name);

        if (key_columns.end() == it)
        {
            gathering_columns.emplace_back(column);
            gathering_column_names.emplace_back(column.name);
        }
        else
        {
            merging_columns.emplace_back(column);
            merging_column_names.emplace_back(column.name);
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

    /// Approximate size of num_rows column elements if column contains num_total_rows elements
    Float64 columnSize(const String & column, size_t num_rows, size_t num_total_rows) const
    {
        return static_cast<Float64>(map.at(column)) / num_total_rows * num_rows;
    }

    /// Relative size of num_rows column elements (in comparison with overall size of all columns) if column contains num_total_rows elements
    Float64 columnProgress(const String & column, size_t num_rows, size_t num_total_rows) const
    {
        return columnSize(column, num_rows, num_total_rows) / sum_total;
    }

    /// Like columnSize, but takes into account only PK columns
    Float64 keyColumnsSize(size_t num_rows, size_t num_total_rows) const
    {
        return static_cast<Float64>(sum_index_columns) / num_total_rows * num_rows;
    }

    /// Like columnProgress, but takes into account only PK columns
    Float64 keyColumnsProgress(size_t num_rows, size_t num_total_rows) const
    {
        return keyColumnsSize(num_rows, num_total_rows) / sum_total;
    }
};

/** Progress callback. Is used by Horizontal merger and first step of Vertical merger.
  * What it should update:
  * - approximate progress
  * - amount of merged rows and their size (PK columns subset is used in case of Vertical merge)
  * - time elapsed for current merge.
  */
class MergeProgressCallback
{
public:
    MergeProgressCallback(MergeList::Entry & merge_entry_, UInt64 & watch_prev_elapsed_)
    : merge_entry(merge_entry_), watch_prev_elapsed(watch_prev_elapsed_) {}

    MergeProgressCallback(MergeList::Entry & merge_entry_, size_t num_total_rows, const ColumnSizeEstimator & column_sizes,
        UInt64 & watch_prev_elapsed_, MergeTreeDataMerger::MergeAlgorithm merge_alg_ = MergeAlgorithm::Vertical)
    : merge_entry(merge_entry_), watch_prev_elapsed(watch_prev_elapsed_), merge_alg(merge_alg_)
    {
        average_elem_progress = (merge_alg == MergeAlgorithm::Horizontal)
            ? 1.0 / num_total_rows
            : column_sizes.keyColumnsProgress(1, num_total_rows);

        updateWatch();
    }

    MergeList::Entry & merge_entry;
    UInt64 & watch_prev_elapsed;
    Float64 average_elem_progress;
    const MergeAlgorithm merge_alg{MergeAlgorithm::Vertical};

    void updateWatch()
    {
        UInt64 watch_curr_elapsed = merge_entry->watch.elapsed();
        ProfileEvents::increment(ProfileEvents::MergesTimeMilliseconds, (watch_curr_elapsed - watch_prev_elapsed) / 1000000);
        watch_prev_elapsed = watch_curr_elapsed;
    }

    void operator() (const Progress & value)
    {
        ProfileEvents::increment(ProfileEvents::MergedUncompressedBytes, value.bytes);
        ProfileEvents::increment(ProfileEvents::MergedRows, value.rows);
        updateWatch();

        merge_entry->bytes_read_uncompressed += value.bytes;
        merge_entry->rows_read += value.rows;
        merge_entry->progress.store(average_elem_progress * merge_entry->rows_read, std::memory_order_relaxed);
    };
};

/** Progress callback for gathering step of Vertical merge.
  * Updates: approximate progress, amount of merged bytes (TODO: two column case should be fixed), elapsed time.
  */
class MergeProgressCallbackVerticalStep : public MergeProgressCallback
{
public:
    MergeProgressCallbackVerticalStep(MergeList::Entry & merge_entry_, size_t num_total_rows_exact,
        const ColumnSizeEstimator & column_sizes, const String & column_name, UInt64 & watch_prev_elapsed_)
    : MergeProgressCallback(merge_entry_, watch_prev_elapsed_), initial_progress(merge_entry->progress.load(std::memory_order_relaxed))
    {
        average_elem_progress = column_sizes.columnProgress(column_name, 1, num_total_rows_exact);
        updateWatch();
    }

    Float64 initial_progress;
    size_t rows_read_internal{0}; // NOTE: not thread safe (to be copyable). It is OK in current single thread use case

    void operator() (const Progress & value)
    {
        merge_entry->bytes_read_uncompressed += value.bytes;
        ProfileEvents::increment(ProfileEvents::MergedUncompressedBytes, value.bytes);
        updateWatch();

        rows_read_internal += value.rows;
        Float64 local_progress = average_elem_progress * rows_read_internal;

        /// NOTE: 'progress' is modified by single thread, but it may be concurrently read from MergeListElement::getInfo() (StorageSystemMerges).
        merge_entry->progress.store(initial_progress + local_progress, std::memory_order_relaxed);
    };
};


/// parts should be sorted.
MergeTreeData::MutableDataPartPtr MergeTreeDataMerger::mergePartsToTemporaryPart(
    const FuturePart & future_part, MergeList::Entry & merge_entry,
    size_t aio_threshold, time_t time_of_merge, DiskSpaceMonitor::Reservation * disk_reservation, bool deduplicate)
{
    static const String TMP_PREFIX = "tmp_merge_";

    if (merges_blocker.isCancelled())
        throw Exception("Cancelled merging parts", ErrorCodes::ABORTED);

    const MergeTreeData::DataPartsVector & parts = future_part.parts;

    LOG_DEBUG(log, "Merging " << parts.size() << " parts: from "
              << parts.front()->name << " to " << parts.back()->name
              << " into " << TMP_PREFIX + future_part.name);

    String new_part_tmp_path = data.getFullPath() + TMP_PREFIX + future_part.name + "/";
    if (Poco::File(new_part_tmp_path).exists())
        throw Exception("Directory " + new_part_tmp_path + " already exists", ErrorCodes::DIRECTORY_ALREADY_EXISTS);

    merge_entry->num_parts = parts.size();

    for (const MergeTreeData::DataPartPtr & part : parts)
    {
        std::shared_lock<std::shared_mutex> part_lock(part->columns_lock);

        merge_entry->total_size_bytes_compressed += part->bytes_on_disk;
        merge_entry->total_size_marks += part->marks_count;
    }

    MergeTreeData::DataPart::ColumnToSize merged_column_to_size;
    for (const MergeTreeData::DataPartPtr & part : parts)
        part->accumulateColumnSizes(merged_column_to_size);

    Names all_column_names = data.getColumns().getNamesOfPhysical();
    NamesAndTypesList all_columns = data.getColumns().getAllPhysical();
    const SortDescription sort_desc = data.getSortDescription();

    NamesAndTypesList gathering_columns, merging_columns;
    Names gathering_column_names, merging_column_names;
    extractMergingAndGatheringColumns(all_columns, data.getPrimaryExpression(), data.getSecondarySortExpression()
            , data.merging_params, gathering_columns, gathering_column_names, merging_columns, merging_column_names);

    MergeTreeData::MutableDataPartPtr new_data_part = std::make_shared<MergeTreeData::DataPart>(
            data, future_part.name, future_part.part_info);
    new_data_part->partition.assign(future_part.getPartition());
    new_data_part->relative_path = TMP_PREFIX + future_part.name;
    new_data_part->is_temp = true;

    size_t sum_input_rows_upper_bound = merge_entry->total_size_marks * data.index_granularity;

    MergeAlgorithm merge_alg = chooseMergeAlgorithm(data, parts, sum_input_rows_upper_bound, gathering_columns, deduplicate);

    LOG_DEBUG(log, "Selected MergeAlgorithm: " << ((merge_alg == MergeAlgorithm::Vertical) ? "Vertical" : "Horizontal"));

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

    for (const auto & part : parts)
    {
        auto input = std::make_unique<MergeTreeBlockInputStream>(
            data, part, DEFAULT_MERGE_BLOCK_SIZE, 0, 0, merging_column_names, MarkRanges(1, MarkRange(0, part->marks_count)),
            false, nullptr, "", true, aio_threshold, DBMS_DEFAULT_BUFFER_SIZE, false);

        input->setProgressCallback(MergeProgressCallback(
                merge_entry, sum_input_rows_upper_bound, column_sizes, watch_prev_elapsed, merge_alg));

        if (data.hasPrimaryKey())
            src_streams.emplace_back(std::make_shared<MaterializingBlockInputStream>(
                std::make_shared<ExpressionBlockInputStream>(BlockInputStreamPtr(std::move(input)), data.getPrimaryExpression())));
        else
            src_streams.emplace_back(std::move(input));
    }

    /// The order of the streams is important: when the key is matched, the elements go in the order of the source stream number.
    /// In the merged part, the lines with the same key must be in the ascending order of the identifier of original part,
    ///  that is going in insertion order.
    std::shared_ptr<IProfilingBlockInputStream> merged_stream;

    switch (data.merging_params.mode)
    {
        case MergeTreeData::MergingParams::Ordinary:
            merged_stream = std::make_unique<MergingSortedBlockInputStream>(
                src_streams, sort_desc, DEFAULT_MERGE_BLOCK_SIZE, 0, rows_sources_write_buf.get(), true);
            break;

        case MergeTreeData::MergingParams::Collapsing:
            merged_stream = std::make_unique<CollapsingSortedBlockInputStream>(
                src_streams, sort_desc, data.merging_params.sign_column, DEFAULT_MERGE_BLOCK_SIZE, rows_sources_write_buf.get());
            break;

        case MergeTreeData::MergingParams::Summing:
            merged_stream = std::make_unique<SummingSortedBlockInputStream>(
                src_streams, sort_desc, data.merging_params.columns_to_sum, DEFAULT_MERGE_BLOCK_SIZE);
            break;

        case MergeTreeData::MergingParams::Aggregating:
            merged_stream = std::make_unique<AggregatingSortedBlockInputStream>(
                src_streams, sort_desc, DEFAULT_MERGE_BLOCK_SIZE);
            break;

        case MergeTreeData::MergingParams::Replacing:
            merged_stream = std::make_unique<ReplacingSortedBlockInputStream>(
                src_streams, sort_desc, data.merging_params.version_column, DEFAULT_MERGE_BLOCK_SIZE, rows_sources_write_buf.get());
            break;

        case MergeTreeData::MergingParams::Graphite:
            merged_stream = std::make_unique<GraphiteRollupSortedBlockInputStream>(
                src_streams, sort_desc, DEFAULT_MERGE_BLOCK_SIZE,
                data.merging_params.graphite_params, time_of_merge);
            break;

        case MergeTreeData::MergingParams::VersionedCollapsing:
            merged_stream = std::make_unique<VersionedCollapsingSortedBlockInputStream>(
                    src_streams, sort_desc, data.merging_params.sign_column, DEFAULT_MERGE_BLOCK_SIZE, false, rows_sources_write_buf.get());
            break;

        default:
            throw Exception("Unknown mode of operation for MergeTreeData: " + toString<int>(data.merging_params.mode), ErrorCodes::LOGICAL_ERROR);
    }

    if (deduplicate && merged_stream->isGroupedOutput())
        merged_stream = std::make_shared<DistinctSortedBlockInputStream>(merged_stream, SizeLimits(), 0 /*limit_hint*/, Names());

    auto compression_settings = data.context.chooseCompressionSettings(
            merge_entry->total_size_bytes_compressed,
            static_cast<double> (merge_entry->total_size_bytes_compressed) / data.getTotalActiveSizeInBytes());

    MergedBlockOutputStream to{
        data, new_part_tmp_path, merging_columns, compression_settings, merged_column_to_size, aio_threshold};

    merged_stream->readPrefix();
    to.writePrefix();

    size_t rows_written = 0;
    const size_t initial_reservation = disk_reservation ? disk_reservation->getSize() : 0;

    Block block;
    while (!merges_blocker.isCancelled() && (block = merged_stream->read()))
    {
        rows_written += block.rows();
        to.write(block);

        merge_entry->rows_written = merged_stream->getProfileInfo().rows;
        merge_entry->bytes_written_uncompressed = merged_stream->getProfileInfo().bytes;

        /// Reservation updates is not performed yet, during the merge it may lead to higher free space requirements
        if (disk_reservation)
        {
            /// The same progress from merge_entry could be used for both algorithms (it should be more accurate)
            /// But now we are using inaccurate row-based estimation in Horizontal case for backward compability
            Float64 progress = (merge_alg == MergeAlgorithm::Horizontal)
                ? std::min(1., 1. * rows_written / sum_input_rows_upper_bound)
                : std::min(1., merge_entry->progress.load(std::memory_order_relaxed));

            disk_reservation->update(static_cast<size_t>((1. - progress) * initial_reservation));
        }
    }
    merged_stream->readSuffix();
    merged_stream.reset();

    if (merges_blocker.isCancelled())
        throw Exception("Cancelled merging parts", ErrorCodes::ABORTED);

    MergeTreeData::DataPart::Checksums checksums_gathered_columns;

    /// Gather ordinary columns
    if (merge_alg == MergeAlgorithm::Vertical)
    {
        size_t sum_input_rows_exact = merge_entry->rows_read;
        merge_entry->columns_written = merging_column_names.size();
        merge_entry->progress.store(column_sizes.keyColumnsProgress(sum_input_rows_exact, sum_input_rows_exact), std::memory_order_relaxed);

        BlockInputStreams column_part_streams(parts.size());
        NameSet offset_columns_written;

        auto it_name_and_type = gathering_columns.cbegin();

        rows_sources_write_buf->next();
        rows_sources_uncompressed_write_buf->next();
        CompressedReadBufferFromFile rows_sources_read_buf(rows_sources_file_path, 0, 0);

        for (size_t column_num = 0, gathering_column_names_size = gathering_column_names.size();
            column_num < gathering_column_names_size;
            ++column_num, ++it_name_and_type)
        {
            const String & column_name = it_name_and_type->name;
            const DataTypePtr & column_type = it_name_and_type->type;
            const String offset_column_name = Nested::extractTableName(column_name);
            Names column_name_{column_name};
            Float64 progress_before = merge_entry->progress.load(std::memory_order_relaxed);
            bool offset_written = offset_columns_written.count(offset_column_name);

            for (size_t part_num = 0; part_num < parts.size(); ++part_num)
            {
                auto column_part_stream = std::make_shared<MergeTreeBlockInputStream>(
                    data, parts[part_num], DEFAULT_MERGE_BLOCK_SIZE, 0, 0, column_name_, MarkRanges{MarkRange(0, parts[part_num]->marks_count)},
                    false, nullptr, "", true, aio_threshold, DBMS_DEFAULT_BUFFER_SIZE, false, Names{}, 0, true);

                column_part_stream->setProgressCallback(MergeProgressCallbackVerticalStep(
                        merge_entry, sum_input_rows_exact, column_sizes, column_name, watch_prev_elapsed));

                column_part_streams[part_num] = std::move(column_part_stream);
            }

            rows_sources_read_buf.seek(0, 0);
            ColumnGathererStream column_gathered_stream(column_name, column_part_streams, rows_sources_read_buf);
            MergedColumnOnlyOutputStream column_to(data, column_gathered_stream.getHeader(), new_part_tmp_path, false, compression_settings, offset_written);
            size_t column_elems_written = 0;

            column_to.writePrefix();
            while ((block = column_gathered_stream.read()))
            {
                column_elems_written += block.rows();
                column_to.write(block);
            }
            column_gathered_stream.readSuffix();
            checksums_gathered_columns.add(column_to.writeSuffixAndGetChecksums());

            if (rows_written != column_elems_written)
            {
                throw Exception("Written " + toString(column_elems_written) + " elements of column " + column_name +
                                ", but " + toString(rows_written) + " rows of PK columns", ErrorCodes::LOGICAL_ERROR);
            }

            if (typeid_cast<const DataTypeArray *>(column_type.get()))
                offset_columns_written.emplace(offset_column_name);

            /// NOTE: 'progress' is modified by single thread, but it may be concurrently read from MergeListElement::getInfo() (StorageSystemMerges).

            merge_entry->columns_written = merging_column_names.size() + column_num;
            merge_entry->bytes_written_uncompressed += column_gathered_stream.getProfileInfo().bytes;
            merge_entry->progress.store(progress_before + column_sizes.columnProgress(column_name, sum_input_rows_exact, sum_input_rows_exact), std::memory_order_relaxed);

            if (merges_blocker.isCancelled())
                throw Exception("Cancelled merging parts", ErrorCodes::ABORTED);
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

    /// For convenience, even CollapsingSortedBlockInputStream can not return zero rows.
    if (0 == to.getRowsCount())
        throw Exception("Empty part after merge", ErrorCodes::LOGICAL_ERROR);

    return new_data_part;
}


MergeTreeDataMerger::MergeAlgorithm MergeTreeDataMerger::chooseMergeAlgorithm(
    const MergeTreeData & data, const MergeTreeData::DataPartsVector & parts, size_t sum_rows_upper_bound,
    const NamesAndTypesList & gathering_columns, bool deduplicate) const
{
    if (deduplicate)
        return MergeAlgorithm::Horizontal;
    if (data.context.getMergeTreeSettings().enable_vertical_merge_algorithm == 0)
        return MergeAlgorithm::Horizontal;

    bool is_supported_storage =
        data.merging_params.mode == MergeTreeData::MergingParams::Ordinary ||
        data.merging_params.mode == MergeTreeData::MergingParams::Collapsing ||
        data.merging_params.mode == MergeTreeData::MergingParams::Replacing ||
        data.merging_params.mode == MergeTreeData::MergingParams::VersionedCollapsing;

    bool enough_ordinary_cols = gathering_columns.size() >= data.context.getMergeTreeSettings().vertical_merge_algorithm_min_columns_to_activate;

    bool enough_total_rows = sum_rows_upper_bound >= data.context.getMergeTreeSettings().vertical_merge_algorithm_min_rows_to_activate;

    bool no_parts_overflow = parts.size() <= RowSourcePart::MAX_PARTS;

    auto merge_alg = (is_supported_storage && enough_total_rows && enough_ordinary_cols && no_parts_overflow) ?
                        MergeAlgorithm::Vertical : MergeAlgorithm::Horizontal;

    return merge_alg;
}


MergeTreeData::DataPartPtr MergeTreeDataMerger::renameMergedTemporaryPart(
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
         * - but ZooKeeper transaction that add its to reference dataset in ZK and unlocks AbandonableLock is failed;
         * - and it is failed due to connection loss, so we don't rollback working dataset in memory,
         *   because we don't know if the part was added to ZK or not
         *   (see ReplicatedMergeTreeBlockOutputStream)
         * - then method selectPartsToMerge selects a range and see, that AbandonableLock for this part is abandoned,
         *   and so, it is possible to merge a range skipping this part.
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


size_t MergeTreeDataMerger::estimateDiskSpaceForMerge(const MergeTreeData::DataPartsVector & parts)
{
    size_t res = 0;
    for (const MergeTreeData::DataPartPtr & part : parts)
        res += part->bytes_on_disk;

    return static_cast<size_t>(res * DISK_USAGE_COEFFICIENT_TO_RESERVE);
}

}
