#include <Storages/MergeTree/MergeTreeDataMerger.h>
#include <Storages/MergeTree/MergeTreeBlockInputStream.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/DiskSpaceMonitor.h>
#include <Storages/MergeTree/MergeTreeSharder.h>
#include <Storages/MergeTree/ReshardingJob.h>
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
#include <DataStreams/MaterializingBlockInputStream.h>
#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/ColumnGathererStream.h>
#include <DataTypes/DataTypeNested.h>
#include <DataTypes/DataTypeArray.h>
#include <Storages/MergeTree/BackgroundProcessingPool.h>
#include <Common/SimpleIncrement.h>
#include <Common/interpolate.h>

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


namespace
{

std::string createMergedPartName(const MergeTreeData::DataPartsVector & parts)
{
    DayNum_t left_date = DayNum_t(std::numeric_limits<UInt16>::max());
    DayNum_t right_date = DayNum_t(std::numeric_limits<UInt16>::min());
    UInt32 level = 0;

    for (const MergeTreeData::DataPartPtr & part : parts)
    {
        level = std::max(level, part->level);
        left_date = std::min(left_date, part->left_date);
        right_date = std::max(right_date, part->right_date);
    }

    return ActiveDataPartSet::getPartName(left_date, right_date, parts.front()->left, parts.back()->right, level + 1);
}

}

/// Do not start to merge parts, if free space is less than sum size of parts times specified coefficient.
/// This value is chosen to not allow big merges to eat all free space. Thus allowing small merges to proceed.
static const double DISK_USAGE_COEFFICIENT_TO_SELECT = 2;

/// To do merge, reserve amount of space equals to sum size of parts times specified coefficient.
/// Must be strictly less than DISK_USAGE_COEFFICIENT_TO_SELECT,
///  because between selecting parts to merge and doing merge, amount of free space could have decreased.
static const double DISK_USAGE_COEFFICIENT_TO_RESERVE = 1.1;

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
    MergeTreeData::DataPartsVector & parts,
    String & merged_name,
    bool aggressive,
    size_t max_total_size_to_merge,
    const AllowedMergingPredicate & can_merge_callback)
{
    parts.clear();

    MergeTreeData::DataPartsVector data_parts = data.getDataPartsVector();

    if (data_parts.empty())
        return false;

    time_t current_time = time(nullptr);

    IMergeSelector::Partitions partitions;

    DayNum_t prev_month = DayNum_t(-1);
    const MergeTreeData::DataPartPtr * prev_part = nullptr;
    for (const MergeTreeData::DataPartPtr & part : data_parts)
    {
        DayNum_t month = part->month;
        if (month != prev_month || (prev_part && !can_merge_callback(*prev_part, part)))
        {
            if (partitions.empty() || !partitions.back().empty())
                partitions.emplace_back();
            prev_month = month;
        }

        IMergeSelector::Part part_info;
        part_info.size = part->size_in_bytes;
        part_info.age = current_time - part->modification_time;
        part_info.level = part->level;
        part_info.data = &part;

        partitions.back().emplace_back(part_info);

        /// Check for consistenty of data parts. If assertion is failed, it requires immediate investigation.
        if (prev_part && part->month == (*prev_part)->month && part->left < (*prev_part)->right)
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
        return false;

    if (parts_to_merge.size() == 1)
        throw Exception("Logical error: merge selector returned only one part to merge", ErrorCodes::LOGICAL_ERROR);

    parts.reserve(parts_to_merge.size());

    DayNum_t left_date = DayNum_t(std::numeric_limits<UInt16>::max());
    DayNum_t right_date = DayNum_t(std::numeric_limits<UInt16>::min());
    UInt32 level = 0;

    for (IMergeSelector::Part & part_info : parts_to_merge)
    {
        const MergeTreeData::DataPartPtr & part = *static_cast<const MergeTreeData::DataPartPtr *>(part_info.data);

        parts.push_back(part);

        level = std::max(level, part->level);
        left_date = std::min(left_date, part->left_date);
        right_date = std::max(right_date, part->right_date);
    }

    merged_name = ActiveDataPartSet::getPartName(
        left_date, right_date, parts.front()->left, parts.back()->right, level + 1);

    LOG_DEBUG(log, "Selected " << parts.size() << " parts from " << parts.front()->name << " to " << parts.back()->name);
    return true;
}


bool MergeTreeDataMerger::selectAllPartsToMergeWithinPartition(
    MergeTreeData::DataPartsVector & what,
    String & merged_name,
    size_t available_disk_space,
    const AllowedMergingPredicate & can_merge,
    DayNum_t partition,
    bool final)
{
    MergeTreeData::DataPartsVector parts = selectAllPartsFromPartition(partition);

    if (parts.empty())
        return false;

    if (!final && parts.size() == 1)
        return false;

    MergeTreeData::DataPartsVector::const_iterator it = parts.begin();
    MergeTreeData::DataPartsVector::const_iterator prev_it = it;

    size_t sum_bytes = 0;
    DayNum_t left_date = DayNum_t(std::numeric_limits<UInt16>::max());
    DayNum_t right_date = DayNum_t(std::numeric_limits<UInt16>::min());
    UInt32 level = 0;

    while (it != parts.end())
    {
        if ((it != parts.begin() || parts.size() == 1)    /// For the case of one part, we check that it can be measured "with itself".
            && !can_merge(*prev_it, *it))
            return false;

        level = std::max(level, (*it)->level);
        left_date = std::min(left_date, (*it)->left_date);
        right_date = std::max(right_date, (*it)->right_date);

        sum_bytes += (*it)->size_in_bytes;

        prev_it = it;
        ++it;
    }

    /// Enough disk space to cover the new merge with a margin.
    if (available_disk_space <= sum_bytes * DISK_USAGE_COEFFICIENT_TO_SELECT)
    {
        time_t now = time(0);
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
        return false;
    }

    what = parts;
    merged_name = ActiveDataPartSet::getPartName(
        left_date, right_date, parts.front()->left, parts.back()->right, level + 1);

    LOG_DEBUG(log, "Selected " << parts.size() << " parts from " << parts.front()->name << " to " << parts.back()->name);
    return true;
}


MergeTreeData::DataPartsVector MergeTreeDataMerger::selectAllPartsFromPartition(DayNum_t partition)
{
    MergeTreeData::DataPartsVector parts_from_partition;

    MergeTreeData::DataParts data_parts = data.getDataParts();

    for (MergeTreeData::DataParts::iterator it = data_parts.cbegin(); it != data_parts.cend(); ++it)
    {
        const MergeTreeData::DataPartPtr & current_part = *it;
        DayNum_t month = current_part->month;
        if (month != partition)
            continue;

        parts_from_partition.push_back(*it);
    }

    return parts_from_partition;
}


/// PK columns are sorted and merged, ordinary columns are gathered using info from merge step
static void extractMergingAndGatheringColumns(const NamesAndTypesList & all_columns, ExpressionActionsPtr primary_key_expressions,
    const MergeTreeData::MergingParams & merging_params,
    NamesAndTypesList & gathering_columns, Names & gathering_column_names,
    NamesAndTypesList & merging_columns, Names & merging_column_names
)
{
    Names key_columns_dup = primary_key_expressions->getRequiredColumns();
    std::set<String> key_columns(key_columns_dup.cbegin(), key_columns_dup.cend());

    /// Force sign column for Collapsing mode
    if (merging_params.mode == MergeTreeData::MergingParams::Collapsing)
        key_columns.emplace(merging_params.sign_column);

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

        sum_total = std::max(1UL, sum_index_columns + sum_ordinary_columns);
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
class MergeProgressCallback : public ProgressCallback
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
        merge_entry->progress = average_elem_progress * merge_entry->rows_read;
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
    : MergeProgressCallback(merge_entry_, watch_prev_elapsed_), initial_progress(merge_entry->progress)
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
        merge_entry->progress = initial_progress + local_progress;
    };
};


/// parts should be sorted.
MergeTreeData::MutableDataPartPtr MergeTreeDataMerger::mergePartsToTemporaryPart(
    MergeTreeData::DataPartsVector & parts, const String & merged_name, MergeList::Entry & merge_entry,
    size_t aio_threshold, time_t time_of_merge, DiskSpaceMonitor::Reservation * disk_reservation, bool deduplicate)
{
    if (isCancelled())
        throw Exception("Cancelled merging parts", ErrorCodes::ABORTED);

    LOG_DEBUG(log, "Merging " << parts.size() << " parts: from " << parts.front()->name << " to " << parts.back()->name << " into " << merged_name);

    String merged_dir = data.getFullPath() + merged_name;
    if (Poco::File(merged_dir).exists())
        throw Exception("Directory " + merged_dir + " already exists", ErrorCodes::DIRECTORY_ALREADY_EXISTS);

    merge_entry->num_parts = parts.size();

    for (const MergeTreeData::DataPartPtr & part : parts)
    {
        Poco::ScopedReadRWLock part_lock(part->columns_lock);

        merge_entry->total_size_bytes_compressed += part->size_in_bytes;
        merge_entry->total_size_marks += part->size;
    }

    MergeTreeData::DataPart::ColumnToSize merged_column_to_size;
    for (const MergeTreeData::DataPartPtr & part : parts)
        part->accumulateColumnSizes(merged_column_to_size);

    Names all_column_names = data.getColumnNamesList();
    NamesAndTypesList all_columns = data.getColumnsList();
    const SortDescription sort_desc = data.getSortDescription();

    NamesAndTypesList gathering_columns, merging_columns;
    Names gathering_column_names, merging_column_names;
    extractMergingAndGatheringColumns(all_columns, data.getPrimaryExpression(), data.merging_params,
        gathering_columns, gathering_column_names, merging_columns, merging_column_names);

    MergeTreeData::MutableDataPartPtr new_data_part = std::make_shared<MergeTreeData::DataPart>(data);
    ActiveDataPartSet::parsePartName(merged_name, *new_data_part);
    new_data_part->name = "tmp_" + merged_name;
    new_data_part->is_temp = true;

    size_t sum_input_rows_upper_bound = merge_entry->total_size_marks * data.index_granularity;

    MergedRowSources merged_rows_sources;
    MergedRowSources * merged_rows_sources_ptr = &merged_rows_sources;
    MergeAlgorithm merge_alg = chooseMergeAlgorithm(data, parts, sum_input_rows_upper_bound, gathering_columns, merged_rows_sources, deduplicate);

    LOG_DEBUG(log, "Selected MergeAlgorithm: " << ((merge_alg == MergeAlgorithm::Vertical) ? "Vertical" : "Horizontal"));

    if (merge_alg != MergeAlgorithm::Vertical)
    {
        merged_rows_sources_ptr = nullptr;
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

    for (size_t i = 0; i < parts.size(); ++i)
    {
        auto input = std::make_unique<MergeTreeBlockInputStream>(
            data, parts[i], DEFAULT_MERGE_BLOCK_SIZE, 0, merging_column_names, MarkRanges(1, MarkRange(0, parts[i]->size)),
            false, nullptr, "", true, aio_threshold, DBMS_DEFAULT_BUFFER_SIZE, false);

        input->setProgressCallback(
            MergeProgressCallback{merge_entry, sum_input_rows_upper_bound, column_sizes, watch_prev_elapsed, merge_alg});

        if (data.merging_params.mode != MergeTreeData::MergingParams::Unsorted)
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
                src_streams, sort_desc, DEFAULT_MERGE_BLOCK_SIZE, 0, merged_rows_sources_ptr, true);
            break;

        case MergeTreeData::MergingParams::Collapsing:
            merged_stream = std::make_unique<CollapsingSortedBlockInputStream>(
                src_streams, sort_desc, data.merging_params.sign_column, DEFAULT_MERGE_BLOCK_SIZE, merged_rows_sources_ptr);
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
                src_streams, sort_desc, data.merging_params.version_column, DEFAULT_MERGE_BLOCK_SIZE);
            break;

        case MergeTreeData::MergingParams::Graphite:
            merged_stream = std::make_unique<GraphiteRollupSortedBlockInputStream>(
                src_streams, sort_desc, DEFAULT_MERGE_BLOCK_SIZE,
                data.merging_params.graphite_params, time_of_merge);
            break;

        case MergeTreeData::MergingParams::Unsorted:
            merged_stream = std::make_unique<ConcatBlockInputStream>(src_streams);
            break;

        default:
            throw Exception("Unknown mode of operation for MergeTreeData: " + toString<int>(data.merging_params.mode), ErrorCodes::LOGICAL_ERROR);
    }

    if (deduplicate && merged_stream->isGroupedOutput())
        merged_stream = std::make_shared<DistinctSortedBlockInputStream>(merged_stream, Limits(), 0 /*limit_hint*/, Names());

    String new_part_tmp_path = data.getFullPath() + "tmp_" + merged_name + "/";

    auto compression_method = data.context.chooseCompressionMethod(
        merge_entry->total_size_bytes_compressed,
        static_cast<double>(merge_entry->total_size_bytes_compressed) / data.getTotalActiveSizeInBytes());

    MergedBlockOutputStream to{
        data, new_part_tmp_path, merging_columns, compression_method, merged_column_to_size, aio_threshold};

    merged_stream->readPrefix();
    to.writePrefix();

    size_t rows_written = 0;
    const size_t initial_reservation = disk_reservation ? disk_reservation->getSize() : 0;

    Block block;
    while (!isCancelled() && (block = merged_stream->read()))
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
                : std::min(1., merge_entry->progress);

            disk_reservation->update(static_cast<size_t>((1. - progress) * initial_reservation));
        }
    }
    merged_stream->readSuffix();
    merged_stream.reset();

    if (isCancelled())
        throw Exception("Cancelled merging parts", ErrorCodes::ABORTED);

    MergeTreeData::DataPart::Checksums checksums_gathered_columns;

    /// Gather ordinary columns
    if (merge_alg == MergeAlgorithm::Vertical)
    {
        size_t sum_input_rows_exact = merge_entry->rows_read;
        merge_entry->columns_written = merging_column_names.size();
        merge_entry->progress = column_sizes.keyColumnsProgress(sum_input_rows_exact, sum_input_rows_exact);

        BlockInputStreams column_part_streams(parts.size());
        NameSet offset_columns_written;

        auto it_name_and_type = gathering_columns.cbegin();

        for (size_t column_num = 0, gathering_column_names_size = gathering_column_names.size();
            column_num < gathering_column_names_size;
            ++column_num, ++it_name_and_type)
        {
            const String & column_name = it_name_and_type->name;
            const DataTypePtr & column_type = it_name_and_type->type;
            const String offset_column_name = DataTypeNested::extractNestedTableName(column_name);
            Names column_name_{column_name};
            Float64 progress_before = merge_entry->progress;
            bool offset_written = offset_columns_written.count(offset_column_name);

            for (size_t part_num = 0; part_num < parts.size(); ++part_num)
            {
                auto column_part_stream = std::make_shared<MergeTreeBlockInputStream>(
                    data, parts[part_num], DEFAULT_MERGE_BLOCK_SIZE, 0, column_name_, MarkRanges{MarkRange(0, parts[part_num]->size)},
                    false, nullptr, "", true, aio_threshold, DBMS_DEFAULT_BUFFER_SIZE, false, Names{}, 0, true);

                column_part_stream->setProgressCallback(
                    MergeProgressCallbackVerticalStep{merge_entry, sum_input_rows_exact, column_sizes, column_name, watch_prev_elapsed});

                column_part_streams[part_num] = std::move(column_part_stream);
            }

            ColumnGathererStream column_gathered_stream(column_part_streams, column_name, merged_rows_sources, DEFAULT_BLOCK_SIZE);
            MergedColumnOnlyOutputStream column_to(data, new_part_tmp_path, false, compression_method, offset_written);
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

            merge_entry->columns_written = merging_column_names.size() + column_num;
            merge_entry->bytes_written_uncompressed += column_gathered_stream.getProfileInfo().bytes;
            merge_entry->progress = progress_before + column_sizes.columnProgress(column_name, sum_input_rows_exact, sum_input_rows_exact);

            if (isCancelled())
                throw Exception("Cancelled merging parts", ErrorCodes::ABORTED);
        }
    }

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

    new_data_part->columns = all_columns;
    if (merge_alg != MergeAlgorithm::Vertical)
        new_data_part->checksums = to.writeSuffixAndGetChecksums();
    else
        new_data_part->checksums = to.writeSuffixAndGetChecksums(all_columns, &checksums_gathered_columns);
    new_data_part->index.swap(to.getIndex());

    /// For convenience, even CollapsingSortedBlockInputStream can not return zero rows.
    if (0 == to.marksCount())
        throw Exception("Empty part after merge", ErrorCodes::LOGICAL_ERROR);

    new_data_part->size = to.marksCount();
    new_data_part->modification_time = time(0);
    new_data_part->size_in_bytes = MergeTreeData::DataPart::calcTotalSize(new_part_tmp_path);
    new_data_part->is_sharded = false;

    return new_data_part;
}


MergeTreeDataMerger::MergeAlgorithm MergeTreeDataMerger::chooseMergeAlgorithm(
    const MergeTreeData & data, const MergeTreeData::DataPartsVector & parts, size_t sum_rows_upper_bound,
    const NamesAndTypesList & gathering_columns, MergedRowSources & rows_sources_to_alloc, bool deduplicate) const
{
    if (deduplicate)
        return MergeAlgorithm::Horizontal;
    if (data.context.getMergeTreeSettings().enable_vertical_merge_algorithm == 0)
        return MergeAlgorithm::Horizontal;

    bool is_supported_storage =
        data.merging_params.mode == MergeTreeData::MergingParams::Ordinary ||
        data.merging_params.mode == MergeTreeData::MergingParams::Collapsing;

    bool enough_ordinary_cols = gathering_columns.size() >= data.context.getMergeTreeSettings().vertical_merge_algorithm_min_columns_to_activate;

    bool enough_total_rows = sum_rows_upper_bound >= data.context.getMergeTreeSettings().vertical_merge_algorithm_min_rows_to_activate;

    bool no_parts_overflow = parts.size() <= RowSourcePart::MAX_PARTS;

    auto merge_alg = (is_supported_storage && enough_total_rows && enough_ordinary_cols && no_parts_overflow) ?
                        MergeAlgorithm::Vertical : MergeAlgorithm::Horizontal;

    if (merge_alg == MergeAlgorithm::Vertical)
    {
        try
        {
            rows_sources_to_alloc.reserve(sum_rows_upper_bound);
        }
        catch (...)
        {
            /// Not enough memory for VERTICAL merge algorithm, make sense for very large tables
            merge_alg = MergeAlgorithm::Horizontal;
        }
    }

    return merge_alg;
}


MergeTreeData::DataPartPtr MergeTreeDataMerger::renameMergedTemporaryPart(
    MergeTreeData::DataPartsVector & parts,
    MergeTreeData::MutableDataPartPtr & new_data_part,
    const String & merged_name,
    MergeTreeData::Transaction * out_transaction)
{
    /// Rename new part, add to the set and remove original parts.
    auto replaced_parts = data.renameTempPartAndReplace(new_data_part, nullptr, out_transaction);

    if (new_data_part->name != merged_name)
        throw Exception("Unexpected part name: " + new_data_part->name + " instead of " + merged_name, ErrorCodes::LOGICAL_ERROR);

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


MergeTreeData::PerShardDataParts MergeTreeDataMerger::reshardPartition(
    const ReshardingJob & job, DiskSpaceMonitor::Reservation * disk_reservation)
{
    size_t aio_threshold = data.context.getSettings().min_bytes_to_use_direct_io;

    /// Assemble all parts of the partition.
    DayNum_t month = MergeTreeData::getMonthFromName(job.partition);
    MergeTreeData::DataPartsVector parts = selectAllPartsFromPartition(month);

    /// Create a temporary folder name.
    std::string merged_name = createMergedPartName(parts);

    MergeList::EntryPtr merge_entry_ptr = data.context.getMergeList().insert(job.database_name,
        job.table_name, merged_name, parts);
    MergeList::Entry & merge_entry = *merge_entry_ptr;
    merge_entry->num_parts = parts.size();

    LOG_DEBUG(log, "Resharding " << parts.size() << " parts from " << parts.front()->name
        << " to " << parts.back()->name << " which span the partition " << job.partition);

    /// Merge all parts of the partition.

    for (const MergeTreeData::DataPartPtr & part : parts)
    {
        Poco::ScopedReadRWLock part_lock(part->columns_lock);

        merge_entry->total_size_bytes_compressed += part->size_in_bytes;
        merge_entry->total_size_marks += part->size;
    }

    MergeTreeData::DataPart::ColumnToSize merged_column_to_size;
    if (aio_threshold > 0)
    {
        for (const MergeTreeData::DataPartPtr & part : parts)
            part->accumulateColumnSizes(merged_column_to_size);
    }

    Names column_names = data.getColumnNamesList();
    NamesAndTypesList column_names_and_types = data.getColumnsList();

    BlockInputStreams src_streams;

    size_t sum_rows_approx = 0;

    const auto rows_total = merge_entry->total_size_marks * data.index_granularity;

    for (size_t i = 0; i < parts.size(); ++i)
    {
        MarkRanges ranges(1, MarkRange(0, parts[i]->size));

        auto input = std::make_unique<MergeTreeBlockInputStream>(
            data, parts[i], DEFAULT_MERGE_BLOCK_SIZE, 0, column_names,
            ranges, false, nullptr, "", true, aio_threshold, DBMS_DEFAULT_BUFFER_SIZE, false);

        input->setProgressCallback([&merge_entry, rows_total] (const Progress & value)
            {
                const auto new_rows_read = merge_entry->rows_read += value.rows;
                merge_entry->progress = static_cast<Float64>(new_rows_read) / rows_total;
                merge_entry->bytes_read_uncompressed += value.bytes;
            });

        if (data.merging_params.mode != MergeTreeData::MergingParams::Unsorted)
            src_streams.emplace_back(std::make_shared<MaterializingBlockInputStream>(
                std::make_shared<ExpressionBlockInputStream>(BlockInputStreamPtr(std::move(input)), data.getPrimaryExpression())));
        else
            src_streams.emplace_back(std::move(input));

        sum_rows_approx += parts[i]->size * data.index_granularity;
    }

    /// Sharding of merged blocks.

    /// For blocks numbering.
    SimpleIncrement increment(job.block_number);

    /// Create a new part for each shard.
    MergeTreeData::PerShardDataParts per_shard_data_parts;

    per_shard_data_parts.reserve(job.paths.size());
    for (size_t shard_no = 0; shard_no < job.paths.size(); ++shard_no)
    {
        Int64 temp_index = increment.get();

        MergeTreeData::MutableDataPartPtr data_part = std::make_shared<MergeTreeData::DataPart>(data);
        data_part->name = "tmp_" + merged_name;
        data_part->is_temp = true;
        data_part->left_date = std::numeric_limits<UInt16>::max();
        data_part->right_date = std::numeric_limits<UInt16>::min();
        data_part->month = month;
        data_part->left = temp_index;
        data_part->right = temp_index;
        data_part->level = 0;
        per_shard_data_parts.emplace(shard_no, data_part);
    }

    /// A very rough estimate for the compressed data size of each sharded partition.
    /// Actually it all depends on the properties of the expression for sharding.
    UInt64 per_shard_size_bytes_compressed = merge_entry->total_size_bytes_compressed / static_cast<double>(job.paths.size());

    auto compression_method = data.context.chooseCompressionMethod(
        per_shard_size_bytes_compressed,
        static_cast<double>(per_shard_size_bytes_compressed) / data.getTotalActiveSizeInBytes());

    using MergedBlockOutputStreamPtr = std::unique_ptr<MergedBlockOutputStream>;
    using PerShardOutput = std::unordered_map<size_t, MergedBlockOutputStreamPtr>;

    /// Create a stream for each shard that writes the corresponding sharded blocks.
    PerShardOutput per_shard_output;

    per_shard_output.reserve(job.paths.size());
    for (size_t shard_no = 0; shard_no < job.paths.size(); ++shard_no)
    {
        std::string new_part_tmp_path = data.getFullPath() + "reshard/" + toString(shard_no) + "/tmp_" + merged_name + "/";
        Poco::File(new_part_tmp_path).createDirectories();

        MergedBlockOutputStreamPtr output_stream;
        output_stream = std::make_unique<MergedBlockOutputStream>(
            data, new_part_tmp_path, column_names_and_types, compression_method, merged_column_to_size, aio_threshold);
        per_shard_output.emplace(shard_no, std::move(output_stream));
    }

    /// The order of the threads is important: when the key is matched, the elements go in the order of the source stream number.
    /// In the merged part, rows with the same key must be in ascending order of the original part identifier,
    ///  that is (approximately) increasing insertion time.
    std::unique_ptr<IProfilingBlockInputStream> merged_stream;

    switch (data.merging_params.mode)
    {
        case MergeTreeData::MergingParams::Ordinary:
            merged_stream = std::make_unique<MergingSortedBlockInputStream>(
                src_streams, data.getSortDescription(), DEFAULT_MERGE_BLOCK_SIZE);
            break;

        case MergeTreeData::MergingParams::Collapsing:
            merged_stream = std::make_unique<CollapsingSortedBlockInputStream>(
                src_streams, data.getSortDescription(), data.merging_params.sign_column, DEFAULT_MERGE_BLOCK_SIZE);
            break;

        case MergeTreeData::MergingParams::Summing:
            merged_stream = std::make_unique<SummingSortedBlockInputStream>(
                src_streams, data.getSortDescription(), data.merging_params.columns_to_sum, DEFAULT_MERGE_BLOCK_SIZE);
            break;

        case MergeTreeData::MergingParams::Aggregating:
            merged_stream = std::make_unique<AggregatingSortedBlockInputStream>(
                src_streams, data.getSortDescription(), DEFAULT_MERGE_BLOCK_SIZE);
            break;

        case MergeTreeData::MergingParams::Replacing:
            merged_stream = std::make_unique<ReplacingSortedBlockInputStream>(
                src_streams, data.getSortDescription(), data.merging_params.version_column, DEFAULT_MERGE_BLOCK_SIZE);
            break;

        case MergeTreeData::MergingParams::Graphite:
            merged_stream = std::make_unique<GraphiteRollupSortedBlockInputStream>(
                src_streams, data.getSortDescription(), DEFAULT_MERGE_BLOCK_SIZE,
                data.merging_params.graphite_params, time(0));
            break;

        case MergeTreeData::MergingParams::Unsorted:
            merged_stream = std::make_unique<ConcatBlockInputStream>(src_streams);
            break;

        default:
            throw Exception("Unknown mode of operation for MergeTreeData: " + toString<int>(data.merging_params.mode), ErrorCodes::LOGICAL_ERROR);
    }

    merged_stream->readPrefix();

    for (auto & entry : per_shard_output)
    {
        MergedBlockOutputStreamPtr & output_stream = entry.second;
        output_stream->writePrefix();
    }

    size_t rows_written = 0;
    const size_t initial_reservation = disk_reservation ? disk_reservation->getSize() : 0;

    MergeTreeSharder sharder(data, job);

    while (Block block = merged_stream->read())
    {
        abortReshardPartitionIfRequested();

        ShardedBlocksWithDateIntervals blocks = sharder.shardBlock(block);

        for (ShardedBlockWithDateInterval & block_with_dates : blocks)
        {
            abortReshardPartitionIfRequested();

            size_t shard_no = block_with_dates.shard_no;
            MergeTreeData::MutableDataPartPtr & data_part = per_shard_data_parts.at(shard_no);
            MergedBlockOutputStreamPtr & output_stream = per_shard_output.at(shard_no);

            rows_written += block_with_dates.block.rows();
            output_stream->write(block_with_dates.block);

            if (block_with_dates.min_date < data_part->left_date)
                data_part->left_date = block_with_dates.min_date;
            if (block_with_dates.max_date > data_part->right_date)
                data_part->right_date = block_with_dates.max_date;

            merge_entry->rows_written = merged_stream->getProfileInfo().rows;
            merge_entry->bytes_written_uncompressed = merged_stream->getProfileInfo().bytes;

            if (disk_reservation)
                disk_reservation->update(static_cast<size_t>((1 - std::min(1., 1. * rows_written / sum_rows_approx)) * initial_reservation));
        }
    }

    merged_stream->readSuffix();

    /// Complete initialization of new partitions parts.
    for (size_t shard_no = 0; shard_no < job.paths.size(); ++shard_no)
    {
        abortReshardPartitionIfRequested();

        MergedBlockOutputStreamPtr & output_stream = per_shard_output.at(shard_no);
        if (0 == output_stream->marksCount())
        {
            /// There was no data in this shard. Ignore.
            LOG_WARNING(log, "No data in partition for shard " + job.paths[shard_no].first);
            per_shard_data_parts.erase(shard_no);
            continue;
        }

        MergeTreeData::MutableDataPartPtr & data_part = per_shard_data_parts.at(shard_no);

        data_part->columns = column_names_and_types;
        data_part->checksums = output_stream->writeSuffixAndGetChecksums();
        data_part->index.swap(output_stream->getIndex());
        data_part->size = output_stream->marksCount();
        data_part->modification_time = time(0);
        data_part->size_in_bytes = MergeTreeData::DataPart::calcTotalSize(output_stream->getPartPath());
        data_part->is_sharded = true;
        data_part->shard_no = shard_no;
    }

    /// Turn parts of new partitions into permanent parts.
    for (auto & entry : per_shard_data_parts)
    {
        size_t shard_no = entry.first;
        MergeTreeData::MutableDataPartPtr & part_from_shard = entry.second;
        part_from_shard->is_temp = false;
        std::string prefix = data.getFullPath() + "reshard/" + toString(shard_no) + "/";
        std::string old_name = part_from_shard->name;
        std::string new_name = ActiveDataPartSet::getPartName(part_from_shard->left_date,
            part_from_shard->right_date, part_from_shard->left, part_from_shard->right, part_from_shard->level);
        part_from_shard->name = new_name;
        Poco::File(prefix + old_name).renameTo(prefix + new_name);
    }

    LOG_TRACE(log, "Resharded the partition " << job.partition);

    return per_shard_data_parts;
}

size_t MergeTreeDataMerger::estimateDiskSpaceForMerge(const MergeTreeData::DataPartsVector & parts)
{
    size_t res = 0;
    for (const MergeTreeData::DataPartPtr & part : parts)
        res += part->size_in_bytes;

    return static_cast<size_t>(res * DISK_USAGE_COEFFICIENT_TO_RESERVE);
}

void MergeTreeDataMerger::abortReshardPartitionIfRequested()
{
    if (isCancelled())
        throw Exception("Cancelled partition resharding", ErrorCodes::ABORTED);

    if (cancellation_hook)
        cancellation_hook();
}

}
