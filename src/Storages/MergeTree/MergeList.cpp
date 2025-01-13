#include <Storages/MergeTree/FutureMergedMutatedPart.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <base/getThreadId.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>

#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

const MergeTreePartInfo MergeListElement::FAKE_RESULT_PART_FOR_PROJECTION = {"all", 0, 0, 0};

MergeListElement::MergeListElement(const StorageID & table_id_, FutureMergedMutatedPartPtr future_part, const ContextPtr & context)
    : table_id{table_id_}
    , partition_id{future_part->part_info.partition_id}
    , result_part_name{future_part->name}
    , result_part_path{future_part->path}
    , result_part_info{future_part->part_info}
    , num_parts{future_part->parts.size()}
    , thread_id{getThreadId()}
    , merge_type{future_part->merge_type}
    , merge_algorithm{MergeAlgorithm::Undecided}
{
    auto format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    if (result_part_name != result_part_info.getPartNameV1())
        format_version = MERGE_TREE_DATA_OLD_FORMAT_VERSION;

    /// FIXME why do we need a merge list element for projection parts at all?
    bool is_fake_projection_part = future_part->part_info == FAKE_RESULT_PART_FOR_PROJECTION;

    size_t normal_parts_count = 0;
    for (const auto & source_part : future_part->parts)
    {
        if (!is_fake_projection_part && !source_part->getParentPart())
        {
            ++normal_parts_count;
            if (!result_part_info.contains(MergeTreePartInfo::fromPartName(source_part->name, format_version)))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Source part {} is not covered by result part {}", source_part->name, result_part_info.getPartNameV1());
        }

        source_part_names.emplace_back(source_part->name);
        source_part_paths.emplace_back(source_part->getDataPartStorage().getFullPath());

        total_size_bytes_compressed += source_part->getBytesOnDisk();
        total_size_bytes_uncompressed += source_part->getTotalColumnsSize().data_uncompressed;
        total_size_marks += source_part->getMarksCount();
        total_rows_count += source_part->index_granularity.getTotalRows();
    }

    if (!future_part->parts.empty())
    {
        source_data_version = future_part->parts[0]->info.getDataVersion();
        is_mutation = (result_part_info.level == future_part->parts[0]->info.level) && !is_fake_projection_part;

        WriteBufferFromString out(partition);
        const auto & part = future_part->parts[0];
        part->partition.serializeText(part->storage, out, {});
    }

    if (!is_fake_projection_part && is_mutation && normal_parts_count != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got {} source parts for mutation {}: {}", future_part->parts.size(),
                        result_part_info.getPartNameV1(), fmt::join(source_part_names, ", "));

    thread_group = ThreadGroup::createForBackgroundProcess(context);
}

MergeInfo MergeListElement::getInfo() const
{
    MergeInfo res;
    res.database = table_id.getDatabaseName();
    res.table = table_id.getTableName();
    res.result_part_name = result_part_name;
    res.result_part_path = result_part_path;
    res.partition_id = partition_id;
    res.partition = partition;
    res.is_mutation = is_mutation;
    res.elapsed = watch.elapsedSeconds();
    res.progress = progress.load(std::memory_order_relaxed);
    res.num_parts = num_parts;
    res.total_size_bytes_compressed = total_size_bytes_compressed;
    res.total_size_bytes_uncompressed = total_size_bytes_uncompressed;
    res.total_size_marks = total_size_marks;
    res.total_rows_count = total_rows_count;
    res.bytes_read_uncompressed = bytes_read_uncompressed.load(std::memory_order_relaxed);
    res.bytes_written_uncompressed = bytes_written_uncompressed.load(std::memory_order_relaxed);
    res.rows_read = rows_read.load(std::memory_order_relaxed);
    res.rows_written = rows_written.load(std::memory_order_relaxed);
    res.columns_written = columns_written.load(std::memory_order_relaxed);
    res.memory_usage = getMemoryTracker().get();
    res.thread_id = thread_id;
    res.merge_type = toString(merge_type);
    res.merge_algorithm = toString(merge_algorithm.load(std::memory_order_relaxed));

    for (const auto & source_part_name : source_part_names)
        res.source_part_names.emplace_back(source_part_name);

    for (const auto & source_part_path : source_part_paths)
        res.source_part_paths.emplace_back(source_part_path);

    return res;
}

MergeListElement::~MergeListElement()
{
    background_memory_tracker.adjustOnBackgroundTaskEnd(&getMemoryTracker());
}

}
