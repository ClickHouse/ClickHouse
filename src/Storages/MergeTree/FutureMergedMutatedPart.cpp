#include <Storages/MergeTree/FutureMergedMutatedPart.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void FutureMergedMutatedPart::assign(MergeTreeData::DataPartsVector parts_, MergeTreeData::DataPartsVector patch_parts_)
{
    if (parts_.empty())
        return;

    size_t sum_rows = 0;
    size_t sum_bytes_uncompressed = 0;
    MergeTreeDataPartType future_part_type;
    MergeTreeDataPartStorageType future_part_storage_type;
    UInt32 max_level = 0;

    for (const auto & part : parts_)
    {
        sum_rows += part->rows_count;
        sum_bytes_uncompressed += part->getTotalColumnsSize().data_uncompressed;
        future_part_type = std::min(future_part_type, part->getType());
        future_part_storage_type = std::min(future_part_storage_type, part->getDataPartStorage().getType());
        max_level = std::max(max_level, part->info.level);
    }

    auto chosen_format = parts_.front()->storage.choosePartFormat(sum_bytes_uncompressed, sum_rows, max_level + 1);
    future_part_type = std::min(future_part_type, chosen_format.part_type);
    future_part_storage_type = std::min(future_part_storage_type, chosen_format.storage_type);
    assign(std::move(parts_), std::move(patch_parts_), {future_part_type, future_part_storage_type});
}

void FutureMergedMutatedPart::assign(MergeTreeData::DataPartsVector parts_, MergeTreeData::DataPartsVector patch_parts_, MergeTreeDataPartFormat future_part_format)
{
    if (parts_.empty())
        return;

    for (const MergeTreeData::DataPartPtr & part : parts_)
    {
        const MergeTreeData::DataPartPtr & first_part = parts_.front();

        if (part->partition.value != first_part->partition.value)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempting to merge parts {} and {} that are in different partitions",
                first_part->name, part->name);
    }

    parts = std::move(parts_);
    patch_parts = std::move(patch_parts_);

    UInt32 max_level = 0;
    Int64 max_mutation = 0;

    for (const auto & part : parts)
    {
        max_level = std::max(max_level, part->info.level);
        max_mutation = std::max(max_mutation, part->info.mutation);
    }

    for (const auto & patch : patch_parts)
    {
        Int64 max_patch_version = patch->getSourcePartsSet().getMaxDataVersion();
        max_mutation = std::max(max_mutation, max_patch_version);
    }

    part_format = future_part_format;
    part_info.setPartitionId(parts.front()->info.getPartitionId());
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
    {
        name = part_info.getPartNameV1();
    }
}

void FutureMergedMutatedPart::updatePath(const MergeTreeData & storage, const IReservation * reservation)
{
    path = fs::path(storage.getFullPathOnDisk(reservation->getDisk())) / name;
    path += "/";
}

}
