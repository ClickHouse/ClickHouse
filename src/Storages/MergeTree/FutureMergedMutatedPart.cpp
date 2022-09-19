#include "Storages/MergeTree/FutureMergedMutatedPart.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

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

void FutureMergedMutatedPart::updatePath(const MergeTreeData & storage, const IReservation * reservation)
{
    path = storage.getFullPathOnDisk(reservation->getDisk()) + name + "/";
}

}
