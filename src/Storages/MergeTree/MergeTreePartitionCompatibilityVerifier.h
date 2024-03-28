#pragma once

#include <Core/Field.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace DB
{

/*
 * Verifies that source and destination partitions are compatible.
 * To be compatible, one of the following criteria must be met:
 * 1. Destination partition expression columns are a subset of source partition columns; or
 * 2. Destination partition expression is monotonic on the source global min_max idx Range AND the computer partition id for
 * the source global min_max idx range is the same.
 *
 * If so, creates a new partition and calculates new partition id. (If the partition expression is the same,
 * the source partition id is used as the new partition id and the new partition is default constructed (not used))
 *
 * If not, an exception is thrown.
 * */

class MergeTreePartitionCompatibilityVerifier
{
public:
    using DataPart = IMergeTreeDataPart;
    using DataPartPtr = std::shared_ptr<const DataPart>;
    using DataPartsVector = std::vector<DataPartPtr>;

    static std::pair<MergeTreePartition, std::string> verifyCompatibilityAndCreatePartition(
        bool is_partition_exp_the_same,
        const MergeTreeData & source_data,
        const MergeTreeData & dst_data,
        const DataPartsVector & src_parts,
        const String & source_partition_id);
};

}
