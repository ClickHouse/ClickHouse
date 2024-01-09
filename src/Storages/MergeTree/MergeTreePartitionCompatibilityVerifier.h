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
 * If not, an exception is thrown.
 * */

class MergeTreePartitionCompatibilityVerifier
{
public:
    using DataPart = IMergeTreeDataPart;
    using DataPartPtr = std::shared_ptr<const DataPart>;
    using DataPartsVector = std::vector<DataPartPtr>;

    static void
    verify(const MergeTreeData & source_storage, const MergeTreeData & destination_storage, const DataPartsVector & source_parts);
};

}
