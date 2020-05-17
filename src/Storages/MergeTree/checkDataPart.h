#pragma once

#include <Storages/MergeTree/MergeTreeData.h>


namespace DB
{

/// Calculates checksums and compares them with checksums.txt.
IMergeTreeDataPart::Checksums checkDataPart(
    MergeTreeData::DataPartPtr data_part,
    bool require_checksums,
    std::function<bool()> is_cancelled = []{ return false; });

IMergeTreeDataPart::Checksums checkDataPart(
    const DiskPtr & disk,
    const String & full_relative_path,
    const NamesAndTypesList & columns_list,
    const MergeTreeDataPartType & part_type,
    bool require_checksums,
    std::function<bool()> is_cancelled = []{ return false; });

bool isNotEnoughMemoryErrorCode(int code);

}
