#pragma once

#include <Storages/MergeTree/MergeTreeData.h>


namespace DB
{

/** Completely checks the part data
    * - Calculates checksums and compares them with checksums.txt.
    * - For arrays and strings, checks the correspondence of the size and amount of data.
    * - Checks the correctness of marks.
    * Throws an exception if the part is corrupted or if the check fails (TODO: you can try to separate these cases).
    */
MergeTreeData::DataPart::Checksums checkDataPart(
    MergeTreeData::DataPartPtr data_part,
    bool require_checksums,
    const DataTypes & primary_key_data_types,    /// Check the primary key. If it is not necessary, pass an empty array.
    const MergeTreeIndices & indices = {}, /// Check skip indices
    std::function<bool()> is_cancelled = []{ return false; });

MergeTreeData::DataPart::Checksums checkDataPart(
    const String & full_path,
    const MergeTreeIndexGranularity & index_granularity,
    const String & marks_file_extension,
    bool require_checksums,
    const DataTypes & primary_key_data_types,    /// Check the primary key. If it is not necessary, pass an empty array.
    const MergeTreeIndices & indices = {}, /// Check skip indices
    std::function<bool()> is_cancelled = []{ return false; });
}
