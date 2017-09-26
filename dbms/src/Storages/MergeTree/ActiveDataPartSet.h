#pragma once

#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <mutex>
#include <common/DateLUT.h>
#include <Core/Types.h>
#include <map>


namespace DB
{

/** Supports multiple names of active parts of data.
  * Repeats part of the MergeTreeData functionality.
  * TODO: generalize with MergeTreeData. It is possible to leave this class approximately as is and use it from MergeTreeData.
  *       Then in MergeTreeData you can make map<String, DataPartPtr> data_parts and all_data_parts.
  */
class ActiveDataPartSet
{
public:
    ActiveDataPartSet(MergeTreeDataFormatVersion format_version_) : format_version(format_version_) {}
    ActiveDataPartSet(MergeTreeDataFormatVersion format_version_, const Strings & names);

    void add(const String & name);

    /// If not found, returns an empty string.
    String getContainingPart(const String & name) const;

    Strings getParts() const; /// In ascending order of the partition_id and block number.

    size_t size() const;

private:
    MergeTreeDataFormatVersion format_version;

    mutable std::mutex mutex;
    std::map<MergeTreePartInfo, String> part_info_to_name;

    /// Do not block mutex.
    void addImpl(const String & name);
    String getContainingPartImpl(const MergeTreePartInfo & part_info) const;
};

}
