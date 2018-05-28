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
  * TODO: generalize with MergeTreeData
  */
class ActiveDataPartSet
{
public:
    ActiveDataPartSet(MergeTreeDataFormatVersion format_version_) : format_version(format_version_) {}
    ActiveDataPartSet(MergeTreeDataFormatVersion format_version_, const Strings & names);

    void add(const String & name);

    /// If not found, returns an empty string.
    String getContainingPart(const String & name) const;

    /// Returns parts in ascending order of the partition_id and block number.
    Strings getParts() const;

    size_t size() const;

    /// Do not block mutex.
    void addUnlocked(const String & name);
    String getContainingPartUnlocked(const MergeTreePartInfo & part_info) const;
    Strings getPartsUnlocked() const;

private:
    MergeTreeDataFormatVersion format_version;

    mutable std::mutex mutex;
    std::map<MergeTreePartInfo, String> part_info_to_name;
};

}
