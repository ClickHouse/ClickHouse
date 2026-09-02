#pragma once

#include <unordered_map>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreeDataFormatVersion.h>
#include <Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>

namespace DB
{

/// All drop ranges in ReplicatedQueue.
/// Used to postpone execution of entries affected by DROP RANGE
class DropPartsRanges
{
private:
    MergeTreeDataFormatVersion format_version;

    /// znode_name -> drop_range
    std::unordered_map<std::string, MergeTreePartInfo> drop_ranges;
public:

    explicit DropPartsRanges(MergeTreeDataFormatVersion format_version_)
        : format_version(format_version_)
    {}

    /// Entry is affected by DROP_PART and must be postponed
    bool isAffectedByDropPart(const ReplicatedMergeTreeLogEntry & entry, std::string & postpone_reason) const;

    /// Part is affected by DROP_PART and must be postponed
    bool isAffectedByDropPart(const std::string & new_part_name, std::string & postpone_reason) const;

    /// Already has equal DROP_RANGE. Don't need to assign new one
    bool hasDropPart(const MergeTreePartInfo & new_drop_range_info, MergeTreePartInfo * out_drop_range_info = nullptr) const;

    /// Add DROP_RANGE to map
    void addDropPart(const ReplicatedMergeTreeLogEntryPtr & entry);

    /// Remove DROP_RANGE from map
    void removeDropPart(const ReplicatedMergeTreeLogEntryPtr & entry);

};

}
