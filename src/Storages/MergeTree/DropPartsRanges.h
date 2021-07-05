#pragma once

#include <map>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreeDataFormatVersion.h>
#include <Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>

namespace DB
{

class DropPartsRanges
{
private:
    MergeTreeDataFormatVersion format_version;

    std::map<std::string, MergeTreePartInfo> drop_ranges;
public:

    explicit DropPartsRanges(MergeTreeDataFormatVersion format_version_)
        : format_version(format_version_)
    {}

    bool isAffectedByDropRange(const ReplicatedMergeTreeLogEntry & entry, std::string & postpone_reason) const;

    bool hasDropRange(const MergeTreePartInfo & new_drop_range_info) const;

    void addDropRange(const ReplicatedMergeTreeLogEntryPtr & entry, Poco::Logger * log);

    void removeDropRange(const ReplicatedMergeTreeLogEntryPtr & entry, Poco::Logger * log);

};

}
