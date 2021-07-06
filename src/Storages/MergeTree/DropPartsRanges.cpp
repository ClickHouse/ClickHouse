#include <Storages/MergeTree/DropPartsRanges.h>
#include <common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


bool DropPartsRanges::isAffectedByDropRange(const std::string & new_part_name, std::string & postpone_reason) const
{
    if (new_part_name.empty())
        return false;

    MergeTreePartInfo entry_info = MergeTreePartInfo::fromPartName(new_part_name, format_version);
    for (const auto & [znode, drop_range] : drop_ranges)
    {
        if (!drop_range.isDisjoint(entry_info))
        {
            postpone_reason = fmt::format("Has DROP RANGE with entry. Will postpone it's execution.", drop_range.getPartName());
            return true;
        }
    }

    return false;
}

bool DropPartsRanges::isAffectedByDropRange(const ReplicatedMergeTreeLogEntry & entry, std::string & postpone_reason) const
{
    return isAffectedByDropRange(entry.new_part_name, postpone_reason);
}

void DropPartsRanges::addDropRange(const ReplicatedMergeTreeLogEntryPtr & entry, Poco::Logger * /*log*/)
{
    if (entry->type != ReplicatedMergeTreeLogEntry::DROP_RANGE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to add entry of type {} to drop ranges, expected DROP_RANGE", entry->typeToString());

    //LOG_DEBUG(log, "ADD DROP RANGE {}", *entry->getDropRange(format_version));
    MergeTreePartInfo entry_info = MergeTreePartInfo::fromPartName(*entry->getDropRange(format_version), format_version);
    drop_ranges.emplace(entry->znode_name, entry_info);
}

void DropPartsRanges::removeDropRange(const ReplicatedMergeTreeLogEntryPtr & entry, Poco::Logger * /*log*/)
{
    if (entry->type != ReplicatedMergeTreeLogEntry::DROP_RANGE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to remove entry of type {} from drop ranges, expected DROP_RANGE", entry->typeToString());

    //LOG_DEBUG(log, "REMOVE DROP RANGE {}", *entry->getDropRange(format_version));
    drop_ranges.erase(entry->znode_name);
}

bool DropPartsRanges::hasDropRange(const MergeTreePartInfo & new_drop_range_info) const
{
    for (const auto & [znode_name, drop_range] : drop_ranges)
    {
        if (drop_range.contains(new_drop_range_info))
            return true;
    }

    return false;
}

}
