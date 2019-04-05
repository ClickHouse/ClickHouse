#pragma once

#include <Storages/MergeTree/MergeTreePartInfo.h>
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
    struct PartPathName
    {
        /// path + name is absolute path to DataPart
        String path;
        String name;
    };
    using PartPathNames = std::vector<PartPathName>;


    ActiveDataPartSet(MergeTreeDataFormatVersion format_version_) : format_version(format_version_) {}
    ActiveDataPartSet(MergeTreeDataFormatVersion format_version_, const PartPathNames & names);

    ActiveDataPartSet(const ActiveDataPartSet & other)
        : format_version(other.format_version)
        , part_info_to_name(other.part_info_to_name)
    {}

    ActiveDataPartSet(ActiveDataPartSet && other) noexcept { swap(other); }

    void swap(ActiveDataPartSet & other) noexcept
    {
        std::swap(format_version, other.format_version);
        std::swap(part_info_to_name, other.part_info_to_name);
    }

    ActiveDataPartSet & operator=(const ActiveDataPartSet & other)
    {
        if (&other != this)
        {
            ActiveDataPartSet tmp(other);
            swap(tmp);
        }
        return *this;
    }

    /// Returns true if the part was actually added. If out_replaced_parts != nullptr, it will contain
    /// parts that were replaced from the set by the newly added part.
    bool add(const String & path, const String & name, PartPathNames * out_replaced_parts = nullptr);

    bool remove(const MergeTreePartInfo & part_info)
    {
        return part_info_to_name.erase(part_info) > 0;
    }

    bool remove(const String & part_name)
    {
        return remove(MergeTreePartInfo::fromPartName(part_name, format_version));
    }

    /// If not found, return an empty string.
    PartPathName getContainingPart(const MergeTreePartInfo & part_info) const;
    PartPathName getContainingPart(const String & name) const;

    PartPathNames getPartsCoveredBy(const MergeTreePartInfo & part_info) const;

    /// Returns parts in ascending order of the partition_id and block number.
    PartPathNames getParts() const;

    size_t size() const;

    MergeTreeDataFormatVersion getFormatVersion() const { return format_version; }

private:
    MergeTreeDataFormatVersion format_version;
    std::map<MergeTreePartInfo, PartPathName> part_info_to_name;

    std::map<MergeTreePartInfo, PartPathName>::const_iterator getContainingPartImpl(const MergeTreePartInfo & part_info) const;
};

}
