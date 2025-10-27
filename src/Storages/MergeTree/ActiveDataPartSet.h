#pragma once

#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <base/types.h>
#include <map>
#include <vector>

namespace Poco
{
class Logger;
}

namespace DB
{

using Strings = std::vector<String>;

/** Supports multiple names of active parts of data.
  * Repeats part of the MergeTreeData functionality.
  * TODO: generalize with MergeTreeData
  */
class ActiveDataPartSet
{
public:
    enum class AddPartOutcome : uint8_t
    {
        Added,
        HasCovering,
        HasIntersectingPart,
    };

    explicit ActiveDataPartSet(MergeTreeDataFormatVersion format_version_) : format_version(format_version_) {}
    ActiveDataPartSet(MergeTreeDataFormatVersion format_version_, const Strings & names);

    ActiveDataPartSet(const ActiveDataPartSet & other) = default;

    ActiveDataPartSet & operator=(const ActiveDataPartSet & other) = default;

    ActiveDataPartSet(ActiveDataPartSet && other) noexcept = default;

    void swap(ActiveDataPartSet & other) noexcept
    {
        std::swap(format_version, other.format_version);
        std::swap(part_info_to_name, other.part_info_to_name);
    }

    /// Returns true if the part was actually added. If out_replaced_parts != nullptr, it will contain
    /// parts that were replaced from the set by the newly added part.
    bool add(const String & name, Strings * out_replaced_parts = nullptr);
    bool add(const MergeTreePartInfo & part_info, const String & name, Strings * out_replaced_parts = nullptr);
    bool add(const MergeTreePartInfo & part_info, Strings * out_replaced_parts = nullptr);

    AddPartOutcome tryAddPart(const MergeTreePartInfo & part_info, String * out_reason = nullptr);

    bool remove(const MergeTreePartInfo & part_info)
    {
        return part_info_to_name.erase(part_info) > 0;
    }

    bool remove(const String & part_name)
    {
        return remove(MergeTreePartInfo::fromPartName(part_name, format_version));
    }

    /// Remove part and all covered parts from active set
    bool removePartAndCoveredParts(const String & part_name)
    {
        Strings parts_covered_by = getPartsCoveredBy(MergeTreePartInfo::fromPartName(part_name, format_version));
        bool result = true;
        result &= remove(part_name);
        for (const auto & part : parts_covered_by)
            result &= remove(part);

        return result;
    }

    /// Remove only covered parts from active set
    bool removePartsCoveredBy(const String & part_name)
    {
        Strings parts_covered_by = getPartsCoveredBy(MergeTreePartInfo::fromPartName(part_name, format_version));
        bool result = true;
        for (const auto & part : parts_covered_by)
            if (part != part_name)
                result &= remove(part);

        return result;
    }

    /// If not found, return an empty string.
    String getContainingPart(const MergeTreePartInfo & part_info) const;
    String getContainingPart(const String & name) const;

    Strings getPartsCoveredBy(const MergeTreePartInfo & part_info) const;
    std::vector<MergeTreePartInfo> getPartInfosCoveredBy(const MergeTreePartInfo & part_info) const;

    /// Returns parts in ascending order of the partition_id and block number.
    Strings getParts() const;
    std::vector<MergeTreePartInfo> getPartInfos() const;

    size_t size() const;

    void clear()
    {
        part_info_to_name.clear();
    }

    MergeTreeDataFormatVersion getFormatVersion() const { return format_version; }

    void checkIntersectingParts(const MergeTreePartInfo & part_info) const;
    void checkIntersectingParts(const String & name) const;

private:

    AddPartOutcome addImpl(const MergeTreePartInfo & part_info, const String & name, Strings * out_replaced_parts = nullptr, String * out_reason = nullptr);
    MergeTreeDataFormatVersion format_version;

    using PartInfoToName = std::map<MergeTreePartInfo, String>;
    PartInfoToName part_info_to_name;

    std::vector<std::map<MergeTreePartInfo, String>::const_iterator> getPartsCoveredByImpl(const MergeTreePartInfo & part_info) const;

    std::map<MergeTreePartInfo, String>::const_iterator getContainingPartImpl(const MergeTreePartInfo & part_info) const;
};

}
