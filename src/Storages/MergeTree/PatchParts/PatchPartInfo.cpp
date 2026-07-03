#include <Storages/MergeTree/PatchParts/PatchPartInfo.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/IMergeTreeDataPartInfoForReader.h>
#include <fmt/ranges.h>

namespace DB
{

template<> String PatchPartInfo::describe() const
{
    return fmt::format("PatchPartInfo(mode: {}, name: {}, source parts: [{}], source_data_version: {}, perform_alter_conversions: {})",
        mode,
        part->name,
        fmt::join(source_parts, ", "),
        source_data_version,
        perform_alter_conversions);
}

template<> String PatchPartInfoForReader::describe() const
{
    return fmt::format("PatchPartInfoForReader(mode: {}, name: {}, source parts: [{}], source_data_version: {}, perform_alter_conversions: {})",
        mode,
        part->getPartName(),
        fmt::join(source_parts, ", "),
        source_data_version,
        perform_alter_conversions);
}

}
