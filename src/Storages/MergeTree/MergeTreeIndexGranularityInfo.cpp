#include <Storages/MergeTree/MergeTreeIndexGranularityInfo.h>
#include <Storages/MergeTree/MergeTreeData.h>


namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_PART_TYPE;
    extern const int INCORRECT_FILE_NAME;
}


MarkType::MarkType(std::string_view extension)
{
    if (extension.starts_with('.'))
        extension = extension.substr(1);

    if (extension.starts_with('c'))
    {
        compressed = true;
        extension = extension.substr(1);
    }

    if (!extension.starts_with("mrk"))
        throw Exception(ErrorCodes::INCORRECT_FILE_NAME, "Mark file extension does not start with .mrk or .cmrk: {}", extension);

    extension = extension.substr(strlen("mrk"));

    if (extension.empty())
    {
        adaptive = false;
        part_type = MergeTreeDataPartType::Wide;
    }
    else if (extension == "2")
    {
        adaptive = true;
        part_type = MergeTreeDataPartType::Wide;
    }
    else if (extension == "3")
    {
        adaptive = true;
        part_type = MergeTreeDataPartType::Compact;
    }
    else
        throw Exception(ErrorCodes::INCORRECT_FILE_NAME, "Unknown mark file extension: '{}'", extension);
}

MarkType::MarkType(bool adaptive_, bool compressed_, MergeTreeDataPartType::Value part_type_)
    : adaptive(adaptive_), compressed(compressed_), part_type(part_type_)
{
    if (!adaptive && part_type != MergeTreeDataPartType::Wide)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: non-Wide data part type with non-adaptive granularity");
    if (part_type == MergeTreeDataPartType::Unknown)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: unknown data part type");
}

bool MarkType::isMarkFileExtension(std::string_view extension)
{
    return extension.find("mrk") != std::string_view::npos;
}

std::string MarkType::getFileExtension() const
{
    std::string res = compressed ? ".cmrk" : ".mrk";

    if (!adaptive)
    {
        if (part_type != MergeTreeDataPartType::Wide)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: non-Wide data part type with non-adaptive granularity");
        return res;
    }

    switch (part_type)
    {
        case MergeTreeDataPartType::Wide:
            return res + "2";
        case MergeTreeDataPartType::Compact:
            return res + "3";
        case MergeTreeDataPartType::InMemory:
            return "";
        case MergeTreeDataPartType::Unknown:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: unknown data part type");
    }
}


std::optional<MarkType> MergeTreeIndexGranularityInfo::getMarksTypeFromFilesystem(const IDataPartStorage & data_part_storage)
{
    if (data_part_storage.exists())
        for (auto it = data_part_storage.iterate(); it->isValid(); it->next())
            if (it->isFile())
                if (std::string ext = fs::path(it->name()).extension(); MarkType::isMarkFileExtension(ext))
                    return MarkType(ext);
    return {};
}

MergeTreeIndexGranularityInfo::MergeTreeIndexGranularityInfo(const MergeTreeData & storage, MergeTreeDataPartType type_)
    : MergeTreeIndexGranularityInfo(storage, {storage.canUseAdaptiveGranularity(), storage.getSettings()->compress_marks, type_.getValue()})
{
}

MergeTreeIndexGranularityInfo::MergeTreeIndexGranularityInfo(const MergeTreeData & storage, MarkType mark_type_)
    : mark_type(mark_type_)
{
    fixed_index_granularity = storage.getSettings()->index_granularity;
}

void MergeTreeIndexGranularityInfo::changeGranularityIfRequired(const IDataPartStorage & data_part_storage)
{
    auto mrk_type = getMarksTypeFromFilesystem(data_part_storage);
    if (mrk_type && !mrk_type->adaptive)
    {
        mark_type.adaptive = false;
        index_granularity_bytes = 0;
    }
}

size_t MergeTreeIndexGranularityInfo::getMarkSizeInBytes(size_t columns_num) const
{
    if (mark_type.part_type == MergeTreeDataPartType::Wide)
        return mark_type.adaptive ? getAdaptiveMrkSizeWide() : getNonAdaptiveMrkSizeWide();
    else if (mark_type.part_type == MergeTreeDataPartType::Compact)
        return getAdaptiveMrkSizeCompact(columns_num);
    else if (mark_type.part_type == MergeTreeDataPartType::InMemory)
        return 0;
    else
        throw Exception(ErrorCodes::UNKNOWN_PART_TYPE, "Unknown part type");
}

size_t getAdaptiveMrkSizeCompact(size_t columns_num)
{
    /// Each mark contains number of rows in granule and two offsets for every column.
    return sizeof(UInt64) * (columns_num * 2 + 1);
}

}
