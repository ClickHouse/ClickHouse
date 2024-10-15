#include <Storages/MergeTree/MergeTreeDataPartBuilder.h>
#include <Storages/MergeTree/MergeTreeDataPartCompact.h>
#include <Storages/MergeTree/MergeTreeDataPartWide.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_PART_TYPE;
}

MergeTreeDataPartBuilder::MergeTreeDataPartBuilder(
    const MergeTreeData & data_, String name_, VolumePtr volume_, String root_path_, String part_dir_)
    : data(data_)
    , name(std::move(name_))
    , volume(std::move(volume_))
    , root_path(std::move(root_path_))
    , part_dir(std::move(part_dir_))
{
}

MergeTreeDataPartBuilder::MergeTreeDataPartBuilder(
    const MergeTreeData & data_, String name_, MutableDataPartStoragePtr part_storage_)
    : data(data_)
    , name(std::move(name_))
    , part_storage(std::move(part_storage_))
{
}

std::shared_ptr<IMergeTreeDataPart> MergeTreeDataPartBuilder::build()
{
    using PartType = MergeTreeDataPartType;
    using PartStorageType = MergeTreeDataPartStorageType;

    if (!part_type)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot create part {}, because part type is not set", name);

    if (!part_storage)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot create part {}, because part storage is not set", name);

    if (parent_part && data.format_version == MERGE_TREE_DATA_OLD_FORMAT_VERSION)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot create projection part in MergeTree table created in old syntax");

    auto part_storage_type = part_storage->getType();
    if (!data.canUsePolymorphicParts() &&
        (part_type != PartType::Wide || part_storage_type != PartStorageType::Full))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot create part with type {} and storage type {} because table does not support polymorphic parts",
            part_type->toString(), part_storage_type.toString());
    }

    if (!part_info)
        part_info = MergeTreePartInfo::fromPartName(name, data.format_version);

    switch (part_type->getValue())
    {
        case PartType::Wide:
            return std::make_shared<MergeTreeDataPartWide>(data, name, *part_info, part_storage, parent_part);
        case PartType::Compact:
            return std::make_shared<MergeTreeDataPartCompact>(data, name, *part_info, part_storage, parent_part);
        default:
            throw Exception(ErrorCodes::UNKNOWN_PART_TYPE,
                "Unknown type of part {}", part_storage->getRelativePath());
    }
}

MutableDataPartStoragePtr MergeTreeDataPartBuilder::getPartStorageByType(
    MergeTreeDataPartStorageType storage_type_,
    const VolumePtr & volume_,
    const String & root_path_,
    const String & part_dir_)
{
    if (!volume_)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot create part storage, because volume is not specified");

    using Type = MergeTreeDataPartStorageType;
    switch (storage_type_.getValue())
    {
        case Type::Full:
            return std::make_shared<DataPartStorageOnDiskFull>(volume_, root_path_, part_dir_);
        default:
            throw Exception(ErrorCodes::UNKNOWN_PART_TYPE,
                "Unknown type of storage for part {}", fs::path(root_path_) / part_dir_);
    }
}

MergeTreeDataPartBuilder & MergeTreeDataPartBuilder::withPartInfo(MergeTreePartInfo part_info_)
{
    part_info = std::move(part_info_);
    return *this;
}

MergeTreeDataPartBuilder & MergeTreeDataPartBuilder::withParentPart(const IMergeTreeDataPart * parent_part_)
{
    if (parent_part_ && parent_part_->isProjectionPart())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Parent part cannot be projection");

    parent_part = parent_part_;
    return *this;
}

MergeTreeDataPartBuilder & MergeTreeDataPartBuilder::withPartType(MergeTreeDataPartType part_type_)
{
    part_type = part_type_;
    return *this;
}

MergeTreeDataPartBuilder & MergeTreeDataPartBuilder::withPartStorageType(MergeTreeDataPartStorageType storage_type_)
{
    part_storage = getPartStorageByType(storage_type_, volume, root_path, part_dir);
    return *this;
}

MergeTreeDataPartBuilder & MergeTreeDataPartBuilder::withPartFormat(MergeTreeDataPartFormat format_)
{
    withPartType(format_.part_type);
    return part_storage ? *this : withPartStorageType(format_.storage_type);
}

MergeTreeDataPartBuilder::PartStorageAndMarkType
MergeTreeDataPartBuilder::getPartStorageAndMarkType(
    const VolumePtr & volume_,
    const String & root_path_,
    const String & part_dir_)
{
    auto disk = volume_->getDisk();
    auto part_relative_path = fs::path(root_path_) / part_dir_;

    for (auto it = disk->iterateDirectory(part_relative_path); it->isValid(); it->next())
    {
        auto it_path = fs::path(it->name());
        auto ext = it_path.extension().string();

        if (MarkType::isMarkFileExtension(ext))
        {
            auto storage = getPartStorageByType(MergeTreeDataPartStorageType::Full, volume_, root_path_, part_dir_);
            return {std::move(storage), MarkType(ext)};
        }
    }

    return {};
}

MergeTreeDataPartBuilder & MergeTreeDataPartBuilder::withPartFormatFromDisk()
{
    if (part_storage)
        return withPartFormatFromStorage();
    return withPartFormatFromVolume();
}

MergeTreeDataPartBuilder & MergeTreeDataPartBuilder::withPartFormatFromVolume()
{
    assert(volume);
    auto [storage, mark_type] = getPartStorageAndMarkType(volume, root_path, part_dir);

    if (!storage || !mark_type)
    {
        /// Didn't find any data or mark file, suppose that part is empty.
        return withBytesAndRowsOnDisk(0, 0);
    }

    part_storage = std::move(storage);
    part_type = mark_type->part_type;
    return *this;
}

MergeTreeDataPartBuilder & MergeTreeDataPartBuilder::withPartFormatFromStorage()
{
    assert(part_storage);
    auto mark_type = MergeTreeIndexGranularityInfo::getMarksTypeFromFilesystem(*part_storage);

    if (!mark_type)
    {
        /// Didn't find any mark file, suppose that part is empty.
        return withBytesAndRowsOnDisk(0, 0);
    }

    part_type = mark_type->part_type;
    return *this;
}

MergeTreeDataPartBuilder & MergeTreeDataPartBuilder::withBytesAndRows(size_t bytes_uncompressed, size_t rows_count)
{
    return withPartFormat(data.choosePartFormat(bytes_uncompressed, rows_count));
}

MergeTreeDataPartBuilder & MergeTreeDataPartBuilder::withBytesAndRowsOnDisk(size_t bytes_uncompressed, size_t rows_count)
{
    return withPartFormat(data.choosePartFormatOnDisk(bytes_uncompressed, rows_count));
}

}
