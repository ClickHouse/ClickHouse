#pragma once

#include <Storages/MergeTree/MergeTreeIndexGranularityInfo.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreeDataPartType.h>
#include <optional>

namespace DB
{

class IDataPartStorage;
class IMergeTreeDataPart;
class IVolume;
class IDisk;
class MergeTreeData;

using MutableDataPartStoragePtr = std::shared_ptr<IDataPartStorage>;
using VolumePtr = std::shared_ptr<IVolume>;

/// Class that helps to create a data part with different variations of arguments.
class MergeTreeDataPartBuilder
{
public:
    MergeTreeDataPartBuilder(const MergeTreeData & data_, String name_, VolumePtr volume_, String root_path_, String part_dir_, const ReadSettings & read_settings_);
    MergeTreeDataPartBuilder(const MergeTreeData & data_, String name_, MutableDataPartStoragePtr part_storage_, const ReadSettings & read_settings_);

    std::shared_ptr<IMergeTreeDataPart> build();

    using Self = MergeTreeDataPartBuilder;

    Self & withPartInfo(MergeTreePartInfo part_info_);
    Self & withParentPart(const IMergeTreeDataPart * parent_part_);
    Self & withPartType(MergeTreeDataPartType part_type_);
    Self & withPartStorageType(MergeTreeDataPartStorageType storage_type_);
    Self & withPartFormat(MergeTreeDataPartFormat format_);
    Self & withPartFormatFromDisk();
    Self & withBytesAndRows(size_t bytes_uncompressed, size_t rows_count);
    Self & withBytesAndRowsOnDisk(size_t bytes_uncompressed, size_t rows_count);

    using PartStorageAndMarkType = std::pair<MutableDataPartStoragePtr, std::optional<MarkType>>;

    static PartStorageAndMarkType getPartStorageAndMarkType(
        const VolumePtr & volume_,
        const String & root_path_,
        const String & part_dir_,
        const ReadSettings & read_settings);

private:
    Self & withPartFormatFromVolume();
    Self & withPartFormatFromStorage();

    static MutableDataPartStoragePtr getPartStorageByType(
        MergeTreeDataPartStorageType storage_type_,
        const VolumePtr & volume_,
        const String & root_path_,
        const String & part_dir_,
        const ReadSettings & read_settings);

    const MergeTreeData & data;
    const String name;
    const VolumePtr volume;
    const String root_path;
    const String part_dir;

    std::optional<MergeTreePartInfo> part_info;
    std::optional<MergeTreeDataPartType> part_type;
    MutableDataPartStoragePtr part_storage;
    const IMergeTreeDataPart * parent_part = nullptr;

    const ReadSettings read_settings;
};

}
