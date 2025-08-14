#pragma once
#include <Storages/ObjectStorage/Iterators/ObjectInfo.h>
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>

namespace DB {

struct ParsedDataFileInfo
{
    ParsedDataFileInfo(
        StorageObjectStorageConfigurationPtr configuration_,
        Iceberg::ManifestFileEntry data_object_,
        const std::vector<Iceberg::ManifestFileEntry> & position_deletes_objects_);
    String data_object_file_path_key;
    String data_object_file_path; // Full path to the data object file
    std::span<const Iceberg::ManifestFileEntry> position_deletes_objects;

    bool operator<(const ParsedDataFileInfo & other) const
    {
        return std::tie(data_object_file_path_key) < std::tie(other.data_object_file_path_key);
    }
};

struct IcebergDataObjectInfo : public ReadableObject
{
    IcebergDataObjectInfo(std::optional<ObjectMetadata> metadata_, ParsedDataFileInfo parsed_data_file_info_);
    ParsedDataFileInfo parsed_data_file_info;
    ActionsDAG schema_transformer;
    std::shared_ptr<NamesAndTypesList> initial_schema;

    bool suitableForNumsRowCache() const override { return !hasPositionDeleteTransformer(); }
    bool hasPositionDeleteTransformer() const override { return !parsed_data_file_info.position_deletes_objects.empty(); }
    std::shared_ptr<ISimpleTransform> getPositionDeleteTransformer(
        const SharedHeader & header, const std::optional<FormatSettings> & format_settings, ContextPtr context_) const override;

};

using IcebergDataObjectInfoPtr = std::shared_ptr<IcebergDataObjectInfo>;

}
