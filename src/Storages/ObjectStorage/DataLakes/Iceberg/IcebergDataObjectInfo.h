#pragma once
#include "config.h"

#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/IObjectIterator.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/ObjectInfo.h>
#include <base/defines.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
};

struct IcebergDataObjectInfo : public DB::ObjectInfoOneFile
{
#if USE_AVRO
    explicit IcebergDataObjectInfo(Iceberg::ManifestFileEntry data_manifest_file_entry_, const std::vector<Iceberg::ManifestFileEntry> & position_deletes_, const String& format);
#endif
    explicit IcebergDataObjectInfo(
        String data_object_file_path_,
        String data_object_file_path_key_,
        Int32 read_schema_id_,
        std::pair<size_t, size_t> position_deletes_objects_range_);


    String data_object_file_path_key; // Full path to the data object file
    Int32 read_schema_id;
    std::pair<size_t, size_t> position_deletes_objects_range;

    bool hasPositionDeleteTransformer() const override
    {
        return position_deletes_objects_range.first < position_deletes_objects_range.second;
    }
    std::optional<DataLakeObjectMetadata> & getDataLakeMetadata() override
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Data lake metadata is not supported for plain object info");
    }
    const std::optional<DataLakeObjectMetadata> & getDataLakeMetadata() const override
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Data lake metadata is not supported for plain object info");
    }
    void setDataLakeMetadata(std::optional<DataLakeObjectMetadata>) override
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Data lake metadata is not supported for plain object info");
    }

    bool suitableForNumsRowCache() const override { return false; }
};

using IcebergDataObjectInfoPtr = std::shared_ptr<IcebergDataObjectInfo>;

}
