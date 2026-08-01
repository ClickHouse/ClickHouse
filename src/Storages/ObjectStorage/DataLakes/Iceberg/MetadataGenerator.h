#pragma once

#include <Storages/ObjectStorage/DataLakes/Iceberg/FileNamesGenerator.h>

namespace DB
{

#if USE_AVRO

class MetadataGenerator
{
public:
    explicit MetadataGenerator(Poco::JSON::Object::Ptr metadata_object_);

    struct NextMetadataResult
    {
        Poco::JSON::Object::Ptr snapshot = nullptr;
        String metadata_path;
        String storage_metadata_path;
    };

    NextMetadataResult generateNextMetadata(
        FileNamesGenerator & generator,
        const String & metadata_filename,
        Int64 parent_snapshot_id,
        Int64 added_files,
        Int64 added_records,
        Int64 added_files_size,
        Int64 num_partitions,
        Int64 added_delete_files,
        Int64 num_deleted_rows,
        std::optional<Int64> user_defined_snapshot_id = std::nullopt,
        std::optional<Int64> user_defined_timestamp = std::nullopt);

    void generateAddColumnMetadata(const String & column_name, DataTypePtr type);
    void generateDropColumnMetadata(const String & column_name);
    void generateModifyColumnMetadata(const String & column_name, DataTypePtr type);
    void generateRenameColumnMetadata(const String & column_name, const String & new_column_name);

private:
    Poco::JSON::Object::Ptr metadata_object;

    pcg64_fast gen;
    std::uniform_int_distribution<Int32> dis;

    Int64 getMaxSequenceNumber();
    Poco::JSON::Object::Ptr getParentSnapshot(Int64 parent_snapshot_id);
};

#endif

}
