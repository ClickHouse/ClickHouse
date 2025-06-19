#pragma once

#include <Poco/UUIDGenerator.h>
#include "config.h"

#if USE_AVRO

#include <Common/randomSeed.h>
#include <Storages/PartitionedSink.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Interpreters/Context_fwd.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <Compiler.hh>
#include <ValidSchema.hh>
#include <Generic.hh>
#include <Encoder.hh>
#include <Stream.hh>

namespace DB
{

class FileNamesGenerator
{
public:
    FileNamesGenerator(const String & data_dir_, const String & metadata_dir_);

    String generateDataFileName();
    String generateManifestEntryName();
    String generateManifestListName();
    String generateMetadataName();

private:
    Poco::UUIDGenerator uuid_generator;
    String data_dir;
    String metadata_dir;
};

class ManifestFileGenerator
{
public:
    explicit ManifestFileGenerator(Poco::JSON::Object::Ptr metadata);

    String generateManifestFile(const String & data_file_name, Poco::JSON::Object::Ptr new_snapshot);

    static constexpr const char * f_format_version = "format-version";
private:
    avro::ValidSchema schema;
};

class ManifestListGenerator
{
public:
    explicit ManifestListGenerator(Poco::JSON::Object::Ptr metadata_);

    String generateManifestList(const String & manifest_entry_name,
        Poco::JSON::Object::Ptr new_snapshot,
        Int32 manifest_length);

    static constexpr const char * f_manifest_path = "manifest_path";
    static constexpr const char * f_manifest_length = "manifest_length";
    static constexpr const char * f_partition_spec_id = "partition_spec_id";
    static constexpr const char * f_content = "content";
    static constexpr const char * f_sequence_number = "sequence_number";
    static constexpr const char * f_min_sequence_number = "min_sequence_number";
    static constexpr const char * f_added_snapshot_id = "added_snapshot_id";
    static constexpr const char * f_added_files_count = "added_files_count";
    static constexpr const char * f_existing_files_count = "existing_files_count";
    static constexpr const char * f_deleted_files_count = "deleted_files_count";
    static constexpr const char * f_added_rows_count = "added_rows_count";
    static constexpr const char * f_existing_rows_count = "existing_rows_count";
    static constexpr const char * f_deleted_rows_count = "deleted_rows_count";
private:
    Poco::JSON::Object::Ptr metadata;
    avro::ValidSchema schema;
};

class MetadataGenerator
{
public:
    explicit MetadataGenerator(Poco::JSON::Object::Ptr metadata_object_);

    Poco::JSON::Object::Ptr generateNextMetadata(
        const String & manifest_list_name,
        Int64 parent_snapshot_id,
        Int32 added_files,
        Int32 added_records,
        Int32 added_files_size
    );

    static constexpr const char * f_sequence_number = "sequence-number";
    static constexpr const char * f_snapshots = "snapshots";
    static constexpr const char * f_snapshot_id = "snapshot-id";
    static constexpr const char * f_parent_snapshot_id = "parent-snapshot-id";
    static constexpr const char * f_timestamp_ms = "timestamp-ms";
    static constexpr const char * f_added_data_files = "added-data-files";
    static constexpr const char * f_added_records = "added-records";
    static constexpr const char * f_added_files_size = "added-files-size";
    static constexpr const char * f_changed_partition_count = "changed-partition-count";
    static constexpr const char * f_total_records = "total-records";
    static constexpr const char * f_total_files_size = "total-files-size";
    static constexpr const char * f_total_data_files = "total-data-files";
    static constexpr const char * f_total_delete_files = "total-delete-files";
    static constexpr const char * f_total_position_deletes = "total-position-deletes";
    static constexpr const char * f_total_equality_deletes = "total-equality-deletes";
    static constexpr const char * f_manifest_list = "manifest-list";
    static constexpr const char * f_schema_id = "schema-id";
    static constexpr const char * f_summary = "summary";

private:
    Poco::JSON::Object::Ptr metadata_object;

    pcg64_fast gen;
    std::uniform_int_distribution<Int32> dis;

    Int64 getMaxSequenceNumber();
    Poco::JSON::Object::Ptr getParentSnapshot(Int64 parent_snapshot_id);
};

class IcebergStorageSink : public SinkToStorage
{
public:
    using ConfigurationPtr = StorageObjectStorage::ConfigurationPtr;

    IcebergStorageSink(
        ObjectStoragePtr object_storage,
        ConfigurationPtr configuration,
        const std::optional<FormatSettings> & format_settings_,
        const Block & sample_block_,
        ContextPtr context);

    ~IcebergStorageSink() override;

    String getName() const override { return "IcebergStorageSink"; }

    void consume(Chunk & chunk) override;

    void onFinish() override;

private:
    const Block sample_block;
    std::unique_ptr<WriteBuffer> write_buf;
    OutputFormatPtr writer;

    void finalizeBuffers();
    void releaseBuffers();
    void cancelBuffers();
};

}

#endif
