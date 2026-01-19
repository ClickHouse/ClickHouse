#pragma once

#include "config.h"

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#include <Storages/ObjectStorage/DataLakes/Common/AvroForIcebergDeserializer.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Core/Field.h>

#include <cstdint>

namespace DB::Iceberg
{

enum class ManifestEntryStatus : uint8_t
{
    EXISTING = 0,
    ADDED = 1,
    DELETED = 2,
};

enum class FileContentType : uint8_t
{
    DATA = 0,
    POSITION_DELETE = 1,
    EQUALITY_DELETE = 2
};


enum class ManifestFileContentType
{
    DATA = 0,
    DELETE = 1
};

String FileContentTypeToString(FileContentType type);

struct ColumnInfo
{
    std::optional<Int64> rows_count;
    std::optional<Int64> bytes_size;
    std::optional<Int64> nulls_count;
    std::optional<DB::Range> hyperrectangle;
};

struct PartitionSpecsEntry
{
    Int32 source_id;
    String transform_name;
    String partition_name;
};
using PartitionSpecification = std::vector<PartitionSpecsEntry>;

/// Description of Data file in manifest file
struct ManifestFileEntry
{
    // It's the original string in the Iceberg metadata
    String file_path_key;
    // It's a processed file path to be used by Object Storage
    String file_path;
    Int64 row_number;

    ManifestEntryStatus status;
    Int64 added_sequence_number;

    Int64 snapshot_id;
    Int32 schema_id;

    DB::Row partition_key_value;
    PartitionSpecification common_partition_specification;
    std::unordered_map<Int32, ColumnInfo> columns_infos;

    String file_format;
    std::optional<String> reference_data_file_path; // For position delete files only.
    std::optional<std::vector<Int32>> equality_ids;
};

/**
 * Manifest file has the following format: '/iceberg_data/db/table_name/metadata/c87bfec7-d36c-4075-ad04-600b6b0f2020-m0.avro'
 *
 * `manifest file` is different in format version V1 and V2 and has the following contents:
 *                        v1     v2
 * status                 req    req
 * snapshot_id            req    opt
 * sequence_number               opt
 * file_sequence_number          opt
 * data_file              req    req
 * Example format version V1:
 * ┌─status─┬─────────snapshot_id─┬─data_file───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
 * │      1 │ 2819310504515118887 │ ('/iceberg_data/db/table_name/data/00000-1-3edca534-15a0-4f74-8a28-4733e0bf1270-00001.parquet','PARQUET',(),100,1070,67108864,[(1,233),(2,210)],[(1,100),(2,100)],[(1,0),(2,0)],[],[(1,'\0'),(2,'0')],[(1,'c'),(2,'99')],NULL,[4],0) │
 * └────────┴─────────────────────┴─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
 * Example format version V2:
 * ┌─status─┬─────────snapshot_id─┬─sequence_number─┬─file_sequence_number─┬─data_file───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
 * │      1 │ 5887006101709926452 │            ᴺᵁᴸᴸ │                 ᴺᵁᴸᴸ │ (0,'/iceberg_data/db/table_name/data/00000-1-c8045c90-8799-4eac-b957-79a0484e223c-00001.parquet','PARQUET',(),100,1070,[(1,233),(2,210)],[(1,100),(2,100)],[(1,0),(2,0)],[],[(1,'\0'),(2,'0')],[(1,'c'),(2,'99')],NULL,[4],[],0) │
 * └────────┴─────────────────────┴─────────────────┴──────────────────────┴─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
 * In case of partitioned data we'll have extra directory partition=value:
 * ─status─┬─────────snapshot_id─┬─data_file──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
 * │      1 │ 2252246380142525104 │ ('/iceberg_data/db/table_name/data/a=0/00000-1-c9535a00-2f4f-405c-bcfa-6d4f9f477235-00001.parquet','PARQUET',(0),1,631,67108864,[(1,46),(2,48)],[(1,1),(2,1)],[(1,0),(2,0)],[],[(1,'\0\0\0\0\0\0\0\0'),(2,'1')],[(1,'\0\0\0\0\0\0\0\0'),(2,'1')],NULL,[4],0) │
 * │      1 │ 2252246380142525104 │ ('/iceberg_data/db/table_name/data/a=1/00000-1-c9535a00-2f4f-405c-bcfa-6d4f9f477235-00002.parquet','PARQUET',(1),1,631,67108864,[(1,46),(2,48)],[(1,1),(2,1)],[(1,0),(2,0)],[],[(1,'\0\0\0\0\0\0\0'),(2,'2')],[(1,'\0\0\0\0\0\0\0'),(2,'2')],NULL,[4],0) │
 * │      1 │ 2252246380142525104 │ ('/iceberg_data/db/table_name/data/a=2/00000-1-c9535a00-2f4f-405c-bcfa-6d4f9f477235-00003.parquet','PARQUET',(2),1,631,67108864,[(1,46),(2,48)],[(1,1),(2,1)],[(1,0),(2,0)],[],[(1,'\0\0\0\0\0\0\0'),(2,'3')],[(1,'\0\0\0\0\0\0\0'),(2,'3')],NULL,[4],0) │
 * └────────┴─────────────────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
 */

class ManifestFileContent : public boost::noncopyable
{
public:
    explicit ManifestFileContent(
        const AvroForIcebergDeserializer & manifest_file_deserializer,
        const String & manifest_file_name,
        Int32 format_version_,
        const String & common_path,
        IcebergSchemaProcessor & schema_processor,
        Int64 inherited_sequence_number,
        Int64 inherited_snapshot_id,
        const std::string & table_location,
        DB::ContextPtr context,
        const String & path_to_manifest_file_);

    const std::vector<ManifestFileEntry> & getFilesWithoutDeleted(FileContentType content_type) const;

    bool hasPartitionKey() const;
    const DB::KeyDescription & getPartitionKeyDescription() const;
    /// Get size in bytes of how much memory one instance of this ManifestFileContent class takes.
    /// Used for in-memory caches size accounting.
    size_t getSizeInMemory() const;

    /// Fields with rows count in manifest files are optional
    /// they can be absent.
    std::optional<Int64> getRowsCountInAllFilesExcludingDeleted(FileContentType content) const;
    std::optional<Int64> getBytesCountInAllDataFilesExcludingDeleted() const;

    bool hasBoundsInfoInManifests() const;
    const std::set<Int32> & getColumnsIDsWithBounds() const;
    const String & getPathToManifestFile() const { return path_to_manifest_file; }

    ManifestFileContent(ManifestFileContent &&) = delete;
    ManifestFileContent & operator=(ManifestFileContent &&) = delete;

private:

    PartitionSpecification common_partition_specification;
    void sortManifestEntriesBySchemaId(std::vector<ManifestFileEntry> & files);

    std::optional<DB::KeyDescription> partition_key_description;
    // Size - number of files
    std::vector<ManifestFileEntry> data_files_without_deleted;
    // Partition level deletes files
    std::vector<ManifestFileEntry> position_deletes_files_without_deleted;
    std::vector<ManifestFileEntry> equality_deletes_files;

    std::set<Int32> column_ids_which_have_bounds;
    String path_to_manifest_file;
};

using ManifestFilePtr = std::shared_ptr<ManifestFileContent>;

bool operator<(const PartitionSpecification & lhs, const PartitionSpecification & rhs);
bool operator<(const DB::Row & lhs, const DB::Row & rhs);


std::weak_ordering operator<=>(const ManifestFileEntry & lhs, const ManifestFileEntry & rhs);
}

#endif
