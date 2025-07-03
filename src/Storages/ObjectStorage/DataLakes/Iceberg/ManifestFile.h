#pragma once

#include "config.h"

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/AvroForIcebergDeserializer.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Core/Field.h>

#include <cstdint>
#include <variant>

namespace Iceberg
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
    POSITION_DELETES = 1,
    EQUALITY_DELETES = 2,
};

struct DataFileEntry
{
    String file_name;
};

struct ColumnInfo
{
    std::optional<Int64> rows_count;
    std::optional<Int64> bytes_size;
    std::optional<Int64> nulls_count;
    std::optional<DB::Range> hyperrectangle;
};

using FileEntry = std::variant<DataFileEntry>; // In the future we will add PositionalDeleteFileEntry and EqualityDeleteFileEntry here

/// Description of Data file in manifest file
struct ManifestFileEntry
{
    ManifestEntryStatus status;
    Int64 added_sequence_number;

    FileEntry file;
    DB::Row partition_key_value;
    std::unordered_map<Int32, ColumnInfo> columns_infos;
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

class ManifestFileContent
{
public:
    explicit ManifestFileContent(
        const AvroForIcebergDeserializer & manifest_file_deserializer,
        Int32 format_version_,
        const String & common_path,
        Int32 schema_id_,
        Poco::JSON::Object::Ptr schema_object_,
        const DB::IcebergSchemaProcessor & schema_processor,
        Int64 inherited_sequence_number,
        const std::string & table_location,
        DB::ContextPtr context);

    const std::vector<ManifestFileEntry> & getFiles() const;
    Int32 getSchemaId() const;

    bool hasPartitionKey() const;
    const DB::KeyDescription & getPartitionKeyDescription() const;
    Poco::JSON::Object::Ptr getSchemaObject() const { return schema_object; }
    /// Get size in bytes of how much memory one instance of this ManifestFileContent class takes.
    /// Used for in-memory caches size accounting.
    size_t getSizeInMemory() const;

    /// Fields with rows count in manifest files are optional
    /// they can be absent.
    std::optional<Int64> getRowsCountInAllDataFilesExcludingDeleted() const;
    std::optional<Int64> getBytesCountInAllDataFiles() const;

    bool hasBoundsInfoInManifests() const;
    const std::set<Int32> & getColumnsIDsWithBounds() const;
private:

    Int32 schema_id;
    Poco::JSON::Object::Ptr schema_object;
    std::optional<DB::KeyDescription> partition_key_description;
    // Size - number of files
    std::vector<ManifestFileEntry> files;

    std::set<Int32> column_ids_which_have_bounds;

};

using ManifestFilePtr = std::shared_ptr<const ManifestFileContent>;

}

#endif
