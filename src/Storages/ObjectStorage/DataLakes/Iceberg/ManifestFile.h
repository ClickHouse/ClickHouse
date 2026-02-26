#pragma once

#include <Common/SharedLockGuard.h>
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

struct ManifestFileCacheableInfo
{
    std::shared_ptr<AvroForIcebergDeserializer> deserializer;
    size_t file_bytes_size;
};

/// Description of Data file in manifest file
struct ManifestFileEntry : public boost::noncopyable
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
    std::optional<String> lower_reference_data_file_path; // For position delete files only.
    std::optional<String> upper_reference_data_file_path; // For position delete files only.
    std::optional<std::vector<Int32>> equality_ids;

    /// Data file is sorted with this sort_order_id (can be read from metadata.json)
    std::optional<Int32> sort_order_id;

    String dumpDeletesMatchingInfo() const;

    ManifestFileEntry(
        const String& file_path_key_,
        const String& file_path_,
        Int64 row_number_,
        ManifestEntryStatus status_,
        Int64 added_sequence_number_,
        Int64 snapshot_id_,
        Int32 schema_id_,
        DB::Row& partition_key_value_,
        PartitionSpecification& common_partition_specification_,
        std::unordered_map<Int32, ColumnInfo>& columns_infos_,
        const String& file_format_,
        std::optional<String> lower_reference_data_file_path_,
        std::optional<String> upper_reference_data_file_path_,
        std::optional<std::vector<Int32>> equality_ids_,
        std::optional<Int32> sort_order_id_)
        : file_path_key(file_path_key_)
        , file_path(file_path_)
        , row_number(row_number_)
        , status(status_)
        , added_sequence_number(added_sequence_number_)
        , snapshot_id(snapshot_id_)
        , schema_id(schema_id_)
        , partition_key_value(std::move(partition_key_value_))
        , common_partition_specification(common_partition_specification_)
        , columns_infos(std::move(columns_infos_))
        , file_format(file_format_)
        , lower_reference_data_file_path(lower_reference_data_file_path_)
        , upper_reference_data_file_path(upper_reference_data_file_path_)
        , equality_ids(std::move(equality_ids_))
        , sort_order_id(sort_order_id_)
    {
    }
};

using ManifestFileEntryPtr = std::shared_ptr<const ManifestFileEntry>;

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

class ManifestFileIterator : public boost::noncopyable
{
public:
    struct ManifestFileEntriesHandle
    {
        const std::vector<ManifestFileEntryPtr> & getFilesWithoutDeleted() const { return *files; }

        ManifestFileEntriesHandle(const std::vector<ManifestFileEntryPtr> * files_, SharedLockGuard<SharedMutex> shared_lock_)
            : files(files_)
            , lock(std::move(shared_lock_))
        {
        }

        const std::vector<ManifestFileEntryPtr> & operator*() const { return getFilesWithoutDeleted(); }
        const std::vector<ManifestFileEntryPtr> * operator->() const { return files; }

    private:
        const std::vector<ManifestFileEntryPtr> * files;
        SharedLockGuard<SharedMutex> lock;
    };

    explicit ManifestFileIterator(
        std::shared_ptr<AvroForIcebergDeserializer> manifest_file_deserializer,
        const String & manifest_file_name,
        Int32 format_version_,
        const String & common_path,
        IcebergSchemaProcessor & schema_processor,
        Int64 inherited_sequence_number,
        Int64 inherited_snapshot_id,
        const std::string & table_location,
        DB::ContextPtr context,
        const String & path_to_manifest_file_,
        bool use_partition_pruning_,
        std::shared_ptr<const ActionsDAG> filter_dag_,
        Int32 table_snapshot_schema_id_);

    ManifestFileEntriesHandle getFilesWithoutDeletedHandle(FileContentType content_type) const;

    bool hasPartitionKey() const;
    const DB::KeyDescription & getPartitionKeyDescription() const;
    /// Get size in bytes of how much memory one instance of this ManifestFileContent class takes.
    /// Used for in-memory caches size accounting.
    size_t getSizeInMemory() const;

    /// Fields with rows count in manifest files are optional
    /// they can be absent.
    std::optional<Int64> getRowsCountInAllFilesExcludingDeleted(FileContentType content) const;
    std::optional<Int64> getBytesCountInAllDataFilesExcludingDeleted() const;

    const String & getPathToManifestFile() const { return path_to_manifest_file; }

    bool areAllDataFilesSortedBySortOrderID(Int32 sort_order_id) const;

    /// Returns true if all manifest file entries have been processed
    bool isInitialized() const;
    /// Process the next manifest file entry and return it. Returns nullptr if all entries have been processed.
    ManifestFileEntryPtr next();

    ManifestFileIterator(ManifestFileIterator &&) = delete;
    ManifestFileIterator & operator=(ManifestFileIterator &&) = delete;

    size_t getFileBytesSize() const { return file_bytes_size; }

private:
    void preinitialize(const String & manifest_file_name, const String & common_path, DB::ContextPtr context);

    ManifestFileEntryPtr processRow(size_t row_index);

    PartitionSpecification common_partition_specification;

    std::optional<DB::KeyDescription> partition_key_description;

    /// Mutex to protect concurrent access to file vectors during iteration
    mutable SharedMutex files_mutex;

    // Size - number of files
    std::vector<ManifestFileEntryPtr> data_files_without_deleted TSA_GUARDED_BY(files_mutex);
    // Partition level deletes files
    std::vector<ManifestFileEntryPtr> position_deletes_files_without_deleted TSA_GUARDED_BY(files_mutex);
    std::vector<ManifestFileEntryPtr> equality_deletes_files TSA_GUARDED_BY(files_mutex);

    // State for iterative initialization
    std::shared_ptr<AvroForIcebergDeserializer> manifest_file_deserializer;
    size_t file_bytes_size;
    String path_to_manifest_file;
    size_t total_rows = 0;
    std::atomic<size_t> current_row_index = 0;
    std::atomic<bool> fully_initialized = false;
    Int32 format_version = 0;
    String common_path;
    IcebergSchemaProcessor * schema_processor_ptr = nullptr;
    Int64 inherited_sequence_number = 0;
    Int64 inherited_snapshot_id = 0;
    String table_location;
    DB::ContextPtr context;
    String manifest_file_name;
    Int32 manifest_schema_id = 0;

    std::atomic<std::size_t> min_max_index_pruned_files = 0;
    std::atomic<std::size_t> partition_pruned_files = 0;
    bool use_partition_pruning;
    const std::shared_ptr<const ActionsDAG> filter_dag;
    const Int32 table_snapshot_schema_id;
};

using ManifestFilePtr = std::shared_ptr<ManifestFileIterator>;

bool operator<(const PartitionSpecification & lhs, const PartitionSpecification & rhs);
bool operator<(const DB::Row & lhs, const DB::Row & rhs);


std::weak_ordering operator<=>(const ManifestFileEntryPtr & lhs, const ManifestFileEntryPtr & rhs);
}

#endif
