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
struct ParsedManifestFileEntry
{
    FileContentType content_type;
    // It's the original string in the Iceberg metadata
    String file_path_key;
    Int64 row_number;

    ManifestEntryStatus status;
    std::optional<Int64> written_sequence_number;
    std::optional<Int64> written_snapshot_id;

    DB::Row partition_key_value;
    std::unordered_map<Int32, ColumnInfo> columns_infos;
    std::unordered_map<Int32, std::pair<Field, Field>> value_bounds;

    String file_format;
    std::optional<String> lower_reference_data_file_path; // For position delete files only.
    std::optional<String> upper_reference_data_file_path; // For position delete files only.
    std::optional<std::vector<Int32>> equality_ids;

    /// Data file is sorted with this sort_order_id (can be read from metadata.json)
    std::optional<Int32> sort_order_id;

    ParsedManifestFileEntry(
        FileContentType content_type_,
        String file_path_key_,
        Int64 row_number_,
        ManifestEntryStatus status_,
        std::optional<Int64> written_sequence_number_,
        std::optional<Int64> written_snapshot_id_,
        DB::Row partition_key_value_,
        std::unordered_map<Int32, ColumnInfo> columns_infos_,
        std::unordered_map<Int32, std::pair<Field, Field>> value_bounds_,
        String file_format_,
        std::optional<String> lower_reference_data_file_path_,
        std::optional<String> upper_reference_data_file_path_,
        std::optional<std::vector<Int32>> equality_ids_,
        std::optional<Int32> sort_order_id_)
        : content_type(content_type_)
        , file_path_key(std::move(file_path_key_))
        , row_number(row_number_)
        , status(status_)
        , written_sequence_number(written_sequence_number_)
        , written_snapshot_id(written_snapshot_id_)
        , partition_key_value(std::move(partition_key_value_))
        , columns_infos(std::move(columns_infos_))
        , value_bounds(std::move(value_bounds_))
        , file_format(std::move(file_format_))
        , lower_reference_data_file_path(std::move(lower_reference_data_file_path_))
        , upper_reference_data_file_path(std::move(upper_reference_data_file_path_))
        , equality_ids(std::move(equality_ids_))
        , sort_order_id(sort_order_id_)
    {
    }

    ParsedManifestFileEntry(const ParsedManifestFileEntry &) = delete;
    ParsedManifestFileEntry & operator=(const ParsedManifestFileEntry &) = delete;
};

struct ProcessedManifestFileEntry
{
    std::shared_ptr<const ParsedManifestFileEntry> parsed_entry;
    std::shared_ptr<const PartitionSpecification> common_partition_specification;

    /// Computed file path for Object Storage (resolved from parsed_entry->file_path_key)
    String file_path;

    /// Inherited or resolved from manifest list level
    Int64 added_sequence_number;
    Int64 snapshot_id;
    Int32 schema_id;

    /// Per-column hyperrectangles computed from value bounds
    std::unordered_map<Int32, DB::Range> hyperrectangles;

    String dumpDeletesMatchingInfo() const;
};

using ProcessedManifestFileEntryPtr = std::shared_ptr<const ProcessedManifestFileEntry>;

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
        const std::vector<ProcessedManifestFileEntryPtr> & getFilesWithoutDeleted() const { return *files; }

        ManifestFileEntriesHandle(const std::vector<ProcessedManifestFileEntryPtr> * files_, SharedLockGuard<SharedMutex> shared_lock_)
            : files(files_)
            , lock(std::move(shared_lock_))
        {
        }

        const std::vector<ProcessedManifestFileEntryPtr> & operator*() const { return getFilesWithoutDeleted(); }
        const std::vector<ProcessedManifestFileEntryPtr> * operator->() const { return files; }

    private:
        const std::vector<ProcessedManifestFileEntryPtr> * files;
        SharedLockGuard<SharedMutex> lock;
    };

    static std::shared_ptr<ManifestFileIterator> create(
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
        std::shared_ptr<const ActionsDAG> filter_dag_,
        Int32 table_snapshot_schema_id_);

    ManifestFileEntriesHandle getFilesWithoutDeletedHandle(FileContentType content_type) const;

    bool hasPartitionKey() const;
    const DB::KeyDescription & getPartitionKeyDescription() const;
    /// Fields with rows count in manifest files are optional
    /// they can be absent.
    std::optional<Int64> getRowsCountInAllFilesExcludingDeleted(FileContentType content) const;
    std::optional<Int64> getBytesCountInAllDataFilesExcludingDeleted() const;

    const String & getPathToManifestFile() const { return path_to_manifest_file; }

    bool areAllDataFilesSortedBySortOrderID(Int32 sort_order_id) const;

    /// Returns true if all manifest file entries have been processed
    bool isInitialized() const;
    /// Process the next manifest file entry and return it. Skips pruned entries.
    /// Returns nullptr only when all entries have been processed (iterator is fully initialized).
    ProcessedManifestFileEntryPtr next();

    ManifestFileIterator(ManifestFileIterator &&) = delete;
    ManifestFileIterator & operator=(ManifestFileIterator &&) = delete;

    size_t getFileBytesSize() const { return file_bytes_size; }

private:
    ManifestFileIterator(
        std::shared_ptr<AvroForIcebergDeserializer> manifest_file_deserializer,
        const String & path_to_manifest_file,
        const String & manifest_file_name,
        Int32 format_version,
        const String & common_path,
        const String & table_location,
        IcebergSchemaProcessor & schema_processor,
        Int64 inherited_sequence_number,
        Int64 inherited_snapshot_id,
        DB::ContextPtr context,
        Int32 manifest_schema_id,
        size_t file_bytes_size,
        std::shared_ptr<const PartitionSpecification> common_partition_specification,
        std::optional<DB::KeyDescription> partition_key_description,
        size_t total_rows,
        std::shared_ptr<const ActionsDAG> filter_dag,
        Int32 table_snapshot_schema_id);

    ProcessedManifestFileEntryPtr processRow(size_t row_index);

    /// Constant properties of this manifest file
    const std::shared_ptr<AvroForIcebergDeserializer> manifest_file_deserializer;
    const String path_to_manifest_file;
    const String manifest_file_name;
    const Int32 format_version;
    const String common_path;
    const String table_location;
    IcebergSchemaProcessor * const schema_processor_ptr;
    const Int64 inherited_sequence_number;
    const Int64 inherited_snapshot_id;
    const DB::ContextPtr context;
    const Int32 manifest_schema_id;
    const size_t file_bytes_size;
    const std::shared_ptr<const PartitionSpecification> common_partition_specification;
    const std::optional<DB::KeyDescription> partition_key_description;

    /// Iteration state
    const size_t total_rows;
    std::atomic<size_t> current_row_index{0};
    std::atomic<bool> fully_initialized{false};

    /// Cached results accumulated during iteration
    mutable SharedMutex files_mutex;
    std::vector<ProcessedManifestFileEntryPtr> data_files_without_deleted TSA_GUARDED_BY(files_mutex);
    std::vector<ProcessedManifestFileEntryPtr> position_deletes_files_without_deleted TSA_GUARDED_BY(files_mutex);
    std::vector<ProcessedManifestFileEntryPtr> equality_deletes_files TSA_GUARDED_BY(files_mutex);

    /// Filtering (partition and min-max index pruning)
    const std::shared_ptr<const ActionsDAG> filter_dag;
    const Int32 table_snapshot_schema_id;
    std::atomic<std::size_t> partition_pruned_files{0};
    std::atomic<std::size_t> min_max_index_pruned_files{0};
};

using ManifestFilePtr = std::shared_ptr<ManifestFileIterator>;

bool operator<(const PartitionSpecification & lhs, const PartitionSpecification & rhs);
bool operator<(const DB::Row & lhs, const DB::Row & rhs);


std::weak_ordering operator<=>(const ProcessedManifestFileEntryPtr & lhs, const ProcessedManifestFileEntryPtr & rhs);
}

#endif
