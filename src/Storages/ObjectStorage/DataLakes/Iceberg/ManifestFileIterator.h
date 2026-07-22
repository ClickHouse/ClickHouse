#pragma once

#include <Common/SharedLockGuard.h>
#include "config.h"

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#include <Storages/ObjectStorage/DataLakes/Common/AvroForIcebergDeserializer.h>
#include <Storages/KeyDescription.h>

#include <mutex>
#include <unordered_map>

namespace DB::Iceberg
{

class ManifestFilesPruner;

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
        using FilesPtr = std::shared_ptr<const std::vector<ProcessedManifestFileEntryPtr>>;

        const std::vector<ProcessedManifestFileEntryPtr> & getFilesWithoutDeleted(FileContentType content_type) const;

        bool areAllDataFilesSortedBySortOrderID(Int32 sort_order_id) const;

        std::optional<Int64> getRowsCountInAllFilesExcludingDeleted(FileContentType content) const;

        std::optional<Int64> getBytesCountInAllDataFilesExcludingDeleted() const;

    private:
        friend class ManifestFileIterator;

        ManifestFileEntriesHandle(FilesPtr data_files_, FilesPtr position_delete_files_, FilesPtr equality_delete_files_)
            : data_files(std::move(data_files_))
            , position_delete_files(std::move(position_delete_files_))
            , equality_delete_files(std::move(equality_delete_files_))
        {
        }

        FilesPtr data_files;
        FilesPtr position_delete_files;
        FilesPtr equality_delete_files;
    };

    static std::shared_ptr<ManifestFileIterator> create(
        std::shared_ptr<AvroForIcebergDeserializer> manifest_file_deserializer,
        const IcebergPathFromMetadata & path_to_manifest_file,
        Int32 format_version_,
        const IcebergPathResolver & path_resolver,
        IcebergSchemaProcessor & schema_processor,
        Int64 inherited_sequence_number,
        Int64 inherited_snapshot_id,
        DB::ContextPtr context,
        std::shared_ptr<const ActionsDAG> filter_dag_,
        Int32 table_snapshot_schema_id_);

    ManifestFileEntriesHandle getFilesWithoutDeletedHandle() const;

    bool hasPartitionKey() const;
    const DB::KeyDescription & getPartitionKeyDescription() const;
    /// Fields with rows count in manifest files are optional
    /// they can be absent.
    std::optional<Int64> getRowsCountInAllFilesExcludingDeleted(FileContentType content) const;
    std::optional<Int64> getBytesCountInAllDataFilesExcludingDeleted() const;

    bool areAllDataFilesSortedBySortOrderID(Int32 sort_order_id) const;

    /// Returns true if all manifest file entries have been processed
    bool isInitialized() const;
    /// Process the next manifest file entry and return it. Skips pruned entries.
    /// Returns nullptr only when all entries have been processed (iterator is fully initialized).
    ProcessedManifestFileEntryPtr next();

    ~ManifestFileIterator();

    ManifestFileIterator(ManifestFileIterator &&) = delete;
    ManifestFileIterator & operator=(ManifestFileIterator &&) = delete;

private:
    ManifestFileIterator(
        std::shared_ptr<AvroForIcebergDeserializer> manifest_file_deserializer,
        const IcebergPathFromMetadata & path_to_manifest_file,
        Int32 format_version,
        const IcebergPathResolver & path_resolver,
        IcebergSchemaProcessor & schema_processor,
        Int64 inherited_sequence_number,
        Int64 inherited_snapshot_id,
        DB::ContextPtr context,
        Int32 manifest_schema_id,
        std::shared_ptr<const PartitionSpecification> common_partition_specification,
        std::optional<DB::KeyDescription> partition_key_description,
        size_t total_rows,
        std::shared_ptr<const ActionsDAG> filter_dag,
        Int32 table_snapshot_schema_id);

    ProcessedManifestFileEntryPtr processRow(size_t row_index);

    /// Constant properties of this manifest file
    const std::shared_ptr<AvroForIcebergDeserializer> manifest_file_deserializer;
    const IcebergPathFromMetadata path_to_manifest_file;
    const Int32 format_version;
    const IcebergPathResolver path_resolver;
    // always zero in case of format version 1
    const Int64 inherited_sequence_number;
    const Int64 inherited_snapshot_id;
    const DB::ContextPtr context;
    const Int32 manifest_schema_id;
    const std::shared_ptr<const PartitionSpecification> common_partition_specification;
    const std::optional<DB::KeyDescription> partition_key_description;
    const Int32 table_snapshot_schema_id;

    /// Iteration state
    const size_t total_rows;
    std::atomic<size_t> current_row_index{0};
    std::atomic<bool> fully_initialized{false};
    std::atomic<size_t> active_fetchers{0};

    /// Cached results accumulated during iteration
    mutable SharedMutex files_mutex;
    std::shared_ptr<std::vector<ProcessedManifestFileEntryPtr>> data_files_without_deleted TSA_GUARDED_BY(files_mutex);
    std::shared_ptr<std::vector<ProcessedManifestFileEntryPtr>> position_deletes_files_without_deleted TSA_GUARDED_BY(files_mutex);
    std::shared_ptr<std::vector<ProcessedManifestFileEntryPtr>> equality_deletes_files_without_deleted TSA_GUARDED_BY(files_mutex);

    /// Filtering (partition and min-max index pruning)
    const std::shared_ptr<const ActionsDAG> filter_dag;
    IcebergSchemaProcessor * const schema_processor_ptr;

    /// Cache of pruners by schema_id to avoid recreation on each row
    mutable std::mutex pruners_mutex;
    std::unordered_map<Int32, std::unique_ptr<ManifestFilesPruner>> pruners_by_schema_id;
    const ManifestFilesPruner * getOrCreatePruner(Int32 schema_id);
};

using ManifestIteratorPtr = std::shared_ptr<ManifestFileIterator>;
}

#endif
