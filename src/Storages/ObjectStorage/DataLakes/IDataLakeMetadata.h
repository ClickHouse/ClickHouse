#pragma once
#include <memory>
#include <boost/noncopyable.hpp>
#include <fmt/format.h>

#include <Core/Field.h>
#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <Core/Range.h>
#include <DataTypes/IDataType.h>
#include <Databases/DataLake/ICatalog.h>
#include <Formats/FormatFilterInfo.h>
#include <Formats/FormatParserSharedResources.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/StorageID.h>
#include <Processors/ISimpleTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/AlterCommands.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeTableStateSnapshot.h>
#include <Storages/MutationCommands.h>
#include <Storages/prepareReadingFromFormat.h>
#include <Disks/DiskType.h>
#include <IO/WriteBuffer.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>


namespace DataLake
{
class ICatalog;
}

namespace DB
{

namespace ErrorCodes
{
extern const int UNSUPPORTED_METHOD;
};

namespace Iceberg
{
struct ColumnInfo;
};

class DataFileMetaInfo
{
public:
    DataFileMetaInfo() = default;

    // Deserialize from json in distributed requests
    explicit DataFileMetaInfo(const Poco::JSON::Object::Ptr file_info);

    // Serialize to json in distributed requests
    Poco::JSON::Object::Ptr toJson() const;

    // subset of Iceberg::ColumnInfo now
    struct ColumnInfo
    {
        std::optional<Int64> rows_count;
        std::optional<Int64> nulls_count;
        std::optional<DB::Range> hyperrectangle;
    };

    // Extract metadata from Iceberg structure.
    // table_schema_id is the current table schema, used to resolve column names that
    // appear as keys in the resulting `columns_info` map.
    // file_schema_id is the schema the data file (and its `value_bounds_`) was written
    // with — bounds bytes are encoded with that schema's column types, so they must be
    // deserialized using those types. After schema evolution (e.g. `int` -> `long`)
    // the two ids differ, and using the table schema's type would misinterpret the
    // bytes and produce a garbage hyperrectangle.
    explicit DataFileMetaInfo(
        const Iceberg::IcebergSchemaProcessor & schema_processor,
        Int32 table_schema_id,
        Int32 file_schema_id,
        const std::unordered_map<Int32, Iceberg::ColumnInfo> & columns_info_,
        const std::unordered_map<Int32, std::pair<Field, Field>> & value_bounds_);

    void serialize(WriteBuffer & out) const;
    static DataFileMetaInfo deserialize(ReadBuffer & in);

    bool empty() const { return columns_info.empty(); }

    std::unordered_map<std::string, ColumnInfo> columns_info;
};

using DataFileMetaInfoPtr = std::shared_ptr<DataFileMetaInfo>;

struct DataFileInfo
{
    std::string file_path;
    std::optional<DataFileMetaInfoPtr> file_meta_info;

    explicit DataFileInfo(const std::string & file_path_)
        : file_path(file_path_) {}

    explicit DataFileInfo(std::string && file_path_)
        : file_path(std::move(file_path_)) {}

    bool operator==(const DataFileInfo & rhs) const
    {
        return file_path == rhs.file_path;
    }
};

class SinkToStorage;
using SinkToStoragePtr = std::shared_ptr<SinkToStorage>;
class StorageObjectStorageConfiguration;
using StorageObjectStorageConfigurationPtr = std::shared_ptr<StorageObjectStorageConfiguration>;
struct StorageID;
struct IObjectIterator;
class IObjectStorage;
struct ObjectInfo;
using ObjectInfoPtr = std::shared_ptr<ObjectInfo>;
using ObjectIterator = std::shared_ptr<IObjectIterator>;
using ObjectStoragePtr = std::shared_ptr<IObjectStorage>;

class IDataLakeMetadata : boost::noncopyable
{
public:
    virtual ~IDataLakeMetadata() = default;

    virtual bool operator==(const IDataLakeMetadata & other) const = 0;

    /// Return iterator to `data files`.
    using FileProgressCallback = std::function<void(FileProgress)>;
    virtual ObjectIterator iterate(
        const ActionsDAG * /* filter_dag */,
        FileProgressCallback /* callback */,
        size_t /* list_batch_size */,
        StorageMetadataPtr storage_metadata,
        ContextPtr context) const
        = 0;

    /// Table schema from data lake metadata.
    virtual NamesAndTypesList getTableSchema(ContextPtr local_context) const = 0;

    /// Returns the current table state snapshot (snapshot version, schema id, etc.)
    /// Used to pin the exact state for both analysis and execution phases of a query,
    /// preventing logical races when the datalake is updated mid-query.
    virtual std::optional<DataLakeTableStateSnapshot> getTableStateSnapshot(ContextPtr) const { return std::nullopt; }

    /// Builds a full StorageInMemoryMetadata (columns, sorting key, etc.) from the
    /// given pinned state. Only called when schema reload for consistency is enabled.
    virtual std::unique_ptr<StorageInMemoryMetadata> buildStorageMetadataFromState(
        const DataLakeTableStateSnapshot &, ContextPtr) const { return nullptr; }

    /// Whether to reload the schema (columns) from metadata before each query in order
    /// to keep the columns stored in the in-memory metadata in sync with the datalake.
    virtual bool shouldReloadSchemaForConsistency(ContextPtr) const { return false; }

    /// Read schema is the schema of actual data files,
    /// which can differ from table schema from data lake metadata.
    /// Return nothing if read schema is the same as table schema.
    virtual ReadFromFormatInfo prepareReadingFromFormat(
        const Strings & requested_columns,
        const StorageSnapshotPtr & storage_snapshot,
        const ContextPtr & context,
        bool supports_subset_of_columns,
        bool supports_tuple_elements);

    virtual std::shared_ptr<NamesAndTypesList> getInitialSchemaByPath(ContextPtr, ObjectInfoPtr) const { return {}; }
    virtual std::shared_ptr<const ActionsDAG> getSchemaTransformer(ContextPtr, ObjectInfoPtr) const { return {}; }

    /// Whether current metadata object is updateable (instead of recreation from scratch)
    /// to the latest version of table state in data lake.
    virtual bool supportsUpdate() const { return false; }
    /// Update metadata to the latest version.
    virtual void update(const ContextPtr &) { }

    virtual bool supportsWrites() const { return false; }
    virtual bool supportsParallelInsert() const { return false; }

    virtual void modifyFormatSettings(FormatSettings &, const Context &) const {}

    static bool supportsTotalRows(ContextPtr, ObjectStorageType) { return false; }
    virtual std::optional<size_t> totalRows(ContextPtr) const { return {}; }
    static bool supportsTotalBytes(ContextPtr, ObjectStorageType) { return false; }
    virtual std::optional<size_t> totalBytes(ContextPtr) const { return {}; }

    /// Data which we are going to read is sorted by sorting key specified in StorageMetadataPtr.
    /// For example in Iceberg it's a valid query to change sort_order for table, but older files will
    /// not be rewritten and will be left unsorted or with previous sort order.
    /// In this case we shouldn't use read in order optimization.
    virtual bool isDataSortedBySortingKey(StorageMetadataPtr, ContextPtr) const { return false; }

    /// Some data lakes specify information for reading files from disks.
    /// For example, Iceberg has Parquet schema field ids in its metadata for reading files.
    virtual ColumnMapperPtr getColumnMapperForObject(ObjectInfoPtr /**/) const { return nullptr; }
    virtual ColumnMapperPtr getColumnMapperForCurrentSchema(StorageMetadataPtr, ContextPtr) const { return nullptr; }

    virtual SinkToStoragePtr write(
        SharedHeader /*sample_block*/,
        const StorageID & /*table_id*/,
        ObjectStoragePtr /*object_storage*/,
        StorageObjectStorageConfigurationPtr /*configuration*/,
        const std::optional<FormatSettings> & /*format_settings*/,
        ContextPtr /*context*/,
        std::shared_ptr<DataLake::ICatalog> /*catalog*/)
    {
        throwNotImplemented("write");
    }

    virtual bool supportsImport(ContextPtr) const
    {
        return false;
    }

    virtual SinkToStoragePtr import(
        std::shared_ptr<DataLake::ICatalog> /* catalog */,
        const std::function<void(const std::string &)> & /* new_file_path_callback */,
        SharedHeader /* sample_block */,
        const std::string & /* iceberg_metadata_json_string */,
        const std::optional<FormatSettings> & /* format_settings_ */,
        ContextPtr /* context */)
    {
        throwNotImplemented("import");
    }

    virtual void commitExportPartitionTransaction(
        std::shared_ptr<DataLake::ICatalog> /* catalog */,
        const StorageID & /* table_id */,
        const String & /* transaction_id */,
        Int64 /* original_schema_id */,
        Int64 /* partition_spec_id */,
        const std::vector<Field> & /* partition_values */,
        SharedHeader /* sample_block */,
        const std::vector<String> & /* data_file_paths */,
        StorageObjectStorageConfigurationPtr /* configuration */,
        ContextPtr /* context */)
        {
            throwNotImplemented("commitExportPartitionTransaction");
        }

    virtual bool optimize(
        const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr /*context*/, const std::optional<FormatSettings> & /*format_settings*/)
    {
        return false;
    }

    virtual bool supportsDelete() const { return false; }
    virtual void mutate(
        const MutationCommands & /*commands*/,
        StorageObjectStorageConfigurationPtr /*configuration*/,
        ContextPtr /*context*/,
        const StorageID & /*storage_id*/,
        StorageMetadataPtr /*metadata_snapshot*/,
        std::shared_ptr<DataLake::ICatalog> /*catalog*/,
        const std::optional<FormatSettings> & /*format_settings*/)
    {
        throwNotImplemented("mutations");
    }

    virtual void checkMutationIsPossible(const MutationCommands & /*commands*/) { throwNotImplemented("mutations"); }

    virtual void addDeleteTransformers(ObjectInfoPtr, QueryPipelineBuilder &, const std::optional<FormatSettings> &, FormatParserSharedResourcesPtr, ContextPtr) const { }
    virtual void checkAlterIsPossible(const AlterCommands & /*commands*/) { throwNotImplemented("alter"); }
    virtual void alter(const AlterCommands & /*params*/, ContextPtr /*context*/) { throwNotImplemented("alter"); }

    virtual Pipe executeCommand(
        const String & command_name,
        const ASTPtr & /*args*/,
        ObjectStoragePtr /*object_storage*/,
        StorageObjectStorageConfigurationPtr /*configuration*/,
        std::shared_ptr<DataLake::ICatalog> /*catalog*/,
        ContextPtr /*context*/,
        const StorageID & /*storage_id*/)
    {
        throwNotImplemented(fmt::format("EXECUTE {}", command_name));
    }

    virtual void drop(ContextPtr) { }

    virtual std::optional<String> partitionKey(ContextPtr) const { return {}; }
    virtual std::optional<String> sortingKey(ContextPtr) const { return {}; }

protected:
    virtual ObjectIterator
    createKeysIterator(Strings && data_files_, ObjectStoragePtr object_storage_, IDataLakeMetadata::FileProgressCallback callback_) const;

    ObjectIterator createKeysIterator(
        Strings && data_files_,
        ObjectStoragePtr object_storage_,
        IDataLakeMetadata::FileProgressCallback callback_,
        UInt64 snapshot_version_) const;

    [[noreturn]] void throwNotImplemented(std::string_view method) const
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Method `{}` is not implemented for {}", method, getName());
    }

    virtual const char * getName() const = 0;
};

using DataLakeMetadataPtr = std::unique_ptr<IDataLakeMetadata>;

}
