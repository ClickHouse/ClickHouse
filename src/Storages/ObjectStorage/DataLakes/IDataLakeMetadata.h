#pragma once
#include <boost/noncopyable.hpp>

#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/ISimpleTransform.h>
#include <Storages/ObjectStorage/IObjectIterator.h>
#include <Storages/prepareReadingFromFormat.h>
#include <Formats/FormatFilterInfo.h>
#include <Formats/FormatParserSharedResources.h>
#include <Storages/MutationCommands.h>
#include <Interpreters/StorageID.h>
#include <Databases/DataLake/ICatalog.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/AlterCommands.h>


namespace DataLake
{
class ICatalog;
}

namespace DB
{

namespace ErrorCodes
{
extern const int UNSUPPORTED_METHOD;
}

class SinkToStorage;
using SinkToStoragePtr = std::shared_ptr<SinkToStorage>;
class StorageObjectStorageConfiguration;
using StorageObjectStorageConfigurationPtr = std::shared_ptr<StorageObjectStorageConfiguration>;
struct StorageID;

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
        ContextPtr context) const = 0;

    /// Table schema from data lake metadata.
    virtual NamesAndTypesList getTableSchema() const = 0;
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

    /// Whether metadata is updateable (instead of recreation from scratch)
    /// to the latest version of table state in data lake.
    virtual bool supportsUpdate() const { return false; }
    /// Update metadata to the latest version.
    virtual bool update(const ContextPtr &) { return false; }

    virtual bool supportsSchemaEvolution() const { return false; }
    virtual bool supportsWrites() const { return false; }

    virtual void modifyFormatSettings(FormatSettings &) const {}

    virtual std::optional<size_t> totalRows(ContextPtr) const { return {}; }
    virtual std::optional<size_t> totalBytes(ContextPtr) const { return {}; }

    /// Some data lakes specify information for reading files from disks.
    /// For example, Iceberg has Parquet schema field ids in its metadata for reading files.
    virtual ColumnMapperPtr getColumnMapperForObject(ObjectInfoPtr /**/) const { return nullptr; }
    virtual ColumnMapperPtr getColumnMapperForCurrentSchema() const { return nullptr; }

    virtual SinkToStoragePtr write(
        SharedHeader /*sample_block*/,
        const StorageID & /*table_id*/,
        ObjectStoragePtr /*object_storage*/,
        StorageObjectStorageConfigurationPtr /*configuration*/,
        const std::optional<FormatSettings> & /*format_settings*/,
        ContextPtr /*context*/,
        std::shared_ptr<DataLake::ICatalog> /*catalog*/) { throwNotImplemented("write"); }

    virtual bool optimize(const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr /*context*/, const std::optional<FormatSettings> & /*format_settings*/)
    {
        return false;
    }

    virtual bool supportsDelete() const { return false; }
    virtual void mutate(const MutationCommands & /*commands*/,
        ContextPtr /*context*/,
        const StorageID & /*storage_id*/,
        StorageMetadataPtr /*metadata_snapshot*/,
        std::shared_ptr<DataLake::ICatalog> /*catalog*/,
        const std::optional<FormatSettings> & /*format_settings*/) { throwNotImplemented("mutations"); }

    virtual void checkMutationIsPossible(const MutationCommands & /*commands*/) { throwNotImplemented("mutations"); }

    virtual void addDeleteTransformers(ObjectInfoPtr, QueryPipelineBuilder &, const std::optional<FormatSettings> &, ContextPtr) const {}
    virtual void checkAlterIsPossible(const AlterCommands & /*commands*/) { throwNotImplemented("alter"); }
    virtual void alter(const AlterCommands & /*params*/, ContextPtr /*context*/) { throwNotImplemented("alter"); }
    virtual void drop(ContextPtr) {}

protected:
    virtual ObjectIterator createKeysIterator(
        Strings && data_files_,
        ObjectStoragePtr object_storage_,
        IDataLakeMetadata::FileProgressCallback callback_) const;

    ObjectIterator createKeysIterator(
        Strings && data_files_,
        ObjectStoragePtr object_storage_,
        IDataLakeMetadata::FileProgressCallback callback_,
        UInt64 snapshot_version_) const;

    [[noreturn]] void throwNotImplemented(std::string_view method) const
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Method `{}` is not implemented", method);
    }
};

using DataLakeMetadataPtr = std::unique_ptr<IDataLakeMetadata>;

}
