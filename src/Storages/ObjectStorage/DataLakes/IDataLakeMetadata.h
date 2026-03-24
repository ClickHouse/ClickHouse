#pragma once
#include <memory>
#include <boost/noncopyable.hpp>

#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeTableStateSnapshot.h>
#include <Storages/prepareReadingFromFormat.h>
#include <Disks/DiskType.h>

namespace DB
{

class ActionsDAG;
struct IObjectIterator;
class IObjectStorage;
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

    virtual std::optional<DataLakeTableStateSnapshot> getTableStateSnapshot(ContextPtr) const { return std::nullopt; }
    virtual std::unique_ptr<StorageInMemoryMetadata> buildStorageMetadataFromState(
        const DataLakeTableStateSnapshot &, ContextPtr) const { return nullptr; }
    virtual bool shouldReloadSchemaForConsistency(ContextPtr) const { return false; }

    virtual ReadFromFormatInfo prepareReadingFromFormat(
        const Strings & requested_columns,
        const StorageSnapshotPtr & storage_snapshot,
        const ContextPtr & context,
        bool supports_subset_of_columns,
        bool supports_tuple_elements);

    virtual bool supportsUpdate() const { return false; }
    virtual void update(const ContextPtr &) { }

    virtual bool supportsWrites() const { return false; }

    virtual void modifyFormatSettings(FormatSettings &, const Context &) const {}

    static bool supportsTotalRows(ContextPtr, ObjectStorageType) { return false; }
    virtual std::optional<size_t> totalRows(ContextPtr) const { return {}; }
    static bool supportsTotalBytes(ContextPtr, ObjectStorageType) { return false; }
    virtual std::optional<size_t> totalBytes(ContextPtr) const { return {}; }

    virtual void drop(ContextPtr) { }

protected:
    virtual ObjectIterator
    createKeysIterator(Strings && data_files_, ObjectStoragePtr object_storage_, IDataLakeMetadata::FileProgressCallback callback_) const;

    ObjectIterator createKeysIterator(
        Strings && data_files_,
        ObjectStoragePtr object_storage_,
        IDataLakeMetadata::FileProgressCallback callback_,
        UInt64 snapshot_version_) const;

    virtual const char * getName() const = 0;
};

using DataLakeMetadataPtr = std::unique_ptr<IDataLakeMetadata>;

}
