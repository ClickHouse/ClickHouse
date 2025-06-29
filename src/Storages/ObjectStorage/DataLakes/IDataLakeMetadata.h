#pragma once
#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <boost/noncopyable.hpp>
#include "Interpreters/ActionsDAG.h"
#include <Storages/ObjectStorage/IObjectIterator.h>
#include <Storages/prepareReadingFromFormat.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNSUPPORTED_METHOD;
}


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
    virtual DB::ReadFromFormatInfo prepareReadingFromFormat(
        const Strings & requested_columns,
        const DB::StorageSnapshotPtr & storage_snapshot,
        const ContextPtr & context,
        bool supports_subset_of_columns);

    virtual std::shared_ptr<NamesAndTypesList> getInitialSchemaByPath(ContextPtr, const String & /* path */) const { return {}; }
    virtual std::shared_ptr<const ActionsDAG> getSchemaTransformer(ContextPtr, const String & /* path */) const { return {}; }

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

protected:
    ObjectIterator createKeysIterator(
        Strings && data_files_,
        ObjectStoragePtr object_storage_,
        IDataLakeMetadata::FileProgressCallback callback_) const;

    [[noreturn]] void throwNotImplemented(std::string_view method) const
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Method `{}` is not implemented", method);
    }
};

using DataLakeMetadataPtr = std::unique_ptr<IDataLakeMetadata>;

}
