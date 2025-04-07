#pragma once
#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <boost/noncopyable.hpp>
#include "Interpreters/ActionsDAG.h"
#include <Storages/ObjectStorage/IObjectIterator.h>

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

    /// List all data files.
    /// For better parallelization, iterate() method should be used.
    virtual Strings getDataFiles() const = 0;
    /// Whether `iterate()` method is supported for the data lake.
    virtual bool supportsFileIterator() const { return false; }
    /// Return iterator to `data files`.
    using FileProgressCallback = std::function<void(FileProgress)>;
    virtual ObjectIterator iterate(
        const ActionsDAG * /* filter_dag */,
        FileProgressCallback /* callback */,
        size_t /* list_batch_size */) const { throwNotImplemented("iterate()"); }

    /// Table schema from data lake metadata.
    virtual NamesAndTypesList getTableSchema() const = 0;
    /// Read schema is the schema of actual data files,
    /// which can differ from table schema from data lake metadata.
    /// Return nothing if read schema is the same as table schema.
    virtual NamesAndTypesList getReadSchema() const { return {}; }

    virtual bool supportsPartitionPruning() { return false; }
    virtual Strings makePartitionPruning(const ActionsDAG &) { throwNotImplemented("makePartitionPrunning()"); }

    virtual std::shared_ptr<NamesAndTypesList> getInitialSchemaByPath(const String &) const { return {}; }
    virtual std::shared_ptr<const ActionsDAG> getSchemaTransformer(const String &) const { return {}; }

    /// Whether metadata is updateable (instead of recreation from scratch)
    /// to the latest version of table state in data lake.
    virtual bool supportsUpdate() const { return false; }
    /// Update metadata to the latest version.
    virtual bool update(const ContextPtr &) { return false; }

    /// Whether schema evolution is supported.
    virtual bool supportsExternalMetadataChange() const { return false; }

    virtual std::optional<size_t> totalRows() const { return {}; }
    virtual std::optional<size_t> totalBytes() const { return {}; }
protected:
    [[noreturn]] void throwNotImplemented(std::string_view method) const
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Method `{}` is not implemented", method);
    }
};

using DataLakeMetadataPtr = std::unique_ptr<IDataLakeMetadata>;

}
