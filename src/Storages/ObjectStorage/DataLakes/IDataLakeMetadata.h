#pragma once
#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <boost/noncopyable.hpp>
#include "Interpreters/ActionsDAG.h"
#include <Storages/ObjectStorage/IObjectIterator.h>

namespace DB
{
class IDataLakeMetadata : boost::noncopyable
{
public:
    virtual ~IDataLakeMetadata() = default;
    virtual Strings getDataFiles() const = 0;
    virtual NamesAndTypesList getTableSchema() const = 0;
    virtual bool operator==(const IDataLakeMetadata & other) const = 0;
    virtual const std::unordered_map<String, String> & getColumnNameToPhysicalNameMapping() const = 0;
    virtual std::shared_ptr<NamesAndTypesList> getInitialSchemaByPath(const String &) const { return {}; }
    virtual std::shared_ptr<const ActionsDAG> getSchemaTransformer(const String &) const { return {}; }
    virtual bool supportsExternalMetadataChange() const { return false; }
    virtual bool supportsUpdate() const { return false; }
    virtual bool update(const ContextPtr &) { return false; }
    virtual bool supportsFileIterator() const { return false; }
    virtual ObjectIterator iterate() const { throwNotImplemented("iterate()"); }

    [[noreturn]] void throwNotImplemented(std::string_view method) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method {} is not implemented", method);
    }
};
using DataLakeMetadataPtr = std::unique_ptr<IDataLakeMetadata>;

}
