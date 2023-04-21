#pragma once

#include <TableFunctions/ITableFunctionFileLike.h>
#include <TableFunctions/TableFunctionURL.h>
#include <Storages/StorageURL.h>
#include <Storages/StorageURLCluster.h>
#include <IO/ReadWriteBufferFromHTTP.h>


namespace DB
{

class Context;

/**
 * urlCluster(cluster, URI, format, structure, compression_method)
 * A table function, which allows to process many files from url on a specific cluster
 * On initiator it creates a connection to _all_ nodes in cluster, discloses asterics
 * in url file path and dispatch each file dynamically.
 * On worker node it asks initiator about next task to process, processes it.
 * This is repeated until the tasks are finished.
 */
class TableFunctionURLCluster : public TableFunctionURL
{
public:
    static constexpr auto name = "urlCluster";
    std::string getName() const override
    {
        return name;
    }

protected:
    StoragePtr getStorage(
        const String & source, const String & format_, const ColumnsDescription & columns, ContextPtr global_context,
        const std::string & table_name, const String & compression_method_) const override;

    const char * getStorageTypeName() const override { return "URLCluster"; }

    AccessType getSourceAccessType() const override { return AccessType::URL; }

    ColumnsDescription getActualTableStructure(ContextPtr) const override;
    void parseArguments(const ASTPtr &, ContextPtr) override;

    String cluster_name;

};

}
