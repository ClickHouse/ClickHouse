#pragma once

#include "config.h"

#if USE_HDFS

#include <TableFunctions/ITableFunctionFileLike.h>
#include <TableFunctions/TableFunctionHDFS.h>
#include <TableFunctions/ITableFunctionCluster.h>


namespace DB
{

class Context;

/**
 * hdfsCluster(cluster, URI, format, structure, compression_method)
 * A table function, which allows to process many files from HDFS on a specific cluster
 * On initiator it creates a connection to _all_ nodes in cluster, discloses asterisks
 * in HDFS file path and dispatch each file dynamically.
 * On worker node it asks initiator about next task to process, processes it.
 * This is repeated until the tasks are finished.
 */
class TableFunctionHDFSCluster : public ITableFunctionCluster<TableFunctionHDFS>
{
public:
    static constexpr auto name = "hdfsCluster";
    static constexpr auto signature = " - cluster_name, uri\n"
                                      " - cluster_name, uri, format\n"
                                      " - cluster_name, uri, format, structure\n"
                                      " - cluster_name, uri, format, structure, compression_method\n";

    String getName() const override
    {
        return name;
    }

    String getSignature() const override
    {
        return signature;
    }

protected:
    StoragePtr getStorage(
        const String & source, const String & format_, const ColumnsDescription & columns, ContextPtr global_context,
        const std::string & table_name, const String & compression_method_) const override;

    const char * getStorageTypeName() const override { return "HDFSCluster"; }
};

}

#endif
