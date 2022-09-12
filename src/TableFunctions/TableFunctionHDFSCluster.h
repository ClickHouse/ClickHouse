#pragma once

#include <Common/config.h>

#if USE_HDFS

#include <TableFunctions/ITableFunction.h>


namespace DB
{

class Context;

/**
 * hdfsCluster(cluster, URI, format, structure, compression_method)
 * A table function, which allows to process many files from HDFS on a specific cluster
 * On initiator it creates a connection to _all_ nodes in cluster, discloses asterics
 * in HDFS file path and dispatch each file dynamically.
 * On worker node it asks initiator about next task to process, processes it.
 * This is repeated until the tasks are finished.
 */
class TableFunctionHDFSCluster : public ITableFunction
{
public:
    static constexpr auto name = "hdfsCluster";
    std::string getName() const override
    {
        return name;
    }
    bool hasStaticStructure() const override { return true; }

protected:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns) const override;

    const char * getStorageTypeName() const override { return "HDFSCluster"; }

    ColumnsDescription getActualTableStructure(ContextPtr) const override;
    void parseArguments(const ASTPtr &, ContextPtr) override;

    String cluster_name;
    String uri;
    String format;
    String structure;
    String compression_method = "auto";
};

}

#endif
