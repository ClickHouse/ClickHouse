#pragma once

#include <Common/config.h>

#if USE_HDFS

#include <TableFunctions/ITableFunctionFileLike.h>


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
class TableFunctionHDFSCluster : public ITableFunctionFileLike
{
public:
    static constexpr auto name = "hdfsCluster";
    std::string getName() const override
    {
        return name;
    }
    bool hasStaticStructure() const override { return true; }

protected:
    StoragePtr getStorage(
        const String & source, const String & format_, const ColumnsDescription & columns, ContextPtr global_context,
        const std::string & table_name, const String & compression_method_) const override;

    const char * getStorageTypeName() const override { return "HDFSCluster"; }

    AccessType getSourceAccessType() const override { return AccessType::HDFS; }

    ColumnsDescription getActualTableStructure(ContextPtr) const override;
    void parseArguments(const ASTPtr &, ContextPtr) override;

    String cluster_name;
};

}

#endif
