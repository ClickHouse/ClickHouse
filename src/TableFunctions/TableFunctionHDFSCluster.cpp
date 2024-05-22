#include "config.h"

#if USE_HDFS

#include <TableFunctions/TableFunctionHDFSCluster.h>
#include <TableFunctions/TableFunctionFactory.h>

#include <Storages/HDFS/StorageHDFSCluster.h>
#include <Storages/HDFS/StorageHDFS.h>
#include "registerTableFunctions.h"

#include <memory>


namespace DB
{

StoragePtr TableFunctionHDFSCluster::getStorage(
    const String & /*source*/, const String & /*format_*/, const ColumnsDescription & columns, ContextPtr context,
    const std::string & table_name, const String & /*compression_method_*/) const
{
    StoragePtr storage;
    if (context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
    {
        /// On worker node this uri won't contains globs
        storage = std::make_shared<StorageHDFS>(
            filename,
            StorageID(getDatabaseName(), table_name),
            format,
            columns,
            ConstraintsDescription{},
            String{},
            context,
            compression_method,
            /*distributed_processing=*/true,
            nullptr);
    }
    else
    {
        storage = std::make_shared<StorageHDFSCluster>(
            context,
            cluster_name,
            filename,
            StorageID(getDatabaseName(), table_name),
            format,
            columns,
            ConstraintsDescription{},
            compression_method);
    }
    return storage;
}

void registerTableFunctionHDFSCluster(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionHDFSCluster>();
}

}

#endif
