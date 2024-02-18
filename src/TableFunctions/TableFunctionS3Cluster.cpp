#include "config.h"

#if USE_AWS_S3

#include <TableFunctions/TableFunctionS3Cluster.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Storages/StorageS3.h>

#include "registerTableFunctions.h"

#include <memory>


namespace DB
{

StoragePtr TableFunctionS3Cluster::executeImpl(
    const ASTPtr & /*function*/, ContextPtr context,
    const std::string & table_name, ColumnsDescription /*cached_columns*/, bool /*is_insert_query*/) const
{
    StoragePtr storage;
    ColumnsDescription columns;

    if (configuration.structure != "auto")
    {
        columns = parseColumnsListFromString(configuration.structure, context);
    }
    else if (!structure_hint.empty())
    {
        columns = structure_hint;
    }

    if (context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
    {
        /// On worker node this filename won't contains globs
        storage = std::make_shared<StorageS3>(
            configuration,
            context,
            StorageID(getDatabaseName(), table_name),
            columns,
            ConstraintsDescription{},
            /* comment */String{},
            /* format_settings */std::nullopt, /// No format_settings for S3Cluster
            /*distributed_processing=*/true);
    }
    else
    {
        storage = std::make_shared<StorageS3Cluster>(
            cluster_name,
            configuration,
            StorageID(getDatabaseName(), table_name),
            columns,
            ConstraintsDescription{},
            context);
    }

    storage->startup();

    return storage;
}


void registerTableFunctionS3Cluster(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionS3Cluster>();
}


}

#endif
