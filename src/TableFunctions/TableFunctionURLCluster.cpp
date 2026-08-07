#include <TableFunctions/TableFunctionURLCluster.h>
#include <TableFunctions/TableFunctionFactory.h>

#include <Common/Exception.h>
#include <TableFunctions/registerTableFunctions.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace
{
    void checkURLClusterDoesNotUseIndexPageWildcards(const String & filename, const StorageURL::Configuration & configuration)
    {
        if (configuration.http_method.empty() && urlPathHasListableGlobs(filename))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "`urlCluster` does not support wildcard expansion from HTTP index pages");
    }
}

ColumnsDescription TableFunctionURLCluster::getActualTableStructure(ContextPtr context, bool is_insert_query) const
{
    checkURLClusterDoesNotUseIndexPageWildcards(filename, configuration);
    return TableFunctionURL::getActualTableStructure(context, is_insert_query);
}

StoragePtr TableFunctionURLCluster::getStorage(
    const String & /*source*/, const String & /*format_*/, const ColumnsDescription & columns, ContextPtr context,
    const std::string & table_name, const String & /*compression_method_*/, bool /*is_insert_query*/) const
{
    checkURLClusterDoesNotUseIndexPageWildcards(filename, configuration);

    if (context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
    {
        //On worker node this uri won't contain globs
        return std::make_shared<StorageURL>(
            filename,
            StorageID(getDatabaseName(), table_name),
            format,
            std::nullopt /*format settings*/,
            columns,
            ConstraintsDescription{},
            String{},
            context,
            compression_method,
            configuration.headers,
            configuration.http_method,
            nullptr,
            /*distributed_processing=*/ true);
    }

    return std::make_shared<StorageURLCluster>(
        context,
        cluster_name,
        filename,
        format,
        compression_method,
        StorageID(getDatabaseName(), table_name),
        getActualTableStructure(context, true),
        ConstraintsDescription{},
        configuration);
}

void registerTableFunctionURLCluster(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionURLCluster>({});
}

}
