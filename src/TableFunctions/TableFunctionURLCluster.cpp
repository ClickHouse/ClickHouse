#include <TableFunctions/TableFunctionURLCluster.h>
#include <TableFunctions/TableFunctionFactory.h>

#include <TableFunctions/registerTableFunctions.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

StoragePtr TableFunctionURLCluster::getStorage(
    const String & /*source*/, const String & /*format_*/, const ColumnsDescription & columns, ContextPtr context,
    const std::string & table_name, const String & /*compression_method_*/, bool is_insert_query) const
{
    /// The `body(...)` argument only makes sense for reading, where it forms the HTTP request body.
    /// For `INSERT INTO FUNCTION urlCluster(...)` the inserted rows themselves are sent as the request body
    /// (see `IStorageURLBase::write`), so a user-provided `body` would be silently ignored. Reject it
    /// explicitly instead of dropping it, mirroring the same guard in `TableFunctionURL::getStorage`.
    /// This must run before `getActualTableStructure` below, which would otherwise send a body `POST`
    /// for schema inference when the structure is omitted.
    if (is_insert_query && !configuration.body.empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The 'body' argument is not supported for INSERT INTO FUNCTION urlCluster(...): "
            "the inserted data is sent as the request body.");

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
            configuration.body,
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
