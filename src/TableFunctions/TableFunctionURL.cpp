#include <TableFunctions/TableFunctionURL.h>

#include "registerTableFunctions.h"
#include <Access/AccessFlags.h>
#include <Formats/FormatFactory.h>
#include <Poco/URI.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageURL.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Storages/StorageExternalDistributed.h>


namespace DB
{
StoragePtr TableFunctionURL::getStorage(
    const String & source, const String & format_, const ColumnsDescription & columns, ContextPtr global_context,
    const std::string & table_name, const String & compression_method_) const
{
    /// If url contains {1..k} or failover options with separator `|`, use a separate storage
    if ((source.find('{') == std::string::npos || source.find('}') == std::string::npos) && source.find('|') == std::string::npos)
    {
        Poco::URI uri(source);
        return StorageURL::create(
            uri,
            StorageID(getDatabaseName(), table_name),
            format_,
            std::nullopt /*format settings*/,
            columns,
            ConstraintsDescription{},
            String{},
            global_context,
            compression_method_);
    }
    else
    {
        return StorageExternalDistributed::create(
            source,
            StorageID(getDatabaseName(), table_name),
            format_,
            std::nullopt,
            compression_method_,
            columns,
            ConstraintsDescription{},
            global_context);
    }
}

void registerTableFunctionURL(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionURL>();
}
}
