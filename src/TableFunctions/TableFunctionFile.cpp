#include <TableFunctions/TableFunctionFile.h>

#include "registerTableFunctions.h"
#include <Access/AccessFlags.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageFile.h>
#include <TableFunctions/TableFunctionFactory.h>

namespace DB
{
StoragePtr TableFunctionFile::getStorage(const String & source,
    const String & format_, const ColumnsDescription & columns,
    ContextPtr global_context, const std::string & table_name,
    const std::string & compression_method_) const
{
    // For `file` table function, we are going to use format settings from the
    // query context.
    StorageFile::CommonArguments args{
        WithContext(global_context),
        StorageID(getDatabaseName(), table_name),
        format_,
        std::nullopt /*format settings*/,
        compression_method_,
        columns,
        ConstraintsDescription{},
        String{},
    };

    return StorageFile::create(source, global_context->getUserFilesPath(), args);
}

void registerTableFunctionFile(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionFile>();
}
}
