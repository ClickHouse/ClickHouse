#pragma once

#include <TableFunctions/ITableFunction.h>
#include <Interpreters/Context.h>

namespace DB
{
class ColumnsDescription;

/*
 * function(source, format, structure) - creates a temporary storage from formated source
 */
class ITableFunctionFileLike : public ITableFunction
{
private:
    StoragePtr executeImpl(const ASTPtr & ast_function, const Context & context, const std::string & table_name) const override;
    virtual StoragePtr getStorage(
        const String & source, const String & format, const ColumnsDescription & columns, Context & global_context, const std::string & table_name, const String & compression_method) const = 0;
};
}
