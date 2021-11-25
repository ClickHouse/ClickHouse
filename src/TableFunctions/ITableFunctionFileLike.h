#pragma once

#include <TableFunctions/ITableFunction.h>

namespace DB
{
class ColumnsDescription;
class Context;

/*
 * function(source, format, structure) - creates a temporary storage from formatted source
 */
class ITableFunctionFileLike : public ITableFunction
{
private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns) const override;

    virtual StoragePtr getStorage(
        const String & source, const String & format, const ColumnsDescription & columns, ContextPtr global_context,
        const std::string & table_name, const String & compression_method) const = 0;

    ColumnsDescription getActualTableStructure(ContextPtr context) const override;

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    bool hasStaticStructure() const override { return true; }

    String filename;
    String format;
    String structure;
    String compression_method = "auto";
};
}
