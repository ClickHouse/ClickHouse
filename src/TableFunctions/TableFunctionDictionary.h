#pragma once

#include <TableFunctions/ITableFunction.h>

namespace DB
{
class Context;

/// dictionary(dictionary_name) - creates a temporary storage from dictionary
class TableFunctionDictionary final : public ITableFunction
{
public:
    static constexpr auto name = "dictionary";
    std::string getName() const override
    {
        return name;
    }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;

    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription, bool is_insert_query) const override;

    const char * getStorageTypeName() const override { return "Dictionary"; }

private:
    String dictionary_name;
    ColumnsDescription dictionary_columns;
};

}
