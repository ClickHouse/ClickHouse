#pragma once

#include <TableFunctions/ITableFunction.h>
#include <Parsers/ASTCreateQuery.h>
#include <base/types.h>

namespace DB
{

/* viewIfPermitted(query ELSE null('structure'))
 * Works as "view(query)" if the current user has the permissions required to execute "query"; works as "null('structure')" otherwise.
 */
class TableFunctionViewIfPermitted : public ITableFunction
{
public:
    static constexpr auto name = "viewIfPermitted";
    std::string getName() const override { return name; }

    const ASTSelectWithUnionQuery & getSelectQuery() const;

private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const String & table_name, ColumnsDescription cached_columns) const override;
    const char * getStorageTypeName() const override { return "ViewIfPermitted"; }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;
    ColumnsDescription getActualTableStructure(ContextPtr context) const override;

    bool isPermitted(const ContextPtr & context, const ColumnsDescription & else_columns) const;

    ASTCreateQuery create;
    ASTPtr else_ast;
    TableFunctionPtr else_table_function;
};

}
