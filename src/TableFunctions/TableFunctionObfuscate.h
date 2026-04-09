#pragma once

#include <TableFunctions/ITableFunction.h>
#include <Parsers/ASTCreateQuery.h>
#include <base/types.h>

namespace DB
{

/* obfuscate(query)
 * Obfuscates original data from a subquery.
 */
class TableFunctionObfuscate : public ITableFunction
{
public:
    static constexpr auto name = "obfuscate";
    std::string getName() const override { return name; }

    const ASTSelectWithUnionQuery & getSelectQuery() const;

private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const String & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;
    const char * getStorageEngineName() const override { return "Obfuscate"; }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;
    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;

    ASTCreateQuery create;
};


}
