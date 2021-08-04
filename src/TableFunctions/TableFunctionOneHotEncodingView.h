#pragma once

#include <TableFunctions/ITableFunction.h>
#include <Parsers/ASTCreateQuery.h>
#include <common/types.h>

namespace DB
{

/* one_hot_encoding_view((query),column[,column,...])
 * Turns a subquery into a table with the selected columns
 * encoded using one hot encoding and presented
 * as an Array(Uint8) encoded column.
 * Used for machine learning data preparation.
 */
class TableFunctionOneHotEncodingView : public ITableFunction
{
public:
    static constexpr auto name = "one_hot_encoding_view";
    std::string getName() const override { return name; }
private:
    std::tuple<const String, const String> getWithAndSelectForColumn(String base_query_str, const String & column_name);

    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context,
        const String & table_name, ColumnsDescription cached_columns) const override;
    const char * getStorageTypeName() const override { return "View"; }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;
    ColumnsDescription getActualTableStructure(ContextPtr context) const override;

    ASTCreateQuery create;
};


}
