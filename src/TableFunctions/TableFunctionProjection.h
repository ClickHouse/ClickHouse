#pragma once

#include <TableFunctions/ITableFunction.h>
#include <Interpreters/StorageID.h>

namespace DB
{
/* projection (table, projection) - creates a temporary StorageProjection.
 * The structure of the table is taken from the projection pipeline.
 * If there is no such table or projection, an exception is thrown.
 */
class TableFunctionProjection : public ITableFunction
{
public:
    static constexpr auto name = "projection";
    std::string getName() const override { return name; }

    void parseArguments(const ASTPtr & ast_function, const Context & context) override;
    ColumnsDescription getActualTableStructure(const Context & context) const override;

private:
    StoragePtr
    executeImpl(const ASTPtr & ast_function, const Context & context, const std::string & table_name, ColumnsDescription cached_columns)
        const override;
    const char * getStorageTypeName() const override { return "Projection"; }
    StorageID source_table_id = StorageID::createEmpty();
    String projection_name;
};


}
