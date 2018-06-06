#pragma once

#include <TableFunctions/ITableFunction.h>


namespace DB
{

/* catboostPool('column_descriptions_file', 'dataset_description_file')
 * Create storage from CatBoost dataset.
 */
class TableFunctionCatBoostPool : public ITableFunction
{
public:
    static constexpr auto name = "catBoostPool";
    std::string getName() const override { return name; }
private:
    StoragePtr executeImpl(const ASTPtr & ast_function, const Context & context) const override;
};

}
