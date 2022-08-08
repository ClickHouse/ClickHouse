#pragma once

#include <TableFunctions/ITableFunction.h>

namespace DB
{
/* directory(path, TODO) - creates a temporary storage from TODO
 *
 * TODO
 */
class TableFunctionDirectory : public ITableFunction
{
public:
    static constexpr auto name = "directory";
    std::string getName() const override { return name; }
    ColumnsDescription getActualTableStructure(ContextPtr context) const override {
        return ColumnsDescription();
    }
};

}
