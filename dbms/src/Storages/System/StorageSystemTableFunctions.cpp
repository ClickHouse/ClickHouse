#include <Storages/System/StorageSystemTableFunctions.h>

#include <TableFunctions/TableFunctionFactory.h>
namespace DB
{
void StorageSystemTableFunctions::fillData(MutableColumns & res_columns) const
{
    const auto & functions = TableFunctionFactory::instance().getAllTableFunctions();
    for (const auto & pair : functions)
    {
        res_columns[0]->insert(pair.first);
    }
}
}
