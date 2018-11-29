#include <Storages/System/StorageSystemTableFunctions.h>

#include <TableFunctions/TableFunctionFactory.h>
namespace DB
{

NamesAndTypesList StorageSystemTableFunctions::getNamesAndTypes()
{
    return {{"name", std::make_shared<DataTypeString>()}};
}

void StorageSystemTableFunctions::fillData(MutableColumns & res_columns, const Context &, const SelectQueryInfo &) const
{
    const auto & functions = TableFunctionFactory::instance().getAllTableFunctions();
    for (const auto & pair : functions)
    {
        res_columns[0]->insert(pair.first);
    }
}

}
