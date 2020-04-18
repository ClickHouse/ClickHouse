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
    const auto & functions_names = TableFunctionFactory::instance().getAllRegisteredNames();
    for (const auto & function_name : functions_names)
    {
        res_columns[0]->insert(function_name);
    }
}

}
