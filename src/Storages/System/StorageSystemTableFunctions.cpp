#include <Storages/System/StorageSystemTableFunctions.h>

#include <TableFunctions/TableFunctionFactory.h>
namespace DB
{

NamesAndTypesList StorageSystemTableFunctions::getNamesAndTypes()
{
    return
    {
        {"name", std::make_shared<DataTypeString>()},
        {"description", std::make_shared<DataTypeString>()}
    };
}

void StorageSystemTableFunctions::fillData(MutableColumns & res_columns, ContextPtr, const SelectQueryInfo &) const
{
    const auto & factory = TableFunctionFactory::instance();
    const auto & functions_names = factory.getAllRegisteredNames();
    for (const auto & function_name : functions_names)
    {
        res_columns[0]->insert(function_name);
        res_columns[1]->insert(factory.getDocumentation(function_name).description);
    }
}

}
