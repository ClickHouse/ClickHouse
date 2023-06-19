#include <Storages/System/StorageSystemTableFunctions.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_FUNCTION;
}

NamesAndTypesList StorageSystemTableFunctions::getNamesAndTypes()
{
    return
        {
            {"name", std::make_shared<DataTypeString>()},
            {"description", std::make_shared<DataTypeString>()},
            {"allow_readonly", std::make_shared<DataTypeUInt8>()}
       };
}

void StorageSystemTableFunctions::fillData(MutableColumns & res_columns, ContextPtr, const SelectQueryInfo &) const
{
    const auto & factory = TableFunctionFactory::instance();
    const auto & functions_names = factory.getAllRegisteredNames();
    for (const auto & function_name : functions_names)
    {
        res_columns[0]->insert(function_name);

        auto properties = factory.tryGetProperties(function_name);
        if (properties)
        {
            res_columns[1]->insert(properties->documentation.description);
            res_columns[2]->insert(properties->allow_readonly);
        }
        else
            throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "Unknown table function {}", function_name);
    }
}

}
