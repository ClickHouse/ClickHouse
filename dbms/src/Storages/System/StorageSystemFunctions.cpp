#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Storages/System/StorageSystemFunctions.h>


namespace DB
{
namespace
{
    template <typename Factory>
    void fillRow(MutableColumns & res_columns, const String & name, UInt64 is_aggregate, const Factory & f)
    {
        res_columns[0]->insert(name);
        res_columns[1]->insert(is_aggregate);
        res_columns[2]->insert(UInt64(f.isCaseInsensitive(name)));
        if (f.isAlias(name))
            res_columns[3]->insert(f.aliasTo(name));
        else
            res_columns[3]->insert(String{});
    }
}

NamesAndTypesList StorageSystemFunctions::getNamesAndTypes()
{
    return {
        {"name", std::make_shared<DataTypeString>()},
        {"is_aggregate", std::make_shared<DataTypeUInt8>()},
        {"case_insensitive", std::make_shared<DataTypeUInt8>()},
        {"alias_to", std::make_shared<DataTypeString>()},
    };
}

void StorageSystemFunctions::fillData(MutableColumns & res_columns, const Context &, const SelectQueryInfo &) const
{
    const auto & functions_factory = FunctionFactory::instance();
    const auto & function_names = functions_factory.getAllRegisteredNames();
    for (const auto & name : function_names)
    {
        fillRow(res_columns, name, UInt64(0), functions_factory);
    }

    const auto & aggregate_functions_factory = AggregateFunctionFactory::instance();
    const auto & aggregate_function_names = aggregate_functions_factory.getAllRegisteredNames();
    for (const auto & name : aggregate_function_names)
    {
        fillRow(res_columns, name, UInt64(1), aggregate_functions_factory);
    }
}
}
