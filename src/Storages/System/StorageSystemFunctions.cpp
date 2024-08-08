#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeEnum.h>
#include <Parsers/queryToString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionFactory.h>
#include <Storages/System/StorageSystemFunctions.h>


namespace DB
{

enum class FunctionOrigin : int8_t
{
    SYSTEM = 0,
    SQL_USER_DEFINED = 1,
    EXECUTABLE_USER_DEFINED = 2
};

namespace
{
    template <typename Factory>
    void fillRow(
        MutableColumns & res_columns,
        const String & name,
        UInt64 is_aggregate,
        const String & create_query,
        FunctionOrigin function_origin,
        const Factory & factory)
    {
        res_columns[0]->insert(name);
        res_columns[1]->insert(is_aggregate);

        if constexpr (std::is_same_v<Factory, UserDefinedSQLFunctionFactory> || std::is_same_v<Factory, UserDefinedExecutableFunctionFactory>)
        {
            res_columns[2]->insert(false);
            res_columns[3]->insertDefault();
        }
        else
        {
            res_columns[2]->insert(factory.isCaseInsensitive(name));
            if (factory.isAlias(name))
                res_columns[3]->insert(factory.aliasTo(name));
            else
                res_columns[3]->insertDefault();
        }

        res_columns[4]->insert(create_query);
        res_columns[5]->insert(static_cast<Int8>(function_origin));

        if constexpr (std::is_same_v<Factory, FunctionFactory>)
        {
            if (factory.isAlias(name))
            {
                res_columns[6]->insertDefault();
                res_columns[7]->insertDefault();
                res_columns[8]->insertDefault();
                res_columns[9]->insertDefault();
                res_columns[10]->insertDefault();
                res_columns[11]->insertDefault();
            }
            else
            {
                auto documentation = factory.getDocumentation(name);
                res_columns[6]->insert(documentation.description);
                res_columns[7]->insert(documentation.syntax);
                res_columns[8]->insert(documentation.argumentsAsString());
                res_columns[9]->insert(documentation.returned_value);
                res_columns[10]->insert(documentation.examplesAsString());
                res_columns[11]->insert(documentation.categoriesAsString());
            }
        }
        else
        {
            res_columns[6]->insertDefault();
            res_columns[7]->insertDefault();
            res_columns[8]->insertDefault();
            res_columns[9]->insertDefault();
            res_columns[10]->insertDefault();
            res_columns[11]->insertDefault();
        }
    }
}


std::vector<std::pair<String, Int8>> getOriginEnumsAndValues()
{
    return std::vector<std::pair<String, Int8>>{
        {"System", static_cast<Int8>(FunctionOrigin::SYSTEM)},
        {"SQLUserDefined", static_cast<Int8>(FunctionOrigin::SQL_USER_DEFINED)},
        {"ExecutableUserDefined", static_cast<Int8>(FunctionOrigin::EXECUTABLE_USER_DEFINED)}
    };
}

ColumnsDescription StorageSystemFunctions::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "The name of the function."},
        {"is_aggregate", std::make_shared<DataTypeUInt8>(), "Whether the function is an aggregate function."},
        {"case_insensitive", std::make_shared<DataTypeUInt8>(), "Whether the function name can be used case-insensitively."},
        {"alias_to", std::make_shared<DataTypeString>(), "The original function name, if the function name is an alias."},
        {"create_query", std::make_shared<DataTypeString>(), "Obsolete."},
        {"origin", std::make_shared<DataTypeEnum8>(getOriginEnumsAndValues()), "Obsolete."},
        {"description", std::make_shared<DataTypeString>(), "A high-level description what the function does."},
        {"syntax", std::make_shared<DataTypeString>(), "Signature of the function."},
        {"arguments", std::make_shared<DataTypeString>(), "What arguments does the function take."},
        {"returned_value", std::make_shared<DataTypeString>(), "What does the function return."},
        {"examples", std::make_shared<DataTypeString>(), "Usage example."},
        {"categories", std::make_shared<DataTypeString>(), "The category of the function."}
    };
}

void StorageSystemFunctions::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto & functions_factory = FunctionFactory::instance();
    const auto & function_names = functions_factory.getAllRegisteredNames();
    for (const auto & function_name : function_names)
    {
        fillRow(res_columns, function_name, 0, "", FunctionOrigin::SYSTEM, functions_factory);
    }

    const auto & aggregate_functions_factory = AggregateFunctionFactory::instance();
    const auto & aggregate_function_names = aggregate_functions_factory.getAllRegisteredNames();
    for (const auto & function_name : aggregate_function_names)
    {
        fillRow(res_columns, function_name, 1, "", FunctionOrigin::SYSTEM, aggregate_functions_factory);
    }

    const auto & user_defined_sql_functions_factory = UserDefinedSQLFunctionFactory::instance();
    const auto & user_defined_sql_functions_names = user_defined_sql_functions_factory.getAllRegisteredNames();
    for (const auto & function_name : user_defined_sql_functions_names)
    {
        auto create_query = queryToString(user_defined_sql_functions_factory.get(function_name));
        fillRow(res_columns, function_name, 0, create_query, FunctionOrigin::SQL_USER_DEFINED, user_defined_sql_functions_factory);
    }

    const auto & user_defined_executable_functions_factory = UserDefinedExecutableFunctionFactory::instance();
    const auto & user_defined_executable_functions_names = user_defined_executable_functions_factory.getRegisteredNames(context); /// NOLINT(readability-static-accessed-through-instance)
    for (const auto & function_name : user_defined_executable_functions_names)
    {
        fillRow(res_columns, function_name, 0, "", FunctionOrigin::EXECUTABLE_USER_DEFINED, user_defined_executable_functions_factory);
    }
}

void StorageSystemFunctions::backupData(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & /* partitions */)
{
    UserDefinedSQLFunctionFactory::instance().backup(backup_entries_collector, data_path_in_backup);
}

void StorageSystemFunctions::restoreDataFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & /* partitions */)
{
    UserDefinedSQLFunctionFactory::instance().restore(restorer, data_path_in_backup);
}

}
