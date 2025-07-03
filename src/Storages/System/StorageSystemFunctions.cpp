#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeEnum.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionFactory.h>
#include <Storages/System/StorageSystemFunctions.h>


namespace DB
{

namespace
{

enum class IsDeterministic : int8_t
{
    No = 0,
    Yes = 1,
    Unknown = 2
};

/// Obsolete
enum class FunctionOrigin : int8_t
{
    System = 0,
    SqlUserDefines = 1,
    ExecutableUserDefined = 2
};

template <typename Factory>
void fillRow(
    MutableColumns & res_columns,
    const String & name,
    UInt64 is_aggregate,
    const String & create_query,
    FunctionOrigin function_origin,
    const Factory & factory,
    ContextPtr context)
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

    if constexpr (std::is_same_v<Factory, FunctionFactory>)
    {
        bool deterministic = FunctionFactory::instance().getImpl(name, context)->isDeterministic();
        res_columns[4]->insert(deterministic ? IsDeterministic::Yes : IsDeterministic::No);
    }
    else
    {
        res_columns[4]->insert(IsDeterministic::Unknown);
    }

    res_columns[5]->insert(create_query);
    res_columns[6]->insert(static_cast<Int8>(function_origin));

    if constexpr (std::is_same_v<Factory, FunctionFactory>)
    {
        if (factory.isAlias(name))
        {
            res_columns[7]->insertDefault();
            res_columns[8]->insertDefault();
            res_columns[9]->insertDefault();
            res_columns[10]->insertDefault();
            res_columns[11]->insertDefault();
            res_columns[12]->insertDefault();
            res_columns[13]->insertDefault();
        }
        else
        {
            auto documentation = factory.getDocumentation(name);
            res_columns[7]->insert(documentation.description);
            res_columns[8]->insert(documentation.syntaxAsString());
            res_columns[9]->insert(documentation.argumentsAsString());
            res_columns[10]->insert(documentation.returnedValueAsString());
            res_columns[11]->insert(documentation.examplesAsString());
            res_columns[12]->insert(documentation.introducedInAsString());
            res_columns[13]->insert(documentation.categoryAsString());
        }
    }
    else
    {
        res_columns[7]->insertDefault();
        res_columns[8]->insertDefault();
        res_columns[9]->insertDefault();
        res_columns[10]->insertDefault();
        res_columns[11]->insertDefault();
        res_columns[12]->insertDefault();
        res_columns[13]->insertDefault();
    }
}

std::vector<std::pair<String, Int8>> getIsDeterministicValues()
{
    return std::vector<std::pair<String, Int8>>{
        {"Yes", static_cast<Int8>(IsDeterministic::Yes)},
        {"No", static_cast<Int8>(IsDeterministic::No)},
        {"Unknown", static_cast<Int8>(IsDeterministic::Unknown)}
    };
}

/// Obsolete
std::vector<std::pair<String, Int8>> getOriginEnumsValues()
{
    return std::vector<std::pair<String, Int8>>{
        {"System", static_cast<Int8>(FunctionOrigin::System)},
        {"SQLUserDefined", static_cast<Int8>(FunctionOrigin::SqlUserDefines)},
        {"ExecutableUserDefined", static_cast<Int8>(FunctionOrigin::ExecutableUserDefined)}
    };
}

}

ColumnsDescription StorageSystemFunctions::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "The name of the function."},
        {"is_aggregate", std::make_shared<DataTypeUInt8>(), "Whether the function is an aggregate function."},
        {"case_insensitive", std::make_shared<DataTypeUInt8>(), "Whether the function name can be used case-insensitively."},
        {"alias_to", std::make_shared<DataTypeString>(), "The original function name, if the function name is an alias."},
        {"is_deterministic", std::make_shared<DataTypeEnum8>(getIsDeterministicValues()), "If the function is deterministic."},
        {"create_query", std::make_shared<DataTypeString>(), "Obsolete."},
        {"origin", std::make_shared<DataTypeEnum8>(getOriginEnumsValues()), "Obsolete."},
        {"description", std::make_shared<DataTypeString>(), "A high-level description what the function does."},
        {"syntax", std::make_shared<DataTypeString>(), "Signature of the function."},
        {"arguments", std::make_shared<DataTypeString>(), "What arguments does the function take."},
        {"returned_value", std::make_shared<DataTypeString>(), "What does the function return."},
        {"examples", std::make_shared<DataTypeString>(), "Usage example."},
        {"introduced_in", std::make_shared<DataTypeString>(), "ClickHouse version in which the function was first introduced."},
        {"categories", std::make_shared<DataTypeString>(), "The category of the function."}
    };
}

void StorageSystemFunctions::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto & functions_factory = FunctionFactory::instance();
    const auto & function_names = functions_factory.getAllRegisteredNames();
    for (const auto & function_name : function_names)
    {
        fillRow(res_columns, function_name, 0, "", FunctionOrigin::System, functions_factory, context);
    }

    const auto & aggregate_functions_factory = AggregateFunctionFactory::instance();
    const auto & aggregate_function_names = aggregate_functions_factory.getAllRegisteredNames();
    for (const auto & function_name : aggregate_function_names)
    {
        fillRow(res_columns, function_name, 1, "", FunctionOrigin::System, aggregate_functions_factory, context);
    }

    const auto & user_defined_sql_functions_factory = UserDefinedSQLFunctionFactory::instance();
    const auto & user_defined_sql_functions_names = user_defined_sql_functions_factory.getAllRegisteredNames();
    for (const auto & function_name : user_defined_sql_functions_names)
    {
        auto create_query = user_defined_sql_functions_factory.get(function_name)->formatWithSecretsOneLine();
        fillRow(res_columns, function_name, 0, create_query, FunctionOrigin::SqlUserDefines, user_defined_sql_functions_factory, context);
    }

    const auto & user_defined_executable_functions_factory = UserDefinedExecutableFunctionFactory::instance();
    const auto & user_defined_executable_functions_names = user_defined_executable_functions_factory.getRegisteredNames(context); /// NOLINT(readability-static-accessed-through-instance)
    for (const auto & function_name : user_defined_executable_functions_names)
    {
        fillRow(res_columns, function_name, 0, "", FunctionOrigin::ExecutableUserDefined, user_defined_executable_functions_factory, context);
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
