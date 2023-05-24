#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeEnum.h>
#include <Parsers/queryToString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/UserDefinedSQLFunctionFactory.h>
#include <Interpreters/UserDefinedExecutableFunctionFactory.h>
#include <Storages/System/StorageSystemFunctions.h>
#include <Common/escapeForFileName.h>
#include <Backups/BackupEntriesCollector.h>
#include <Backups/BackupEntryFromMemory.h>
#include <Backups/RestorerFromBackup.h>
#include <Backups/IBackup.h>
#include <Parsers/ParserCreateFunctionQuery.h>
#include <Parsers/parseQuery.h>

namespace fs = std::filesystem;


namespace DB
{

enum class FunctionOrigin : Int8
{
    SYSTEM = 0,
    SQL_USER_DEFINED = 1,
    EXECUTABLE_USER_DEFINED = 2
};

namespace ErrorCodes
{
    extern const int CANNOT_RESTORE_TABLE;
}

namespace
{
    template <typename Factory>
    void fillRow(MutableColumns & res_columns, const String & name, UInt64 is_aggregate, const String & create_query, FunctionOrigin function_origin, const Factory & f)
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
            res_columns[2]->insert(f.isCaseInsensitive(name));
            if (f.isAlias(name))
                res_columns[3]->insert(f.aliasTo(name));
            else
                res_columns[3]->insertDefault();
        }

        res_columns[4]->insert(create_query);
        res_columns[5]->insert(static_cast<Int8>(function_origin));
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

NamesAndTypesList StorageSystemFunctions::getNamesAndTypes()
{
    return {
        {"name", std::make_shared<DataTypeString>()},
        {"is_aggregate", std::make_shared<DataTypeUInt8>()},
        {"case_insensitive", std::make_shared<DataTypeUInt8>()},
        {"alias_to", std::make_shared<DataTypeString>()},
        {"create_query", std::make_shared<DataTypeString>()},
        {"origin", std::make_shared<DataTypeEnum8>(getOriginEnumsAndValues())}
    };
}

void StorageSystemFunctions::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    const auto & functions_factory = FunctionFactory::instance();
    const auto & function_names = functions_factory.getAllRegisteredNames();
    for (const auto & function_name : function_names)
    {
        fillRow(res_columns, function_name, UInt64(0), "", FunctionOrigin::SYSTEM, functions_factory);
    }

    const auto & aggregate_functions_factory = AggregateFunctionFactory::instance();
    const auto & aggregate_function_names = aggregate_functions_factory.getAllRegisteredNames();
    for (const auto & function_name : aggregate_function_names)
    {
        fillRow(res_columns, function_name, UInt64(1), "", FunctionOrigin::SYSTEM, aggregate_functions_factory);
    }

    const auto & user_defined_sql_functions_factory = UserDefinedSQLFunctionFactory::instance();
    const auto & user_defined_sql_functions_names = user_defined_sql_functions_factory.getAllRegisteredNames();
    for (const auto & function_name : user_defined_sql_functions_names)
    {
        auto create_query = queryToString(user_defined_sql_functions_factory.get(function_name));
        fillRow(res_columns, function_name, UInt64(0), create_query, FunctionOrigin::SQL_USER_DEFINED, user_defined_sql_functions_factory);
    }

    const auto & user_defined_executable_functions_factory = UserDefinedExecutableFunctionFactory::instance();
    const auto & user_defined_executable_functions_names = user_defined_executable_functions_factory.getRegisteredNames(context);
    for (const auto & function_name : user_defined_executable_functions_names)
    {
        fillRow(res_columns, function_name, UInt64(0), "", FunctionOrigin::EXECUTABLE_USER_DEFINED, user_defined_executable_functions_factory);
    }
}

void StorageSystemFunctions::backupData(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & /* partitions */)
{
    const auto & user_defined_sql_functions_factory = UserDefinedSQLFunctionFactory::instance();
    const auto & user_defined_sql_functions_names = user_defined_sql_functions_factory.getAllRegisteredNames();
    fs::path data_path_in_backup_fs{data_path_in_backup};
    for (const auto & function_name : user_defined_sql_functions_names)
    {
        auto ast = user_defined_sql_functions_factory.tryGet(function_name);
        if (!ast)
            continue;
        backup_entries_collector.addBackupEntry(
            data_path_in_backup_fs / (escapeForFileName(function_name) + ".sql"),
            std::make_shared<BackupEntryFromMemory>(queryToString(ast)));
    }
}

void StorageSystemFunctions::restoreDataFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & /* partitions */)
{
    auto backup = restorer.getBackup();
    fs::path data_path_in_backup_fs{data_path_in_backup};

    Strings filenames = backup->listFiles(data_path_in_backup);
    for (const auto & filename : filenames)
    {
        if (!filename.ends_with(".sql"))
        {
            throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE, "Cannot restore table {}: File name {} doesn't have the extension .sql",
                            getStorageID().getFullTableName(), String{data_path_in_backup_fs / filename});
        }
    }

    auto & user_defined_sql_functions_factory = UserDefinedSQLFunctionFactory::instance();
    const auto & restore_settings = restorer.getRestoreSettings();
    auto context = restorer.getContext();

    for (const auto & filename : filenames)
    {
        String escaped_function_name = filename.substr(0, filename.length() - strlen(".sql"));
        String function_name = unescapeForFileName(escaped_function_name);

        String filepath = data_path_in_backup_fs / filename;
        auto function_def_entry = backup->readFile(filepath);
        auto function_def_in = function_def_entry->getReadBuffer();
        String function_def;
        readStringUntilEOF(function_def, *function_def_in);

        ParserCreateFunctionQuery parser;
        ASTPtr ast = parseQuery(
            parser,
            function_def.data(),
            function_def.data() + function_def.size(),
            "in file " + filepath + " from backup " + backup->getName(),
            0,
            context->getSettingsRef().max_parser_depth);

        bool replace = (restore_settings.create_function == RestoreUDFCreationMode::kReplace);
        bool if_not_exists = (restore_settings.create_function == RestoreUDFCreationMode::kCreateIfNotExists);
        user_defined_sql_functions_factory.registerFunction(context, function_name, ast, replace, if_not_exists, true);
    }
}

}
