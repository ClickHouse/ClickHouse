#include <TableFunctions/Hive/TableFunctionHive.h>

#if USE_HIVE
#include <memory>
#include <Common/Exception.h>
#include <Common/ErrorCodes.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/queryToString.h>
#include <Parsers/parseQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Storages/Hive/HiveSettings.h>
#include <Storages/Hive/StorageHive.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Storages/ExternalDataSourceConfiguration.h>
#include <Common/logger_useful.h>

namespace DB
{
    namespace ErrorCodes
    {
        extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    }

    void TableFunctionHive::parseArguments(const ASTPtr & ast_function_, ContextPtr context_)
    {
        ASTs & args_func = ast_function_->children;
        if (args_func.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' must have arguments.", getName());

        ASTs & args = args_func.at(0)->children;

        const auto message = fmt::format(
            "The signature of function {} is:\n"
            " - hive_url, hive_database, hive_table, structure, partition_by_keys",
            getName());

        ExternalDataSourceConfiguration configuration;
        if (auto named_collection = getExternalDataSourceConfiguration(args, context_))
        {
            auto [common_configuration, storage_specific_args, _] = named_collection.value();
            configuration.set(common_configuration);

            for (const auto & [arg_name, arg_value] : storage_specific_args)
            {
                if (arg_name == "structure")
                    table_structure = checkAndGetLiteralArgument<String>(arg_value, "structure");
                else
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected key-value argument." "Got: {}. {}", arg_name, message);
            }

            hive_metastore_url = configuration.host;
            hive_database = configuration.database;
            hive_table = configuration.table;
            actual_columns = parseColumnsListFromString(table_structure, context_);

            assert(!hive_metastore_url.empty());
        }
        else
        {
            if (args.size() != 5)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, message);

            for (auto & arg : args)
                arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context_);

            hive_metastore_url = checkAndGetLiteralArgument<String>(args[0], "hive_url");
            hive_database = checkAndGetLiteralArgument<String>(args[1], "hive_database");
            hive_table = checkAndGetLiteralArgument<String>(args[2], "hive_table");
            table_structure = checkAndGetLiteralArgument<String>(args[3], "structure");
            partition_by_def = checkAndGetLiteralArgument<String>(args[4], "partition_by_keys");

            actual_columns = parseColumnsListFromString(table_structure, context_);
        }
    }

    ColumnsDescription TableFunctionHive::getActualTableStructure(ContextPtr /*context_*/) const { return actual_columns; }

    StoragePtr TableFunctionHive::executeImpl(
        const ASTPtr & /*ast_function_*/,
        ContextPtr context_,
        const std::string & table_name_,
        ColumnsDescription /*cached_columns_*/) const
    {
        const Settings & settings = context_->getSettings();
        ParserExpression partition_by_parser;
        ASTPtr partition_by_ast = parseQuery(
            partition_by_parser,
            "(" + partition_by_def + ")",
            "partition by declaration list",
            settings.max_query_size,
            settings.max_parser_depth);
        StoragePtr storage;
        storage = std::make_shared<StorageHive>(
            hive_metastore_url,
            hive_database,
            hive_table,
            StorageID(getDatabaseName(), table_name_),
            actual_columns,
            ConstraintsDescription{},
            "",
            partition_by_ast,
            std::make_unique<HiveSettings>(),
            context_);

        return storage;
    }


    void registerTableFunctionHive(TableFunctionFactory & factory_) { factory_.registerFunction<TableFunctionHive>(); }

}
#endif
