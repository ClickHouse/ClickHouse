#include <TableFunctions/Hive/TableFunctionHive.h>

#if USE_HIVE
#include <memory>
#include <type_traits>
#include <Common/Exception.h>
#include <Common/ErrorCodes.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserPartition.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/queryToString.h>
#include <Parsers/parseQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Storages/Hive/HiveSettings.h>
#include <Storages/Hive/StorageHive.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>
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

        if (args.size() != 5)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, message);

        for (auto & arg : args)
            arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context_);

        hive_metastore_url = args[0]->as<ASTLiteral &>().value.safeGet<String>();
        hive_database = args[1]->as<ASTLiteral &>().value.safeGet<String>();
        hive_table = args[2]->as<ASTLiteral &>().value.safeGet<String>();
        table_structure = args[3]->as<ASTLiteral &>().value.safeGet<String>();
        partition_by_def = args[4]->as<ASTLiteral &>().value.safeGet<String>();

        actual_columns = parseColumnsListFromString(table_structure, context_);
    }

    ColumnsDescription TableFunctionHive::getActualTableStructure(ContextPtr /*context_*/) const { return actual_columns; }

    StoragePtr TableFunctionHive::executeImpl(
        const ASTPtr & /*ast_function_*/,
        ContextPtr context_,
        const std::string & table_name_,
        ColumnsDescription /*cached_columns_*/) const
    {
        const Settings & settings = context_->getSettings();
        ParserLambdaExpression partition_by_parser;
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
