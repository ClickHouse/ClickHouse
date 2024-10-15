#include "config.h"

#if USE_HIVE

#include <TableFunctions/ITableFunction.h>
#include <Poco/Logger.h>
#include <memory>
#include <Common/Exception.h>
#include <Common/ErrorCodes.h>
#include <Core/Settings.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Storages/Hive/HiveSettings.h>
#include <Storages/Hive/StorageHive.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/parseColumnsListForTableFunction.h>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_query_size;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


namespace
{

class TableFunctionHive : public ITableFunction
{
public:
    static constexpr auto name = "hive";
    static constexpr auto storage_type_name = "Hive";
    std::string getName() const override { return name; }

    bool hasStaticStructure() const override { return true; }

    StoragePtr executeImpl(
        const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;

    const char * getStorageTypeName() const override { return storage_type_name; }
    ColumnsDescription getActualTableStructure(ContextPtr, bool is_insert_query) const override;
    void parseArguments(const ASTPtr & ast_function_, ContextPtr context_) override;

private:
    LoggerPtr logger = getLogger("TableFunctionHive");

    String cluster_name;
    String hive_metastore_url;
    String hive_database;
    String hive_table;
    String table_structure;
    String partition_by_def;

    ColumnsDescription actual_columns;
};

void TableFunctionHive::parseArguments(const ASTPtr & ast_function_, ContextPtr context_)
{
    ASTs & args_func = ast_function_->children;
    if (args_func.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' must have arguments.", getName());

    ASTs & args = args_func.at(0)->children;

    if (args.size() != 5)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "The signature of function {} is:\n - hive_url, hive_database, hive_table, structure, partition_by_keys",
                        getName());

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context_);

    hive_metastore_url = checkAndGetLiteralArgument<String>(args[0], "hive_url");
    hive_database = checkAndGetLiteralArgument<String>(args[1], "hive_database");
    hive_table = checkAndGetLiteralArgument<String>(args[2], "hive_table");
    table_structure = checkAndGetLiteralArgument<String>(args[3], "structure");
    partition_by_def = checkAndGetLiteralArgument<String>(args[4], "partition_by_keys");

    actual_columns = parseColumnsListFromString(table_structure, context_);
}

ColumnsDescription TableFunctionHive::getActualTableStructure(ContextPtr /*context_*/, bool /*is_insert_query*/) const { return actual_columns; }

StoragePtr TableFunctionHive::executeImpl(
    const ASTPtr & /*ast_function_*/,
    ContextPtr context_,
    const std::string & table_name_,
    ColumnsDescription /*cached_columns_*/,
    bool /*is_insert_query*/) const
{
    const Settings & settings = context_->getSettingsRef();
    ParserExpression partition_by_parser;
    ASTPtr partition_by_ast = parseQuery(
        partition_by_parser,
        "(" + partition_by_def + ")",
        "partition by declaration list",
        settings[Setting::max_query_size],
        settings[Setting::max_parser_depth],
        settings[Setting::max_parser_backtracks]);
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

}


void registerTableFunctionHive(TableFunctionFactory & factory_) { factory_.registerFunction<TableFunctionHive>(); }

}

#endif
