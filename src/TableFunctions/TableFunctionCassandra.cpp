#include <Common/config.h>
#include <cassandra.h>

#if USE_CASSANDRA

#include "TableFunctionCassandra.h"
#include <Storages/StorageCassandra.h>
#include <Storages/fetchCassandraList.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include "Core/Block.h"
#include "Core/NamesAndTypes.h"
#include "DataTypes/Serializations/ISerialization.h"
#include "Processors/Executors/PullingPipelineExecutor.h"
#include "QueryPipeline/QueryPipeline.h"
#include "Storages/ColumnsDescription.h"
#include "Storages/ExternalDataSourceConfiguration.h"





namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void registerTableFunctionCassandra(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionCassandra>();
}

StoragePtr TableFunctionCassandra::executeImpl(
    const ASTPtr & /*ast_function*/,
    ContextPtr context,
    const std::string & table_name,
    ColumnsDescription /*cached_columns*/) const
{
    auto columns = getActualTableStructure(context);
    auto res = std::make_shared<StorageCassandra>(
        StorageID(configuration->database, table_name),
        configuration->host,
        configuration->port,
        configuration->database,
        configuration->table,
        configuration->consistency,
        configuration->username,
        configuration->password,
        configuration->options,
        columns,
        ConstraintsDescription{},
        String{});

    res->startup();
    return res;
}

void TableFunctionCassandra::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & args_func = ast_function->as<ASTFunction &>();

    if (!args_func.arguments)
        throw Exception("Table function 'cassandra' must have arguments.", ErrorCodes::LOGICAL_ERROR);
    configuration = StorageCassandra::getConfiguration(args_func.arguments->children, context);
}



ColumnsDescription TableFunctionCassandra::getActualTableStructure(ContextPtr /*context*/) const
{
    const auto tables_and_columns = fetchTablesCassandra(
        configuration->database,
        configuration->table,
        configuration->port,
        configuration->host,
        configuration->username,
        configuration->password,
        configuration->consistency
    );
    const auto columns = tables_and_columns.find(configuration->table);
    if (columns == tables_and_columns.end())
        throw Exception("wrong", ErrorCodes::UNKNOWN_TABLE);
    return columns->second;
}

}

#endif
