#include "getStructureOfRemoteTable.h"
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/InterpreterDescribeQuery.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Storages/IStorage.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTFunction.h>
#include <Common/quoteString.h>
#include <TableFunctions/TableFunctionFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NO_REMOTE_SHARD_FOUND;
    extern const int NO_REMOTE_SHARD_AVAILABLE;
}


ColumnsDescription getStructureOfRemoteTable(
    const Cluster & cluster,
    const std::string & database,
    const std::string & table,
    const Context & context,
    const ASTPtr & table_func_ptr)
{
    const auto & shards_info = cluster.getShardsInfo();

    std::string fail_messages;

    for (auto & shard_info : shards_info)
    {
        try {
            const auto & res = getStructureOfRemoteTableInShard(shard_info, database, table, context, table_func_ptr);
            /// Expect at least some columns.
            /// This is a hack to handle the empty block case returned by Connection when skip_unavailable_shards is set.
            if (res.size() == 0)
            {
                continue;
            }

            return res;
        }
        catch (const DB::NetException & e)
        {
            /// THIS DOES NOT WORK AND IS ALWAYS FALSE???
            if (context.getSettingsRef().skip_unavailable_shards)
            {
                std::string fail_message = getCurrentExceptionMessage(false);
                fail_messages += fail_message + '\n';

                continue;
            }
            throw;
        }
    }

    throw DB::NetException(
        "All attempts to get table structure failed. Log: \n\n" + fail_messages + "\n",
        DB::ErrorCodes::NO_REMOTE_SHARD_AVAILABLE);
}

ColumnsDescription getStructureOfRemoteTableInShard(
    const Cluster::ShardInfo & shard_info,
    const std::string & database,
    const std::string & table,
    const Context & context,
    const ASTPtr & table_func_ptr)
{
    String query;

    if (table_func_ptr)
    {
        if (shard_info.isLocal())
        {
            const auto * table_function = table_func_ptr->as<ASTFunction>();
            TableFunctionPtr table_function_ptr = TableFunctionFactory::instance().get(table_function->name, context);
            return table_function_ptr->execute(table_func_ptr, context, table_function_ptr->getName())->getColumns();
        }

        auto table_func_name = queryToString(table_func_ptr);
        query = "DESC TABLE " + table_func_name;
    }
    else
    {
        if (shard_info.isLocal())
            return context.getTable(database, table)->getColumns();

        /// Request for a table description
        query = "DESC TABLE " + backQuoteIfNeed(database) + "." + backQuoteIfNeed(table);
    }

    ColumnsDescription res;

    auto new_context = ClusterProxy::removeUserRestrictionsFromSettings(context, context.getSettingsRef());

    /// Expect only needed columns from the result of DESC TABLE. NOTE 'comment' column is ignored for compatibility reasons.
    Block sample_block
    {
        { ColumnString::create(), std::make_shared<DataTypeString>(), "name" },
        { ColumnString::create(), std::make_shared<DataTypeString>(), "type" },
        { ColumnString::create(), std::make_shared<DataTypeString>(), "default_type" },
        { ColumnString::create(), std::make_shared<DataTypeString>(), "default_expression" },
    };

    /// Execute remote query without restrictions (because it's not real user query, but part of implementation)
    auto input = std::make_shared<RemoteBlockInputStream>(shard_info.pool, query, sample_block, new_context);
    input->setPoolMode(PoolMode::GET_ONE);
    if (!table_func_ptr)
        input->setMainTable(QualifiedTableName{database, table});
    input->readPrefix();

    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();

    ParserExpression expr_parser;

    while (Block current = input->read())
    {
        ColumnPtr name = current.getByName("name").column;
        ColumnPtr type = current.getByName("type").column;
        ColumnPtr default_kind = current.getByName("default_type").column;
        ColumnPtr default_expr = current.getByName("default_expression").column;
        size_t size = name->size();

        for (size_t i = 0; i < size; ++i)
        {
            ColumnDescription column;

            column.name = (*name)[i].get<const String &>();

            String data_type_name = (*type)[i].get<const String &>();
            column.type = data_type_factory.get(data_type_name);

            String kind_name = (*default_kind)[i].get<const String &>();
            if (!kind_name.empty())
            {
                column.default_desc.kind = columnDefaultKindFromString(kind_name);
                String expr_str = (*default_expr)[i].get<const String &>();
                column.default_desc.expression = parseQuery(
                    expr_parser, expr_str.data(), expr_str.data() + expr_str.size(), "default expression", 0);
            }

            res.add(column);
        }
    }

    return res;
}

}
