#include "getStructureOfRemoteTable.h"
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterDescribeQuery.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <DataTypes/DataTypeFactory.h>
#include <Storages/IStorage.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTFunction.h>
#include <TableFunctions/TableFunctionFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NO_REMOTE_SHARD_FOUND;
}


ColumnsDescription getStructureOfRemoteTable(
    const Cluster & cluster,
    const std::string & database,
    const std::string & table,
    const Context & context,
    const ASTPtr & table_func_ptr)
{
    /// Send to the first any remote shard.
    const auto & shard_info = cluster.getAnyShardInfo();
    
    String query;

    if (table_func_ptr)
    {
        if (shard_info.isLocal())
        {
            auto table_function = static_cast<ASTFunction *>(table_func_ptr.get());
            return TableFunctionFactory::instance().get(table_function->name, context)->execute(table_func_ptr, context)->getColumns();
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

    auto input = std::make_shared<RemoteBlockInputStream>(shard_info.pool, query, InterpreterDescribeQuery::getSampleBlock(), context);
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
            String column_name = (*name)[i].get<const String &>();
            String data_type_name = (*type)[i].get<const String &>();
            String kind_name = (*default_kind)[i].get<const String &>();

            auto data_type = data_type_factory.get(data_type_name);

            if (kind_name.empty())
                res.ordinary.emplace_back(column_name, std::move(data_type));
            else
            {
                auto kind = columnDefaultKindFromString(kind_name);

                String expr_str = (*default_expr)[i].get<const String &>();
                ASTPtr expr = parseQuery(expr_parser, expr_str.data(), expr_str.data() + expr_str.size(), "default expression", 0);
                res.defaults.emplace(column_name, ColumnDefault{kind, expr});

                if (ColumnDefaultKind::Default == kind)
                    res.ordinary.emplace_back(column_name, std::move(data_type));
                else if (ColumnDefaultKind::Materialized == kind)
                    res.materialized.emplace_back(column_name, std::move(data_type));
                else if (ColumnDefaultKind::Alias == kind)
                    res.aliases.emplace_back(column_name, std::move(data_type));
            }
        }
    }

    return res;
}

}
