#include "getStructureOfRemoteTable.h"
#include <Core/Settings.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/DatabaseCatalog.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Storages/IStorage.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTFunction.h>
#include <Common/quoteString.h>
#include <Common/NetException.h>
#include <TableFunctions/TableFunctionFactory.h>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_result_bytes;
    extern const SettingsUInt64 max_result_rows;
}

namespace ErrorCodes
{
    extern const int NO_REMOTE_SHARD_AVAILABLE;
}


ColumnsDescription getStructureOfRemoteTableInShard(
    const Cluster & cluster,
    const Cluster::ShardInfo & shard_info,
    const StorageID & table_id,
    ContextPtr context,
    const ASTPtr & table_func_ptr)
{
    String query;
    const Settings & settings = context->getSettingsRef();

    if (table_func_ptr)
    {
        if (shard_info.isLocal())
        {
            TableFunctionPtr table_function_ptr = TableFunctionFactory::instance().get(table_func_ptr, context);
            return table_function_ptr->getActualTableStructure(context, /*is_insert_query*/ true);
        }

        auto table_func_name = queryToString(table_func_ptr);
        query = "DESC TABLE " + table_func_name;
    }
    else
    {
        if (shard_info.isLocal())
        {
            auto storage_ptr = DatabaseCatalog::instance().getTable(table_id, context);
            return storage_ptr->getInMemoryMetadataPtr()->getColumns();
        }

        /// Request for a table description
        query = "DESC TABLE " + table_id.getFullTableName();
    }

    ColumnsDescription res;
    auto new_context = ClusterProxy::updateSettingsForCluster(cluster, context, context->getSettingsRef(), table_id);

    /// Ignore limit for result number of rows (that could be set during handling CSE/CTE),
    /// since this is a service query and should not lead to query failure.
    {
        Settings new_settings = new_context->getSettingsCopy();
        new_settings[Setting::max_result_rows] = 0;
        new_settings[Setting::max_result_bytes] = 0;
        new_context->setSettings(new_settings);
    }

    /// Expect only needed columns from the result of DESC TABLE. NOTE 'comment' column is ignored for compatibility reasons.
    Block sample_block
    {
        { ColumnString::create(), std::make_shared<DataTypeString>(), "name" },
        { ColumnString::create(), std::make_shared<DataTypeString>(), "type" },
        { ColumnString::create(), std::make_shared<DataTypeString>(), "default_type" },
        { ColumnString::create(), std::make_shared<DataTypeString>(), "default_expression" },
    };

    /// Execute remote query without restrictions (because it's not real user query, but part of implementation)
    RemoteQueryExecutor executor(shard_info.pool, query, sample_block, new_context);
    executor.setPoolMode(PoolMode::GET_ONE);
    if (!table_func_ptr)
        executor.setMainTable(table_id);

    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();

    ParserExpression expr_parser;

    while (Block current = executor.readBlock())
    {
        ColumnPtr name = current.getByName("name").column;
        ColumnPtr type = current.getByName("type").column;
        ColumnPtr default_kind = current.getByName("default_type").column;
        ColumnPtr default_expr = current.getByName("default_expression").column;
        size_t size = name->size();

        for (size_t i = 0; i < size; ++i)
        {
            ColumnDescription column;

            column.name = (*name)[i].safeGet<const String &>();

            String data_type_name = (*type)[i].safeGet<const String &>();
            column.type = data_type_factory.get(data_type_name);

            String kind_name = (*default_kind)[i].safeGet<const String &>();
            if (!kind_name.empty())
            {
                column.default_desc.kind = columnDefaultKindFromString(kind_name);
                String expr_str = (*default_expr)[i].safeGet<const String &>();
                column.default_desc.expression = parseQuery(
                    expr_parser, expr_str.data(), expr_str.data() + expr_str.size(), "default expression",
                    0, settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);
            }

            res.add(column);
        }
    }

    executor.finish();
    return res;
}

ColumnsDescription getStructureOfRemoteTable(
    const Cluster & cluster,
    const StorageID & table_id,
    ContextPtr context,
    const ASTPtr & table_func_ptr)
{
    const auto & shards_info = cluster.getShardsInfo();

    std::string fail_messages;

    /// Use local shard as first priority, as it needs no network communication
    for (const auto & shard_info : shards_info)
    {
        if (shard_info.isLocal())
        {
            const auto & res = getStructureOfRemoteTableInShard(cluster, shard_info, table_id, context, table_func_ptr);
            chassert(!res.empty());
            return res;
        }
    }

    for (const auto & shard_info : shards_info)
    {
        try
        {
            const auto & res = getStructureOfRemoteTableInShard(cluster, shard_info, table_id, context, table_func_ptr);

            /// Expect at least some columns.
            /// This is a hack to handle the empty block case returned by Connection when skip_unavailable_shards is set.
            if (res.empty())
                continue;

            return res;
        }
        catch (const NetException &)
        {
            std::string fail_message = getCurrentExceptionMessage(false);
            fail_messages += fail_message + '\n';
            continue;
        }
    }

    throw NetException(ErrorCodes::NO_REMOTE_SHARD_AVAILABLE,
        "All attempts to get table structure failed. Log: \n\n{}\n", fail_messages);
}

ColumnsDescriptionByShardNum getExtendedObjectsOfRemoteTables(
    const Cluster & cluster,
    const StorageID & remote_table_id,
    const ColumnsDescription & storage_columns,
    ContextPtr context)
{
    const auto & shards_info = cluster.getShardsInfo();
    auto query = "DESC TABLE " + remote_table_id.getFullTableName();

    auto new_context = ClusterProxy::updateSettingsForCluster(cluster, context, context->getSettingsRef(), remote_table_id);
    new_context->setSetting("describe_extend_object_types", true);

    /// Expect only needed columns from the result of DESC TABLE.
    Block sample_block
    {
        { ColumnString::create(), std::make_shared<DataTypeString>(), "name" },
        { ColumnString::create(), std::make_shared<DataTypeString>(), "type" },
    };

    auto execute_query_on_shard = [&](const auto & shard_info)
    {
        /// Execute remote query without restrictions (because it's not real user query, but part of implementation)
        RemoteQueryExecutor executor(shard_info.pool, query, sample_block, new_context);

        executor.setPoolMode(PoolMode::GET_ONE);
        executor.setMainTable(remote_table_id);

        ColumnsDescription res;
        while (auto block = executor.readBlock())
        {
            const auto & name_col = *block.getByName("name").column;
            const auto & type_col = *block.getByName("type").column;

            size_t size = name_col.size();
            for (size_t i = 0; i < size; ++i)
            {
                auto name = name_col[i].safeGet<const String &>();
                auto type_name = type_col[i].safeGet<const String &>();

                auto storage_column = storage_columns.tryGetPhysical(name);
                if (storage_column && storage_column->type->hasDynamicSubcolumnsDeprecated())
                    res.add(ColumnDescription(std::move(name), DataTypeFactory::instance().get(type_name)));
            }
        }

        return res;
    };

    ColumnsDescriptionByShardNum columns;
    for (const auto & shard_info : shards_info)
    {
        auto res = execute_query_on_shard(shard_info);

        /// Expect at least some columns.
        /// This is a hack to handle the empty block case returned by Connection when skip_unavailable_shards is set.
        if (!res.empty())
            columns.emplace(shard_info.shard_num, std::move(res));
    }

    if (columns.empty())
        throw NetException(ErrorCodes::NO_REMOTE_SHARD_AVAILABLE, "All attempts to get table structure failed");

    return columns;
}

}
