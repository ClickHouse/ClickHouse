#include "TaskTable.h"

#include "ClusterPartition.h"
#include "TaskCluster.h"

#include <Parsers/ASTFunction.h>
#include <Common/escapeForFileName.h>

#include <boost/algorithm/string/join.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int LOGICAL_ERROR;
}

TaskTable::TaskTable(TaskCluster & parent, const Poco::Util::AbstractConfiguration & config,
                     const String & prefix_, const String & table_key)
        : task_cluster(parent)
{
    String table_prefix = prefix_ + "." + table_key + ".";

    name_in_config = table_key;

    number_of_splits = config.getUInt64(table_prefix + "number_of_splits", 3);

    allow_to_copy_alias_and_materialized_columns = config.getBool(table_prefix + "allow_to_copy_alias_and_materialized_columns", false);
    allow_to_drop_target_partitions = config.getBool(table_prefix + "allow_to_drop_target_partitions", false);

    cluster_pull_name = config.getString(table_prefix + "cluster_pull");
    cluster_push_name = config.getString(table_prefix + "cluster_push");

    table_pull.first = config.getString(table_prefix + "database_pull");
    table_pull.second = config.getString(table_prefix + "table_pull");

    table_push.first = config.getString(table_prefix + "database_push");
    table_push.second = config.getString(table_prefix + "table_push");

    /// Used as node name in ZooKeeper
    table_id = escapeForFileName(cluster_push_name)
               + "." + escapeForFileName(table_push.first)
               + "." + escapeForFileName(table_push.second);

    engine_push_str = config.getString(table_prefix + "engine", "rand()");

    {
        ParserStorage parser_storage{ParserStorage::TABLE_ENGINE};
        engine_push_ast = parseQuery(parser_storage, engine_push_str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
        engine_push_partition_key_ast = extractPartitionKey(engine_push_ast);
        primary_key_comma_separated = boost::algorithm::join(extractPrimaryKeyColumnNames(engine_push_ast), ", ");
        is_replicated_table = isReplicatedTableEngine(engine_push_ast);
    }

    sharding_key_str = config.getString(table_prefix + "sharding_key");

    auxiliary_engine_split_asts.reserve(number_of_splits);
    {
        ParserExpressionWithOptionalAlias parser_expression(false);
        sharding_key_ast = parseQuery(parser_expression, sharding_key_str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
        main_engine_split_ast = createASTStorageDistributed(cluster_push_name, table_push.first, table_push.second,
                                                            sharding_key_ast);

        for (const auto piece_number : collections::range(0, number_of_splits))
        {
            auxiliary_engine_split_asts.emplace_back
                    (
                            createASTStorageDistributed(cluster_push_name, table_push.first,
                                                        table_push.second + "_piece_" + toString(piece_number), sharding_key_ast)
                    );
        }
    }

    where_condition_str = config.getString(table_prefix + "where_condition", "");
    if (!where_condition_str.empty())
    {
        ParserExpressionWithOptionalAlias parser_expression(false);
        where_condition_ast = parseQuery(parser_expression, where_condition_str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);

        // Will use canonical expression form
        where_condition_str = queryToString(where_condition_ast);
    }

    String enabled_partitions_prefix = table_prefix + "enabled_partitions";
    has_enabled_partitions = config.has(enabled_partitions_prefix);

    if (has_enabled_partitions)
    {
        Strings keys;
        config.keys(enabled_partitions_prefix, keys);

        if (keys.empty())
        {
            /// Parse list of partition from space-separated string
            String partitions_str = config.getString(table_prefix + "enabled_partitions");
            boost::trim_if(partitions_str, isWhitespaceASCII);
            boost::split(enabled_partitions, partitions_str, isWhitespaceASCII, boost::token_compress_on);
        }
        else
        {
            /// Parse sequence of <partition>...</partition>
            for (const String &key : keys)
            {
                if (!startsWith(key, "partition"))
                    throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG, "Unknown key {} in {}", key, enabled_partitions_prefix);

                enabled_partitions.emplace_back(config.getString(enabled_partitions_prefix + "." + key));
            }
        }

        std::copy(enabled_partitions.begin(), enabled_partitions.end(), std::inserter(enabled_partitions_set, enabled_partitions_set.begin()));
    }
}


String TaskTable::getPartitionPath(const String & partition_name) const
{
    return task_cluster.task_zookeeper_path             // root
           + "/tables/" + table_id                      // tables/dst_cluster.merge.hits
           + "/" + escapeForFileName(partition_name);   // 201701
}

String TaskTable::getPartitionAttachIsActivePath(const String & partition_name) const
{
    return getPartitionPath(partition_name) + "/attach_active";
}

String TaskTable::getPartitionAttachIsDonePath(const String & partition_name) const
{
    return getPartitionPath(partition_name) + "/attach_is_done";
}

String TaskTable::getPartitionPiecePath(const String & partition_name, size_t piece_number) const
{
    assert(piece_number < number_of_splits);
    return getPartitionPath(partition_name) + "/piece_" + toString(piece_number);  // 1...number_of_splits
}

String TaskTable::getCertainPartitionIsDirtyPath(const String &partition_name) const
{
    return getPartitionPath(partition_name) + "/is_dirty";
}

String TaskTable::getCertainPartitionPieceIsDirtyPath(const String & partition_name, const size_t piece_number) const
{
    return getPartitionPiecePath(partition_name, piece_number) + "/is_dirty";
}

String TaskTable::getCertainPartitionIsCleanedPath(const String & partition_name) const
{
    return getCertainPartitionIsDirtyPath(partition_name) + "/cleaned";
}

String TaskTable::getCertainPartitionPieceIsCleanedPath(const String & partition_name, const size_t piece_number) const
{
    return getCertainPartitionPieceIsDirtyPath(partition_name, piece_number) + "/cleaned";
}

String TaskTable::getCertainPartitionTaskStatusPath(const String & partition_name) const
{
    return getPartitionPath(partition_name) + "/shards";
}

String TaskTable::getCertainPartitionPieceTaskStatusPath(const String & partition_name, const size_t piece_number) const
{
    return getPartitionPiecePath(partition_name, piece_number) + "/shards";
}

bool TaskTable::isReplicatedTable() const
{
    return is_replicated_table;
}

String TaskTable::getStatusAllPartitionCount() const
{
    return task_cluster.task_zookeeper_path + "/status/all_partitions_count";
}

String TaskTable::getStatusProcessedPartitionsCount() const
{
    return task_cluster.task_zookeeper_path + "/status/processed_partitions_count";
}

ASTPtr TaskTable::rewriteReplicatedCreateQueryToPlain() const
{
    ASTPtr prev_engine_push_ast = engine_push_ast->clone();

    auto & new_storage_ast = prev_engine_push_ast->as<ASTStorage &>();
    auto & new_engine_ast = new_storage_ast.engine->as<ASTFunction &>();

    /// Remove "Replicated" from name
    new_engine_ast.name = new_engine_ast.name.substr(10);

    if (new_engine_ast.arguments)
    {
        auto & replicated_table_arguments = new_engine_ast.arguments->children;


        /// In some cases of Atomic database engine usage ReplicatedMergeTree tables
        /// could be created without arguments.
        if (!replicated_table_arguments.empty())
        {
            /// Delete first two arguments of Replicated...MergeTree() table.
            replicated_table_arguments.erase(replicated_table_arguments.begin());
            replicated_table_arguments.erase(replicated_table_arguments.begin());
        }
    }

    return new_storage_ast.clone();
}

ClusterPartition & TaskTable::getClusterPartition(const String & partition_name)
{
    auto it = cluster_partitions.find(partition_name);
    if (it == cluster_partitions.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There are no cluster partition {} in {}", partition_name, table_id);
    return it->second;
}

}
