#pragma once

#include <chrono>
#include <optional>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/Logger.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/PatternFormatter.h>
#include <Poco/UUIDGenerator.h>
#include <Poco/File.h>
#include <Poco/Process.h>
#include <Poco/FileChannel.h>
#include <Poco/SplitterChannel.h>
#include <Poco/Util/HelpFormatter.h>
#include <boost/algorithm/string.hpp>
#include <pcg_random.hpp>
#include <common/logger_useful.h>
#include <Common/ThreadPool.h>
#include <Common/Exception.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/getFQDNOrHostName.h>
#include <Common/isLocalAddress.h>
#include <Common/typeid_cast.h>
#include <Common/ClickHouseRevision.h>
#include <Common/formatReadable.h>
#include <Common/DNSResolver.h>
#include <Common/CurrentThread.h>
#include <Common/escapeForFileName.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/ThreadStatus.h>
#include <Client/Connection.h>
#include <Interpreters/Context.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterExistsQuery.h>
#include <Interpreters/InterpreterShowCreateQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Formats/FormatSettings.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <DataStreams/AsynchronousBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <DataStreams/NullBlockOutputStream.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadBufferFromFile.h>
#include <Functions/registerFunctions.h>
#include <TableFunctions/registerTableFunctions.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Storages/registerStorages.h>
#include <Storages/StorageDistributed.h>
#include <Dictionaries/registerDictionaries.h>
#include <Disks/registerDisks.h>
#include <Databases/DatabaseMemory.h>
#include <Common/StatusFile.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_ZOOKEEPER;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_TABLE;
    extern const int UNFINISHED;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
}

using DatabaseAndTableName = std::pair<String, String>;

String getQuotedTable(const String & database, const String & table)
{
    if (database.empty())
    {
        return backQuoteIfNeed(table);
    }

    return backQuoteIfNeed(database) + "." + backQuoteIfNeed(table);
}

String getQuotedTable(const DatabaseAndTableName & db_and_table)
{
    return getQuotedTable(db_and_table.first, db_and_table.second);
}

enum class TaskState
{
    Started = 0,
    Finished,
    Unknown
};

/// Used to mark status of shard partition tasks
struct TaskStateWithOwner
{
    TaskStateWithOwner() = default;
    TaskStateWithOwner(TaskState state_, const String & owner_) : state(state_), owner(owner_) {}

    TaskState state{TaskState::Unknown};
    String owner;

    static String getData(TaskState state, const String & owner)
    {
        return TaskStateWithOwner(state, owner).toString();
    }

    String toString()
    {
        WriteBufferFromOwnString wb;
        wb << static_cast<UInt32>(state) << "\n" << escape << owner;
        return wb.str();
    }

    static TaskStateWithOwner fromString(const String & data)
    {
        ReadBufferFromString rb(data);
        TaskStateWithOwner res;
        UInt32 state;

        rb >> state >> "\n" >> escape >> res.owner;

        if (state >= static_cast<int>(TaskState::Unknown))
            throw Exception("Unknown state " + data, ErrorCodes::LOGICAL_ERROR);

        res.state = static_cast<TaskState>(state);
        return res;
    }
};


/// Hierarchical description of the tasks
struct ShardPartitionPiece;
struct ShardPartition;
struct TaskShard;
struct TaskTable;
struct TaskCluster;
struct ClusterPartition;

using PartitionPieces = std::vector<ShardPartitionPiece>;
using TasksPartition = std::map<String, ShardPartition, std::greater<>>;
using ShardInfo = Cluster::ShardInfo;
using TaskShardPtr = std::shared_ptr<TaskShard>;
using TasksShard = std::vector<TaskShardPtr>;
using TasksTable = std::list<TaskTable>;
using ClusterPartitions = std::map<String, ClusterPartition, std::greater<>>;



struct ShardPriority
{
    UInt8 is_remote = 1;
    size_t hostname_difference = 0;
    UInt8 random = 0;

    static bool greaterPriority(const ShardPriority & current, const ShardPriority & other)
    {
        return std::forward_as_tuple(current.is_remote, current.hostname_difference, current.random)
               < std::forward_as_tuple(other.is_remote, other.hostname_difference, other.random);
    }
};

/// Tables has many shards and table's partiton can be stored on different shards.
/// When we copy partition we have to discover it's shards (shards which store this partition)
/// For simplier retrieval of which partitions are stored in particular shard we created TaskShard.
struct TaskShard
{
    TaskShard(TaskTable & parent, const ShardInfo & info_) : task_table(parent), info(info_) {}

    TaskTable & task_table;

    ShardInfo info;
    UInt32 numberInCluster() const { return info.shard_num; }
    UInt32 indexInCluster() const { return info.shard_num - 1; }

    String getDescription() const;
    String getHostNameExample() const;

    /// Used to sort clusters by their proximity
    ShardPriority priority;

    /// Column with unique destination partitions (computed from engine_push_partition_key expr.) in the shard
    ColumnWithTypeAndName partition_key_column;

    /// There is a task for each destination partition
    TasksPartition partition_tasks;

    /// Which partitions have been checked for existence
    /// If some partition from this lists is exists, it is in partition_tasks
    std::set<String> checked_partitions;

    /// Last CREATE TABLE query of the table of the shard
    ASTPtr current_pull_table_create_query;

    /// Internal distributed tables
    DatabaseAndTableName table_read_shard;
    DatabaseAndTableName table_split_shard;
};


/// Contains info about all shards that contain a partition
struct ClusterPartition
{
    double elapsed_time_seconds = 0;
    UInt64 bytes_copied = 0;
    UInt64 rows_copied = 0;
    UInt64 blocks_copied = 0;

    UInt64 total_tries = 0;
};


struct MultiTransactionInfo
{
    int32_t code;
    Coordination::Requests requests;
    Coordination::Responses responses;
};

// Creates AST representing 'ENGINE = Distributed(cluster, db, table, [sharding_key])
std::shared_ptr<ASTStorage> createASTStorageDistributed(
        const String & cluster_name, const String & database, const String & table, const ASTPtr & sharding_key_ast = nullptr)
{
    auto args = std::make_shared<ASTExpressionList>();
    args->children.emplace_back(std::make_shared<ASTLiteral>(cluster_name));
    args->children.emplace_back(std::make_shared<ASTIdentifier>(database));
    args->children.emplace_back(std::make_shared<ASTIdentifier>(table));
    if (sharding_key_ast)
        args->children.emplace_back(sharding_key_ast);

    auto engine = std::make_shared<ASTFunction>();
    engine->name = "Distributed";
    engine->arguments = args;

    auto storage = std::make_shared<ASTStorage>();
    storage->set(storage->engine, engine);

    return storage;
}


BlockInputStreamPtr squashStreamIntoOneBlock(const BlockInputStreamPtr & stream)
{
    return std::make_shared<SquashingBlockInputStream>(
            stream,
            std::numeric_limits<size_t>::max(),
            std::numeric_limits<size_t>::max());
}

Block getBlockWithAllStreamData(const BlockInputStreamPtr & stream)
{
    return squashStreamIntoOneBlock(stream)->read();
}


static bool isExtendedDefinitionStorage(const ASTPtr & storage_ast)
{
    const auto & storage = storage_ast->as<ASTStorage &>();
    return storage.partition_by || storage.order_by || storage.sample_by;
}

static ASTPtr extractPartitionKey(const ASTPtr & storage_ast)
{
    String storage_str = queryToString(storage_ast);

    const auto & storage = storage_ast->as<ASTStorage &>();
    const auto & engine = storage.engine->as<ASTFunction &>();

    if (!endsWith(engine.name, "MergeTree"))
    {
        throw Exception("Unsupported engine was specified in " + storage_str + ", only *MergeTree engines are supported",
                        ErrorCodes::BAD_ARGUMENTS);
    }

    if (isExtendedDefinitionStorage(storage_ast))
    {
        if (storage.partition_by)
            return storage.partition_by->clone();

        static const char * all = "all";
        return std::make_shared<ASTLiteral>(Field(all, strlen(all)));
    }
    else
    {
        bool is_replicated = startsWith(engine.name, "Replicated");
        size_t min_args = is_replicated ? 3 : 1;

        if (!engine.arguments)
            throw Exception("Expected arguments in " + storage_str, ErrorCodes::BAD_ARGUMENTS);

        ASTPtr arguments_ast = engine.arguments->clone();
        ASTs & arguments = arguments_ast->children;

        if (arguments.size() < min_args)
            throw Exception("Expected at least " + toString(min_args) + " arguments in " + storage_str, ErrorCodes::BAD_ARGUMENTS);

        ASTPtr & month_arg = is_replicated ? arguments[2] : arguments[1];
        return makeASTFunction("toYYYYMM", month_arg->clone());
    }
}



static ShardPriority getReplicasPriority(const Cluster::Addresses & replicas, const std::string & local_hostname, UInt8 random)
{
    ShardPriority res;

    if (replicas.empty())
        return res;

    res.is_remote = 1;
    for (auto & replica : replicas)
    {
        if (isLocalAddress(DNSResolver::instance().resolveHost(replica.host_name)))
        {
            res.is_remote = 0;
            break;
        }
    }

    res.hostname_difference = std::numeric_limits<size_t>::max();
    for (auto & replica : replicas)
    {
        size_t difference = getHostNameDifference(local_hostname, replica.host_name);
        res.hostname_difference = std::min(difference, res.hostname_difference);
    }

    res.random = random;
    return res;
}

}
