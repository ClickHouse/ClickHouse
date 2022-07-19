#pragma once

#include <chrono>
#include <optional>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/Logger.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/PatternFormatter.h>
#include <Poco/UUIDGenerator.h>
#include <Poco/Process.h>
#include <Poco/FileChannel.h>
#include <Poco/SplitterChannel.h>
#include <Poco/Util/HelpFormatter.h>
#include <boost/algorithm/string.hpp>
#include <common/logger_useful.h>
#include <Common/ThreadPool.h>
#include <Common/Exception.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <common/getFQDNOrHostName.h>
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
#include <DataTypes/NestedUtils.h>
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

#include "Aliases.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


ConfigurationPtr getConfigurationFromXMLString(const std::string & xml_data);

String getQuotedTable(const String & database, const String & table);

String getQuotedTable(const DatabaseAndTableName & db_and_table);


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

    static String getData(TaskState state, const String &owner)
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

/// Execution status of a task.
/// Is used for: partition copying task status, partition piece copying task status, partition moving task status.
enum class TaskStatus
{
    Active,
    Finished,
    Error,
};

struct MultiTransactionInfo
{
    int32_t code;
    Coordination::Requests requests;
    Coordination::Responses responses;
};

// Creates AST representing 'ENGINE = Distributed(cluster, db, table, [sharding_key])
std::shared_ptr<ASTStorage> createASTStorageDistributed(
        const String & cluster_name, const String & database, const String & table,
        const ASTPtr & sharding_key_ast = nullptr);


BlockInputStreamPtr squashStreamIntoOneBlock(const BlockInputStreamPtr & stream);

Block getBlockWithAllStreamData(const BlockInputStreamPtr & stream);

bool isExtendedDefinitionStorage(const ASTPtr & storage_ast);

ASTPtr extractPartitionKey(const ASTPtr & storage_ast);

/*
* Choosing a Primary Key that Differs from the Sorting Key
* It is possible to specify a primary key (an expression with values that are written in the index file for each mark)
* that is different from the sorting key (an expression for sorting the rows in data parts).
* In this case the primary key expression tuple must be a prefix of the sorting key expression tuple.
* This feature is helpful when using the SummingMergeTree and AggregatingMergeTree table engines.
* In a common case when using these engines, the table has two types of columns: dimensions and measures.
* Typical queries aggregate values of measure columns with arbitrary GROUP BY and filtering by dimensions.
* Because SummingMergeTree and AggregatingMergeTree aggregate rows with the same value of the sorting key,
* it is natural to add all dimensions to it. As a result, the key expression consists of a long list of columns
* and this list must be frequently updated with newly added dimensions.
* In this case it makes sense to leave only a few columns in the primary key that will provide efficient
* range scans and add the remaining dimension columns to the sorting key tuple.
* ALTER of the sorting key is a lightweight operation because when a new column is simultaneously added t
* o the table and to the sorting key, existing data parts don't need to be changed.
* Since the old sorting key is a prefix of the new sorting key and there is no data in the newly added column,
* the data is sorted by both the old and new sorting keys at the moment of table modification.
*
* */
ASTPtr extractPrimaryKey(const ASTPtr & storage_ast);

ASTPtr extractOrderBy(const ASTPtr & storage_ast);

Names extractPrimaryKeyColumnNames(const ASTPtr & storage_ast);

bool isReplicatedTableEngine(const ASTPtr & storage_ast);

ShardPriority getReplicasPriority(const Cluster::Addresses & replicas, const std::string & local_hostname, UInt8 random);

}
