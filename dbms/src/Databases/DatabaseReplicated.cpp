#include <iomanip>

#include <Core/Settings.h>
#include <Databases/DatabaseOnDisk.h>
#include <Databases/DatabaseReplicated.h>
#include <Databases/DatabasesCommon.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Storages/StorageFactory.h>
#include <TableFunctions/TableFunctionFactory.h>

#include <Parsers/queryToString.h>

#include <Poco/DirectoryIterator.h>
#include <Poco/Event.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include <Common/escapeForFileName.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>
#include <common/logger_useful.h>

#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>

#include <ext/scope_guard.h>

namespace DB
{


namespace ErrorCodes
{
    extern const int NO_ZOOKEEPER;
}

void DatabaseReplicated::setZooKeeper(zkutil::ZooKeeperPtr zookeeper)
{
    std::lock_guard lock(current_zookeeper_mutex);
    current_zookeeper = zookeeper;
}

zkutil::ZooKeeperPtr DatabaseReplicated::tryGetZooKeeper() const
{
    std::lock_guard lock(current_zookeeper_mutex);
    return current_zookeeper;
}

zkutil::ZooKeeperPtr DatabaseReplicated::getZooKeeper() const
{
    auto res = tryGetZooKeeper();
    if (!res)
        throw Exception("Cannot get ZooKeeper", ErrorCodes::NO_ZOOKEEPER);
    return res;
}


DatabaseReplicated::DatabaseReplicated(
    const String & name_,
    const String & metadata_path_,
    const String & zookeeper_path_,
    const String & replica_name_,
    const Context & context_)
    : DatabaseOrdinary(name_, metadata_path_, context_)
    , zookeeper_path(zookeeper_path_)
    , replica_name(replica_name_)
{

    if (!zookeeper_path.empty() && zookeeper_path.back() == '/')
        zookeeper_path.resize(zookeeper_path.size() - 1);
    /// If zookeeper chroot prefix is used, path should start with '/', because chroot concatenates without it.
    if (!zookeeper_path.empty() && zookeeper_path.front() != '/')
        zookeeper_path = "/" + zookeeper_path;
    replica_path = zookeeper_path + "/replicas/" + replica_name;

    if (context_.hasZooKeeper()) {
        current_zookeeper = context_.getZooKeeper();
    }

    if (!current_zookeeper)
    {
        // TODO wtf is attach
        // if (!attach)
            throw Exception("Can't create replicated table without ZooKeeper", ErrorCodes::NO_ZOOKEEPER);

        /// Do not activate the replica. It will be readonly.
        // TODO is it relevant for engines?
        // LOG_ERROR(log, "No ZooKeeper: database will be in readonly mode.");
        // TODO is_readonly = true;
        // return;
    }
    // getObjectDefinitionFromCreateQuery
    // TODO what to do?
    // TODO createDatabaseIfNotExists ?
    // TODO check database structure ?
}

void DatabaseReplicated::createTable(
    const Context & context,
    const String & table_name,
    const StoragePtr & table,
    const ASTPtr & query)
{
    // try
    DatabaseOnDisk::createTable(context, table_name, table, query);
    // replicated stuff
    String statement = getObjectDefinitionFromCreateQuery(query);
    // ...

}


void DatabaseReplicated::renameTable(
        const Context & context,
        const String & table_name,
        IDatabase & to_database,
        const String & to_table_name,
        TableStructureWriteLockHolder & lock)
{
    // try
    DatabaseOnDisk::renameTable(context, table_name, to_database, to_table_name, lock);
    // replicated stuff
    String statement = getObjectDefinitionFromCreateQuery(query);
    // ...
}

void DatabaseReplicated::removeTable(
        const Context & context,
        const String & table_name)
{
    // try
    DatabaseOnDisk::removeTable(context, table_name);
    // replicated stuff
    String statement = getObjectDefinitionFromCreateQuery(query);
    // ...
}

void DatabaseReplicated::drop(const Context & context)
{
    DatabaseOnDisk::drop(context);
    // replicated stuff
    String statement = getObjectDefinitionFromCreateQuery(query);
    // ...
}

}
