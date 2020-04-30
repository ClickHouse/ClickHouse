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
    : DatabaseOrdinary(name_, metadata_path_, "data/", "DatabaseReplicated (" + name_ + ")", context_)
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
            throw Exception("Can't create replicated database without ZooKeeper", ErrorCodes::NO_ZOOKEEPER);


    }

    // test without this fancy mess (prob wont work)
    current_zookeeper->createAncestors(replica_path);
    current_zookeeper->createOrUpdate(replica_path, String(), zkutil::CreateMode::Persistent);

//    if (!current_zookeeper->exists(zookeeper_path)) {
//
//        LOG_DEBUG(log, "Creating database " << zookeeper_path);
//        current_zookeeper->createAncestors(zookeeper_path);

        // Coordination::Requests ops;
        // ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path, "",
        //     zkutil::CreateMode::Persistent));
        // ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/replicas", "",
        //     zkutil::CreateMode::Persistent));

        // Coordination::Responses responses;
        // auto code = current_zookeeper->tryMulti(ops, responses);
        // if (code && code != Coordination::ZNODEEXISTS)
        //     throw Coordination::Exception(code);
        // }
}

void DatabaseReplicated::createTable(
    const Context & context,
    const String & table_name,
    const StoragePtr & table,
    const ASTPtr & query)
{
    // try?
    DatabaseOnDisk::createTable(context, table_name, table, query);

    // suppose it worked
    String statement = getObjectDefinitionFromCreateQuery(query);
    LOG_DEBUG(log, "CREATE TABLE STATEMENT " << statement);

    // let's do dumb write to zk at the first iteration
    current_zookeeper = getZooKeeper();
    current_zookeeper->createOrUpdate(replica_path + "/" + table_name, statement, zkutil::CreateMode::Persistent);
}


void DatabaseReplicated::renameTable(
        const Context & context,
        const String & table_name,
        IDatabase & to_database,
        const String & to_table_name,
        bool exchange)
{
    // try
    DatabaseOnDisk::renameTable(context, table_name, to_database, to_table_name, exchange);
    // replicated stuff; what to put to a znode
    // String statement = getObjectDefinitionFromCreateQuery(query);
    // this one is fairly more complex
    current_zookeeper = getZooKeeper();

    // no need for now to have stat
    Coordination::Stat metadata_stat;
    auto statement = current_zookeeper->get(replica_path + "/" + table_name, &metadata_stat);
    current_zookeeper->createOrUpdate(replica_path + "/" + to_table_name, statement, zkutil::CreateMode::Persistent);
    current_zookeeper->remove(replica_path + "/" + table_name);
    // TODO add rename statement to the log
}

void DatabaseReplicated::dropTable(
        const Context & context,
        const String & table_name,
        bool no_delay)
{
    // try
    DatabaseOnDisk::dropTable(context, table_name, no_delay);

    // let's do dumb remove from zk at the first iteration
    current_zookeeper = getZooKeeper();
    current_zookeeper->remove(replica_path + "/" + table_name);
}

void DatabaseReplicated::drop(const Context & context)
{
    DatabaseOnDisk::drop(context);
    // replicated stuff
    //String statement = getObjectDefinitionFromCreateQuery(query);
    // should it be possible to recover after a drop. 
    // if not, we can just delete all the zookeeper nodes starting from
    // zookeeper path. does it work recursively? hope so...
}

// sync replica's zookeeper metadata
void DatabaseReplicated::syncReplicaState(Context & context) {
    auto c = context; // fixes unuser parameter error
    return;
}

// get the up to date metadata from zookeeper to local metadata dir
// for replicated (only?) tables
void DatabaseReplicated::updateMetadata(Context & context) {
    auto c = context; // fixes unuser parameter error
    return;
}

void DatabaseReplicated::loadStoredObjects(
    Context & context,
    bool has_force_restore_data_flag)
{
    syncReplicaState(context);
    updateMetadata(context);

    DatabaseOrdinary::loadStoredObjects(context, has_force_restore_data_flag);

}



}
