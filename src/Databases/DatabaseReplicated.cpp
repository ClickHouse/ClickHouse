#include <iomanip>

#include <Core/Settings.h>
#include <Databases/DatabaseOnDisk.h>
#include <Databases/DatabaseOrdinary.h>
#include <Databases/DatabaseReplicated.h>
#include <Databases/DatabasesCommon.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/executeQuery.h>
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
#include <Common/setThreadName.h>
#include <Common/ThreadPool.h>
#include <Common/escapeForFileName.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>
#include <common/logger_useful.h>
#include <Common/Exception.h>

#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/Lock.h>

#include <ext/scope_guard.h>
#include <common/sleep.h>

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
    Context & context_)
//    : DatabaseOrdinary(name_, metadata_path_, "data/" + escapeForFileName(name_) + "/", "DatabaseReplicated (" + name_ + ")", context_)
    // TODO add constructor to Atomic and call it here with path and logger name specification
    : DatabaseAtomic(name_, metadata_path_, context_)
    , zookeeper_path(zookeeper_path_)
    , replica_name(replica_name_)
{
    if (!zookeeper_path.empty() && zookeeper_path.back() == '/')
        zookeeper_path.resize(zookeeper_path.size() - 1);
    // If zookeeper chroot prefix is used, path should start with '/', because chroot concatenates without it.
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

    if (!current_zookeeper->exists(zookeeper_path, {}, NULL)) {
        createDatabaseZKNodes();
    } 

    // replica
    if (!current_zookeeper->exists(replica_path, {}, NULL)) {
        current_zookeeper->createAncestors(replica_path);
        current_zookeeper->createOrUpdate(replica_path, String(), zkutil::CreateMode::Persistent);
    }

    //loadMetadataFromSnapshot();

    background_log_executor = global_context.getReplicatedSchedulePool().createTask(database_name + "(DatabaseReplicated::the_threeeed)", [this]{ runBackgroundLogExecutor();} );
    background_log_executor->schedule();
}

void DatabaseReplicated::createDatabaseZKNodes() {
    current_zookeeper = getZooKeeper();

    if (current_zookeeper->exists(zookeeper_path))
        return;

    current_zookeeper->createAncestors(zookeeper_path);

    current_zookeeper->createIfNotExists(zookeeper_path, String());
    current_zookeeper->createIfNotExists(zookeeper_path + "/last_entry", "0");
    current_zookeeper->createIfNotExists(zookeeper_path + "/log", String());
    current_zookeeper->createIfNotExists(zookeeper_path + "/snapshot", String());
}

void DatabaseReplicated::runBackgroundLogExecutor() {
    current_zookeeper = getZooKeeper();
    Strings log_entry_names = current_zookeeper->getChildren(zookeeper_path + "/log");

    std::sort(log_entry_names.begin(), log_entry_names.end());
    auto newest_entry_it = std::upper_bound(log_entry_names.begin(), log_entry_names.end(), last_executed_log_entry);

    log_entry_names.erase(log_entry_names.begin(), newest_entry_it);

    for (const String & log_entry_name : log_entry_names) {
        String log_entry_path = zookeeper_path + "/log/" + log_entry_name;
        executeFromZK(log_entry_path);
        last_executed_log_entry = log_entry_name;
    }

    background_log_executor->scheduleAfter(500);

    // String last_n = current_zookeeper->get(zookeeper_path + "/last_entry", {}, NULL);
    // size_t last_n_parsed = parse<size_t>(last_n);

    // bool newEntries = current_log_entry_n < last_n_parsed;
    // while (current_log_entry_n < last_n_parsed) {
    //     current_log_entry_n++;
    //     String log_path = zookeeper_path + "/log/log." + std::to_string(current_log_entry_n);
    //     executeFromZK(log_path);
    // }
    // if (newEntries) {
    //     saveState();
    // }
    // background_log_executor->scheduleAfter(500);
}

void DatabaseReplicated::saveState() {
    String state = std::to_string(current_log_entry_n);

    current_zookeeper = getZooKeeper();
    current_zookeeper->createOrUpdate(replica_path + "/last_entry", state, zkutil::CreateMode::Persistent);

    String metadata_file = getMetadataPath() + ".last_entry";
    WriteBufferFromFile out(metadata_file, state.size(), O_WRONLY | O_CREAT);
    writeString(state, out);
    out.next();
    if (global_context.getSettingsRef().fsync_metadata)
        out.sync();
    out.close();
}

void DatabaseReplicated::executeFromZK(String & path) {
        current_zookeeper = getZooKeeper();
        String query_to_execute = current_zookeeper->get(path, {}, NULL);
        ReadBufferFromString istr(query_to_execute);
        String dummy_string;
        WriteBufferFromString ostr(dummy_string);

        try
        {
            current_context = std::make_unique<Context>(global_context);
            current_context->getClientInfo().query_kind = ClientInfo::QueryKind::REPLICATED_LOG_QUERY;
            current_context->setCurrentDatabase(database_name);
            current_context->setCurrentQueryId(""); // generate random query_id
            executeQuery(istr, ostr, false, *current_context, {});
        }
        catch (...)
        {
            tryLogCurrentException(log, "Query from zookeeper " + query_to_execute + " wasn't finished successfully");
    
        }

        LOG_DEBUG(log, "Executed query: " << query_to_execute);
}

// TODO Move to ZooKeeper/Lock and remove it from here and ddlworker
// static std::unique_ptr<zkutil::Lock> createSimpleZooKeeperLock(
//     const std::shared_ptr<zkutil::ZooKeeper> & zookeeper, const String & lock_prefix, const String & lock_name, const String & lock_message)
// {
//     auto zookeeper_holder = std::make_shared<zkutil::ZooKeeperHolder>();
//     zookeeper_holder->initFromInstance(zookeeper);
//     return std::make_unique<zkutil::Lock>(std::move(zookeeper_holder), lock_prefix, lock_name, lock_message);
// }


void DatabaseReplicated::propose(const ASTPtr & query) {
    current_zookeeper = getZooKeeper();

    LOG_DEBUG(log, "PROPOSINGGG query: " << queryToString(query));
    current_zookeeper->create(zookeeper_path + "/log/log-", queryToString(query), zkutil::CreateMode::PersistentSequential);

    background_log_executor->schedule();
}

void DatabaseReplicated::updateSnapshot() {
    current_zookeeper = getZooKeeper();
    current_zookeeper->tryRemoveChildren(zookeeper_path + "/snapshot");
    for (auto iterator = getTablesIterator({}); iterator->isValid(); iterator->next()) {
        String table_name = iterator->name();
        auto query = getCreateQueryFromMetadata(getObjectMetadataPath(table_name), true);
        String statement = queryToString(query);
        current_zookeeper->createOrUpdate(zookeeper_path + "/snapshot/" + table_name, statement, zkutil::CreateMode::Persistent);
    }
}

void DatabaseReplicated::loadMetadataFromSnapshot() {
    current_zookeeper = getZooKeeper();

    Strings metadatas;
    if (current_zookeeper->tryGetChildren(zookeeper_path + "/snapshot", metadatas) != Coordination::ZOK)
        return;

    for (auto t = metadatas.begin(); t != metadatas.end(); ++t) {
        String path = zookeeper_path + "/snapshot/" + *t;
        executeFromZK(path);
    }
}

}
