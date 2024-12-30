#include <Common/Scheduler/Workload/WorkloadEntityKeeperStorage.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateWorkloadQuery.h>
#include <Parsers/ASTCreateResourceQuery.h>
#include <Parsers/ParserCreateWorkloadEntity.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <base/sleep.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/scope_guard_safe.h>
#include <Common/setThreadName.h>
#include <Core/Settings.h>

namespace DB
{
namespace Setting
{
extern const SettingsUInt64 max_parser_backtracks;
extern const SettingsUInt64 max_parser_depth;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

WorkloadEntityKeeperStorage::WorkloadEntityKeeperStorage(
    const ContextPtr & global_context_, const String & zookeeper_path_)
    : WorkloadEntityStorageBase(global_context_)
    , zookeeper_getter{[global_context_]() { return global_context_->getZooKeeper(); }}
    , zookeeper_path{zookeeper_path_}
    , watch{std::make_shared<WatchEvent>()}
{
    log = getLogger("WorkloadEntityKeeperStorage");
    if (zookeeper_path.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ZooKeeper path must be non-empty");

    if (zookeeper_path.back() == '/')
        zookeeper_path.pop_back();

    /// If zookeeper chroot prefix is used, path should start with '/', because chroot concatenates without it.
    if (zookeeper_path.front() != '/')
        zookeeper_path = "/" + zookeeper_path;
}

WorkloadEntityKeeperStorage::~WorkloadEntityKeeperStorage()
{
    SCOPE_EXIT_SAFE(stopWatchingThread());
}

void WorkloadEntityKeeperStorage::startWatchingThread()
{
    if (!watching_flag.exchange(true))
        watching_thread = ThreadFromGlobalPool(&WorkloadEntityKeeperStorage::processWatchQueue, this);
}

void WorkloadEntityKeeperStorage::stopWatchingThread()
{
    if (watching_flag.exchange(false))
    {
        watch->cv.notify_one();
        if (watching_thread.joinable())
            watching_thread.join();
    }
}

zkutil::ZooKeeperPtr WorkloadEntityKeeperStorage::getZooKeeper()
{
    auto [zookeeper, session_status] = zookeeper_getter.getZooKeeper();

    if (session_status == zkutil::ZooKeeperCachingGetter::SessionStatus::New)
    {
        /// It's possible that we connected to different [Zoo]Keeper instance
        /// so we may read a bit stale state.
        zookeeper->sync(zookeeper_path);

        createRootNodes(zookeeper);

        auto lock = getLock();
        refreshEntities(zookeeper);
    }

    return zookeeper;
}

void WorkloadEntityKeeperStorage::loadEntities()
{
    /// loadEntities() is called at start from Server::main(), so it's better not to stop here on no connection to ZooKeeper or any other error.
    /// However the watching thread must be started anyway in case the connection will be established later.
    try
    {
        auto lock = getLock();
        refreshEntities(getZooKeeper());
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to load workload entities");
    }
    startWatchingThread();
}


void WorkloadEntityKeeperStorage::processWatchQueue()
{
    LOG_DEBUG(log, "Started watching thread");
    setThreadName("WrkldEntWatch");

    UInt64 handled = 0;
    while (watching_flag)
    {
        try
        {
            /// Re-initialize ZooKeeper session if expired
            getZooKeeper();

            {
                std::unique_lock lock{watch->mutex};
                if (!watch->cv.wait_for(lock, std::chrono::seconds(10), [&] { return !watching_flag || handled != watch->triggered; }))
                    continue;
                handled = watch->triggered;
            }

            auto lock = getLock();
            refreshEntities(getZooKeeper());
        }
        catch (...)
        {
            tryLogCurrentException(log, "Will try to restart watching thread after error");
            zookeeper_getter.resetCache();
            sleepForSeconds(5);
        }
    }

    LOG_DEBUG(log, "Stopped watching thread");
}


void WorkloadEntityKeeperStorage::stopWatching()
{
    stopWatchingThread();
}

void WorkloadEntityKeeperStorage::createRootNodes(const zkutil::ZooKeeperPtr & zookeeper)
{
    zookeeper->createAncestors(zookeeper_path);
    // If node does not exist we consider it to be equal to empty node: no workload entities
    zookeeper->createIfNotExists(zookeeper_path, "");
}

WorkloadEntityStorageBase::OperationResult WorkloadEntityKeeperStorage::storeEntityImpl(
    const ContextPtr & /*current_context*/,
    WorkloadEntityType entity_type,
    const String & entity_name,
    ASTPtr create_entity_query,
    bool /*throw_if_exists*/,
    bool /*replace_if_exists*/,
    const Settings &)
{
    LOG_DEBUG(log, "Storing workload entity {}", backQuote(entity_name));

    String new_data = serializeAllEntities(Event{entity_type, entity_name, create_entity_query});
    auto zookeeper = getZooKeeper();

    Coordination::Stat stat;
    auto code = zookeeper->trySet(zookeeper_path, new_data, current_version, &stat);
    if (code != Coordination::Error::ZOK)
    {
        refreshEntities(zookeeper);
        return OperationResult::Retry;
    }

    current_version = stat.version;

    LOG_DEBUG(log, "Workload entity {} stored", backQuote(entity_name));

    return OperationResult::Ok;
}


WorkloadEntityStorageBase::OperationResult WorkloadEntityKeeperStorage::removeEntityImpl(
    const ContextPtr & /*current_context*/,
    WorkloadEntityType entity_type,
    const String & entity_name,
    bool /*throw_if_not_exists*/)
{
    LOG_DEBUG(log, "Removing workload entity {}", backQuote(entity_name));

    String new_data = serializeAllEntities(Event{entity_type, entity_name, {}});
    auto zookeeper = getZooKeeper();

    Coordination::Stat stat;
    auto code = zookeeper->trySet(zookeeper_path, new_data, current_version, &stat);
    if (code != Coordination::Error::ZOK)
    {
        refreshEntities(zookeeper);
        return OperationResult::Retry;
    }

    current_version = stat.version;

    LOG_DEBUG(log, "Workload entity {} removed", backQuote(entity_name));

    return OperationResult::Ok;
}

std::pair<String, Int32> WorkloadEntityKeeperStorage::getDataAndSetWatch(const zkutil::ZooKeeperPtr & zookeeper)
{
    const auto data_watcher = [my_watch = watch](const Coordination::WatchResponse & response)
    {
        if (response.type == Coordination::Event::CHANGED)
        {
            std::unique_lock lock{my_watch->mutex};
            my_watch->triggered++;
            my_watch->cv.notify_one();
        }
    };

    Coordination::Stat stat;
    String data;
    bool exists = zookeeper->tryGetWatch(zookeeper_path, data, &stat, data_watcher);
    if (!exists)
    {
        createRootNodes(zookeeper);
        data = zookeeper->getWatch(zookeeper_path, &stat, data_watcher);
    }
    return {data, stat.version};
}

void WorkloadEntityKeeperStorage::refreshEntities(const zkutil::ZooKeeperPtr & zookeeper)
{
    auto [data, version] = getDataAndSetWatch(zookeeper);
    if (version == current_version)
        return;

    LOG_DEBUG(log, "Refreshing workload entities from keeper");
    ASTs queries;
    ParserCreateWorkloadEntity parser;
    const char * begin = data.data(); /// begin of current query
    const char * pos = begin; /// parser moves pos from begin to the end of current query
    const char * end = begin + data.size();
    while (pos < end)
    {
        queries.emplace_back(parseQueryAndMovePosition(parser, pos, end, "", true, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS));
        while (isWhitespaceASCII(*pos) || *pos == ';')
            ++pos;
    }

    /// Read and parse all SQL entities from data we just read from ZooKeeper
    std::vector<std::pair<String, ASTPtr>> new_entities;
    for (const auto & query : queries)
    {
        LOG_TRACE(log, "Read keeper entity definition: {}", serializeAST(*query));
        if (auto * create_workload_query = query->as<ASTCreateWorkloadQuery>())
            new_entities.emplace_back(create_workload_query->getWorkloadName(), query);
        else if (auto * create_resource_query = query->as<ASTCreateResourceQuery>())
            new_entities.emplace_back(create_resource_query->getResourceName(), query);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid workload entity query in keeper storage: {}", query->getID());
    }

    setAllEntities(new_entities);
    current_version = version;

    LOG_DEBUG(log, "Workload entities refreshing is done");
}

}

