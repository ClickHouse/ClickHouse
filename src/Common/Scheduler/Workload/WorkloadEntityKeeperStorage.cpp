#include <Common/Scheduler/Workload/WorkloadEntityKeeperStorage.h>
#include <Common/Scheduler/Workload/WorkloadEntityConfigStorage.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateWorkloadQuery.h>
#include <Parsers/ASTCreateResourceQuery.h>
#include <Parsers/ParserCreateWorkloadEntity.h>
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

#include <unordered_set>

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
    , zookeeper_watch(std::make_shared<Coordination::WatchCallback>(
      [my_watch = watch](const Coordination::WatchResponse & response)
      {
          if (response.type == Coordination::Event::CHANGED)
          {
              std::unique_lock lock{my_watch->mutex};
              my_watch->triggered++;
              my_watch->cv.notify_one();
          }
      }))
    , config_storage(std::make_shared<WorkloadEntityConfigStorage>(global_context_))
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
        refreshEntities(zookeeper, false);
    }

    return zookeeper;
}

bool WorkloadEntityKeeperStorage::loadEntities(const Poco::Util::AbstractConfiguration & config)
{
    // Load config entities first
    bool config_changed = config_storage->loadEntities(config);
    bool changed = false;

    try
    {
        auto lock = getLock();
        changed = refreshEntities(getZooKeeper(), config_changed);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to load workload entities");
    }

    // Start watching thread (if not started yet)
    startWatchingThread();

    return changed;
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
            refreshEntities(getZooKeeper(), false);
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
    bool throw_if_exists,
    bool /*replace_if_exists*/,
    const Settings &)
{
    // Check if this entity exists in config storage (config entities cannot be stored to ZooKeeper)
    if (config_storage->has(entity_name))
    {
        if (throw_if_exists)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot store workload entity '{}' to ZooKeeper: it is defined in server configuration", entity_name);
        return OperationResult::Failed;
    }

    LOG_DEBUG(log, "Storing workload entity {}", backQuote(entity_name));

    String new_data = serializeAllEntities(Event{entity_type, entity_name, create_entity_query});
    auto zookeeper = getZooKeeper();

    Coordination::Stat stat;
    auto code = zookeeper->trySet(zookeeper_path, new_data, current_version, &stat);
    if (code != Coordination::Error::ZOK)
    {
        refreshEntities(zookeeper, false);
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
    bool throw_if_not_exists)
{
    // Check if this entity exists in config storage (config entities cannot be removed)
    if (config_storage->has(entity_name))
    {
        if (throw_if_not_exists)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot remove workload entity '{}': it is defined in server configuration", entity_name);
        return OperationResult::Failed;
    }

    LOG_DEBUG(log, "Removing workload entity {}", backQuote(entity_name));

    String new_data = serializeAllEntities(Event{entity_type, entity_name, {}});
    auto zookeeper = getZooKeeper();

    Coordination::Stat stat;
    auto code = zookeeper->trySet(zookeeper_path, new_data, current_version, &stat);
    if (code != Coordination::Error::ZOK)
    {
        refreshEntities(zookeeper, false);
        return OperationResult::Retry;
    }

    current_version = stat.version;

    LOG_DEBUG(log, "Workload entity {} removed", backQuote(entity_name));

    return OperationResult::Ok;
}

std::pair<String, Int32> WorkloadEntityKeeperStorage::getDataAndSetWatch(const zkutil::ZooKeeperPtr & zookeeper)
{
    Coordination::Stat stat;
    String data;
    bool exists = zookeeper->tryGetWatch(zookeeper_path, data, &stat, zookeeper_watch);
    if (!exists)
    {
        createRootNodes(zookeeper);
        data = zookeeper->getWatch(zookeeper_path, &stat, zookeeper_watch);
    }
    return {data, stat.version};
}

bool WorkloadEntityKeeperStorage::refreshEntities(const zkutil::ZooKeeperPtr & zookeeper, bool config_changed)
{
    auto [data, version] = getDataAndSetWatch(zookeeper);
    if (version == current_version && !config_changed)
        return false;

    LOG_DEBUG(log, "Refreshing workload entities from keeper");

    std::vector<std::pair<String, ASTPtr>> new_entities;

    // First, add entities from config (they take precedence)
    auto config_entities = config_storage->getAllEntities();
    for (const auto & [name, ast] : config_entities)
        new_entities.emplace_back(name, ast);

    // Create a set of config entity names for quick lookup
    std::unordered_set<String> config_entity_names;
    for (const auto & [name, ast] : config_entities)
        config_entity_names.insert(name);

    // Then parse and add ZooKeeper entities, but skip those that exist in config
    auto keeper_entities = parseEntitiesFromString(data, log);

    /// Add keeper entities that don't conflict with config entities
    for (const auto & [entity_name, query] : keeper_entities)
    {
        // Skip if this entity exists in config (config takes precedence)
        if (!config_entity_names.contains(entity_name))
            new_entities.emplace_back(entity_name, query);
    }

    bool changed = setAllEntities(new_entities);
    current_version = version;

    LOG_DEBUG(log, "Workload entities refreshing is done");

    return changed;
}

}
