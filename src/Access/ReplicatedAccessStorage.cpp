#include <memory>
#include <Access/AccessEntityIO.h>
#include <Access/MemoryAccessStorage.h>
#include <Access/ReplicatedAccessStorage.h>
#include <Access/AccessChangesNotifier.h>
#include <Access/AccessBackup.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/setThreadName.h>
#include <Common/ThreadPool.h>
#include <Common/escapeForFileName.h>
#include <base/range.h>
#include <base/sleep.h>
#include <boost/range/algorithm_ext/erase.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NO_ZOOKEEPER;
}

static UUID parseUUID(const String & text)
{
    UUID uuid = UUIDHelpers::Nil;
    auto buffer = ReadBufferFromMemory(text.data(), text.length());
    readUUIDText(uuid, buffer);
    return uuid;
}

ReplicatedAccessStorage::ReplicatedAccessStorage(
    const String & storage_name_,
    const String & zookeeper_path_,
    zkutil::GetZooKeeper get_zookeeper_,
    AccessChangesNotifier & changes_notifier_,
    bool allow_backup_)
    : IAccessStorage(storage_name_)
    , zookeeper_path(zookeeper_path_)
    , get_zookeeper(get_zookeeper_)
    , watched_queue(std::make_shared<ConcurrentBoundedQueue<UUID>>(std::numeric_limits<size_t>::max()))
    , memory_storage(storage_name_, changes_notifier_, false)
    , changes_notifier(changes_notifier_)
    , backup_allowed(allow_backup_)
{
    if (zookeeper_path.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ZooKeeper path must be non-empty");

    if (zookeeper_path.back() == '/')
        zookeeper_path.resize(zookeeper_path.size() - 1);

    /// If zookeeper chroot prefix is used, path should start with '/', because chroot concatenates without it.
    if (zookeeper_path.front() != '/')
        zookeeper_path = "/" + zookeeper_path;

    initZooKeeperWithRetries(/* max_retries= */ 2);
}

ReplicatedAccessStorage::~ReplicatedAccessStorage()
{
    try
    {
        ReplicatedAccessStorage::shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void ReplicatedAccessStorage::shutdown()
{
    stopWatchingThread();
}

void ReplicatedAccessStorage::startWatchingThread()
{
    bool prev_watching_flag = watching.exchange(true);
    if (!prev_watching_flag)
        watching_thread = std::make_unique<ThreadFromGlobalPool>(&ReplicatedAccessStorage::runWatchingThread, this);
}

void ReplicatedAccessStorage::stopWatchingThread()
{
    bool prev_watching_flag = watching.exchange(false);
    if (prev_watching_flag)
    {
        watched_queue->finish();
        if (watching_thread && watching_thread->joinable())
            watching_thread->join();
    }
}

template <typename Func>
static void retryOnZooKeeperUserError(size_t attempts, Func && function)
{
    while (attempts > 0)
    {
        try
        {
            function();
            return;
        }
        catch (zkutil::KeeperException & keeper_exception)
        {
            if (Coordination::isUserError(keeper_exception.code) && attempts > 1)
                attempts -= 1;
            else
                throw;
        }
    }
}

bool ReplicatedAccessStorage::insertImpl(const UUID & id, const AccessEntityPtr & new_entity, bool replace_if_exists, bool throw_if_exists, UUID * conflicting_id)
{
    const AccessEntityTypeInfo type_info = AccessEntityTypeInfo::get(new_entity->getType());
    const String & name = new_entity->getName();
    LOG_DEBUG(getLogger(), "Inserting entity of type {} named {} with id {}", type_info.name, name, toString(id));

    auto zookeeper = getZooKeeper();
    bool ok = false;
    retryOnZooKeeperUserError(10, [&]{ ok = insertZooKeeper(zookeeper, id, new_entity, replace_if_exists, throw_if_exists, conflicting_id); });

    if (!ok)
        return false;

    refreshEntity(zookeeper, id);
    return true;
}


bool ReplicatedAccessStorage::insertZooKeeper(
    const zkutil::ZooKeeperPtr & zookeeper,
    const UUID & id,
    const AccessEntityPtr & new_entity,
    bool replace_if_exists,
    bool throw_if_exists,
    UUID * conflicting_id)
{
    const String & name = new_entity->getName();
    const AccessEntityType type = new_entity->getType();
    const AccessEntityTypeInfo type_info = AccessEntityTypeInfo::get(type);

    const String entity_uuid = toString(id);
    /// The entity data will be stored here, this ensures all entities have unique ids
    const String entity_path = zookeeper_path + "/uuid/" + entity_uuid;
    /// Then we create a znode with the entity name, inside the znode of each entity type
    /// This ensure all entities of the same type have a unique name
    const String name_path = zookeeper_path + "/" + type_info.unique_char + "/" + escapeForFileName(name);

    Coordination::Requests ops;
    const String new_entity_definition = serializeAccessEntity(*new_entity);
    ops.emplace_back(zkutil::makeCreateRequest(entity_path, new_entity_definition, zkutil::CreateMode::Persistent));
    /// The content of the "name" znode is the uuid of the entity owning that name
    ops.emplace_back(zkutil::makeCreateRequest(name_path, entity_uuid, zkutil::CreateMode::Persistent));

    Coordination::Responses responses;
    const Coordination::Error res = zookeeper->tryMulti(ops, responses);

    if (res == Coordination::Error::ZNODEEXISTS)
    {
        if (!replace_if_exists)
        {
            if (responses[0]->error == Coordination::Error::ZNODEEXISTS)
            {
                /// Couldn't insert the new entity because there is an existing entity with such UUID.
                if (throw_if_exists)
                {
                    /// To fail with a nice error message, we need info about what already exists.
                    /// This itself can fail if the conflicting uuid disappears in the meantime.
                    /// If that happens, then retryOnZooKeeperUserError() will just retry the operation from the start.
                    String existing_entity_definition = zookeeper->get(entity_path);

                    AccessEntityPtr existing_entity = deserializeAccessEntity(existing_entity_definition, entity_path);
                    AccessEntityType existing_type = existing_entity->getType();
                    String existing_name = existing_entity->getName();
                    throwIDCollisionCannotInsert(id, type, name, existing_type, existing_name);
                }
                else
                {
                    if (conflicting_id)
                        *conflicting_id = id;
                    return false;
                }
            }
            else if (responses[1]->error == Coordination::Error::ZNODEEXISTS)
            {
                /// Couldn't insert the new entity because there is an existing entity with the same name.
                if (throw_if_exists)
                {
                    throwNameCollisionCannotInsert(type, name);
                }
                else
                {
                    if (conflicting_id)
                    {
                        /// Get UUID of the existing entry with the same name.
                        /// This itself can fail if the conflicting name disappears in the meantime.
                        /// If that happens, then retryOnZooKeeperUserError() will just retry the operation from the start.
                        *conflicting_id = parseUUID(zookeeper->get(name_path));
                    }
                    return false;
                }
            }
            else
            {
                zkutil::KeeperMultiException::check(res, ops, responses);
            }
        }

        assert(replace_if_exists);
        Coordination::Requests replace_ops;
        if (responses[0]->error == Coordination::Error::ZNODEEXISTS)
        {
            /// The UUID is already associated with some existing entity, we will get rid of the conflicting entity first.
            /// This itself could fail if the conflicting entity disappears in the meantime.
            /// If that happens, then we'll just retry from the start.
            Coordination::Stat stat;
            String existing_entity_definition = zookeeper->get(entity_path, &stat);
            auto existing_entity = deserializeAccessEntity(existing_entity_definition, entity_path);
            const String & existing_entity_name = existing_entity->getName();
            const AccessEntityType existing_entity_type = existing_entity->getType();
            const AccessEntityTypeInfo existing_entity_type_info = AccessEntityTypeInfo::get(existing_entity_type);
            const String existing_name_path = zookeeper_path + "/" + existing_entity_type_info.unique_char + "/" + escapeForFileName(existing_entity_name);

            if (existing_name_path != name_path)
                replace_ops.emplace_back(zkutil::makeRemoveRequest(existing_name_path, -1));

            replace_ops.emplace_back(zkutil::makeSetRequest(entity_path, new_entity_definition, stat.version));
        }
        else
        {
            replace_ops.emplace_back(zkutil::makeCreateRequest(entity_path, new_entity_definition, zkutil::CreateMode::Persistent));
        }

        if (responses[1]->error == Coordination::Error::ZNODEEXISTS)
        {
            /// The name is already associated with some existing entity, we will get rid of the conflicting entity first.
            /// This itself could fail if the conflicting entity disappears in the meantime.
            /// If that happens, then we'll just retry from the start.
            Coordination::Stat stat;
            String existing_entity_uuid = zookeeper->get(name_path, &stat);
            const String existing_entity_path = zookeeper_path + "/uuid/" + existing_entity_uuid;

            if (existing_entity_path != entity_path)
                replace_ops.emplace_back(zkutil::makeRemoveRequest(existing_entity_path, -1));

            replace_ops.emplace_back(zkutil::makeSetRequest(name_path, entity_uuid, stat.version));
        }
        else
        {
            replace_ops.emplace_back(zkutil::makeCreateRequest(name_path, entity_uuid, zkutil::CreateMode::Persistent));
        }

        /// If this fails, then we'll just retry from the start.
        zookeeper->multi(replace_ops);

        /// Everything's fine, the new entity has been inserted instead of an existing entity.
        return true;
    }

    /// If this fails, then we'll just retry from the start.
    zkutil::KeeperMultiException::check(res, ops, responses);

    /// Everything's fine, the new entity has been inserted.
    return true;
}

bool ReplicatedAccessStorage::removeImpl(const UUID & id, bool throw_if_not_exists)
{
    LOG_DEBUG(getLogger(), "Removing entity {}", toString(id));

    auto zookeeper = getZooKeeper();
    bool ok = false;
    retryOnZooKeeperUserError(10, [&] { ok = removeZooKeeper(zookeeper, id, throw_if_not_exists); });

    if (!ok)
        return false;

    std::lock_guard lock{mutex};
    removeEntityNoLock(id);
    return true;
}


bool ReplicatedAccessStorage::removeZooKeeper(const zkutil::ZooKeeperPtr & zookeeper, const UUID & id, bool throw_if_not_exists)
{
    const String entity_uuid = toString(id);
    const String entity_path = zookeeper_path + "/uuid/" + entity_uuid;

    String entity_definition;
    Coordination::Stat entity_stat;
    const bool uuid_exists = zookeeper->tryGet(entity_path, entity_definition, &entity_stat);
    if (!uuid_exists)
    {
        /// Couldn't remove, there is no such entity.
        if (throw_if_not_exists)
            throwNotFound(id);
        else
            return false;
    }

    const AccessEntityPtr entity = deserializeAccessEntity(entity_definition, entity_path);
    const AccessEntityTypeInfo type_info = AccessEntityTypeInfo::get(entity->getType());
    const String & name = entity->getName();

    const String entity_name_path = zookeeper_path + "/" + type_info.unique_char + "/" + escapeForFileName(name);

    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeRemoveRequest(entity_path, entity_stat.version));
    ops.emplace_back(zkutil::makeRemoveRequest(entity_name_path, -1));

    /// If this fails, then we'll just retry from the start.
    zookeeper->multi(ops);

    /// Everything's fine, the entity has been removed.
    return true;
}


bool ReplicatedAccessStorage::updateImpl(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists)
{
    LOG_DEBUG(getLogger(), "Updating entity {}", toString(id));

    auto zookeeper = getZooKeeper();
    bool ok = false;
    retryOnZooKeeperUserError(10, [&] { ok = updateZooKeeper(zookeeper, id, update_func, throw_if_not_exists); });

    if (!ok)
        return false;

    refreshEntity(zookeeper, id);
    return true;
}


bool ReplicatedAccessStorage::updateZooKeeper(const zkutil::ZooKeeperPtr & zookeeper, const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists)
{
    const String entity_uuid = toString(id);
    const String entity_path = zookeeper_path + "/uuid/" + entity_uuid;

    String old_entity_definition;
    Coordination::Stat stat;
    const bool uuid_exists = zookeeper->tryGet(entity_path, old_entity_definition, &stat);
    if (!uuid_exists)
    {
        if (throw_if_not_exists)
            throwNotFound(id);
        else
            return false;
    }

    const AccessEntityPtr old_entity = deserializeAccessEntity(old_entity_definition, entity_path);
    const AccessEntityPtr new_entity = update_func(old_entity, id);

    if (!new_entity->isTypeOf(old_entity->getType()))
        throwBadCast(id, new_entity->getType(), new_entity->getName(), old_entity->getType());

    const AccessEntityTypeInfo type_info = AccessEntityTypeInfo::get(new_entity->getType());

    Coordination::Requests ops;
    const String new_entity_definition = serializeAccessEntity(*new_entity);
    ops.emplace_back(zkutil::makeSetRequest(entity_path, new_entity_definition, stat.version));

    const String & old_name = old_entity->getName();
    const String & new_name = new_entity->getName();
    if (new_name != old_name)
    {
        auto old_name_path = zookeeper_path + "/" + type_info.unique_char + "/" + escapeForFileName(old_name);
        auto new_name_path = zookeeper_path + "/" + type_info.unique_char + "/" + escapeForFileName(new_name);
        ops.emplace_back(zkutil::makeRemoveRequest(old_name_path, -1));
        ops.emplace_back(zkutil::makeCreateRequest(new_name_path, entity_uuid, zkutil::CreateMode::Persistent));
    }

    Coordination::Responses responses;
    const Coordination::Error res = zookeeper->tryMulti(ops, responses);
    if (res == Coordination::Error::ZNODEEXISTS)
    {
        throwNameCollisionCannotRename(new_entity->getType(), old_name, new_name);
    }
    else if (res == Coordination::Error::ZNONODE)
    {
        throwNotFound(id);
    }
    else
    {
        /// If this fails, then we'll just retry from the start.
        zkutil::KeeperMultiException::check(res, ops, responses);

        /// Everything's fine, the entity has been updated.
        return true;
    }
}


void ReplicatedAccessStorage::runWatchingThread()
{
    LOG_DEBUG(getLogger(), "Started watching thread");
    setThreadName("ReplACLWatch");

    while (watching)
    {
        bool refreshed = false;
        try
        {
            initZooKeeperIfNeeded();
            refreshed = refresh();
        }
        catch (...)
        {
            tryLogCurrentException(getLogger(), "Will try to restart watching thread after error");
            resetAfterError();
            sleepForSeconds(5);
            continue;
        }

        if (refreshed)
        {
            try
            {
                changes_notifier.sendNotifications();
            }
            catch (...)
            {
                tryLogCurrentException(getLogger(), "Error while sending notifications");
            }
        }
    }
}

void ReplicatedAccessStorage::resetAfterError()
{
    /// Make watching thread reinitialize ZooKeeper and reread everything.
    std::lock_guard lock{cached_zookeeper_mutex};
    cached_zookeeper = nullptr;
}

void ReplicatedAccessStorage::initZooKeeperWithRetries(size_t max_retries)
{
    for (size_t attempt = 0; attempt < max_retries; ++attempt)
    {
        try
        {
            initZooKeeperIfNeeded();
            break; /// If we're here the initialization has been successful.
        }
        catch (const Exception & e)
        {
            bool need_another_attempt = false;

            if (const auto * coordination_exception = dynamic_cast<const Coordination::Exception *>(&e);
                coordination_exception && Coordination::isHardwareError(coordination_exception->code))
            {
                /// In case of a network error we'll try to initialize again.
                LOG_ERROR(getLogger(), "Initialization failed. Error: {}", e.message());
                need_another_attempt = (attempt + 1 < max_retries);
            }

            if (!need_another_attempt)
                throw;
        }
    }
}

void ReplicatedAccessStorage::initZooKeeperIfNeeded()
{
    getZooKeeper();
}

zkutil::ZooKeeperPtr ReplicatedAccessStorage::getZooKeeper()
{
    std::lock_guard lock{cached_zookeeper_mutex};
    return getZooKeeperNoLock();
}

zkutil::ZooKeeperPtr ReplicatedAccessStorage::getZooKeeperNoLock()
{
    if (!cached_zookeeper || cached_zookeeper->expired())
    {
        auto zookeeper = get_zookeeper();
        if (!zookeeper)
            throw Exception(ErrorCodes::NO_ZOOKEEPER, "Can't have Replicated access without ZooKeeper");

        /// It's possible that we connected to different [Zoo]Keeper instance
        /// so we may read a bit stale state.
        zookeeper->sync(zookeeper_path);

        createRootNodes(zookeeper);
        refreshEntities(zookeeper, /* all= */ true);
        cached_zookeeper = zookeeper;
    }
    return cached_zookeeper;
}

void ReplicatedAccessStorage::reload(ReloadMode reload_mode)
{
    if (reload_mode != ReloadMode::ALL)
        return;

    /// Reinitialize ZooKeeper and reread everything.
    std::lock_guard lock{cached_zookeeper_mutex};
    cached_zookeeper = nullptr;
    getZooKeeperNoLock();
}

void ReplicatedAccessStorage::createRootNodes(const zkutil::ZooKeeperPtr & zookeeper)
{
    zookeeper->createAncestors(zookeeper_path);
    zookeeper->createIfNotExists(zookeeper_path, "");
    zookeeper->createIfNotExists(zookeeper_path + "/uuid", "");
    for (const auto type : collections::range(AccessEntityType::MAX))
    {
        /// Create a znode for each type of AccessEntity
        const auto type_info = AccessEntityTypeInfo::get(type);
        zookeeper->createIfNotExists(zookeeper_path + "/" + type_info.unique_char, "");
    }
}

bool ReplicatedAccessStorage::refresh()
{
    UUID id;
    if (!watched_queue->tryPop(id, /* timeout_ms: */ 10000))
        return false;

    auto zookeeper = getZooKeeper();

    if (id == UUIDHelpers::Nil)
        refreshEntities(zookeeper, /* all= */ false);
    else
        refreshEntity(zookeeper, id);

    return true;
}


void ReplicatedAccessStorage::refreshEntities(const zkutil::ZooKeeperPtr & zookeeper, bool all)
{
    LOG_DEBUG(getLogger(), "Refreshing entities list");

    if (all)
    {
        /// It doesn't make sense to keep the queue because we will reread everything in this function.
        watched_queue->clear();
    }

    const String zookeeper_uuids_path = zookeeper_path + "/uuid";
    auto watch_entities_list = [my_watched_queue = watched_queue](const Coordination::WatchResponse &)
    {
        [[maybe_unused]] bool push_result = my_watched_queue->push(UUIDHelpers::Nil);
    };
    Coordination::Stat stat;
    const auto entity_uuid_strs = zookeeper->getChildrenWatch(zookeeper_uuids_path, &stat, watch_entities_list);

    std::vector<UUID> entity_uuids;
    entity_uuids.reserve(entity_uuid_strs.size());
    for (const String & entity_uuid_str : entity_uuid_strs)
        entity_uuids.emplace_back(parseUUID(entity_uuid_str));

    std::lock_guard lock{mutex};

    if (all)
    {
        /// all=true means we read & parse all access entities from ZooKeeper.
        std::vector<std::pair<UUID, AccessEntityPtr>> entities;
        for (const auto & uuid : entity_uuids)
        {
            if (auto entity = tryReadEntityFromZooKeeper(zookeeper, uuid))
                entities.emplace_back(uuid, entity);
        }
        memory_storage.setAll(entities);
    }
    else
    {
        /// all=false means we read & parse only new access entities from ZooKeeper.
        memory_storage.removeAllExcept(entity_uuids);
        for (const auto & uuid : entity_uuids)
        {
            if (!memory_storage.exists(uuid))
                refreshEntityNoLock(zookeeper, uuid);
        }
    }

    LOG_DEBUG(getLogger(), "Refreshing entities list finished");
}


void ReplicatedAccessStorage::refreshEntity(const zkutil::ZooKeeperPtr & zookeeper, const UUID & id)
{
    LOG_DEBUG(getLogger(), "Refreshing entity {}", toString(id));

    auto entity = tryReadEntityFromZooKeeper(zookeeper, id);

    std::lock_guard lock{mutex};

    if (entity)
        setEntityNoLock(id, entity);
    else
        removeEntityNoLock(id);
}

void ReplicatedAccessStorage::refreshEntityNoLock(const zkutil::ZooKeeperPtr & zookeeper, const UUID & id)
{
    LOG_DEBUG(getLogger(), "Refreshing entity {}", toString(id));

    auto entity = tryReadEntityFromZooKeeper(zookeeper, id);
    if (entity)
        setEntityNoLock(id, entity);
    else
        removeEntityNoLock(id);
}

AccessEntityPtr ReplicatedAccessStorage::tryReadEntityFromZooKeeper(const zkutil::ZooKeeperPtr & zookeeper, const UUID & id) const
{
    const auto watch_entity = [my_watched_queue = watched_queue, id](const Coordination::WatchResponse & response)
    {
        if (response.type == Coordination::Event::CHANGED)
            [[maybe_unused]] bool push_result = my_watched_queue->push(id);
    };

    Coordination::Stat entity_stat;
    const String entity_path = zookeeper_path + "/uuid/" + toString(id);
    String entity_definition;
    bool exists = zookeeper->tryGetWatch(entity_path, entity_definition, &entity_stat, watch_entity);
    if (!exists)
        return nullptr;

    try
    {
        return deserializeAccessEntity(entity_definition, entity_path);
    }
    catch (...)
    {
        tryLogCurrentException(getLogger(), "Error while reading the definition of " + toString(id));
        return nullptr;
    }
}

void ReplicatedAccessStorage::setEntityNoLock(const UUID & id, const AccessEntityPtr & entity)
{
    LOG_DEBUG(getLogger(), "Setting id {} to entity named {}", toString(id), entity->getName());
    memory_storage.insert(id, entity, /* replace_if_exists= */ true, /* throw_if_exists= */ false);
}


void ReplicatedAccessStorage::removeEntityNoLock(const UUID & id)
{
    LOG_DEBUG(getLogger(), "Removing entity with id {}", toString(id));
    memory_storage.remove(id, /* throw_if_not_exists= */ false); // NOLINT
}


std::optional<UUID> ReplicatedAccessStorage::findImpl(AccessEntityType type, const String & name) const
{
    std::lock_guard lock{mutex};
    return memory_storage.find(type, name);
}


std::vector<UUID> ReplicatedAccessStorage::findAllImpl(AccessEntityType type) const
{
    std::lock_guard lock{mutex};
    return memory_storage.findAll(type);
}


bool ReplicatedAccessStorage::exists(const UUID & id) const
{
    std::lock_guard lock{mutex};
    return memory_storage.exists(id);
}


AccessEntityPtr ReplicatedAccessStorage::readImpl(const UUID & id, bool throw_if_not_exists) const
{
    std::lock_guard lock{mutex};
    return memory_storage.read(id, throw_if_not_exists);
}

}
