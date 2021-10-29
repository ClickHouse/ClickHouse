#include <Access/AccessEntityIO.h>
#include <Access/MemoryAccessStorage.h>
#include <Access/ReplicatedAccessStorage.h>
#include <IO/ReadHelpers.h>
#include <boost/container/flat_set.hpp>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/escapeForFileName.h>
#include <base/range.h>
#include <base/sleep.h>


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
    zkutil::GetZooKeeper get_zookeeper_)
    : IAccessStorage(storage_name_)
    , zookeeper_path(zookeeper_path_)
    , get_zookeeper(get_zookeeper_)
    , refresh_queue(std::numeric_limits<size_t>::max())
{
    if (zookeeper_path.empty())
        throw Exception("ZooKeeper path must be non-empty", ErrorCodes::BAD_ARGUMENTS);

    if (zookeeper_path.back() == '/')
        zookeeper_path.resize(zookeeper_path.size() - 1);

    /// If zookeeper chroot prefix is used, path should start with '/', because chroot concatenates without it.
    if (zookeeper_path.front() != '/')
        zookeeper_path = "/" + zookeeper_path;
}

ReplicatedAccessStorage::~ReplicatedAccessStorage()
{
    ReplicatedAccessStorage::shutdown();
}


void ReplicatedAccessStorage::startup()
{
    initializeZookeeper();
    worker_thread = ThreadFromGlobalPool(&ReplicatedAccessStorage::runWorkerThread, this);
}

void ReplicatedAccessStorage::shutdown()
{
    bool prev_stop_flag = stop_flag.exchange(true);
    if (!prev_stop_flag)
    {
        refresh_queue.finish();

        if (worker_thread.joinable())
            worker_thread.join();
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

UUID ReplicatedAccessStorage::insertImpl(const AccessEntityPtr & new_entity, bool replace_if_exists)
{
    const UUID id = generateRandomID();
    const EntityTypeInfo type_info = EntityTypeInfo::get(new_entity->getType());
    const String & name = new_entity->getName();
    LOG_DEBUG(getLogger(), "Inserting entity of type {} named {} with id {}", type_info.name, name, toString(id));

    auto zookeeper = get_zookeeper();
    retryOnZooKeeperUserError(10, [&]{ insertZooKeeper(zookeeper, id, new_entity, replace_if_exists); });

    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });
    std::lock_guard lock{mutex};
    refreshEntityNoLock(zookeeper, id, notifications);
    return id;
}


void ReplicatedAccessStorage::insertZooKeeper(
    const zkutil::ZooKeeperPtr & zookeeper, const UUID & id, const AccessEntityPtr & new_entity, bool replace_if_exists)
{
    const String & name = new_entity->getName();
    const EntityType type = new_entity->getType();
    const EntityTypeInfo type_info = EntityTypeInfo::get(type);

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
        if (responses[0]->error == Coordination::Error::ZNODEEXISTS)
        {
            /// The UUID already exists, simply fail.

            /// To fail with a nice error message, we need info about what already exists.
            /// This itself could fail if the conflicting uuid disappears in the meantime.
            /// If that happens, then we'll just retry from the start.
            String existing_entity_definition = zookeeper->get(entity_path);

            AccessEntityPtr existing_entity = deserializeAccessEntity(existing_entity_definition, entity_path);
            EntityType existing_type = existing_entity->getType();
            String existing_name = existing_entity->getName();
            throwIDCollisionCannotInsert(id, type, name, existing_type, existing_name);
        }
        else if (replace_if_exists)
        {
            /// The name already exists for this type.
            /// If asked to, we need to replace the existing entity.

            /// First get the uuid of the existing entity
            /// This itself could fail if the conflicting name disappears in the meantime.
            /// If that happens, then we'll just retry from the start.
            Coordination::Stat name_stat;
            String existing_entity_uuid = zookeeper->get(name_path, &name_stat);

            const String existing_entity_path = zookeeper_path + "/uuid/" + existing_entity_uuid;
            Coordination::Requests replace_ops;
            replace_ops.emplace_back(zkutil::makeRemoveRequest(existing_entity_path, -1));
            replace_ops.emplace_back(zkutil::makeCreateRequest(entity_path, new_entity_definition, zkutil::CreateMode::Persistent));
            replace_ops.emplace_back(zkutil::makeSetRequest(name_path, entity_uuid, name_stat.version));

            /// If this fails, then we'll just retry from the start.
            zookeeper->multi(replace_ops);
        }
        else
        {
            throwNameCollisionCannotInsert(type, name);
        }
    }
    else
    {
        zkutil::KeeperMultiException::check(res, ops, responses);
    }
}

void ReplicatedAccessStorage::removeImpl(const UUID & id)
{
    LOG_DEBUG(getLogger(), "Removing entity {}", toString(id));

    auto zookeeper = get_zookeeper();
    retryOnZooKeeperUserError(10, [&] { removeZooKeeper(zookeeper, id); });

    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });
    std::lock_guard lock{mutex};
    removeEntityNoLock(id, notifications);
}


void ReplicatedAccessStorage::removeZooKeeper(const zkutil::ZooKeeperPtr & zookeeper, const UUID & id)
{
    const String entity_uuid = toString(id);
    const String entity_path = zookeeper_path + "/uuid/" + entity_uuid;

    String entity_definition;
    Coordination::Stat entity_stat;
    const bool uuid_exists = zookeeper->tryGet(entity_path, entity_definition, &entity_stat);
    if (!uuid_exists)
        throwNotFound(id);

    const AccessEntityPtr entity = deserializeAccessEntity(entity_definition, entity_path);
    const EntityTypeInfo type_info = EntityTypeInfo::get(entity->getType());
    const String & name = entity->getName();

    const String entity_name_path = zookeeper_path + "/" + type_info.unique_char + "/" + escapeForFileName(name);

    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeRemoveRequest(entity_path, entity_stat.version));
    ops.emplace_back(zkutil::makeRemoveRequest(entity_name_path, -1));
    /// If this fails, then we'll just retry from the start.
    zookeeper->multi(ops);
}


void ReplicatedAccessStorage::updateImpl(const UUID & id, const UpdateFunc & update_func)
{
    LOG_DEBUG(getLogger(), "Updating entity {}", toString(id));

    auto zookeeper = get_zookeeper();
    retryOnZooKeeperUserError(10, [&] { updateZooKeeper(zookeeper, id, update_func); });

    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });
    std::lock_guard lock{mutex};
    refreshEntityNoLock(zookeeper, id, notifications);
}


void ReplicatedAccessStorage::updateZooKeeper(const zkutil::ZooKeeperPtr & zookeeper, const UUID & id, const UpdateFunc & update_func)
{
    const String entity_uuid = toString(id);
    const String entity_path = zookeeper_path + "/uuid/" + entity_uuid;

    String old_entity_definition;
    Coordination::Stat stat;
    const bool uuid_exists = zookeeper->tryGet(entity_path, old_entity_definition, &stat);
    if (!uuid_exists)
        throwNotFound(id);

    const AccessEntityPtr old_entity = deserializeAccessEntity(old_entity_definition, entity_path);
    const AccessEntityPtr new_entity = update_func(old_entity);

    if (!new_entity->isTypeOf(old_entity->getType()))
        throwBadCast(id, new_entity->getType(), new_entity->getName(), old_entity->getType());

    const EntityTypeInfo type_info = EntityTypeInfo::get(new_entity->getType());

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
        zkutil::KeeperMultiException::check(res, ops, responses);
    }
}


void ReplicatedAccessStorage::runWorkerThread()
{
    LOG_DEBUG(getLogger(), "Started worker thread");
    while (!stop_flag)
    {
        try
        {
            if (!initialized)
                initializeZookeeper();
            refresh();
        }
        catch (...)
        {
            tryLogCurrentException(getLogger(), "Unexpected error, will try to restart worker thread:");
            resetAfterError();
            sleepForSeconds(5);
        }
    }
}

void ReplicatedAccessStorage::resetAfterError()
{
    initialized = false;

    UUID id;
    while (refresh_queue.tryPop(id)) {}

    std::lock_guard lock{mutex};
    for (const auto type : collections::range(EntityType::MAX))
        entries_by_name_and_type[static_cast<size_t>(type)].clear();
    entries_by_id.clear();
}

void ReplicatedAccessStorage::initializeZookeeper()
{
    assert(!initialized);
    auto zookeeper = get_zookeeper();

    if (!zookeeper)
        throw Exception("Can't have Replicated access without ZooKeeper", ErrorCodes::NO_ZOOKEEPER);

    createRootNodes(zookeeper);

    refreshEntities(zookeeper);

    initialized = true;
}

void ReplicatedAccessStorage::createRootNodes(const zkutil::ZooKeeperPtr & zookeeper)
{
    zookeeper->createAncestors(zookeeper_path);
    zookeeper->createIfNotExists(zookeeper_path, "");
    zookeeper->createIfNotExists(zookeeper_path + "/uuid", "");
    for (const auto type : collections::range(EntityType::MAX))
    {
        /// Create a znode for each type of AccessEntity
        const auto type_info = EntityTypeInfo::get(type);
        zookeeper->createIfNotExists(zookeeper_path + "/" + type_info.unique_char, "");
    }
}

void ReplicatedAccessStorage::refresh()
{
    UUID id;
    if (refresh_queue.tryPop(id, /* timeout_ms: */ 10000))
    {
        if (stop_flag)
            return;

        auto zookeeper = get_zookeeper();

        if (id == UUIDHelpers::Nil)
            refreshEntities(zookeeper);
        else
            refreshEntity(zookeeper, id);
    }
}


void ReplicatedAccessStorage::refreshEntities(const zkutil::ZooKeeperPtr & zookeeper)
{
    LOG_DEBUG(getLogger(), "Refreshing entities list");

    const String zookeeper_uuids_path = zookeeper_path + "/uuid";
    auto watch_entities_list = [this](const Coordination::WatchResponse &)
    {
        [[maybe_unused]] bool push_result = refresh_queue.push(UUIDHelpers::Nil);
    };
    Coordination::Stat stat;
    const auto entity_uuid_strs = zookeeper->getChildrenWatch(zookeeper_uuids_path, &stat, watch_entities_list);

    std::unordered_set<UUID> entity_uuids;
    entity_uuids.reserve(entity_uuid_strs.size());
    for (const String & entity_uuid_str : entity_uuid_strs)
        entity_uuids.insert(parseUUID(entity_uuid_str));

    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });
    std::lock_guard lock{mutex};

    std::vector<UUID> entities_to_remove;
    /// Locally remove entities that were removed from ZooKeeper
    for (const auto & pair : entries_by_id)
    {
        const UUID & entity_uuid = pair.first;
        if (!entity_uuids.contains(entity_uuid))
            entities_to_remove.push_back(entity_uuid);
    }
    for (const auto & entity_uuid : entities_to_remove)
        removeEntityNoLock(entity_uuid, notifications);

    /// Locally add entities that were added to ZooKeeper
    for (const auto & entity_uuid : entity_uuids)
    {
        const auto it = entries_by_id.find(entity_uuid);
        if (it == entries_by_id.end())
            refreshEntityNoLock(zookeeper, entity_uuid, notifications);
    }

    LOG_DEBUG(getLogger(), "Refreshing entities list finished");
}

void ReplicatedAccessStorage::refreshEntity(const zkutil::ZooKeeperPtr & zookeeper, const UUID & id)
{
    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });
    std::lock_guard lock{mutex};

    refreshEntityNoLock(zookeeper, id, notifications);
}

void ReplicatedAccessStorage::refreshEntityNoLock(const zkutil::ZooKeeperPtr & zookeeper, const UUID & id, Notifications & notifications)
{
    LOG_DEBUG(getLogger(), "Refreshing entity {}", toString(id));

    const auto watch_entity = [this, id](const Coordination::WatchResponse & response)
    {
        if (response.type == Coordination::Event::CHANGED)
            [[maybe_unused]] bool push_result = refresh_queue.push(id);
    };
    Coordination::Stat entity_stat;
    const String entity_path = zookeeper_path + "/uuid/" + toString(id);
    String entity_definition;
    const bool exists = zookeeper->tryGetWatch(entity_path, entity_definition, &entity_stat, watch_entity);
    if (exists)
    {
        const AccessEntityPtr entity = deserializeAccessEntity(entity_definition, entity_path);
        setEntityNoLock(id, entity, notifications);
    }
    else
    {
        removeEntityNoLock(id, notifications);
    }
}


void ReplicatedAccessStorage::setEntityNoLock(const UUID & id, const AccessEntityPtr & entity, Notifications & notifications)
{
    LOG_DEBUG(getLogger(), "Setting id {} to entity named {}", toString(id), entity->getName());
    const EntityType type = entity->getType();
    const String & name = entity->getName();

    /// If the type+name already exists and is a different entity, remove old entity
    auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
    if (auto it = entries_by_name.find(name); it != entries_by_name.end() && it->second->id != id)
    {
        removeEntityNoLock(it->second->id, notifications);
    }

    /// If the entity already exists under a different type+name, remove old type+name
    if (auto it = entries_by_id.find(id); it != entries_by_id.end())
    {
        const AccessEntityPtr & existing_entity = it->second.entity;
        const EntityType existing_type = existing_entity->getType();
        const String & existing_name = existing_entity->getName();
        if (existing_type != type || existing_name != name)
        {
            auto & existing_entries_by_name = entries_by_name_and_type[static_cast<size_t>(existing_type)];
            existing_entries_by_name.erase(existing_name);
        }
    }

    auto & entry = entries_by_id[id];
    entry.id = id;
    entry.entity = entity;
    entries_by_name[name] = &entry;
    prepareNotifications(entry, false, notifications);
}


void ReplicatedAccessStorage::removeEntityNoLock(const UUID & id, Notifications & notifications)
{
    LOG_DEBUG(getLogger(), "Removing entity with id {}", toString(id));
    const auto it = entries_by_id.find(id);
    if (it == entries_by_id.end())
    {
        LOG_DEBUG(getLogger(), "Id {} not found, ignoring removal", toString(id));
        return;
    }

    const Entry & entry = it->second;
    const EntityType type = entry.entity->getType();
    const String & name = entry.entity->getName();
    prepareNotifications(entry, true, notifications);

    auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
    const auto name_it = entries_by_name.find(name);
    if (name_it == entries_by_name.end())
        LOG_WARNING(getLogger(), "Entity {} not found in names, ignoring removal of name", toString(id));
    else if (name_it->second != &(it->second))
        LOG_WARNING(getLogger(), "Name {} not pointing to entity {}, ignoring removal of name", name, toString(id));
    else
        entries_by_name.erase(name);

    entries_by_id.erase(id);
    LOG_DEBUG(getLogger(), "Removed entity with id {}", toString(id));
}


std::optional<UUID> ReplicatedAccessStorage::findImpl(EntityType type, const String & name) const
{
    std::lock_guard lock{mutex};
    const auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
    const auto it = entries_by_name.find(name);
    if (it == entries_by_name.end())
        return {};

    const Entry * entry = it->second;
    return entry->id;
}


std::vector<UUID> ReplicatedAccessStorage::findAllImpl(EntityType type) const
{
    std::lock_guard lock{mutex};
    std::vector<UUID> result;
    result.reserve(entries_by_id.size());
    for (const auto & [id, entry] : entries_by_id)
        if (entry.entity->isTypeOf(type))
            result.emplace_back(id);
    return result;
}


bool ReplicatedAccessStorage::existsImpl(const UUID & id) const
{
    std::lock_guard lock{mutex};
    return entries_by_id.count(id);
}


AccessEntityPtr ReplicatedAccessStorage::readImpl(const UUID & id) const
{
    std::lock_guard lock{mutex};
    const auto it = entries_by_id.find(id);
    if (it == entries_by_id.end())
        throwNotFound(id);
    const Entry & entry = it->second;
    return entry.entity;
}


String ReplicatedAccessStorage::readNameImpl(const UUID & id) const
{
    return readImpl(id)->getName();
}


void ReplicatedAccessStorage::prepareNotifications(const Entry & entry, bool remove, Notifications & notifications) const
{
    const AccessEntityPtr entity = remove ? nullptr : entry.entity;
    for (const auto & handler : entry.handlers_by_id)
        notifications.push_back({handler, entry.id, entity});

    for (const auto & handler : handlers_by_type[static_cast<size_t>(entry.entity->getType())])
        notifications.push_back({handler, entry.id, entity});
}


scope_guard ReplicatedAccessStorage::subscribeForChangesImpl(EntityType type, const OnChangedHandler & handler) const
{
    std::lock_guard lock{mutex};
    auto & handlers = handlers_by_type[static_cast<size_t>(type)];
    handlers.push_back(handler);
    auto handler_it = std::prev(handlers.end());

    return [this, type, handler_it]
    {
        std::lock_guard lock2{mutex};
        auto & handlers2 = handlers_by_type[static_cast<size_t>(type)];
        handlers2.erase(handler_it);
    };
}


scope_guard ReplicatedAccessStorage::subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const
{
    std::lock_guard lock{mutex};
    const auto it = entries_by_id.find(id);
    if (it == entries_by_id.end())
        return {};
    const Entry & entry = it->second;
    auto handler_it = entry.handlers_by_id.insert(entry.handlers_by_id.end(), handler);

    return [this, id, handler_it]
    {
        std::lock_guard lock2{mutex};
        auto it2 = entries_by_id.find(id);
        if (it2 != entries_by_id.end())
        {
            const Entry & entry2 = it2->second;
            entry2.handlers_by_id.erase(handler_it);
        }
    };
}


bool ReplicatedAccessStorage::hasSubscriptionImpl(const UUID & id) const
{
    std::lock_guard lock{mutex};
    const auto & it = entries_by_id.find(id);
    if (it != entries_by_id.end())
    {
        const Entry & entry = it->second;
        return !entry.handlers_by_id.empty();
    }
    return false;
}


bool ReplicatedAccessStorage::hasSubscriptionImpl(EntityType type) const
{
    std::lock_guard lock{mutex};
    const auto & handlers = handlers_by_type[static_cast<size_t>(type)];
    return !handlers.empty();
}
}
