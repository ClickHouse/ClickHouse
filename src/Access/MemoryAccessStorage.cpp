#include <Access/MemoryAccessStorage.h>
#include <ext/scope_guard.h>
#include <unordered_set>


namespace DB
{
MemoryAccessStorage::MemoryAccessStorage(const String & storage_name_)
    : IAccessStorage(storage_name_), shared_ptr_to_this{std::make_shared<const MemoryAccessStorage *>(this)}
{
}


MemoryAccessStorage::~MemoryAccessStorage() {}


std::optional<UUID> MemoryAccessStorage::findImpl(std::type_index type, const String & name) const
{
    std::lock_guard lock{mutex};
    auto it = names.find({name, type});
    if (it == names.end())
        return {};

    Entry & entry = *(it->second);
    return entry.id;
}


std::vector<UUID> MemoryAccessStorage::findAllImpl(std::type_index type) const
{
    std::lock_guard lock{mutex};
    std::vector<UUID> result;
    result.reserve(entries.size());
    for (const auto & [id, entry] : entries)
        if (entry.entity->isTypeOf(type))
            result.emplace_back(id);
    return result;
}


bool MemoryAccessStorage::existsImpl(const UUID & id) const
{
    std::lock_guard lock{mutex};
    return entries.count(id);
}


AccessEntityPtr MemoryAccessStorage::readImpl(const UUID & id) const
{
    std::lock_guard lock{mutex};
    auto it = entries.find(id);
    if (it == entries.end())
        throwNotFound(id);
    const Entry & entry = it->second;
    return entry.entity;
}


String MemoryAccessStorage::readNameImpl(const UUID & id) const
{
    return readImpl(id)->getFullName();
}


UUID MemoryAccessStorage::insertImpl(const AccessEntityPtr & new_entity, bool replace_if_exists)
{
    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });

    UUID id = generateRandomID();
    std::lock_guard lock{mutex};
    insertNoLock(generateRandomID(), new_entity, replace_if_exists, notifications);
    return id;
}


void MemoryAccessStorage::insertNoLock(const UUID & id, const AccessEntityPtr & new_entity, bool replace_if_exists, Notifications & notifications)
{
    const String & name = new_entity->getFullName();
    std::type_index type = new_entity->getType();

    /// Check that we can insert.
    auto it = entries.find(id);
    if (it != entries.end())
    {
        const auto & existing_entry = it->second;
        throwIDCollisionCannotInsert(id, type, name, existing_entry.entity->getType(), existing_entry.entity->getFullName());
    }

    auto it2 = names.find({name, type});
    if (it2 != names.end())
    {
        const auto & existing_entry = *(it2->second);
        if (replace_if_exists)
            removeNoLock(existing_entry.id, notifications);
        else
            throwNameCollisionCannotInsert(type, name);
    }

    /// Do insertion.
    auto & entry = entries[id];
    entry.id = id;
    entry.entity = new_entity;
    names[std::pair{name, type}] = &entry;
    prepareNotifications(entry, false, notifications);
}


void MemoryAccessStorage::removeImpl(const UUID & id)
{
    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });

    std::lock_guard lock{mutex};
    removeNoLock(id, notifications);
}


void MemoryAccessStorage::removeNoLock(const UUID & id, Notifications & notifications)
{
    auto it = entries.find(id);
    if (it == entries.end())
        throwNotFound(id);

    Entry & entry = it->second;
    const String & name = entry.entity->getFullName();
    std::type_index type = entry.entity->getType();

    prepareNotifications(entry, true, notifications);

    /// Do removing.
    names.erase({name, type});
    entries.erase(it);
}


void MemoryAccessStorage::updateImpl(const UUID & id, const UpdateFunc & update_func)
{
    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });

    std::lock_guard lock{mutex};
    updateNoLock(id, update_func, notifications);
}


void MemoryAccessStorage::updateNoLock(const UUID & id, const UpdateFunc & update_func, Notifications & notifications)
{
    auto it = entries.find(id);
    if (it == entries.end())
        throwNotFound(id);

    Entry & entry = it->second;
    auto old_entity = entry.entity;
    auto new_entity = update_func(old_entity);

    if (*new_entity == *old_entity)
        return;

    entry.entity = new_entity;

    if (new_entity->getFullName() != old_entity->getFullName())
    {
        auto it2 = names.find({new_entity->getFullName(), new_entity->getType()});
        if (it2 != names.end())
            throwNameCollisionCannotRename(old_entity->getType(), old_entity->getFullName(), new_entity->getFullName());

        names.erase({old_entity->getFullName(), old_entity->getType()});
        names[std::pair{new_entity->getFullName(), new_entity->getType()}] = &entry;
    }

    prepareNotifications(entry, false, notifications);
}


void MemoryAccessStorage::setAll(const std::vector<AccessEntityPtr> & all_entities)
{
    std::vector<std::pair<UUID, AccessEntityPtr>> entities_with_ids;
    entities_with_ids.reserve(all_entities.size());
    for (const auto & entity : all_entities)
        entities_with_ids.emplace_back(generateRandomID(), entity);
    setAll(entities_with_ids);
}


void MemoryAccessStorage::setAll(const std::vector<std::pair<UUID, AccessEntityPtr>> & all_entities)
{
    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });

    std::lock_guard lock{mutex};
    setAllNoLock(all_entities, notifications);
}


void MemoryAccessStorage::setAllNoLock(const std::vector<std::pair<UUID, AccessEntityPtr>> & all_entities, Notifications & notifications)
{
    /// Get list of the currently used IDs. Later we will remove those of them which are not used anymore.
    std::unordered_set<UUID> not_used_ids;
    for (const auto & id_and_entry : entries)
        not_used_ids.emplace(id_and_entry.first);

    /// Remove conflicting entities.
    for (const auto & [id, entity] : all_entities)
    {
        auto it = entries.find(id);
        if (it != entries.end())
        {
            not_used_ids.erase(id); /// ID is used.
            Entry & entry = it->second;
            if (entry.entity->getType() != entity->getType())
            {
                removeNoLock(id, notifications);
                continue;
            }
        }
        auto it2 = names.find({entity->getFullName(), entity->getType()});
        if (it2 != names.end())
        {
            Entry & entry = *(it2->second);
            if (entry.id != id)
                removeNoLock(id, notifications);
        }
    }

    /// Remove entities which are not used anymore.
    for (const auto & id : not_used_ids)
        removeNoLock(id, notifications);

    /// Insert or update entities.
    for (const auto & [id, entity] : all_entities)
    {
        auto it = entries.find(id);
        if (it != entries.end())
        {
            if (*(it->second.entity) != *entity)
            {
                const AccessEntityPtr & changed_entity = entity;
                updateNoLock(id, [&changed_entity](const AccessEntityPtr &) { return changed_entity; }, notifications);
            }
        }
        else
            insertNoLock(id, entity, false, notifications);
    }
}


void MemoryAccessStorage::prepareNotifications(const Entry & entry, bool remove, Notifications & notifications) const
{
    for (const auto & handler : entry.handlers_by_id)
        notifications.push_back({handler, entry.id, remove ? nullptr : entry.entity});

    auto range = handlers_by_type.equal_range(entry.entity->getType());
    for (auto it = range.first; it != range.second; ++it)
        notifications.push_back({it->second, entry.id, remove ? nullptr : entry.entity});
}


IAccessStorage::SubscriptionPtr MemoryAccessStorage::subscribeForChangesImpl(std::type_index type, const OnChangedHandler & handler) const
{
    class SubscriptionImpl : public Subscription
    {
    public:
        SubscriptionImpl(
            const MemoryAccessStorage & storage_,
            std::type_index type_,
            const OnChangedHandler & handler_)
            : storage_weak(storage_.shared_ptr_to_this)
        {
            std::lock_guard lock{storage_.mutex};
            handler_it = storage_.handlers_by_type.emplace(type_, handler_);
        }

        ~SubscriptionImpl() override
        {
            auto storage = storage_weak.lock();
            if (storage)
            {
                std::lock_guard lock{(*storage)->mutex};
                (*storage)->handlers_by_type.erase(handler_it);
            }
        }

    private:
        std::weak_ptr<const MemoryAccessStorage *> storage_weak;
        std::unordered_multimap<std::type_index, OnChangedHandler>::iterator handler_it;
    };

    return std::make_unique<SubscriptionImpl>(*this, type, handler);
}


IAccessStorage::SubscriptionPtr MemoryAccessStorage::subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const
{
    class SubscriptionImpl : public Subscription
    {
    public:
        SubscriptionImpl(
            const MemoryAccessStorage & storage_,
            const UUID & id_,
            const OnChangedHandler & handler_)
            : storage_weak(storage_.shared_ptr_to_this),
              id(id_)
        {
            std::lock_guard lock{storage_.mutex};
            auto it = storage_.entries.find(id);
            if (it == storage_.entries.end())
            {
                storage_weak.reset();
                return;
            }
            const Entry & entry = it->second;
            handler_it = entry.handlers_by_id.insert(entry.handlers_by_id.end(), handler_);
        }

        ~SubscriptionImpl() override
        {
            auto storage = storage_weak.lock();
            if (storage)
            {
                std::lock_guard lock{(*storage)->mutex};
                auto it = (*storage)->entries.find(id);
                if (it != (*storage)->entries.end())
                {
                    const Entry & entry = it->second;
                    entry.handlers_by_id.erase(handler_it);
                }
            }
        }

    private:
        std::weak_ptr<const MemoryAccessStorage *> storage_weak;
        UUID id;
        std::list<OnChangedHandler>::iterator handler_it;
    };

    return std::make_unique<SubscriptionImpl>(*this, id, handler);
}


bool MemoryAccessStorage::hasSubscriptionImpl(const UUID & id) const
{
    auto it = entries.find(id);
    if (it != entries.end())
    {
        const Entry & entry = it->second;
        return !entry.handlers_by_id.empty();
    }
    return false;
}


bool MemoryAccessStorage::hasSubscriptionImpl(std::type_index type) const
{
    auto range = handlers_by_type.equal_range(type);
    return range.first != range.second;
}
}
