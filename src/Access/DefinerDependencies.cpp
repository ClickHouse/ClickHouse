#include <Access/AccessControl.h>
#include <Access/DefinerDependencies.h>
#include <Access/User.h>
#include <Interpreters/Context.h>

#include <mutex>


namespace DB
{

std::unique_ptr<DefinerDependencies> DefinerDependencies::global_instance;
std::once_flag DefinerDependencies::instance_flag;

DefinerDependencies & DefinerDependencies::instance()
{
    std::call_once(instance_flag, []
        {
            global_instance = std::unique_ptr<DefinerDependencies>(new DefinerDependencies());
        });

    return *global_instance;
}

void DefinerDependencies::addDependency(const String & definer, const StorageID & object_id)
{
    /// Objects without a UUID are not tracked.
    if (object_id.uuid == UUIDHelpers::Nil)
        return;

    std::lock_guard lock(mutex);

    auto object_it = object_to_definer.find(object_id.uuid);
    if (object_it != object_to_definer.end())
    {
        if (object_it->second == definer)
            return;

        unregisterObject(object_id.uuid);
    }

    definer_to_objects[definer].insert(object_id.uuid);
    object_to_definer[object_id.uuid] = definer;
}

void DefinerDependencies::removeDependencies(const StorageID & object_id)
{
    std::lock_guard lock(mutex);
    unregisterObject(object_id.uuid);
}

void DefinerDependencies::unregisterObject(const UUID & object_uuid)
{
    auto object_it = object_to_definer.find(object_uuid);
    if (object_it == object_to_definer.end())
        return;

    const String definer = object_it->second;
    object_to_definer.erase(object_it);

    auto definer_it = definer_to_objects.find(definer);
    if (definer_it != definer_to_objects.end())
    {
        definer_it->second.erase(object_uuid);
        if (definer_it->second.empty())
            definer_to_objects.erase(definer_it);
    }
    releaseDefinerIfUnusedLocked(definer);
}

void DefinerDependencies::releaseDefinerIfUnusedLocked(const String & definer)
{
    if (!definer.ends_with(":definer") || definer_to_objects.contains(definer) || definer_to_handlers.contains(definer))
        return;

    auto & access_control = Context::getGlobalContextInstance()->getGlobalContext()->getAccessControl();
    if (const auto uuid = access_control.find<User>(definer))
        access_control.tryRemove(*uuid);
}

void DefinerDependencies::setHandlerDefiners(const std::unordered_map<String, String> & handler_to_definer)
{
    std::lock_guard lock(mutex);

    auto previous = std::exchange(definer_to_handlers, {});
    for (const auto & [handler, definer] : handler_to_definer)
        definer_to_handlers[definer].insert(handler);

    for (const auto & [definer, _] : previous)
        releaseDefinerIfUnusedLocked(definer);
}

void DefinerDependencies::addHandlerDefiner(const String & handler, const String & definer)
{
    std::lock_guard lock(mutex);
    definer_to_handlers[definer].insert(handler);
}

std::vector<String> DefinerDependencies::getHandlersForDefiner(const String & definer) const
{
    std::lock_guard lock(mutex);

    auto it = definer_to_handlers.find(definer);
    if (it == definer_to_handlers.end())
        return {};

    return {it->second.begin(), it->second.end()};
}

std::vector<UUID> DefinerDependencies::getObjectsForDefiner(const String & definer) const
{
    std::lock_guard lock(mutex);

    auto it = definer_to_objects.find(definer);
    if (it == definer_to_objects.end())
        return {};

    return {it->second.begin(), it->second.end()};
}

bool DefinerDependencies::hasDependencies(const String & definer) const
{
    std::lock_guard lock(mutex);

    auto it = definer_to_objects.find(definer);
    if (it != definer_to_objects.end() && !it->second.empty())
        return true;

    auto handlers_it = definer_to_handlers.find(definer);
    return handlers_it != definer_to_handlers.end() && !handlers_it->second.empty();
}

}
