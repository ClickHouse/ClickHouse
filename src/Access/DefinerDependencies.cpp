#include <Access/AccessControl.h>
#include <Access/DefinerDependencies.h>
#include <Access/User.h>
#include <Interpreters/Context.h>

#include <mutex>
#include <optional>


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

std::optional<String> DefinerDependencies::tryDetachLocked(const StorageID & object_id)
{
    auto object_it = object_to_definer.find(object_id);
    if (object_it == object_to_definer.end())
        return {};

    String definer = object_it->second;
    object_to_definer.erase(object_it);

    auto definer_it = definer_to_objects.find(definer);
    chassert(definer_it != definer_to_objects.end());
    if (definer_it != definer_to_objects.end())
    {
        definer_it->second.erase(object_id);
        if (definer_it->second.empty())
            definer_to_objects.erase(definer_it);
    }

    return definer;
}

void DefinerDependencies::attachLocked(const String & definer, const StorageID & object_id)
{
    definer_to_objects[definer].insert(object_id);
    object_to_definer[object_id] = definer;
}

void DefinerDependencies::addDependency(const String & definer, const StorageID & object_id)
{
    std::lock_guard lock(mutex);
    tryDetachLocked(object_id);
    attachLocked(definer, object_id);
}

void DefinerDependencies::removeDependencies(const StorageID & object_id)
{
    std::lock_guard lock(mutex);

    auto object_it = object_to_definer.find(object_id);
    if (object_it != object_to_definer.end())
    {
        const auto & definer = object_it->second;
        auto definer_it = definer_to_objects.find(definer);
        if (definer_it != definer_to_objects.end())
        {
            definer_it->second.erase(object_id);
            if (definer_it->second.empty())
            {
                if (definer.ends_with(":definer"))
                {
                    auto & access_control = Context::getGlobalContextInstance()->getGlobalContext()->getAccessControl();
                    if (const auto uuid = access_control.find<User>(definer))
                        access_control.tryRemove(*uuid);
                }

                definer_to_objects.erase(definer_it);
            }
        }
        object_to_definer.erase(object_it);
    }
}

void DefinerDependencies::rename(const StorageID & old_object_id, const StorageID & new_object_id)
{
    std::lock_guard lock(mutex);
    if (auto definer = tryDetachLocked(old_object_id))
        attachLocked(*definer, new_object_id);
}

void DefinerDependencies::exchange(const StorageID & object_id_a, const StorageID & object_id_b)
{
    std::lock_guard lock(mutex);
    auto definer_a = tryDetachLocked(object_id_a);
    auto definer_b = tryDetachLocked(object_id_b);
    if (definer_a)
        attachLocked(*definer_a, object_id_b);
    if (definer_b)
        attachLocked(*definer_b, object_id_a);
}

std::vector<StorageID> DefinerDependencies::getObjectsForDefiner(const String & definer) const
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
    return it != definer_to_objects.end() && !it->second.empty();
}

}
