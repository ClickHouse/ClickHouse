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
    definer_to_objects[definer].insert(object_id.uuid);
    object_to_definer[object_id.uuid] = definer;
}

void DefinerDependencies::removeDependencies(const StorageID & object_id)
{
    std::lock_guard lock(mutex);

    auto object_it = object_to_definer.find(object_id.uuid);
    if (object_it != object_to_definer.end())
    {
        const auto & definer = object_it->second;
        auto definer_it = definer_to_objects.find(definer);
        if (definer_it != definer_to_objects.end())
        {
            definer_it->second.erase(object_id.uuid);
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
    return it != definer_to_objects.end() && !it->second.empty();
}

}
