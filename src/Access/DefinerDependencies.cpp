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
    std::lock_guard lock(mutex);

    /// Remove any existing mapping for this object to a different definer
    auto existing_definer_it = object_to_definer.find(object_id);
    if (existing_definer_it != object_to_definer.end() && existing_definer_it->second != definer)
    {
        auto & old_objects = definer_to_objects[existing_definer_it->second];
        old_objects.erase(object_id);
        if (old_objects.empty())
            definer_to_objects.erase(existing_definer_it->second);
    }

    definer_to_objects[definer].insert(object_id);
    object_to_definer[object_id] = definer;
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
