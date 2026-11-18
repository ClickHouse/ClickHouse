#include <Access/AccessControl.h>
#include <Access/ViewDefinerDependencies.h>
#include <Access/User.h>
#include <Interpreters/Context.h>

#include <mutex>


namespace DB
{

std::unique_ptr<ViewDefinerDependencies> ViewDefinerDependencies::global_instance;
std::once_flag ViewDefinerDependencies::instance_flag;

ViewDefinerDependencies & ViewDefinerDependencies::instance()
{
    std::call_once(instance_flag, []
        {
            global_instance = std::unique_ptr<ViewDefinerDependencies>(new ViewDefinerDependencies());
        });

    return *global_instance;
}

void ViewDefinerDependencies::addViewDependency(const String & definer, const StorageID & view_id)
{
    std::lock_guard lock(mutex);

    /// Remove any existing mapping for this view to a different definer
    auto existing_definer_it = view_to_definer.find(view_id);
    if (existing_definer_it != view_to_definer.end() && existing_definer_it->second != definer)
    {
        auto & old_views = definer_to_views[existing_definer_it->second];
        old_views.erase(view_id);
        if (old_views.empty())
            definer_to_views.erase(existing_definer_it->second);
    }

    definer_to_views[definer].insert(view_id);
    view_to_definer[view_id] = definer;
}

void ViewDefinerDependencies::removeViewDependencies(const StorageID & view_id)
{
    std::lock_guard lock(mutex);

    auto view_it = view_to_definer.find(view_id);
    if (view_it != view_to_definer.end())
    {
        const auto & definer = view_it->second;
        auto definer_it = definer_to_views.find(definer);
        if (definer_it != definer_to_views.end())
        {
            definer_it->second.erase(view_id);
            if (definer_it->second.empty())
            {
                if (definer.ends_with(":definer"))
                {
                    auto & access_control = Context::getGlobalContextInstance()->getGlobalContext()->getAccessControl();
                    if (const auto uuid = access_control.find<User>(definer))
                        access_control.tryRemove(*uuid);
                }

                definer_to_views.erase(definer_it);
            }
        }
        view_to_definer.erase(view_it);
    }
}

std::vector<StorageID> ViewDefinerDependencies::getViewsForDefiner(const String & definer) const
{
    std::lock_guard lock(mutex);

    auto it = definer_to_views.find(definer);
    if (it == definer_to_views.end())
        return {};

    return {it->second.begin(), it->second.end()};
}

bool ViewDefinerDependencies::hasViewDependencies(const String & definer) const
{
    std::lock_guard lock(mutex);

    auto it = definer_to_views.find(definer);
    return it != definer_to_views.end() && !it->second.empty();
}

}
