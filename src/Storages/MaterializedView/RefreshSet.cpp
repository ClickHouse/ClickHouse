#include <Storages/MaterializedView/RefreshSet.h>
#include <Storages/MaterializedView/RefreshTask.h>

namespace CurrentMetrics
{
    extern const Metric RefreshableViews;
}

namespace DB
{

RefreshSet::Handle::Handle(Handle && other) noexcept
{
    *this = std::move(other);
}

RefreshSet::Handle & RefreshSet::Handle::operator=(Handle && other) noexcept
{
    if (this == &other)
        return *this;
    reset();
    parent_set = std::exchange(other.parent_set, nullptr);
    id = std::move(other.id);
    dependencies = std::move(other.dependencies);
    iter = std::move(other.iter);
    metric_increment = std::move(other.metric_increment);
    return *this;
}

RefreshSet::Handle::~Handle()
{
    reset();
}

void RefreshSet::Handle::rename(StorageID new_id)
{
    std::lock_guard lock(parent_set->mutex);
    RefreshTaskHolder task = *iter;
    parent_set->removeDependenciesLocked(task, dependencies);
    parent_set->removeTaskLocked(id, iter);
    id = new_id;
    iter = parent_set->addTaskLocked(id, task);
    parent_set->addDependenciesLocked(task, dependencies);
}

void RefreshSet::Handle::changeDependencies(std::vector<StorageID> deps)
{
    std::lock_guard lock(parent_set->mutex);
    RefreshTaskHolder task = *iter;
    parent_set->removeDependenciesLocked(task, dependencies);
    dependencies = std::move(deps);
    parent_set->addDependenciesLocked(task, dependencies);
}

void RefreshSet::Handle::reset()
{
    if (!parent_set)
        return;

    {
        std::lock_guard lock(parent_set->mutex);
        parent_set->removeDependenciesLocked(*iter, dependencies);
        parent_set->removeTaskLocked(id, iter);
    }

    parent_set = nullptr;
    metric_increment.reset();
}

RefreshSet::RefreshSet() = default;

void RefreshSet::emplace(StorageID id, const std::vector<StorageID> & dependencies, RefreshTaskHolder task)
{
    std::lock_guard guard(mutex);
    const auto iter = addTaskLocked(id, task);
    addDependenciesLocked(task, dependencies);

    task->setRefreshSetHandleUnlock(Handle(this, id, iter, dependencies));
}

RefreshTaskList::iterator RefreshSet::addTaskLocked(StorageID id, RefreshTaskHolder task)
{
    RefreshTaskList & list = tasks[id];
    list.push_back(task);
    return std::prev(list.end());
}

void RefreshSet::removeTaskLocked(StorageID id, RefreshTaskList::iterator iter)
{
    const auto it = tasks.find(id);
    it->second.erase(iter);
    if (it->second.empty())
        tasks.erase(it);
}

void RefreshSet::addDependenciesLocked(RefreshTaskHolder task, const std::vector<StorageID> & dependencies)
{
    for (const StorageID & dep : dependencies)
        dependents[dep].insert(task);
}

void RefreshSet::removeDependenciesLocked(RefreshTaskHolder task, const std::vector<StorageID> & dependencies)
{
    for (const StorageID & dep : dependencies)
    {
        auto & set = dependents[dep];
        set.erase(task);
        if (set.empty())
            dependents.erase(dep);
    }
}

RefreshTaskList RefreshSet::findTasks(const StorageID & id) const
{
    std::lock_guard lock(mutex);
    if (auto it = tasks.find(id); it != tasks.end())
        return it->second;
    return {};
}

RefreshSet::InfoContainer RefreshSet::getInfo() const
{
    std::unique_lock lock(mutex);
    auto tasks_copy = tasks;
    lock.unlock();

    InfoContainer res;
    for (const auto & [id, list] : tasks_copy)
        for (const auto & task : list)
            res.push_back(task->getInfo());
    return res;
}

std::vector<RefreshTaskHolder> RefreshSet::getDependents(const StorageID & id) const
{
    std::lock_guard lock(mutex);
    auto it = dependents.find(id);
    if (it == dependents.end())
        return {};
    return std::vector<RefreshTaskHolder>(it->second.begin(), it->second.end());
}

RefreshSet::Handle::Handle(RefreshSet * parent_set_, StorageID id_, RefreshTaskList::iterator iter_, std::vector<StorageID> dependencies_)
    : parent_set(parent_set_), id(std::move(id_)), dependencies(std::move(dependencies_))
    , iter(iter_), metric_increment(CurrentMetrics::Increment(CurrentMetrics::RefreshableViews)) {}

}
