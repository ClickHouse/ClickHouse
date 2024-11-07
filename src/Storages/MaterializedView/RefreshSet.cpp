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
    inner_table_id = std::move(other.inner_table_id);
    dependencies = std::move(other.dependencies);
    iter = std::move(other.iter);
    inner_table_iter = std::move(other.inner_table_iter);
    metric_increment = std::move(other.metric_increment);
    return *this;
}

RefreshSet::Handle::~Handle()
{
    reset();
}

void RefreshSet::Handle::rename(StorageID new_id, std::optional<StorageID> new_inner_table_id)
{
    std::lock_guard lock(parent_set->mutex);
    RefreshTaskPtr task = *iter;
    parent_set->removeDependenciesLocked(task, dependencies);
    parent_set->removeTaskLocked(id, iter);
    if (inner_table_id)
        parent_set->removeInnerTableLocked(*inner_table_id, inner_table_iter);
    id = new_id;
    inner_table_id = new_inner_table_id;
    iter = parent_set->addTaskLocked(id, task);
    if (inner_table_id)
        inner_table_iter = parent_set->addInnerTableLocked(*inner_table_id, task);
    parent_set->addDependenciesLocked(task, dependencies);
}

void RefreshSet::Handle::changeDependencies(std::vector<StorageID> deps)
{
    std::lock_guard lock(parent_set->mutex);
    RefreshTaskPtr task = *iter;
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
        if (inner_table_id)
            parent_set->removeInnerTableLocked(*inner_table_id, inner_table_iter);
    }

    parent_set = nullptr;
    metric_increment.reset();
}

RefreshSet::RefreshSet() = default;

void RefreshSet::emplace(StorageID id, std::optional<StorageID> inner_table_id, const std::vector<StorageID> & dependencies, RefreshTaskPtr task)
{
    std::lock_guard guard(mutex);
    const auto iter = addTaskLocked(id, task);
    RefreshTaskList::iterator inner_table_iter;
    if (inner_table_id)
        inner_table_iter = addInnerTableLocked(*inner_table_id, task);
    addDependenciesLocked(task, dependencies);

    task->setRefreshSetHandleUnlock(Handle(this, id, inner_table_id, iter, inner_table_iter, dependencies));
}

RefreshTaskList::iterator RefreshSet::addTaskLocked(StorageID id, RefreshTaskPtr task)
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

RefreshTaskList::iterator RefreshSet::addInnerTableLocked(StorageID inner_table_id, RefreshTaskPtr task)
{
    RefreshTaskList & list = inner_tables[inner_table_id];
    list.push_back(task);
    return std::prev(list.end());
}

void RefreshSet::removeInnerTableLocked(StorageID inner_table_id, RefreshTaskList::iterator inner_table_iter)
{
    const auto it = inner_tables.find(inner_table_id);
    it->second.erase(inner_table_iter);
    if (it->second.empty())
        inner_tables.erase(it);
}

void RefreshSet::addDependenciesLocked(RefreshTaskPtr task, const std::vector<StorageID> & dependencies)
{
    for (const StorageID & dep : dependencies)
        dependents[dep].insert(task);
}

void RefreshSet::removeDependenciesLocked(RefreshTaskPtr task, const std::vector<StorageID> & dependencies)
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

std::vector<RefreshTaskPtr> RefreshSet::getTasks() const
{
    std::unique_lock lock(mutex);
    std::vector<RefreshTaskPtr> res;
    for (const auto & [_, list] : tasks)
        for (const auto & task : list)
            res.push_back(task);
    return res;
}

RefreshTaskPtr RefreshSet::tryGetTaskForInnerTable(const StorageID & inner_table_id) const
{
    std::unique_lock lock(mutex);
    auto it = inner_tables.find(inner_table_id);
    if (it == inner_tables.end())
        return nullptr;
    return *it->second.begin();
}

void RefreshSet::notifyDependents(const StorageID & id) const
{
    std::vector<RefreshTaskPtr> res;
    {
        std::lock_guard lock(mutex);
        auto it = dependents.find(id);
        if (it == dependents.end())
            return;
        for (const auto & task : it->second)
            res.push_back(task);
    }
    for (const RefreshTaskPtr & t : res)
        t->notify();
}

void RefreshSet::setRefreshesStopped(bool stopped)
{

    TaskMap tasks_copy;
    {
        std::lock_guard lock(mutex);
        refreshes_stopped.store(stopped);
        tasks_copy = tasks;
    }
    for (const auto & kv : tasks_copy)
        for (const RefreshTaskPtr & t : kv.second)
            t->notify();
}

bool RefreshSet::refreshesStopped() const
{
    return refreshes_stopped.load();
}

RefreshSet::Handle::Handle(RefreshSet * parent_set_, StorageID id_, std::optional<StorageID> inner_table_id_, RefreshTaskList::iterator iter_, RefreshTaskList::iterator inner_table_iter_, std::vector<StorageID> dependencies_)
    : parent_set(parent_set_), id(std::move(id_)), inner_table_id(std::move(inner_table_id_)), dependencies(std::move(dependencies_))
    , iter(iter_), inner_table_iter(inner_table_iter_), metric_increment(CurrentMetrics::Increment(CurrentMetrics::RefreshableViews)) {}

}
