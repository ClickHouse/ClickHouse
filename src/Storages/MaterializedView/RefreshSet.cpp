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
    parent_set->removeDependenciesLocked(id, dependencies);
    auto it = parent_set->tasks.find(id);
    auto task = it->second;
    parent_set->tasks.erase(it);
    id = new_id;
    parent_set->tasks.emplace(id, task);
    parent_set->addDependenciesLocked(id, dependencies);
}

void RefreshSet::Handle::changeDependencies(std::vector<StorageID> deps)
{
    std::lock_guard lock(parent_set->mutex);
    parent_set->removeDependenciesLocked(id, dependencies);
    dependencies = std::move(deps);
    parent_set->addDependenciesLocked(id, dependencies);
}

void RefreshSet::Handle::reset()
{
    if (!parent_set)
        return;

    {
        std::lock_guard lock(parent_set->mutex);
        parent_set->removeDependenciesLocked(id, dependencies);
        parent_set->tasks.erase(id);
    }

    parent_set = nullptr;
    metric_increment.reset();
}

RefreshSet::RefreshSet() = default;

RefreshSet::Handle RefreshSet::emplace(StorageID id, std::vector<StorageID> dependencies, RefreshTaskHolder task)
{
    std::lock_guard guard(mutex);
    auto [it, is_inserted] = tasks.emplace(id, task);
    if (!is_inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Refresh set entry already exists for table {}", id.getFullTableName());
    addDependenciesLocked(id, dependencies);

    return Handle(this, id, dependencies);
}

void RefreshSet::addDependenciesLocked(const StorageID & id, const std::vector<StorageID> & dependencies)
{
    for (const StorageID & dep : dependencies)
        dependents[dep].insert(id);
}

void RefreshSet::removeDependenciesLocked(const StorageID & id, const std::vector<StorageID> & dependencies)
{
    for (const StorageID & dep : dependencies)
    {
        auto & set = dependents[dep];
        set.erase(id);
        if (set.empty())
            dependents.erase(dep);
    }
}

RefreshTaskHolder RefreshSet::getTask(const StorageID & id) const
{
    std::lock_guard lock(mutex);
    if (auto task = tasks.find(id); task != tasks.end())
        return task->second;
    return nullptr;
}

RefreshSet::InfoContainer RefreshSet::getInfo() const
{
    std::unique_lock lock(mutex);
    auto tasks_copy = tasks;
    lock.unlock();

    InfoContainer res;
    for (auto [id, task] : tasks_copy)
        res.push_back(task->getInfo());
    return res;
}

std::vector<RefreshTaskHolder> RefreshSet::getDependents(const StorageID & id) const
{
    std::lock_guard lock(mutex);
    std::vector<RefreshTaskHolder> res;
    auto it = dependents.find(id);
    if (it == dependents.end())
        return {};
    for (auto & dep_id : it->second)
        if (auto task = tasks.find(dep_id); task != tasks.end())
            res.push_back(task->second);
    return res;
}

RefreshSet::Handle::Handle(RefreshSet * parent_set_, StorageID id_, std::vector<StorageID> dependencies_)
    : parent_set(parent_set_), id(std::move(id_)), dependencies(std::move(dependencies_))
    , metric_increment(CurrentMetrics::Increment(CurrentMetrics::RefreshableViews)) {}

}
