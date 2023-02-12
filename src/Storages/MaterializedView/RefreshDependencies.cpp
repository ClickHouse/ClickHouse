#include <Storages/MaterializedView/RefreshDependencies.h>

#include <Storages/MaterializedView/RefreshTask.h>

namespace DB
{

RefreshDependencies::Entry::Entry(RefreshDependencies & deps, ContainerIter it)
    : dependencies{&deps}
    , entry_it{it}
{}

RefreshDependencies::Entry::Entry(Entry && other) noexcept
    : dependencies(std::exchange(other.dependencies, nullptr))
    , entry_it(std::move(other.entry_it))
{}

RefreshDependencies::Entry & RefreshDependencies::Entry::operator=(Entry && other) noexcept
{
    if (this == &other)
        return *this;
    cleanup(std::exchange(dependencies, std::exchange(other.dependencies, nullptr)));
    entry_it = std::move(other.entry_it);
    return *this;
}

RefreshDependencies::Entry::~Entry()
{
    cleanup(dependencies);
}

void RefreshDependencies::Entry::cleanup(RefreshDependencies * deps)
{
    if (deps)
        deps->erase(entry_it);
}

RefreshDependenciesEntry RefreshDependencies::add(RefreshTaskHolder dependency)
{
    std::lock_guard lock(dependencies_mutex);
    return Entry(*this, dependencies.emplace(dependencies.end(), dependency));
}

void RefreshDependencies::notifyAll(const StorageID & id)
{
    std::lock_guard lock(dependencies_mutex);
    for (auto && dep : dependencies)
    {
        if (auto task = dep.lock())
            task->notify(id);
    }
}

void RefreshDependencies::erase(ContainerIter it)
{
    std::lock_guard lock(dependencies_mutex);
    dependencies.erase(it);
}

}
