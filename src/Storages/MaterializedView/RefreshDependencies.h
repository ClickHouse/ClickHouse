#pragma once

#include <Storages/MaterializedView/RefreshTask_fwd.h>

#include <Interpreters/StorageID.h>

#include <list>


namespace DB
{

class RefreshTask;

/// Concurrent primitive for managing list of dependent task and notifying them
class RefreshDependencies
{
    using Container = std::list<RefreshTaskObserver>;
    using ContainerIter = typename Container::iterator;

public:
    class Entry
    {
        friend class RefreshDependencies;

    public:
        Entry(Entry &&) noexcept;
        Entry & operator=(Entry &&) noexcept;

        ~Entry();

    private:
        Entry(RefreshDependencies & deps, ContainerIter it);

        void cleanup(RefreshDependencies * deps);

        RefreshDependencies * dependencies;
        ContainerIter entry_it;
    };

    RefreshDependencies() = default;

    Entry add(RefreshTaskHolder dependency);

    void notifyAll(const StorageID & id);

private:
    void erase(ContainerIter it);

    std::mutex dependencies_mutex;
    std::list<RefreshTaskObserver> dependencies;
};

using RefreshDependenciesEntry = RefreshDependencies::Entry;

}
