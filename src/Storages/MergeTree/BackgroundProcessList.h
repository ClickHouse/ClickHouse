#pragma once

#include <Common/CurrentMetrics.h>
#include <memory>
#include <list>
#include <mutex>
#include <atomic>

namespace DB
{

/// Common code for background processes lists, like system.merges and system.replicated_fetches
/// Look at examples in MergeList and ReplicatedFetchList

template <typename ListElement, typename Info>
class BackgroundProcessList;

template <typename ListElement, typename Info>
class BackgroundProcessListEntry
{
    BackgroundProcessList<ListElement, Info> & list;
    using container_t = std::list<ListElement>;
    typename container_t::iterator it;
    CurrentMetrics::Increment metric_increment;
public:
    BackgroundProcessListEntry(const BackgroundProcessListEntry &) = delete;
    BackgroundProcessListEntry & operator=(const BackgroundProcessListEntry &) = delete;

    BackgroundProcessListEntry(BackgroundProcessList<ListElement, Info> & list_, const typename container_t::iterator it_, const CurrentMetrics::Metric & metric)
        : list(list_), it{it_}, metric_increment{metric}
    {
        list.onEntryCreate(*this);
    }

    ~BackgroundProcessListEntry()
    {
        std::lock_guard lock{list.mutex};
        list.onEntryDestroy(*this);
        list.entries.erase(it);
    }

    ListElement * operator->() { return &*it; }
    const ListElement * operator->() const { return &*it; }
};


template <typename ListElement, typename Info>
class BackgroundProcessList
{
protected:
    friend class BackgroundProcessListEntry<ListElement, Info>;

    using container_t = std::list<ListElement>;
    using info_container_t = std::list<Info>;

    mutable std::mutex mutex;
    container_t entries;

    CurrentMetrics::Metric metric;

    BackgroundProcessList(const CurrentMetrics::Metric & metric_)
        : metric(metric_)
    {}
public:

    using Entry = BackgroundProcessListEntry<ListElement, Info>;
    using EntryPtr = std::unique_ptr<Entry>;

    template <typename... Args>
    EntryPtr insert(Args &&... args)
    {
        std::lock_guard lock{mutex};
        auto entry = std::make_unique<Entry>(*this, entries.emplace(entries.end(), std::forward<Args>(args)...), metric);
        return entry;
    }

    info_container_t get() const
    {
        std::lock_guard lock{mutex};
        info_container_t res;
        for (const auto & list_element : entries)
            res.emplace_back(list_element.getInfo());
        return res;
    }

    virtual void onEntryCreate(const Entry & /* entry */) {}
    virtual void onEntryDestroy(const Entry & /* entry */) {}
    virtual inline ~BackgroundProcessList() {}
};

}
