#include <Storages/MaterializedView/RefreshSet.h>
#include <Storages/MaterializedView/RefreshTask.h>

namespace CurrentMetrics
{
    extern const Metric RefreshingViews;
}

namespace DB
{

RefreshSetElement::RefreshSetElement(RefreshTaskHolder task, StorageID id)
    : corresponding_task(task)
    , view_id(std::move(id))
{}

RefreshInfo RefreshSetElement::getInfo() const
{
    return {
        .database = view_id.getDatabaseName(),
        .view_name = view_id.getTableName(),
        .refresh_status = toString(RefreshTask::TaskState{state.load()}),
        .last_refresh_status = toString(RefreshTask::LastTaskState{last_state.load()}),
        .last_refresh_time = static_cast<UInt32>(last_s.load(std::memory_order_relaxed)),
        .next_refresh_time = static_cast<UInt32>(next_s.load(std::memory_order_relaxed)),
        .progress = static_cast<Float64>(written_rows) / total_rows_to_read,
        .elapsed_ns = elapsed_ns / 1e9,
        .read_rows = read_rows.load(std::memory_order_relaxed),
        .read_bytes = read_bytes.load(std::memory_order_relaxed),
        .total_rows_to_read = total_rows_to_read.load(std::memory_order_relaxed),
        .total_bytes_to_read = total_bytes_to_read.load(std::memory_order_relaxed),
        .written_rows = written_rows.load(std::memory_order_relaxed),
        .written_bytes = written_bytes.load(std::memory_order_relaxed),
        .result_rows = result_rows.load(std::memory_order_relaxed),
        .result_bytes = result_bytes.load(std::memory_order_relaxed)
    };
}

const StorageID & RefreshSetElement::getID() const
{
    return view_id;
}

RefreshTaskHolder RefreshSetElement::getTask() const
{
    return corresponding_task.lock();
}

bool RefreshSetLess::operator()(const RefreshSetElement & l, const RefreshSetElement & r) const
{
    return l.getID().uuid < r.getID().uuid;
}

bool RefreshSetLess::operator()(const StorageID & l, const RefreshSetElement & r) const
{
    return l.uuid < r.getID().uuid;
}

bool RefreshSetLess::operator()(const RefreshSetElement & l, const StorageID & r) const
{
    return l.getID().uuid < r.uuid;
}

bool RefreshSetLess::operator()(const StorageID & l, const StorageID & r) const
{
    return l.uuid < r.uuid;
}

RefreshSet::Entry::Entry()
    : parent_set{nullptr}
{}

RefreshSet::Entry::Entry(Entry && other) noexcept
    : parent_set{std::exchange(other.parent_set, nullptr)}
    , iter(std::move(other.iter))
    , metric_increment(std::move(other.metric_increment))
{}

RefreshSet::Entry & RefreshSet::Entry::operator=(Entry && other) noexcept
{
    if (this == &other)
        return *this;
    cleanup(std::exchange(parent_set, std::exchange(other.parent_set, nullptr)));
    iter = std::move(other.iter);
    metric_increment = std::move(other.metric_increment);
    return *this;
}

RefreshSet::Entry::~Entry()
{
    cleanup(parent_set);
}

RefreshSet::Entry::Entry(RefreshSet & set, ContainerIter it, const CurrentMetrics::Metric & metric)
    : parent_set{&set}, iter(std::move(it)), metric_increment(metric)
{}

void RefreshSet::Entry::cleanup(RefreshSet * set)
{
    if (set)
        set->erase(iter);
}

RefreshSet::RefreshSet()
    : set_metric(CurrentMetrics::RefreshingViews)
{}

RefreshTaskHolder RefreshSet::getTask(const StorageID & id) const
{
    std::lock_guard lock(elements_mutex);
    if (auto element = elements.find(id); element != elements.end())
        return element->getTask();
    return nullptr;
}

RefreshSet::InfoContainer RefreshSet::getInfo() const
{
    std::lock_guard lock(elements_mutex);
    InfoContainer res;
    res.reserve(elements.size());
    for (auto && element : elements)
        res.emplace_back(element.getInfo());
    return res;
}

void RefreshSet::erase(ContainerIter it)
{
    std::lock_guard lock(elements_mutex);
    elements.erase(it);
}

}
