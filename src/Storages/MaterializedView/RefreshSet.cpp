#include <Storages/MaterializedView/RefreshSet.h>
#include <Storages/MaterializedView/RefreshTask.h>

namespace CurrentMetrics
{
    extern const Metric RefreshableViews;
}

namespace DB
{

RefreshSetElement::RefreshSetElement(StorageID id, std::vector<StorageID> deps, RefreshTaskHolder task)
    : corresponding_task(task)
    , view_id(std::move(id))
    , dependencies(std::move(deps))
{}

RefreshInfo RefreshSetElement::getInfo() const
{
    return {
        .database = view_id.getDatabaseName(),
        .view_name = view_id.getTableName(),
        .refresh_status = toString(RefreshTask::RefreshState{state.load()}),
        .last_refresh_result = toString(RefreshTask::LastTaskResult{last_result.load()}),
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

RefreshTaskHolder RefreshSetElement::getTask() const
{
    return corresponding_task.lock();
}

const StorageID & RefreshSetElement::getID() const
{
    return view_id;
}

const std::vector<StorageID> & RefreshSetElement::getDependencies() const
{
    return dependencies;
}

RefreshSet::Entry::Entry(Entry && other) noexcept
    : parent_set{std::exchange(other.parent_set, nullptr)}
    , iter(std::move(other.iter))
    , metric_increment(std::move(other.metric_increment))
{}

RefreshSet::Entry & RefreshSet::Entry::operator=(Entry && other) noexcept
{
    if (this == &other)
        return *this;
    reset();
    parent_set = std::exchange(other.parent_set, nullptr);
    iter = std::move(other.iter);
    metric_increment = std::move(other.metric_increment);
    return *this;
}

RefreshSet::Entry::~Entry()
{
    reset();
}

RefreshSet::Entry::Entry(RefreshSet & set, ElementMapIter it)
    : parent_set{&set}, iter(std::move(it)), metric_increment(CurrentMetrics::RefreshableViews)
{}

void RefreshSet::Entry::reset()
{
    if (!parent_set)
        return;
    std::exchange(parent_set, nullptr)->erase(iter);
    metric_increment.reset();
}

RefreshSet::RefreshSet() {}

RefreshSet::Entry RefreshSet::emplace(StorageID id, std::vector<StorageID> dependencies, RefreshTaskHolder task)
{
    std::lock_guard guard(mutex);
    auto [it, is_inserted] = elements.emplace(std::piecewise_construct, std::forward_as_tuple(id), std::forward_as_tuple(id, dependencies, std::move(task)));
    if (!is_inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Refresh set entry already exists for table {}", id.getFullTableName());

    for (const StorageID & dep : dependencies)
    {
        auto [unused, dep_inserted] = dependents[dep].insert(id);
        if (!dep_inserted)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Refresh set entry already contains dependency of {} on {}", id.getFullTableName(), dep.getFullTableName());
    }

    return Entry(*this, std::move(it));
}

RefreshTaskHolder RefreshSet::getTask(const StorageID & id) const
{
    std::lock_guard lock(mutex);
    if (auto element = elements.find(id); element != elements.end())
        return element->second.getTask();
    return nullptr;
}

RefreshSet::InfoContainer RefreshSet::getInfo() const
{
    std::lock_guard lock(mutex);
    InfoContainer res;
    res.reserve(elements.size());
    for (auto && element : elements)
        res.emplace_back(element.second.getInfo());
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
        if (auto element = elements.find(dep_id); element != elements.end())
            res.push_back(element->second.getTask());
    return res;
}

void RefreshSet::erase(ElementMapIter it)
{
    std::lock_guard lock(mutex);
    for (const StorageID & dep : it->second.getDependencies())
    {
        auto & set = dependents[dep];
        set.erase(it->second.getID());
        if (set.empty())
            dependents.erase(dep);
    }
    elements.erase(it);
}

}
