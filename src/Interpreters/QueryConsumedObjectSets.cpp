#include <Interpreters/QueryConsumedObjectSets.h>


namespace DB
{

void QueryConsumedObjectSets::beginCapture(const UUID & table_uuid)
{
    std::lock_guard lock(mutex);
    objects_by_table.try_emplace(table_uuid);
}

void QueryConsumedObjectSets::add(const UUID & table_uuid, Object object)
{
    std::lock_guard lock(mutex);
    objects_by_table[table_uuid].push_back(std::move(object));
}

void QueryConsumedObjectSets::markPruned(const UUID & table_uuid)
{
    std::lock_guard lock(mutex);
    pruned_tables.insert(table_uuid);
}

bool QueryConsumedObjectSets::isPruned(const UUID & table_uuid) const
{
    std::lock_guard lock(mutex);
    return pruned_tables.contains(table_uuid);
}

std::optional<std::vector<QueryConsumedObjectSets::Object>> QueryConsumedObjectSets::get(const UUID & table_uuid) const
{
    std::lock_guard lock(mutex);
    auto it = objects_by_table.find(table_uuid);
    if (it == objects_by_table.end())
        return {};
    return it->second;
}

}
