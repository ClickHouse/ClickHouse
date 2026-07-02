#include <Interpreters/QueryConsumedObjectSets.h>


namespace DB
{

void QueryConsumedObjectSets::add(const UUID & table_uuid, Object object)
{
    std::lock_guard lock(mutex);
    objects_by_table[table_uuid].push_back(std::move(object));
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
