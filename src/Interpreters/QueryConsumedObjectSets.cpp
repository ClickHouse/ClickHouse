#include <Interpreters/QueryConsumedObjectSets.h>

#include <algorithm>


namespace DB
{

size_t QueryConsumedObjectSets::beginCapture(const UUID & table_uuid)
{
    std::lock_guard lock(mutex);
    auto & reads = objects_by_table[table_uuid];
    reads.emplace_back();
    return reads.size() - 1;
}

void QueryConsumedObjectSets::add(const UUID & table_uuid, size_t read_index, Object object)
{
    std::lock_guard lock(mutex);
    objects_by_table.at(table_uuid).at(read_index).push_back(std::move(object));
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

std::optional<QueryConsumedObjectSets::ConsumedObjects> QueryConsumedObjectSets::get(const UUID & table_uuid) const
{
    std::vector<ObjectSet> reads;
    {
        std::lock_guard lock(mutex);
        auto it = objects_by_table.find(table_uuid);
        if (it == objects_by_table.end())
            return {};
        reads = it->second;
    }

    ConsumedObjects result;
    for (auto & read : reads)
        canonicalize(read);
    result.objects = std::move(reads.front());
    for (size_t i = 1; i < reads.size(); ++i)
        if (reads[i] != result.objects)
            result.reads_agree = false;
    return result;
}

void QueryConsumedObjectSets::canonicalize(ObjectSet & objects)
{
    std::sort(objects.begin(), objects.end());
    objects.erase(std::unique(objects.begin(), objects.end()), objects.end());
}

}
