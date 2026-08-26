#include <Interpreters/QueryStatisticsCache.h>

#include <Common/SipHash.h>

#include <algorithm>

namespace DB
{

Names QueryStatisticsCache::Key::makeRequiredColumns(const Names & columns)
{
    Names result = columns;
    std::sort(result.begin(), result.end());
    result.erase(std::unique(result.begin(), result.end()), result.end());
    return result;
}

size_t QueryStatisticsCache::KeyHash::operator()(const Key & key) const
{
    SipHash hash;
    hash.update(reinterpret_cast<uintptr_t>(key.storage));
    /// Names are length-prefixed: `SipHash::update` feeds a string as raw bytes, so an element count
    /// alone separates vectors of different length but not different splits of one concatenation -
    /// `{"a", "bc"}` and `{"ab", "c"}` would hash alike.
    hash.update(key.part_names.size());
    for (const auto & name : key.part_names)
    {
        hash.update(name.size());
        hash.update(name);
    }
    hash.update(key.required_columns.size());
    for (const auto & name : key.required_columns)
    {
        hash.update(name.size());
        hash.update(name);
    }
    return hash.get64();
}

std::optional<ConditionSelectivityPayloadPtr> QueryStatisticsCache::get(const Key & key) const
{
    std::lock_guard lock(cache_mutex);
    if (auto it = cache.find(key); it != cache.end())
        return it->second;
    return {};
}

void QueryStatisticsCache::set(Key key, ConditionSelectivityPayloadPtr payload, size_t max_entries)
{
    std::lock_guard lock(cache_mutex);
    if (cache.size() >= max_entries && !cache.contains(key))
        return;
    cache.insert_or_assign(std::move(key), std::move(payload));
}

}
