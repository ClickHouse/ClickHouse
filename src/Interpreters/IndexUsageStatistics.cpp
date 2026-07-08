#include <Interpreters/IndexUsageStatistics.h>

#include <Common/SipHash.h>

namespace DB
{

IndexUsageStatistics::Key IndexUsageStatistics::makeKey(const StorageID & storage_id, IndexKind kind, const String & name)
{
    Key key;
    key.kind = kind;
    key.name = name;

    /// Prefer the table UUID: it survives renames and does not clash after DROP + CREATE
    /// of a different table with the same name.
    if (storage_id.hasUUID())
    {
        key.table_uuid = storage_id.uuid;
    }
    else
    {
        key.database = storage_id.database_name;
        key.table = storage_id.table_name;
    }

    return key;
}

size_t IndexUsageStatistics::KeyHash::operator()(const Key & key) const
{
    SipHash hash;
    hash.update(key.table_uuid.toUnderType());
    hash.update(key.database.data(), key.database.size());
    hash.update(key.table.data(), key.table.size());
    hash.update(static_cast<UInt8>(key.kind));
    hash.update(key.name.data(), key.name.size());
    return hash.get64();
}

void IndexUsageStatistics::record(const Key & key, UInt64 granules_evaluated, UInt64 granules_dropped, time_t now)
{
    std::lock_guard lock(mutex);
    auto & entry = counters[key];
    ++entry.times_used;
    entry.granules_evaluated += granules_evaluated;
    entry.granules_dropped += granules_dropped;
    entry.last_used_time = now;
}

void IndexUsageStatistics::addGranulesDropped(const Key & key, UInt64 granules_dropped)
{
    if (granules_dropped == 0)
        return;

    std::lock_guard lock(mutex);
    counters[key].granules_dropped += granules_dropped;
}

IndexUsageStatistics::Counters IndexUsageStatistics::get(const Key & key) const
{
    std::lock_guard lock(mutex);
    auto it = counters.find(key);
    if (it == counters.end())
        return {};
    return it->second;
}

}
