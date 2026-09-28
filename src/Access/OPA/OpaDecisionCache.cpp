#include <Access/OPA/OpaDecisionCache.h>

#include <Common/SipHash.h>


namespace DB
{

size_t OpaDecisionCache::Hash::operator()(const Key & key) const
{
    SipHash hash;

    hash.update(key.operations.size());
    for (const auto & operation : key.operations)
        hash.update(operation);

    hash.update(key.resource.database);
    hash.update(key.resource.table);

    hash.update(key.resource.columns.size());
    for (const auto & column : key.resource.columns)
        hash.update(column);

    return hash.get64();
}

std::optional<bool> OpaDecisionCache::get(const Key & key) const
{
    std::lock_guard lock{mutex};

    if (const auto it = decisions.find(key); it != decisions.end())
        return it->second;

    return {};
}

void OpaDecisionCache::set(const Key & key, bool decision)
{
    std::lock_guard lock{mutex};
    decisions.emplace(key, decision);
}

}
