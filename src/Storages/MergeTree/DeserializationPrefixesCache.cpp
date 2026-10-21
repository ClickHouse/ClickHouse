#include <Storages/MergeTree/DeserializationPrefixesCache.h>

namespace DB
{

std::unordered_map<std::string, ISerialization::DeserializeBinaryBulkStatePtr> DeserializationPrefixesCache::getOrSet(
    const std::function<std::unordered_map<std::string, ISerialization::DeserializeBinaryBulkStatePtr>()> & read_prefixes)
{
    if (is_set)
        return clonePrefixes();

    std::unique_lock lock(mutex);
    if (is_set)
        return clonePrefixes();

    prefixes = read_prefixes();
    is_set = true;
    return clonePrefixes();
}

std::unordered_map<std::string, ISerialization::DeserializeBinaryBulkStatePtr> DeserializationPrefixesCache::clonePrefixes() const
{
    std::unordered_map<std::string, ISerialization::DeserializeBinaryBulkStatePtr> cloned;
    cloned.reserve(prefixes->size());
    for (const auto & [name, prefix] : *prefixes)
        cloned[name] = prefix ? prefix->clone() : nullptr;
    return cloned;
}

}
