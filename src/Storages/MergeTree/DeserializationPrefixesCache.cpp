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

void DeserializationPrefixesCache::addToOwnershipValidator(ColumnsOwnershipValidator & validator) const
{
    /// `prefixes` is written once under `mutex` before `is_set` becomes true and is never mutated
    /// afterwards, so once `is_set` is observed as true the stored states can be read without locking.
    /// The states themselves are the originals shared with the reader clones; enumerate them read-only.
    if (!is_set)
        return;
    validator.add(*prefixes);
}

}
