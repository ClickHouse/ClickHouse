#include <Interpreters/Cache/IFileCachePriority.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>

namespace CurrentMetrics
{
    extern const Metric FilesystemCacheSizeLimit;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

IFileCachePriority::IFileCachePriority(size_t max_size_, size_t max_elements_)
    : max_size(max_size_), max_elements(max_elements_)
{
    CurrentMetrics::add(CurrentMetrics::FilesystemCacheSizeLimit, max_size_);
}

IFileCachePriority::Entry::Entry(
    const Key & key_,
    size_t offset_,
    size_t size_,
    KeyMetadataPtr key_metadata_)
    : key(key_)
    , offset(offset_)
    , key_metadata(key_metadata_)
    , size(size_)
{
}

IFileCachePriority::Entry::Entry(const Entry & other)
    : key(other.key)
    , offset(other.offset)
    , key_metadata(other.key_metadata)
    , size(other.size.load())
    , hits(other.hits.load())
{
}

void IFileCachePriority::check(const CacheStateGuard::Lock & lock) const
{
    if (getSize(lock) > max_size || getElementsCount(lock) > max_elements)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cache limits violated. "
                        "{}", getStateInfoForLog(lock));
    }

    if (getSize(lock) > (1ull << 63) || getElementsCount(lock) > (1ull << 63))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cache became inconsistent. There must be a bug");
}

std::unordered_map<std::string, IFileCachePriority::UsageStat> IFileCachePriority::getUsageStatPerClient()
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "getUsageStatPerClient() is not implemented for {} policy",
        magic_enum::enum_name(getType()));
}

void IFileCachePriority::removeEntries(
    const std::vector<InvalidatedEntryInfo> & entries,
    const CachePriorityGuard::WriteLock & lock)
{
    if (entries.empty())
        return;

    for (const auto & [entry, it] : entries)
    {
        if (!entry->isRemoved(lock))
            it->remove(lock);
    }
}

}
