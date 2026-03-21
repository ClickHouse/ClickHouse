#include <Interpreters/Cache/IFileCachePriority.h>
#include <Interpreters/Cache/EvictionCandidates.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/filesystemHelpers.h>
#include <Interpreters/Cache/Metadata.h>
#include <Interpreters/Cache/FileCacheSettings.h>


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
    , aligned_size(key_metadata->alignFileSize(size_))
    , use_real_disk_size(key_metadata->useRealDiskSize())
{
}

IFileCachePriority::Entry::Entry(const Entry & other)
    : key(other.key)
    , offset(other.offset)
    , key_metadata(other.key_metadata)
    , hits(other.hits.load())
    , size(other.size.load())
    , aligned_size(key_metadata->alignFileSize(other.size.load()))
    , use_real_disk_size(key_metadata->useRealDiskSize())
{
}

size_t IFileCachePriority::Entry::getSize(IFileCachePriority::Entry::SizeAlignment alignment) const
{
    switch (alignment)
    {
        case IFileCachePriority::Entry::SizeAlignment::DEFAULT_ALIGNMENT:
            if (useRealDiskSize())
                return aligned_size.load();
            return size.load();
        case IFileCachePriority::Entry::SizeAlignment::ALIGNED:
            return aligned_size.load();
        case IFileCachePriority::Entry::SizeAlignment::NOT_ALIGNED:
            return size.load();
    }
    chassert(false);
    return 0;
}

bool IFileCachePriority::Entry::useRealDiskSize() const
{
    return use_real_disk_size;
}

void IFileCachePriority::Entry::setSize(size_t size_)
{
    size.store(size_);
    if (use_real_disk_size)
        aligned_size.store(key_metadata->alignFileSize(size_));
}

void IFileCachePriority::Entry::increaseSize(size_t size_)
{
    size += size_;
    if (use_real_disk_size)
        aligned_size.store(key_metadata->alignFileSize(size.load()));
}

void IFileCachePriority::Entry::decreaseSize(size_t size_)
{
    chassert(size.load() >= size_);
    size -= size_;
    if (use_real_disk_size)
        aligned_size.store(key_metadata->alignFileSize(size.load()));
}

std::string IFileCachePriority::Entry::toString(const std::string & prefix) const
{
    return fmt::format(
        "{}{}:{}:{} (state: {})",
        prefix, key, offset, size.load(),
        magic_enum::enum_name(state.load(std::memory_order_relaxed)));
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
        /// We store `entry` shared pointer in addition to `it`
        /// (which is an iterator pointing to the same entry)
        /// because `it` could become invalid,
        /// so we use `entry` to check validity of the iterator.
        const auto entry_state = entry->getState();
        chassert(entry_state == Entry::State::Invalidated || entry_state == Entry::State::Removed,
                 fmt::format("Unexpected state: {}", magic_enum::enum_name(entry_state)));
        if (entry_state != Entry::State::Removed)
            it->remove(lock);
    }
}

}
