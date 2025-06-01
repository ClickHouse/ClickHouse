#include <Interpreters/Cache/IFileCachePriority.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/filesystemHelpers.h>
#include <Interpreters/Cache/Metadata.h>
#include <Interpreters/Cache/FileCacheSettings.h>

namespace CurrentMetrics
{
    extern const Metric FilesystemCacheSizeLimit;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
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
    , aligned_size(key_metadata->alignFileSize(size_))
    , use_real_disk_size(key_metadata->useRealDiskSize())
{
}

IFileCachePriority::Entry::Entry(const Entry & other)
    : key(other.key)
    , offset(other.offset)
    , key_metadata(other.key_metadata)
    , hits(other.hits)
    , size(other.size.load())
    , aligned_size(key_metadata->alignFileSize(size.load()))
    , use_real_disk_size(key_metadata->useRealDiskSize())
{
}

size_t IFileCachePriority::Entry::getSize(IFileCachePriority::Entry::SizeAlignment alignment) const
{
    switch (alignment)
    {
        case IFileCachePriority::Entry::SizeAlignment::CACHE_ALIGNMENT:
            if (useRealDiskSize())
            {
                return aligned_size.load();
            }
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
    {
        aligned_size.store(key_metadata->alignFileSize(size.load()));
    }
}

void IFileCachePriority::Entry::increaseSize(size_t size_)
{
    size += size_;
    if (use_real_disk_size)
    {
        aligned_size.store(key_metadata->alignFileSize(size.load()));
    }
}

void IFileCachePriority::Entry::decreaseSize(size_t size_)
{
    chassert(size.load() >= size_);
    size -= size_;
    if (use_real_disk_size)
    {
        aligned_size.store(key_metadata->alignFileSize(size.load()));
    }
}

void IFileCachePriority::check(const CachePriorityGuard::Lock & lock) const
{
    if (getSize(lock) > max_size || getElementsCount(lock) > max_elements)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cache limits violated. "
                        "{}", getStateInfoForLog(lock));
    }
}

}
