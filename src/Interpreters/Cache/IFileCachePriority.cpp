#include <Interpreters/Cache/IFileCachePriority.h>
#include <Common/CurrentMetrics.h>


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
{
}

IFileCachePriority::Entry::Entry(const Entry & other)
    : key(other.key)
    , offset(other.offset)
    , key_metadata(other.key_metadata)
    , size(other.size.load())
    , hits(other.hits)
{
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
