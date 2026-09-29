#include <Storages/ObjectStorage/DataLakes/PuffinFilesCache.h>

#include <Common/CurrentMetrics.h>
#include <base/arithmeticOverflow.h>

#include <initializer_list>
#include <limits>

namespace CurrentMetrics
{
extern const Metric PuffinFilesCacheBytes;
extern const Metric PuffinFilesCacheFiles;
}

namespace ProfileEvents
{
extern const Event PuffinFilesCacheWeightLost;
}

namespace DB
{

namespace
{

UInt64 saturatingAdd(UInt64 left, UInt64 right)
{
    UInt64 result = 0;
    if (common::addOverflow(left, right, result))
        return std::numeric_limits<UInt64>::max();
    return result;
}

UInt64 saturatingAdd(std::initializer_list<UInt64> values)
{
    UInt64 result = 0;
    for (UInt64 value : values)
        result = saturatingAdd(result, value);
    return result;
}

}

std::unique_ptr<roaring::Roaring64Map> PuffinFilesCache::cloneBitmap(const PuffinFilesCacheCell & cell)
{
    return std::make_unique<roaring::Roaring64Map>(*cell.bitmap);
}

bool PuffinFilesCacheKey::operator==(const PuffinFilesCacheKey & other) const
{
    return storage_identity == other.storage_identity
        && file_path == other.file_path
        && etag == other.etag
        && content_offset == other.content_offset
        && content_size_in_bytes == other.content_size_in_bytes;
}

UInt64 PuffinFilesCacheKey::approximateMemoryBytes() const
{
    return saturatingAdd(
        {static_cast<UInt64>(sizeof(PuffinFilesCacheKey)),
         static_cast<UInt64>(storage_identity.size()),
         static_cast<UInt64>(file_path.size()),
         static_cast<UInt64>(etag.size())});
}

size_t PuffinFilesCacheKeyHash::operator()(const PuffinFilesCacheKey & key) const
{
    size_t hash = 0;
    boost::hash_combine(hash, CityHash_v1_0_2::CityHash64(key.storage_identity.data(), key.storage_identity.size()));
    boost::hash_combine(hash, CityHash_v1_0_2::CityHash64(key.file_path.data(), key.file_path.size()));
    boost::hash_combine(hash, CityHash_v1_0_2::CityHash64(key.etag.data(), key.etag.size()));
    boost::hash_combine(hash, key.content_offset);
    boost::hash_combine(hash, key.content_size_in_bytes);
    return hash;
}

UInt64 PuffinFilesCacheCell::calculateMemorySize(const roaring::Roaring64Map & bitmap_, UInt64 key_memory_bytes_)
{
    return saturatingAdd(
        {key_memory_bytes_,
         static_cast<UInt64>(bitmap_.getSizeInBytes(/*portable=*/ false)),
         static_cast<UInt64>(sizeof(PuffinFilesCacheCell)),
         static_cast<UInt64>(SIZE_IN_MEMORY_OVERHEAD)});
}

PuffinFilesCacheCell::PuffinFilesCacheCell(std::shared_ptr<const roaring::Roaring64Map> bitmap_, UInt64 key_memory_bytes_)
    : bitmap(std::move(bitmap_))
    , memory_bytes(calculateMemorySize(*bitmap, key_memory_bytes_))
{
}

size_t PuffinFilesCacheWeightFunction::operator()(const PuffinFilesCacheCell & cell) const
{
    return cell.memory_bytes;
}

PuffinFilesCache::PuffinFilesCache(
    const String & cache_policy,
    size_t max_size_in_bytes,
    size_t max_count,
    double size_ratio)
    : Base(cache_policy, CurrentMetrics::PuffinFilesCacheBytes, CurrentMetrics::PuffinFilesCacheFiles, max_size_in_bytes, max_count, size_ratio)
    , log(getLogger("PuffinFilesCache"))
{
}

String PuffinFilesCache::makeStorageIdentity(const IObjectStorage & object_storage)
{
    /// `getDescription` (S3 endpoint, Azure account URL, local path, ...) keeps two backends with
    /// the same bucket and prefix on different hosts from sharing entries.
    return object_storage.getName() + "://" + object_storage.getDescription() + "/"
        + object_storage.getObjectsNamespace() + "/" + object_storage.getCommonKeyPrefix();
}

std::optional<PuffinFilesCacheKey> PuffinFilesCache::tryCreateKey(
    const String & storage_identity,
    const String & file_path,
    const ObjectMetadata & metadata,
    Int64 content_offset,
    Int64 content_size_in_bytes)
{
    if (!metadata.isEtagUsableAsCacheKey())
        return std::nullopt;

    return PuffinFilesCacheKey{
        storage_identity,
        file_path,
        metadata.etag,
        content_offset,
        content_size_in_bytes};
}

void PuffinFilesCache::onEntryRemoval(const size_t weight_loss, const MappedPtr &)
{
    LOG_TRACE(log, "Puffin files cache eviction");
    ProfileEvents::increment(ProfileEvents::PuffinFilesCacheWeightLost, weight_loss);
}

}
