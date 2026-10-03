#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/BlobLinkCounts.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

uint32_t BlobLinkCounts::get(const std::string & blob_key) const
{
    std::lock_guard lock(mutex);
    const auto it = counts.find(blob_key);
    return it == counts.end() ? 1 : it->second;
}

void BlobLinkCounts::apply(const std::unordered_map<std::string, int64_t> & deltas)
{
    std::lock_guard lock(mutex);
    for (const auto & [blob_key, delta] : deltas)
    {
        if (delta == 0)
            continue;

        const auto it = counts.find(blob_key);
        const int64_t previous = it == counts.end() ? 1 : it->second;
        const int64_t updated = previous + delta;

        /// Zero means that the last link was removed: the blob is not tracked anymore.
        if (updated < 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Negative number of links ({}) to the blob '{}'", updated, blob_key);

        if (updated <= 1)
        {
            if (it != counts.end())
                counts.erase(it);
        }
        else if (it == counts.end())
        {
            counts.emplace(blob_key, static_cast<uint32_t>(updated));
        }
        else
        {
            it->second = static_cast<uint32_t>(updated);
        }
    }
}

void BlobLinkCounts::replace(const std::unordered_map<std::string, uint32_t> & new_counts)
{
    std::lock_guard lock(mutex);
    counts.clear();
    for (const auto & [blob_key, count] : new_counts)
        if (count > 1)
            counts.emplace(blob_key, count);
}

}
