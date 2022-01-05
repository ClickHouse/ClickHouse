#include "PartMetadataManagerWithCache.h"

#if USE_ROCKSDB
#include <IO/HashingReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace ProfileEvents
{
    extern const Event MergeTreeMetadataCacheHit;
    extern const Event MergeTreeMetadataCacheMiss;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB
{
PartMetadataManagerWithCache::PartMetadataManagerWithCache(const IMergeTreeDataPart * part_, const MergeTreeMetadataCachePtr & cache_)
    : IPartMetadataManager(part_), cache(cache_)
{
}

String PartMetadataManagerWithCache::getKeyFromFilePath(const String & file_path) const
{
    return disk->getName() + ":" + file_path;
}

String PartMetadataManagerWithCache::getFilePathFromKey(const String & key) const
{
    return key.substr(disk->getName().size() + 1);
}

std::unique_ptr<SeekableReadBuffer> PartMetadataManagerWithCache::read(const String & file_name) const
{
    String file_path = fs::path(part->getFullRelativePath()) / file_name;
    String key = getKeyFromFilePath(file_path);
    String value;
    auto status = cache->get(key, value);
    if (!status.ok())
    {
        ProfileEvents::increment(ProfileEvents::MergeTreeMetadataCacheMiss);
        auto in = disk->readFile(file_path);
        readStringUntilEOF(value, *in);
        cache->put(key, value);
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::MergeTreeMetadataCacheHit);
    }
    return std::make_unique<ReadBufferFromOwnString>(value);
}

bool PartMetadataManagerWithCache::exists(const String & file_name) const
{
    String file_path = fs::path(part->getFullRelativePath()) / file_name;
    String key = getKeyFromFilePath(file_path);
    String value;
    auto status = cache->get(key, value);
    if (status.ok())
    {
        ProfileEvents::increment(ProfileEvents::MergeTreeMetadataCacheHit);
        return true;
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::MergeTreeMetadataCacheMiss);
        return disk->exists(fs::path(part->getFullRelativePath()) / file_name);
    }
}

void PartMetadataManagerWithCache::deleteAll(bool include_projection)
{
    Strings file_names;
    part->appendFilesOfColumnsChecksumsIndexes(file_names, include_projection);

    String value;
    for (const auto & file_name : file_names)
    {
        String file_path = fs::path(part->getFullRelativePath()) / file_name;
        String key = getKeyFromFilePath(file_path);
        auto status = cache->del(key);
        if (!status.ok())
        {
            status = cache->get(key, value);
            if (status.IsNotFound())
                continue;

            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "deleteAll failed include_projection:{} status:{}, file_path:{}",
                include_projection,
                status.ToString(),
                file_path);
        }
    }
}

void PartMetadataManagerWithCache::updateAll(bool include_projection)
{
    Strings file_names;
    part->appendFilesOfColumnsChecksumsIndexes(file_names, include_projection);

    String value;
    String read_value;
    for (const auto & file_name : file_names)
    {
        String file_path = fs::path(part->getFullRelativePath()) / file_name;
        if (!disk->exists(file_path))
            continue;
        auto in = disk->readFile(file_path);
        readStringUntilEOF(value, *in);

        String key = getKeyFromFilePath(file_path);
        auto status = cache->put(key, value);
        if (!status.ok())
        {
            status = cache->get(key, read_value);
            if (status.IsNotFound() || read_value == value)
                continue;

            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "updateAll failed include_projection:{} status:{}, file_path:{}",
                include_projection,
                status.ToString(),
                file_path);
        }
    }
}

void PartMetadataManagerWithCache::assertAllDeleted(bool include_projection) const
{
    Strings keys;
    std::vector<uint128> _;
    getKeysAndCheckSums(keys, _);
    if (keys.empty())
        return;

    String file_path;
    String file_name;
    for (const auto & key : keys)
    {
        file_path = getFilePathFromKey(key);
        file_name = fs::path(file_path).filename();

        /// Metadata file belongs to current part
        if (fs::path(part->getFullRelativePath()) / file_name == file_path)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Data part {} with type {} with meta file {} still in cache",
                part->name,
                part->getType().toString(),
                file_path);

        /// File belongs to projection part of current part
        if (!part->isProjectionPart() && include_projection)
        {
            const auto & projection_parts = part->getProjectionParts();
            for (const auto & [projection_name, projection_part] : projection_parts)
            {
                if (fs::path(projection_part->getFullRelativePath()) / file_name == file_path)
                {
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Data part {} with type {} with meta file {} with projection name still in cache",
                        part->name,
                        part->getType().toString(),
                        file_path,
                        projection_name);
                }
            }
        }
    }
}

void PartMetadataManagerWithCache::getKeysAndCheckSums(Strings & keys, std::vector<uint128> & checksums) const
{
    String prefix = getKeyFromFilePath(fs::path(part->getFullRelativePath()) / "");
    Strings values;
    cache->getByPrefix(prefix, keys, values);
    size_t size = keys.size();
    for (size_t i = 0; i < size; ++i)
    {
        ReadBufferFromString rbuf(values[i]);
        HashingReadBuffer hbuf(rbuf);
        checksums.push_back(hbuf.getHash());
    }
}

}
#endif
