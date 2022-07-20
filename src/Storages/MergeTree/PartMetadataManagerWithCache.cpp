#include "PartMetadataManagerWithCache.h"

#if USE_ROCKSDB
#include <Common/hex.h>
#include <Common/ErrorCodes.h>
#include <IO/HashingReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace ProfileEvents
{
    extern const Event MergeTreeMetadataCacheHit;
    extern const Event MergeTreeMetadataCacheMiss;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CORRUPTED_DATA;
    extern const int NO_SUCH_PROJECTION_IN_TABLE;
}

PartMetadataManagerWithCache::PartMetadataManagerWithCache(const IMergeTreeDataPart * part_, const MergeTreeMetadataCachePtr & cache_)
    : IPartMetadataManager(part_), cache(cache_)
{
}

String PartMetadataManagerWithCache::getKeyFromFilePath(const String & file_path) const
{
    return part->data_part_storage->getDiskName() + ":" + file_path;
}

String PartMetadataManagerWithCache::getFilePathFromKey(const String & key) const
{
    return key.substr(part->data_part_storage->getDiskName().size() + 1);
}

std::unique_ptr<SeekableReadBuffer> PartMetadataManagerWithCache::read(const String & file_name) const
{
    String file_path = fs::path(part->data_part_storage->getRelativePath()) / file_name;
    String key = getKeyFromFilePath(file_path);
    String value;
    auto status = cache->get(key, value);
    if (!status.ok())
    {
        ProfileEvents::increment(ProfileEvents::MergeTreeMetadataCacheMiss);
        auto in = part->data_part_storage->readFile(file_name, {}, std::nullopt, std::nullopt);
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
    String file_path = fs::path(part->data_part_storage->getRelativePath()) / file_name;
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
        return part->data_part_storage->exists(file_name);
    }
}

void PartMetadataManagerWithCache::deleteAll(bool include_projection)
{
    Strings file_names;
    part->appendFilesOfColumnsChecksumsIndexes(file_names, include_projection);

    String value;
    for (const auto & file_name : file_names)
    {
        String file_path = fs::path(part->data_part_storage->getRelativePath()) / file_name;
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
        String file_path = fs::path(part->data_part_storage->getRelativePath()) / file_name;
        if (!part->data_part_storage->exists(file_name))
            continue;
        auto in = part->data_part_storage->readFile(file_name, {}, std::nullopt, std::nullopt);
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
        if (fs::path(part->data_part_storage->getRelativePath()) / file_name == file_path)
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
                if (fs::path(part->data_part_storage->getRelativePath()) / (projection_name + ".proj") / file_name == file_path)
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
    String prefix = getKeyFromFilePath(fs::path(part->data_part_storage->getRelativePath()) / "");
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

std::unordered_map<String, IPartMetadataManager::uint128> PartMetadataManagerWithCache::check() const
{
    /// Only applies for normal part stored on disk
    if (part->isProjectionPart() || !part->isStoredOnDisk())
        return {};

    /// The directory of projection part is under the directory of its parent part
    const auto filenames_without_checksums = part->getFileNamesWithoutChecksums();

    std::unordered_map<String, uint128> results;
    Strings keys;
    std::vector<uint128> cache_checksums;
    std::vector<uint128> disk_checksums;
    getKeysAndCheckSums(keys, cache_checksums);
    for (size_t i = 0; i < keys.size(); ++i)
    {
        const auto & key = keys[i];
        String file_path = getFilePathFromKey(key);
        String file_name = fs::path(file_path).filename();
        results.emplace(file_name, cache_checksums[i]);

        /// File belongs to normal part
        if (fs::path(part->data_part_storage->getRelativePath()) / file_name == file_path)
        {
            auto disk_checksum = part->getActualChecksumByFile(file_name);
            if (disk_checksum != cache_checksums[i])
                throw Exception(
                    ErrorCodes::CORRUPTED_DATA,
                    "Checksums doesn't match in part {} for {}. Expected: {}. Found {}.",
                    part->name, file_path,
                    getHexUIntUppercase(disk_checksum.first) + getHexUIntUppercase(disk_checksum.second),
                    getHexUIntUppercase(cache_checksums[i].first) + getHexUIntUppercase(cache_checksums[i].second));

            disk_checksums.push_back(disk_checksum);
            continue;
        }

        /// File belongs to projection part
        String proj_dir_name = fs::path(file_path).parent_path().filename();
        auto pos = proj_dir_name.find_last_of('.');
        if (pos == String::npos)
        {
            throw Exception(
                ErrorCodes::NO_SUCH_PROJECTION_IN_TABLE,
                "There is no projection in part: {} contains file: {} with directory name: {}",
                part->name,
                file_path,
                proj_dir_name);
        }

        String proj_name = proj_dir_name.substr(0, pos);
        const auto & projection_parts = part->getProjectionParts();
        auto it = projection_parts.find(proj_name);
        if (it == projection_parts.end())
        {
            throw Exception(
                ErrorCodes::NO_SUCH_PROJECTION_IN_TABLE,
                "There is no projection {} in part: {} contains file: {}",
                proj_name, part->name, file_path);
        }

        auto disk_checksum = it->second->getActualChecksumByFile(file_name);
        if (disk_checksum != cache_checksums[i])
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Checksums doesn't match in projection part {} {}. Expected: {}. Found {}.",
                part->name, proj_name,
                getHexUIntUppercase(disk_checksum.first) + getHexUIntUppercase(disk_checksum.second),
                getHexUIntUppercase(cache_checksums[i].first) + getHexUIntUppercase(cache_checksums[i].second));
        disk_checksums.push_back(disk_checksum);
    }
    return results;
}

}
#endif
