#include "PartMetadataCache.h"

#if USE_ROCKSDB
#include <rocksdb/db.h>
#include <Disks/IDisk.h>
#include <IO/HashingReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeMetadataCache.h>

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

std::unique_ptr<SeekableReadBuffer>
PartMetadataCache::readOrSet(const DiskPtr & disk, const String & file_name, String & value)
{
    String file_path = fs::path(getFullRelativePath()) / file_name;
    String key = getKey(file_path);
    auto status = cache->get(key, value);
    if (!status.ok())
    {
        ProfileEvents::increment(ProfileEvents::MergeTreeMetadataCacheMiss);
        if (!disk->exists(file_path))
        {
            return nullptr;
        }

        auto in = disk->readFile(file_path);
        if (in)
        {
            readStringUntilEOF(value, *in);
            cache->put(key, value);
        }
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::MergeTreeMetadataCacheHit);
    }
    return std::make_unique<ReadBufferFromString>(value);
}

void PartMetadataCache::batchSet(const DiskPtr & disk, const Strings & file_names)
{
    String text;
    String read_value;
    for (const auto & file_name : file_names)
    {
        String file_path = fs::path(getFullRelativePath()) / file_name;
        String key = getKey(file_path);
        if (!disk->exists(file_path))
            continue;

        auto in = disk->readFile(file_path);
        if (!in)
            continue;

        readStringUntilEOF(text, *in);
        auto status = cache->put(key, text);
        if (!status.ok())
        {
            status = cache->get(key, read_value);
            if (status.IsNotFound() || read_value == text)
                continue;
            throw Exception(ErrorCodes::LOGICAL_ERROR, "set meta failed status:{}, file_path:{}", status.ToString(), file_path);
        }
    }
}

void PartMetadataCache::batchDelete(const Strings & file_names)
{
    for (const auto & file_name : file_names)
    {
        String file_path = fs::path(getFullRelativePath()) / file_name;
        String key = getKey(file_path);
        auto status = cache->del(key);
        if (!status.ok())
        {
            String read_value;
            status = cache->get(key, read_value);
            if (status.IsNotFound())
                continue;
            throw Exception(ErrorCodes::LOGICAL_ERROR, "drop meta failed status:{}, file_path:{}", status.ToString(), file_path);
        }
    }
}

void PartMetadataCache::set(const String & file_name, const String & value)
{
    String file_path = fs::path(getFullRelativePath()) / file_name;
    String key = getKey(file_path);
    String read_value;
    auto status = cache->get(key, read_value);
    if (status == rocksdb::Status::OK() && value == read_value)
        return;

    status = cache->put(key, value);
    if (!status.ok())
    {
        status = cache->get(key, read_value);
        if (status.IsNotFound() || read_value == value)
            return;

        throw Exception(ErrorCodes::LOGICAL_ERROR, "set meta failed status:{}, file_path:{}", status.ToString(), file_path);
    }
}

void PartMetadataCache::getFilesAndCheckSums(Strings & files, std::vector<uint128> & checksums) const
{
    String prefix = getKey(fs::path(getFullRelativePath()) / "");
    Strings values;
    cache->getByPrefix(prefix, files, values);
    size_t size = files.size();
    for (size_t i = 0; i < size; ++i)
    {
        ReadBufferFromString rbuf(values[i]);
        HashingReadBuffer hbuf(rbuf);
        checksums.push_back(hbuf.getHash());
    }
}

String PartMetadataCache::getFullRelativePath() const
{
    return fs::path(relative_data_path) / (parent_part ? parent_part->relative_path : "") / relative_path / "";
}

String PartMetadataCache::getKey(const String & file_path) const
{
    return disk_name + ":" + file_path;
}

}
#endif
