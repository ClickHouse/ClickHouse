#include <Storages/MergeTree/MergeTreeIndexClearTTL.h>

#include <Storages/MergeTree/DataPartStorageOnDiskBase.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeIndicesSerialization.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/Exception.h>

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool allow_remote_fs_zero_copy_replication;
    extern const MergeTreeSettingsBool always_use_copy_instead_of_hardlinks;
}

std::map<String, time_t> getIndexesWithExpiredClearTTL(
    const StorageInMemoryMetadata & metadata, const MergeTreeDataPartTTLInfos & ttl_infos, time_t current_time)
{
    std::map<String, time_t> result;
    for (const auto & ttl : metadata.getIndexClearTTLs())
    {
        const auto it = ttl_infos.index_clear_ttl.find(ttl.result_column);
        if (it == ttl_infos.index_clear_ttl.end() || !it->second.max || it->second.max > current_time)
            continue;

        auto [expired_it, inserted] = result.emplace(ttl.index_name, it->second.max);
        if (!inserted)
            expired_it->second = std::min(expired_it->second, it->second.max);
    }
    return result;
}

ExpiredIndexFiles getExpiredIndexFiles(
    const IMergeTreeDataPart & part, const std::shared_ptr<const StorageInMemoryMetadata> & metadata_snapshot, time_t current_time)
{
    const auto expired = getIndexesWithExpiredClearTTL(*metadata_snapshot, part.ttl_infos, current_time);

    std::vector<MergeTreeIndexPtr> indexes;
    for (const auto & index : metadata_snapshot->getSecondaryIndices())
        if (expired.contains(index.name))
            indexes.push_back(MergeTreeIndexFactory::instance().get(metadata_snapshot, index, *part.storage.getSettings()));

    ExpiredIndexFiles result;
    if (indexes.empty())
        return result;

    const auto & storage = part.getDataPartStorage();

    /// The data and mark files of every substream of the indexes that the part holds, under both the
    /// logical name and the hashed name that `replace_long_file_name_to_hash` gives it in checksums.
    const String mrk_extension = part.getMarksFileExtension();
    for (const auto & index : indexes)
    {
        for (const auto & substream : index->getAllSubstreamsInPart(part.checksums, index->getFileName(), &storage))
        {
            const String stream_name = index->getFileName() + substream.suffix;
            for (const auto & extension : {substream.extension, mrk_extension})
            {
                result.files.insert(stream_name + extension);
                if (auto hashed = IMergeTreeDataPart::getStreamNameOrHash(stream_name, extension, part.checksums))
                    result.files.insert(*hashed + extension);
            }
        }
    }

    if (const auto * disk_storage = dynamic_cast<const DataPartStorageOnDiskBase *>(&storage))
        for (const auto & file : result.files)
            result.packed_archive_dirty |= disk_storage->isFileInPackedSkipIndicesArchive(file);

    return result;
}

bool canHardlinkFilesForIndexClear(const MergeTreeDataPartPtr & part)
{
    const auto settings = part->storage.getSettings();
    if ((*settings)[MergeTreeSetting::always_use_copy_instead_of_hardlinks])
        return false;

    /// Zero-copy replication needs hardlinked files recorded at commit. Merges don't record them.
    /// Object storage otherwise supports metadata hardlinks, and mutations rely on that.
    if (part->storage.supportsReplication()
        && (*settings)[MergeTreeSetting::allow_remote_fs_zero_copy_replication]
        && part->isStoredOnRemoteDiskWithZeroCopySupport())
        return false;

    const auto * disk_storage = dynamic_cast<const DataPartStorageOnDiskBase *>(&part->getDataPartStorage());
    return disk_storage && disk_storage->getDisk()->supportsHardLinks();
}

}
