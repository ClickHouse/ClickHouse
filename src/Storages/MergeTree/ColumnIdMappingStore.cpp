#include <Storages/MergeTree/ColumnIdMappingStore.h>

#include <Core/MergeTreeSerializationEnums.h>
#include <Core/Settings.h>
#include <Disks/IDisk.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace Setting
{
    extern const SettingsBool fsync_metadata;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsMergeTreeSerializationInfoVersion serialization_info_version;
}

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

DiskColumnIdMappingStore::DiskColumnIdMappingStore(MergeTreeData & data_, LoggerPtr log_)
    : data(data_)
    , log(std::move(log_))
{
}

std::optional<ColumnIdMapping> DiskColumnIdMappingStore::load(bool attach)
{
    const auto column_ids_path = fs::path(data.getRelativeDataPath()) / MergeTreeData::COLUMN_IDS_FILE_NAME;

    const DiskPtr authoritative_disk = getAuthoritativeDisk(data.getStoragePolicy());
    std::unique_ptr<ReadBufferFromFileBase> mapping_buf;
    if (!authoritative_disk->isBroken())
        mapping_buf = authoritative_disk->readFileIfExists(column_ids_path, getReadSettings());

    if (!mapping_buf)
    {
        /// Not rebuildable: DROP + re-ADD of one name makes the two columns' files indistinguishable
        /// by column ID alone, so a rebuild from the schema could mix them.
        if (attach
            && (*data.getSettings())[MergeTreeSetting::serialization_info_version] == MergeTreeSerializationInfoVersion::WITH_COLUMN_IDS)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "Table {} has `serialization_info_version = 'with_column_ids'` but `{}` cannot be read "
                "from disk `{}` and cannot be rebuilt. Restore the table from backup.",
                data.getStorageID().getNameForLogs(), column_ids_path.string(), authoritative_disk->getName());
        return {};
    }

    auto loaded_mapping = ColumnIdMapping::deserialize(*mapping_buf);
    skipWhitespaceIfAny(*mapping_buf);
    assertEOF(*mapping_buf);
    return loaded_mapping;
}

/// One copy, not one per disk like `format_version.txt`: parts are self-describing (`columns.txt`
/// stores the IDs), so no disk ever reads its own copy of the mapping.
DiskPtr DiskColumnIdMappingStore::getAuthoritativeDisk(const StoragePolicyPtr & policy) const
{
    const auto & disks = policy->getDisks();
    if (disks.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Storage policy for table {} has no disks to hold `{}`",
            data.getStorageID().getNameForLogs(), MergeTreeData::COLUMN_IDS_FILE_NAME);
    return disks.front();
}

void DiskColumnIdMappingStore::store(const ColumnIdMapping & mapping, const StoragePolicyPtr & target_policy)
{
    const fs::path table_path = data.getRelativeDataPath();
    const auto column_ids_path = table_path / MergeTreeData::COLUMN_IDS_FILE_NAME;
    const auto column_ids_tmp_path = table_path / (String(MergeTreeData::COLUMN_IDS_FILE_NAME) + ".tmp");

    auto disk = getAuthoritativeDisk(target_policy);
    if (disk->isBroken() || disk->isReadOnly() || disk->isWriteOnce())
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Cannot store `{}` for table {}: the storage policy's first disk `{}` is not writable. "
            "Put a writable disk first in the policy.",
            MergeTreeData::COLUMN_IDS_FILE_NAME, data.getStorageID().getNameForLogs(), disk->getName());

    /// A crash between the write and the replace leaves either the previous file intact or only the
    /// `.tmp` behind, so a reader never sees a torn one.
    try
    {
        {
            auto buf = disk->writeFile(column_ids_tmp_path, 4096, WriteMode::Rewrite, data.getContext()->getWriteSettings());
            mapping.serialize(*buf);
            buf->finalize();
            if (data.getContext()->getSettingsRef()[Setting::fsync_metadata])
                buf->sync();
        }
        disk->replaceFile(column_ids_tmp_path, column_ids_path);
    }
    catch (...)
    {
        try
        {
            disk->removeFileIfExists(column_ids_tmp_path);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
        throw;
    }

    /// Best-effort: drop any stale copy on the other disks (e.g. left by the old
    /// multi-disk format) so exactly one authoritative copy exists.
    for (const auto & other : target_policy->getDisks())
    {
        if (other->getName() == disk->getName())
            continue;
        if (other->isBroken() || other->isReadOnly() || other->isWriteOnce())
            continue;
        try
        {
            other->removeFileIfExists(column_ids_path);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to remove stale column ID mapping copy");
        }
    }
}

/// Swept off every disk, not just the one `store` wrote to: a surviving inactive mapping makes a
/// post-restart ALTER take the already-active path while parts still hold logical filenames.
void DiskColumnIdMappingStore::remove() noexcept
{
    try
    {
        const auto column_ids_path = fs::path(data.getRelativeDataPath()) / MergeTreeData::COLUMN_IDS_FILE_NAME;
        for (const auto & disk : data.getStoragePolicy()->getDisks())
        {
            if (disk->isBroken() || disk->isReadOnly() || disk->isWriteOnce())
                continue;
            try
            {
                disk->removeFileIfExists(column_ids_path);
            }
            catch (...)
            {
                tryLogCurrentException(log, "Failed to remove column ID mapping from disk");
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to resolve the disks holding the column ID mapping");
    }
}

}
