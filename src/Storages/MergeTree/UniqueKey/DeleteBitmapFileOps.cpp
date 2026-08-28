#include <Storages/MergeTree/UniqueKey/DeleteBitmapFileOps.h>

#include <Disks/IDisk.h>
#include <Storages/MergeTree/IDataPartStorage.h>

#include <IO/ReadSettings.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteSettings.h>

#include <Common/Exception.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
}

namespace DeleteBitmapFileOps
{

void sortByVersion(std::vector<BitmapFile> & files)
{
    std::sort(files.begin(), files.end(), [](const BitmapFile & l, const BitmapFile & r)
    {
        if (l.isStaged() != r.isStaged())
            return r.isStaged();
        return l.isStaged() ? l.staged_for_target < r.staged_for_target : l.version < r.version;
    });
}

std::vector<BitmapFile> enumerateFiles(const IDataPartStorage & storage)
{
    std::vector<BitmapFile> result;
    for (auto it = storage.iterate(); it->isValid(); it->next())
    {
        const auto & file_name = it->name();
        if (DeleteBitmap::isDeleteBitmapFile(file_name))
            result.push_back({DeleteBitmap::parseCSNFromFileName(file_name), file_name, /*staged_for_target=*/{}});
        else if (DeleteBitmap::isStagedBitmapFile(file_name))
            result.push_back({/*version=*/0, file_name, DeleteBitmap::parseStagedTargetFromFileName(file_name)});
    }
    return result;
}

namespace
{

void writeBitmapUnderName(IDataPartStorage & storage, const String & final_name, const DeleteBitmap & bitmap)
{
    const String tmp_name = final_name + ".tmp";

    /// Clear any stale `.tmp` from a previous failed attempt.
    storage.removeFileIfExists(tmp_name);

    {
        WriteSettings write_settings;
        auto buf = storage.writeFile(tmp_name, /*buf_size=*/4096, WriteMode::Rewrite, write_settings);
        bitmap.serialize(*buf);
        /// fsync the tmp file before rename: a power loss after rename but before flush would otherwise resurrect deleted rows.
        buf->sync();
        buf->finalize();
    }

    /// Dir-sync guard makes the rename itself durable.
    auto sync_guard = storage.getDirectorySyncGuard();
    storage.replaceFile(tmp_name, final_name);
}

DeleteBitmapPtr openAndDeserialize(const IDataPartStorage & storage, const String & file_name)
{
    ReadSettings read_settings;
    auto buf = storage.readFile(file_name, read_settings, /*read_hint=*/{});
    return DeleteBitmap::deserialize(*buf);
}

/// Opens without an `existsFile` first, and catches instead: a concurrent settle publishing and
/// unlinking is exactly the case these callers exist to survive, and a check-then-read would race
/// with it -- the check would pass and the open would then throw from the disk layer.
DeleteBitmapPtr tryReadBitmapFile(const IDataPartStorage & storage, const String & file_name)
{
    try
    {
        return openAndDeserialize(storage, file_name);
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::FILE_DOESNT_EXIST)
            throw;
        return nullptr;
    }
}

}

void writeBitmapToStorage(
    IDataPartStorage & storage,
    BitmapVersion version,
    const DeleteBitmap & bitmap)
{
    writeBitmapUnderName(storage, DeleteBitmap::fileNameForCSN(version), bitmap);
}

void writeStagedBitmapToStorage(
    IDataPartStorage & storage,
    const String & target_part_name,
    const DeleteBitmap & bitmap)
{
    writeBitmapUnderName(storage, DeleteBitmap::fileNameForStagedTarget(target_part_name), bitmap);
}

DeleteBitmapPtr readBitmapFile(
    const IDataPartStorage & storage,
    const String & file_name,
    const String & diag_part_name)
{
    /// Checked first only to name the part in the message; the callers that tolerate a missing file
    /// go through `tryReadBitmapFile`, which does not pre-check.
    if (!storage.existsFile(file_name))
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST,
            "Delete bitmap file '{}' does not exist in part '{}'",
            file_name, diag_part_name);

    return openAndDeserialize(storage, file_name);
}

DeleteBitmapPtr tryReadVersion(const IDataPartStorage & storage, BitmapVersion version)
{
    return tryReadBitmapFile(storage, DeleteBitmap::fileNameForCSN(version));
}

DeleteBitmapPtr tryReadStagedFor(const IDataPartStorage & storage, const String & target_part_name)
{
    return tryReadBitmapFile(storage, DeleteBitmap::fileNameForStagedTarget(target_part_name));
}

void settleStagedFile(
    IDataPartStorage & owner,
    const String & staged_file_name,
    IDataPartStorage & target,
    BitmapVersion version,
    const String & diag_owner_name)
{
    const String published_name = DeleteBitmap::fileNameForCSN(version);
    if (!target.existsFile(published_name))
    {
        const auto bitmap = readBitmapFile(owner, staged_file_name, diag_owner_name);
        writeBitmapUnderName(target, published_name, *bitmap);
    }

    owner.removeFileIfExists(staged_file_name);
}

void removeStagedFile(IDataPartStorage & owner, const String & staged_file_name)
{
    owner.removeFileIfExists(staged_file_name);
}

bool removeVersion(IDataPartStorage & storage, BitmapVersion version)
{
    const String file_name = DeleteBitmap::fileNameForCSN(version);
    const bool existed = storage.existsFile(file_name);
    storage.removeFileIfExists(file_name);
    return existed;
}

}

}
