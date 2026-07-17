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

std::vector<std::pair<UInt64, std::string>> enumerateFiles(const IDataPartStorage & storage)
{
    std::vector<std::pair<UInt64, std::string>> result;
    for (auto it = storage.iterate(); it->isValid(); it->next())
    {
        const auto & file_name = it->name();
        if (DeleteBitmap::isDeleteBitmapFile(file_name))
            result.emplace_back(DeleteBitmap::parseBlockNumberFromFileName(file_name), file_name);
    }
    return result;
}

std::optional<std::pair<UInt64, std::string>> pickHighest(
    const std::vector<std::pair<UInt64, std::string>> & files)
{
    if (files.empty())
        return std::nullopt;

    auto it = std::max_element(
        files.begin(), files.end(),
        [](const auto & a, const auto & b) { return a.first < b.first; });
    return *it;
}

void writeBitmapToStorage(
    IDataPartStorage & storage,
    UInt64 version,
    const DeleteBitmap & bitmap,
    const String & /*diag_part_name*/)
{
    const String final_name = DeleteBitmap::fileNameForBlockNumber(version);
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

std::shared_ptr<DeleteBitmap> readBitmapFromStorage(
    const IDataPartStorage & storage,
    UInt64 version,
    const String & diag_part_name)
{
    const String file_name = DeleteBitmap::fileNameForBlockNumber(version);
    if (!storage.existsFile(file_name))
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST,
            "Delete bitmap file '{}' does not exist in part '{}'",
            file_name, diag_part_name);

    ReadSettings read_settings;
    auto buf = storage.readFile(file_name, read_settings, /*read_hint=*/{});
    auto deserialized = DeleteBitmap::deserialize(*buf);
    return std::shared_ptr<DeleteBitmap>(deserialized.release());
}

UInt64 getCurrentVersionFromStorage(const IDataPartStorage & storage)
{
    auto files = enumerateFiles(storage);
    auto highest = pickHighest(files);
    if (!highest)
        return 0;
    /// Block number from filename == version of the bitmap on disk.
    return highest->first;
}

}

}
