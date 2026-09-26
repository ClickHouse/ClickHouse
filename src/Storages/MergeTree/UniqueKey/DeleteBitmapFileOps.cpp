#include <Storages/MergeTree/UniqueKey/DeleteBitmapFileOps.h>

#include <Disks/IDisk.h>
#include <Storages/MergeTree/IDataPartStorage.h>

#include <IO/ReadSettings.h>
#include <IO/copyData.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteSettings.h>

#include <Common/Exception.h>

#include <algorithm>
#include <tuple>

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
}

namespace DeleteBitmapFileOps
{

std::string BitmapFile::toString() const
{
    return isCarried() ? fmt::format("{}_for_{}", version, target) : fmt::format("for_{}", target);
}

void sortByVersion(std::vector<BitmapFile> & files)
{
    std::sort(files.begin(), files.end(), [](const BitmapFile & l, const BitmapFile & r)
    {
        /// One part can hold several versions of one target: its own, and any it carried in.
        return std::tie(l.target, l.version) < std::tie(r.target, r.version);
    });
}

std::vector<BitmapFile> enumerateFiles(const IDataPartStorage & storage)
{
    std::vector<BitmapFile> result;
    for (auto it = storage.iterate(); it->isValid(); it->next())
    {
        const auto & file_name = it->name();
        if (DeleteBitmap::isStagedBitmapFile(file_name))
            result.push_back({/*version=*/0, DeleteBitmap::parseStagedTargetFromFileName(file_name)});
        else if (DeleteBitmap::isCarriedBitmapFile(file_name))
        {
            auto carried = DeleteBitmap::parseCarriedFromFileName(file_name);
            result.push_back({carried.csn, std::move(carried.target_part_name)});
        }
    }
    return result;
}

namespace
{

/// Both writers land the bytes the same way; only what produces them differs.
template <typename WriteBody>
MergeTreeDataPartChecksum writeUnderName(IDataPartStorage & storage, const String & final_name, WriteBody && write_body)
{
    const String tmp_name = final_name + ".tmp";

    /// Clear any stale `.tmp` from a previous failed attempt.
    storage.removeFileIfExists(tmp_name);

    MergeTreeDataPartChecksum checksum;
    {
        WriteSettings write_settings;
        auto buf = storage.writeFile(tmp_name, /*buf_size=*/4096, WriteMode::Rewrite, write_settings);
        HashingWriteBuffer hashing(*buf);
        write_body(hashing);
        hashing.finalize();
        checksum = {hashing.count(), hashing.getHash()};
        /// fsync the tmp file before rename: a power loss after rename but before flush would otherwise resurrect deleted rows.
        buf->sync();
        buf->finalize();
    }

    /// Dir-sync guard makes the rename itself durable.
    auto sync_guard = storage.getDirectorySyncGuard();
    storage.replaceFile(tmp_name, final_name);

    return checksum;
}

DeleteBitmapPtr openAndDeserialize(const IDataPartStorage & storage, const String & file_name)
{
    ReadSettings read_settings;
    auto buf = storage.readFile(file_name, read_settings, /*read_hint=*/{});
    return DeleteBitmap::deserialize(*buf);
}

/// Opens without an `existsFile` first, and catches instead: a check-then-read is a TOCTOU
/// against anything that can make the file vanish between the two calls, and the check would
/// pass while the open still throws from the disk layer.
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

MergeTreeDataPartChecksum stageBitmap(
    IDataPartStorage & holder,
    const BitmapFile & file,
    const DeleteBitmap & bitmap)
{
    return writeUnderName(holder, file.fileName(), [&](WriteBuffer & buf) { bitmap.serialize(buf); });
}

MergeTreeDataPartChecksum carryBitmap(
    const IDataPartStorage & from,
    const BitmapFile & from_file,
    IDataPartStorage & to,
    const BitmapFile & to_file)
{
    /// Without one the name reads as a staged bitmap, whose version comes from whoever holds it.
    chassert(to_file.isCarried());

    ReadSettings read_settings;
    auto in = from.readFile(from_file.fileName(), read_settings, /*read_hint=*/{});
    return writeUnderName(to, to_file.fileName(), [&](WriteBuffer & buf) { copyData(*in, buf); });
}

DeleteBitmapPtr tryReadBitmap(const IDataPartStorage & holder, const BitmapFile & file)
{
    return tryReadBitmapFile(holder, file.fileName());
}

}

}
