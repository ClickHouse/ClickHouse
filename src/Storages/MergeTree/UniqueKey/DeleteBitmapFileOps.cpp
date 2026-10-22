#include <Storages/MergeTree/UniqueKey/DeleteBitmapFileOps.h>

#include <Disks/IDisk.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

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

/// Both writers land the bytes the same way; only what produces them differs. The file goes
/// straight to its final name: it is written into a part nothing can see before `publish`, and
/// each name at most once, so a crash leaves a torn file only in a temporary part. The file and
/// then the directory are fsynced: a published bitmap lost on power failure resurrects its rows.
template <typename WriteBody>
MergeTreeDataPartChecksum writeBitmapFile(IDataPartStorage & storage, const String & file_name, WriteBody && write_body)
{
    /// Syncs the directory on scope exit, after the file is written and synced.
    auto sync_guard = storage.getDirectorySyncGuard();
    WriteSettings write_settings;
    auto buf = storage.writeFile(file_name, /*buf_size=*/4096, WriteMode::Rewrite, write_settings);
    HashingWriteBuffer hashing(*buf);
    write_body(hashing);
    hashing.finalize();
    const MergeTreeDataPartChecksum checksum{hashing.count(), hashing.getHash()};
    buf->sync();
    buf->finalize();

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
    return writeBitmapFile(holder, file.fileName(), [&](WriteBuffer & buf) { bitmap.serialize(buf); });
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
    return writeBitmapFile(to, to_file.fileName(), [&](WriteBuffer & buf) { copyData(*in, buf); });
}

DeleteBitmapPtr tryReadBitmap(const IDataPartStorage & holder, const BitmapFile & file)
{
    return tryReadBitmapFile(holder, file.fileName());
}

std::vector<BitmapFile> removeClonedBitmaps(IMergeTreeDataPart & clone)
{
    auto & storage = clone.getDataPartStorage();
    auto files = enumerateFiles(storage);
    if (files.empty())
        return files;

    for (const auto & file : files)
    {
        storage.removeFile(file.fileName());
        clone.checksums.remove(file.fileName());
    }

    {
        auto out = storage.writeFile("checksums.txt", /*buf_size=*/4096, WriteSettings{});
        clone.checksums.write(*out);
        out->sync();
        out->finalize();
    }

    clone.setBytesOnDisk(clone.checksums.getTotalSizeOnDisk());
    return files;
}

}

}
