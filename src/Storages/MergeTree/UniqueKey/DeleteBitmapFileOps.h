#pragma once

#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <base/types.h>

#include <memory>
#include <string>
#include <vector>

namespace DB
{

class IDataPartStorage;

/// Primitive file-I/O helpers for delete-bitmap files
namespace DeleteBitmapFileOps
{
    /// One entry from `enumerateFiles`. A settled file names its version; a staged one names the
    /// target it is for, and its version is the `creation_csn` of the part whose directory it sits
    /// in -- so `version` is meaningless until the caller reads that part.
    struct BitmapFile
    {
        BitmapVersion version = 0;
        std::string name;
        std::string staged_for_target;

        bool isStaged() const { return !staged_for_target.empty(); }
    };

    /// Every bitmap file in the part directory, in either naming form, in filesystem order.
    std::vector<BitmapFile> enumerateFiles(const IDataPartStorage & storage);

    /// Settled versions first, ascending by csn, then the staged ones by target name.
    /// Numerically, not by file name: `delete_bitmap_10.rbm` sorts before `delete_bitmap_9.rbm`,
    /// so any caller presenting these in name order starts lying the moment a table crosses
    /// csn 10. Staged files have no version of their own, so they sort last.
    void sortByVersion(std::vector<BitmapFile> & files);

    /// Atomic write: tmp-file + fsync + dir-sync rename. Not internally
    /// synchronised — caller serialises concurrent writers to the same target.
    void writeBitmapToStorage(
        IDataPartStorage & storage,
        BitmapVersion version,
        const DeleteBitmap & bitmap);

    /// Stage `bitmap` for `target_part_name` inside `storage`, the WRITER's own part directory:
    /// the name is known before the commit point, so the bytes land before a csn exists, and the
    /// directory rename that publishes the part publishes them with it.
    void writeStagedBitmapToStorage(
        IDataPartStorage & storage,
        const String & target_part_name,
        const DeleteBitmap & bitmap);

    /// Read a bitmap file by name, in either naming form. Throws `FILE_DOESNT_EXIST` if missing.
    DeleteBitmapPtr readBitmapFile(
        const IDataPartStorage & storage,
        const String & file_name,
        const String & diag_part_name = "");

    /// Settled version `version` of the part owning `storage`, or null when that file is absent --
    /// the one outcome a caller has to interpret rather than treat as a failure.
    DeleteBitmapPtr tryReadVersion(const IDataPartStorage & storage, BitmapVersion version);

    /// The bitmap `storage`'s part stages for `target_part_name`, or null when that file is absent.
    DeleteBitmapPtr tryReadStagedFor(const IDataPartStorage & storage, const String & target_part_name);

    /// Move a staged bitmap into its target: write it there as `delete_bitmap_<version>.rbm`, then
    /// unlink the staged copy. Publish before unlink, so a crash between them leaves the version in
    /// both places rather than in neither, and a version the target already has is left alone -- which
    /// is how a settle retried past a crash converges.
    void settleStagedFile(
        IDataPartStorage & owner,
        const String & staged_file_name,
        IDataPartStorage & target,
        BitmapVersion version,
        const String & diag_owner_name = "");

    /// Unlink a staged bitmap without publishing it. Only for a target that is provably gone, whose
    /// kills nothing can observe; for any other target this destroys them.
    void removeStagedFile(IDataPartStorage & owner, const String & staged_file_name);

    /// Unlink one settled version, reporting whether it was there -- an indexed version can already
    /// have lost its file.
    bool removeVersion(IDataPartStorage & storage, BitmapVersion version);
}

}
