#pragma once

#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <base/types.h>

#include <string>
#include <vector>

namespace DB
{

class IDataPartStorage;

/// Primitive file-I/O helpers for delete-bitmap files
namespace DeleteBitmapFileOps
{
    /// One bitmap file: which part it kills into, and at which version. Every file names its
    /// target; only a carried one names its version too, a staged one taking its holder's. This
    /// type is what knows the difference, so the name on disk, the `system.parts` string and the
    /// sort order all come from here.
    struct BitmapFile
    {
        BitmapVersion version = 0;
        std::string target;

        /// No committed transaction has csn 0, so a recorded version can only be an inherited one.
        bool isCarried() const { return version != 0; }

        std::string fileName() const
        {
            return isCarried()
                ? DeleteBitmap::fileNameForCarriedTarget(version, target)
                : DeleteBitmap::fileNameForStagedTarget(target);
        }

        /// The `system.parts` form: the name without the prefix and suffix all of them share.
        std::string toString() const;

        bool operator==(const BitmapFile & other) const = default;
    };

    /// Every bitmap file in the part directory, in filesystem order. Anything else there is
    /// somebody else's file.
    std::vector<BitmapFile> enumerateFiles(const IDataPartStorage & storage);

    /// By target, then by version -- numerically, so a listing does not start putting
    /// `delete_bitmap_10_for_x` before `delete_bitmap_9_for_x` once a table crosses csn 10.
    void sortByVersion(std::vector<BitmapFile> & files);

    /// Both writes are atomic (tmp-file + fsync + dir-sync rename) and neither is internally
    /// synchronised: the caller serialises concurrent writers to one target. Both land before the
    /// commit point, so the rename that publishes the part publishes them with it.
    ///
    /// Staged: `holder` wrote this itself, so the version is `holder`'s own csn -- which does not
    /// exist yet, and is left out of the name.
    void stageBitmap(
        IDataPartStorage & holder,
        const String & target_part_name,
        const DeleteBitmap & bitmap);

    /// Copy a bitmap file under a carried name. The version travels with the bytes: renumbering
    /// it to the destination's csn would land a copy newest-by-number and stale-by-content.
    void carryBitmap(
        const IDataPartStorage & from,
        const BitmapFile & from_file,
        IDataPartStorage & to,
        const BitmapFile & to_file);

    /// Null when the file is absent -- the one outcome a caller interprets rather than treats as
    /// a failure.
    DeleteBitmapPtr tryReadBitmap(const IDataPartStorage & holder, const BitmapFile & file);

    /// Reports whether the file was there: an indexed version can already have lost it. Only past
    /// the GC floor, which is what makes a removal unobservable where an addition would not be.
    bool removeBitmapFile(IDataPartStorage & holder, const BitmapFile & file);
}

}
