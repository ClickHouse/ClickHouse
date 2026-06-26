#pragma once

#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <base/types.h>

#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace DB
{

class IDataPartStorage;

/// UNIQUE KEY — primitive file-I/O helpers for `delete_bitmap_{csn}.rbm`
/// sidecar files. `version` in this API is the bitmap's csn; the
/// filename is materialised one-to-one as `delete_bitmap_{version}.rbm`.
namespace DeleteBitmapFileOps
{
    /// One entry from `enumerateFiles`: the parsed `version` and the
    /// canonical file name.
    struct BitmapFile
    {
        BitmapVersion version;
        std::string name;
    };

    /// Every `delete_bitmap_{N}.rbm` file in the part directory parsed to
    /// (version, name). Filesystem iteration order.
    std::vector<BitmapFile> enumerateFiles(const IDataPartStorage & storage);

    /// Entry with the highest version, or std::nullopt if empty.
    std::optional<BitmapFile> pickHighest(const std::vector<BitmapFile> & files);

    /// Atomic write: tmp-file + fsync + dir-sync rename. Not internally
    /// synchronised — caller serialises concurrent writers to the same target.
    void writeBitmapToStorage(
        IDataPartStorage & storage,
        BitmapVersion version,
        const DeleteBitmap & bitmap,
        const String & diag_part_name = "");

    /// Read `delete_bitmap_{version}.rbm`. Throws `FILE_DOESNT_EXIST` if missing.
    std::shared_ptr<DeleteBitmap> readBitmapFromStorage(
        const IDataPartStorage & storage,
        BitmapVersion version,
        const String & diag_part_name = "");

    /// Highest version on disk, or 0 if none.
    BitmapVersion getLastVersionFromStorage(const IDataPartStorage & storage);
}

}
