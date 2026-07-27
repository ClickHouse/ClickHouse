#pragma once

#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <base/types.h>

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace DB
{

class IDataPartStorage;

/// UNIQUE KEY — primitive file-I/O helpers for `delete_bitmap_{N}.rbm`
/// sidecar files. The on-disk filename encodes a `block_number`; the
/// public write/read API speaks a caller-provided `version`, materialised
/// one-to-one as `delete_bitmap_{version}.rbm`.
namespace DeleteBitmapFileOps
{
    /// Every `delete_bitmap_{N}.rbm` file in the part directory parsed to
    /// (block_number, name). Filesystem iteration order.
    std::vector<std::pair<UInt64, std::string>> enumerateFiles(const IDataPartStorage & storage);

    /// Entry with the highest block number, or std::nullopt if empty.
    std::optional<std::pair<UInt64, std::string>> pickHighest(
        const std::vector<std::pair<UInt64, std::string>> & files);

    /// Atomic write: tmp-file + fsync + dir-sync rename. Not internally
    /// synchronised — caller serialises concurrent writers to the same target.
    void writeBitmapToStorage(
        IDataPartStorage & storage,
        UInt64 version,
        const DeleteBitmap & bitmap,
        const String & diag_part_name = "");

    /// Read `delete_bitmap_{version}.rbm`. Throws `FILE_DOESNT_EXIST` if missing.
    std::shared_ptr<DeleteBitmap> readBitmapFromStorage(
        const IDataPartStorage & storage,
        UInt64 version,
        const String & diag_part_name = "");

    /// Highest version on disk, or 0 if none.
    UInt64 getCurrentVersionFromStorage(const IDataPartStorage & storage);
}

}
