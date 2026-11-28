#pragma once

#include "config.h"

#include <Storages/MergeTree/UniqueKey/UniqueKeyProbe.h>

#include <base/types.h>

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

/// This whole translation unit is RocksDB-only: the SST backend is unavailable
/// without RocksDB, and its only caller (the probe factory / gtests) is guarded.
#if USE_ROCKSDB

#include <rocksdb/env.h>
#include <rocksdb/sst_file_reader.h>

#include <IO/ReadSettings.h>
#include <Storages/MergeTree/IDataPartStorage.h>

namespace DB
{

class IMergeTreeDataPart;
class DeleteBitmap;

/// Reads a part's `unique_key_index.sst` through `IDataPartStorage` via a custom
/// RocksDB `Env`, so remote (S3 etc.) disks work as well. Block
/// cache is disabled: load-time validation would only pollute it.
class SSTFileReader
{
public:
    SSTFileReader(const DataPartStoragePtr & storage, const String & sst_file_name, const ReadSettings & read_settings);

    std::unique_ptr<rocksdb::Iterator> newIterator(const rocksdb::ReadOptions & options) const;

    std::shared_ptr<const rocksdb::TableProperties> getProperties() const;

    /// Delegates to `SstFileReader::VerifyChecksum` - re-reads every block.
    rocksdb::Status verifyChecksum() const;

private:
    /// Declared before `index_reader` so the Env outlives the reader.
    std::unique_ptr<rocksdb::Env> sst_env;
    std::unique_ptr<rocksdb::SstFileReader> index_reader;
};

using SSTFileReaderPtr = std::shared_ptr<SSTFileReader>;

/// `read_settings` is forwarded to every `IDataPartStorage::readFile`, so the
/// caller's filesystem-cache / remote-read policy applies to the sidecar too.
/// Never returns null. Open failure throws `CORRUPTED_DATA` (corruption -
/// caller may rebuild) or `CANNOT_OPEN_FILE` (possibly transient - keep file).
SSTFileReaderPtr openSSTReaderFromStorage(
    const DataPartStoragePtr & storage,
    const String & sst_file_name,
    const ReadSettings & read_settings);

/// `IProbeTargetPart` backed by `unique_key_index.sst`. The SST's embedded
/// bloom filter short-circuits absent keys.
///
/// TODO(unique-key): per-batch `MultiGet` + range-pruning with the parallel driver.
class SSTProbeTargetPart : public IProbeTargetPart
{
public:
    /// `part_` may be nullptr (the lookup itself needs only the reader).
    SSTProbeTargetPart(
        const IMergeTreeDataPart * part_,
        std::shared_ptr<const DeleteBitmap> pinned_bitmap_,
        SSTFileReaderPtr reader_);

    void findRowIndexBatch(
        const std::vector<std::string_view> & encoded_keys,
        std::vector<std::optional<UInt64>> & out) const override;
    bool isRowDead(UInt64 row_number) const override;
    const IMergeTreeDataPart * getUnderlyingPart() const override { return part; }

private:
    const IMergeTreeDataPart * part;
    std::shared_ptr<const DeleteBitmap> pinned_bitmap;
    SSTFileReaderPtr reader;
};

}

#endif
