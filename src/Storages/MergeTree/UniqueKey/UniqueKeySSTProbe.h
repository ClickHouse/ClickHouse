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

#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/UniqueKey/UniqueKeyIndexCache.h>

#include <rocksdb/env.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/sst_file_reader.h>
#include <rocksdb/table.h>
#include <rocksdb/table_properties.h>

namespace DB
{

class IMergeTreeDataPart;
class DeleteBitmap;
class ReadBufferFromFileBase;

/// Opens a RocksDB SST read through a ClickHouse `ReadBuffer` (a custom `Env`).
/// Storage mode reads via `IDataPartStorage` (a remote sidecar read in place);
/// injected mode wraps one external `SeekableReadBuffer` the caller keeps alive.
/// `sst_env` is declared before `index_reader` so it outlives it. A constructed
/// object is open: `Open` failure throws `CANNOT_OPEN_FILE`. Implementation in .cpp.
class SSTFileReader
{
public:
    SSTFileReader(const DataPartStoragePtr & storage, const String & sst_file_name, const UniqueKeyIndexCachePtr & block_cache);

    /// Injected mode: `buffer` is not owned and must outlive this reader. The file
    /// name is empty - the injected file system ignores it and serves `buffer`.
    SSTFileReader(SeekableReadBuffer & buffer, uint64_t file_size, const UniqueKeyIndexCachePtr & block_cache);

    std::unique_ptr<rocksdb::Iterator> newIterator(const rocksdb::ReadOptions & options) const;

    std::shared_ptr<const rocksdb::TableProperties> getProperties() const;

private:
    void init(const String & file_name, const UniqueKeyIndexCachePtr & block_cache);

    /// Declared before `index_reader` so the Env outlives the reader.
    std::unique_ptr<rocksdb::Env> sst_env;
    std::unique_ptr<rocksdb::SstFileReader> index_reader;
};

using SSTFileReaderPtr = std::shared_ptr<SSTFileReader>;

/// Opened-reader handle for a part's `unique_key_index.sst`. Owns the
/// `SSTFileReader` (and thus the Env + RocksDB reader), so the buffer outlives the
/// open call.
///
/// TODO(unique-key): route handles through a reader cache (open once, share across probes).
struct SSTReaderHandle
{
    SSTFileReaderPtr reader;
};

/// Open through `DataPartStorage`: a remote sidecar (S3 etc.) is read in place.
/// `block_cache` (usually the shared `UniqueKeyIndexCache`) is wired into the
/// table options; null means no block cache. A returned reader is open; open
/// failure throws `CANNOT_OPEN_FILE`.
SSTFileReaderPtr openSSTReaderFromStorage(
    const DataPartStoragePtr & storage,
    const String & sst_file_name,
    const UniqueKeyIndexCachePtr & block_cache = {});

/// Open over an externally managed `ReadBuffer` (any source, not just a part
/// storage). The buffer is not owned and must outlive the returned reader. The
/// file size is taken from `buffer.tryGetFileSize()`; a buffer that cannot report
/// it fails closed (throws). A returned reader is open; open failure throws.
SSTFileReaderPtr openSSTReaderFromBuffer(
    ReadBufferFromFileBase & buffer,
    const UniqueKeyIndexCachePtr & block_cache = {});

/// UNIQUE KEY `IProbeTargetPart` backed by an opened `unique_key_index.sst`.
/// The driver owns encoding and hands over encoded bytes; the SST's embedded
/// bloom filter short-circuits absent keys inside RocksDB.
///
/// TODO(unique-key): per-batch `MultiGet` + `keyRangeIntersects` range-pruning with the parallel driver.
class SSTProbeTargetPart : public IProbeTargetPart
{
public:
    /// `part_` may be nullptr (surfaced through `getUnderlyingPart`); the lookup
    /// itself needs only the reader handle.
    SSTProbeTargetPart(
        const IMergeTreeDataPart * part_,
        std::shared_ptr<const DeleteBitmap> pinned_bitmap_,
        SSTReaderHandle handle_);

    void findRowIndexBatch(
        const std::vector<std::string_view> & encoded_keys,
        std::vector<std::optional<UInt64>> & out) const override;
    bool isRowDead(UInt64 row_number) const override;
    const IMergeTreeDataPart * getUnderlyingPart() const override { return part; }

private:
    const IMergeTreeDataPart * part;
    std::shared_ptr<const DeleteBitmap> pinned_bitmap;
    SSTReaderHandle handle;
};

}

#endif
