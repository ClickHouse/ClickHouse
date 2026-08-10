#pragma once

#include "config.h"
#include <Common/VectorWithMemoryTracking.h>

#include <Interpreters/Context_fwd.h>

#include <base/types.h>
#include <Core/Block.h>
#include <Core/Names.h>
#include <Columns/IColumn.h>

#include <memory>
#include <string>
#include <string_view>
#include <vector>


namespace DB
{

class IDataPartStorage;


/// Streaming writer for the per-part UNIQUE KEY dense-index SST.
///
/// Output: `unique_key_index.sst`, a single-file RocksDB SST containing
/// `(encoded_key -> row_number_be32)` entries with an embedded ~1% FPR
/// bloom filter (`NewBloomFilterPolicy(10)`). Instead of staging a local
/// temp file, RocksDB is driven through a custom `rocksdb::Env` whose
/// writable file forwards every byte into the part storage's
/// `WriteBuffer` (`part_storage.writeFile`). The target file is opened
/// lazily on the first `addEncoded`, so an empty input produces no
/// `.sst` file.
///
/// Streaming `addEncoded` requires strictly-increasing encoded-key
/// order (RocksDB `SstFileWriter::Put` invariant). Static helpers below
/// cover the common batch-producer shapes.
class SSTIndexWriter
{
public:
    static const char * const FILE_NAME;

    /// Bloom filter bits-per-key. 10 → ~1% FPR.
    static constexpr double BLOOM_BITS_PER_KEY = 10.0;

    /// Dense-index entry point. Empty `uk_names` → no-op (returns 0). Otherwise
    /// dispatches to `writeFromBlock` when the block is already sorted by UK
    /// (UK is a non-Nullable ascending prefix of ORDER BY), else
    /// `writeFromBlockUnsorted` (re-sorts ascending).
    static UInt64 write(
        IDataPartStorage & part_storage,
        const Block & block,
        const Names & uk_names,
        const Names & sort_names,
        const VectorWithMemoryTracking<bool> & sort_reverse_flags,
        const IColumn::Permutation * permutation,
        UInt64 max_encoded_size,
        ContextPtr context);

    /// Build an SST from a Block whose UK columns are in encoded-key order
    /// after applying `permutation` (or block order if null). O(N).
    /// `context` supplies the `WriteSettings` for `part_storage.writeFile`.
    static UInt64 writeFromBlock(
        IDataPartStorage & part_storage,
        const Block & block,
        const Names & unique_key_column_names,
        const IColumn::Permutation * permutation,
        size_t max_encoded_size,
        ContextPtr context);

    /// Non-prefix UK path: sort source rows by UK columns via
    /// `stableGetPermutation`, batch-encode in UK order via
    /// `encodeBlock`, Put each entry. `permutation`, if non-null, is the
    /// caller's part-offset permutation (so SST `row_number = part_offset`).
    static UInt64 writeFromBlockUnsorted(
        IDataPartStorage & part_storage,
        const Block & block,
        const Names & unique_key_column_names,
        const IColumn::Permutation * permutation,
        size_t max_encoded_size,
        ContextPtr context);

    /// `finalizeToStorage` is the only commit point — it closes the RocksDB
    /// writer and finalizes the part-storage `WriteBuffer`. Dropping the
    /// writer without calling it cancels the (unfinalized) `WriteBuffer`,
    /// so no partial `.sst` is committed.
    explicit SSTIndexWriter(IDataPartStorage & part_storage, ContextPtr context);
    ~SSTIndexWriter();

    SSTIndexWriter(const SSTIndexWriter &) = delete;
    SSTIndexWriter & operator=(const SSTIndexWriter &) = delete;

    /// Put one already-encoded key.
    void addEncoded(const std::string_view & encoded_key, UInt32 row_number);

    UInt64 entriesAdded() const { return entries_added; }

    /// Finalize the SST: close the RocksDB writer, then finalize the
    /// part-storage `WriteBuffer`. Empty input → no SST file produced;
    /// returns 0.
    UInt64 finalizeToStorage();

private:
#if USE_ROCKSDB
    /// Open the part-storage target file and the RocksDB writer on demand,
    /// the first time an entry is added. Idempotent. Deferred (not done in
    /// the constructor) so that an empty input never materializes a `.sst`.
    void openOutputStreamOnFirstEntry();
#endif

    /// Close the underlying RocksDB writer, flushing the SST footer into the
    /// part-storage `WriteBuffer`. Only called from `finalizeToStorage`.
    void finish();

    struct Impl;
    std::unique_ptr<Impl> impl;

    IDataPartStorage & part_storage;
    UInt64 entries_added = 0;
    [[maybe_unused]] bool finalized = false;
};

}
