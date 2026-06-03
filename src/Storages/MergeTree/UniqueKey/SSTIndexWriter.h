#pragma once

#include "config.h"

#include <base/types.h>
#include <Core/Block.h>
#include <Core/Names.h>
#include <Columns/IColumn.h>

#include <memory>
#include <string>
#include <string_view>


namespace DB
{

class IDataPartStorage;


/// Streaming writer for the per-part UNIQUE KEY dense-index SST.
///
/// Output: `unique_key_index.sst`, a single-file RocksDB SST containing
/// `(encoded_key -> row_number_be32)` entries with an embedded ~1% FPR
/// bloom filter (`NewBloomFilterPolicy(10)`). Atomic write: stages to
/// `unique_key_index.sst.tmp`, renames on `finalizeToStorage`. The dtor
/// best-effort removes a leftover `.tmp` on early drop.
///
/// Streaming `addEncoded` requires strictly-increasing encoded-key
/// order (RocksDB `SstFileWriter::Put` invariant). Static helpers below
/// cover the common batch-producer shapes.
class SSTIndexWriter
{
public:
    static const char * const FILE_NAME;
    static const char * const TMP_FILE_NAME;

    /// Bloom filter bits-per-key. 10 → ~1% FPR.
    static constexpr double BLOOM_BITS_PER_KEY = 10.0;

    /// Build an SST from a Block whose UK columns are in encoded-key order
    /// after applying `permutation` (or block order if null). O(N).
    static UInt64 writeFromBlock(
        IDataPartStorage & part_storage,
        const Block & block,
        const Names & unique_key_column_names,
        const IColumn::Permutation * permutation,
        size_t max_encoded_size);

    /// Non-prefix UK path: sort source rows by UK columns via
    /// `stableGetPermutation`, batch-encode in UK order via
    /// `encodeBlock`, Put each entry. `permutation`, if non-null, is the
    /// caller's part-offset permutation (so SST `row_number = part_offset`).
    static UInt64 writeFromBlockUnsorted(
        IDataPartStorage & part_storage,
        const Block & block,
        const Names & unique_key_column_names,
        const IColumn::Permutation * permutation,
        size_t max_encoded_size);

    /// Caller must call `finish()` (or `finalizeToStorage`, which does it
    /// internally) before drop. Dropping without finishing leaks the
    /// underlying RocksDB writer state; the dtor only best-effort removes
    /// the `.tmp`.
    explicit SSTIndexWriter(IDataPartStorage & part_storage);
    ~SSTIndexWriter();

    SSTIndexWriter(const SSTIndexWriter &) = delete;
    SSTIndexWriter & operator=(const SSTIndexWriter &) = delete;

    /// Put one already-encoded key.
    void addEncoded(const std::string_view & encoded_key, UInt32 row_number);

    UInt64 entriesAdded() const { return entries_added; }

    /// Finalize the SST and atomic-rename into place. Empty input → no
    /// SST file produced; returns 0.
    UInt64 finalizeToStorage();

    /// Close the underlying RocksDB writer. Throws on real Finish failure;
    /// `InvalidArgument` (zero-`Put` case) is treated as success.
    /// Idempotent.
    void finish();

private:
    struct Impl;
    std::unique_ptr<Impl> impl;

    IDataPartStorage & part_storage;
    UInt64 entries_added = 0;
    [[maybe_unused]] bool finalized = false;
};

}
