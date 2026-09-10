#pragma once

#include <Interpreters/AdaptiveAggregationStaging.h>
#include <Processors/Chunk.h>

namespace DB
{

/// The frozen kernel's recording of one block's misses, attached to the columns the producer
/// forwards. Records are in row order. For a count-only aggregation, consecutive misses of one
/// key collapse into one record whose multiplicity is the number of rows it stands for.
///
/// Where a record's key bytes live depends on the key type. Fixed-size keys are stored inline in
/// `key_bytes`. A string key that is the block's own key column is not copied: the producer forwards
/// that column with the arguments, so a record only remembers the key's size and the bytes are read
/// from the column when the chunk is built. Every other variable-size key is copied, because its
/// hashing-state holder may own the bytes or roll back its scratch allocation on discard.
class AdaptiveAggregationMissesInfo final : public ChunkInfo
{
public:
    enum class KeyBytes : UInt8
    {
        Fixed,
        Recorded,
        InKeyColumn,
    };

    explicit AdaptiveAggregationMissesInfo(bool use_own_memory_tracker_) : use_own_memory_tracker(use_own_memory_tracker_) {}

    Ptr clone() const override;
    size_t size() const { return hashes.size(); }
    bool empty() const { return hashes.empty(); }

    /// Fixes the layout for the block and sizes the arrays the layout uses from the previous
    /// block's counts, so recording does not grow them through repeated reallocations.
    void beginRecording(
        KeyBytes key_bytes_source_, size_t fixed_key_size_, bool constant_key_, bool counts_only, size_t records_hint, size_t key_bytes_hint)
    {
        chassert(empty());
        key_bytes_source = key_bytes_source_;
        fixed_key_size = fixed_key_size_;
        constant_key = constant_key_;
        source_rows.reserve(records_hint);
        hashes.reserve(records_hint);
        buckets.reserve(records_hint);
        if (counts_only)
            multiplicities.reserve(records_hint);
        switch (key_bytes_source)
        {
            case KeyBytes::Fixed:
                key_bytes.reserve(key_bytes_hint);
                break;
            case KeyBytes::Recorded:
                key_bytes.reserve(key_bytes_hint);
                key_offsets.reserve(records_hint + 1);
                key_offsets.push_back(0);
                break;
            case KeyBytes::InKeyColumn:
                key_sizes.reserve(records_hint);
                break;
        }
    }

    /// Records one delayed row. `key_in_column` selects the layout statically in the kernel, so
    /// the per-row work is the pushes alone.
    template <typename SharedKey, bool key_in_column>
    void ALWAYS_INLINE recordMiss(UInt32 row, UInt64 hash, UInt8 bucket, const SharedKey & key)
    {
        source_rows.push_back(row);
        hashes.push_back(hash);
        buckets.push_back(bucket);
        if constexpr (key_in_column)
        {
            static_assert(adaptive_key_stages_bytes<SharedKey>);
            key_sizes.push_back(static_cast<UInt64>(static_cast<std::string_view>(key).size()));
        }
        else if constexpr (adaptive_key_stages_bytes<SharedKey>)
        {
            /// An empty packed key has a null data pointer, which the copy must not dereference.
            const auto bytes = static_cast<std::string_view>(key);
            if (!bytes.empty())
                key_bytes.insert(bytes.begin(), bytes.end());
            key_offsets.push_back(key_bytes.size());
        }
        else
        {
            const auto * bytes = reinterpret_cast<const char *>(&key);
            key_bytes.insert(bytes, bytes + sizeof(SharedKey));
        }
    }

    /// Records one count contribution, represented by its first source row and run length.
    template <typename SharedKey, bool key_in_column>
    void ALWAYS_INLINE recordCountRun(UInt32 row, UInt64 hash, UInt8 bucket, const SharedKey & key, UInt32 multiplicity)
    {
        recordMiss<SharedKey, key_in_column>(row, hash, bucket, key);
        multiplicities.push_back(multiplicity);
    }

    /// Tests the hash of the last recorded count run. The caller must also compare the keys.
    bool lastCountRunHasHash(UInt64 hash) const { return !hashes.empty() && hashes.back() == hash; }

    /// Extends the count run after the caller has established key equality.
    void extendLastCountRun()
    {
        chassert(!multiplicities.empty());
        ++multiplicities.back();
    }

    /// The pre-deduplication hashes, one per record, for the thaw sample.
    std::span<const UInt64> getHashes() const { return {hashes.data(), hashes.size()}; }

    /// Key bytes held by this recording: the inline or copied bytes. Column-resident keys hold none.
    size_t keyBytesRecorded() const { return key_bytes.size(); }

    const bool use_own_memory_tracker;

private:
    friend class StagedChunkConverter;

    PaddedPODArray<UInt32> source_rows;
    PaddedPODArray<UInt64> hashes;
    PaddedPODArray<UInt8> buckets;
    /// Populated only for count-only aggregation.
    PaddedPODArray<UInt32> multiplicities;
    /// `Fixed`: record i occupies `[i * fixed_key_size, (i + 1) * fixed_key_size)`.
    /// `Recorded`: record i occupies `[key_offsets[i], key_offsets[i + 1])`.
    PaddedPODArray<char> key_bytes;
    PaddedPODArray<UInt64> key_offsets;
    /// `InKeyColumn`: record i's key is the first `key_sizes[i]` bytes of its row in the key column.
    /// When the block's key is constant, the kernel probes the constant's single-row data column,
    /// which is what the producer forwards, so every record's key is that column's row zero.
    PaddedPODArray<UInt64> key_sizes;
    KeyBytes key_bytes_source = KeyBytes::Fixed;
    size_t fixed_key_size = 0;
    bool constant_key = false;
};

/// Owns partitioned keys beside dense payload columns. Publication moves the keys into a staged
/// chunk and prepares instructions only after coalescing and pressure sizing are complete.
class StagedKeysInfo final : public ChunkInfo
{
public:
    explicit StagedKeysInfo(bool use_own_memory_tracker_) : use_own_memory_tracker(use_own_memory_tracker_) {}
    Ptr clone() const override;

    StagedChunk::StagedKeys keys;
    bool use_own_memory_tracker;
};

}
