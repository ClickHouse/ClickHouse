#pragma once

#include <base/types.h>
#include <Common/HashTable/HashSet.h>

#include <memory>
#include <vector>


namespace DB
{

class ReadBuffer;
class WriteBuffer;

/// Approximate set membership (Fan et al., Cuckoo Filter). Used by MergeTree `cuckoo_filter` skip index.
/// No false negatives for inserted keys; false positives possible.
class CuckooFilter
{
public:
    static constexpr size_t SLOTS_PER_BUCKET = 4;
    static constexpr double DEFAULT_LOAD_FACTOR = 0.9;
    static constexpr size_t MAX_KICKS = 500;
    static constexpr size_t MAX_SEED_RETRIES = 3;

    /// Derive fingerprint width (8 or 16 bits) from target false positive rate.
    static size_t fingerprintBitsFromFalsePositiveRate(double false_positive_rate);

    /// Build from distinct key hashes. Retries with different seeds up to MAX_SEED_RETRIES; throws on failure.
    static CuckooFilter buildFromHashes(const HashSet<UInt64> & hashes, double false_positive_rate);

    /// Empty filter (e.g. placeholder granule before data is loaded).
    static CuckooFilter empty(size_t fingerprint_bits);

    CuckooFilter() = default;

    bool alwaysFalse() const { return num_buckets == 0; }

    /// Membership test for a key hash (same scheme as insertion).
    bool contains(UInt64 item_hash) const;

    void serializeBinary(WriteBuffer & ostr) const;
    void deserializeBinary(ReadBuffer & istr, UInt8 format_version);

    size_t memoryUsageBytes() const;

private:
    CuckooFilter(size_t fingerprint_bits_, UInt64 seed_, size_t num_buckets_, std::vector<uint8_t> slot_data_);

    static size_t bucketCountForDistinctKeys(size_t distinct_keys, double load_factor);

    bool buildFromHashesImpl(const HashSet<UInt64> & hashes);

    UInt64 mixKey(UInt64 item_hash) const;
    UInt64 fingerprintFromKey(UInt64 item_hash) const;
    size_t primaryBucket(UInt64 item_hash) const;
    size_t altHashFingerprint(UInt64 fp) const;
    size_t alternateBucket(size_t bucket, UInt64 fp) const;

    bool insertKey(UInt64 item_hash);
    bool insertFingerprintIntoBucket(size_t bucket, UInt64 fp);
    bool kickInsert(UInt64 item_hash, UInt64 fp, size_t bucket1, size_t bucket2);

    size_t bytesNeeded(size_t num_slots, size_t bits) const;
    UInt64 readSlot(size_t slot_idx) const;
    void writeSlot(size_t slot_idx, UInt64 fp);

    size_t f_bits = 8;
    UInt64 fp_mask = 0xFF;
    UInt64 seed = 0;
    size_t num_buckets = 0;
    size_t bucket_mask = 0;
    /// Bitpacked slots, each slot occupies exactly `f_bits` bits. 0 = empty slot.
    std::vector<uint8_t> slot_data;
};

using CuckooFilterPtr = std::shared_ptr<CuckooFilter>;

}
