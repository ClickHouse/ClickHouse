#pragma once

#include <base/types.h>
#include <Common/HashTable/HashSet.h>

#include <memory>
#include <vector>


namespace DB
{

class ReadBuffer;
class WriteBuffer;

/// Approximate set membership filter based on xor-style binary fuse construction.
class BinaryFuseFilter
{
public:
    enum class Variant : UInt8
    {
        Empty = 0,
        Single = 1,
        SmallList = 2,
        Fuse3 = 3,
        Fuse4 = 4,
    };

    static constexpr size_t MAX_SEED_RETRIES = 12;
    static constexpr size_t SMALL_LIST_THRESHOLD = 32;
    /// SmallList stores at most SMALL_LIST_THRESHOLD distinct hashes; writer and reader use the same bound.

    static size_t fingerprintBitsFromFalsePositiveRate(double false_positive_rate);

    static BinaryFuseFilter buildFromHashes(const HashSet<UInt64> & hashes, double false_positive_rate);

    static BinaryFuseFilter empty(size_t fingerprint_bits);

    BinaryFuseFilter() = default;

    bool alwaysFalse() const { return variant == Variant::Empty; }
    bool contains(UInt64 item_hash) const;

    void serializeBinary(WriteBuffer & ostr) const;
    void deserializeBinary(ReadBuffer & istr, UInt8 format_version);

    size_t memoryUsageBytes() const;
    Variant getVariant() const { return variant; }
    UInt8 getFingerprintBits() const { return static_cast<UInt8>(f_bits); }

private:
    BinaryFuseFilter(size_t fingerprint_bits_, UInt64 seed_);

    bool buildFromHashesImpl(const HashSet<UInt64> & hashes);
    bool tryBuildFuse3(const std::vector<UInt64> & keys, UInt64 seed_candidate);
    bool tryBuildFuse4(const std::vector<UInt64> & keys, UInt64 seed_candidate);
    UInt64 mixKey(UInt64 item_hash) const;
    UInt32 fingerprintFromKey(UInt64 item_hash) const;
    size_t bytesPerFingerprint() const;
    UInt32 getSlot(size_t index) const;
    void setSlot(size_t index, UInt32 value);
    void getIndexes3(UInt64 item_hash, size_t & i0, size_t & i1, size_t & i2) const;
    void getIndexes4(UInt64 item_hash, size_t & i0, size_t & i1, size_t & i2, size_t & i3) const;
    void resetFusePayload();

    size_t f_bits = 8;
    UInt32 fp_mask = 0xFF;
    UInt64 seed = 0;
    Variant variant = Variant::Empty;
    UInt8 arity = 3;

    UInt64 single_hash = 0;
    std::vector<UInt64> small_hashes;

    size_t segment_length = 0;
    size_t segment_count = 0;
    size_t array_size = 0;
    std::vector<UInt8> packed_fingerprints;
};

using BinaryFuseFilterPtr = std::shared_ptr<BinaryFuseFilter>;

}
