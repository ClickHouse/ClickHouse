#include <Interpreters/CuckooFilter.h>

#include <city.h>
#include <Common/Exception.h>
#include <Common/HashTable/Hash.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <cmath>
#include <cstring>
#include <limits>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

namespace
{
constexpr size_t MAX_SERIALIZED_CUCKOO_PAYLOAD_BYTES = 1ULL << 26; /// 64 MiB — skip index payloads beyond this are pathological
}

size_t CuckooFilter::fingerprintBitsFromFalsePositiveRate(double false_positive_rate)
{
    if (!std::isfinite(false_positive_rate) || false_positive_rate <= 0.0 || false_positive_rate >= 1.0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cuckoo filter false positive rate must be between 0 and 1 exclusive, got {}", false_positive_rate);

    /// Cuckoo filter FPR formula: actual_fpr = 2 * b / 2^f, where b = SLOTS_PER_BUCKET.
    /// Solving for f: f = log2(2 * SLOTS_PER_BUCKET / fpr).
    static_assert(CuckooFilter::SLOTS_PER_BUCKET == 4, "FPR formula assumes 4 slots per bucket");
    const double bits_needed = std::log2(2.0 * CuckooFilter::SLOTS_PER_BUCKET / false_positive_rate);
    if (bits_needed <= 12.0) /// midpoint between 8 and 16
        return 8;
    return 16;
}

size_t CuckooFilter::bucketCountForDistinctKeys(size_t distinct_keys, double load_factor)
{
    if (distinct_keys == 0)
        return 0;

    static constexpr size_t max_buckets = std::numeric_limits<size_t>::max() / SLOTS_PER_BUCKET;

    const long double slots_needed = static_cast<long double>(distinct_keys) / static_cast<long double>(load_factor);
    if (slots_needed > static_cast<long double>(max_buckets) * static_cast<long double>(SLOTS_PER_BUCKET))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cuckoo filter distinct key count {} exceeds maximum supported bucket count",
            distinct_keys);

    size_t buckets = static_cast<size_t>(std::ceil(slots_needed / static_cast<long double>(SLOTS_PER_BUCKET)));
    if (buckets > max_buckets)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cuckoo filter bucket count overflow (distinct keys: {})",
            distinct_keys);

    /// Round up to power of two so bucket_mask works with XOR alternate index.
    size_t power = 1;
    while (power < buckets)
    {
        if (power > max_buckets / 2)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cuckoo filter bucket count overflow (distinct keys: {})",
                distinct_keys);
        power <<= 1;
    }

    return std::max<size_t>(power, 1);
}

CuckooFilter::CuckooFilter(size_t fingerprint_bits_, UInt64 seed_, size_t num_buckets_, std::vector<uint8_t> slot_data_)
    : f_bits(fingerprint_bits_)
    , seed(seed_)
    , num_buckets(num_buckets_)
    , slot_data(std::move(slot_data_))
{
    if (f_bits != 8 && f_bits != 16)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid cuckoo filter fingerprint width {}, must be 8 or 16", f_bits);

    fp_mask = (f_bits == 16) ? 0xFFFFu : 0xFFu;
    bucket_mask = num_buckets > 0 ? (num_buckets - 1) : 0;
}

CuckooFilter CuckooFilter::empty(size_t fingerprint_bits)
{
    return CuckooFilter(fingerprint_bits, 0, 0, {});
}

CuckooFilter CuckooFilter::buildFromHashes(const HashSet<UInt64> & hashes, double false_positive_rate)
{
    const size_t f_bits_local = fingerprintBitsFromFalsePositiveRate(false_positive_rate);

    static constexpr UInt64 seed_candidates[MAX_SEED_RETRIES] = {
        0x9e3779b97f4a7c15ULL,
        0xbf58476d1ce4e5b9ULL,
        0x94d049bb133111ebULL,
    };

    for (UInt64 seed_candidate : seed_candidates)
    {
        CuckooFilter filter(f_bits_local, seed_candidate, 0, {});
        if (filter.buildFromHashesImpl(hashes))
            return filter;
    }

    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "Could not build cuckoo filter after {} attempts (distinct keys: {}). "
        "Try a larger false positive rate or increasing index granularity.",
        MAX_SEED_RETRIES,
        hashes.size());
}

bool CuckooFilter::buildFromHashesImpl(const HashSet<UInt64> & hashes)
{
    num_buckets = bucketCountForDistinctKeys(hashes.size(), DEFAULT_LOAD_FACTOR);
    bucket_mask = num_buckets > 0 ? (num_buckets - 1) : 0;
    const size_t num_slots = num_buckets * SLOTS_PER_BUCKET;
    const size_t bytes = bytesNeeded(num_slots, f_bits);
    if (bytes > MAX_SERIALIZED_CUCKOO_PAYLOAD_BYTES)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cuckoo filter payload byte size {} exceeds maximum {} (distinct keys: {})",
            bytes,
            MAX_SERIALIZED_CUCKOO_PAYLOAD_BYTES,
            hashes.size());
    slot_data.assign(bytes, 0);

    if (hashes.empty())
        return true;

    for (const auto & cell : hashes)
        if (!insertKey(cell.getKey()))
            return false;

    return true;
}

UInt64 CuckooFilter::mixKey(UInt64 item_hash) const
{
    return intHash64(item_hash ^ seed);
}

UInt64 CuckooFilter::fingerprintFromKey(UInt64 item_hash) const
{
    UInt64 h = CityHash_v1_0_2::Hash128to64(CityHash_v1_0_2::uint128(item_hash, seed ^ 0xc6a4a7935bd1e995ULL));
    UInt64 fp = h & fp_mask;
    return fp ? fp : 1;
}

size_t CuckooFilter::primaryBucket(UInt64 item_hash) const
{
    return static_cast<size_t>(mixKey(item_hash)) & bucket_mask;
}

size_t CuckooFilter::altHashFingerprint(UInt64 fp) const
{
    /// Paper-style alternate index: i2 = hash(fingerprint) XOR i1 (symmetric under XOR). Single mix matches `mixKey` spirit.
    return static_cast<size_t>(intHash64(fp ^ seed)) & bucket_mask;
}

size_t CuckooFilter::alternateBucket(size_t bucket, UInt64 fp) const
{
    return bucket ^ altHashFingerprint(fp);
}

size_t CuckooFilter::bytesNeeded(size_t num_slots, size_t bits) const
{
    if (num_slots == 0)
        return 0;

    if (unlikely(bits != 8 && bits != 16))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid cuckoo filter fingerprint width {}, must be 8 or 16", bits);

    if (unlikely(num_slots > (std::numeric_limits<size_t>::max() - 7) / bits))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cuckoo filter slots bit-size overflows size_t");

    const size_t total_bits = num_slots * bits;
    return (total_bits + 7) / 8;
}

UInt64 CuckooFilter::readSlot(size_t slot_idx) const
{
    const size_t bit_offset = slot_idx * f_bits;
    const size_t byte_idx = bit_offset / 8;
    const size_t bit_shift = bit_offset % 8;
    const size_t bytes_to_read = (f_bits + bit_shift + 7) / 8;

    UInt64 val = 0;
    for (size_t i = 0; i < bytes_to_read && (byte_idx + i) < slot_data.size(); ++i)
        val |= (static_cast<UInt64>(slot_data[byte_idx + i]) << (i * 8));

    return (val >> bit_shift) & fp_mask;
}

void CuckooFilter::writeSlot(size_t slot_idx, UInt64 fp)
{
    const UInt64 fp_local = fp & fp_mask;

    const size_t bit_offset = slot_idx * f_bits;
    const size_t byte_idx = bit_offset / 8;
    const size_t bit_shift = bit_offset % 8;
    const size_t bytes_to_write = (f_bits + bit_shift + 7) / 8;

    UInt64 mask = fp_mask << bit_shift;
    UInt64 val = fp_local << bit_shift;

    for (size_t i = 0; i < bytes_to_write && (byte_idx + i) < slot_data.size(); ++i)
    {
        slot_data[byte_idx + i] &= ~static_cast<uint8_t>(mask >> (i * 8));
        slot_data[byte_idx + i] |= static_cast<uint8_t>(val >> (i * 8));
    }
}

bool CuckooFilter::insertFingerprintIntoBucket(size_t bucket, UInt64 fp)
{
    const size_t base = bucket * SLOTS_PER_BUCKET;
    for (size_t s = 0; s < SLOTS_PER_BUCKET; ++s)
    {
        if (readSlot(base + s) == 0)
        {
            writeSlot(base + s, fp);
            return true;
        }
    }
    return false;
}

bool CuckooFilter::kickInsert(UInt64 item_hash, UInt64 fp, size_t bucket1, size_t bucket2)
{
    size_t b = ((item_hash >> 1) & 1) ? bucket2 : bucket1;

    for (size_t kick = 0; kick < MAX_KICKS; ++kick)
    {
        const size_t base = b * SLOTS_PER_BUCKET;
        const size_t slot = (static_cast<size_t>(item_hash) + kick) % SLOTS_PER_BUCKET;
        UInt64 displaced = readSlot(base + slot);
        writeSlot(base + slot, fp);

        if (displaced == 0)
            return true;

        fp = displaced;
        b = alternateBucket(b, fp);

        if (insertFingerprintIntoBucket(b, fp))
            return true;
    }

    return false;
}

bool CuckooFilter::insertKey(UInt64 item_hash)
{
    UInt64 fp = fingerprintFromKey(item_hash);
    size_t i1 = primaryBucket(item_hash);
    size_t i2 = alternateBucket(i1, fp);

    if (insertFingerprintIntoBucket(i1, fp))
        return true;
    if (insertFingerprintIntoBucket(i2, fp))
        return true;

    return kickInsert(item_hash, fp, i1, i2);
}

bool CuckooFilter::contains(UInt64 item_hash) const
{
    if (slot_data.empty())
        return false;

    UInt64 fp = fingerprintFromKey(item_hash);
    size_t i1 = primaryBucket(item_hash);
    size_t i2 = alternateBucket(i1, fp);

    if (f_bits == 8)
    {
        const uint8_t fp8 = static_cast<uint8_t>(fp);
        const size_t base1 = i1 * SLOTS_PER_BUCKET;
        const size_t base2 = i2 * SLOTS_PER_BUCKET;
        for (size_t s = 0; s < SLOTS_PER_BUCKET; ++s)
        {
            if (slot_data[base1 + s] == fp8 || slot_data[base2 + s] == fp8)
                return true;
        }
        return false;
    }

    for (size_t s = 0; s < SLOTS_PER_BUCKET; ++s)
    {
        if (readSlot(i1 * SLOTS_PER_BUCKET + s) == fp)
            return true;
        if (readSlot(i2 * SLOTS_PER_BUCKET + s) == fp)
            return true;
    }
    return false;
}

void CuckooFilter::serializeBinary(WriteBuffer & ostr) const
{
    if (slot_data.size() > MAX_SERIALIZED_CUCKOO_PAYLOAD_BYTES)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cuckoo filter payload byte size {} exceeds maximum {}",
            slot_data.size(),
            MAX_SERIALIZED_CUCKOO_PAYLOAD_BYTES);
    writeVarUInt(static_cast<UInt64>(f_bits), ostr);
    writeBinaryLittleEndian(seed, ostr);
    writeVarUInt(num_buckets, ostr);
    if (!slot_data.empty())
        ostr.write(reinterpret_cast<const char *>(slot_data.data()), slot_data.size());
}

void CuckooFilter::deserializeBinary(ReadBuffer & istr, UInt8 format_version)
{
    if (format_version != 1)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Unknown cuckoo filter granule format version {}.",
            static_cast<UInt32>(format_version));

    UInt64 f_bits_u64 = 0;
    readVarUInt(f_bits_u64, istr);
    f_bits = static_cast<size_t>(f_bits_u64);

    if (f_bits != 8 && f_bits != 16)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid cuckoo filter fingerprint width {}, must be 8 or 16", f_bits);

    readBinaryLittleEndian(seed, istr);
    UInt64 num_buckets_u64 = 0;
    readVarUInt(num_buckets_u64, istr);

    static constexpr size_t max_buckets_for_slots = std::numeric_limits<size_t>::max() / SLOTS_PER_BUCKET;
    if (num_buckets_u64 > static_cast<UInt64>(max_buckets_for_slots))
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Invalid cuckoo filter bucket count in stream (too large): {}",
            num_buckets_u64);

    num_buckets = static_cast<size_t>(num_buckets_u64);

    if (num_buckets != 0 && (num_buckets & (num_buckets - 1)) != 0)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Invalid cuckoo filter bucket count in stream (must be a power of two): {}",
            num_buckets);

    fp_mask = (f_bits == 16) ? 0xFFFFu : 0xFFu;
    bucket_mask = num_buckets > 0 ? (num_buckets - 1) : 0;

    const size_t num_slots = num_buckets * SLOTS_PER_BUCKET;
    const size_t bytes = bytesNeeded(num_slots, f_bits);
    if (bytes > MAX_SERIALIZED_CUCKOO_PAYLOAD_BYTES)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Cuckoo filter payload byte size {} exceeds maximum {}",
            bytes,
            MAX_SERIALIZED_CUCKOO_PAYLOAD_BYTES);
    slot_data.resize(bytes);
    if (!slot_data.empty())
        istr.readStrict(reinterpret_cast<char *>(slot_data.data()), bytes);
}

size_t CuckooFilter::memoryUsageBytes() const
{
    return sizeof(CuckooFilter) + slot_data.capacity();
}

}
