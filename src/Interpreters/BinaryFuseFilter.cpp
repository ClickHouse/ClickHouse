#include <Interpreters/BinaryFuseFilter.h>

#include <city.h>
#include <base/unaligned.h>
#include <Common/Exception.h>
#include <Common/HashTable/Hash.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <cstring>
#include <limits>
#include <utility>

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
constexpr UInt8 BINARY_FUSE_VERSION = 1;
constexpr size_t MAX_SERIALIZED_FUSE_PAYLOAD_BYTES = 1ULL << 26; /// 64 MiB — skip index payloads beyond this are pathological
constexpr size_t MAX_FUSE_BUILD_SCRATCH_BYTES = 1ULL << 30; /// 1 GiB peak transient memory per granule build attempt

size_t fastRange64(UInt64 x, size_t range)
{
    if (range == 0)
        return 0;
    return static_cast<size_t>((static_cast<unsigned __int128>(x) * range) >> 64);
}

size_t bytesPerFingerprintBits(size_t bits)
{
    return bits <= 8 ? 1 : 2;
}

size_t powerOfTwoFromExponentFloor(double exponent_floor)
{
    if (exponent_floor <= 0.0)
        return 1;
    const auto exp_i = static_cast<unsigned>(exponent_floor);
    if (exp_i >= sizeof(size_t) * 8)
        return static_cast<size_t>(1) << ((sizeof(size_t) * 8) - 1);
    return static_cast<size_t>(1) << exp_i;
}

struct FuseCandidate
{
    BinaryFuseFilter::Variant variant = BinaryFuseFilter::Variant::Empty;
    UInt64 seed = 0;
    UInt8 arity = 0;
    size_t segment_length = 0;
    size_t segment_count = 0;
    size_t array_size = 0;
    std::vector<UInt8> bytes;
    bool ok = false;
};

struct PeelEntry
{
    UInt64 key;
    size_t selected;
};

bool fuseBuildScratchWithinLimit(size_t array_size, size_t payload_bytes, size_t keys_count)
{
    const size_t array_vectors_bytes = array_size * (sizeof(UInt32) + sizeof(UInt64) + sizeof(size_t));
    const size_t stack_bytes = keys_count * sizeof(PeelEntry);

    size_t total = 0;
    auto add = [&](size_t bytes) -> bool
    {
        if (bytes > std::numeric_limits<size_t>::max() - total)
            return false;
        total += bytes;
        return true;
    };

    return add(payload_bytes) && add(array_vectors_bytes) && add(stack_bytes) && total <= MAX_FUSE_BUILD_SCRATCH_BYTES;
}

}

size_t BinaryFuseFilter::fingerprintBitsFromFalsePositiveRate(double false_positive_rate)
{
    if (!std::isfinite(false_positive_rate) || false_positive_rate <= 0.0 || false_positive_rate >= 1.0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Binary fuse filter false positive rate must be between 0 and 1 exclusive, got {}", false_positive_rate);
    /// Round to nearest whole-byte fingerprint width.
    /// fpr=0.003 needs 8.4 bits → round to 8-bit (slightly looser FPR)
    /// fpr=0.001 needs 10 bits → rounds to 8-bit (closer to 8 than 16)
    const double bits_needed = std::log2(1.0 / false_positive_rate);
    if (bits_needed <= 12.0) /// midpoint between 8 and 16
        return 8;
    return 16;
}

BinaryFuseFilter::BinaryFuseFilter(size_t fingerprint_bits_, UInt64 seed_)
    : f_bits(fingerprint_bits_)
    , seed(seed_)
{
    if (f_bits != 8 && f_bits != 16)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Binary fuse filter fingerprint width must be 8 or 16, got {}", f_bits);
    fp_mask = f_bits == 8 ? 0xFFu : 0xFFFFu;
}

BinaryFuseFilter BinaryFuseFilter::empty(size_t fingerprint_bits)
{
    BinaryFuseFilter filter(fingerprint_bits, 0);
    filter.variant = Variant::Empty;
    return filter;
}

BinaryFuseFilter BinaryFuseFilter::buildFromHashes(const HashSet<UInt64> & hashes, double false_positive_rate)
{
    BinaryFuseFilter filter(fingerprintBitsFromFalsePositiveRate(false_positive_rate), 0);
    if (filter.buildFromHashesImpl(hashes))
        return filter;
    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Could not build binary fuse filter (distinct keys: {}). "
        "Try a larger false positive rate or increasing index granularity.",
        hashes.size());
}

UInt64 BinaryFuseFilter::mixKey(UInt64 item_hash) const
{
    return intHash64(item_hash ^ seed);
}

UInt32 BinaryFuseFilter::fingerprintFromKey(UInt64 item_hash) const
{
    UInt64 h = CityHash_v1_0_2::Hash128to64(CityHash_v1_0_2::uint128(item_hash, seed ^ 0x9e3779b97f4a7c15ULL));
    return static_cast<UInt32>(h & fp_mask);
}

size_t BinaryFuseFilter::bytesPerFingerprint() const
{
    return bytesPerFingerprintBits(f_bits);
}

UInt32 BinaryFuseFilter::getSlot(size_t index) const
{
    const size_t bpf = bytesPerFingerprint();
    const size_t byte_index = index * bpf;
    if (byte_index + bpf > packed_fingerprints.size())
        return 0;
    if (bpf == 1)
        return packed_fingerprints[byte_index];

    return unalignedLoadLittleEndian<UInt16>(packed_fingerprints.data() + byte_index);
}

void BinaryFuseFilter::setSlot(size_t index, UInt32 value)
{
    const size_t bpf = bytesPerFingerprint();
    const size_t byte_index = index * bpf;
    if (byte_index + bpf > packed_fingerprints.size())
        return;
    if (bpf == 1)
    {
        packed_fingerprints[byte_index] = static_cast<UInt8>(value & 0xFFu);
        return;
    }

    unalignedStoreLittleEndian<UInt16>(
        packed_fingerprints.data() + byte_index, static_cast<UInt16>(value & 0xFFFFu));
}

void BinaryFuseFilter::getIndexes3(UInt64 item_hash, size_t & i0, size_t & i1, size_t & i2) const
{
    /// Three intra-segment offsets must be statistically independent or peeling fails.
    /// Previously i2 used (h * MULT) & mask whose low bits are a permutation of (h & mask), making i2 a deterministic function of i0.
    /// That broke peeling on most seeds. Use intHash64(h) for a fully mixed second hash.
    const UInt64 h = mixKey(item_hash);
    const UInt64 h2 = intHash64(h);
    const size_t seg = segment_count > 2 ? fastRange64(h, segment_count - 2) : 0;
    const size_t base = seg * segment_length;
    const size_t mask = segment_length - 1;
    i0 = base + (static_cast<size_t>(h) & mask);
    i1 = base + segment_length + ((static_cast<size_t>(h >> 32)) & mask);
    i2 = base + 2 * segment_length + (static_cast<size_t>(h2) & mask);
}

void BinaryFuseFilter::getIndexes4(UInt64 item_hash, size_t & i0, size_t & i1, size_t & i2, size_t & i3) const
{
    const UInt64 h = mixKey(item_hash);
    const UInt64 h2 = intHash64(h);
    const size_t seg = segment_count > 3 ? fastRange64(h, segment_count - 3) : 0;
    const size_t base = seg * segment_length;
    const size_t mask = segment_length - 1;
    i0 = base + (static_cast<size_t>(h) & mask);
    i1 = base + segment_length + ((static_cast<size_t>(h >> 32)) & mask);
    i2 = base + 2 * segment_length + (static_cast<size_t>(h2) & mask);
    i3 = base + 3 * segment_length + ((static_cast<size_t>(h2 >> 32)) & mask);
}

void BinaryFuseFilter::resetFusePayload()
{
    arity = 0;
    segment_length = 0;
    segment_count = 0;
    array_size = 0;
    packed_fingerprints.clear();
}

bool BinaryFuseFilter::buildFromHashesImpl(const HashSet<UInt64> & hashes)
{
    small_hashes.clear();
    if (hashes.empty())
    {
        variant = Variant::Empty;
        single_hash = 0;
        resetFusePayload();
        return true;
    }

    std::vector<UInt64> keys;
    keys.reserve(hashes.size());
    for (const auto & cell : hashes)
        keys.push_back(cell.getKey());

    if (keys.size() == 1)
    {
        variant = Variant::Single;
        single_hash = keys[0];
        resetFusePayload();
        return true;
    }

    if (keys.size() <= SMALL_LIST_THRESHOLD)
    {
        variant = Variant::SmallList;
        small_hashes = std::move(keys);
        std::sort(small_hashes.begin(), small_hashes.end());
        resetFusePayload();
        return true;
    }

    static constexpr std::array<UInt64, MAX_SEED_RETRIES> seeds = {
        0x243f6a8885a308d3ULL, 0x13198a2e03707344ULL, 0xa4093822299f31d0ULL, 0x082efa98ec4e6c89ULL,
        0x452821e638d01377ULL, 0xbe5466cf34e90c6cULL, 0xc0ac29b7c97c50ddULL, 0x3f84d5b5b5470917ULL,
        0x9216d5d98979fb1bULL, 0xd1310ba698dfb5acULL, 0x2ffd72dbd01adfb7ULL, 0xb8e1afed6a267e96ULL};

    FuseCandidate best_fuse4;
    for (UInt64 s : seeds)
    {
        BinaryFuseFilter f4(f_bits, s);
        if (f4.tryBuildFuse4(keys, s))
        {
            best_fuse4.variant = Variant::Fuse4;
            best_fuse4.seed = s;
            best_fuse4.arity = 4;
            best_fuse4.segment_length = f4.segment_length;
            best_fuse4.segment_count = f4.segment_count;
            best_fuse4.array_size = f4.array_size;
            best_fuse4.bytes = f4.packed_fingerprints;
            best_fuse4.ok = true;
            break;
        }
    }

    FuseCandidate best_fuse3;
    for (UInt64 s : seeds)
    {
        const UInt64 seed3 = s ^ 0x9e3779b97f4a7c15ULL;
        BinaryFuseFilter f3(f_bits, seed3);
        if (f3.tryBuildFuse3(keys, seed3))
        {
            best_fuse3.variant = Variant::Fuse3;
            best_fuse3.seed = f3.seed;
            best_fuse3.arity = 3;
            best_fuse3.segment_length = f3.segment_length;
            best_fuse3.segment_count = f3.segment_count;
            best_fuse3.array_size = f3.array_size;
            best_fuse3.bytes = f3.packed_fingerprints;
            best_fuse3.ok = true;
            break;
        }
    }

    FuseCandidate best;
    if (best_fuse4.ok && best_fuse3.ok)
        best = (best_fuse4.bytes.size() <= best_fuse3.bytes.size()) ? std::move(best_fuse4) : std::move(best_fuse3);
    else if (best_fuse4.ok)
        best = std::move(best_fuse4);
    else if (best_fuse3.ok)
        best = std::move(best_fuse3);

    if (!best.ok)
        return false;

    variant = best.variant;
    seed = best.seed;
    arity = best.arity;
    segment_length = best.segment_length;
    segment_count = best.segment_count;
    array_size = best.array_size;
    packed_fingerprints = std::move(best.bytes);
    single_hash = 0;
    small_hashes.clear();
    return true;
}

bool BinaryFuseFilter::contains(UInt64 item_hash) const
{
    switch (variant)
    {
        case Variant::Empty:
            return false;
        case Variant::Single:
            return item_hash == single_hash;
        case Variant::SmallList:
            return std::binary_search(small_hashes.begin(), small_hashes.end(), item_hash);
        case Variant::Fuse3:
        {
            size_t i0 = 0;
            size_t i1 = 0;
            size_t i2 = 0;
            getIndexes3(item_hash, i0, i1, i2);
            return (getSlot(i0) ^ getSlot(i1) ^ getSlot(i2)) == fingerprintFromKey(item_hash);
        }
        case Variant::Fuse4:
        {
            size_t i0 = 0;
            size_t i1 = 0;
            size_t i2 = 0;
            size_t i3 = 0;
            getIndexes4(item_hash, i0, i1, i2, i3);
            return (getSlot(i0) ^ getSlot(i1) ^ getSlot(i2) ^ getSlot(i3)) == fingerprintFromKey(item_hash);
        }
    }
    return false;
}

void BinaryFuseFilter::serializeBinary(WriteBuffer & ostr) const
{
    writeIntBinary(BINARY_FUSE_VERSION, ostr);
    writeIntBinary(static_cast<UInt8>(variant), ostr);
    writeIntBinary(static_cast<UInt8>(f_bits), ostr);

    switch (variant)
    {
        case Variant::Empty:
            return;
        case Variant::Single:
            writeBinaryLittleEndian(single_hash, ostr);
            return;
        case Variant::SmallList:
        {
            if (small_hashes.size() > SMALL_LIST_THRESHOLD)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Binary fuse small list size {} exceeds maximum {}",
                    small_hashes.size(),
                    SMALL_LIST_THRESHOLD);
            writeVarUInt(static_cast<UInt64>(small_hashes.size()), ostr);
            for (UInt64 x : small_hashes)
                writeBinaryLittleEndian(x, ostr);
            return;
        }
        case Variant::Fuse3:
        case Variant::Fuse4:
            if (packed_fingerprints.size() > MAX_SERIALIZED_FUSE_PAYLOAD_BYTES)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Binary fuse payload byte size {} exceeds maximum {}",
                    packed_fingerprints.size(),
                    MAX_SERIALIZED_FUSE_PAYLOAD_BYTES);
            writeBinaryLittleEndian(seed, ostr);
            writeIntBinary(arity, ostr);
            writeVarUInt(static_cast<UInt64>(segment_length), ostr);
            writeVarUInt(static_cast<UInt64>(segment_count), ostr);
            writeVarUInt(static_cast<UInt64>(array_size), ostr);
            writeVarUInt(static_cast<UInt64>(packed_fingerprints.size()), ostr);
            if (!packed_fingerprints.empty())
                ostr.write(reinterpret_cast<const char *>(packed_fingerprints.data()), packed_fingerprints.size());
            return;
    }
}

void BinaryFuseFilter::deserializeBinary(ReadBuffer & istr, UInt8 format_version)
{
    if (format_version != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown binary fuse index format version {}.", static_cast<UInt32>(format_version));

    UInt8 internal_version = 0;
    readIntBinary(internal_version, istr);
    if (internal_version != BINARY_FUSE_VERSION)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown binary fuse payload version {}", static_cast<UInt32>(internal_version));

    UInt8 variant_u8 = 0;
    readIntBinary(variant_u8, istr);
    if (variant_u8 > static_cast<UInt8>(Variant::Fuse4))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown binary fuse filter variant {}", static_cast<UInt32>(variant_u8));
    variant = static_cast<Variant>(variant_u8);

    UInt8 bits_u8 = 0;
    readIntBinary(bits_u8, istr);
    if (bits_u8 != 8 && bits_u8 != 16)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid binary fuse fingerprint width {}", static_cast<UInt32>(bits_u8));
    f_bits = bits_u8;
    fp_mask = f_bits == 8 ? 0xFFu : 0xFFFFu;

    single_hash = 0;
    small_hashes.clear();
    resetFusePayload();

    switch (variant)
    {
        case Variant::Empty:
            return;
        case Variant::Single:
            readBinaryLittleEndian(single_hash, istr);
            return;
        case Variant::SmallList:
        {
            UInt64 n = 0;
            readVarUInt(n, istr);
            if (n > SMALL_LIST_THRESHOLD)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Binary fuse small list size {} exceeds maximum {}",
                    n,
                    SMALL_LIST_THRESHOLD);
            small_hashes.resize(static_cast<size_t>(n));
            for (auto & x : small_hashes)
                readBinaryLittleEndian(x, istr);
            if (!std::is_sorted(small_hashes.begin(), small_hashes.end()))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Binary fuse small list payload must be sorted");
            return;
        }
        case Variant::Fuse3:
        case Variant::Fuse4:
        {
            readBinaryLittleEndian(seed, istr);
            readIntBinary(arity, istr);
            if ((variant == Variant::Fuse3 && arity != 3) || (variant == Variant::Fuse4 && arity != 4))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Binary fuse arity {} does not match variant {}", static_cast<UInt16>(arity), static_cast<UInt16>(variant));
            UInt64 seg_len = 0;
            UInt64 seg_cnt = 0;
            UInt64 arr_size = 0;
            UInt64 byte_size = 0;
            readVarUInt(seg_len, istr);
            readVarUInt(seg_cnt, istr);
            readVarUInt(arr_size, istr);
            readVarUInt(byte_size, istr);
            segment_length = static_cast<size_t>(seg_len);
            segment_count = static_cast<size_t>(seg_cnt);
            array_size = static_cast<size_t>(arr_size);
            if (segment_length == 0 || (segment_length & (segment_length - 1)) != 0)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid binary fuse segment length {}", segment_length);
            if ((variant == Variant::Fuse3 && segment_count < 3) || (variant == Variant::Fuse4 && segment_count < 4))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid binary fuse segment count {}", segment_count);
            if (segment_count > 0 && segment_length > std::numeric_limits<size_t>::max() / segment_count)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Binary fuse segment overflow");
            if (segment_length * segment_count != array_size)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Binary fuse array_size {} != segment_length {} * segment_count {}",
                    array_size,
                    segment_length,
                    segment_count);
            if (array_size > std::numeric_limits<size_t>::max() / bytesPerFingerprint())
                throw Exception(ErrorCodes::INCORRECT_DATA, "Binary fuse payload size overflow");
            const size_t expected = array_size * bytesPerFingerprint();
            if (expected != static_cast<size_t>(byte_size))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid binary fuse payload byte size {}, expected {}", byte_size, expected);
            if (expected > MAX_SERIALIZED_FUSE_PAYLOAD_BYTES)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Binary fuse payload byte size {} exceeds maximum {}",
                    byte_size,
                    MAX_SERIALIZED_FUSE_PAYLOAD_BYTES);
            packed_fingerprints.resize(expected);
            if (expected)
                istr.readStrict(reinterpret_cast<char *>(packed_fingerprints.data()), expected);
            return;
        }
    }
}

size_t BinaryFuseFilter::memoryUsageBytes() const
{
    return sizeof(BinaryFuseFilter) + small_hashes.capacity() * sizeof(UInt64) + packed_fingerprints.capacity();
}

bool BinaryFuseFilter::tryBuildFuse3(const std::vector<UInt64> & keys, UInt64 seed_candidate)
{
    seed = seed_candidate;
    arity = 3;
    const size_t n = keys.size();
    const double ratio = 0.875 + 0.25 * std::max(1.0, std::log(1e6) / std::log(static_cast<double>(n)));
    const double nd = static_cast<double>(n);
    size_t target = static_cast<size_t>(std::floor(ratio * nd));
    target = std::max(target, static_cast<size_t>(std::floor(1.125 * nd)));
    const double seg_exp = std::floor(std::log(static_cast<double>(n)) / std::log(3.33) + 2.25);
    segment_length = powerOfTwoFromExponentFloor(seg_exp);
    segment_count = std::max<size_t>(3, (target + segment_length - 1) / segment_length);
    if (segment_length != 0 && segment_count > std::numeric_limits<size_t>::max() / segment_length)
        return false;
    array_size = segment_count * segment_length;
    if (array_size > std::numeric_limits<size_t>::max() / bytesPerFingerprint())
        return false;
    const size_t payload_bytes = array_size * bytesPerFingerprint();
    if (payload_bytes > MAX_SERIALIZED_FUSE_PAYLOAD_BYTES)
        return false;
    if (!fuseBuildScratchWithinLimit(array_size, payload_bytes, n))
        return false;
    packed_fingerprints.assign(payload_bytes, 0);

    std::vector<UInt32> count(array_size, 0);
    std::vector<UInt64> xormask(array_size, 0);
    std::vector<size_t> queue;
    queue.reserve(array_size);
    std::vector<PeelEntry> stack;
    stack.reserve(keys.size());

    for (UInt64 key : keys)
    {
        size_t h0 = 0;
        size_t h1 = 0;
        size_t h2 = 0;
        getIndexes3(key, h0, h1, h2);
        ++count[h0]; ++count[h1]; ++count[h2];
        xormask[h0] ^= key; xormask[h1] ^= key; xormask[h2] ^= key;
    }
    for (size_t i = 0; i < array_size; ++i)
        if (count[i] == 1)
            queue.push_back(i);

    for (size_t qi = 0; qi < queue.size(); ++qi)
    {
        const size_t idx = queue[qi];
        if (count[idx] != 1)
            continue;
        const UInt64 key = xormask[idx];
        stack.push_back({key, idx});
        size_t h0 = 0;
        size_t h1 = 0;
        size_t h2 = 0;
        getIndexes3(key, h0, h1, h2);
        for (size_t h : {h0, h1, h2})
        {
            --count[h];
            xormask[h] ^= key;
            if (count[h] == 1)
                queue.push_back(h);
        }
    }
    if (stack.size() != keys.size())
        return false;

    std::fill(packed_fingerprints.begin(), packed_fingerprints.end(), 0);
    for (size_t i = stack.size(); i > 0; --i)
    {
        const auto & e = stack[i - 1];
        size_t h0 = 0;
        size_t h1 = 0;
        size_t h2 = 0;
        getIndexes3(e.key, h0, h1, h2);
        UInt32 v = fingerprintFromKey(e.key);
        if (e.selected != h0) v ^= getSlot(h0);
        if (e.selected != h1) v ^= getSlot(h1);
        if (e.selected != h2) v ^= getSlot(h2);
        setSlot(e.selected, v);
    }
    return true;
}

bool BinaryFuseFilter::tryBuildFuse4(const std::vector<UInt64> & keys, UInt64 seed_candidate)
{
    seed = seed_candidate;
    arity = 4;
    const size_t n = keys.size();
    const double ratio = 0.77 + 0.305 * std::max(1.0, std::log(6e5) / std::log(static_cast<double>(n)));
    const double nd = static_cast<double>(n);
    size_t target = static_cast<size_t>(std::floor(ratio * nd));
    target = std::max(target, static_cast<size_t>(std::floor(1.075 * nd)));
    const double seg_exp = std::floor(std::log(static_cast<double>(n)) / std::log(2.91) - 0.5);
    segment_length = powerOfTwoFromExponentFloor(seg_exp);
    segment_count = std::max<size_t>(4, (target + segment_length - 1) / segment_length);
    if (segment_length != 0 && segment_count > std::numeric_limits<size_t>::max() / segment_length)
        return false;
    array_size = segment_count * segment_length;
    if (array_size > std::numeric_limits<size_t>::max() / bytesPerFingerprint())
        return false;
    const size_t payload_bytes = array_size * bytesPerFingerprint();
    if (payload_bytes > MAX_SERIALIZED_FUSE_PAYLOAD_BYTES)
        return false;
    if (!fuseBuildScratchWithinLimit(array_size, payload_bytes, n))
        return false;
    packed_fingerprints.assign(payload_bytes, 0);

    std::vector<UInt32> count(array_size, 0);
    std::vector<UInt64> xormask(array_size, 0);
    std::vector<size_t> queue;
    queue.reserve(array_size);
    std::vector<PeelEntry> stack;
    stack.reserve(keys.size());

    for (UInt64 key : keys)
    {
        size_t h0 = 0;
        size_t h1 = 0;
        size_t h2 = 0;
        size_t h3 = 0;
        getIndexes4(key, h0, h1, h2, h3);
        ++count[h0]; ++count[h1]; ++count[h2]; ++count[h3];
        xormask[h0] ^= key; xormask[h1] ^= key; xormask[h2] ^= key; xormask[h3] ^= key;
    }
    for (size_t i = 0; i < array_size; ++i)
        if (count[i] == 1)
            queue.push_back(i);

    for (size_t qi = 0; qi < queue.size(); ++qi)
    {
        const size_t idx = queue[qi];
        if (count[idx] != 1)
            continue;
        const UInt64 key = xormask[idx];
        stack.push_back({key, idx});
        size_t h0 = 0;
        size_t h1 = 0;
        size_t h2 = 0;
        size_t h3 = 0;
        getIndexes4(key, h0, h1, h2, h3);
        for (size_t h : {h0, h1, h2, h3})
        {
            --count[h];
            xormask[h] ^= key;
            if (count[h] == 1)
                queue.push_back(h);
        }
    }
    if (stack.size() != keys.size())
        return false;

    std::fill(packed_fingerprints.begin(), packed_fingerprints.end(), 0);
    for (size_t i = stack.size(); i > 0; --i)
    {
        const auto & e = stack[i - 1];
        size_t h0 = 0;
        size_t h1 = 0;
        size_t h2 = 0;
        size_t h3 = 0;
        getIndexes4(e.key, h0, h1, h2, h3);
        UInt32 v = fingerprintFromKey(e.key);
        if (e.selected != h0) v ^= getSlot(h0);
        if (e.selected != h1) v ^= getSlot(h1);
        if (e.selected != h2) v ^= getSlot(h2);
        if (e.selected != h3) v ^= getSlot(h3);
        setSlot(e.selected, v);
    }
    return true;
}

}
