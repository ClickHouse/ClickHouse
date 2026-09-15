#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEnumWireTable.h>
#include <Common/Exception.h>
#include <base/defines.h>
#include <base/extended_types.h>
#include <base/hex.h>
#include <base/types.h>
#include <fmt/format.h>
#include <array>
#include <compare>
#include <cstdint>
#include <cstring>
#include <string_view>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

namespace DB::Cas
{

/// Blob identity for the content-addressed pool: the hash-algorithm vocabulary (`BlobHashAlgo`),
/// the digest value (`BlobDigest`), its width-aware representation converter (`DigestCodec`), and
/// the complete identity pair (`BlobRef`). The streaming machinery that PRODUCES digests lives in
/// `CasBlobHashingWriteBuffer.h`; this header is dependency-light on purpose — it is included by
/// virtually every CAS translation unit through `CasTypes.h`.

/// The content-address hash function selected for a blob pool. The numeric values are persisted as a
/// byte in the binary source-edge run format and must remain stable; the textual name used in object
/// paths and pool metadata is returned by `blobHashAlgoName`.
///
/// `CityHash128` and `XXH3_128` produce 16-byte digests, while `Sha256` produces a 32-byte digest.
/// The digest representation and codec derive their width from this algorithm for each blob; there
/// is no single pool-wide digest width when a pool admits more than one algorithm.
enum class BlobHashAlgo : uint8_t
{
    CityHash128 = 1,
    XXH3_128 = 2,
    Sha256 = 3,
};

/// The `BlobHashAlgo` wire vocabulary (also the blob PATH SEGMENT, e.g.
/// `<pool>/blobs/<algo>/<shard>/<hex>`); coverage is proven in `CasBlobDigest.cpp`.
inline constexpr EnumWireTable<BlobHashAlgo, 3> kBlobHashAlgoWords{{{
    {BlobHashAlgo::CityHash128, "ch128"},
    {BlobHashAlgo::XXH3_128, "xxh3"},
    {BlobHashAlgo::Sha256, "sha256"},
}}};

/// The blob PATH SEGMENT for `algo`, e.g. `<pool>/blobs/<algo>/<shard>/<hex>`: `"ch128"` | `"xxh3"` |
/// `"sha256"`. Throws `LOGICAL_ERROR` for an out-of-range enum value.
std::string_view blobHashAlgoName(BlobHashAlgo algo);

/// Returns the digest byte width for `algo`: 16 for `CityHash128` and `XXH3_128`, or 32 for
/// `Sha256`. This is also the width used by `Cas::codecFor(algo)`'s `DigestCodec`; callers must
/// derive it from the algorithm rather than from pool state. The functions over `BlobHashAlgo`
/// intentionally use different defensive codes: this one throws `BAD_ARGUMENTS` for an out-of-range
/// enum value, while `blobHashAlgoName` throws `LOGICAL_ERROR`.
uint64_t blobHashLenFor(BlobHashAlgo algo);

/// Parses the per-disk `blob_hash` CONFIG value: `"cityhash128"` | `"xxh3-128"` | `"sha256"`. Throws
/// `BAD_ARGUMENTS` on any other value (fail-closed).
BlobHashAlgo parseBlobHashAlgo(std::string_view config_value);

/// A content digest whose width is selected by its hash algorithm. The fixed 32-byte big-endian
/// storage accommodates the existing 128-bit algorithms (`cityHash128` and `xxh3-128`) and
/// `sha256`; only the first `blobHashLenFor(algo)` bytes are meaningful and the remaining bytes
/// must be zero. A fixed array avoids a per-manifest-entry allocation that a variable `String`
/// would require for 32-byte digests.
///
/// This type is reserved for content hashes. Protocol identifiers such as `payload_digest`,
/// `RunRef::checksum`, source-edge identifiers, lease owners, and cleanup shards remain
/// `UInt128`, because widening the content digest does not change their separate wire or ordering
/// contracts. A blob's complete identity is `BlobRef`, which pairs this digest with its algorithm.
struct BlobDigest
{
    std::array<uint8_t, 32> bytes{};

    auto operator<=>(const BlobDigest &) const = default;
    bool operator==(const BlobDigest &) const = default;

    /// Converts a 128-bit content hash to the common representation: big-endian in `bytes[0:16]`
    /// and zero in the tail. This is the bridge for the 128-bit hash algorithms.
    static BlobDigest fromU128(const UInt128 & v)
    {
        BlobDigest d;
        for (int i = 0; i < 16; ++i)
            d.bytes[static_cast<size_t>(i)] = static_cast<uint8_t>(static_cast<UInt8>(v >> (8 * (15 - i))));
        return d;
    }

    /// Reads `bytes[0:16]` as big-endian into a `UInt128`. The conversion is meaningful only for a
    /// 128-bit digest; the caller is responsible for selecting that width and the tail is ignored.
    UInt128 toU128() const
    {
        UInt128 v = 0;
        for (int i = 0; i < 16; ++i)
            v = (v << 8) | static_cast<UInt8>(bytes[static_cast<size_t>(i)]);
        return v;
    }
};

/// Hasher for `BlobDigest` as an `unordered_map`/`unordered_set` key. This is an in-process hash
/// table key, not a content address, so a cheap FNV-1a mix over the raw bytes is sufficient -- no
/// cryptographic property is needed here.
struct BlobDigestHash
{
    size_t operator()(const BlobDigest & d) const noexcept
    {
        size_t h = 1469598103934665603ull; /// FNV-1a 64-bit offset basis
        for (uint8_t b : d.bytes)
        {
            h ^= b;
            h *= 1099511628211ull; /// FNV-1a 64-bit prime
        }
        return h;
    }
};

/// Converts a `BlobDigest` using one algorithm's width. A codec must be obtained from the algorithm
/// through `codecFor`, never from a pool-wide width: a pool may contain multiple algorithms. Hex
/// and raw-byte conversions accept and produce exactly the selected width. `shardOf` reads the
/// first eight digest bytes in big-endian order, preserving the existing shard mapping for every
/// 128-bit digest.
class DigestCodec
{
public:
    /// Creates a codec for a supported digest width: 16 bytes for the 128-bit algorithms or
    /// 32 bytes for `sha256`. Any other width violates the per-algorithm representation invariant.
    explicit DigestCodec(uint64_t digest_len_) : len(digest_len_)
    {
        chassert(len == 16 || len == 32, "DigestCodec: digest length must be 16 or 32 bytes");
    }

    /// Renders exactly `2 * len` lowercase hex chars.
    String toHex(const BlobDigest & d) const
    {
        checkZeroTail(d, "toHex");
        return hexString(d.bytes.data(), len);
    }

    /// Requires exactly `2 * len` hex chars; throws `BAD_ARGUMENTS` otherwise (wrong
    /// width or a non-hex character). Zero-fills the tail beyond `len`.
    BlobDigest fromHex(std::string_view hex) const
    {
        if (hex.size() != 2 * len)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "DigestCodec::fromHex: expected {} hex chars for a {}-byte digest, got {}",
                2 * len, len, hex.size());

        for (char c : hex)
        {
            if (unhex(c) == 0xff)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "DigestCodec::fromHex: invalid hex character '{}'", c);
        }

        BlobDigest d;
        for (uint64_t i = 0; i < len; ++i)
            d.bytes[i] = unhex2(hex.data() + i * 2);
        return d;
    }

    /// Serializes exactly `len` raw bytes, big-endian (i.e. `bytes[0:len]`).
    String toBytesBE(const BlobDigest & d) const
    {
        checkZeroTail(d, "toBytesBE");
        return String(reinterpret_cast<const char *>(d.bytes.data()), len);
    }

    /// Requires exactly `len` bytes; throws `BAD_ARGUMENTS` otherwise. Zero-fills the
    /// tail beyond `len`.
    BlobDigest fromBytesBE(std::string_view b) const
    {
        if (b.size() != len)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "DigestCodec::fromBytesBE: expected {} bytes for the pool's digest width, got {}", len, b.size());

        BlobDigest d;
        memcpy(d.bytes.data(), b.data(), len);
        return d;
    }

    /// Returns the first eight digest bytes as a big-endian `uint64_t`. Keep this explicit rather
    /// than using a native-endian `memcpy`: changing the byte order would silently remap shards on
    /// little-endian hosts and break compatibility with the 128-bit hash mapping.
    uint64_t shardOf(const BlobDigest & d) const
    {
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i)
            v = (v << 8) | d.bytes[static_cast<size_t>(i)];
        return v;
    }

private:
    uint64_t len;

    /// Checks the representation invariant that bytes beyond the selected width are zero. This is
    /// a debug-only internal assertion; wrong-width external input is rejected by the decoding
    /// methods above.
    void checkZeroTail(const BlobDigest & d, [[maybe_unused]] const char * what) const
    {
        for (uint64_t i = len; i < d.bytes.size(); ++i)
            chassert(d.bytes[i] == 0, fmt::format("DigestCodec::{}: non-zero byte at tail position {} (len={})", what, i, len));
    }
};

/// The complete blob identity is the pair of hash algorithm and
/// digest. A bare digest is NOT a blob identity anywhere -- `ch128` and `xxh3` digests are both
/// 16-byte, so the same digest value under two algos names two DIFFERENT objects. BlobRef is
/// constructed ONLY where algo and digest are born together (the write mint / the hasher) or read
/// together (a durable form: settlement key, blob path, manifest entry, envelope). Every other
/// site COPIES BlobRefs -- never assemble one from an algo and a digest obtained separately.
struct BlobRef
{
    BlobHashAlgo algo = BlobHashAlgo::CityHash128;
    BlobDigest digest{};

    auto operator<=>(const BlobRef &) const = default;
    bool operator==(const BlobRef &) const = default;
};

/// Hasher for unordered_map/unordered_set keys (in-process only, not a content address).
struct BlobRefHash
{
    size_t operator()(const BlobRef & r) const noexcept
    {
        size_t h = BlobDigestHash{}(r.digest);
        h ^= static_cast<size_t>(r.algo) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        return h;
    }
};

/// Returns the codec whose width belongs to `algo`; callers must not substitute a pool-wide width.
inline DigestCodec codecFor(BlobHashAlgo algo)
{
    return DigestCodec(blobHashLenFor(algo));
}

/// Bare lowercase hex of the digest at the algo's width -- for OBJECT KEY construction only
/// (the algo lives in the key's path segment `blobs/<algo>/...`).
inline String blobHexOf(const BlobRef & r)
{
    return codecFor(r.algo).toHex(r.digest);
}

/// Human/log identity: "<algoName>:<hex>", e.g. "sha256:ab12...". Rendered ids must never be a
/// bare hex (ambiguous across algos) -- events, inspect JSON and error messages use this.
inline String blobIdOf(const BlobRef & r)
{
    return String(blobHashAlgoName(r.algo)) + ":" + blobHexOf(r);
}

}
