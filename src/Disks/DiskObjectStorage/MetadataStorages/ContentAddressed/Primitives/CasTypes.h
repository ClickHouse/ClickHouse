#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasBlobDigest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFormat.h>
#include <base/defines.h>
#include <base/extended_types.h>
#include <base/hex.h>
#include <base/types.h>
#include <Common/Exception.h>
#include <array>
#include <charconv>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <fmt/format.h>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}
}

namespace DB::Cas
{

/// Strongly typed value types and codecs shared by the content-addressed metadata and object paths.
///
/// These types deliberately keep protocol identity, content digests, backend tokens, and their
/// textual forms distinct. Their ordering and equality operators make them usable as ordered keys;
/// hash specializations below support unordered containers. Conversion to a storage key or wire
/// representation remains explicit at the boundary that owns that representation.
///
/// Opaque namespace under which root manifests live. The core never interprets its contents:
/// the wiring composes strings like "srv1/<table_uuid>" or "shadow/<backup>/<table_uuid>".
/// Layout only validates its shape (non-empty, no leading/trailing or empty segments,
/// no segment equal to the reserved "_files").
/// Construction from `String` is explicit, preventing unrelated identifier types from being mixed;
/// the underlying string is exposed only through `string` at object-storage boundaries.
class RootNamespace
{
public:
    RootNamespace() = default;
    explicit RootNamespace(String value_) : value(std::move(value_)) {}
    const String & string() const { return value; }
    auto operator<=>(const RootNamespace &) const = default;
    bool operator==(const RootNamespace &) const = default;

private:
    String value;
};

/// Returns 32 lowercase hex chars encoding `v`.
inline String u128ToHex(const UInt128 & v)
{
    return getHexUIntLowercase(v);
}

/// Parses 32-char lowercase (or uppercase) hex string to UInt128.
/// Throws BAD_ARGUMENTS on wrong length or non-hex characters.
inline UInt128 hexToU128(const String & hex)
{
    if (hex.size() != 32)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS,
            "hexToU128: expected 32 hex chars, got {}", hex.size());

    // Validate each character is a valid hex digit (unhex(c) returns 0xff for invalid chars).
    for (char c : hex)
    {
        if (::unhex(c) == 0xff)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS,
                "hexToU128: invalid hex character '{}'", c);
    }

    return unhexUInt<UInt128>(hex.data());
}

}

namespace std
{

template <>
struct hash<DB::Cas::RootNamespace>
{
    size_t operator()(const DB::Cas::RootNamespace & value) const noexcept
    {
        return std::hash<std::string>{}(value.string());
    }
};

}

namespace DB::Cas
{

/// The compact reference a root journal stores for a part manifest. It is not a string key:
/// `CasLayout::manifestKey` derives the object key from this reference and the owning namespace.
/// The namespace is deliberately absent because it comes from the owning root context and must not
/// be serialized into the journal reference.
///
///   writer_epoch         - durable monotone writer epoch allocated under the mounted `server_root_id`;
///                          never reused for that server root.
///   build_sequence       - monotone build sequence inside one writer incarnation; part of identity
///                          and of the build-scoped debris prefix.
///   manifest_ordinal     - monotone ordinal inside one build, rendered as `000001.zst` in the key.
struct ManifestRef
{
    uint64_t writer_epoch = 0;
    uint64_t build_sequence = 0;
    uint32_t manifest_ordinal = 0;

    bool operator==(const ManifestRef & o) const = default;

    /// Total order for std::map / std::set keys. Field order is arbitrary but stable.
    bool operator<(const ManifestRef & o) const
    {
        return std::tie(writer_epoch, build_sequence, manifest_ordinal)
             < std::tie(o.writer_epoch, o.build_sequence, o.manifest_ordinal);
    }
};

/// The namespace-qualified protocol identity used by GC for source edges, blob deltas, cleanup work,
/// and addressing. It is the pair `(root_namespace, ManifestRef)`.
/// Two namespaces may legally carry the same `ManifestRef` tuple without addressing the same object;
/// therefore those structures must use `ManifestId`, never `ManifestRef` alone.
struct ManifestId
{
    RootNamespace root_namespace;   /// owning namespace; NOT part of the journal ref
    ManifestRef ref;

    bool operator==(const ManifestId & o) const = default;

    bool operator<(const ManifestId & o) const
    {
        if (root_namespace.string() != o.root_namespace.string())
            return root_namespace.string() < o.root_namespace.string();
        return ref < o.ref;
    }
};

inline constexpr uint32_t kMaxManifestOrdinal = 999999;

/// Six-digit filename for a per-build part-manifest ordinal: `000001.zst` through `999999.zst`
/// (the registered v3 stored suffix for `FormatId::PartManifest`). `0` is reserved as an invalid
/// sentinel and is never emitted.
inline String manifestOrdinalFileName(uint32_t manifest_ordinal)
{
    if (manifest_ordinal == 0 || manifest_ordinal > kMaxManifestOrdinal)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS,
            "Manifest ordinal must be in [1, {}], got {}", kMaxManifestOrdinal, manifest_ordinal);
    return fmt::format("{:06}{}", manifest_ordinal, storedSuffix(FormatId::PartManifest));
}

/// The canonical `writer_epoch:build_sequence:manifest_ordinal` text form of a manifest reference.
/// It is not an object-key encoding: keys are derived by `CasLayout::manifestKey`. Besides logs and
/// diagnostics it is the wire form of the relink confirm token (spec §confirm-primitive), so
/// `tryParseManifestRef` below is its exact inverse and the two must be changed together.
inline String manifestRefDebugString(const ManifestRef & ref)
{
    return fmt::format("{}:{}:{}", ref.writer_epoch, ref.build_sequence, ref.manifest_ordinal);
}

/// Parses the canonical text form produced by `manifestRefDebugString` back into a reference.
///
/// Everything that is not EXACTLY three colon-separated decimal fields in range is `nullopt`. The text
/// arrives from a remote peer in the relink confirm token, so it is untrusted: `from_chars` is used
/// precisely because it accepts no sign, no whitespace and no partial consumption, and the ordinal is
/// range-checked the same way `manifestOrdinalFileName` checks it (`0` is the reserved invalid
/// sentinel, never emitted, so a token carrying it can only be malformed or forged). A refusal here is
/// never a `No`: the caller cannot tell what was asked, which is an ambiguity, not knowledge.
inline std::optional<ManifestRef> tryParseManifestRef(std::string_view text)
{
    const auto parse_field = [](std::string_view field, auto & out) -> bool
    {
        if (field.empty())
            return false;
        const auto * const begin = field.data();
        const auto * const end = field.data() + field.size();
        const auto result = std::from_chars(begin, end, out);
        return result.ec == std::errc{} && result.ptr == end;
    };

    const size_t first = text.find(':');
    if (first == std::string_view::npos)
        return std::nullopt;
    const size_t second = text.find(':', first + 1);
    if (second == std::string_view::npos || text.find(':', second + 1) != std::string_view::npos)
        return std::nullopt;

    ManifestRef ref;
    if (!parse_field(text.substr(0, first), ref.writer_epoch)
        || !parse_field(text.substr(first + 1, second - first - 1), ref.build_sequence)
        || !parse_field(text.substr(second + 1), ref.manifest_ordinal))
        return std::nullopt;
    if (ref.manifest_ordinal == 0 || ref.manifest_ordinal > kMaxManifestOrdinal)
        return std::nullopt;
    return ref;
}

}

/// Hash specializations for the identity types. Each combines exactly the fields used by its
/// corresponding `operator==`, so equal keys always have equal hashes.
namespace std
{

template <>
struct hash<DB::Cas::ManifestRef>
{
    size_t operator()(const DB::Cas::ManifestRef & r) const
    {
        const size_t h1 = std::hash<uint64_t>{}(r.writer_epoch);
        const size_t h2 = std::hash<uint64_t>{}(r.build_sequence);
        const size_t h3 = std::hash<uint32_t>{}(r.manifest_ordinal);
        size_t h = h1;
        h ^= h2 + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        h ^= h3 + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        return h;
    }
};

template <>
struct hash<DB::Cas::ManifestId>
{
    size_t operator()(const DB::Cas::ManifestId & id) const
    {
        const size_t h1 = std::hash<::String>{}(id.root_namespace.string());
        const size_t h2 = std::hash<DB::Cas::ManifestRef>{}(id.ref);
        return h1 ^ (h2 + 0x9e3779b97f4a7c15ULL + (h1 << 6) + (h1 >> 2));
    }
};

}

namespace DB::Cas
{

/// How a backend identifies one physical incarnation of an object key.
enum class TokenType : uint8_t
{
    ETag = 1,        /// S3-family / Azure
    Generation = 2,  /// GCS (binding deferred; fail-closed until probed)
    Emulated = 3,    /// test backends (in-memory fake, Local emulation)
};

/// A backend-native incarnation token. Opaque; sent back to the backend EXACTLY as observed.
struct Token
{
    String value;
    TokenType type = TokenType::ETag;

    bool empty() const { return value.empty(); }
    bool operator==(const Token &) const = default;
};

/// The ordered ref-transaction identifier. A successful writer mount establishes a strictly newer
/// `writer_epoch`; within an epoch, one namespace's `ref_sequence` values are CONTIGUOUS from 1 --
/// derived per append from the table's own greatest applied id (`nextRefTxnId`), not drawn from any
/// counter, so two namespaces of the same mount both count 1, 2, 3... independently. Both fields
/// are nonzero for a valid id -- {0, 0} is never a real transaction. `writer_epoch` is the primary
/// ordering component, so tuple order matches the intended timeline even across an epoch restart that
/// resets `ref_sequence` back to one.
struct RefTxnId
{
    uint64_t writer_epoch = 0;
    uint64_t ref_sequence = 0;

    auto operator<=>(const RefTxnId &) const = default;
};

/// Renders the canonical form: two fixed-width, lower-case, 16-digit hexadecimal numbers joined by
/// '-' (e.g. "0000000000000007-000000000000008e"). Lexical order of the render equals tuple order of
/// `id`, because '-' (0x2d) sorts below every hex digit character and both fields are fixed-width.
/// Throws LOGICAL_ERROR if either field is zero: this render becomes an object key, and an invalid id
/// must never silently produce a well-formed-looking one.
inline String renderRefTxnId(const RefTxnId & id)
{
    if (id.writer_epoch == 0 || id.ref_sequence == 0)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR,
            "RefTxnId: writer_epoch and ref_sequence must both be nonzero, got {}-{}",
            id.writer_epoch, id.ref_sequence);
    return getHexUIntLowercase(id.writer_epoch) + "-" + getHexUIntLowercase(id.ref_sequence);
}

/// Parses the canonical form only: exactly 33 characters, '-' at index 16, exactly 16 lower-case hex
/// digits ('0'-'9', 'a'-'f') either side, and both parsed fields nonzero. Any other shape -- short,
/// long, upper-case, non-hex, misplaced separator, or a zero component -- returns nullopt rather than
/// throwing, since parsing an untrusted listed key is an ordinary "is this ours" question.
inline std::optional<RefTxnId> parseRefTxnId(std::string_view s)
{
    constexpr size_t kFieldLen = 16;
    constexpr size_t kTotalLen = kFieldLen * 2 + 1;
    if (s.size() != kTotalLen || s[kFieldLen] != '-')
        return std::nullopt;

    /// Strict lower-case-hex-only parse: `unhexUInt` (base/hex.h) also accepts upper-case, which the
    /// canonical form must reject, so digits are validated and accumulated by hand here.
    const auto parseField = [](std::string_view field) -> std::optional<uint64_t>
    {
        uint64_t value = 0;
        for (char c : field)
        {
            uint64_t digit = 0;
            if (c >= '0' && c <= '9')
                digit = static_cast<uint64_t>(c - '0');
            else if (c >= 'a' && c <= 'f')
                digit = static_cast<uint64_t>(c - 'a') + 10;
            else
                return std::nullopt;
            value = (value << 4) | digit;
        }
        return value;
    };

    const auto epoch = parseField(s.substr(0, kFieldLen));
    const auto seq = parseField(s.substr(kFieldLen + 1, kFieldLen));
    if (!epoch || !seq || *epoch == 0 || *seq == 0)
        return std::nullopt;
    return RefTxnId{*epoch, *seq};
}

}
