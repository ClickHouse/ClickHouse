#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Common/Exception.h>
#include <base/types.h>
#include <optional>
#include <string_view>
#include <utility>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

namespace DB::Cas
{

/// Opaque pool-wide physical identity of one namespace life. The catalog's existing `incarnation`
/// field is this value; the alias makes listed-key APIs explicit without renaming catalog wire data.
using NamespaceLifePhysicalId = UInt128;

/// Builds the key segment for `incarnation`: 32 fixed-width lower-case hex digits, so the segment has
/// one canonical spelling and `<life_id>/` sorts stably. Throws `LOGICAL_ERROR` on a zero incarnation
/// for the same reason `renderRefTxnId` does: this render becomes an object key, and an invalid
/// identity must never silently produce a well-formed-looking one.
inline String renderIncarnation(const UInt128 & incarnation)
{
    if (incarnation == 0)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR,
            "NamespaceLifeId: incarnation must be nonzero -- 0 never names a life");
    return u128ToHex(incarnation);
}

/// Inverse of `renderIncarnation` for ONE listed path segment. Accepts the canonical form only:
/// exactly 32 lower-case hex digits encoding a nonzero value. Upper case, a short or long segment,
/// non-hex characters and an all-zero segment all return `std::nullopt`; the CALLER decides whether
/// that is "not one of our keys" or corruption, because only the caller knows whether the rest of the
/// key already identified the object as ours.
inline std::optional<UInt128> parseIncarnation(std::string_view s)
{
    constexpr size_t kHexLen = 32;
    if (s.size() != kHexLen)
        return std::nullopt;

    /// `unhexUInt` also accepts upper case, which the canonical form must reject, so the digits are
    /// validated by hand first and only then handed to it.
    for (char c : s)
    {
        const bool canonical_digit = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
        if (!canonical_digit)
            return std::nullopt;
    }

    /// `unhexUInt` reads exactly sizeof(UInt128)*2 == kHexLen bytes from the pointer, already
    /// validated above; it needs no NUL terminator.
    // NOLINTNEXTLINE(bugprone-suspicious-stringview-data-usage)
    const UInt128 value = unhexUInt<UInt128>(s.data());
    if (value == 0)
        return std::nullopt;
    return value;
}

class Layout;

/// The logical and physical identity of ONE LIFE of a namespace: the catalog name plus its opaque,
/// pool-wide incarnation. Physical life-owned keys use only `incarnation`; the name remains the
/// catalog authority used after a listed physical id is joined through one immutable catalog cut.
///
/// Part manifests and loose mountpoint objects are deliberately NOT qualified (Constraint 12,
/// directive §2): manifests already carry globally unique build identities, and a loose mountpoint
/// object is outside namespace ownership altogether.
///
/// There is deliberately NO conversion from a bare `RootNamespace`, none to one, and no default
/// construction: code holding only the name cannot name a ref object or a namespace file at all, so
/// losing the incarnation is a compile error rather than a runtime aliasing bug (spec §2 r9-3). The
/// call sites that legitimately need the bare name -- manifest identities, loose mountpoint objects --
/// say `.ns`, and are visible in review because they say it.
///
/// Permanent logical/physical pairs come only from `fromCatalogEntry` (recovery, fold, fsck and the
/// sweeps). A held reader (e.g. `RefTableRuntime`) keeps the `NamespaceLifeId` it resolved this way
/// at table-open time and threads it through, rather than re-deriving it. `Layout` parsers
/// deliberately return only an untrusted `NamespaceLifePhysicalId`; a listed key can name any
/// physical id, including one no longer in the catalog, and only one immutable catalog cut may
/// attach a logical name to it.
struct NamespaceLifeId
{
    RootNamespace ns;
    NamespaceLifePhysicalId incarnation;   /// 0 is INVALID -- never a wildcard, never "any life"

    bool operator==(const NamespaceLifeId &) const = default;

    /// The catalog is the universe authority (INV-3): a discovery path learns of a namespace ONLY
    /// from a catalog entry, and takes BOTH fields from that same entry. Pairing a namespace with an
    /// incarnation obtained from anywhere else re-opens the rebirth alias this type exists to close.
    static NamespaceLifeId fromCatalogEntry(RootNamespace ns, const UInt128 & incarnation)
    {
        return NamespaceLifeId{std::move(ns), incarnation};
    }

private:
    /// Kept private so only the explicit factories above can construct a logical/physical pair.
    friend class Layout;

    NamespaceLifeId(RootNamespace ns_, const UInt128 & incarnation_)
        : ns(std::move(ns_)), incarnation(incarnation_)
    {
        if (incarnation_ == 0)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR,
                "NamespaceLifeId: incarnation must be nonzero for namespace '{}' -- 0 never names a life",
                ns.string());
    }
};

}
