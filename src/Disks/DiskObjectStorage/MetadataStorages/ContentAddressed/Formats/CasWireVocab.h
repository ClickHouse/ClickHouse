#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasEtag.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEnumWireTable.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasBlobDigest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasBlobEnvelopeFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <cstdint>
#include <optional>
#include <string_view>

namespace DB::Cas
{

/// Shared JSON vocabulary for the value sub-types embedded by the CAS text codecs. These helpers
/// keep the same sub-object key names and full-word enum values across outcome logs, record streams,
/// ref logs, ref snapshots, part manifests, and blob envelopes. Every reverse map rejects an
/// unrecognized value with `CORRUPTED_DATA`; silently choosing a default would turn malformed
/// persisted data into a different valid-looking record.

/// The incarnation-dialect wire vocabulary; coverage is proven in `CasWireVocab.cpp`. It has two
/// persisted encodings -- the word, used by every JSON codec here, and the one byte the condemned-row
/// payload stores -- and both go through this one table so they can never name different sets.
inline constexpr EnumWireTable<Dialect, 3> kTokenTypeWords{{{
    {Dialect::ETag, "etag"},
    {Dialect::Generation, "generation"},
    {Dialect::Emulated, "emulated"},
}}};

/// The `ObjectKind` wire vocabulary; coverage is proven in `CasWireVocab.cpp`.
inline constexpr EnumWireTable<ObjectKind, 1> kObjectKindWords{{{
    {ObjectKind::Blob, "blob"},
}}};

/// Validate a persisted dialect word and return its canonical spelling. `what` identifies the
/// containing codec or field in the `CORRUPTED_DATA` exception; an unrecognized word is rejected
/// rather than carried into a record no reader could decode.
std::string_view dialectWordFromString(std::string_view w, std::string_view what);

/// The same vocabulary in the one-byte form the condemned-row payload stores. Both directions are
/// fail-closed: an unrecognized word or byte is `CORRUPTED_DATA`.
uint8_t dialectByteFromWord(std::string_view w, std::string_view what);
std::string_view dialectWordFromByte(uint8_t byte, std::string_view what);

/// Parse a canonical blob-hash algorithm word. The write side uses `blobHashAlgoName` directly, so
/// this is its fail-closed inverse. `what` identifies the containing codec or field in the
/// `CORRUPTED_DATA` exception.
BlobHashAlgo blobHashAlgoFromWord(std::string_view w, std::string_view what);

/// Convert an envelope object-kind discriminator to its canonical wire word. Throws `LOGICAL_ERROR`
/// for an out-of-range enum value.
std::string_view objectKindToWord(ObjectKind k);

/// Parse a canonical envelope object-kind word. `what` identifies the containing codec or field in
/// the `CORRUPTED_DATA` exception; unknown words are rejected rather than treated as a default kind.
ObjectKind objectKindFromWord(std::string_view w, std::string_view what);

/// Append the sibling fields `token_type` and `token` to an in-progress JSON object. The caller owns `first`,
/// which must describe the fields already written to that object; the value is JSON-escaped. The
/// dialect is validated on the way out, so a record can never persist a word its reader would reject.
void writeTokenFields(CasJsonWriter & out, bool & first, const PersistedEtag & inc);

/// Append the sibling fields `algo` and `digest` to an in-progress JSON object. The algorithm word and
/// lowercase digest are canonical, and the digest is rendered at the width required by `r.algo`.
void writeBlobRefFields(CasJsonWriter & out, bool & first, const BlobRef & r);

/// The `algo`/`digest` and `token_type`/`token` key spellings, named once so `writeBlobRefFields`/`writeTokenFields`
/// and the `match*Fields` collectors below can never drift apart on the literal.
namespace SharedWire
{
    inline constexpr WireKey algo{"algo"};
    inline constexpr WireKey digest{"digest"};
    inline constexpr WireKey token_type{"token_type"};
    inline constexpr WireKey token{"token"};
}

/// One `ManifestRef`'s three flat key names. Every bundle spells the SAME wire representation
/// (two decimal-string `uint64_t`s and one JSON-number ordinal); only the key names vary per
/// binding role. Member names carry the semantic role the ref plays (`epoch`/`build`/`ord`); the
/// bundle constants below carry the CURRENT wire spelling for each role.
struct ManifestRefWireKeys
{
    WireKey epoch;
    WireKey build;
    WireKey ord;
};

/// The unprefixed `epoch`/`build`/`ord` spelling used by part manifests, snapshot rows, and the
/// `set_published_at` ref-log op.
inline constexpr ManifestRefWireKeys kBareManifestRefKeys{WireKey{"epoch"}, WireKey{"build"}, WireKey{"ord"}};
/// The `old_epoch`/`old_build`/`old_ord` spelling for a ref-log owner_transition's OLD binding.
inline constexpr ManifestRefWireKeys kOldManifestRefKeys{WireKey{"old_epoch"}, WireKey{"old_build"}, WireKey{"old_ord"}};
/// The `new_epoch`/`new_build`/`new_ord` spelling for a ref-log owner_transition's NEW binding.
inline constexpr ManifestRefWireKeys kNewManifestRefKeys{WireKey{"new_epoch"}, WireKey{"new_build"}, WireKey{"new_ord"}};

/// One owner binding's key names: the owner-kind word, the ref name, and its nested `ManifestRef`
/// bundle. Only the ref-log owner_transition op uses this bundle (old/new binding sides).
struct BindingWireKeys
{
    WireKey kind;
    WireKey ref;
    ManifestRefWireKeys manifest;
};

/// The `old_kind`/`old_ref`/`old_epoch`/`old_build`/`old_ord` spelling for the OLD binding side.
inline constexpr BindingWireKeys kOldBindingKeys{WireKey{"old_kind"}, WireKey{"old_ref"}, kOldManifestRefKeys};
/// The `new_kind`/`new_ref`/`new_epoch`/`new_build`/`new_ord` spelling for the NEW binding side.
inline constexpr BindingWireKeys kNewBindingKeys{WireKey{"new_kind"}, WireKey{"new_ref"}, kNewManifestRefKeys};

/// Append the three flat `ManifestRef` fields named by `keys` to an in-progress JSON object. The
/// two unbounded `uint64_t` values are decimal JSON strings; the bounded ordinal is a JSON number.
/// All consumers use this exact representation; only the key spelling varies by `keys`.
void writeManifestRefFields(CasJsonWriter & out, bool & first, const ManifestRefWireKeys & keys, const ManifestRef & r);

/// Construct a `ManifestRef` from decoded field values and validate the complete domain range:
/// nonzero `writer_epoch` and `build_sequence`, and `manifest_ordinal` in
/// `[1, kMaxManifestOrdinal]`. The upper bound is checked before narrowing to the in-memory
/// `uint32_t` ordinal. `caller` and `what` identify the codec and field in `CORRUPTED_DATA`
/// exceptions.
ManifestRef manifestRefFromFields(uint64_t writer_epoch, uint64_t build_sequence, uint64_t manifest_ordinal,
                                  std::string_view caller, std::string_view what);

/// Collector for one `ManifestRef`'s three flat fields, filled in by repeated calls to
/// `matchManifestRefFields` as a tolerant reader walks an object's keys. `buildRef` checks that the
/// group is all-or-nothing complete, then delegates the completed group to `manifestRefFromFields`,
/// which performs the nonzero and range checks.
struct ManifestRefFields
{
    std::optional<uint64_t> epoch;
    std::optional<uint64_t> build;
    std::optional<uint64_t> ord;

    bool any() const { return epoch || build || ord; }

    /// `what` names the codec (passed through to `manifestRefFromFields` as its `caller`); `context`
    /// names the field being reconstructed (e.g. "descriptor", "committed"). Throws `CORRUPTED_DATA`
    /// if the group is not all-or-nothing complete.
    ManifestRef buildRef(std::string_view what, std::string_view context) const;
};

/// Collector for one `BlobRef`'s two flat fields (`algo`/`digest`), filled in by `matchBlobRefFields`.
struct BlobRefFields
{
    std::optional<String> algo_word;
    std::optional<String> digest_hex;

    /// Requires both fields, parses the algorithm word, and checks the digest hex width against the
    /// algorithm's width BEFORE calling `fromHex` -- a width mismatch must surface as `CORRUPTED_DATA`
    /// (malformed persisted input), not `DigestCodec::fromHex`'s `BAD_ARGUMENTS` (a caller-contract
    /// violation). `what` identifies the field in the exception.
    BlobRef build(std::string_view what) const;
};

/// Collector for one persisted incarnation's two flat fields (`token_type`/`token`), filled in by
/// `matchTokenFields`.
struct TokenFields
{
    std::optional<String> type_word;
    std::optional<String> value;

    /// Requires both fields and validates the dialect word. `what` identifies the enclosing codec
    /// in `CORRUPTED_DATA` exceptions.
    PersistedEtag build(std::string_view what) const;
};

/// Each `match*Fields` helper tests `key` against the one or two field names it owns, consumes the
/// value on a match via `r`, and reports whether it recognized the key. None of them loop over an
/// object's keys or validate a completed group -- that is the caller's (tolerant-reader loop) and
/// the collector's `build`/`buildRef` job respectively. Defined inline: a decoder's per-key dispatch
/// is a hot path, so the helpers are header-defined for the per-key dispatch to inline them.

inline bool matchManifestRefFields(std::string_view key, JsonObjectReader & r, const ManifestRefWireKeys & keys, ManifestRefFields & fields)
{
    if (key == keys.epoch) { fields.epoch = r.readU64String(); return true; }
    if (key == keys.build) { fields.build = r.readU64String(); return true; }
    if (key == keys.ord)   { fields.ord = r.readU64Number(); return true; }
    return false;
}

inline bool matchBlobRefFields(std::string_view key, JsonObjectReader & r, BlobRefFields & fields)
{
    if (key == SharedWire::algo)   { fields.algo_word = r.readString(); return true; }
    if (key == SharedWire::digest) { fields.digest_hex = r.readString(); return true; }
    return false;
}

inline bool matchTokenFields(std::string_view key, JsonObjectReader & r, TokenFields & fields)
{
    if (key == SharedWire::token_type) { fields.type_word = r.readString(); return true; }
    if (key == SharedWire::token)      { fields.value = r.readString(); return true; }
    return false;
}

}
