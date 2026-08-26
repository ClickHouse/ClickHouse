#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasBlobDigest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasBlobEnvelopeFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <cstdint>
#include <string_view>

namespace DB::Cas
{

/// Shared JSON vocabulary for the value sub-types embedded by the CAS text codecs. These helpers
/// keep the same sub-object key names and full-word enum values across outcome logs, record streams,
/// ref logs, ref snapshots, part manifests, and blob envelopes. Every reverse map rejects an
/// unrecognized value with `CORRUPTED_DATA`; silently choosing a default would turn malformed
/// persisted data into a different valid-looking record.

/// Convert a token discriminator to its canonical wire word. Throws `CORRUPTED_DATA` if `t` is not
/// one of the token types understood by this build.
std::string_view tokenTypeToWord(TokenType t);

/// Parse a canonical token-type word. `what` identifies the containing codec or field in the
/// `CORRUPTED_DATA` exception; unknown words are rejected rather than treated as a default type.
TokenType tokenTypeFromWord(std::string_view w, std::string_view what);

/// Parse a canonical blob-hash algorithm word. The write side uses `blobHashAlgoName` directly, so
/// this is its fail-closed inverse. `what` identifies the containing codec or field in the
/// `CORRUPTED_DATA` exception.
BlobHashAlgo blobHashAlgoFromWord(std::string_view w, std::string_view what);

/// Convert an envelope object-kind discriminator to its canonical wire word. Throws
/// `CORRUPTED_DATA` if `k` is not represented by this format.
std::string_view objectKindToWord(ObjectKind k);

/// Parse a canonical envelope object-kind word. `what` identifies the containing codec or field in
/// the `CORRUPTED_DATA` exception; unknown words are rejected rather than treated as a default kind.
ObjectKind objectKindFromWord(std::string_view w, std::string_view what);

/// Append the sibling fields `tt` and `tv` to an in-progress JSON object. The caller owns `first`,
/// which must describe the fields already written to that object; the token value is JSON-escaped.
void writeTokenFields(CasJsonWriter & out, bool & first, const Token & t);

/// Append the sibling fields `ha` and `h` to an in-progress JSON object. The algorithm word and
/// lowercase digest are canonical, and the digest is rendered at the width required by `r.algo`.
void writeBlobRefFields(CasJsonWriter & out, bool & first, const BlobRef & r);

/// Append the three flat `ManifestRef` fields `me`, `mb`, and `mo` to an in-progress JSON object.
/// `prefix` is prepended to each key, allowing the ref codecs to distinguish old and new owner
/// bindings (`ome`/`omb`/`omo` and `nme`/`nmb`/`nmo`) while part manifests and ordinary rows use an
/// empty prefix. The two unbounded `uint64_t` values are decimal JSON strings; the bounded ordinal
/// is a JSON number. All consumers use this exact spelling and representation.
void writeManifestRefFields(CasJsonWriter & out, bool & first, std::string_view prefix, const ManifestRef & r);

/// Construct a `ManifestRef` from decoded field values and validate the complete domain range:
/// nonzero `writer_epoch` and `build_sequence`, and `manifest_ordinal` in
/// `[1, kMaxManifestOrdinal]`. The upper bound is checked before narrowing to the in-memory
/// `uint32_t` ordinal. `caller` and `what` identify the codec and field in `CORRUPTED_DATA`
/// exceptions.
ManifestRef manifestRefFromFields(uint64_t writer_epoch, uint64_t build_sequence, uint64_t manifest_ordinal,
                                  std::string_view caller, std::string_view what);

}
