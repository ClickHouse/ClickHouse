#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFormat.h>
#include <base/types.h>
#include <cstdint>
#include <string_view>

namespace DB::Cas
{

/// The two states of the per-hash freshness marker used by the writer's deduplication gate and by GC.
/// This marker is only a point-read hint, not the linearization point for blob lifetime: the body's
/// in-body `incarnation_tag` and the body's exact-token delete provide the safety guarantee. A stale
/// marker can therefore make a writer re-upload conservatively, but it is never authority for deleting
/// the body.
enum class MetaState : uint8_t
{
    Clean = 0,       /// The body is present and may be referenced.
    Condemned = 1,   /// GC observed zero in-degree; the body remains present until exact-token deletion,
                     /// so a writer may republish it by replacing the body and updating this marker.
};

/// Convert a meta-state discriminator to its canonical wire word. Throws `LOGICAL_ERROR` for an
/// out-of-range enum value.
std::string_view metaStateToWireWord(MetaState state);

/// Its fail-closed inverse: an unknown word is `CORRUPTED_DATA`.
MetaState metaStateFromWireWord(std::string_view w);

/// The durable per-hash meta record. Its text representation consists of a format header followed by
/// one JSON object with the state word, the GC condemnation round, and the raw body size. `size` is
/// retained for introspection, fsck, and GC accounting; reads of the blob never consult the meta.
/// Lifecycle transitions are conditional on the backend etag, while the encoded bytes themselves are
/// not compared. The body header's `v` is the authoritative format version, so `version` remains only
/// for the inspection interface and is deliberately not serialized in the JSON body.
struct BlobMeta
{
    uint8_t version = 1;
    MetaState state = MetaState::Clean;
    uint64_t condemn_round = 0;   /// The GC round that condemned this blob; distinguishes a stale
                                  /// condemnation from a later spare-and-recondemn transition.
    uint64_t size = 0;
};

/// Serialize `meta` as the header line and one JSON body line. Invalid `MetaState` values are rejected
/// with `CORRUPTED_DATA`; the body does not contain `version` because the header owns format versioning.
String encodeBlobMeta(const BlobMeta & meta);

/// Decode a stored meta record. The header, required state, field types, and complete input are checked;
/// malformed input, an unknown state, or trailing bytes throws `CORRUPTED_DATA`. Unknown JSON keys are
/// tolerated so the format can add nonessential fields without breaking older readers.
BlobMeta decodeBlobMeta(std::string_view bytes);

}
