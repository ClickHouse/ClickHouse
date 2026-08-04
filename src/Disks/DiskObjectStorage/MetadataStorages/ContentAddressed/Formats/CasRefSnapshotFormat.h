#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefWireVocab.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefLogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <base/types.h>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

namespace DB::Cas
{

/// Text codec for `cas_ref_snap`, the complete per-namespace ref table snapshot at
/// `_snap/<snapshot_id>`. The object is read whole rather than streamed and belongs to the Control
/// family: callers store the encoded text as an Always/`.zst` object. Its canonical text consists of
/// a header, a metadata line, committed and precommit row lines, and a `{"n":count}` trailer.
///
/// There is no such thing as a "seal snapshot". An epoch is closed IN-BAND, by an `EpochSeal`
/// transaction the recovery CAS-walk places at `{E, T+1}` in the `_log` stream (INV-2) -- the exact key
/// a dying predecessor's in-flight PUT would have taken, so the store's write-once create is the fence.
/// The retired alternative was a synthetic snapshot at `{E-1, UINT64_MAX}` carrying a `sealed_from`
/// bound: it occupied no log key, so it fenced nothing and needed a separate after-the-fact detector for
/// the writes it let through. All `RefTxnId` components are still encoded as decimal strings. Rows are
/// emitted in canonical order, making re-encoding deterministic by construction; these objects are
/// published through the ordinary single-owner `putIfAbsentControlled` path, not a
/// `putDeterministicArtifact` byte-adoption gate.

/// In-memory ref-table lifecycle. Only `Live` is serializable as a generation-8 snapshot; terminal
/// state lives in the removal log and fold evidence and has no snapshot DTO representation.
enum class RefLifecycle : uint8_t
{
    Live = 1,
    Removed = 2,
};

/// One committed ref-name-to-manifest row in a `RefTableSnapshot`. `published_at_ms` is the only
/// mutable metadata field a committed row carries.
struct RefCommittedRow
{
    String ref_name;
    ManifestRef manifest_ref;
    uint64_t published_at_ms = 0;

    bool operator==(const RefCommittedRow &) const = default;
};

/// The complete state of one namespace's ref table in one canonical snapshot object. `precommits`
/// reuses `RefOwnerBinding` from `CasRefWireVocab.h`; every entry's `kind` must be `Precommit`.
/// Generation 8 serializes only `Live` snapshots. Both row vectors must already be strictly sorted by
/// their documented keys, because the codec
/// validates and emits the caller-provided order rather than sorting it.
struct RefTableSnapshot
{
    String ns;
    RefTxnId snapshot_id;
    std::vector<RefCommittedRow> committed;     /// sorted by canonical bytewise ref_name, no duplicates
    std::vector<RefOwnerBinding> precommits;    /// sorted by (ref_name, manifest_ref), no duplicates

    bool operator==(const RefTableSnapshot &) const = default;
};

/// Hard encoded-size limit over the uncompressed text. The snapshot reuses the removal-class
/// complete-table budget from `CasRefLogFormat.h`.
inline constexpr size_t ref_snapshot_max_bytes = ref_removal_max_bytes;

/// Encode to the canonical text (not sealed): the caller compresses via
/// `sealObject(FormatId::RefSnapshot, …)` on the persist path (Always/`.zst`), and the in-memory
/// validation and `admits` size-estimate callers use the uncompressed text. Throws
/// CORRUPTED_DATA on: a zero `snapshot_id`; a non-canonical `ref_name`; an
/// out-of-range `manifest_ref`; non-strictly-ascending
/// `committed` / `precommits`; a `precommits` entry not `Precommit`; or an over-budget object.
String encodeRefTableSnapshot(const RefTableSnapshot & snapshot);

/// Decode the canonical text (the caller `openObject`s the stored `.zst` first). `expected_ns` /
/// `expected_snapshot_id` are recovered from the object key; the decoded body must equal them (the
/// key↔body binding). Throws UNKNOWN_FORMAT_VERSION for a header `v` above this build, CORRUPTED_DATA
/// for truncation, a missing/non-`live` lifecycle word, either retired `rte`/`rts` field, an unknown
/// owner kind, or any validation failure listed above.
RefTableSnapshot decodeRefTableSnapshot(
    std::string_view data, const String & expected_ns, const RefTxnId & expected_snapshot_id);

/// Encoded byte size of exactly one committed row line, as `encodeRefTableSnapshot` would emit it.
/// Reuses the same writer, so it is byte-identical to that row's contribution to a full encode.
size_t committedRowEncodedSize(const RefCommittedRow & row);

/// Encoded byte size of exactly one precommit row line, as `encodeRefTableSnapshot` would emit it.
size_t precommitRowEncodedSize(const RefOwnerBinding & binding);

/// Encoded byte size of a snapshot's framing (header + meta line + trailer) for the given metadata and
/// row count, excluding all row lines. `snapshotFramingSize(...) + Σ committedRowEncodedSize +
/// Σ precommitRowEncodedSize` equals `encodeRefTableSnapshot(...).size()` exactly.
size_t snapshotFramingSize(const String & ns, const RefTxnId & snapshot_id, uint64_t row_count);

}
