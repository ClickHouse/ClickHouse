#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequestControl.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasBlobMetaFormat.h>

#include <optional>

namespace DB::Cas
{

class Pool;

/// A decoded blob meta record together with the backend token observed for the same incarnation.
/// The token is returned with the decoded record because the next conditional update or exact delete
/// must be guarded by the version that was actually read; comparing encoded meta bytes would not
/// provide that protection.
struct LoadedMeta
{
    BlobMeta meta;
    Token etag;
};

/// Shared lifecycle operations for the blob freshness marker used by the writer and GC. The key is
/// built from the complete `BlobRef`, so each algorithm uses its own digest representation and no
/// pool-wide digest width is threaded through these functions. The marker is a point-read hint rather
/// than the blob lifetime's linearization point: the blob body's incarnation tag and exact-token body
/// deletion provide the safety guarantee, while a stale marker can at most make a writer re-upload.
///
/// `loadMeta` is used in the adopt path, so its backend must provide strong read-after-write
/// consistency: after a successful meta write, the one subsequent GET must observe that write.
/// Conditional updates and deletion use the backend token, not the encoded meta bytes.
///
/// Returns the current decoded marker and its conditional token, or nullopt when the meta key is
/// absent. Decoding errors propagate as exceptions.
std::optional<LoadedMeta> loadMeta(Backend & backend, const Layout & layout, const BlobRef & ref);

/// Creates the marker only when its key is absent, controlled: a SlowDown/429/5xx on the attempt is
/// resolved-and-reissued within budget rather than escaping as a raw client error (triage: S22 RCA).
/// A precondition failure (another
/// writer already created the marker -- possibly with a DIFFERENT record, e.g. a stale `Condemned`
/// marker still present when a vanished body is freshly re-uploaded) is reported as
/// `CasOverwriteOutcome::Conflict`, never thrown -- this uses `putIfAbsentControlledMutable`, NOT the
/// ref-log lane's `putIfAbsentControlled` (that method's resolve throws `CORRUPTED_DATA` on any
/// different bytes at the key, which is correct for the ref-log's immutable content-addressed keys
/// but wrong for this mutable marker, where a pre-existing different value is an expected, non-corrupt
/// outcome).
CasOverwriteResult putMetaIfAbsent(Pool & pool, const BlobRef & ref, const BlobMeta & meta);

/// Replaces the marker only when its current backend token equals `expected`, controlled (same
/// budgeted resolve-and-reissue as putMetaIfAbsent). A genuine conflict (current token AND bytes both
/// differ from what this call intended) is reported as `CasOverwriteOutcome::Conflict`, never thrown --
/// exactly like the previous uncontrolled `CasResult` contract -- so the caller's existing
/// reload-and-retry metadata reconciliation in `PartWriteTxn::ensureBlobPresent` keeps working unchanged.
CasOverwriteResult casMeta(Pool & pool, const BlobRef & ref, const Token & expected, const BlobMeta & meta);

/// Deletes only the marker incarnation identified by `expected`. A token mismatch leaves the current
/// marker untouched; `NotFound` is distinct from that case so callers can tell absence from a raced
/// replacement. The backend's complete `DeleteOutcome` is returned, including any storage-specific
/// delete-marker status.
DeleteOutcome deleteMetaExact(Backend & backend, const Layout & layout, const BlobRef & ref, const Token & expected);

}
