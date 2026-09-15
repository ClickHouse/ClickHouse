#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasBlobMetaFormat.h>

#include <optional>

namespace DB::Cas
{

/// A decoded blob meta record together with the incarnation the same read observed. The incarnation
/// travels with the record because the next conditional update or exact delete must be guarded by the
/// version that was actually read; comparing encoded meta bytes would not provide that protection.
struct LoadedMeta
{
    BlobMeta meta;
    Etag etag;
};

/// Shared lifecycle operations for the blob freshness marker used by the writer and GC. The key is
/// built from the complete `BlobRef`, so each algorithm uses its own digest representation and no
/// pool-wide digest width is threaded through these functions. The marker is a point-read hint rather
/// than the blob lifetime's linearization point: the blob body's incarnation tag and exact-incarnation
/// body deletion provide the safety guarantee, while a stale marker can at most make a writer
/// re-upload.
///
/// `loadMeta` is used in the adopt path, so its backend must provide strong read-after-write
/// consistency: after a successful meta write, the one subsequent read must observe that write.
/// Conditional updates and deletion use the observed incarnation, not the encoded meta bytes.
///
/// Returns the current decoded marker and the incarnation to guard the next write with, or nullopt
/// when the meta key is absent. Decoding errors propagate as exceptions.
///
/// `policy` lets a hand-written loop pass the bound it froze at entry, so this read ends with the rest
/// of the loop instead of starting a fresh window. Same for `putMetaIfAbsent` below.
std::optional<LoadedMeta> loadMeta(CasOperation & op, const Layout & layout, const BlobRef & ref,
                                   const Retry & policy = Retry::standard());

/// Creates the marker only when its key is absent, on the plane `op` belongs to -- like its siblings,
/// so one caller's decision cannot end up split across two fences. Anything at the key that this call
/// did not itself write -- a stale `Condemned` marker still present when a vanished body is freshly
/// re-uploaded, or a racing writer's byte-identical marker -- comes back as `Conflict` carrying what
/// was observed, never as a throw: this marker is mutable, so a pre-existing different value is an
/// expected outcome rather than corruption.
WriteResult putMetaIfAbsent(CasOperation & op, const Layout & layout, const BlobRef & ref,
                            const BlobMeta & meta, const Retry & policy = Retry::standard());

/// Replaces the marker only when its current incarnation is `expected`, on the plane `op` belongs to.
/// A competing write is reported as `Conflict` carrying what the resolve read observed, never thrown,
/// so the caller's own reload-and-retry reconciliation decides what to do about it.
WriteResult casMeta(CasOperation & op, const Layout & layout, const BlobRef & ref,
                    const Etag & expected, const BlobMeta & meta);

/// Deletes only the marker incarnation named by `expected`. `Mismatch` leaves the current marker
/// untouched and is distinct from `Gone`, so callers can tell absence from a raced replacement. A
/// versioned bucket that archives instead of reclaiming raises `CAS_DELETE_MARKER`.
Removal deleteMetaExact(CasOperation & op, const Layout & layout, const BlobRef & ref,
                        const Etag & expected);

}
