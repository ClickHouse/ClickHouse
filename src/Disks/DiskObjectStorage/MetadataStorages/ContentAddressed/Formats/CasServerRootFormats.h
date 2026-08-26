#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFormat.h>
#include <base/types.h>
#include <base/extended_types.h>
#include <cstdint>
#include <optional>
#include <string_view>

namespace DB::Cas
{

/// Text codecs for the three per-server-root control objects under
/// `gc/server-roots/<server_root_id>/`. Each object is a CAS text object consisting of a type/version
/// header line and one JSON body line; the codec layer only maps fields and checks the syntax that is
/// local to that object. Coordination and CAS operations remain in the pool/server-root layer.
///
/// The owner object permanently binds a configured `server_root_id` to one server UUID. The epoch
/// object stores the next writer epoch and is CAS-bumped so an epoch is never reused after a
/// restart or supersession. The mount object is the expirable liveness lease for one writer
/// incarnation; its `min_active` value carries the GC acknowledgement floor, while `gc_fenced` is a
/// terminal fence-out marker for that incarnation.

/// Permanent identity anchor for one configured server root. It is created with put-if-absent, never
/// reassigned, and tombstoned in place when explicitly decommissioned, so `decodeOwner` returns both
/// the UUID that the caller must compare with its local server identity and the retirement state.
struct OwnerObject
{
    UInt128 server_uuid{};
    /// Set when this identity was explicitly decommissioned by an operator (`CasDecommission`'s
    /// final step tombstones the owner object in place rather than deleting it, since its content
    /// alone cannot distinguish decommissioned debris from a legitimate successor's live anchor).
    /// A normal claim (`claimOwnerOrThrow`) must refuse to silently resume a tombstoned identity.
    std::optional<uint64_t> retired_at_ms;
};

/// Durable counter state used to allocate unique writer epochs. The stored value is the next epoch
/// to allocate; the caller CAS-writes a larger value and adopts the previous value for its mount.
struct ServerEpoch
{
    uint64_t next_writer_epoch = 0;
};

/// The current liveness lease for one `(server_uuid, writer_epoch)` writer incarnation. The pool
/// layer renews and replaces this object with CAS/overwrite operations, and GC may fence an expired
/// lease by setting `gc_fenced`; a fenced incarnation must not resume writing. `min_active` is the
/// merged GC acknowledgement floor, with `UINT64_MAX` marking a clean farewell (retired lease).
struct MountLease
{
    UInt128 server_uuid{};
    uint64_t writer_epoch = 0;
    String hostname;
    uint64_t pid = 0;
    uint64_t started_at_ms = 0;
    uint64_t seq = 0;
    uint64_t expires_at_ms = 0;
    uint64_t min_active = 0;   /// UINT64_MAX = retired (farewell)
    bool gc_fenced = false;    /// GC fence-out of an expired lease; terminal
    /// One holder-originated body identity. Every physical retry of that one logical write reuses
    /// it; a GC fence copies the observed value while every successor holder body mints a new one.
    UInt128 write_attempt_id{};
};

/// Encode the owner anchor as canonical text with the `cas_owner` header and a final newline. The
/// optional retirement timestamp is omitted for a never-retired owner, preserving its historical
/// bytes. This function does not perform the put-if-absent operation or validate ownership; those
/// decisions belong to the caller that coordinates the server-root object.
String encodeOwner(const OwnerObject & o);

/// Decode an owner anchor, requiring its `su` field, tolerating an absent optional `rt` retirement
/// timestamp, and rejecting bytes after the body line. Unknown JSON fields are skipped for
/// forward-compatible reads; malformed input, a missing `su`, and trailing data throw
/// `CORRUPTED_DATA`.
OwnerObject decodeOwner(std::string_view data);

/// Encode the durable next-writer-epoch counter as canonical text with the `cas_epoch` header and a
/// final newline. Allocation and the CAS loop that makes the counter monotone are outside this
/// codec.
String encodeServerEpoch(const ServerEpoch & e);

/// Decode the epoch counter, requiring its `nwe` field and rejecting bytes after the body line.
/// Unknown JSON fields are skipped for forward-compatible reads; malformed input, a missing `nwe`,
/// and trailing data throw `CORRUPTED_DATA`.
ServerEpoch decodeServerEpoch(std::string_view data);

/// Encode the complete mount-lease body as canonical text with the `cas_mount_lease` header and a
/// final newline. This preserves full-range `uint64_t` values such as `min_active` as decimal JSON
/// strings and writes `gc_fenced` as a JSON boolean; lease renewal, fencing, and token checks remain
/// in the caller.
String encodeMountLease(const MountLease & m);

/// Decode a mount lease and reject bytes after its body line. The reader accepts unknown JSON fields
/// so newer lease metadata can be read by older code, while malformed field values, a missing or zero
/// required identity field, and trailing data throw `CORRUPTED_DATA`; cross-field lease validity is enforced by
/// the mount/GC protocol, not here.
MountLease decodeMountLease(std::string_view data);

}
