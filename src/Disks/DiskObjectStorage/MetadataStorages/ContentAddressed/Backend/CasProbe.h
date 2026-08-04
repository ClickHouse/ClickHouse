#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <base/types.h>

namespace DB::Cas
{

/// Run the capability battery against `backend`, using throwaway keys under `probe_prefix`.
///
/// The probe validates the backend preconditions required by a writable content-addressed pool:
///   1. Store-level safety checks pass, including the requirement that conditional writes use one
///      underlying HTTP attempt. Hidden SDK retries can outlive the writer's mount lease and obscure
///      whether a conditional operation committed; retries must therefore be explicit CAS state-machine
///      transitions rather than transparent client behavior.
///   2. Conditional-create and conditional-overwrite are enforced (`putIfAbsent` prevents overwrites,
///      and `putOverwrite` rejects a wrong-token update).
///   3. `casPut` supports create-if-absent, conflict-on-existing, conflict-on-stale, and commit-on-current.
///   4. Conditional-delete is enforced (`deleteExact` with a wrong token is rejected and the object survives).
///   5. Listing reflects both creation and deletion of a probe object.
///   6. Successful deletion does not create a versioning delete marker. A content-addressed pool cannot
///      reclaim storage correctly from a versioned bucket: garbage-collection deletes would archive old
///      versions instead of removing objects, and repeated ref updates would accumulate versions.
///
/// On any failed check, throws a DB::Exception(ErrorCodes::NOT_IMPLEMENTED) with a message naming the
/// specific failed check. This is fail-closed: a backend that does not pass the battery MUST NOT be
/// used to coordinate a content-addressed pool.
///
/// Cleanup of probe keys is best-effort and runs unconditionally: after the battery completes, or on
/// the failure path immediately before the check-failing exception is rethrown. Cleanup itself suppresses
/// exceptions so that it cannot hide the capability-check failure.
void runCapabilityProbe(Backend & backend, const String & probe_prefix);

/// Probe whether `object_storage` ENFORCES a write-once conditional server-side copy
/// (`IObjectStorage::copyObjectConditional`, `If-None-Match: *`) — an OPTIONAL capability, unlike
/// the mandatory battery above (`runCapabilityProbe`). It is meaningful only for a disk configured
/// with `staging_backend=s3`. S3-native staging promotes a temporary object into a content-addressed
/// blob with this copy; if the destination precondition is ignored, the copy can silently overwrite a
/// live blob. Such a backend is unsafe for S3-native staging, so the metadata layer must fall back to
/// local staging rather than refuse to mount.
///
/// Goes directly through `IObjectStorage`, not through `Backend`/`CasProbe`'s battery — this keeps
/// the probe decoupled from the `Backend`'s content-addressed operations. The metadata layer uses the
/// result only to decide whether the optional S3-native staging path is safe to enable.
///
/// Writes a tiny throwaway object at `<probe_prefix>/src`, conditionally copies it to
/// `<probe_prefix>/dst` (expects `created == true` — a fresh destination), then repeats the SAME
/// conditional copy onto the now-existing `dst` (expects `created == false` — the destination must
/// be REJECTED, proving the backend enforces `If-None-Match`). Returns `true` only when both
/// expectations hold.
///
/// Fail-close: ANY exception (including the default `NOT_IMPLEMENTED` a backend throws when it does
/// not override `copyObjectConditional` at all) OR a non-enforcing result (the second copy also
/// reports `created == true`, i.e. the backend silently overwrote the destination) returns `false`.
/// This function never throws — the caller treats `false` as "fall back to local staging", never as
/// a mount failure.
///
/// Cleanup of the probe objects (`src`, `dst`) is best-effort and runs unconditionally on normal and
/// exceptional exits, mirroring `runCapabilityProbe`.
bool probeConditionalCopy(IObjectStorage & object_storage, const String & probe_prefix);

}
