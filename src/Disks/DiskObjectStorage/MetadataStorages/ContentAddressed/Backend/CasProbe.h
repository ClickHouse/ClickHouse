#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
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

}
