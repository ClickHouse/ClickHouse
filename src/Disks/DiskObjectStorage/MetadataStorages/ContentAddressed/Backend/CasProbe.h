#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <base/types.h>

namespace DB::Cas
{

/// Run the capability battery against `op`, using throwaway keys under `probe_prefix`.
///
/// `op` must already be admitted. The two store-level precondition hooks —
/// `Backend::checkPoolPreconditions` and `Backend::checkConditionalWriteSingleAttemptSupport` — are the
/// CALLER's responsibility, run through `CasRequests::backendForCapabilityPredicates()` before admitting
/// `op` and calling this: an admitted `CasOperation` carries no route back to the raw backend, by design,
/// so this function cannot reach them itself.
///
/// The battery validates the backend preconditions required by a writable content-addressed pool:
///   1. `create` and `replace` are conditional and exact: `create` refuses an occupied key
///      (create-if-absent, conflict-on-existing) and `replace` refuses a stale incarnation
///      (conflict-on-stale), while a matching incarnation commits (commit-on-current).
///   2. Conditional-delete is enforced (`remove` with a stale incarnation is rejected and the object
///      survives).
///   3. Listing reflects both creation and deletion of a probe object.
///   4. Successful deletion does not create a versioning delete marker. A content-addressed pool cannot
///      reclaim storage correctly from a versioned bucket: garbage-collection deletes would archive old
///      versions instead of removing objects, and repeated ref updates would accumulate versions.
///
/// On any failed check, throws a DB::Exception(ErrorCodes::NOT_IMPLEMENTED) with a message naming the
/// specific failed check. This is fail-closed: a backend that does not pass the battery MUST NOT be
/// used to coordinate a content-addressed pool.
///
/// Cleanup of the probe key is best-effort and runs unconditionally: after the battery completes, or on
/// the failure path immediately before the check-failing exception is rethrown. Cleanup itself suppresses
/// exceptions so that it cannot hide the capability-check failure.
void runCapabilityProbe(CasOperation & op, const String & probe_prefix);

}
