#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasOrphanManifestSweep.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <base/types.h>

namespace DB::Cas::tests
{

/// TEST-ONLY variant of the cursor page: plans a page via the production `planManifestCursorPage` and
/// then exact-token-deletes every nomination immediately, with no source-edge retirement and no
/// `gc/state` adoption of the retirement. Production deletion always goes through `Gc::fold`'s
/// orphan_sweep phase instead, which adopts the retirements in the same round CAS before deleting —
/// this shortcut recreates the accounting hole that path exists to close, so it must never be reached
/// from a production translation unit.
inline ManifestSweepResult sweepManifestCursorPageForTest(
    Pool & store,
    const String & cursor,
    uint64_t list_budget,
    uint64_t delete_budget,
    GcRoundWorkBudget * work_budget = nullptr)
{
    ManifestSweepResult result = planManifestCursorPage(
        store, cursor, list_budget, delete_budget, /*catalog_recovery_authoritative=*/true, work_budget);
    CasOperation op = store.openRequests().admit();
    for (const ManifestSweepResult::Nomination & nomination : result.nominations)
    {
        /// A nomination records the incarnation it was planned against, so the delete re-observes the
        /// key and refuses unless what is there now is still that one: a key a fresh owner has since
        /// replaced must survive.
        const std::optional<Meta> seen = op.head(nomination.key, Retry::standard());
        if (seen && nomination.token.matches(seen->etag)
            && op.remove(nomination.key, seen->etag, Retry::standard()) == Removal::Removed)
            ++result.deleted;
        else
            ++result.skipped;
    }
    return result;
}

}
