#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
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
    for (const ManifestSweepResult::Nomination & nomination : result.nominations)
    {
        const DeleteOutcome outcome = store.backend().deleteExact(nomination.key, nomination.token);
        if (classifyDeleteOutcome(outcome) == DeleteClass::Deleted)
            ++result.deleted;
        else
            ++result.skipped;
    }
    return result;
}

}
