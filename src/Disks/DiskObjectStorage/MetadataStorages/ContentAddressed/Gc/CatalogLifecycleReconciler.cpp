#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CatalogLifecycleReconciler.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>

#include <Common/Exception.h>
#include <fmt/format.h>
#include <algorithm>
#include <utility>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

namespace DB::Cas
{

CatalogLifecycleReconciler::CatalogLifecycleReconciler(
    CasOperation & op_, const Layout & layout_, const CasFoldSeal & adopted_parent_)
    : op(op_)
    , layout(layout_)
    , adopted_parent(adopted_parent_)
{
}

std::optional<CatalogEntry> CatalogLifecycleReconciler::selectEligible(
    const CasRefCatalog::Snapshot & catalog) const
{
    for (const CatalogEntry & entry : catalog.catalog.entries)
    {
        if (entry.state != NsState::Removing)
            continue;

        const auto parent_row = adopted_parent.ref_lives.find(entry.incarnation);
        if (parent_row == adopted_parent.ref_lives.end()
            || !parent_row->second.cleanup_evidence
            || parent_row->second.coverage.hold)
            continue;

        return entry;
    }
    return std::nullopt;
}

CatalogResolution CatalogLifecycleReconciler::resolveExactRow(
    const CasRefCatalog::Snapshot & catalog, const CatalogEntry & observed)
{
    const auto current = std::find_if(
        catalog.catalog.entries.begin(),
        catalog.catalog.entries.end(),
        [&](const CatalogEntry & entry) { return entry.ns.string() == observed.ns.string(); });
    if (current == catalog.catalog.entries.end())
        return CatalogResolution::ExactRowAbsent;
    if (current->incarnation != observed.incarnation)
        return CatalogResolution::ExactRowReplaced;
    return CatalogResolution::ExactRowStillPresent;
}

CatalogLifecycleReconcileResult CatalogLifecycleReconciler::reconcile(
    const std::function<void()> & refresh_authority)
{
    CatalogLifecycleReconcileResult result{
        .authority_status = AuthorityStatus::Authoritative,
        .catalog_resolution = CatalogResolution::DrainComplete,
        .retired_lives = {},
        .final_catalog_cut = std::nullopt,
        .deleted = 0};
    CasRefCatalog::Snapshot catalog = CasRefCatalog::read(op, layout);

    for (;;)
    {
        const std::optional<CatalogEntry> eligible = selectEligible(catalog);
        if (!eligible)
        {
            /// The verdict gets its own refresh, not just each erase: a deposition landing after the
            /// last erase is invisible to the reading that erase took, so without this the drain hands
            /// a deposed leader `Authoritative` and the round only learns better at its `gc/state`
            /// commit -- after a ref walk and a fold seal it never had the authority to build.
            refresh_authority();
            if (!op.admitted())
            {
                result.authority_status = AuthorityStatus::FencedOut;
                return result;
            }
            result.catalog_resolution = CatalogResolution::DrainComplete;
            result.final_catalog_cut = std::move(catalog);
            return result;
        }

        CasRefCatalog::CompletedRemovingDeleteResult delete_result
            = CasRefCatalog::deleteCompletedRemovingAtSnapshot(
                op, layout, std::move(catalog), *eligible, adopted_parent, refresh_authority);
        if (!delete_result.catalog_snapshot)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "CAS catalog lifecycle reconciliation returned no catalog resolution snapshot");

        catalog = std::move(*delete_result.catalog_snapshot);
        result.catalog_resolution = resolveExactRow(catalog, *eligible);
        if (delete_result.invalidated_life)
            result.retired_lives.push_back(*delete_result.invalidated_life);

        if (delete_result.outcome == CasRefCatalog::CompletedRemovingDeleteOutcome::FencedOut)
        {
            result.authority_status = AuthorityStatus::FencedOut;
            return result;
        }
        if (delete_result.outcome == CasRefCatalog::CompletedRemovingDeleteOutcome::ProofRefused)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "CAS catalog lifecycle reconciliation selected namespace '{}' without matching no-hold cleanup evidence",
                eligible->ns.string());
        if (result.catalog_resolution == CatalogResolution::ExactRowStillPresent)
            throwCasWriteRetryLater(fmt::format(
                "CAS catalog lifecycle reconciliation left completed-removal namespace '{}' present",
                eligible->ns.string()));
        if (delete_result.outcome == CasRefCatalog::CompletedRemovingDeleteOutcome::Deleted)
            ++result.deleted;
    }
}

}
