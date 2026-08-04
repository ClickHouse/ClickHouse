#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CatalogLifecycleReconciler.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequestControl.h>

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
    Backend & backend_, const Layout & layout_, const CasFoldSeal & adopted_parent_,
    uint64_t admitted_generation_,
    std::function<CasRefCatalog::LeaderFenceStatus(uint64_t)> check_fence_)
    : backend(backend_)
    , layout(layout_)
    , adopted_parent(adopted_parent_)
    , admitted_generation(admitted_generation_)
    , check_fence(std::move(check_fence_))
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

CatalogLifecycleReconcileResult CatalogLifecycleReconciler::reconcile()
{
    CatalogLifecycleReconcileResult result{
        .authority_status = AuthorityStatus::Authoritative,
        .catalog_resolution = CatalogResolution::DrainComplete,
        .retired_lives = {},
        .final_catalog_cut = std::nullopt,
        .deleted = 0};
    CasRefCatalog::Snapshot catalog = CasRefCatalog::read(backend, layout);

    for (;;)
    {
        const std::optional<CatalogEntry> eligible = selectEligible(catalog);
        if (!eligible)
        {
            if (check_fence(admitted_generation) == CasRefCatalog::LeaderFenceStatus::Moved)
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
                backend, layout, std::move(catalog), *eligible, adopted_parent,
                admitted_generation, check_fence);
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
