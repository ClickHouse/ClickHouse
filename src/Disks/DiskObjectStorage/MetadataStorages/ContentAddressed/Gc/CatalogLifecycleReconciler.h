#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCatalog.h>

#include <cstdint>
#include <functional>
#include <optional>
#include <vector>

namespace DB::Cas
{

enum class AuthorityStatus : uint8_t
{
    Authoritative,
    FencedOut,
};

enum class CatalogResolution : uint8_t
{
    DrainComplete,
    ExactRowAbsent,
    ExactRowReplaced,
    ExactRowStillPresent,
};

/// The catalog-only result produced before a GC round may enumerate or publish successor state.
struct CatalogLifecycleReconcileResult
{
    AuthorityStatus authority_status;
    CatalogResolution catalog_resolution;
    std::vector<NamespaceLifeId> retired_lives;
    std::optional<CasRefCatalog::Snapshot> final_catalog_cut;
    uint64_t deleted = 0;
};

/// Settles catalog rows that an already-adopted parent fold seal proved safe to remove.
///
/// This component owns only deterministic eligible-row selection and the catalog `N + 1` drain.
/// It neither discovers the parent seal nor performs a hot ref LIST, ref walk, fold, publication,
/// runtime invalidation, or physical deletion.
class CatalogLifecycleReconciler
{
public:
    CatalogLifecycleReconciler(
        Backend & backend_, const Layout & layout_, const CasFoldSeal & adopted_parent_,
        uint64_t admitted_generation_,
        std::function<CasRefCatalog::LeaderFenceStatus(uint64_t)> check_fence_);

    CatalogLifecycleReconcileResult reconcile();

private:
    std::optional<CatalogEntry> selectEligible(const CasRefCatalog::Snapshot & catalog) const;
    static CatalogResolution resolveExactRow(
        const CasRefCatalog::Snapshot & catalog, const CatalogEntry & observed);

    Backend & backend;
    const Layout & layout;
    const CasFoldSeal & adopted_parent;
    uint64_t admitted_generation;
    std::function<CasRefCatalog::LeaderFenceStatus(uint64_t)> check_fence;
};

}
