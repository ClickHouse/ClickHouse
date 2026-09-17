#pragma once

#include <DataTypes/UDT/Definition.h>
#include <DataTypes/UDT/ResourceLimits.h>

#include <Core/Types.h>

#include <array>
#include <bitset>
#include <memory>
#include <optional>
#include <string_view>

namespace DB::UDT
{

enum class QuotaState : UInt8
{
    WithinQuota,
    OverQuota,
};

class ResourceUsage final
{
public:
    UInt64 get(ResourceLimit limit) const noexcept;
    void set(ResourceLimit limit, UInt64 value) noexcept;

    bool operator==(const ResourceUsage &) const = default;

private:
    std::array<UInt64, resource_limit_count> values{};
};

/// Additions and removals are kept separate so a replacement can be evaluated
/// against its final deterministic charge without signed arithmetic.
class ResourceDelta final
{
public:
    ResourceDelta & add(ResourceLimit limit, UInt64 amount) noexcept;
    ResourceDelta & remove(ResourceLimit limit, UInt64 amount) noexcept;

    UInt64 getAddition(ResourceLimit limit) const noexcept;
    UInt64 getRemoval(ResourceLimit limit) const noexcept;
    bool additionOverflowed(ResourceLimit limit) const noexcept;
    bool removalOverflowed(ResourceLimit limit) const noexcept;

private:
    std::array<UInt64, resource_limit_count> additions{};
    std::array<UInt64, resource_limit_count> removals{};
    std::bitset<resource_limit_count> addition_overflow;
    std::bitset<resource_limit_count> removal_overflow;
};

enum class ResourceAdmissionStatus : UInt8
{
    Accepted,
    LimitExceeded,
    ArithmeticOverflow,
    InvalidRemoval,
};

/// A rejected result is directly translatable to a stable limit-specific
/// error. An accepted result may remain `QuotaState::OverQuota` after a quota decrease; it
/// is accepted only when no independently over-quota counter grows.
struct ResourceAdmissionResult
{
    ResourceAdmissionStatus status = ResourceAdmissionStatus::Accepted;
    QuotaState current_state = QuotaState::WithinQuota;
    QuotaState prospective_state = QuotaState::WithinQuota;
    std::optional<ResourceLimit> limit;
    UInt64 current = 0;
    UInt64 removal = 0;
    UInt64 addition = 0;
    UInt64 prospective = 0;
    UInt64 maximum = 0;

    bool isAccepted() const noexcept { return status == ResourceAdmissionStatus::Accepted; }
};

QuotaState getQuotaState(const ResourceUsage & usage, const EffectiveResourceLimits & limits) noexcept;

/// Computes the complete next usage before exposing it. `prospective_usage` is
/// written only on acceptance, so callers cannot accidentally publish a
/// partially charged result.
ResourceAdmissionResult evaluateResourceAdmission(
    const ResourceUsage & current,
    const ResourceDelta & delta,
    const EffectiveResourceLimits & limits,
    ResourceUsage * prospective_usage = nullptr) noexcept;

/// Commits the prospective usage only after every independent counter passes.
ResourceAdmissionResult
tryApplyResourceDelta(ResourceUsage & current, const ResourceDelta & delta, const EffectiveResourceLimits & limits) noexcept;

/// Query-owned monotonic state shared by every authority-specific budget used
/// during one query. It retains only canonical descriptor identities, never
/// definition handles, catalog roots, ASTs, or resolved results.
class QueryResourceLedger final
{
public:
    QueryResourceLedger();
    ~QueryResourceLedger();

    QueryResourceLedger(const QueryResourceLedger &) = delete;
    QueryResourceLedger & operator=(const QueryResourceLedger &) = delete;

    const ResourceUsage & getUsage() const noexcept;

private:
    ResourceAdmissionResult admitCurrentUsage(const EffectiveResourceLimits & limits);
    ResourceAdmissionResult charge(const ResourceDelta & delta, const EffectiveResourceLimits & limits) noexcept;

    /// Charges one exact (definition identity, canonical arguments) tuple only
    /// on its first query-wide insertion. The counter and retained key bytes are
    /// admitted atomically before the key is copied into the ledger.
    ResourceAdmissionResult chargeDistinctDescriptor(
        const DefinitionIdentity & identity, std::string_view canonical_arguments, const EffectiveResourceLimits & limits);

    EffectiveResourceLimits getApplicableLimits(const EffectiveResourceLimits & fallback) const noexcept;
    UInt64 getLimitsGeneration() const noexcept;

    friend class ProspectiveResourceBudget;

    struct Impl;
    std::unique_ptr<Impl> impl;
};

/// Monotonic query/checker budget. `charge` updates its counter before the
/// protected work starts and never refunds it, including when later work
/// throws. A rejected charge leaves all counters unchanged.
class ProspectiveResourceBudget final
{
public:
    explicit ProspectiveResourceBudget(EffectiveResourceLimits limits_);
    ProspectiveResourceBudget(EffectiveResourceLimits limits_, std::shared_ptr<QueryResourceLedger> query_ledger_);

    ResourceAdmissionResult charge(ResourceLimit limit, UInt64 amount = 1) noexcept;
    ResourceAdmissionResult charge(const ResourceDelta & delta) noexcept;
    ResourceAdmissionResult chargeDistinctDescriptor(const DefinitionIdentity & identity, std::string_view canonical_arguments);
    /// Joins a newly discovered authority into the query-wide effective
    /// minimum and rejects an already excessive ledger before authority work.
    /// Ordinary charges deliberately keep the OVER_QUOTA no-growth semantics
    /// used after a quota decrease.
    ResourceAdmissionResult admitCurrentUsage();
    /// Joins another authority's effective limits into the same query ledger
    /// without replacing this budget, so existing graph/planner borrows stay
    /// valid while every later charge observes the stricter shared minimum.
    ResourceAdmissionResult admitCurrentUsage(const EffectiveResourceLimits & additional_limits);
    const ResourceUsage & getUsage() const noexcept;
    /// Live snapshot of the shared effective minimum. It is returned by value;
    /// a later sibling-authority join may make a prior copy stale.
    EffectiveResourceLimits getLimits() const noexcept;

private:
    void refreshApplicableLimits() const noexcept;

    mutable EffectiveResourceLimits limits;
    mutable UInt64 cached_limits_generation = 0;
    std::shared_ptr<QueryResourceLedger> query_ledger;
};

/// Stable limit-specific diagnostic payload suitable for a LIMIT_EXCEEDED
/// exception at a SQL boundary.
String formatResourceAdmissionFailure(const ResourceAdmissionResult & result);

inline constexpr UInt16 deterministic_catalog_charge_abi = 1;

enum class DeterministicCatalogChargeComponent : UInt8
{
    CanonicalRecordBytes,
    Definitions,
    Specializations,
    DependencyEdges,
    OccurrencePaths,
    InventoryNodes,
    InventoryLeaves,
};

std::string_view deterministicCatalogChargeComponentName(DeterministicCatalogChargeComponent component) noexcept;

/// Exact canonical bytes plus ABI-fixed logical charges. These values never
/// depend on allocator capacity, host word size, or insertion order.
struct DeterministicCatalogChargeInput
{
    UInt64 canonical_record_bytes = 0;
    UInt64 definitions = 0;
    UInt64 specializations = 0;
    UInt64 dependency_edges = 0;
    UInt64 occurrence_paths = 0;
    UInt64 inventory_nodes = 0;
    UInt64 inventory_leaves = 0;

    bool operator==(const DeterministicCatalogChargeInput &) const = default;
};

struct DeterministicCatalogChargeRates
{
    UInt64 definition_bytes = 128;
    UInt64 specialization_bytes = 128;
    UInt64 dependency_edge_bytes = 64;
    UInt64 occurrence_path_bytes = 64;
    UInt64 inventory_node_bytes = 64;
    UInt64 inventory_leaf_bytes = 64;

    bool operator==(const DeterministicCatalogChargeRates &) const = default;
};

const DeterministicCatalogChargeRates & getDeterministicCatalogChargeRates() noexcept;

enum class DeterministicCatalogChargeStatus : UInt8
{
    Accepted,
    LimitExceeded,
    ArithmeticOverflow,
    InvalidMaximum,
};

struct DeterministicCatalogChargeResult
{
    DeterministicCatalogChargeStatus status = DeterministicCatalogChargeStatus::Accepted;
    std::optional<DeterministicCatalogChargeComponent> component;
    UInt64 charged_bytes = 0;
    UInt64 maximum_bytes = 0;

    bool isAccepted() const noexcept { return status == DeterministicCatalogChargeStatus::Accepted; }
};

/// Charges every component prospectively in canonical order. No partial total
/// is returned on failure.
DeterministicCatalogChargeResult
calculateDeterministicCatalogCharge(const DeterministicCatalogChargeInput & input, UInt64 maximum_bytes) noexcept;

DeterministicCatalogChargeResult
calculateDeterministicCatalogCharge(const DeterministicCatalogChargeInput & input, const EffectiveResourceLimits & limits) noexcept;

}
