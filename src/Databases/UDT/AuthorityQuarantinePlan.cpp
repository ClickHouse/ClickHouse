#include <Databases/UDT/AuthorityQuarantinePlan.h>

#include <Databases/UDT/AuthorityRoot.h>

#include <DataTypes/UDT/CanonicalHash.h>

#include <algorithm>
#include <limits>
#include <set>
#include <utility>

namespace DB::UDT
{
namespace
{

using PlanError = AuthorityQuarantinePlanError;

constexpr UInt64 canonical_uuid_bytes = 16;
constexpr UInt64 schema_object_identity_canonical_bytes = sizeof(UInt8) + 2 * canonical_uuid_bytes;
constexpr UInt64 authority_root_identity_canonical_bytes = canonical_uuid_bytes + sizeof(UInt64) + sizeof(Digest);
constexpr UInt64 quarantine_plan_base_canonical_bytes = authority_root_identity_canonical_bytes + sizeof(Digest) + 2 * sizeof(UInt64);

[[noreturn]] void fail(PlanError::Code code, std::string_view message)
{
    throw PlanError(code, message);
}

UInt64 toUInt64(size_t value) noexcept
{
    static_assert(sizeof(size_t) <= sizeof(UInt64));
    return static_cast<UInt64>(value);
}

UInt64 checkedAdd(UInt64 lhs, UInt64 rhs, std::string_view message)
{
    if (rhs > std::numeric_limits<UInt64>::max() - lhs)
        fail(PlanError::Code::ArithmeticOverflow, message);
    return lhs + rhs;
}

UInt64 checkedMultiply(UInt64 lhs, UInt64 rhs, std::string_view message)
{
    if (lhs != 0 && rhs > std::numeric_limits<UInt64>::max() / lhs)
        fail(PlanError::Code::ArithmeticOverflow, message);
    return lhs * rhs;
}

bool isZeroDigest(const Digest & digest) noexcept
{
    return std::all_of(digest.begin(), digest.end(), [](CanonicalByte byte) { return byte == 0; });
}

UInt64 treeLookupWorkBound(UInt64 maximum_objects)
{
    UInt64 levels = 0;
    UInt64 remaining = maximum_objects;
    while (remaining != 0)
    {
        ++levels;
        remaining >>= 1;
    }
    return checkedAdd(
        checkedMultiply(levels, 2, "quarantine tree-lookup work bound overflows UInt64"),
        8,
        "quarantine tree-lookup work bound overflows UInt64");
}

void validateLimits(const AuthorityQuarantinePlanLimits & limits)
{
    constexpr AuthorityQuarantinePlanLimits maxima;
    const auto valid = [](UInt64 value, UInt64 maximum) { return value != 0 && value <= maximum; };
    if (!valid(limits.maximum_seed_objects, maxima.maximum_seed_objects)
        || !valid(limits.maximum_closure_objects, maxima.maximum_closure_objects)
        || !valid(limits.maximum_reverse_edges_per_object, maxima.maximum_reverse_edges_per_object)
        || !valid(limits.maximum_walked_edges, maxima.maximum_walked_edges) || !valid(limits.maximum_work_units, maxima.maximum_work_units)
        || !valid(limits.maximum_retained_canonical_bytes, maxima.maximum_retained_canonical_bytes)
        || limits.maximum_seed_objects > limits.maximum_closure_objects
        || limits.maximum_reverse_edges_per_object > limits.maximum_walked_edges)
        fail(PlanError::Code::InvalidConfiguration, "authority quarantine-plan limits are invalid");
}

class PlanBudget final
{
public:
    PlanBudget(const AuthorityQuarantinePlanLimits & limits_, AuthorityQuarantinePlanStatistics & statistics_)
        : limits(limits_)
        , statistics(statistics_)
    {
    }

    void chargeClosureObjects(UInt64 amount)
    {
        statistics.closure_objects = admit(
            statistics.closure_objects, amount, limits.maximum_closure_objects, "authority quarantine closure exceeds its object limit");
    }

    void chargeEdges(UInt64 amount)
    {
        statistics.reverse_edges_inspected = admit(
            statistics.reverse_edges_inspected,
            amount,
            limits.maximum_walked_edges,
            "authority quarantine closure exceeds its reverse-edge limit");
    }

    void chargeAdjacency()
    {
        statistics.reverse_adjacencies_read = admit(
            statistics.reverse_adjacencies_read,
            1,
            limits.maximum_closure_objects,
            "authority quarantine closure exceeds its reverse-adjacency limit");
    }

    void chargeWork(UInt64 amount)
    {
        statistics.work_units
            = admit(statistics.work_units, amount, limits.maximum_work_units, "authority quarantine closure exceeds its work limit");
    }

    void chargeRetainedCanonicalBytes(UInt64 amount)
    {
        statistics.retained_canonical_bytes = admit(
            statistics.retained_canonical_bytes,
            amount,
            limits.maximum_retained_canonical_bytes,
            "authority quarantine plan exceeds its retained canonical-byte limit");
    }

private:
    static UInt64 admit(UInt64 current, UInt64 amount, UInt64 maximum, std::string_view message)
    {
        if (current > maximum || amount > maximum - current)
            fail(PlanError::Code::LimitExceeded, message);
        return current + amount;
    }

    const AuthorityQuarantinePlanLimits & limits;
    AuthorityQuarantinePlanStatistics & statistics;
};

void validateRoot(const AuthorityRootGraphIdentity & root, const SchemaObjectDependencyGraph::Ptr & graph)
{
    const auto & authority = root.authority_root;
    if (authority.database_uuid == UUIDHelpers::Nil || authority.database_catalog_epoch == 0 || isZeroDigest(authority.authority_anchor)
        || isZeroDigest(root.schema_graph_root))
        fail(PlanError::Code::InvalidRootIdentity, "authority quarantine root identity is invalid");
    if (!graph || graph->getDatabaseUUID() != authority.database_uuid || graph->computeRoot() != root.schema_graph_root)
        fail(PlanError::Code::GraphMismatch, "pinned schema graph differs from its exact authority-root identity");
}

void validateSeeds(std::span<const SchemaObjectID> seeds, const UUID & database_uuid, const SchemaObjectDependencyGraph & graph)
{
    if (seeds.empty())
        fail(PlanError::Code::InvalidSeedSet, "authority quarantine requires at least one failing seed object");
    for (size_t index = 0; index < seeds.size(); ++index)
    {
        const auto & seed = seeds[index];
        if (!seed.isValid() || seed.database_uuid != database_uuid)
            fail(PlanError::Code::InvalidObjectIdentity, "authority quarantine seed identity is invalid");
        if (index != 0 && !(seeds[index - 1] < seed))
            fail(PlanError::Code::InvalidSeedSet, "authority quarantine seeds are not strictly sorted and unique");
        if (!graph.containsNode(seed))
            fail(PlanError::Code::MissingSeedObject, "authority quarantine seed is absent from the pinned schema graph");
    }
}

void validateDependent(const SchemaObjectDependencyNeighbor & dependent, const UUID & database_uuid)
{
    if (!dependent.object.isValid() || dependent.object.database_uuid != database_uuid
        || !isKnownSchemaObjectDependencyEdgeKind(dependent.kind))
        fail(PlanError::Code::GraphMismatch, "pinned schema graph contains an invalid reverse dependency");
}

}

AuthorityQuarantinePlanError::AuthorityQuarantinePlanError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

AuthorityQuarantinePlan::AuthorityQuarantinePlan(
    AuthorityRootGraphIdentity root_,
    std::vector<SchemaObjectID> failing_seeds_,
    std::vector<SchemaObjectID> quarantined_objects_,
    AuthorityQuarantinePlanStatistics statistics_) noexcept
    : root(std::move(root_))
    , failing_seeds(std::move(failing_seeds_))
    , quarantined_objects(std::move(quarantined_objects_))
    , statistics(std::move(statistics_))
{
}

AuthorityQuarantinePlan::Ptr AuthorityQuarantinePlan::build(
    const AuthorityRoot & authority,
    std::span<const SchemaObjectID> sorted_unique_failing_seeds,
    const AuthorityQuarantinePlanLimits & limits)
{
    validateLimits(limits);
    AuthorityQuarantinePlanStatistics statistics;
    PlanBudget budget(limits, statistics);
    budget.chargeWork(4);

    const auto pinned_graph = authority.pinSchemaObjectDependencyGraph();
    if (!pinned_graph)
        fail(PlanError::Code::GraphMismatch, "authority quarantine cannot pin its schema graph");
    const auto & state = authority.getAuthorityState();
    const AuthorityRootGraphIdentity root{
        .authority_root = AuthorityRootIdentity{
            .database_uuid = state.database_uuid,
            .database_catalog_epoch = state.database_catalog_epoch,
            .authority_anchor = state.anchor_hash,
        },
        .schema_graph_root = pinned_graph->computeRoot(),
    };
    validateRoot(root, pinned_graph);

    const UInt64 seed_count = toUInt64(sorted_unique_failing_seeds.size());
    if (seed_count > limits.maximum_seed_objects)
        fail(PlanError::Code::LimitExceeded, "authority quarantine exceeds its failing-seed limit");
    budget.chargeWork(seed_count);
    validateSeeds(sorted_unique_failing_seeds, root.authority_root.database_uuid, *pinned_graph);

    const UInt64 maximum_reachable_objects = std::min(limits.maximum_closure_objects, pinned_graph->getNodeCount());
    const UInt64 lookup_work = treeLookupWorkBound(maximum_reachable_objects);

    statistics.seed_objects = seed_count;
    budget.chargeRetainedCanonicalBytes(quarantine_plan_base_canonical_bytes);
    budget.chargeRetainedCanonicalBytes(
        checkedMultiply(seed_count, schema_object_identity_canonical_bytes, "quarantine seed canonical bytes overflow UInt64"));
    budget.chargeClosureObjects(seed_count);
    budget.chargeRetainedCanonicalBytes(
        checkedMultiply(seed_count, schema_object_identity_canonical_bytes, "quarantine closure canonical bytes overflow UInt64"));

    std::vector<SchemaObjectID> failing_seeds(sorted_unique_failing_seeds.begin(), sorted_unique_failing_seeds.end());
    std::set<SchemaObjectID> closure;
    std::vector<SchemaObjectID> pending;
    budget.chargeWork(checkedMultiply(seed_count, 2, "quarantine seed insertion work overflows UInt64"));
    for (const auto & seed : sorted_unique_failing_seeds)
    {
        closure.emplace_hint(closure.end(), seed);
        pending.push_back(seed);
    }

    size_t pending_index = 0;
    while (pending_index < pending.size())
    {
        budget.chargeWork(1);
        const SchemaObjectID dependency = pending[pending_index++];
        budget.chargeAdjacency();
        const UInt64 dependent_count = pinned_graph->getDependentCount(dependency);
        if (dependent_count > limits.maximum_reverse_edges_per_object)
            fail(PlanError::Code::LimitExceeded, "authority quarantine reverse adjacency exceeds its edge limit");
        budget.chargeEdges(dependent_count);
        budget.chargeWork(checkedMultiply(
            dependent_count, checkedAdd(lookup_work, 1, "quarantine edge work overflows UInt64"), "quarantine edge work overflows UInt64"));
        const auto dependents = pinned_graph->getDependents(dependency);
        if (toUInt64(dependents.size()) != dependent_count)
            fail(PlanError::Code::GraphMismatch, "pinned schema graph reverse cardinality changed during immutable traversal");
        for (const auto & dependent : dependents)
        {
            validateDependent(dependent, root.authority_root.database_uuid);
            const auto position = closure.lower_bound(dependent.object);
            if (position != closure.end() && *position == dependent.object)
                continue;
            budget.chargeClosureObjects(1);
            budget.chargeRetainedCanonicalBytes(schema_object_identity_canonical_bytes);
            const auto inserted = closure.emplace_hint(position, dependent.object);
            pending.push_back(*inserted);
        }
    }

    budget.chargeWork(toUInt64(closure.size()));
    std::vector<SchemaObjectID> quarantined_objects(closure.begin(), closure.end());
    return Ptr(new AuthorityQuarantinePlan(root, std::move(failing_seeds), std::move(quarantined_objects), statistics));
}

bool AuthorityQuarantinePlan::contains(const SchemaObjectID & object) const noexcept
{
    return std::binary_search(quarantined_objects.begin(), quarantined_objects.end(), object);
}

}
