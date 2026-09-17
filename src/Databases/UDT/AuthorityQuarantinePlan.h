#pragma once

#include <Core/Types.h>
#include <Databases/SchemaObjectDependencyGraph.h>
#include <Databases/UDT/AuthorityVerificationStamp.h>

#include <memory>
#include <span>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace DB::UDT
{

class AuthorityRoot;
class AuthorityRepairAudit;

/// Exact identity of the graph pinned from one immutable authority root. The
/// aggregate authority anchor cannot substitute for the graph root digest.
/// Only `AuthorityQuarantinePlan::build` and the resumable exact-root repair
/// audit construct this value, from one pinned `AuthorityRoot`.
struct AuthorityRootGraphIdentity
{
    AuthorityRootIdentity authority_root;
    Digest schema_graph_root{};

    bool operator==(const AuthorityRootGraphIdentity &) const = default;
};

struct AuthorityQuarantinePlanLimits
{
    UInt64 maximum_seed_objects = 65'536;
    UInt64 maximum_closure_objects = 100'000;
    UInt64 maximum_reverse_edges_per_object = 65'536;
    UInt64 maximum_walked_edges = 262'144;
    UInt64 maximum_work_units = 16'777'216;
    /// Exact canonical payload retained by the immutable result. Temporary
    /// containers are independently bounded by the node/edge limits and by
    /// the process MemoryTracker; allocator capacity is not canonical state.
    UInt64 maximum_retained_canonical_bytes = 16ULL << 20;
};

struct AuthorityQuarantinePlanStatistics
{
    UInt64 seed_objects = 0;
    UInt64 closure_objects = 0;
    UInt64 reverse_adjacencies_read = 0;
    UInt64 reverse_edges_inspected = 0;
    UInt64 work_units = 0;
    UInt64 retained_canonical_bytes = 0;

    bool operator==(const AuthorityQuarantinePlanStatistics &) const = default;
};

class AuthorityQuarantinePlanError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidConfiguration,
        LimitExceeded,
        ArithmeticOverflow,
        InvalidRootIdentity,
        GraphMismatch,
        InvalidSeedSet,
        InvalidObjectIdentity,
        MissingSeedObject,
    };

    AuthorityQuarantinePlanError(Code code_, std::string_view message);

    const Code code;
};

/// Immutable, feature-inert reverse dependency closure. It retains canonical
/// identities only, never the graph, authority root, object, or record handles.
class AuthorityQuarantinePlan final
{
public:
    using Ptr = std::shared_ptr<const AuthorityQuarantinePlan>;

    [[nodiscard]] static Ptr build(
        const AuthorityRoot & authority,
        std::span<const SchemaObjectID> sorted_unique_failing_seeds,
        const AuthorityQuarantinePlanLimits & limits = {});

    const AuthorityRootGraphIdentity & getRoot() const noexcept { return root; }
    std::span<const SchemaObjectID> getFailingSeeds() const noexcept { return failing_seeds; }
    std::span<const SchemaObjectID> getQuarantinedObjects() const noexcept { return quarantined_objects; }
    const AuthorityQuarantinePlanStatistics & getStatistics() const noexcept { return statistics; }
    bool contains(const SchemaObjectID & object) const noexcept;

private:
    friend class AuthorityRepairAudit;
    AuthorityQuarantinePlan(
        AuthorityRootGraphIdentity root_,
        std::vector<SchemaObjectID> failing_seeds_,
        std::vector<SchemaObjectID> quarantined_objects_,
        AuthorityQuarantinePlanStatistics statistics_) noexcept;

    const AuthorityRootGraphIdentity root;
    const std::vector<SchemaObjectID> failing_seeds;
    const std::vector<SchemaObjectID> quarantined_objects;
    const AuthorityQuarantinePlanStatistics statistics;
};

}
