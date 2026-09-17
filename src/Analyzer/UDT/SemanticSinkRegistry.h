#pragma once

#include <Analyzer/UDT/SemanticRoleTypes.h>

#include <DataTypes/UDT/Definition.h>

#include <optional>

namespace DB::UDT
{

/// This is a closed registry. Adding a sink requires an explicit descriptor,
/// an analyzer hook, and tests; arbitrary function names cannot opt in.
enum class SemanticSinkKind : UInt8
{
    /// Reserved closed-registry vocabulary. Admission remains unreachable
    /// until construction ownership and transfer rules are activated together.
    ExplicitUDTConstruction,
    ExplicitUDTCast,
    /// Reserved target vocabulary. `find()` deliberately rejects these kinds
    /// while mutation-target semantic admission is inactive.
    InsertTarget,
    AssignmentTarget,
    EqualityConstant,
    InConstant,
    GlobalInConstant,
    HasConstant,
    HasAnyConstant,
    /// Reserved observation vocabulary. These kinds remain deterministically
    /// ineligible while logical rendering and introspection are inactive.
    LogicalRendering,
    LogicalIntrospection,
    /// Reserved propagation vocabulary. Subquery output remains ineligible
    /// until its ownership-transfer contract is active.
    SubqueryOutput,
    Count,
};

struct SemanticExpectedRole
{
    LogicalRoleInput role;

    /// Explicit query syntax may transfer one independently owned immutable
    /// specialization handle to planner state. Prebound schema roles leave it
    /// null and remain borrowed from their metadata snapshot.
    InstantiatedTypeDescriptor::Ptr retained_descriptor;
};

struct SemanticSink
{
    SemanticNodePath source;
    SemanticSinkKind kind = SemanticSinkKind::Count;

    /// The object-level OR is only a constant-time fast negative. Eligibility
    /// always checks the selected ordinal/path mask as well.
    SemanticCapabilityMask object_semantic_capabilities = 0;
    SemanticCapabilityMask selected_semantic_capabilities = 0;

    /// Set only for an explicit logical response, never merely because a UDT
    /// descriptor is present in the schema.
    bool observes_identity = false;
    std::optional<SemanticExpectedRole> expected_role;
};

struct SemanticSinkDescriptor
{
    SemanticSinkKind kind = SemanticSinkKind::Count;
    SemanticCapabilityMask activation_capabilities = 0;
    bool requires_expected_role = false;
    bool allows_expected_role = false;
    bool allows_identity_observation = false;
};

class SemanticSinkRegistry final
{
public:
    static const SemanticSinkDescriptor * find(SemanticSinkKind kind) noexcept;

    /// Allocation-free, constant-time, and fail-closed. Invalid or descriptor-
    /// mismatched masks, unsupported identity observation, and malformed
    /// expected roles are ineligible instead of creating planner state.
    static bool isEligible(const SemanticSink & sink) noexcept;
};

}
