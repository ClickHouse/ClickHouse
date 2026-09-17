#pragma once

#include <Analyzer/UDT/SemanticRoleTypes.h>

#include <optional>

namespace DB::UDT
{

/// Every function/operator that is not deliberately mapped to one of these
/// values is Unregistered and therefore a semantic-role barrier.
enum class SemanticTransferKind : UInt8
{
    Unregistered,
    Identity,
    Rename,
    StaticChildSelection,
    StaticReshape,
    NullableLift,
    LowCardinalityReshape,
    UnanimousBranch,
    UnanimousUnion,
    ExactInstantiationCast,
    JoinDirectNonSynthesizing,
    JoinDirectNullableLift,
    JoinUsingUnanimous,
    Count,
};

enum class SemanticTransferPolicy : UInt8
{
    PreserveUnary,
    ReshapeUnary,
    MeetUnanimous,
    PreserveExactInstantiation,
};

enum class SemanticNullContract : UInt8
{
    None,
    /// Outer NULL rows bypass UDT INPUT, OUTPUT, and CHECK programs; only
    /// compacted non-null values enter those programs.
    OuterNullMapBypassesSemanticPrograms,
};

struct SemanticTransferDescriptor
{
    SemanticTransferKind kind = SemanticTransferKind::Unregistered;
    SemanticTransferPolicy policy = SemanticTransferPolicy::PreserveUnary;
    UInt32 minimum_inputs = 0;
    UInt32 maximum_inputs = 0;
    bool requires_result_shape = false;
    bool allows_result_shape = false;
    bool requires_exact_target = false;
    SemanticNullContract null_contract = SemanticNullContract::None;
};

class SemanticTransferRegistry final
{
public:
    /// Returns null for Unregistered, Count, and invalid enum values. This is
    /// the deny-by-default boundary used by the planner.
    static const SemanticTransferDescriptor * find(SemanticTransferKind kind) noexcept;
};

enum class SemanticRoleSourceKind : UInt8
{
    None,
    NullOnly,
    Exact,
    PreboundExact,
};

struct SemanticRoleSource
{
    SemanticRoleSourceKind kind = SemanticRoleSourceKind::None;
    LogicalRoleInput exact_role;

    static SemanticRoleSource none() noexcept { return {}; }
    static SemanticRoleSource nullOnly() noexcept { return {SemanticRoleSourceKind::NullOnly, {}}; }
    static SemanticRoleSource exact(LogicalRoleInput role) noexcept { return {SemanticRoleSourceKind::Exact, role}; }
    static SemanticRoleSource preboundExact(LogicalRoleInput role) noexcept { return {SemanticRoleSourceKind::PreboundExact, role}; }
};

/// Allocation-free description of one demanded node/path state. Input edges
/// are fetched individually only after the planner admits their prospective
/// work. Views must remain valid while the graph generation is current.
struct SemanticRoleNode
{
    SemanticRoleSource source;
    SemanticTransferKind transfer = SemanticTransferKind::Unregistered;
    UInt32 input_count = 0;
    std::optional<LogicalShapeInput> result_shape;
    std::optional<LogicalRoleInput> exact_target;
};

class ISemanticRoleGraph
{
public:
    virtual ~ISemanticRoleGraph() = default;

    virtual UInt64 getGeneration() const noexcept = 0;
    virtual SemanticRoleNode describe(const SemanticNodePath & state) const = 0;
    virtual SemanticNodePath getInput(const SemanticNodePath & state, UInt32 input_index) const = 0;
};

}
