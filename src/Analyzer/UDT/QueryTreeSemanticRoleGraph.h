#pragma once

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/UDT/SemanticSinkRegistry.h>
#include <Analyzer/UDT/SemanticTransferRegistry.h>

#include <memory>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

namespace DB::UDT
{

class ProspectiveResourceBudget;

struct QueryTreeSemanticRoleGraphLimits
{
    UInt64 maximum_query_nodes = 131'072;
    UInt64 maximum_explicit_cast_boundaries = 131'072;
    UInt64 maximum_semantic_sinks = 16'384;
    UInt64 maximum_single_sink_shape_bytes = 64ULL << 10;
};

class QueryTreeSemanticRoleGraphError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidConfiguration,
        InvalidRegistration,
        UnsupportedCastShape,
        MutableAfterSeal,
        NotSealed,
        LimitExceeded,
        UnexpectedEdge,
    };

    QueryTreeSemanticRoleGraphError(Code code_, std::string_view message);

    const Code code;
};

/// Allocation-free view used to build the first sink candidate before graph
/// or planner activation. The retained descriptor is an immutable standalone
/// specialization handle and never owns an authority/catalog generation.
struct DirectExplicitCastTarget
{
    LogicalRoleInput role;
    InstantiatedTypeDescriptor::Ptr retained_descriptor;
    SemanticCapabilityMask selected_semantic_capabilities = 0;
};

struct QueryTreeSemanticRoleGraphStatistics
{
    UInt64 query_nodes = 0;
    UInt64 explicit_cast_boundaries = 0;
    UInt64 eligible_sinks = 0;
    UInt64 semantic_scratch_bytes = 0;
};

/// Narrow QueryTree adapter for the first production semantic slice. It
/// exposes only explicitly registered direct CAST boundaries. A CAST result
/// is never an exact source by itself: it is an ExactInstantiationCast
/// transfer from its operand to the checked target role. Every unregistered
/// node/path is a barrier and has no inspected inputs. Registration is closed
/// by `seal` before planner proof begins.
class QueryTreeSemanticRoleGraph final : public ISemanticRoleGraph
{
public:
    using Ptr = std::unique_ptr<QueryTreeSemanticRoleGraph>;

    /// Starts an explicit authoritative enumeration for one analyzer
    /// generation. This is also the only route to a sealed empty graph and a
    /// later explicit PhysicalOnly dependency.
    static Ptr
    create(UInt64 generation, ProspectiveResourceBudget & query_resource_budget, const QueryTreeSemanticRoleGraphLimits & limits = {});

    /// Does not allocate when first_candidate is ineligible. An eligible first
    /// candidate is registered atomically with activation, so it cannot be
    /// omitted from the sealed sink enumeration.
    static Ptr createIfEligible(
        UInt64 generation,
        const SemanticSink & first_candidate,
        ProspectiveResourceBudget & query_resource_budget,
        const QueryTreeSemanticRoleGraphLimits & limits = {});

    /// Supports exactly one logical occurrence at the root of the target
    /// tree. Nested/multiple occurrences must be rejected by the caller until
    /// a path-aware adapter is installed.
    static DirectExplicitCastTarget inspectDirectExplicitCastTarget(const BoundDeclaredTypeTree & target);

    /// Binds the checked direct destination to the analyzer-supplied stable
    /// node/path identity of the CAST result. This only prepares the demand
    /// candidate; it neither allocates a graph/planner nor claims that the
    /// source operand already has the destination role.
    static SemanticSink makeDirectExplicitCastSink(const SemanticNodePath & result, const BoundDeclaredTypeTree & target);

    ~QueryTreeSemanticRoleGraph() override;

    QueryTreeSemanticRoleGraph(const QueryTreeSemanticRoleGraph &) = delete;
    QueryTreeSemanticRoleGraph & operator=(const QueryTreeSemanticRoleGraph &) = delete;
    QueryTreeSemanticRoleGraph(QueryTreeSemanticRoleGraph &&) = delete;
    QueryTreeSemanticRoleGraph & operator=(QueryTreeSemanticRoleGraph &&) = delete;

    /// Returns false for an idempotent duplicate registration. The function
    /// must already be resolved as public CAST and have exactly the physical
    /// result represented by target. `result` and `input` are stable identities
    /// from the same sealed analyzer generation.
    bool registerDirectExplicitCastBoundary(
        const SemanticNodePath & result, const SemanticNodePath & input, FunctionNodePtr function, BoundDeclaredTypeTree::Ptr target);

    /// Registers one approved contextual literal together with the exact
    /// prebound schema column that supplied its expected role. The column and
    /// immutable bound snapshot are retained only until planner sealing. The
    /// candidate may already be sink 0 when it activated the graph.
    bool registerContextualConstantBoundary(
        const SemanticNodePath & constant,
        ConstantNodePtr constant_node,
        const SemanticNodePath & expected_column,
        ColumnNodePtr expected_column_node,
        BoundObjectTypeReferences::Ptr references,
        PersistedTypeOccurrencePath use_path,
        const SemanticSink & sink);

    /// Demand-slice primitives used by the analyzer adapter after an eligible
    /// sink has activated the graph. These never discover nodes on their own:
    /// callers register only exact edges already visited for that sink.
    bool registerPreboundExactSource(
        const SemanticNodePath & state,
        QueryTreeNodePtr state_node,
        ColumnNodePtr storage_column,
        BoundObjectTypeReferences::Ptr references,
        PersistedTypeOccurrencePath use_path);
    bool registerNullOnlySource(const SemanticNodePath & state, QueryTreeNodePtr state_node);
    bool registerTransferBoundary(
        const SemanticNodePath & result,
        QueryTreeNodePtr result_node,
        SemanticTransferKind transfer,
        std::vector<std::pair<SemanticNodePath, QueryTreeNodePtr>> inputs,
        std::optional<String> result_shape = std::nullopt,
        std::optional<UInt32> sparse_projection_ordinal = std::nullopt);

    /// Reserves an exact query-local identity as soon as the demand adapter
    /// first sees a node. This keeps IDs stable for shared barrier nodes even
    /// before a later registered transfer owns their edges.
    void registerStableNodeIdentity(SemanticNodeID id, const IQueryTreeNode * node);

    /// Builds an allocation-free contextual candidate borrowing its exact
    /// role from an immutable prebound object use.
    static SemanticSink makePreboundContextualConstantSink(
        const SemanticNodePath & constant,
        SemanticSinkKind kind,
        const BoundObjectTypeReferences & references,
        const BoundObjectTypeReferenceUse & use);

    /// Allocation-free lookup used by the analyzer adapter to reuse the same
    /// query-local stable ID when a registered CAST result is consumed by a
    /// later boundary. The graph remains the sole owner of the pointer/ID map.
    std::optional<SemanticNodeID> findRegisteredNodeID(const IQueryTreeNode * node) const noexcept;

    /// Rebinds every registered exact node identity through one analyzer-owned
    /// deep-clone mapping without changing stable semantic IDs or allocating a
    /// second graph generation. Nodes outside the cloned subtree are retained;
    /// a replacement with an incompatible node kind fails closed.
    void remapQueryTreeNodes(const IQueryTreeNode::CloneNodeMapping & clone_node_mapping);

    /// Registers one further candidate while the graph generation is open.
    /// Ineligible candidates are ignored. Eligible candidates are owned by the
    /// graph and later imported by the planner as one complete sealed set.
    std::optional<SemanticSinkID> registerSink(const SemanticSink & sink);

    void seal();
    bool isSealed() const noexcept;

    /// Revalidates every registered QueryTree boundary and sink against the
    /// live nodes retained by this exact graph instance. Planner sealing and
    /// every subsequent sealed-result access use this guard.
    void validateSealed() const;

    /// Sealed enumeration consumed by SemanticRolePlanner. These methods do
    /// not mint a completeness claim; only planner sealing can do that.
    UInt64 getSinkCount() const;
    SemanticSink getSink(SemanticSinkID sink) const;

    UInt64 getGeneration() const noexcept override;
    SemanticRoleNode describe(const SemanticNodePath & state) const override;
    SemanticNodePath getInput(const SemanticNodePath & state, UInt32 input_index) const override;

    const QueryTreeSemanticRoleGraphStatistics & getStatistics() const noexcept;

private:
    struct Impl;

    QueryTreeSemanticRoleGraph(
        UInt64 generation,
        ProspectiveResourceBudget & query_resource_budget,
        const QueryTreeSemanticRoleGraphLimits & limits,
        UInt64 admitted_base_scratch_bytes);

    std::unique_ptr<Impl> impl;
};

}
