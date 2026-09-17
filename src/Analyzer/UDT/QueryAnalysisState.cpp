#include <Analyzer/UDT/QueryAnalysisState.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/UDT/QueryTreeSemanticRoleGraph.h>
#include <Analyzer/UDT/SemanticRoleLimitAdapters.h>
#include <Analyzer/UDT/SemanticRolePlanner.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>

#include <Columns/IColumn.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/UDT/InstantiatedTypeDescriptor.h>
#include <DataTypes/UDT/ResourceAccounting.h>
#include <DataTypes/UDT/ResourceLimitAdapters.h>
#include <DataTypes/UDT/ResourceLimits.h>
#include <DataTypes/hasNullable.h>

#include <Storages/StorageSnapshot.h>

#include <algorithm>
#include <array>
#include <limits>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LIMIT_EXCEEDED;
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
}
}

namespace DB::UDT
{
namespace
{

[[noreturn]] void rethrowSemanticAnalysisError(const QueryTreeSemanticRoleGraphError & error)
{
    using Code = QueryTreeSemanticRoleGraphError::Code;
    const int exception_code = [&]
    {
        switch (error.code)
        {
            case Code::UnsupportedCastShape: return ErrorCodes::NOT_IMPLEMENTED;
            case Code::LimitExceeded: return ErrorCodes::LIMIT_EXCEEDED;
            case Code::InvalidConfiguration:
            case Code::InvalidRegistration:
            case Code::MutableAfterSeal:
            case Code::NotSealed:
            case Code::UnexpectedEdge: return ErrorCodes::LOGICAL_ERROR;
        }
        return ErrorCodes::LOGICAL_ERROR;
    }();
    throw Exception(exception_code, "UDT semantic CAST analysis failed: {}", error.what());
}

[[noreturn]] void rethrowSemanticAnalysisError(const SemanticRolePlannerError & error)
{
    using Code = SemanticRolePlannerError::Code;
    const int exception_code = error.code == Code::LimitExceeded ? ErrorCodes::LIMIT_EXCEEDED : ErrorCodes::LOGICAL_ERROR;
    throw Exception(exception_code, "UDT semantic-role planning failed: {}", error.what());
}

[[noreturn]] void rethrowSemanticAnalysisError(const SemanticCacheDependencyError & error)
{
    using Code = SemanticCacheDependencyError::Code;
    const int exception_code = error.code == Code::LimitExceeded ? ErrorCodes::LIMIT_EXCEEDED : ErrorCodes::LOGICAL_ERROR;
    throw Exception(exception_code, "UDT semantic cache dependency failed: {}", error.what());
}

bool isWrapperType(const DataTypePtr & type) noexcept
{
    return type && (type->getTypeId() == TypeIndex::Nullable || type->getTypeId() == TypeIndex::LowCardinality);
}

DataTypePtr unwrapWrapper(const DataTypePtr & type)
{
    if (!type)
        return {};
    if (type->getTypeId() == TypeIndex::Nullable)
        return assert_cast<const DataTypeNullable &>(*type).getNestedType();
    if (type->getTypeId() == TypeIndex::LowCardinality)
        return assert_cast<const DataTypeLowCardinality &>(*type).getDictionaryType();
    return {};
}

DataTypePtr supportedPhysicalChild(const DataTypePtr & type, UInt64 ordinal)
{
    if (!type)
        return {};
    switch (type->getTypeId())
    {
        case TypeIndex::Array: return ordinal == 0 ? assert_cast<const DataTypeArray &>(*type).getNestedType() : DataTypePtr{};
        case TypeIndex::Nullable: return ordinal == 0 ? assert_cast<const DataTypeNullable &>(*type).getNestedType() : DataTypePtr{};
        case TypeIndex::LowCardinality:
            return ordinal == 0 ? assert_cast<const DataTypeLowCardinality &>(*type).getDictionaryType() : DataTypePtr{};
        case TypeIndex::Tuple: {
            const auto & elements = assert_cast<const DataTypeTuple &>(*type).getElements();
            return ordinal < elements.size() ? elements[static_cast<size_t>(ordinal)] : DataTypePtr{};
        }
        default: return {};
    }
}

struct ContextualEndpoint
{
    std::vector<UInt64> path;
    DataTypePtr physical_type;
};

void appendWrapperEndpoints(std::vector<ContextualEndpoint> & endpoints, std::vector<UInt64> path, DataTypePtr type, UInt64 maximum_depth)
{
    while (type)
    {
        endpoints.push_back({path, type});
        if (!isWrapperType(type))
            return;
        if (path.size() >= maximum_depth)
            throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UDT contextual wrapper path exceeds its depth limit");
        path.push_back(0);
        type = unwrapWrapper(type);
    }
}

}

QueryAnalysisState::QueryAnalysisState() = default;
QueryAnalysisState::~QueryAnalysisState() = default;

ProspectiveResourceBudget & QueryAnalysisState::getOrCreateSemanticResourceBudget(const TypeAuthorityLimits & authority_limits)
{
    const auto effective_limits = [&]
    {
        try
        {
            return makeDefaultQueryEffectiveResourceLimits(authority_limits);
        }
        catch (const ResourceLimitError & error)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid UDT semantic resource limit configuration: {}", error.what());
        }
    }();
    return getOrCreateSemanticResourceBudget(effective_limits);
}

ProspectiveResourceBudget & QueryAnalysisState::getOrCreateSemanticResourceBudget(const EffectiveResourceLimits & effective_limits)
{
    if (!resource_ledger)
        resource_ledger = std::make_shared<QueryResourceLedger>();
    if (!semantic_resource_budget)
        semantic_resource_budget = std::make_unique<ProspectiveResourceBudget>(effective_limits, resource_ledger);

    const auto admission = semantic_resource_budget->admitCurrentUsage(effective_limits);
    if (!admission.isAccepted())
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "{}", formatResourceAdmissionFailure(admission));

    return *semantic_resource_budget;
}

void QueryAnalysisState::chargeSemanticDiscoveryWork(UInt64 node_path_states, UInt64 inspected_edges, UInt64 scratch_bytes)
{
    if (node_path_states < charged_semantic_discovery_node_path_states || inspected_edges < charged_semantic_discovery_inspected_edges
        || scratch_bytes < charged_semantic_discovery_scratch_bytes)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "UDT semantic discovery work totals are not monotonic");

    if (!semantic_resource_budget)
        return;

    ResourceDelta delta;
    delta.add(ResourceLimit::NodePathStatesPerQuery, node_path_states - charged_semantic_discovery_node_path_states);
    delta.add(ResourceLimit::InspectedEdgesPerQuery, inspected_edges - charged_semantic_discovery_inspected_edges);
    delta.add(ResourceLimit::SemanticScratchBytesPerQuery, scratch_bytes - charged_semantic_discovery_scratch_bytes);
    const auto admission = semantic_resource_budget->charge(delta);
    if (!admission.isAccepted())
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "{}", formatResourceAdmissionFailure(admission));

    charged_semantic_discovery_node_path_states = node_path_states;
    charged_semantic_discovery_inspected_edges = inspected_edges;
    charged_semantic_discovery_scratch_bytes = scratch_bytes;
}

bool QueryAnalysisState::hasQueryTreeRegistrations() const noexcept
{
    return semantic_role_graph != nullptr || !resolved_explicit_cast_targets.empty();
}

void QueryAnalysisState::remapSemanticGenerationAfterQueryTreeReplacement(const IQueryTreeNode::CloneNodeMapping & clone_node_mapping)
{
    if (semantic_analysis_finalized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A QueryTree generation replacement reached finalized UDT semantic analysis");

    for (auto target = resolved_explicit_cast_targets.begin(); target != resolved_explicit_cast_targets.end();)
    {
        const auto replacement = clone_node_mapping.find(target->first);
        if (replacement == clone_node_mapping.end())
        {
            ++target;
            continue;
        }
        const auto * replacement_function = replacement->second ? replacement->second->as<FunctionNode>() : nullptr;
        if (!replacement_function)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A cloned resolved UDT CAST no longer maps to a FunctionNode");

        auto node = resolved_explicit_cast_targets.extract(target++);
        node.key() = replacement_function;
        const auto inserted = resolved_explicit_cast_targets.insert(std::move(node));
        if (!inserted.inserted)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cloned resolved UDT CAST targets collide on one QueryTree node");
    }
    if (semantic_role_graph)
        semantic_role_graph->remapQueryTreeNodes(clone_node_mapping);
}

bool QueryAnalysisState::isDirectExplicitCastEligible(const BoundDeclaredTypeTree & target) const
{
    const auto * descriptor = SemanticSinkRegistry::find(SemanticSinkKind::ExplicitUDTCast);
    if (!descriptor)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The closed UDT semantic sink registry has no explicit CAST entry");

    /// Preserve the registry's object-level constant-time fast negative. In
    /// particular, an output-only nested logical target stays on the physical
    /// path and must not be rejected merely because the direct-root adapter is
    /// intentionally narrower than the full declared-type tree.
    if ((target.getSemanticCapabilities() & descriptor->activation_capabilities) == 0)
        return false;

    try
    {
        constexpr SemanticNodePath probe_path{0, 0};
        return SemanticSinkRegistry::isEligible(QueryTreeSemanticRoleGraph::makeDirectExplicitCastSink(probe_path, target));
    }
    catch (const QueryTreeSemanticRoleGraphError & error)
    {
        rethrowSemanticAnalysisError(error);
    }
}

void QueryAnalysisState::rememberResolvedExplicitCastTarget(
    const FunctionNode * function, std::shared_ptr<const BoundDeclaredTypeTree> target)
{
    if (!function || !target)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Resolved UDT CAST target retention is incomplete");
    if (semantic_analysis_finalized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Resolved UDT CAST target reached a finalized query analysis");
    const auto * target_identity = target.get();
    const auto [found, inserted] = resolved_explicit_cast_targets.emplace(function, std::move(target));
    if (!inserted && found->second.get() != target_identity)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "One resolved CAST node maps to conflicting UDT targets");
}

std::shared_ptr<const BoundDeclaredTypeTree>
QueryAnalysisState::findResolvedExplicitCastTarget(const FunctionNode * function) const noexcept
{
    const auto found = resolved_explicit_cast_targets.find(function);
    return found == resolved_explicit_cast_targets.end() ? nullptr : found->second;
}

std::shared_ptr<const BoundDeclaredTypeTree>
QueryAnalysisState::inspectPreboundStoredExplicitCast(const BoundObjectTypeReferences::Ptr & references, UInt64 stored_expression_ordinal)
{
    if (!references)
        return {};
    const auto & object = references->getObject();
    if (!object.isValid() || object.kind != SchemaObjectKind::View || !references->getObjectSchemaRevision())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A stored UDT CAST has no trusted bound View identity");

    const String owner = "stored-expression:" + std::to_string(stored_expression_ordinal);
    const auto uses = references->findRuntimeUsesByPrefix(
        PersistedTypePathSection::ViewExpression, PersistedTypeOccurrenceSite::StoredExpression, owner, {});
    if (uses.empty())
        return {}; /// The trusted stored CAST endpoint is physical-only.

    const auto * sink_descriptor = SemanticSinkRegistry::find(SemanticSinkKind::ExplicitUDTCast);
    if (!sink_descriptor)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The closed UDT semantic sink registry has no explicit CAST entry");

    SemanticCapabilityMask selected_capabilities = 0;
    for (const auto * use : uses)
    {
        if (!use || use->getPath().section != PersistedTypePathSection::ViewExpression
            || use->getPath().site != PersistedTypeOccurrenceSite::StoredExpression
            || use->getPath().object_ordinal != stored_expression_ordinal || use->getRuntimeOwnerKey() != owner)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A stored UDT CAST endpoint disagrees with its V2 owner locator");
        selected_capabilities = static_cast<SemanticCapabilityMask>(selected_capabilities | use->getSemanticCapabilities());
    }
    if ((selected_capabilities & sink_descriptor->activation_capabilities) == 0)
        return {};

    const auto * use = uses.front();
    if (uses.size() != 1 || !use->getPath().type_child_ordinals.empty() || use->getPath().occurrence_ordinal != 0)
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "A stored UDT CAST with executable semantics must have exactly one direct-root logical occurrence");
    }
    const auto descriptors = references->getDescriptors();
    if (use->getDescriptorIndex() >= descriptors.size() || !descriptors[use->getDescriptorIndex()] || !use->getPhysicalType())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A stored UDT CAST endpoint lost its checked descriptor or physical type");

    try
    {
        std::vector<BoundDeclaredTypeNodeInput> nodes{{.type_child_ordinals = {}, .physical_type = use->getPhysicalType()}};
        std::vector<BoundDeclaredTypeOccurrenceInput> occurrences{{
            .type_child_ordinals = {},
            .logical_descriptor = descriptors[use->getDescriptorIndex()],
            .logical_preorder = 0,
        }};
        return BoundDeclaredTypeTree::build(std::move(nodes), std::move(occurrences), {});
    }
    catch (const DescriptorError & error)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A stored UDT CAST cannot rebuild its checked direct target: {}", error.what());
    }
}

bool QueryAnalysisState::registerResolvedDirectExplicitCast(
    std::shared_ptr<FunctionNode> function,
    std::shared_ptr<const BoundDeclaredTypeTree> target,
    const TypeAuthorityLimits & authority_limits,
    const EffectiveResourceLimits * exact_effective_query_limits,
    const std::function<bool(const ColumnNode &)> & source_column_is_non_synthesizing,
    const std::function<std::optional<SparseProjectionSource>(const ColumnNode &)> & sparse_projection_source,
    const std::function<bool(const FunctionNode &)> & source_is_full_join_using_unanimous)
{
    if (!function || !target)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "UDT semantic CAST registration has no resolved function or exact target");

    if (!isDirectExplicitCastEligible(*target))
        return false;
    if (semantic_analysis_finalized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "UDT semantic CAST reached analysis after the semantic graph was sealed");

    auto & budget = exact_effective_query_limits ? getOrCreateSemanticResourceBudget(*exact_effective_query_limits)
                                                 : getOrCreateSemanticResourceBudget(authority_limits);

    try
    {
        const auto stable_node_id = [&](const IQueryTreeNode * node)
        {
            if (semantic_role_graph)
            {
                if (const auto registered = semantic_role_graph->findRegisteredNodeID(node))
                    return *registered;
            }
            if (next_semantic_node_id == invalid_semantic_node_id)
                throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UDT semantic query-local node IDs are exhausted");
            const auto id = next_semantic_node_id++;
            if (semantic_role_graph)
                semantic_role_graph->registerStableNodeIdentity(id, node);
            return id;
        };

        const auto & arguments = function->getArguments().getNodes();
        if (arguments.size() != 2 || !arguments.front())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Resolved UDT semantic CAST has an invalid argument list");
        const auto source_node = arguments.front();

        const SemanticNodePath result{stable_node_id(function.get()), 0};
        const SemanticNodePath input{stable_node_id(source_node.get()), 0};
        const auto sink = QueryTreeSemanticRoleGraph::makeDirectExplicitCastSink(result, *target);

        const bool activate_graph = !semantic_role_graph;
        if (!semantic_role_graph)
        {
            semantic_role_graph
                = QueryTreeSemanticRoleGraph::createIfEligible(1, sink, budget, makeQueryTreeSemanticRoleGraphLimits(budget.getLimits()));
            if (!semantic_role_graph)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Eligible UDT semantic CAST did not activate its query graph");
        }
        if (!semantic_role_graph->registerDirectExplicitCastBoundary(result, input, std::move(function), std::move(target)))
            return true;
        if (!activate_graph && !semantic_role_graph->registerSink(sink))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Eligible UDT semantic CAST sink was omitted from its query graph");

        const auto allocate_path = [&]() -> LogicalPathID
        {
            if (next_semantic_path_id == invalid_logical_path_id)
                throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UDT semantic query-local logical path IDs are exhausted");
            return next_semantic_path_id++;
        };

        const auto charge_source_discovery_work = [&](UInt64 node_path_states, UInt64 inspected_edges, UInt64 scratch_bytes)
        {
            ResourceDelta delta;
            delta.add(ResourceLimit::NodePathStatesPerQuery, node_path_states);
            delta.add(ResourceLimit::InspectedEdgesPerQuery, inspected_edges);
            delta.add(ResourceLimit::SemanticScratchBytesPerQuery, scratch_bytes);
            const auto admission = budget.charge(delta);
            if (!admission.isAccepted())
                throw Exception(ErrorCodes::LIMIT_EXCEEDED, "{}", formatResourceAdmissionFailure(admission));
        };
        const auto ensure_vector_capacity = [&]<typename T>(std::vector<T> & values, size_t required_capacity)
        {
            if (required_capacity <= values.capacity())
                return;
            const size_t current_capacity = values.capacity();
            const size_t doubled_capacity
                = current_capacity > values.max_size() / 2 ? values.max_size() : std::max<size_t>(1, current_capacity * 2);
            const size_t next_capacity = std::max(required_capacity, doubled_capacity);
            if (next_capacity > values.max_size())
                throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UDT semantic discovery vector capacity is exhausted");
            const size_t added_capacity = next_capacity - current_capacity;
            if (added_capacity > std::numeric_limits<UInt64>::max() / sizeof(T))
                throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UDT semantic discovery scratch bytes overflow UInt64");
            charge_source_discovery_work(0, 0, static_cast<UInt64>(added_capacity) * sizeof(T));
            values.reserve(next_capacity);
        };

        const auto register_column_source = [&](const ColumnNodePtr & column, const SemanticNodePath & output) -> bool
        {
            if (!column || column->getColumn().isSubcolumn())
                return false;
            auto candidate = inspectPreboundContextualConstant(*column, SemanticSinkKind::ExplicitUDTCast);
            if (!candidate)
                return false;
            const auto * use = candidate->references->findUse(candidate->use_path);
            if (!use || !use->getPhysicalType())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "A demanded prebound UDT CAST source lost its exact use");

            const auto storage_type = column->getColumn().getTypeInStorage();
            const auto output_type = column->getResultType();
            if (!storage_type || !output_type)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "A demanded prebound UDT CAST source has no physical type");
            const bool join_nullable_lift = !output_type->equals(*storage_type) && isNullableOrLowCardinalityNullable(output_type)
                && removeNullableOrLowCardinalityNullable(output_type)->equals(*storage_type);
            if (!output_type->equals(*storage_type) && !join_nullable_lift)
                return false;

            std::vector<DataTypePtr> wrapper_types;
            ensure_vector_capacity(wrapper_types, candidate->use_path.type_child_ordinals.size());
            auto current_type = storage_type;
            for (const UInt64 ordinal : candidate->use_path.type_child_ordinals)
            {
                if (ordinal != 0 || !isWrapperType(current_type))
                    return false;
                wrapper_types.push_back(current_type);
                current_type = unwrapWrapper(current_type);
            }
            if (!current_type || !current_type->equals(*use->getPhysicalType()))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "A demanded prebound UDT CAST source wrapper path changed after lookup");

            const bool needs_transfer = !wrapper_types.empty() || join_nullable_lift;
            SemanticNodePath current_state = needs_transfer ? SemanticNodePath{output.node, allocate_path()} : output;
            semantic_role_graph->registerPreboundExactSource(current_state, column, column, candidate->references, candidate->use_path);

            for (size_t depth = wrapper_types.size(); depth > 0; --depth)
            {
                const auto & wrapper = wrapper_types[depth - 1];
                const auto transfer = wrapper->getTypeId() == TypeIndex::Nullable ? SemanticTransferKind::NullableLift
                                                                                  : SemanticTransferKind::LowCardinalityReshape;
                const bool is_storage_root = depth == 1;
                const auto next_state = is_storage_root && !join_nullable_lift ? output : SemanticNodePath{output.node, allocate_path()};
                semantic_role_graph->registerTransferBoundary(
                    next_state,
                    column,
                    transfer,
                    std::vector<std::pair<SemanticNodePath, QueryTreeNodePtr>>{{current_state, column}},
                    wrapper->getName());
                current_state = next_state;
            }
            if (join_nullable_lift)
            {
                semantic_role_graph->registerTransferBoundary(
                    output,
                    column,
                    SemanticTransferKind::JoinDirectNullableLift,
                    std::vector<std::pair<SemanticNodePath, QueryTreeNodePtr>>{{current_state, column}},
                    output_type->getName());
            }
            return true;
        };

        struct StaticColumnPath
        {
            ColumnNodePtr column;
            std::vector<UInt64> path;
        };
        std::function<std::optional<StaticColumnPath>(const QueryTreeNodePtr &, UInt64)> find_static_column;
        find_static_column = [&](const QueryTreeNodePtr & node, UInt64 depth) -> std::optional<StaticColumnPath>
        {
            if (!node || depth > 256)
                return std::nullopt;
            charge_source_discovery_work(1, depth ? 1 : 0, 0);
            if (const auto * column = node->as<ColumnNode>())
            {
                if (column->getColumn().isSubcolumn())
                    return std::nullopt;
                return StaticColumnPath{std::static_pointer_cast<ColumnNode>(node), {}};
            }
            const auto * static_child = node->as<FunctionNode>();
            if (!static_child || !static_child->isResolved() || !equalsCaseInsensitive(static_child->getFunctionName(), "tupleElement"))
                return std::nullopt;
            const auto & static_arguments = static_child->getArguments().getNodes();
            if (static_arguments.size() != 2)
                return std::nullopt;
            auto source = find_static_column(static_arguments[0], depth + 1);
            const auto * selector = static_arguments[1] ? static_arguments[1]->as<ConstantNode>() : nullptr;
            if (!source || !selector)
                return std::nullopt;
            auto type = source->column->getColumn().getTypeInStorage();
            for (const UInt64 ordinal : source->path)
                type = supportedPhysicalChild(type, ordinal);
            while (isWrapperType(type))
            {
                ensure_vector_capacity(source->path, source->path.size() + 1);
                source->path.push_back(0);
                type = unwrapWrapper(type);
            }
            const auto * tuple = type ? typeid_cast<const DataTypeTuple *>(type.get()) : nullptr;
            if (!tuple)
                return std::nullopt;
            const Field selector_value = selector->getValue();
            std::optional<size_t> element;
            if (selector_value.getType() == Field::Types::UInt64)
            {
                const auto one_based = selector_value.safeGet<UInt64>();
                if (one_based && one_based <= tuple->getElements().size())
                    element = static_cast<size_t>(one_based - 1);
            }
            else if (selector_value.getType() == Field::Types::Int64)
            {
                const auto one_based = selector_value.safeGet<Int64>();
                if (one_based > 0 && static_cast<UInt64>(one_based) <= tuple->getElements().size())
                    element = static_cast<size_t>(one_based - 1);
            }
            else if (selector_value.getType() == Field::Types::String)
                element = tuple->tryGetPositionByName(selector_value.safeGet<String>());
            if (!element)
                return std::nullopt;
            ensure_vector_capacity(source->path, source->path.size() + 1);
            source->path.push_back(static_cast<UInt64>(*element));
            return source;
        };

        const auto planner_limits = makeSemanticRolePlannerLimits(budget.getLimits());
        enum class SourceSliceAction : UInt8
        {
            Visit,
            FinishRename,
            FinishBranch,
            FinishSparseProjection,
            FinishJoinUsing,
        };
        struct SourceSliceFrame
        {
            SourceSliceAction action = SourceSliceAction::Visit;
            QueryTreeNodePtr node;
            SemanticNodePath state;
            UInt64 depth = 0;
            std::vector<std::pair<SemanticNodePath, QueryTreeNodePtr>> inputs;
            std::optional<String> result_shape;
            std::optional<UInt32> sparse_projection_ordinal;
            bool unanimous_union = false;
            bool nullable_lift = false;
        };
        const auto register_source_slice = [&](const QueryTreeNodePtr & root, const SemanticNodePath & root_state)
        {
            std::vector<SourceSliceFrame> stack;
            const auto push_frame = [&](SourceSliceFrame frame)
            {
                ensure_vector_capacity(stack, stack.size() + 1);
                stack.push_back(std::move(frame));
            };
            push_frame({
                .node = root,
                .state = root_state,
                .inputs = {},
                .result_shape = std::nullopt,
                .sparse_projection_ordinal = std::nullopt,
            });
            UInt64 visited_states = 0;
            while (!stack.empty())
            {
                auto frame = std::move(stack.back());
                stack.pop_back();
                if (!frame.node || frame.depth >= planner_limits.maximum_active_depth)
                    throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UDT semantic source slice exceeds its active-depth limit");
                if (frame.action == SourceSliceAction::FinishRename)
                {
                    semantic_role_graph->registerTransferBoundary(
                        frame.state, frame.node, SemanticTransferKind::Rename, std::move(frame.inputs));
                    continue;
                }
                if (frame.action == SourceSliceAction::FinishBranch)
                {
                    semantic_role_graph->registerTransferBoundary(
                        frame.state,
                        frame.node,
                        SemanticTransferKind::UnanimousBranch,
                        std::move(frame.inputs),
                        std::move(frame.result_shape));
                    continue;
                }
                if (frame.action == SourceSliceAction::FinishSparseProjection)
                {
                    semantic_role_graph->registerTransferBoundary(
                        frame.state,
                        frame.node,
                        frame.unanimous_union     ? SemanticTransferKind::UnanimousUnion
                            : frame.nullable_lift ? SemanticTransferKind::JoinDirectNullableLift
                                                  : SemanticTransferKind::Rename,
                        std::move(frame.inputs),
                        std::move(frame.result_shape),
                        frame.sparse_projection_ordinal);
                    continue;
                }
                if (frame.action == SourceSliceAction::FinishJoinUsing)
                {
                    semantic_role_graph->registerTransferBoundary(
                        frame.state,
                        frame.node,
                        SemanticTransferKind::JoinUsingUnanimous,
                        std::move(frame.inputs),
                        std::move(frame.result_shape));
                    continue;
                }
                if (++visited_states > planner_limits.maximum_demanded_states)
                    throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UDT semantic source slice exceeds its demanded-state limit");
                charge_source_discovery_work(1, 0, 0);

                const auto & node = frame.node;
                const auto & state = frame.state;
                if (const auto * constant = node->as<ConstantNode>())
                {
                    if (constant->isNull())
                        semantic_role_graph->registerNullOnlySource(state, node);
                    continue;
                }
                if (const auto * column = node->as<ColumnNode>())
                {
                    const auto column_ptr = std::static_pointer_cast<ColumnNode>(node);
                    if (source_column_is_non_synthesizing && !source_column_is_non_synthesizing(*column))
                        continue;
                    if (register_column_source(column_ptr, state))
                        continue;
                    if (column->hasExpression() && column->getResultType() && column->getExpression()->getResultType()
                        && column->getResultType()->equals(*column->getExpression()->getResultType()))
                    {
                        const auto input_state = SemanticNodePath{stable_node_id(column->getExpression().get()), 0};
                        std::vector<std::pair<SemanticNodePath, QueryTreeNodePtr>> rename_inputs;
                        ensure_vector_capacity(rename_inputs, 1);
                        rename_inputs.emplace_back(input_state, column->getExpression());
                        charge_source_discovery_work(0, 1, 0);
                        push_frame({
                            .action = SourceSliceAction::FinishRename,
                            .node = node,
                            .state = state,
                            .depth = frame.depth,
                            .inputs = std::move(rename_inputs),
                            .result_shape = std::nullopt,
                            .sparse_projection_ordinal = std::nullopt,
                        });
                        push_frame({
                            .node = column->getExpression(),
                            .state = input_state,
                            .depth = frame.depth + 1,
                            .inputs = {},
                            .result_shape = std::nullopt,
                            .sparse_projection_ordinal = std::nullopt,
                        });
                        continue;
                    }
                    if (!column->getResultType())
                        continue;
                    auto sparse = sparse_projection_source ? sparse_projection_source(*column) : std::nullopt;
                    if (!sparse || sparse->inputs.empty())
                        continue;
                    std::vector<std::pair<SemanticNodePath, QueryTreeNodePtr>> projection_inputs;
                    ensure_vector_capacity(projection_inputs, sparse->inputs.size());
                    charge_source_discovery_work(0, static_cast<UInt64>(sparse->inputs.size()), 0);
                    bool requires_reshape = false;
                    for (const auto & projection_input : sparse->inputs)
                    {
                        if (!projection_input)
                            throw Exception(ErrorCodes::LOGICAL_ERROR, "UDT sparse projection source contains an empty input");
                        const auto projection_state = SemanticNodePath{stable_node_id(projection_input.get()), 0};
                        projection_inputs.emplace_back(projection_state, projection_input);
                        requires_reshape = requires_reshape || !projection_input->getResultType() || !column->getResultType()
                            || !column->getResultType()->equals(*projection_input->getResultType());
                    }
                    if (requires_reshape && !sparse->unanimous_union && !sparse->nullable_lift)
                        continue;
                    push_frame({
                        .action = SourceSliceAction::FinishSparseProjection,
                        .node = node,
                        .state = state,
                        .depth = frame.depth,
                        .inputs = std::move(projection_inputs),
                        .result_shape = requires_reshape ? std::optional<String>{column->getResultType()->getName()} : std::nullopt,
                        .sparse_projection_ordinal = sparse->ordinal,
                        .unanimous_union = sparse->unanimous_union,
                        .nullable_lift = sparse->nullable_lift,
                    });
                    for (auto input_it = sparse->inputs.rbegin(); input_it != sparse->inputs.rend(); ++input_it)
                    {
                        const auto input_state = SemanticNodePath{stable_node_id(input_it->get()), 0};
                        push_frame({
                            .node = *input_it,
                            .state = input_state,
                            .depth = frame.depth + 1,
                            .inputs = {},
                            .result_shape = std::nullopt,
                            .sparse_projection_ordinal = std::nullopt,
                        });
                    }
                    continue;
                }

                const auto * expression = node->as<FunctionNode>();
                if (!expression || !expression->isResolved())
                    continue;
                if (equalsCaseInsensitive(expression->getFunctionName(), "tupleElement"))
                {
                    auto source = find_static_column(node, 0);
                    if (!source)
                        continue;
                    auto candidate = inspectPreboundContextualConstant(*source->column, SemanticSinkKind::ExplicitUDTCast, source->path);
                    if (!candidate)
                        continue;
                    if (source_column_is_non_synthesizing && !source_column_is_non_synthesizing(*source->column))
                        continue;
                    const auto source_state = SemanticNodePath{stable_node_id(source->column.get()), allocate_path()};
                    semantic_role_graph->registerPreboundExactSource(
                        source_state, source->column, source->column, candidate->references, candidate->use_path);
                    const auto * use = candidate->references->findUse(candidate->use_path);
                    const bool same_shape
                        = use && use->getPhysicalType() && node->getResultType() && use->getPhysicalType()->equals(*node->getResultType());
                    semantic_role_graph->registerTransferBoundary(
                        state,
                        node,
                        same_shape ? SemanticTransferKind::StaticChildSelection : SemanticTransferKind::StaticReshape,
                        std::vector<std::pair<SemanticNodePath, QueryTreeNodePtr>>{{source_state, source->column}},
                        same_shape ? std::nullopt : std::optional<String>{node->getResultType()->getName()});
                    continue;
                }

                const auto & name = expression->getFunctionName();
                const auto & expression_arguments = expression->getArguments().getNodes();
                if (source_is_full_join_using_unanimous && source_is_full_join_using_unanimous(*expression))
                {
                    std::vector<std::pair<SemanticNodePath, QueryTreeNodePtr>> semantic_inputs;
                    ensure_vector_capacity(semantic_inputs, expression_arguments.size());
                    charge_source_discovery_work(0, static_cast<UInt64>(expression_arguments.size()), 0);
                    bool requires_reshape = false;
                    for (const auto & argument : expression_arguments)
                    {
                        if (!argument)
                            throw Exception(ErrorCodes::LOGICAL_ERROR, "Analyzer-generated FULL JOIN USING projection has an empty input");
                        const auto argument_state = SemanticNodePath{stable_node_id(argument.get()), 0};
                        semantic_inputs.emplace_back(argument_state, argument);
                        requires_reshape
                            = requires_reshape || !argument->getResultType() || !node->getResultType()->equals(*argument->getResultType());
                    }
                    push_frame({
                        .action = SourceSliceAction::FinishJoinUsing,
                        .node = node,
                        .state = state,
                        .depth = frame.depth,
                        .inputs = std::move(semantic_inputs),
                        .result_shape = requires_reshape ? std::optional<String>{node->getResultType()->getName()} : std::nullopt,
                        .sparse_projection_ordinal = std::nullopt,
                    });
                    for (auto input_it = expression_arguments.rbegin(); input_it != expression_arguments.rend(); ++input_it)
                    {
                        const auto input_state = SemanticNodePath{stable_node_id(input_it->get()), 0};
                        push_frame({
                            .node = *input_it,
                            .state = input_state,
                            .depth = frame.depth + 1,
                            .inputs = {},
                            .result_shape = std::nullopt,
                            .sparse_projection_ordinal = std::nullopt,
                        });
                    }
                    continue;
                }
                std::vector<size_t> value_indices;
                if (equalsCaseInsensitive(name, "if") && expression_arguments.size() == 3)
                {
                    ensure_vector_capacity(value_indices, 2);
                    value_indices.push_back(1);
                    value_indices.push_back(2);
                }
                else if (equalsCaseInsensitive(name, "multiIf") && expression_arguments.size() >= 3 && expression_arguments.size() % 2 == 1)
                {
                    ensure_vector_capacity(value_indices, (expression_arguments.size() + 1) / 2);
                    for (size_t index = 1; index + 1 < expression_arguments.size(); index += 2)
                        value_indices.push_back(index);
                    value_indices.push_back(expression_arguments.size() - 1);
                }
                else if (
                    equalsCaseInsensitive(name, "caseWithExpression") && expression_arguments.size() >= 4
                    && expression_arguments.size() % 2 == 0)
                {
                    ensure_vector_capacity(value_indices, expression_arguments.size() / 2);
                    for (size_t index = 2; index + 1 < expression_arguments.size(); index += 2)
                        value_indices.push_back(index);
                    value_indices.push_back(expression_arguments.size() - 1);
                }
                else
                    continue; /// Every unregistered function/operator is a barrier.

                std::vector<std::pair<SemanticNodePath, QueryTreeNodePtr>> semantic_inputs;
                ensure_vector_capacity(semantic_inputs, value_indices.size());
                charge_source_discovery_work(0, static_cast<UInt64>(value_indices.size()), 0);
                bool requires_reshape = false;
                for (const size_t index : value_indices)
                {
                    const auto & arm = expression_arguments[index];
                    const auto arm_state = SemanticNodePath{stable_node_id(arm.get()), 0};
                    semantic_inputs.emplace_back(arm_state, arm);
                    requires_reshape = requires_reshape || !arm->getResultType() || !node->getResultType()->equals(*arm->getResultType());
                }
                push_frame({
                    .action = SourceSliceAction::FinishBranch,
                    .node = node,
                    .state = state,
                    .depth = frame.depth,
                    .inputs = std::move(semantic_inputs),
                    .result_shape = requires_reshape ? std::optional<String>{node->getResultType()->getName()} : std::nullopt,
                    .sparse_projection_ordinal = std::nullopt,
                });
                for (auto input_it = value_indices.rbegin(); input_it != value_indices.rend(); ++input_it)
                {
                    const auto & arm = expression_arguments[*input_it];
                    const auto arm_state = SemanticNodePath{stable_node_id(arm.get()), 0};
                    push_frame({
                        .node = arm,
                        .state = arm_state,
                        .depth = frame.depth + 1,
                        .inputs = {},
                        .result_shape = std::nullopt,
                        .sparse_projection_ordinal = std::nullopt,
                    });
                }
            }
        };

        register_source_slice(source_node, input);
        return true;
    }
    catch (const QueryTreeSemanticRoleGraphError & error)
    {
        rethrowSemanticAnalysisError(error);
    }
}

std::optional<QueryAnalysisState::PreboundContextualConstantCandidate> QueryAnalysisState::inspectPreboundContextualConstant(
    const ColumnNode & expected_column, SemanticSinkKind kind, std::span<const UInt64> type_child_prefix)
{
    const auto * sink_descriptor = SemanticSinkRegistry::find(kind);
    if (!sink_descriptor || !sink_descriptor->requires_expected_role)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown UDT contextual-constant sink kind");
    if (expected_column.getColumn().isSubcolumn())
        return std::nullopt;

    const auto column_source = expected_column.getColumnSourceOrNull();
    const auto * table = column_source ? column_source->as<TableNode>() : nullptr;
    const auto * table_function = column_source ? column_source->as<TableFunctionNode>() : nullptr;
    const auto snapshot = table ? table->getStorageSnapshot()
        : table_function        ? table_function->getStorageSnapshot()
                                : StorageSnapshotPtr{};
    if (!snapshot || !snapshot->metadata)
        return std::nullopt;
    const auto & references = snapshot->metadata->getBoundUDTReferences();
    if (!references)
        return std::nullopt;

    /// Preserve the object-level constant-time fast negative before any
    /// wrapper-path materialization or exact-use lookup. Capability-free
    /// bound aliases and objects whose semantic roles are all unrelated to
    /// this sink must remain on the allocation-free physical path.
    if ((references->getSemanticCapabilities() & sink_descriptor->activation_capabilities) == 0)
        return std::nullopt;

    const auto section = [&]() -> std::optional<PersistedTypePathSection>
    {
        switch (references->getObject().kind)
        {
            case SchemaObjectKind::Table: return PersistedTypePathSection::ColumnType;
            case SchemaObjectKind::View: return PersistedTypePathSection::ViewExpression;
            default: return std::nullopt;
        }
    }();
    if (!section)
        return std::nullopt;

    constexpr UInt64 maximum_contextual_path_depth = 64;
    std::vector<ContextualEndpoint> endpoints;
    auto storage_type = expected_column.getColumn().getTypeInStorage();
    if (!storage_type)
        return std::nullopt;
    if (type_child_prefix.size() > maximum_contextual_path_depth)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UDT contextual static-child path exceeds its depth limit");
    std::vector<UInt64> base_path(type_child_prefix.begin(), type_child_prefix.end());
    for (const UInt64 ordinal : type_child_prefix)
    {
        storage_type = supportedPhysicalChild(storage_type, ordinal);
        if (!storage_type)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "UDT contextual static-child path disagrees with its storage type");
    }

    if (kind == SemanticSinkKind::HasConstant || kind == SemanticSinkKind::HasAnyConstant)
    {
        auto element_path = std::move(base_path);
        while (isWrapperType(storage_type))
        {
            if (element_path.size() >= maximum_contextual_path_depth)
                throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UDT contextual array path exceeds its depth limit");
            element_path.push_back(0);
            storage_type = unwrapWrapper(storage_type);
        }
        if (!storage_type || storage_type->getTypeId() != TypeIndex::Array)
            return std::nullopt;
        if (element_path.size() >= maximum_contextual_path_depth)
            throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UDT contextual array path exceeds its depth limit");
        element_path.push_back(0);
        storage_type = assert_cast<const DataTypeArray &>(*storage_type).getNestedType();
        appendWrapperEndpoints(endpoints, std::move(element_path), std::move(storage_type), maximum_contextual_path_depth);
    }
    else
        appendWrapperEndpoints(endpoints, std::move(base_path), std::move(storage_type), maximum_contextual_path_depth);

    std::optional<PreboundContextualConstantCandidate> selected;
    for (const auto & endpoint : endpoints)
    {
        const auto lookup = references->findUniqueRuntimeUse(*section, expected_column.getColumn().getNameInStorage(), endpoint.path);
        if (!lookup.use)
        {
            if (lookup.ambiguous && (lookup.semantic_capabilities & sink_descriptor->activation_capabilities) != 0)
            {
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "A contextual UDT constant cannot select one of multiple logical roles stacked at column {}",
                    backQuote(expected_column.getColumn().getNameInStorage()));
            }
            continue;
        }

        const auto descriptors = references->getDescriptors();
        if (lookup.use->getDescriptorIndex() >= descriptors.size() || !descriptors[lookup.use->getDescriptorIndex()])
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A contextual UDT endpoint lost its checked descriptor");
        const auto & persisted = descriptors[lookup.use->getDescriptorIndex()]->getPersistedDescriptor();
        const auto selected_capabilities = lookup.use->getSemanticCapabilities();
        const SemanticSink probe{
            .source = SemanticNodePath{0, 0},
            .kind = kind,
            .object_semantic_capabilities = references->getSemanticCapabilities(),
            .selected_semantic_capabilities = selected_capabilities,
            .observes_identity = false,
            .expected_role = SemanticExpectedRole{
                .role = LogicalRoleInput::fromBoundObjectUse(
                    *references, *lookup.use, persisted.getCanonicalPhysicalType()),
                .retained_descriptor = {},
            },
        };
        if (!SemanticSinkRegistry::isEligible(probe))
            continue;
        if (!lookup.use->getPhysicalType() || !endpoint.physical_type || !lookup.use->getPhysicalType()->equals(*endpoint.physical_type))
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "A contextual UDT constant disagrees with the bound physical subtree of column {}",
                backQuote(expected_column.getColumn().getNameInStorage()));
        }
        if (selected)
        {
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "A contextual UDT constant crosses multiple eligible logical wrapper roles at column {}",
                backQuote(expected_column.getColumn().getNameInStorage()));
        }
        selected = PreboundContextualConstantCandidate{
            .references = references,
            .use_path = lookup.use->getPath(),
            .effective_query_limits = std::nullopt,
        };
    }
    return selected;
}

bool QueryAnalysisState::registerPreboundContextualConstant(
    std::shared_ptr<ColumnNode> expected_column,
    std::shared_ptr<ConstantNode> constant,
    SemanticSinkKind kind,
    PreboundContextualConstantCandidate candidate)
{
    if (!expected_column || !constant || !candidate.references)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "UDT contextual-constant registration is incomplete");
    if (semantic_analysis_finalized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "UDT contextual constant reached analysis after the semantic graph was sealed");

    const auto * use = candidate.references->findUse(candidate.use_path);
    if (!use)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "UDT contextual-constant candidate lost its exact bound use");

    try
    {
        if (!candidate.effective_query_limits)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A contextual UDT constant has no exact owning-database resource limits");
        auto & budget = getOrCreateSemanticResourceBudget(*candidate.effective_query_limits);

        const auto stable_node_id = [&](const IQueryTreeNode * node)
        {
            if (semantic_role_graph)
            {
                if (const auto registered = semantic_role_graph->findRegisteredNodeID(node))
                    return *registered;
            }
            if (next_semantic_node_id == invalid_semantic_node_id)
                throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UDT semantic query-local node IDs are exhausted");
            const auto id = next_semantic_node_id++;
            if (semantic_role_graph)
                semantic_role_graph->registerStableNodeIdentity(id, node);
            return id;
        };

        if (next_semantic_path_id == invalid_logical_path_id
            || next_semantic_path_id == static_cast<LogicalPathID>(invalid_logical_path_id - 1))
            throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UDT semantic query-local logical path IDs are exhausted");
        const SemanticNodePath constant_path{stable_node_id(constant.get()), next_semantic_path_id++};
        const SemanticNodePath column_path{stable_node_id(expected_column.get()), next_semantic_path_id++};
        const auto sink = QueryTreeSemanticRoleGraph::makePreboundContextualConstantSink(constant_path, kind, *candidate.references, *use);
        if (!SemanticSinkRegistry::isEligible(sink))
            return false;

        if (!semantic_role_graph)
        {
            semantic_role_graph
                = QueryTreeSemanticRoleGraph::createIfEligible(1, sink, budget, makeQueryTreeSemanticRoleGraphLimits(budget.getLimits()));
            if (!semantic_role_graph)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Eligible contextual UDT constant did not activate its query graph");
        }

        if (!semantic_role_graph->registerContextualConstantBoundary(
                constant_path,
                constant,
                column_path,
                expected_column,
                std::move(candidate.references),
                std::move(candidate.use_path),
                sink))
            return true;

        const UInt64 literal_bytes = static_cast<UInt64>(constant->getColumn()->byteSize());
        if (literal_bytes > std::numeric_limits<UInt64>::max() - semantic_literal_bytes)
            throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UDT contextual literal bytes overflow UInt64");
        semantic_literal_bytes += literal_bytes;
        return true;
    }
    catch (const QueryTreeSemanticRoleGraphError & error)
    {
        rethrowSemanticAnalysisError(error);
    }
}

void QueryAnalysisState::finalizeSemanticGraphGeneration()
{
    if (!semantic_role_graph)
    {
        if (semantic_literal_bytes)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "UDT semantic literal accounting exists without a query graph");
        return;
    }
    if (!semantic_resource_budget)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "UDT semantic graph has no query resource budget");

    try
    {
        semantic_role_graph->seal();
        semantic_role_planner = SemanticRolePlanner::create(
            *semantic_role_graph, *semantic_resource_budget, makeSemanticRolePlannerLimits(semantic_resource_budget->getLimits()));
        if (semantic_literal_bytes)
            semantic_role_planner->chargeLiteralBytes(semantic_literal_bytes);
        semantic_role_planner->seal();

        const auto boundaries = semantic_role_planner->getPlannedBoundaries();
        for (const auto & boundary : boundaries)
        {
            if (boundary.kind == PlannedBoundaryKind::Conflict)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "UDT semantic boundary source has conflicting logical roles");
            if (boundary.kind != PlannedBoundaryKind::PreserveSourceRole && boundary.kind != PlannedBoundaryKind::ApplyExpectedRole)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "UDT semantic sink produced an invalid boundary instruction");
        }

        const auto & dependency = semantic_role_planner->getCacheDependencyDigest();
        if (dependency.getKind() != SemanticCacheDependencyKind::Semantic || dependency.getRoleCount() == 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Eligible UDT semantic analysis produced no complete cache dependency");

        /// The public CAST already is the ordinary physical action selected by
        /// both supported boundary instructions. No logical role, descriptor,
        /// graph ID, or cache digest is allowed to outlive query analysis and
        /// leak into executable planning or serialization.
        semantic_role_planner.reset();
        semantic_role_graph.reset();
        semantic_literal_bytes = 0;
    }
    catch (const QueryTreeSemanticRoleGraphError & error)
    {
        rethrowSemanticAnalysisError(error);
    }
    catch (const SemanticRolePlannerError & error)
    {
        rethrowSemanticAnalysisError(error);
    }
    catch (const SemanticCacheDependencyError & error)
    {
        rethrowSemanticAnalysisError(error);
    }
}

void QueryAnalysisState::finalizeSemanticAnalysis()
{
    if (semantic_analysis_finalized)
        return;
    semantic_analysis_finalized = true;

    /// Explicit CAST targets are needed only by the DDL selected-output
    /// publisher, which runs before this stable-generation barrier.
    resolved_explicit_cast_targets.clear();
    finalizeSemanticGraphGeneration();
}
}
