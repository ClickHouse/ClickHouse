#include <Analyzer/UDT/QueryTreeSemanticRoleGraph.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/UnionNode.h>

#include <Common/StringUtils.h>
#include <Common/typeid_cast.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/UDT/ResourceAccounting.h>
#include <DataTypes/hasNullable.h>

#include <Storages/StorageSnapshot.h>

#include <algorithm>
#include <array>
#include <limits>
#include <unordered_map>
#include <utility>
#include <vector>

namespace DB::UDT
{
namespace
{

using Error = QueryTreeSemanticRoleGraphError;

[[noreturn]] void fail(Error::Code code, std::string_view message)
{
    throw Error(code, message);
}

constexpr QueryTreeSemanticRoleGraphLimits implementation_maxima{
    .maximum_query_nodes = 1ULL << 20,
    .maximum_explicit_cast_boundaries = 1ULL << 20,
    .maximum_semantic_sinks = 1ULL << 20,
    .maximum_single_sink_shape_bytes = 1ULL << 20,
};

constexpr UInt64 hash_node_overhead = 3 * sizeof(void *);

enum class GraphLifecycle : UInt8
{
    Open,
    Sealed,
};

void requireAdmission(ProspectiveResourceBudget & budget, ResourceDelta delta, UInt64 scratch_bytes)
{
    delta.add(ResourceLimit::SemanticScratchBytesPerQuery, scratch_bytes);
    const auto admission = budget.charge(delta);
    if (!admission.isAccepted())
        fail(Error::Code::LimitExceeded, formatResourceAdmissionFailure(admission));
}

StorageSnapshotPtr getClosedExactSourceSnapshot(const QueryTreeNodePtr & source)
{
    if (!source)
        return {};
    if (const auto * table = source->as<TableNode>())
        return table->getStorageSnapshot();
    if (const auto * table_function = source->as<TableFunctionNode>(); table_function && table_function->isResolved())
        return table_function->getStorageSnapshot();
    return {};
}

void validateLimits(const QueryTreeSemanticRoleGraphLimits & limits)
{
    if (!limits.maximum_query_nodes || !limits.maximum_explicit_cast_boundaries || !limits.maximum_semantic_sinks
        || !limits.maximum_single_sink_shape_bytes)
        fail(Error::Code::InvalidConfiguration, "every QueryTree semantic-role graph limit must be nonzero");
    if (limits.maximum_query_nodes > implementation_maxima.maximum_query_nodes
        || limits.maximum_explicit_cast_boundaries > implementation_maxima.maximum_explicit_cast_boundaries
        || limits.maximum_semantic_sinks > implementation_maxima.maximum_semantic_sinks
        || limits.maximum_single_sink_shape_bytes > implementation_maxima.maximum_single_sink_shape_bytes)
        fail(Error::Code::InvalidConfiguration, "a QueryTree semantic-role graph limit exceeds its implementation maximum");
}

struct SemanticNodePathHash
{
    std::size_t operator()(const SemanticNodePath & state) const noexcept
    {
        UInt64 value = state.node ^ (static_cast<UInt64>(state.path) << 32);
        value ^= value >> 30;
        value *= 0xbf58476d1ce4e5b9ULL;
        value ^= value >> 27;
        value *= 0x94d049bb133111ebULL;
        return static_cast<std::size_t>(value ^ (value >> 31));
    }
};

struct DirectTargetSelection
{
    UInt32 descriptor_index = std::numeric_limits<UInt32>::max();
    InstantiatedTypeDescriptor::Ptr descriptor;
};

DirectTargetSelection selectDirectTarget(const BoundDeclaredTypeTree & target)
{
    if (target.getOccurrenceCount() != 1)
        fail(Error::Code::UnsupportedCastShape, "direct QueryTree UDT CAST support requires exactly one logical occurrence in the target");

    const auto root_descriptors = target.getDescriptorIndices(0);
    if (root_descriptors.size() != 1)
        fail(
            Error::Code::UnsupportedCastShape,
            "direct QueryTree UDT CAST support requires exactly one logical occurrence at the target root");

    const auto descriptor_index = root_descriptors.front();
    const auto & descriptors = target.getDescriptors();
    if (descriptor_index >= descriptors.size() || !descriptors[descriptor_index])
        fail(Error::Code::InvalidRegistration, "direct QueryTree UDT CAST target references an invalid descriptor");

    const auto & descriptor = descriptors[descriptor_index];
    const auto & physical_type = target.getPhysicalType();
    if (!physical_type || !descriptor->getPhysicalType() || !physical_type->equals(*descriptor->getPhysicalType())
        || physical_type->getName() != descriptor->getPersistedDescriptor().getCanonicalPhysicalType())
        fail(Error::Code::InvalidRegistration, "direct QueryTree UDT CAST target descriptor disagrees with its root physical type");

    return {descriptor_index, descriptor};
}

struct DirectExplicitCastBoundary
{
    FunctionNodePtr function;
    BoundDeclaredTypeTree::Ptr target;
    UInt32 descriptor_index = std::numeric_limits<UInt32>::max();
    SemanticNodePath input;
    const IQueryTreeNode * type_argument = nullptr;
    String type_argument_value;
};

struct PreboundExactSource
{
    QueryTreeNodePtr state_node;
    ColumnNodePtr storage_column;
    BoundObjectTypeReferences::Ptr references;
    PersistedTypeOccurrencePath use_path;
};

struct NullOnlySource
{
    QueryTreeNodePtr node;
};

struct RegisteredTransferBoundary
{
    QueryTreeNodePtr node;
    SemanticTransferKind transfer = SemanticTransferKind::Unregistered;
    std::vector<SemanticNodePath> inputs;
    std::vector<const IQueryTreeNode *> input_nodes;
    String result_type_name;
    std::optional<String> result_shape;
    std::vector<UInt64> static_child_path;
    std::optional<UInt32> sparse_projection_ordinal;
};

struct ContextualConstantBoundary
{
    ConstantNodePtr constant;
    SemanticNodePath expected_column;
    SemanticSinkID sink = invalid_semantic_sink_id;
};

bool isContextualConstantKind(SemanticSinkKind kind) noexcept
{
    switch (kind)
    {
        case SemanticSinkKind::EqualityConstant:
        case SemanticSinkKind::InConstant:
        case SemanticSinkKind::GlobalInConstant:
        case SemanticSinkKind::HasConstant:
        case SemanticSinkKind::HasAnyConstant: return true;
        default: return false;
    }
}

std::optional<PersistedTypePathSection> objectRootSection(SchemaObjectKind kind) noexcept
{
    switch (kind)
    {
        case SchemaObjectKind::Table: return PersistedTypePathSection::ColumnType;
        case SchemaObjectKind::View: return PersistedTypePathSection::ViewExpression;
        default: return std::nullopt;
    }
}

DataTypePtr physicalTypeAtSupportedSemanticPath(DataTypePtr type, std::span<const UInt64> path)
{
    for (const UInt64 ordinal : path)
    {
        if (!type)
            return {};
        switch (type->getTypeId())
        {
            case TypeIndex::Array:
                if (ordinal != 0)
                    return {};
                type = assert_cast<const DataTypeArray &>(*type).getNestedType();
                break;
            case TypeIndex::Nullable:
                if (ordinal != 0)
                    return {};
                type = assert_cast<const DataTypeNullable &>(*type).getNestedType();
                break;
            case TypeIndex::LowCardinality:
                if (ordinal != 0)
                    return {};
                type = assert_cast<const DataTypeLowCardinality &>(*type).getDictionaryType();
                break;
            case TypeIndex::Tuple: {
                const auto & elements = assert_cast<const DataTypeTuple &>(*type).getElements();
                if (ordinal >= elements.size())
                    return {};
                type = elements[static_cast<size_t>(ordinal)];
                break;
            }
            default: return {};
        }
    }
    return type;
}

std::optional<size_t> resolveTupleSelector(const ConstantNode & selector, const DataTypeTuple & tuple)
{
    const Field value = selector.getValue();
    if (value.getType() == Field::Types::UInt64)
    {
        const UInt64 one_based = value.safeGet<UInt64>();
        if (one_based && one_based <= tuple.getElements().size())
            return static_cast<size_t>(one_based - 1);
    }
    else if (value.getType() == Field::Types::Int64)
    {
        const Int64 one_based = value.safeGet<Int64>();
        if (one_based > 0 && static_cast<UInt64>(one_based) <= tuple.getElements().size())
            return static_cast<size_t>(one_based - 1);
    }
    else if (value.getType() == Field::Types::String)
        return tuple.tryGetPositionByName(value.safeGet<String>());
    return std::nullopt;
}

std::optional<std::vector<UInt64>>
captureStaticTupleElementPath(const QueryTreeNodePtr & result_node, const IQueryTreeNode * expected_column)
{
    std::vector<const ConstantNode *> selectors;
    QueryTreeNodePtr current = result_node;
    while (const auto * function = current ? current->as<FunctionNode>() : nullptr)
    {
        if (!function->isResolved() || !equalsCaseInsensitive(function->getFunctionName(), "tupleElement"))
            return std::nullopt;
        const auto & arguments = function->getArguments().getNodes();
        if (arguments.size() != 2)
            return std::nullopt;
        const auto * selector = arguments[1] ? arguments[1]->as<ConstantNode>() : nullptr;
        if (!selector)
            return std::nullopt;
        selectors.push_back(selector);
        current = arguments[0];
    }
    const auto * column = current ? current->as<ColumnNode>() : nullptr;
    if (!column || column != expected_column || selectors.empty())
        return std::nullopt;

    std::vector<UInt64> path;
    auto type = column->getColumn().getTypeInStorage();
    for (auto selector = selectors.rbegin(); selector != selectors.rend(); ++selector)
    {
        while (type && (type->getTypeId() == TypeIndex::Nullable || type->getTypeId() == TypeIndex::LowCardinality))
        {
            path.push_back(0);
            type = type->getTypeId() == TypeIndex::Nullable ? assert_cast<const DataTypeNullable &>(*type).getNestedType()
                                                            : assert_cast<const DataTypeLowCardinality &>(*type).getDictionaryType();
        }
        const auto * tuple = type ? typeid_cast<const DataTypeTuple *>(type.get()) : nullptr;
        const auto selected = tuple ? resolveTupleSelector(**selector, *tuple) : std::nullopt;
        if (!selected)
            return std::nullopt;
        path.push_back(static_cast<UInt64>(*selected));
        type = tuple->getElements()[*selected];
    }
    return path;
}

std::optional<std::vector<const IQueryTreeNode *>> captureBranchValueInputs(const QueryTreeNodePtr & result_node)
{
    const auto * function = result_node ? result_node->as<FunctionNode>() : nullptr;
    if (!function || !function->isResolved())
        return std::nullopt;
    const auto & name = function->getFunctionName();
    const auto & arguments = function->getArguments().getNodes();
    std::vector<const IQueryTreeNode *> result;
    if (equalsCaseInsensitive(name, "if") && arguments.size() == 3)
        result = {arguments[1].get(), arguments[2].get()};
    else if (equalsCaseInsensitive(name, "multiIf") && arguments.size() >= 3 && arguments.size() % 2 == 1)
    {
        result.reserve((arguments.size() + 1) / 2);
        for (size_t index = 1; index + 1 < arguments.size(); index += 2)
            result.push_back(arguments[index].get());
        result.push_back(arguments.back().get());
    }
    else if (equalsCaseInsensitive(name, "caseWithExpression") && arguments.size() >= 4 && arguments.size() % 2 == 0)
    {
        result.reserve(arguments.size() / 2);
        for (size_t index = 2; index + 1 < arguments.size(); index += 2)
            result.push_back(arguments[index].get());
        result.push_back(arguments.back().get());
    }
    else
        return std::nullopt;
    if (std::ranges::any_of(result, [](const auto * input) { return input == nullptr; }))
        return std::nullopt;
    return result;
}

struct SparseProjectionInputs
{
    SemanticTransferKind transfer = SemanticTransferKind::Unregistered;
    std::vector<const IQueryTreeNode *> inputs;
};

std::optional<SparseProjectionInputs> captureSparseProjectionInputs(const QueryTreeNodePtr & result_node, UInt32 ordinal)
{
    const auto * column = result_node ? result_node->as<ColumnNode>() : nullptr;
    const auto source = column ? column->getColumnSourceOrNull() : TableExpressionNodePtr{};
    if (!source)
        return std::nullopt;
    if (const auto * query = source->as<QueryNode>())
    {
        const auto & projection = query->getProjection().getNodes();
        if (ordinal >= projection.size() || !projection[ordinal])
            return std::nullopt;
        auto transfer = SemanticTransferKind::Rename;
        if (!column->getResultType() || !projection[ordinal]->getResultType())
            return std::nullopt;
        if (!column->getResultType()->equals(*projection[ordinal]->getResultType()))
        {
            if (!isNullableOrLowCardinalityNullable(column->getResultType())
                || !removeNullableOrLowCardinalityNullable(column->getResultType())->equals(*projection[ordinal]->getResultType()))
                return std::nullopt;
            transfer = SemanticTransferKind::JoinDirectNullableLift;
        }
        return SparseProjectionInputs{.transfer = transfer, .inputs = {projection[ordinal].get()}};
    }
    const auto * root_union = source->as<UnionNode>();
    if (!root_union)
        return std::nullopt;

    SparseProjectionInputs result{.transfer = SemanticTransferKind::UnanimousUnion, .inputs = {}};
    std::vector<const IQueryTreeNode *> pending{root_union};
    while (!pending.empty())
    {
        const auto * current = pending.back();
        pending.pop_back();
        if (const auto * query = current->as<QueryNode>())
        {
            const auto & projection = query->getProjection().getNodes();
            if (ordinal >= projection.size() || !projection[ordinal])
                return std::nullopt;
            result.inputs.push_back(projection[ordinal].get());
            continue;
        }
        const auto * union_node = current->as<UnionNode>();
        if (!union_node || union_node->isRecursiveCTE() || union_node->hasRecursiveCTETable()
            || union_node->getUnionMode() > SelectUnionMode::UNION_DISTINCT)
            return std::nullopt;
        const auto & queries = union_node->getQueries().getNodes();
        for (auto query = queries.rbegin(); query != queries.rend(); ++query)
        {
            if (!*query)
                return std::nullopt;
            pending.push_back(query->get());
        }
    }
    if (result.inputs.empty())
        return std::nullopt;
    return result;
}

bool hasClosedTransferTopology(
    SemanticTransferKind transfer,
    const QueryTreeNodePtr & result_node,
    std::span<const IQueryTreeNode * const> input_nodes,
    std::span<const UInt64> registered_static_child_path,
    std::optional<UInt32> sparse_projection_ordinal)
{
    if (sparse_projection_ordinal)
    {
        const auto captured = captureSparseProjectionInputs(result_node, *sparse_projection_ordinal);
        return captured && captured->transfer == transfer && std::ranges::equal(captured->inputs, input_nodes);
    }
    switch (transfer)
    {
        case SemanticTransferKind::Identity:
        case SemanticTransferKind::NullableLift:
        case SemanticTransferKind::LowCardinalityReshape:
        case SemanticTransferKind::JoinDirectNonSynthesizing:
        case SemanticTransferKind::JoinDirectNullableLift: return input_nodes.size() == 1 && result_node.get() == input_nodes.front();
        case SemanticTransferKind::Rename: {
            const auto * column = result_node ? result_node->as<ColumnNode>() : nullptr;
            return input_nodes.size() == 1 && column && column->hasExpression() && column->getExpression().get() == input_nodes.front();
        }
        case SemanticTransferKind::StaticChildSelection:
        case SemanticTransferKind::StaticReshape: {
            if (input_nodes.size() != 1)
                return false;
            const auto path = captureStaticTupleElementPath(result_node, input_nodes.front());
            return path && std::ranges::equal(*path, registered_static_child_path);
        }
        case SemanticTransferKind::UnanimousBranch: {
            const auto current_inputs = captureBranchValueInputs(result_node);
            return current_inputs && std::ranges::equal(*current_inputs, input_nodes);
        }
        case SemanticTransferKind::JoinUsingUnanimous: {
            const auto * function = result_node ? result_node->as<FunctionNode>() : nullptr;
            if (!function || !function->isResolved() || function->hasOriginalAST() || !function->hasAlias()
                || !equalsCaseInsensitive(function->getFunctionName(), "firstNonDefault"))
                return false;
            const auto & arguments = function->getArguments().getNodes();
            return arguments.size() >= 2 && arguments.size() == input_nodes.size()
                && std::ranges::all_of(
                       arguments,
                       [](const auto & argument)
                       {
                           return argument && argument->template as<ColumnNode>()
                               && isNullableOrLowCardinalityNullable(argument->getResultType());
                       })
                && std::equal(
                       arguments.begin(),
                       arguments.end(),
                       input_nodes.begin(),
                       [](const auto & argument, const auto * input) { return argument.get() == input; });
        }
        /// UNION and coalesced JOIN keys need their own analyzer provenance
        /// adapters. They are deliberately not admitted through this generic
        /// QueryTree function boundary.
        case SemanticTransferKind::UnanimousUnion:
        case SemanticTransferKind::ExactInstantiationCast:
        case SemanticTransferKind::Unregistered:
        case SemanticTransferKind::Count: return false;
    }
    return false;
}

bool hasClosedTransferPhysicalShape(
    const SemanticTransferDescriptor & descriptor,
    const QueryTreeNodePtr & result_node,
    std::span<const IQueryTreeNode * const> input_nodes,
    const std::optional<String> & result_shape)
{
    const auto & result_type = result_node->getResultType();
    if (!result_type)
        return false;
    if (result_shape && *result_shape != result_type->getName())
        return false;
    if (descriptor.kind == SemanticTransferKind::Rename)
    {
        return input_nodes.size() == 1 && input_nodes.front() && input_nodes.front()->getResultType()
            && result_type->equals(*input_nodes.front()->getResultType());
    }
    if (descriptor.policy != SemanticTransferPolicy::MeetUnanimous || result_shape)
        return true;
    return std::ranges::all_of(
        input_nodes, [&](const auto * input) { return input && input->getResultType() && result_type->equals(*input->getResultType()); });
}

struct RegisteredSemanticSink
{
    SemanticSink sink;
    String expected_shape;

    SemanticSink view() const
    {
        auto result = sink;
        if (result.expected_role)
        {
            result.expected_role->role.shape.canonical_encoding = expected_shape;
            if (result.expected_role->retained_descriptor)
            {
                result.expected_role->role.descriptor = std::addressof(result.expected_role->retained_descriptor->getPersistedDescriptor());
            }
        }
        return result;
    }
};
}

struct QueryTreeSemanticRoleGraph::Impl
{
    Impl(
        UInt64 generation_,
        ProspectiveResourceBudget & query_resource_budget_,
        QueryTreeSemanticRoleGraphLimits limits_,
        UInt64 admitted_base_scratch_bytes)
        : generation(generation_)
        , query_resource_budget(query_resource_budget_)
        , limits(limits_)
    {
        statistics.semantic_scratch_bytes = admitted_base_scratch_bytes;
    }

    void charge(ResourceDelta delta, UInt64 scratch_bytes)
    {
        requireAdmission(query_resource_budget, std::move(delta), scratch_bytes);
        statistics.semantic_scratch_bytes += scratch_bytes;
    }

    const DirectExplicitCastBoundary * findValidatedBoundary(const SemanticNodePath & state) const
    {
        const auto boundary = explicit_cast_boundaries.find(state);
        if (boundary == explicit_cast_boundaries.end())
            return nullptr;

        const auto & value = boundary->second;
        if (!value.function || !value.target || !value.function->isResolved()
            || !equalsCaseInsensitive(value.function->getFunctionName(), "CAST") || value.function->getArguments().getNodes().size() != 2)
            fail(Error::Code::InvalidRegistration, "sealed QueryTree semantic-role CAST boundary changed after registration");

        const auto result_node = nodes_by_id.find(state.node);
        const auto input_node = nodes_by_id.find(value.input.node);
        const auto * current_input = value.function->getArguments().getNodes().front().get();
        const auto * current_type_argument = value.function->getArguments().getNodes().back().get();
        const auto * current_type_constant = current_type_argument ? current_type_argument->as<ConstantNode>() : nullptr;
        if (result_node == nodes_by_id.end() || result_node->second != value.function.get() || input_node == nodes_by_id.end()
            || input_node->second != current_input || !value.type_argument || value.type_argument != current_type_argument
            || !current_type_constant || current_type_constant->getDataAt() != std::string_view(value.type_argument_value))
            fail(Error::Code::InvalidRegistration, "sealed QueryTree semantic-role CAST arguments changed after registration");

        const auto selected = selectDirectTarget(*value.target);
        const auto result_type = value.function->getResultType();
        if (selected.descriptor_index != value.descriptor_index || !result_type || !result_type->equals(*value.target->getPhysicalType())
            || result_type->getName() != selected.descriptor->getPersistedDescriptor().getCanonicalPhysicalType())
            fail(Error::Code::InvalidRegistration, "sealed QueryTree semantic-role CAST target changed after registration");
        return std::addressof(value);
    }

    const BoundObjectTypeReferenceUse * findValidatedPreboundSource(const SemanticNodePath & state) const
    {
        const auto source = prebound_sources.find(state);
        if (source == prebound_sources.end())
            return nullptr;

        const auto & value = source->second;
        if (!value.state_node || !value.storage_column || !value.references || value.storage_column->getColumn().isSubcolumn())
            fail(Error::Code::InvalidRegistration, "sealed prebound semantic source changed its schema-column shape");
        const auto node = nodes_by_id.find(state.node);
        if (node == nodes_by_id.end() || node->second != value.state_node.get() || value.storage_column->getColumnName().empty())
            fail(Error::Code::InvalidRegistration, "sealed prebound semantic source changed its stable node identity");

        const auto * use = value.references->findUse(value.use_path);
        const auto expected_section = objectRootSection(value.references->getObject().kind);
        if (!use || !expected_section || use->getPath().section != *expected_section
            || use->getPath().site != PersistedTypeOccurrenceSite::Declaration || use->getPath().occurrence_ordinal != 0
            || use->getRuntimeOwnerKey() != value.storage_column->getColumn().getNameInStorage() || !use->getPhysicalType()
            || !physicalTypeAtSupportedSemanticPath(
                value.storage_column->getColumn().getTypeInStorage(), use->getPath().type_child_ordinals)
            || !use->getPhysicalType()->equals(*physicalTypeAtSupportedSemanticPath(
                value.storage_column->getColumn().getTypeInStorage(), use->getPath().type_child_ordinals)))
            fail(Error::Code::InvalidRegistration, "sealed prebound semantic source no longer matches its exact bound use");

        const auto snapshot = getClosedExactSourceSnapshot(value.storage_column->getColumnSourceOrNull());
        if (!snapshot || !snapshot->metadata || snapshot->metadata->getBoundUDTReferences().get() != value.references.get())
            fail(Error::Code::InvalidRegistration, "sealed prebound semantic source lost its owning metadata snapshot");
        return use;
    }

    const RegisteredTransferBoundary * findValidatedTransfer(const SemanticNodePath & state) const
    {
        const auto found = transfer_boundaries.find(state);
        if (found == transfer_boundaries.end())
            return nullptr;
        const auto & value = found->second;
        const auto * descriptor = SemanticTransferRegistry::find(value.transfer);
        const auto node = nodes_by_id.find(state.node);
        if (!value.node || !descriptor || node == nodes_by_id.end() || node->second != value.node.get()
            || value.inputs.size() != value.input_nodes.size() || value.inputs.size() < descriptor->minimum_inputs
            || value.inputs.size() > descriptor->maximum_inputs)
            fail(Error::Code::InvalidRegistration, "sealed QueryTree semantic transfer changed its registered shape");
        const auto result_type = value.node->getResultType();
        if (!result_type || result_type->getName() != value.result_type_name || (descriptor->requires_result_shape && !value.result_shape)
            || (value.result_shape && (!descriptor->allows_result_shape || value.result_shape->empty()))
            || !hasClosedTransferPhysicalShape(*descriptor, value.node, value.input_nodes, value.result_shape))
            fail(Error::Code::InvalidRegistration, "sealed QueryTree semantic transfer changed its result type or shape");
        for (size_t index = 0; index < value.inputs.size(); ++index)
        {
            const auto input = nodes_by_id.find(value.inputs[index].node);
            if (!value.inputs[index].isValid() || !value.input_nodes[index] || input == nodes_by_id.end()
                || input->second != value.input_nodes[index])
                fail(Error::Code::InvalidRegistration, "sealed QueryTree semantic transfer changed an exact input edge");
        }
        if (!hasClosedTransferTopology(
                value.transfer, value.node, value.input_nodes, value.static_child_path, value.sparse_projection_ordinal))
            fail(Error::Code::InvalidRegistration, "sealed QueryTree semantic transfer changed its closed adapter topology");
        return std::addressof(value);
    }

    void validateRegistrations() const
    {
        for (const auto & boundary : explicit_cast_boundaries)
            static_cast<void>(findValidatedBoundary(boundary.first));
        for (const auto & source : prebound_sources)
            static_cast<void>(findValidatedPreboundSource(source.first));
        for (const auto & [state, source] : null_only_sources)
        {
            const auto node = nodes_by_id.find(state.node);
            const auto * constant = source.node ? source.node->as<ConstantNode>() : nullptr;
            if (!state.isValid() || !source.node || node == nodes_by_id.end() || node->second != source.node.get() || !constant
                || !constant->isNull())
                fail(Error::Code::InvalidRegistration, "sealed QueryTree NULL-only source changed its exact node");
        }
        for (const auto & transfer : transfer_boundaries)
            static_cast<void>(findValidatedTransfer(transfer.first));

        for (const auto & [sink_id, boundary] : contextual_constant_boundaries)
        {
            if (sink_id >= sinks.size() || sink_id != boundary.sink || !boundary.constant)
                fail(Error::Code::InvalidRegistration, "sealed contextual UDT constant has an invalid sink binding");
            const auto constant_node = nodes_by_id.find(sinks[sink_id].sink.source.node);
            if (constant_node == nodes_by_id.end() || constant_node->second != boundary.constant.get())
                fail(Error::Code::InvalidRegistration, "sealed contextual UDT constant changed its stable node identity");
            const auto * use = findValidatedPreboundSource(boundary.expected_column);
            const auto sink = sinks[sink_id].view();
            if (!use || !isContextualConstantKind(sink.kind) || !sink.expected_role)
                fail(Error::Code::InvalidRegistration, "sealed contextual UDT constant lost its expected prebound role");
            const auto source = prebound_sources.find(boundary.expected_column);
            const auto descriptors = source->second.references->getDescriptors();
            if (use->getDescriptorIndex() >= descriptors.size() || !descriptors[use->getDescriptorIndex()]
                || !descriptors[use->getDescriptorIndex()]->getPersistedDescriptor().hasSameInstantiation(
                    *sink.expected_role->role.descriptor))
                fail(Error::Code::InvalidRegistration, "sealed contextual UDT constant disagrees with its expected prebound role");
        }

        for (const auto & registered_sink : sinks)
        {
            const auto sink = registered_sink.view();
            if (!SemanticSinkRegistry::isEligible(sink))
                fail(Error::Code::InvalidRegistration, "sealed QueryTree semantic-role sink changed after registration");
            if (sink.kind == SemanticSinkKind::ExplicitUDTCast)
            {
                const auto * boundary = findValidatedBoundary(sink.source);
                if (!boundary || !sink.expected_role)
                    fail(Error::Code::InvalidRegistration, "sealed explicit UDT CAST sink has no matching QueryTree boundary");

                const auto & descriptors = boundary->target->getDescriptors();
                if (boundary->descriptor_index >= descriptors.size() || !descriptors[boundary->descriptor_index]
                    || !descriptors[boundary->descriptor_index]->getPersistedDescriptor().hasSameInstantiation(
                        *sink.expected_role->role.descriptor)
                    || sink.expected_role->role.shape.canonical_encoding
                        != std::string_view(descriptors[boundary->descriptor_index]->getPersistedDescriptor().getCanonicalPhysicalType()))
                {
                    fail(Error::Code::InvalidRegistration, "sealed explicit UDT CAST sink disagrees with its QueryTree boundary");
                }
            }
        }
    }

    const UInt64 generation;
    ProspectiveResourceBudget & query_resource_budget;
    const QueryTreeSemanticRoleGraphLimits limits;
    QueryTreeSemanticRoleGraphStatistics statistics;
    GraphLifecycle lifecycle = GraphLifecycle::Open;

    std::unordered_map<SemanticNodePath, DirectExplicitCastBoundary, SemanticNodePathHash> explicit_cast_boundaries;
    std::unordered_map<SemanticNodePath, PreboundExactSource, SemanticNodePathHash> prebound_sources;
    std::unordered_map<SemanticNodePath, NullOnlySource, SemanticNodePathHash> null_only_sources;
    std::unordered_map<SemanticNodePath, RegisteredTransferBoundary, SemanticNodePathHash> transfer_boundaries;
    std::unordered_map<SemanticSinkID, ContextualConstantBoundary> contextual_constant_boundaries;
    std::unordered_map<SemanticNodeID, const IQueryTreeNode *> nodes_by_id;
    std::unordered_map<const IQueryTreeNode *, SemanticNodeID> ids_by_node;
    std::vector<RegisteredSemanticSink> sinks;
};

QueryTreeSemanticRoleGraphError::QueryTreeSemanticRoleGraphError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

QueryTreeSemanticRoleGraph::QueryTreeSemanticRoleGraph(
    UInt64 generation,
    ProspectiveResourceBudget & query_resource_budget,
    const QueryTreeSemanticRoleGraphLimits & limits,
    UInt64 admitted_base_scratch_bytes)
    : impl(std::make_unique<Impl>(generation, query_resource_budget, limits, admitted_base_scratch_bytes))
{
}

QueryTreeSemanticRoleGraph::~QueryTreeSemanticRoleGraph() = default;

QueryTreeSemanticRoleGraph::Ptr QueryTreeSemanticRoleGraph::create(
    UInt64 generation, ProspectiveResourceBudget & query_resource_budget, const QueryTreeSemanticRoleGraphLimits & limits)
{
    if (generation == 0)
        fail(Error::Code::InvalidRegistration, "QueryTree semantic-role graph generation must be nonzero");
    validateLimits(limits);
    constexpr UInt64 base_scratch_bytes = sizeof(QueryTreeSemanticRoleGraph) + sizeof(Impl);
    requireAdmission(query_resource_budget, {}, base_scratch_bytes);
    return Ptr(new QueryTreeSemanticRoleGraph(generation, query_resource_budget, limits, base_scratch_bytes));
}

QueryTreeSemanticRoleGraph::Ptr QueryTreeSemanticRoleGraph::createIfEligible(
    UInt64 generation,
    const SemanticSink & first_candidate,
    ProspectiveResourceBudget & query_resource_budget,
    const QueryTreeSemanticRoleGraphLimits & limits)
{
    if (!SemanticSinkRegistry::isEligible(first_candidate))
        return {};
    auto graph = create(generation, query_resource_budget, limits);
    const auto first_sink = graph->registerSink(first_candidate);
    if (!first_sink)
        fail(Error::Code::InvalidRegistration, "eligible QueryTree semantic-role activation candidate was not registered");
    return graph;
}

DirectExplicitCastTarget QueryTreeSemanticRoleGraph::inspectDirectExplicitCastTarget(const BoundDeclaredTypeTree & target)
{
    auto selected = selectDirectTarget(target);
    const auto capabilities = selected.descriptor->getPersistedDescriptor().getSemanticCapabilities();
    auto role
        = LogicalRoleInput::fromDescriptor(*selected.descriptor, selected.descriptor->getPersistedDescriptor().getCanonicalPhysicalType());
    return {
        .role = role,
        .retained_descriptor = std::move(selected.descriptor),
        .selected_semantic_capabilities = capabilities,
    };
}

SemanticSink QueryTreeSemanticRoleGraph::makeDirectExplicitCastSink(const SemanticNodePath & result, const BoundDeclaredTypeTree & target)
{
    if (!result.isValid())
        fail(Error::Code::InvalidRegistration, "direct QueryTree UDT CAST sink has no stable node/path identity");

    auto direct_target = inspectDirectExplicitCastTarget(target);
    return {
        .source = result,
        .kind = SemanticSinkKind::ExplicitUDTCast,
        .object_semantic_capabilities = target.getSemanticCapabilities(),
        .selected_semantic_capabilities = direct_target.selected_semantic_capabilities,
        .observes_identity = false,
        .expected_role = SemanticExpectedRole{
            .role = direct_target.role,
            .retained_descriptor = std::move(direct_target.retained_descriptor),
        },
    };
}

SemanticSink QueryTreeSemanticRoleGraph::makePreboundContextualConstantSink(
    const SemanticNodePath & constant,
    SemanticSinkKind kind,
    const BoundObjectTypeReferences & references,
    const BoundObjectTypeReferenceUse & use)
{
    if (!constant.isValid() || !isContextualConstantKind(kind))
        fail(Error::Code::InvalidRegistration, "prebound contextual UDT constant has an invalid sink identity or kind");
    const auto descriptors = references.getDescriptors();
    if (use.getDescriptorIndex() >= descriptors.size() || !descriptors[use.getDescriptorIndex()])
        fail(Error::Code::InvalidRegistration, "prebound contextual UDT constant references an invalid descriptor");
    const auto & descriptor = *descriptors[use.getDescriptorIndex()];
    const auto selected_capabilities = use.getSemanticCapabilities();
    const auto shape = std::string_view(descriptor.getPersistedDescriptor().getCanonicalPhysicalType());
    return {
        .source = constant,
        .kind = kind,
        .object_semantic_capabilities = references.getSemanticCapabilities(),
        .selected_semantic_capabilities = selected_capabilities,
        .observes_identity = false,
        .expected_role = SemanticExpectedRole{
            .role = LogicalRoleInput::fromBoundObjectUse(references, use, shape),
            .retained_descriptor = {},
        },
    };
}

bool QueryTreeSemanticRoleGraph::registerDirectExplicitCastBoundary(
    const SemanticNodePath & result, const SemanticNodePath & input, FunctionNodePtr function, BoundDeclaredTypeTree::Ptr target)
{
    if (impl->lifecycle == GraphLifecycle::Sealed)
        fail(Error::Code::MutableAfterSeal, "QueryTree semantic-role graph cannot be changed after seal");
    if (!result.isValid() || !input.isValid() || !function || !target)
        fail(Error::Code::InvalidRegistration, "direct QueryTree UDT CAST boundary registration is incomplete");
    if (result == input || result.node == input.node)
        fail(Error::Code::InvalidRegistration, "direct QueryTree UDT CAST result and input must have distinct stable node identities");
    if (!function->isResolved() || !equalsCaseInsensitive(function->getFunctionName(), "CAST")
        || function->getArguments().getNodes().size() != 2)
        fail(Error::Code::InvalidRegistration, "QueryTree semantic-role boundary is not a resolved public CAST function");

    const auto * function_ptr = function.get();
    const auto * input_ptr = function->getArguments().getNodes().front().get();
    const auto * type_argument_ptr = function->getArguments().getNodes().back().get();
    const auto * type_argument_constant = type_argument_ptr ? type_argument_ptr->as<ConstantNode>() : nullptr;
    if (!input_ptr || !type_argument_ptr || input_ptr == function_ptr || type_argument_ptr == function_ptr
        || input_ptr == type_argument_ptr)
        fail(Error::Code::InvalidRegistration, "direct QueryTree UDT CAST has invalid source or type arguments");

    auto selected = selectDirectTarget(*target);
    const auto result_type = function->getResultType();
    if (!result_type || !result_type->equals(*target->getPhysicalType())
        || result_type->getName() != selected.descriptor->getPersistedDescriptor().getCanonicalPhysicalType())
        fail(
            Error::Code::UnsupportedCastShape,
            "resolved QueryTree CAST result has a wrapper or physical shape not represented by its direct UDT target");
    if (!type_argument_constant
        || type_argument_constant->getDataAt()
            != std::string_view(selected.descriptor->getPersistedDescriptor().getCanonicalPhysicalType()))
        fail(Error::Code::InvalidRegistration, "resolved QueryTree CAST type argument disagrees with its direct UDT target");
    const auto type_argument_value = type_argument_constant->getDataAt();
    if (type_argument_value.size() > impl->limits.maximum_single_sink_shape_bytes)
        fail(Error::Code::LimitExceeded, "resolved QueryTree CAST type argument exceeds its registration byte limit");

    const auto validate_node_identity = [&](SemanticNodeID stable_node_id, const IQueryTreeNode * node_ptr)
    {
        if (const auto node = impl->nodes_by_id.find(stable_node_id); node != impl->nodes_by_id.end() && node->second != node_ptr)
            fail(Error::Code::InvalidRegistration, "one stable semantic node ID maps to different QueryTree nodes");
        if (const auto id = impl->ids_by_node.find(node_ptr); id != impl->ids_by_node.end() && id->second != stable_node_id)
            fail(Error::Code::InvalidRegistration, "one QueryTree node maps to different stable semantic node IDs");
    };
    validate_node_identity(result.node, function_ptr);
    validate_node_identity(input.node, input_ptr);

    if (const auto boundary = impl->explicit_cast_boundaries.find(result); boundary != impl->explicit_cast_boundaries.end())
    {
        const auto & existing_descriptors = boundary->second.target->getDescriptors();
        if (boundary->second.function.get() == function_ptr && boundary->second.input == input
            && boundary->second.type_argument == type_argument_ptr
            && std::string_view(boundary->second.type_argument_value) == type_argument_value
            && boundary->second.descriptor_index < existing_descriptors.size() && existing_descriptors[boundary->second.descriptor_index]
            && existing_descriptors[boundary->second.descriptor_index]->getPersistedDescriptor().hasSameInstantiation(
                selected.descriptor->getPersistedDescriptor()))
            return false;
        fail(Error::Code::InvalidRegistration, "one semantic node/path state maps to conflicting explicit CAST boundaries");
    }

    const bool new_result_node = !impl->nodes_by_id.contains(result.node);
    const bool new_input_node = !impl->nodes_by_id.contains(input.node);
    const UInt64 new_node_count = static_cast<UInt64>(new_result_node) + static_cast<UInt64>(new_input_node);
    if (impl->nodes_by_id.size() > impl->limits.maximum_query_nodes
        || new_node_count > impl->limits.maximum_query_nodes - impl->nodes_by_id.size())
        fail(Error::Code::LimitExceeded, "QueryTree semantic-role node registrations exceed their limit");
    if (impl->explicit_cast_boundaries.size() >= impl->limits.maximum_explicit_cast_boundaries)
        fail(Error::Code::LimitExceeded, "QueryTree semantic-role explicit CAST boundaries exceed their limit");

    UInt64 scratch_bytes = sizeof(SemanticNodePath) + sizeof(DirectExplicitCastBoundary) + hash_node_overhead + type_argument_value.size();
    scratch_bytes += new_node_count
        * (sizeof(SemanticNodeID) + sizeof(const IQueryTreeNode *) + sizeof(const IQueryTreeNode *) + sizeof(SemanticNodeID)
           + 2 * hash_node_overhead);
    impl->charge({}, scratch_bytes);

    std::array<std::pair<SemanticNodeID, const IQueryTreeNode *>, 2> inserted_nodes{};
    size_t inserted_node_count = 0;
    const auto insert_node = [&](SemanticNodeID stable_node_id, const IQueryTreeNode * node_ptr)
    {
        if (impl->nodes_by_id.contains(stable_node_id))
            return;
        auto [node, inserted] = impl->nodes_by_id.emplace(stable_node_id, node_ptr);
        if (!inserted)
            fail(Error::Code::InvalidRegistration, "stable semantic node registration changed during insertion");
        try
        {
            const bool id_inserted = impl->ids_by_node.emplace(node_ptr, stable_node_id).second;
            if (!id_inserted)
                fail(Error::Code::InvalidRegistration, "QueryTree semantic node registration changed during insertion");
        }
        catch (...)
        {
            impl->nodes_by_id.erase(node);
            throw;
        }
        inserted_nodes[inserted_node_count++] = {stable_node_id, node_ptr};
    };

    try
    {
        insert_node(result.node, function_ptr);
        insert_node(input.node, input_ptr);
        const bool inserted = impl->explicit_cast_boundaries
                                  .emplace(
                                      result,
                                      DirectExplicitCastBoundary{
                                          .function = std::move(function),
                                          .target = std::move(target),
                                          .descriptor_index = selected.descriptor_index,
                                          .input = input,
                                          .type_argument = type_argument_ptr,
                                          .type_argument_value = String(type_argument_value),
                                      })
                                  .second;
        if (!inserted)
            fail(Error::Code::InvalidRegistration, "explicit CAST boundary registration changed during insertion");
    }
    catch (...)
    {
        while (inserted_node_count)
        {
            const auto [stable_node_id, node_ptr] = inserted_nodes[--inserted_node_count];
            impl->ids_by_node.erase(node_ptr);
            impl->nodes_by_id.erase(stable_node_id);
        }
        throw;
    }

    impl->statistics.query_nodes = impl->nodes_by_id.size();
    impl->statistics.explicit_cast_boundaries = impl->explicit_cast_boundaries.size();
    return true;
}

bool QueryTreeSemanticRoleGraph::registerContextualConstantBoundary(
    const SemanticNodePath & constant,
    ConstantNodePtr constant_node,
    const SemanticNodePath & expected_column,
    ColumnNodePtr expected_column_node,
    BoundObjectTypeReferences::Ptr references,
    PersistedTypeOccurrencePath use_path,
    const SemanticSink & sink)
{
    if (impl->lifecycle == GraphLifecycle::Sealed)
        fail(Error::Code::MutableAfterSeal, "QueryTree semantic-role graph cannot be changed after seal");
    if (!constant.isValid() || !expected_column.isValid() || constant == expected_column || !constant_node || !expected_column_node
        || !references)
        fail(Error::Code::InvalidRegistration, "contextual UDT constant boundary registration is incomplete");
    if (sink.source != constant || !SemanticSinkRegistry::isEligible(sink) || !isContextualConstantKind(sink.kind) || !sink.expected_role
        || sink.expected_role->retained_descriptor)
        fail(Error::Code::InvalidRegistration, "contextual UDT constant has an invalid closed-registry sink");
    if (expected_column_node->getColumn().isSubcolumn())
        fail(Error::Code::InvalidRegistration, "contextual UDT constant expected role is not a schema column");

    const auto * use = references->findUse(use_path);
    const auto expected_section = objectRootSection(references->getObject().kind);
    if (!use || !expected_section || use->getPath().section != *expected_section
        || use->getPath().site != PersistedTypeOccurrenceSite::Declaration || use->getPath().occurrence_ordinal != 0
        || use->getRuntimeOwnerKey() != expected_column_node->getColumn().getNameInStorage() || !use->getPhysicalType()
        || !physicalTypeAtSupportedSemanticPath(expected_column_node->getColumn().getTypeInStorage(), use->getPath().type_child_ordinals)
        || !use->getPhysicalType()->equals(
            *physicalTypeAtSupportedSemanticPath(expected_column_node->getColumn().getTypeInStorage(), use->getPath().type_child_ordinals)))
        fail(Error::Code::InvalidRegistration, "contextual UDT constant expected column disagrees with its bound use");

    const auto descriptors = references->getDescriptors();
    if (use->getDescriptorIndex() >= descriptors.size() || !descriptors[use->getDescriptorIndex()]
        || !descriptors[use->getDescriptorIndex()]->getPersistedDescriptor().hasSameInstantiation(*sink.expected_role->role.descriptor))
        fail(Error::Code::InvalidRegistration, "contextual UDT constant expected role disagrees with its bound descriptor");

    const auto snapshot = getClosedExactSourceSnapshot(expected_column_node->getColumnSourceOrNull());
    if (!snapshot || !snapshot->metadata || snapshot->metadata->getBoundUDTReferences().get() != references.get())
        fail(Error::Code::InvalidRegistration, "contextual UDT constant expected column has no matching bound metadata snapshot");

    const auto validate_node_identity = [&](SemanticNodeID stable_node_id, const IQueryTreeNode * node_ptr)
    {
        if (const auto node = impl->nodes_by_id.find(stable_node_id); node != impl->nodes_by_id.end() && node->second != node_ptr)
            fail(Error::Code::InvalidRegistration, "one stable semantic node ID maps to different QueryTree nodes");
        if (const auto id = impl->ids_by_node.find(node_ptr); id != impl->ids_by_node.end() && id->second != stable_node_id)
            fail(Error::Code::InvalidRegistration, "one QueryTree node maps to different stable semantic node IDs");
    };
    validate_node_identity(constant.node, constant_node.get());
    validate_node_identity(expected_column.node, expected_column_node.get());

    const auto same_sink = [&](const RegisteredSemanticSink & registered)
    {
        const auto candidate = registered.view();
        return candidate.source == sink.source && candidate.kind == sink.kind
            && candidate.object_semantic_capabilities == sink.object_semantic_capabilities
            && candidate.selected_semantic_capabilities == sink.selected_semantic_capabilities && candidate.expected_role
            && candidate.expected_role->role.shape.canonical_encoding == sink.expected_role->role.shape.canonical_encoding
            && candidate.expected_role->role.descriptor->hasSameInstantiation(*sink.expected_role->role.descriptor);
    };
    SemanticSinkID sink_id = invalid_semantic_sink_id;
    for (size_t index = 0; index < impl->sinks.size(); ++index)
    {
        if (same_sink(impl->sinks[index]))
        {
            sink_id = static_cast<SemanticSinkID>(index);
            break;
        }
    }
    if (sink_id == invalid_semantic_sink_id)
    {
        const auto registered = registerSink(sink);
        if (!registered)
            fail(Error::Code::InvalidRegistration, "eligible contextual UDT constant sink was omitted from its query graph");
        sink_id = *registered;
    }

    if (const auto existing = impl->contextual_constant_boundaries.find(sink_id); existing != impl->contextual_constant_boundaries.end())
    {
        if (existing->second.constant.get() == constant_node.get() && existing->second.expected_column == expected_column)
            return false;
        fail(Error::Code::InvalidRegistration, "one contextual UDT sink maps to conflicting QueryTree boundaries");
    }
    if (const auto existing = impl->prebound_sources.find(expected_column); existing != impl->prebound_sources.end())
    {
        if (existing->second.state_node.get() != expected_column_node.get()
            || existing->second.storage_column.get() != expected_column_node.get() || existing->second.references.get() != references.get()
            || existing->second.use_path != use_path)
            fail(Error::Code::InvalidRegistration, "one semantic node/path maps to conflicting prebound schema sources");
    }

    const bool new_constant_node = !impl->nodes_by_id.contains(constant.node);
    const bool new_column_node = !impl->nodes_by_id.contains(expected_column.node);
    const UInt64 new_node_count = static_cast<UInt64>(new_constant_node) + static_cast<UInt64>(new_column_node);
    if (impl->nodes_by_id.size() > impl->limits.maximum_query_nodes
        || new_node_count > impl->limits.maximum_query_nodes - impl->nodes_by_id.size())
        fail(Error::Code::LimitExceeded, "QueryTree semantic-role node registrations exceed their limit");

    UInt64 scratch_bytes = sizeof(SemanticSinkID) + sizeof(ContextualConstantBoundary) + hash_node_overhead;
    if (!impl->prebound_sources.contains(expected_column))
    {
        scratch_bytes += sizeof(SemanticNodePath) + sizeof(PreboundExactSource) + hash_node_overhead
            + static_cast<UInt64>(use_path.type_child_ordinals.size()) * sizeof(UInt64);
    }
    scratch_bytes += new_node_count
        * (sizeof(SemanticNodeID) + sizeof(const IQueryTreeNode *) + sizeof(const IQueryTreeNode *) + sizeof(SemanticNodeID)
           + 2 * hash_node_overhead);
    impl->charge({}, scratch_bytes);

    const auto insert_node = [&](SemanticNodeID stable_node_id, const IQueryTreeNode * node_ptr)
    {
        if (impl->nodes_by_id.contains(stable_node_id))
            return;
        impl->nodes_by_id.emplace(stable_node_id, node_ptr);
        impl->ids_by_node.emplace(node_ptr, stable_node_id);
    };
    insert_node(constant.node, constant_node.get());
    insert_node(expected_column.node, expected_column_node.get());
    impl->prebound_sources.try_emplace(
        expected_column,
        PreboundExactSource{
            .state_node = expected_column_node,
            .storage_column = std::move(expected_column_node),
            .references = std::move(references),
            .use_path = std::move(use_path),
        });
    impl->contextual_constant_boundaries.emplace(
        sink_id,
        ContextualConstantBoundary{
            .constant = std::move(constant_node),
            .expected_column = expected_column,
            .sink = sink_id,
        });
    impl->statistics.query_nodes = impl->nodes_by_id.size();
    return true;
}

bool QueryTreeSemanticRoleGraph::registerPreboundExactSource(
    const SemanticNodePath & state,
    QueryTreeNodePtr state_node,
    ColumnNodePtr storage_column,
    BoundObjectTypeReferences::Ptr references,
    PersistedTypeOccurrencePath use_path)
{
    if (impl->lifecycle == GraphLifecycle::Sealed)
        fail(Error::Code::MutableAfterSeal, "QueryTree semantic-role graph cannot be changed after seal");
    if (!state.isValid() || !state_node || !storage_column || state_node.get() != storage_column.get() || !references)
        fail(Error::Code::InvalidRegistration, "prebound QueryTree semantic source registration is incomplete");
    if (impl->explicit_cast_boundaries.contains(state) || impl->null_only_sources.contains(state)
        || impl->transfer_boundaries.contains(state))
        fail(Error::Code::InvalidRegistration, "one semantic node/path has conflicting source and transfer registrations");

    const auto * use = references->findUse(use_path);
    const auto expected_section = objectRootSection(references->getObject().kind);
    const auto physical_subtree
        = physicalTypeAtSupportedSemanticPath(storage_column->getColumn().getTypeInStorage(), use_path.type_child_ordinals);
    if (!use || !expected_section || use_path.section != *expected_section || use_path.site != PersistedTypeOccurrenceSite::Declaration
        || use_path.occurrence_ordinal != 0 || use->getRuntimeOwnerKey() != storage_column->getColumn().getNameInStorage()
        || !use->getPhysicalType() || !physical_subtree || !use->getPhysicalType()->equals(*physical_subtree))
        fail(Error::Code::InvalidRegistration, "prebound QueryTree semantic source disagrees with its exact bound use");
    const auto snapshot = getClosedExactSourceSnapshot(storage_column->getColumnSourceOrNull());
    if (!snapshot || !snapshot->metadata || snapshot->metadata->getBoundUDTReferences().get() != references.get())
        fail(Error::Code::InvalidRegistration, "prebound QueryTree semantic source has no matching metadata snapshot");

    if (const auto existing = impl->prebound_sources.find(state); existing != impl->prebound_sources.end())
    {
        if (existing->second.state_node.get() == state_node.get() && existing->second.storage_column.get() == storage_column.get()
            && existing->second.references.get() == references.get() && existing->second.use_path == use_path)
            return false;
        fail(Error::Code::InvalidRegistration, "one semantic node/path maps to conflicting prebound sources");
    }
    if (const auto node = impl->nodes_by_id.find(state.node); node != impl->nodes_by_id.end() && node->second != state_node.get())
        fail(Error::Code::InvalidRegistration, "one stable semantic node ID maps to different QueryTree nodes");
    if (const auto id = impl->ids_by_node.find(state_node.get()); id != impl->ids_by_node.end() && id->second != state.node)
        fail(Error::Code::InvalidRegistration, "one QueryTree node maps to different stable semantic node IDs");
    if (!impl->nodes_by_id.contains(state.node) && impl->nodes_by_id.size() >= impl->limits.maximum_query_nodes)
        fail(Error::Code::LimitExceeded, "QueryTree semantic-role node registrations exceed their limit");

    const bool new_node = !impl->nodes_by_id.contains(state.node);
    UInt64 scratch_bytes = sizeof(SemanticNodePath) + sizeof(PreboundExactSource) + hash_node_overhead
        + static_cast<UInt64>(use_path.type_child_ordinals.size()) * sizeof(UInt64);
    if (new_node)
        scratch_bytes += sizeof(SemanticNodeID) + sizeof(const IQueryTreeNode *) + sizeof(const IQueryTreeNode *) + sizeof(SemanticNodeID)
            + 2 * hash_node_overhead;
    impl->charge({}, scratch_bytes);
    if (new_node)
    {
        impl->nodes_by_id.emplace(state.node, state_node.get());
        impl->ids_by_node.emplace(state_node.get(), state.node);
    }
    impl->prebound_sources.emplace(
        state,
        PreboundExactSource{
            .state_node = std::move(state_node),
            .storage_column = std::move(storage_column),
            .references = std::move(references),
            .use_path = std::move(use_path),
        });
    impl->statistics.query_nodes = impl->nodes_by_id.size();
    return true;
}

bool QueryTreeSemanticRoleGraph::registerNullOnlySource(const SemanticNodePath & state, QueryTreeNodePtr state_node)
{
    if (impl->lifecycle == GraphLifecycle::Sealed)
        fail(Error::Code::MutableAfterSeal, "QueryTree semantic-role graph cannot be changed after seal");
    const auto * constant = state_node ? state_node->as<ConstantNode>() : nullptr;
    if (!state.isValid() || !constant || !constant->isNull())
        fail(Error::Code::InvalidRegistration, "QueryTree NULL-only semantic source is not a NULL constant");
    if (impl->explicit_cast_boundaries.contains(state) || impl->prebound_sources.contains(state)
        || impl->transfer_boundaries.contains(state))
        fail(Error::Code::InvalidRegistration, "one semantic node/path has conflicting source and transfer registrations");
    if (const auto existing = impl->null_only_sources.find(state); existing != impl->null_only_sources.end())
    {
        if (existing->second.node.get() == state_node.get())
            return false;
        fail(Error::Code::InvalidRegistration, "one semantic node/path maps to conflicting NULL-only sources");
    }
    if (const auto node = impl->nodes_by_id.find(state.node); node != impl->nodes_by_id.end() && node->second != state_node.get())
        fail(Error::Code::InvalidRegistration, "one stable semantic node ID maps to different QueryTree nodes");
    if (const auto id = impl->ids_by_node.find(state_node.get()); id != impl->ids_by_node.end() && id->second != state.node)
        fail(Error::Code::InvalidRegistration, "one QueryTree node maps to different stable semantic node IDs");
    if (!impl->nodes_by_id.contains(state.node) && impl->nodes_by_id.size() >= impl->limits.maximum_query_nodes)
        fail(Error::Code::LimitExceeded, "QueryTree semantic-role node registrations exceed their limit");
    const bool new_node = !impl->nodes_by_id.contains(state.node);
    UInt64 scratch_bytes = sizeof(SemanticNodePath) + sizeof(NullOnlySource) + hash_node_overhead;
    if (new_node)
        scratch_bytes += sizeof(SemanticNodeID) + sizeof(const IQueryTreeNode *) + sizeof(const IQueryTreeNode *) + sizeof(SemanticNodeID)
            + 2 * hash_node_overhead;
    impl->charge({}, scratch_bytes);
    if (new_node)
    {
        impl->nodes_by_id.emplace(state.node, state_node.get());
        impl->ids_by_node.emplace(state_node.get(), state.node);
    }
    impl->null_only_sources.emplace(state, NullOnlySource{.node = std::move(state_node)});
    impl->statistics.query_nodes = impl->nodes_by_id.size();
    return true;
}

bool QueryTreeSemanticRoleGraph::registerTransferBoundary(
    const SemanticNodePath & result,
    QueryTreeNodePtr result_node,
    SemanticTransferKind transfer,
    std::vector<std::pair<SemanticNodePath, QueryTreeNodePtr>> inputs,
    std::optional<String> result_shape,
    std::optional<UInt32> sparse_projection_ordinal)
{
    if (impl->lifecycle == GraphLifecycle::Sealed)
        fail(Error::Code::MutableAfterSeal, "QueryTree semantic-role graph cannot be changed after seal");
    const auto * descriptor = SemanticTransferRegistry::find(transfer);
    if (!result.isValid() || !result_node || !result_node->getResultType() || !descriptor || descriptor->requires_exact_target
        || inputs.size() < descriptor->minimum_inputs || inputs.size() > descriptor->maximum_inputs
        || (descriptor->requires_result_shape && !result_shape)
        || (result_shape && (!descriptor->allows_result_shape || result_shape->empty())))
        fail(Error::Code::InvalidRegistration, "QueryTree semantic transfer registration disagrees with its closed descriptor");
    if (impl->explicit_cast_boundaries.contains(result) || impl->prebound_sources.contains(result)
        || impl->null_only_sources.contains(result))
        fail(Error::Code::InvalidRegistration, "one semantic node/path has conflicting source and transfer registrations");
    for (const auto & [input, node] : inputs)
    {
        if (!input.isValid() || !node || input == result)
            fail(Error::Code::InvalidRegistration, "QueryTree semantic transfer has an invalid exact input edge");
    }

    std::vector<const IQueryTreeNode *> input_nodes;
    input_nodes.reserve(inputs.size());
    for (const auto & [input, node] : inputs)
    {
        static_cast<void>(input);
        input_nodes.push_back(node.get());
    }
    std::vector<UInt64> static_child_path;
    if (transfer == SemanticTransferKind::StaticChildSelection || transfer == SemanticTransferKind::StaticReshape)
    {
        const auto captured = input_nodes.size() == 1 ? captureStaticTupleElementPath(result_node, input_nodes.front()) : std::nullopt;
        if (!captured)
            fail(Error::Code::InvalidRegistration, "QueryTree static-child semantic transfer is not an exact tupleElement chain");
        static_child_path = *captured;

        const auto source = impl->prebound_sources.find(inputs.front().first);
        if (source == impl->prebound_sources.end() || source->second.use_path.type_child_ordinals.size() < static_child_path.size()
            || !std::equal(static_child_path.begin(), static_child_path.end(), source->second.use_path.type_child_ordinals.begin()))
            fail(Error::Code::InvalidRegistration, "QueryTree static-child semantic transfer does not consume its exact prebound path");
        const auto selected_type
            = physicalTypeAtSupportedSemanticPath(source->second.storage_column->getColumn().getTypeInStorage(), static_child_path);
        if (!selected_type || !result_node->getResultType()->equals(*selected_type))
            fail(Error::Code::InvalidRegistration, "QueryTree static-child semantic transfer result changed physical subtree");
    }
    if (!hasClosedTransferPhysicalShape(*descriptor, result_node, input_nodes, result_shape))
        fail(Error::Code::InvalidRegistration, "QueryTree semantic transfer result shape disagrees with its physical result");
    if (!hasClosedTransferTopology(transfer, result_node, input_nodes, static_child_path, sparse_projection_ordinal))
        fail(Error::Code::InvalidRegistration, "QueryTree semantic transfer is not produced by its closed analyzer adapter");
    if (const auto existing = impl->transfer_boundaries.find(result); existing != impl->transfer_boundaries.end())
    {
        if (existing->second.node.get() == result_node.get() && existing->second.transfer == transfer
            && existing->second.result_shape == result_shape && existing->second.sparse_projection_ordinal == sparse_projection_ordinal
            && existing->second.inputs.size() == inputs.size()
            && std::equal(
                existing->second.inputs.begin(),
                existing->second.inputs.end(),
                inputs.begin(),
                [](const auto & lhs, const auto & rhs) { return lhs == rhs.first; }))
            return false;
        fail(Error::Code::InvalidRegistration, "one semantic node/path maps to conflicting transfer boundaries");
    }

    const auto validate_identity = [&](SemanticNodeID id, const IQueryTreeNode * node)
    {
        if (const auto found = impl->nodes_by_id.find(id); found != impl->nodes_by_id.end() && found->second != node)
            fail(Error::Code::InvalidRegistration, "one stable semantic node ID maps to different QueryTree nodes");
        if (const auto found = impl->ids_by_node.find(node); found != impl->ids_by_node.end() && found->second != id)
            fail(Error::Code::InvalidRegistration, "one QueryTree node maps to different stable semantic node IDs");
    };
    validate_identity(result.node, result_node.get());
    for (const auto & [input, node] : inputs)
        validate_identity(input.node, node.get());

    std::vector<SemanticNodeID> prospective_ids;
    if (!impl->nodes_by_id.contains(result.node))
        prospective_ids.push_back(result.node);
    for (const auto & [input, node] : inputs)
    {
        static_cast<void>(node);
        if (!impl->nodes_by_id.contains(input.node)
            && std::find(prospective_ids.begin(), prospective_ids.end(), input.node) == prospective_ids.end())
            prospective_ids.push_back(input.node);
    }
    const UInt64 new_node_count = prospective_ids.size();
    if (impl->nodes_by_id.size() > impl->limits.maximum_query_nodes
        || new_node_count > impl->limits.maximum_query_nodes - impl->nodes_by_id.size())
        fail(Error::Code::LimitExceeded, "QueryTree semantic-role node registrations exceed their limit");
    if (impl->transfer_boundaries.size() >= impl->limits.maximum_explicit_cast_boundaries)
        fail(Error::Code::LimitExceeded, "QueryTree semantic transfer boundaries exceed their limit");

    UInt64 scratch_bytes = sizeof(SemanticNodePath) + sizeof(RegisteredTransferBoundary) + hash_node_overhead
        + static_cast<UInt64>(inputs.size()) * (sizeof(SemanticNodePath) + sizeof(const IQueryTreeNode *))
        + static_cast<UInt64>(static_child_path.size()) * sizeof(UInt64) + result_node->getResultType()->getName().size()
        + (result_shape ? result_shape->size() : 0);
    scratch_bytes += new_node_count
        * (sizeof(SemanticNodeID) + sizeof(const IQueryTreeNode *) + sizeof(const IQueryTreeNode *) + sizeof(SemanticNodeID)
           + 2 * hash_node_overhead);
    impl->charge({}, scratch_bytes);
    const auto insert_identity = [&](SemanticNodeID id, const IQueryTreeNode * node)
    {
        if (impl->nodes_by_id.contains(id))
            return;
        impl->nodes_by_id.emplace(id, node);
        impl->ids_by_node.emplace(node, id);
    };
    insert_identity(result.node, result_node.get());
    RegisteredTransferBoundary stored{
        .node = std::move(result_node),
        .transfer = transfer,
        .inputs = {},
        .input_nodes = {},
        .result_type_name = {},
        .result_shape = std::move(result_shape),
        .static_child_path = std::move(static_child_path),
        .sparse_projection_ordinal = sparse_projection_ordinal,
    };
    stored.result_type_name = stored.node->getResultType()->getName();
    stored.inputs.reserve(inputs.size());
    stored.input_nodes.reserve(inputs.size());
    for (auto & [input, node] : inputs)
    {
        insert_identity(input.node, node.get());
        stored.inputs.push_back(input);
        stored.input_nodes.push_back(node.get());
    }
    impl->transfer_boundaries.emplace(result, std::move(stored));
    impl->statistics.query_nodes = impl->nodes_by_id.size();
    return true;
}

void QueryTreeSemanticRoleGraph::registerStableNodeIdentity(SemanticNodeID id, const IQueryTreeNode * node)
{
    if (impl->lifecycle == GraphLifecycle::Sealed)
        fail(Error::Code::MutableAfterSeal, "QueryTree semantic-role graph cannot reserve node identities after seal");
    if (id == invalid_semantic_node_id || !node)
        fail(Error::Code::InvalidRegistration, "QueryTree semantic-role node identity is invalid");
    if (const auto found = impl->nodes_by_id.find(id); found != impl->nodes_by_id.end())
    {
        if (found->second != node)
            fail(Error::Code::InvalidRegistration, "one stable semantic node ID maps to different QueryTree nodes");
        return;
    }
    if (const auto found = impl->ids_by_node.find(node); found != impl->ids_by_node.end())
    {
        if (found->second != id)
            fail(Error::Code::InvalidRegistration, "one QueryTree node maps to different stable semantic node IDs");
        return;
    }
    if (impl->nodes_by_id.size() >= impl->limits.maximum_query_nodes)
        fail(Error::Code::LimitExceeded, "QueryTree semantic-role node registrations exceed their limit");

    const UInt64 scratch_bytes = sizeof(SemanticNodeID) + sizeof(const IQueryTreeNode *) + sizeof(const IQueryTreeNode *)
        + sizeof(SemanticNodeID) + 2 * hash_node_overhead;
    impl->charge({}, scratch_bytes);
    const auto inserted_node = impl->nodes_by_id.emplace(id, node).first;
    try
    {
        if (!impl->ids_by_node.emplace(node, id).second)
            fail(Error::Code::InvalidRegistration, "QueryTree semantic node identity changed during insertion");
    }
    catch (...)
    {
        impl->nodes_by_id.erase(inserted_node);
        throw;
    }
    impl->statistics.query_nodes = impl->nodes_by_id.size();
}

std::optional<SemanticNodeID> QueryTreeSemanticRoleGraph::findRegisteredNodeID(const IQueryTreeNode * node) const noexcept
{
    const auto found = impl->ids_by_node.find(node);
    if (found == impl->ids_by_node.end())
        return std::nullopt;
    return found->second;
}

void QueryTreeSemanticRoleGraph::remapQueryTreeNodes(const IQueryTreeNode::CloneNodeMapping & clone_node_mapping)
{
    if (impl->lifecycle == GraphLifecycle::Sealed)
        fail(Error::Code::MutableAfterSeal, "A sealed QueryTree semantic-role graph cannot change generations");
    if (clone_node_mapping.empty())
        return;

    const auto find_replacement = [&](const IQueryTreeNode * node) -> QueryTreeNodePtr
    {
        const auto replacement = clone_node_mapping.find(node);
        if (replacement == clone_node_mapping.end())
            return {};
        if (!replacement->second)
            fail(Error::Code::InvalidRegistration, "A QueryTree clone mapping contains a null semantic node replacement");
        return replacement->second;
    };

    /// Preserve stable IDs without allocating a second pointer map. Node-handle
    /// key replacement reuses the already-accounted unordered-map storage.
    for (auto & [id, node] : impl->nodes_by_id)
    {
        const auto replacement = find_replacement(node);
        if (!replacement)
            continue;
        const auto * replacement_ptr = replacement.get();
        if (const auto existing = impl->ids_by_node.find(replacement_ptr); existing != impl->ids_by_node.end() && existing->second != id)
        {
            fail(Error::Code::InvalidRegistration, "A QueryTree clone mapping merges distinct stable semantic node IDs");
        }

        auto pointer_identity = impl->ids_by_node.extract(node);
        if (pointer_identity.empty() || pointer_identity.mapped() != id)
            fail(Error::Code::InvalidRegistration, "A QueryTree semantic node lost its reverse stable identity during remap");
        pointer_identity.key() = replacement_ptr;
        const auto inserted = impl->ids_by_node.insert(std::move(pointer_identity));
        if (!inserted.inserted)
            fail(Error::Code::InvalidRegistration, "A QueryTree clone mapping collides with an existing semantic node");
        node = replacement_ptr;
    }

    const auto remap_node = [&](QueryTreeNodePtr & node)
    {
        if (const auto replacement = find_replacement(node.get()))
            node = replacement;
    };
    const auto remap_raw_node = [&](const IQueryTreeNode *& node)
    {
        if (const auto replacement = find_replacement(node))
            node = replacement.get();
    };
    const auto remap_function = [&](FunctionNodePtr & function)
    {
        const auto replacement = find_replacement(function.get());
        if (!replacement)
            return;
        if (!replacement->as<FunctionNode>())
            fail(Error::Code::InvalidRegistration, "A QueryTree clone changed a registered function node kind");
        function = std::static_pointer_cast<FunctionNode>(replacement);
    };
    const auto remap_column = [&](ColumnNodePtr & column)
    {
        const auto replacement = find_replacement(column.get());
        if (!replacement)
            return;
        if (!replacement->as<ColumnNode>())
            fail(Error::Code::InvalidRegistration, "A QueryTree clone changed a registered column node kind");
        column = std::static_pointer_cast<ColumnNode>(replacement);
    };
    const auto remap_constant = [&](ConstantNodePtr & constant)
    {
        const auto replacement = find_replacement(constant.get());
        if (!replacement)
            return;
        if (!replacement->as<ConstantNode>())
            fail(Error::Code::InvalidRegistration, "A QueryTree clone changed a registered constant node kind");
        constant = std::static_pointer_cast<ConstantNode>(replacement);
    };

    for (auto & [_, boundary] : impl->explicit_cast_boundaries)
    {
        remap_function(boundary.function);
        remap_raw_node(boundary.type_argument);
    }
    for (auto & [_, source] : impl->prebound_sources)
    {
        remap_node(source.state_node);
        remap_column(source.storage_column);
    }
    for (auto & [_, source] : impl->null_only_sources)
        remap_node(source.node);
    for (auto & [_, transfer] : impl->transfer_boundaries)
    {
        remap_node(transfer.node);
        for (auto & input : transfer.input_nodes)
            remap_raw_node(input);
    }
    for (auto & [_, boundary] : impl->contextual_constant_boundaries)
        remap_constant(boundary.constant);

    /// The clone operation updates ColumnNode weak sources. Revalidate the
    /// complete closed topology now, so a table-expression replacement cannot
    /// silently change the bound metadata snapshot or a transfer edge.
    impl->validateRegistrations();
}

std::optional<SemanticSinkID> QueryTreeSemanticRoleGraph::registerSink(const SemanticSink & sink)
{
    if (impl->lifecycle == GraphLifecycle::Sealed)
        fail(Error::Code::MutableAfterSeal, "QueryTree semantic-role graph cannot register sinks after seal");
    if (!SemanticSinkRegistry::isEligible(sink))
        return std::nullopt;
    if (impl->sinks.size() >= impl->limits.maximum_semantic_sinks)
        fail(Error::Code::LimitExceeded, "QueryTree semantic-role eligible sinks exceed their registration limit");

    const std::string_view expected_shape = sink.expected_role ? sink.expected_role->role.shape.canonical_encoding : std::string_view{};
    if (expected_shape.size() > impl->limits.maximum_single_sink_shape_bytes)
        fail(Error::Code::LimitExceeded, "QueryTree semantic-role sink shape exceeds its registration byte limit");

    ResourceDelta delta;
    delta.add(ResourceLimit::SemanticSinksPerQuery, 1);
    impl->charge(std::move(delta), sizeof(RegisteredSemanticSink) + expected_shape.size());

    const auto id = static_cast<SemanticSinkID>(impl->sinks.size());
    impl->sinks.push_back({sink, String(expected_shape)});
    impl->statistics.eligible_sinks = impl->sinks.size();
    return id;
}

void QueryTreeSemanticRoleGraph::seal()
{
    impl->validateRegistrations();
    impl->lifecycle = GraphLifecycle::Sealed;
}

bool QueryTreeSemanticRoleGraph::isSealed() const noexcept
{
    return impl->lifecycle == GraphLifecycle::Sealed;
}

void QueryTreeSemanticRoleGraph::validateSealed() const
{
    if (impl->lifecycle != GraphLifecycle::Sealed)
        fail(Error::Code::NotSealed, "QueryTree semantic-role graph must be sealed before validation");
    impl->validateRegistrations();
}

UInt64 QueryTreeSemanticRoleGraph::getSinkCount() const
{
    if (impl->lifecycle != GraphLifecycle::Sealed)
        fail(Error::Code::NotSealed, "QueryTree semantic-role sinks must be sealed before enumeration");
    return impl->sinks.size();
}

SemanticSink QueryTreeSemanticRoleGraph::getSink(SemanticSinkID sink) const
{
    if (impl->lifecycle != GraphLifecycle::Sealed)
        fail(Error::Code::NotSealed, "QueryTree semantic-role sinks must be sealed before enumeration");
    if (sink >= impl->sinks.size())
        fail(Error::Code::InvalidRegistration, "QueryTree semantic-role sink ID is outside the sealed enumeration");
    auto result = impl->sinks[sink].view();
    if (!SemanticSinkRegistry::isEligible(result))
        fail(Error::Code::InvalidRegistration, "sealed QueryTree semantic-role sink changed after registration");
    return result;
}

UInt64 QueryTreeSemanticRoleGraph::getGeneration() const noexcept
{
    return impl->generation;
}

SemanticRoleNode QueryTreeSemanticRoleGraph::describe(const SemanticNodePath & state) const
{
    if (impl->lifecycle != GraphLifecycle::Sealed)
        fail(Error::Code::NotSealed, "QueryTree semantic-role graph must be sealed before proof");

    if (const auto * use = impl->findValidatedPreboundSource(state))
    {
        const auto source = impl->prebound_sources.find(state);
        const auto descriptors = source->second.references->getDescriptors();
        if (use->getDescriptorIndex() >= descriptors.size() || !descriptors[use->getDescriptorIndex()])
            fail(Error::Code::InvalidRegistration, "sealed prebound QueryTree source lost its descriptor");
        const auto & shape = descriptors[use->getDescriptorIndex()]->getPersistedDescriptor().getCanonicalPhysicalType();
        return {
            .source = SemanticRoleSource::preboundExact(LogicalRoleInput::fromBoundObjectUse(*source->second.references, *use, shape)),
            .result_shape = std::nullopt,
            .exact_target = std::nullopt,
        };
    }

    if (const auto source = impl->null_only_sources.find(state); source != impl->null_only_sources.end())
    {
        const auto node = impl->nodes_by_id.find(state.node);
        const auto * constant = source->second.node ? source->second.node->as<ConstantNode>() : nullptr;
        if (node == impl->nodes_by_id.end() || node->second != source->second.node.get() || !constant || !constant->isNull())
            fail(Error::Code::InvalidRegistration, "sealed QueryTree NULL-only source changed after registration");
        return {
            .source = SemanticRoleSource::nullOnly(),
            .result_shape = std::nullopt,
            .exact_target = std::nullopt,
        };
    }

    if (const auto * transfer = impl->findValidatedTransfer(state))
    {
        return {
            .source = SemanticRoleSource::none(),
            .transfer = transfer->transfer,
            .input_count = static_cast<UInt32>(transfer->inputs.size()),
            .result_shape
            = transfer->result_shape ? std::optional<LogicalShapeInput>{LogicalShapeInput{*transfer->result_shape}} : std::nullopt,
            .exact_target = std::nullopt,
        };
    }

    const auto * boundary = impl->findValidatedBoundary(state);
    if (!boundary)
        return {};

    const auto & descriptors = boundary->target->getDescriptors();
    if (boundary->descriptor_index >= descriptors.size() || !descriptors[boundary->descriptor_index])
        fail(Error::Code::InvalidRegistration, "sealed QueryTree semantic-role CAST boundary lost its descriptor");
    const auto & descriptor = *descriptors[boundary->descriptor_index];
    const auto & canonical_shape = descriptor.getPersistedDescriptor().getCanonicalPhysicalType();
    return {
        .source = SemanticRoleSource::none(),
        .transfer = SemanticTransferKind::ExactInstantiationCast,
        .input_count = 1,
        .result_shape = std::nullopt,
        .exact_target = LogicalRoleInput::fromDescriptor(descriptor, canonical_shape),
    };
}

SemanticNodePath QueryTreeSemanticRoleGraph::getInput(const SemanticNodePath & state, UInt32 input_index) const
{
    if (impl->lifecycle != GraphLifecycle::Sealed)
        fail(Error::Code::NotSealed, "QueryTree semantic-role graph must be sealed before proof");
    if (const auto * transfer = impl->findValidatedTransfer(state))
    {
        if (input_index >= transfer->inputs.size())
            fail(Error::Code::UnexpectedEdge, "QueryTree semantic-role graph received an unknown registered transfer input edge");
        return transfer->inputs[input_index];
    }
    const auto * boundary = impl->findValidatedBoundary(state);
    if (boundary && input_index == 0)
        return boundary->input;
    fail(Error::Code::UnexpectedEdge, "QueryTree semantic-role graph received an unknown explicit CAST input edge");
}

const QueryTreeSemanticRoleGraphStatistics & QueryTreeSemanticRoleGraph::getStatistics() const noexcept
{
    return impl->statistics;
}

}
