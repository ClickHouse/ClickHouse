#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/UDT/QueryTreeSemanticRoleGraph.h>
#include <Analyzer/UDT/SemanticCacheDependencyDigest.h>
#include <Analyzer/UDT/SemanticRolePlanner.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/UDT/CanonicalHash.h>
#include <DataTypes/UDT/ResourceAccounting.h>
#include <DataTypes/UDT/TemplateChecker.h>

#include <Functions/IFunction.h>

#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace DB::UDT
{
namespace
{

class ResolvedTestFunction final : public IFunctionBase
{
public:
    ResolvedTestFunction(String name_, DataTypePtr result_type_, DataTypes argument_types_)
        : name(std::move(name_))
        , result_type(std::move(result_type_))
        , argument_types(std::move(argument_types_))
    {
    }

    String getName() const override { return name; }
    const DataTypePtr & getResultType() const override { return result_type; }
    const DataTypes & getArgumentTypes() const override { return argument_types; }
    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override { return {}; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

private:
    const String name;
    const DataTypePtr result_type;
    const DataTypes argument_types;
};

UUID testUUID(UInt64 high, UInt64 low)
{
    UUID result{};
    UUIDHelpers::getHighBytes(result) = high;
    UUIDHelpers::getLowBytes(result) = low;
    return result;
}

Definition::Ptr checkedDefinition(UInt64 type_id, SemanticCapabilityMask capabilities)
{
    DefinitionInput input;
    input.identity = {
        .database_uuid = testUUID(0x550e8400e29b41d4ULL, 0xa716446655440000ULL),
        .type_uuid = testUUID(0x223456789abcdef0ULL, type_id),
        .revision = 1,
    };
    input.normalized_name = "semantic_graph.GraphType" + std::to_string(type_id);
    input.normalized_local_name = "GraphType" + std::to_string(type_id);
    TemplateNode root;
    root.kind = TemplateNodeKind::BuiltIn;
    root.atom = "UInt64";
    input.nodes.push_back(std::move(root));
    input.semantic_capabilities = capabilities;
    input.policy_bearing = true;
    input.policy_semantic_hash = hashDomainSeparated("ClickHouse UDT semantic graph test V1", input.normalized_name);
    return TemplateChecker::checkAll({std::move(input)}).front();
}

InstantiatedTypeDescriptor::Ptr descriptor(UInt64 type_id)
{
    const auto capabilities = semanticCapabilityBit(SemanticCapability::Input) | semanticCapabilityBit(SemanticCapability::ValueChecks)
        | semanticCapabilityBit(SemanticCapability::Default);
    return InstantiatedTypeDescriptor::create(
        checkedDefinition(type_id, capabilities), CanonicalTypeArguments::validate({}, {}), std::make_shared<DataTypeUInt64>());
}

BoundDeclaredTypeTree::Ptr directTree(const InstantiatedTypeDescriptor::Ptr & logical_descriptor)
{
    std::vector<BoundDeclaredTypeNodeInput> nodes{
        {.type_child_ordinals = {}, .physical_type = logical_descriptor->getPhysicalType()},
    };
    std::vector<BoundDeclaredTypeOccurrenceInput> occurrences{
        {.type_child_ordinals = {}, .logical_descriptor = logical_descriptor, .logical_preorder = 0},
    };
    return BoundDeclaredTypeTree::build(std::move(nodes), std::move(occurrences), {});
}

EffectiveResourceLimits testResourceLimits()
{
    return calculateEffectiveResourceLimits(std::span<const ResourceLimitLayer>{});
}

FunctionNodePtr resolvedFunction(String name, const DataTypePtr & result_type, QueryTreeNodes arguments)
{
    auto function = std::make_shared<FunctionNode>(name);
    DataTypes argument_types;
    argument_types.reserve(arguments.size());
    for (const auto & argument : arguments)
    {
        argument_types.push_back(argument->getResultType());
        function->getArguments().getNodes().push_back(argument);
    }
    function->resolveAsFunction(std::make_shared<ResolvedTestFunction>(std::move(name), result_type, std::move(argument_types)));
    return function;
}

struct DirectCastFixture
{
    InstantiatedTypeDescriptor::Ptr logical_descriptor = descriptor(1);
    BoundDeclaredTypeTree::Ptr target = directTree(logical_descriptor);
    ConstantNodePtr input = std::make_shared<ConstantNode>(Field(UInt64{7}), std::make_shared<DataTypeUInt64>());
    ConstantNodePtr type_argument = std::make_shared<ConstantNode>(Field(String{"UInt64"}), std::make_shared<DataTypeString>());
    FunctionNodePtr function = resolvedFunction("CAST", logical_descriptor->getPhysicalType(), {input, type_argument});
    SemanticNodePath result{.node = 1, .path = 0};
    SemanticNodePath source{.node = 2, .path = 0};

    SemanticSink sink() const { return QueryTreeSemanticRoleGraph::makeDirectExplicitCastSink(result, *target); }
};

SemanticSink contextualSink(const SemanticNodePath & source, const InstantiatedTypeDescriptor::Ptr & expected)
{
    const auto capabilities = semanticCapabilityBit(SemanticCapability::Input);
    return {
        .source = source,
        .kind = SemanticSinkKind::EqualityConstant,
        .object_semantic_capabilities = capabilities,
        .selected_semantic_capabilities = capabilities,
        .observes_identity = false,
        .expected_role = SemanticExpectedRole{
            .role = LogicalRoleInput::fromDescriptor(*expected, expected->getPersistedDescriptor().getCanonicalPhysicalType()),
            .retained_descriptor = expected,
        },
    };
}

template <typename Callback>
void expectGraphError(QueryTreeSemanticRoleGraphError::Code code, Callback && callback)
{
    try
    {
        callback();
        FAIL() << "expected a QueryTree semantic-role graph error";
    }
    catch (const QueryTreeSemanticRoleGraphError & error)
    {
        EXPECT_EQ(error.code, code) << error.what();
    }
}

template <typename Callback>
void expectPlannerError(SemanticRolePlannerError::Code code, Callback && callback)
{
    try
    {
        callback();
        FAIL() << "expected a semantic-role planner error";
    }
    catch (const SemanticRolePlannerError & error)
    {
        EXPECT_EQ(error.code, code) << error.what();
    }
}

template <typename Callback>
void expectCacheDependencyError(SemanticCacheDependencyError::Code code, Callback && callback)
{
    try
    {
        callback();
        FAIL() << "expected a semantic cache dependency error";
    }
    catch (const SemanticCacheDependencyError & error)
    {
        EXPECT_EQ(error.code, code) << error.what();
    }
}

UInt64 varUIntBytes(UInt64 value)
{
    UInt64 result = 1;
    while (value >= 0x80)
    {
        value >>= 7;
        ++result;
    }
    return result;
}

struct CacheDependencyImage
{
    SemanticCacheDependencyKind kind = SemanticCacheDependencyKind::PhysicalOnly;
    Digest digest{};
    UInt64 role_count = 0;
    UInt64 canonical_bytes = 0;
};

CacheDependencyImage cacheDependencyFor(std::span<const InstantiatedTypeDescriptor::Ptr> roles)
{
    ProspectiveResourceBudget budget(testResourceLimits());
    auto graph = QueryTreeSemanticRoleGraph::create(1, budget);
    SemanticNodeID source_node = 1;
    for (const auto & role : roles)
    {
        const auto sink = contextualSink({.node = source_node++, .path = 0}, role);
        EXPECT_TRUE(graph->registerSink(sink));
    }
    graph->seal();
    auto planner = SemanticRolePlanner::create(*graph, budget);
    planner->seal();
    const auto & dependency = planner->getCacheDependencyDigest();
    return {
        .kind = dependency.getKind(),
        .digest = dependency.getDigest(),
        .role_count = dependency.getRoleCount(),
        .canonical_bytes = dependency.getCanonicalEncodingBytes(),
    };
}

}

TEST(UDTSemanticGraph, DirectCastOwnsExactASTAndSurvivesDeepCloneRemap)
{
    ProspectiveResourceBudget budget(testResourceLimits());
    DirectCastFixture fixture;
    auto graph = QueryTreeSemanticRoleGraph::createIfEligible(17, fixture.sink(), budget);
    ASSERT_TRUE(graph);
    EXPECT_TRUE(graph->registerDirectExplicitCastBoundary(fixture.result, fixture.source, fixture.function, fixture.target));
    EXPECT_FALSE(graph->registerDirectExplicitCastBoundary(fixture.result, fixture.source, fixture.function, fixture.target));

    IQueryTreeNode::CloneNodeMapping clone_mapping;
    const auto cloned_root = fixture.function->cloneAndReplace({}, &clone_mapping);
    ASSERT_TRUE(cloned_root->as<FunctionNode>());
    ASSERT_TRUE(clone_mapping.contains(fixture.function.get()));
    ASSERT_TRUE(clone_mapping.contains(fixture.input.get()));
    ASSERT_TRUE(clone_mapping.contains(fixture.type_argument.get()));
    graph->remapQueryTreeNodes(clone_mapping);

    fixture.function.reset();
    fixture.input.reset();
    fixture.type_argument.reset();
    fixture.target.reset();
    graph->seal();
    graph->validateSealed();

    auto planner = SemanticRolePlanner::create(*graph, budget);
    planner->seal();
    ASSERT_EQ(planner->getPlannedBoundaries().size(), 1);
    const auto & boundary = planner->getPlannedBoundary(0);
    EXPECT_EQ(boundary.kind, PlannedBoundaryKind::ApplyExpectedRole);
    EXPECT_EQ(boundary.source_proof.getKind(), RoleProof::Kind::NoRole);
    EXPECT_NE(boundary.retained_definition_handle, invalid_query_definition_handle_id);
    EXPECT_EQ(planner->getGeneration(), 17);
}

TEST(UDTSemanticGraph, PartialCloneRemapPreservesUnmappedStableNodes)
{
    ProspectiveResourceBudget budget(testResourceLimits());
    auto graph = QueryTreeSemanticRoleGraph::create(1, budget);
    auto first = std::make_shared<ConstantNode>(Field(Null{}));
    auto second = std::make_shared<ConstantNode>(Field(Null{}));
    const SemanticNodePath first_state{.node = 1, .path = 0};
    const SemanticNodePath second_state{.node = 2, .path = 0};
    ASSERT_TRUE(graph->registerNullOnlySource(first_state, first));
    ASSERT_TRUE(graph->registerNullOnlySource(second_state, second));

    IQueryTreeNode::CloneNodeMapping clone_mapping;
    const auto cloned_first = first->cloneAndReplace({}, &clone_mapping);
    ASSERT_NE(cloned_first.get(), first.get());
    ASSERT_TRUE(cloned_first->as<ConstantNode>());
    ASSERT_TRUE(clone_mapping.contains(first.get()));
    graph->remapQueryTreeNodes(clone_mapping);
    first.reset();

    EXPECT_EQ(graph->findRegisteredNodeID(cloned_first.get()), first_state.node);
    EXPECT_EQ(graph->findRegisteredNodeID(second.get()), second_state.node);
    graph->seal();

    auto planner = SemanticRolePlanner::create(*graph, budget);
    EXPECT_EQ(planner->prove(first_state).getKind(), RoleProof::Kind::NullOnly);
    EXPECT_EQ(planner->prove(second_state).getKind(), RoleProof::Kind::NullOnly);
}

TEST(UDTSemanticGraph, CloneRemapRejectsMergedIdentitiesAndNodeKindChanges)
{
    {
        ProspectiveResourceBudget budget(testResourceLimits());
        auto graph = QueryTreeSemanticRoleGraph::create(1, budget);
        const auto first = std::make_shared<ConstantNode>(Field(Null{}));
        const auto second = std::make_shared<ConstantNode>(Field(Null{}));
        const auto replacement = std::make_shared<ConstantNode>(Field(Null{}));
        graph->registerStableNodeIdentity(1, first.get());
        graph->registerStableNodeIdentity(2, second.get());

        IQueryTreeNode::CloneNodeMapping clone_mapping;
        clone_mapping.emplace(first.get(), replacement);
        clone_mapping.emplace(second.get(), replacement);
        expectGraphError(QueryTreeSemanticRoleGraphError::Code::InvalidRegistration, [&] { graph->remapQueryTreeNodes(clone_mapping); });
    }

    {
        ProspectiveResourceBudget budget(testResourceLimits());
        DirectCastFixture fixture;
        auto graph = QueryTreeSemanticRoleGraph::createIfEligible(2, fixture.sink(), budget);
        ASSERT_TRUE(graph);
        ASSERT_TRUE(graph->registerDirectExplicitCastBoundary(fixture.result, fixture.source, fixture.function, fixture.target));

        IQueryTreeNode::CloneNodeMapping clone_mapping;
        clone_mapping.emplace(fixture.function.get(), std::make_shared<ConstantNode>(Field(Null{})));
        expectGraphError(QueryTreeSemanticRoleGraphError::Code::InvalidRegistration, [&] { graph->remapQueryTreeNodes(clone_mapping); });
    }
}

TEST(UDTSemanticGraph, DirectCastLiveMutationFailsClosedAtPublicationBarrier)
{
    ProspectiveResourceBudget budget(testResourceLimits());
    DirectCastFixture fixture;
    auto graph = QueryTreeSemanticRoleGraph::createIfEligible(1, fixture.sink(), budget);
    ASSERT_TRUE(graph);
    ASSERT_TRUE(graph->registerDirectExplicitCastBoundary(fixture.result, fixture.source, fixture.function, fixture.target));

    fixture.function->getArguments().getNodes()[1]
        = std::make_shared<ConstantNode>(Field(String{"String"}), std::make_shared<DataTypeString>());
    expectGraphError(QueryTreeSemanticRoleGraphError::Code::InvalidRegistration, [&] { graph->seal(); });
    EXPECT_FALSE(graph->isSealed());
}

TEST(UDTSemanticGraph, SharedCastAcrossManySinksIsMemoizedAndLimitsFailBeforeOmission)
{
    ProspectiveResourceBudget budget(testResourceLimits());
    DirectCastFixture fixture;
    QueryTreeSemanticRoleGraphLimits graph_limits;
    graph_limits.maximum_semantic_sinks = 8;
    auto graph = QueryTreeSemanticRoleGraph::createIfEligible(1, fixture.sink(), budget, graph_limits);
    ASSERT_TRUE(graph);
    ASSERT_TRUE(graph->registerDirectExplicitCastBoundary(fixture.result, fixture.source, fixture.function, fixture.target));
    for (size_t index = 1; index < graph_limits.maximum_semantic_sinks; ++index)
    {
        const auto sink_id = graph->registerSink(fixture.sink());
        ASSERT_TRUE(sink_id);
        EXPECT_EQ(*sink_id, index);
    }
    expectGraphError(QueryTreeSemanticRoleGraphError::Code::LimitExceeded, [&] { static_cast<void>(graph->registerSink(fixture.sink())); });

    graph->seal();
    auto planner = SemanticRolePlanner::create(*graph, budget);
    planner->seal();
    EXPECT_EQ(planner->getPlannedBoundaries().size(), graph_limits.maximum_semantic_sinks);
    EXPECT_EQ(planner->getStatistics().demanded_states, 2);
    EXPECT_EQ(planner->getStatistics().inspected_edges, 1);
    for (const auto & boundary : planner->getPlannedBoundaries())
        EXPECT_EQ(boundary.kind, PlannedBoundaryKind::ApplyExpectedRole);

    ProspectiveResourceBudget small_budget(testResourceLimits());
    QueryTreeSemanticRoleGraphLimits node_limits;
    node_limits.maximum_query_nodes = 1;
    auto too_small = QueryTreeSemanticRoleGraph::createIfEligible(2, fixture.sink(), small_budget, node_limits);
    ASSERT_TRUE(too_small);
    expectGraphError(
        QueryTreeSemanticRoleGraphError::Code::LimitExceeded,
        [&]
        {
            static_cast<void>(
                too_small->registerDirectExplicitCastBoundary(fixture.result, fixture.source, fixture.function, fixture.target));
        });
}

TEST(UDTSemanticGraph, DeepNullOnlyChainIsIterativeAndHonorsDepthLimit)
{
    constexpr UInt32 depth = 128;
    const auto null_node = std::make_shared<ConstantNode>(Field(Null{}));
    const SemanticNodeID node_id = 11;
    const auto expected = descriptor(10);

    ProspectiveResourceBudget budget(testResourceLimits());
    auto graph = QueryTreeSemanticRoleGraph::createIfEligible(1, contextualSink({.node = node_id, .path = depth}, expected), budget);
    ASSERT_TRUE(graph);
    ASSERT_TRUE(graph->registerNullOnlySource({.node = node_id, .path = 0}, null_node));
    for (UInt32 path = 1; path <= depth; ++path)
    {
        ASSERT_TRUE(graph->registerTransferBoundary(
            {.node = node_id, .path = path},
            null_node,
            SemanticTransferKind::Identity,
            {{{.node = node_id, .path = path - 1}, null_node}}));
    }
    graph->seal();

    auto planner = SemanticRolePlanner::create(*graph, budget);
    EXPECT_EQ(planner->prove(node_id, depth).getKind(), RoleProof::Kind::NullOnly);
    EXPECT_EQ(planner->getStatistics().demanded_states, depth + 1);
    EXPECT_EQ(planner->getStatistics().inspected_edges, depth);
    planner->seal();
    EXPECT_EQ(planner->getPlannedBoundary(0).kind, PlannedBoundaryKind::ApplyExpectedRole);

    ProspectiveResourceBudget limited_budget(testResourceLimits());
    auto limited_planner_limits = SemanticRolePlannerLimits{};
    limited_planner_limits.maximum_active_depth = depth;
    auto limited_planner = SemanticRolePlanner::create(*graph, limited_budget, limited_planner_limits);
    expectPlannerError(SemanticRolePlannerError::Code::LimitExceeded, [&] { static_cast<void>(limited_planner->prove(node_id, depth)); });
    expectPlannerError(SemanticRolePlannerError::Code::InvalidGraph, [&] { static_cast<void>(limited_planner->prove(node_id, depth)); });
    expectPlannerError(SemanticRolePlannerError::Code::InvalidGraph, [&] { limited_planner->seal(); });
    EXPECT_FALSE(limited_planner->isSealed());
}

TEST(UDTSemanticGraph, WideBranchAccountsEveryNullArmAndEveryEdge)
{
    constexpr UInt32 value_count = 96;
    const auto nullable_uint64 = DataTypeFactory::instance().get("Nullable(UInt64)");
    const auto uint8 = std::make_shared<DataTypeUInt8>();
    QueryTreeNodes function_arguments;
    std::vector<ConstantNodePtr> values;
    values.reserve(value_count);
    for (UInt32 index = 0; index + 1 < value_count; ++index)
    {
        function_arguments.push_back(std::make_shared<ConstantNode>(Field(UInt64{index & 1}), uint8));
        auto value = std::make_shared<ConstantNode>(Field(Null{}), nullable_uint64);
        values.push_back(value);
        function_arguments.push_back(value);
    }
    auto final_value = std::make_shared<ConstantNode>(Field(Null{}), nullable_uint64);
    values.push_back(final_value);
    function_arguments.push_back(final_value);
    auto branch = resolvedFunction("multiIf", nullable_uint64, std::move(function_arguments));

    ProspectiveResourceBudget budget(testResourceLimits());
    auto graph = QueryTreeSemanticRoleGraph::create(1, budget);
    std::vector<std::pair<SemanticNodePath, QueryTreeNodePtr>> inputs;
    inputs.reserve(values.size());
    for (UInt32 index = 0; index < values.size(); ++index)
    {
        const SemanticNodePath state{.node = static_cast<SemanticNodeID>(index + 1), .path = 0};
        ASSERT_TRUE(graph->registerNullOnlySource(state, values[index]));
        inputs.emplace_back(state, values[index]);
    }
    const SemanticNodePath result{.node = 10'000, .path = 0};
    ASSERT_TRUE(graph->registerTransferBoundary(result, branch, SemanticTransferKind::UnanimousBranch, std::move(inputs)));
    graph->seal();

    auto planner = SemanticRolePlanner::create(*graph, budget);
    EXPECT_EQ(planner->prove(result).getKind(), RoleProof::Kind::NullOnly);
    EXPECT_EQ(planner->getStatistics().demanded_states, value_count + 1);
    EXPECT_EQ(planner->getStatistics().inspected_edges, value_count);
}

TEST(UDTSemanticGraph, OpaqueAndCyclicGraphsNeverInventProvenance)
{
    ProspectiveResourceBudget opaque_budget(testResourceLimits());
    auto opaque_graph = QueryTreeSemanticRoleGraph::create(1, opaque_budget);
    opaque_graph->seal();
    auto opaque_planner = SemanticRolePlanner::create(*opaque_graph, opaque_budget);
    EXPECT_EQ(opaque_planner->prove(91, 7).getKind(), RoleProof::Kind::NoRole);

    ProspectiveResourceBudget cycle_budget(testResourceLimits());
    auto cycle_graph = QueryTreeSemanticRoleGraph::create(2, cycle_budget);
    const auto node = std::make_shared<ConstantNode>(Field(UInt64{1}), std::make_shared<DataTypeUInt64>());
    const SemanticNodePath first{.node = 5, .path = 0};
    const SemanticNodePath second{.node = 5, .path = 1};
    ASSERT_TRUE(cycle_graph->registerTransferBoundary(first, node, SemanticTransferKind::Identity, {{second, node}}));
    ASSERT_TRUE(cycle_graph->registerTransferBoundary(second, node, SemanticTransferKind::Identity, {{first, node}}));
    cycle_graph->seal();

    auto cycle_planner = SemanticRolePlanner::create(*cycle_graph, cycle_budget);
    const auto conflict = cycle_planner->prove(first);
    ASSERT_TRUE(conflict.isConflict());
    EXPECT_EQ(cycle_planner->getConflict(conflict.getConflict()).kind, SemanticRoleConflictKind::Cycle);
}

TEST(UDTSemanticGraph, OuterJoinNullableAndPreservedSidesUseDistinctClosedTransfers)
{
    const auto run_orientation = [](bool left_is_outer_null_side)
    {
        ProspectiveResourceBudget budget(testResourceLimits());
        auto graph = QueryTreeSemanticRoleGraph::create(1, budget);
        const auto nullable_uint64 = DataTypeFactory::instance().get("Nullable(UInt64)");
        const auto left = std::make_shared<ColumnNode>(NameAndTypePair{"left", nullable_uint64}, TableExpressionNodeWeakPtr{});
        const auto right = std::make_shared<ColumnNode>(NameAndTypePair{"right", nullable_uint64}, TableExpressionNodeWeakPtr{});
        const SemanticNodePath left_input{.node = 1, .path = 0};
        const SemanticNodePath left_result{.node = 1, .path = 1};
        const SemanticNodePath right_input{.node = 2, .path = 0};
        const SemanticNodePath right_result{.node = 2, .path = 1};

        const auto register_side
            = [&](bool outer_null_side, const SemanticNodePath & result, const ColumnNodePtr & node, const SemanticNodePath & input)
        {
            return graph->registerTransferBoundary(
                result,
                node,
                outer_null_side ? SemanticTransferKind::JoinDirectNullableLift : SemanticTransferKind::JoinDirectNonSynthesizing,
                {{input, node}},
                outer_null_side ? std::optional<String>{nullable_uint64->getName()} : std::nullopt);
        };
        ASSERT_TRUE(register_side(left_is_outer_null_side, left_result, left, left_input));
        ASSERT_TRUE(register_side(!left_is_outer_null_side, right_result, right, right_input));
        graph->seal();

        EXPECT_EQ(
            graph->describe(left_result).transfer,
            left_is_outer_null_side ? SemanticTransferKind::JoinDirectNullableLift : SemanticTransferKind::JoinDirectNonSynthesizing);
        EXPECT_EQ(
            graph->describe(right_result).transfer,
            left_is_outer_null_side ? SemanticTransferKind::JoinDirectNonSynthesizing : SemanticTransferKind::JoinDirectNullableLift);
        auto planner = SemanticRolePlanner::create(*graph, budget);
        EXPECT_EQ(planner->prove(left_result).getKind(), RoleProof::Kind::NoRole);
        EXPECT_EQ(planner->prove(right_result).getKind(), RoleProof::Kind::NoRole);
    };

    run_orientation(true);
    run_orientation(false);
}

TEST(UDTSemanticGraph, TupleElementArrayElementAndCastHaveNoPhysicalIdentityFallback)
{
    ProspectiveResourceBudget budget(testResourceLimits());
    auto graph = QueryTreeSemanticRoleGraph::create(1, budget);
    const auto tuple_type = DataTypeFactory::instance().get("Tuple(UInt64, UInt64)");
    const auto array_type = DataTypeFactory::instance().get("Array(UInt64)");
    const auto uint64 = std::make_shared<DataTypeUInt64>();
    const auto tuple_column = std::make_shared<ColumnNode>(NameAndTypePair{"tuple", tuple_type}, TableExpressionNodeWeakPtr{});
    const auto array_column = std::make_shared<ColumnNode>(NameAndTypePair{"array", array_type}, TableExpressionNodeWeakPtr{});
    const auto selector = std::make_shared<ConstantNode>(Field(UInt64{1}), uint64);
    const auto tuple_element = resolvedFunction("tupleElement", uint64, {tuple_column, selector});
    const auto array_element = resolvedFunction("arrayElement", uint64, {array_column, selector});

    expectGraphError(
        QueryTreeSemanticRoleGraphError::Code::InvalidRegistration,
        [&]
        {
            static_cast<void>(graph->registerTransferBoundary(
                {.node = 2, .path = 0},
                tuple_element,
                SemanticTransferKind::StaticChildSelection,
                {{{.node = 1, .path = 0}, tuple_column}}));
        });
    expectGraphError(
        QueryTreeSemanticRoleGraphError::Code::InvalidRegistration,
        [&]
        {
            static_cast<void>(graph->registerTransferBoundary(
                {.node = 4, .path = 0},
                array_element,
                SemanticTransferKind::StaticChildSelection,
                {{{.node = 3, .path = 0}, array_column}}));
        });

    DirectCastFixture cast;
    auto cast_graph = QueryTreeSemanticRoleGraph::createIfEligible(2, cast.sink(), budget);
    ASSERT_TRUE(cast_graph);
    EXPECT_TRUE(cast_graph->registerDirectExplicitCastBoundary(cast.result, cast.source, cast.function, cast.target));
}

TEST(UDTSemanticGraph, PlannerProducesEveryBoundaryKindReachableThroughClosedSinks)
{
    const auto expected = descriptor(30);
    const SemanticNodePath first{.node = 30, .path = 0};
    const SemanticNodePath second{.node = 30, .path = 1};
    const auto node = std::make_shared<ConstantNode>(Field(UInt64{1}), std::make_shared<DataTypeUInt64>());

    ProspectiveResourceBudget budget(testResourceLimits());
    auto graph = QueryTreeSemanticRoleGraph::createIfEligible(1, contextualSink(first, expected), budget);
    ASSERT_TRUE(graph);
    ASSERT_TRUE(graph->registerTransferBoundary(first, node, SemanticTransferKind::Identity, {{second, node}}));
    ASSERT_TRUE(graph->registerTransferBoundary(second, node, SemanticTransferKind::Identity, {{first, node}}));
    graph->seal();

    auto planner = SemanticRolePlanner::create(*graph, budget);
    const auto apply = planner->satisfy(0, RoleProof::noRole());
    ASSERT_NE(apply.expected_role, invalid_logical_role_id);
    EXPECT_EQ(apply.kind, PlannedBoundaryKind::ApplyExpectedRole);

    const auto preserve = planner->satisfy(0, RoleProof::exact(apply.expected_role));
    EXPECT_EQ(preserve.kind, PlannedBoundaryKind::PreserveSourceRole);
    EXPECT_EQ(preserve.expected_role, apply.expected_role);

    const auto source_proof = planner->prove(first);
    ASSERT_TRUE(source_proof.isConflict());
    const auto conflict = planner->satisfy(0, source_proof);
    EXPECT_EQ(conflict.kind, PlannedBoundaryKind::Conflict);
    EXPECT_EQ(planner->getConflict(source_proof.getConflict()).kind, SemanticRoleConflictKind::Cycle);

    planner->seal();
    ASSERT_EQ(planner->getPlannedBoundaries().size(), 1);
    EXPECT_EQ(planner->getPlannedBoundary(0).kind, PlannedBoundaryKind::Conflict);
}

TEST(UDTSemanticGraph, CacheDependencyCandidatesRequireExactStateNamespaceAndCanonicalImage)
{
    ProspectiveResourceBudget semantic_budget(testResourceLimits());
    const auto expected_role = descriptor(40);
    auto semantic_graph
        = QueryTreeSemanticRoleGraph::createIfEligible(1, contextualSink({.node = 1, .path = 0}, expected_role), semantic_budget);
    ASSERT_TRUE(semantic_graph);
    semantic_graph->seal();
    auto semantic_planner = SemanticRolePlanner::create(*semantic_graph, semantic_budget);
    semantic_planner->seal();
    const auto & semantic = semantic_planner->getCacheDependencyDigest();
    ASSERT_EQ(semantic.getKind(), SemanticCacheDependencyKind::Semantic);
    ASSERT_GT(semantic.getRoleCount(), 0);
    ASSERT_GT(semantic.getCanonicalEncodingBytes(), 0);

    ProspectiveResourceBudget physical_budget(testResourceLimits());
    auto physical_graph = QueryTreeSemanticRoleGraph::create(2, physical_budget);
    physical_graph->seal();
    auto physical_planner = SemanticRolePlanner::create(*physical_graph, physical_budget);
    physical_planner->seal();
    const auto & physical = physical_planner->getCacheDependencyDigest();
    ASSERT_EQ(physical.getKind(), SemanticCacheDependencyKind::PhysicalOnly);
    EXPECT_EQ(physical.getRoleCount(), 0);
    EXPECT_EQ(physical.getCanonicalEncodingBytes(), 0);
    EXPECT_EQ(physical.getDigest(), Digest{});

    const auto semantic_candidate = SemanticCacheDependencyCandidate::fromCanonical(semantic);
    EXPECT_EQ(semantic_candidate.state, SemanticCacheDependencyCandidateState::CompleteCanonical);
    EXPECT_EQ(semantic_candidate.format_version, semantic_cache_dependency_digest_format_version);
    EXPECT_EQ(semantic_candidate.digest, semantic.getDigest());
    EXPECT_EQ(semantic_candidate.role_count, semantic.getRoleCount());
    EXPECT_EQ(semantic_candidate.canonical_encoding_bytes, semantic.getCanonicalEncodingBytes());
    EXPECT_EQ(compareSemanticCacheDependency(semantic, semantic_candidate), SemanticCacheDependencyComparison::Match);

    const auto physical_candidate = SemanticCacheDependencyCandidate::fromCanonical(physical);
    EXPECT_EQ(physical_candidate.state, SemanticCacheDependencyCandidateState::PhysicalOnly);
    EXPECT_EQ(physical_candidate.format_version, 0);
    EXPECT_EQ(physical_candidate.digest, Digest{});
    EXPECT_EQ(physical_candidate.role_count, 0);
    EXPECT_EQ(physical_candidate.canonical_encoding_bytes, 0);
    EXPECT_EQ(compareSemanticCacheDependency(physical, physical_candidate), SemanticCacheDependencyComparison::Match);
    EXPECT_EQ(compareSemanticCacheDependency(semantic, physical_candidate), SemanticCacheDependencyComparison::NamespaceMismatch);
    EXPECT_EQ(compareSemanticCacheDependency(physical, semantic_candidate), SemanticCacheDependencyComparison::NamespaceMismatch);

    for (const auto [state, comparison] : {
             std::pair{SemanticCacheDependencyCandidateState::Absent, SemanticCacheDependencyComparison::Absent},
             std::pair{SemanticCacheDependencyCandidateState::Partial, SemanticCacheDependencyComparison::Partial},
             std::pair{SemanticCacheDependencyCandidateState::Stale, SemanticCacheDependencyComparison::Stale},
             std::pair{SemanticCacheDependencyCandidateState::NonCanonical, SemanticCacheDependencyComparison::NonCanonical},
         })
    {
        SemanticCacheDependencyCandidate candidate{.state = state};
        EXPECT_EQ(compareSemanticCacheDependency(semantic, candidate), comparison);
        EXPECT_EQ(compareSemanticCacheDependency(physical, candidate), comparison);
    }

    const auto expect_noncanonical_physical = [&](SemanticCacheDependencyCandidate candidate)
    { EXPECT_EQ(compareSemanticCacheDependency(physical, candidate), SemanticCacheDependencyComparison::NonCanonical); };
    auto malformed_physical = physical_candidate;
    malformed_physical.format_version = 1;
    expect_noncanonical_physical(malformed_physical);
    malformed_physical = physical_candidate;
    malformed_physical.digest[0] = 1;
    expect_noncanonical_physical(malformed_physical);
    malformed_physical = physical_candidate;
    malformed_physical.role_count = 1;
    expect_noncanonical_physical(malformed_physical);
    malformed_physical = physical_candidate;
    malformed_physical.canonical_encoding_bytes = 1;
    expect_noncanonical_physical(malformed_physical);

    const auto expect_noncanonical_semantic = [&](SemanticCacheDependencyCandidate candidate)
    { EXPECT_EQ(compareSemanticCacheDependency(semantic, candidate), SemanticCacheDependencyComparison::NonCanonical); };
    auto malformed_semantic = semantic_candidate;
    malformed_semantic.format_version = 0;
    expect_noncanonical_semantic(malformed_semantic);
    malformed_semantic = semantic_candidate;
    malformed_semantic.role_count = 0;
    expect_noncanonical_semantic(malformed_semantic);
    malformed_semantic = semantic_candidate;
    malformed_semantic.canonical_encoding_bytes = 0;
    expect_noncanonical_semantic(malformed_semantic);

    const auto expect_digest_mismatch = [&](SemanticCacheDependencyCandidate candidate)
    { EXPECT_EQ(compareSemanticCacheDependency(semantic, candidate), SemanticCacheDependencyComparison::DigestMismatch); };
    auto different_semantic = semantic_candidate;
    different_semantic.digest[0] ^= 1;
    expect_digest_mismatch(different_semantic);
    different_semantic = semantic_candidate;
    ++different_semantic.role_count;
    expect_digest_mismatch(different_semantic);
    different_semantic = semantic_candidate;
    ++different_semantic.canonical_encoding_bytes;
    expect_digest_mismatch(different_semantic);

    SemanticCacheDependencyCandidate unknown_state;
    unknown_state.state = static_cast<SemanticCacheDependencyCandidateState>(255);
    EXPECT_EQ(compareSemanticCacheDependency(semantic, unknown_state), SemanticCacheDependencyComparison::NonCanonical);
}

TEST(UDTSemanticGraph, CacheDependencyIsCanonicalAcrossSinkOrderAndDuplicateRoles)
{
    const auto first = descriptor(50);
    const auto second = descriptor(51);
    const std::array forward{first, second, first};
    const std::array reverse{second, first};
    const auto forward_image = cacheDependencyFor(forward);
    const auto reverse_image = cacheDependencyFor(reverse);

    ASSERT_EQ(forward_image.kind, SemanticCacheDependencyKind::Semantic);
    EXPECT_EQ(forward_image.kind, reverse_image.kind);
    EXPECT_EQ(forward_image.digest, reverse_image.digest);
    EXPECT_EQ(forward_image.role_count, 2);
    EXPECT_EQ(forward_image.role_count, reverse_image.role_count);
    EXPECT_EQ(forward_image.canonical_bytes, reverse_image.canonical_bytes);

    const auto encoded_role_bytes = [](const InstantiatedTypeDescriptor & role)
    {
        const auto & persisted = role.getPersistedDescriptor();
        const UInt64 arguments_bytes = persisted.getCanonicalArgumentsEncoding().size();
        const UInt64 shape_bytes = role.getPhysicalType()->getName().size();
        constexpr UInt64 fixed_role_bytes = 2 * sizeof(CanonicalUUID) + sizeof(UInt64) + 2 * sizeof(Digest);
        return fixed_role_bytes + varUIntBytes(arguments_bytes) + arguments_bytes + varUIntBytes(shape_bytes) + shape_bytes;
    };
    const UInt64 expected_bytes = sizeof(UInt16) + varUIntBytes(2) + encoded_role_bytes(*first) + encoded_role_bytes(*second);
    EXPECT_EQ(forward_image.canonical_bytes, expected_bytes);
}

TEST(UDTSemanticGraph, CacheDependencyRejectsEveryIndependentConfiguredLimit)
{
    const auto first = descriptor(60);
    const auto second = descriptor(61);
    const std::array roles{first, second};

    const auto expect_limit_error = [&](std::span<const InstantiatedTypeDescriptor::Ptr> selected_roles,
                                        const SemanticCacheDependencyDigestLimits & limits,
                                        SemanticCacheDependencyError::Code code)
    {
        ProspectiveResourceBudget budget(testResourceLimits());
        auto graph = QueryTreeSemanticRoleGraph::create(1, budget);
        SemanticNodeID source = 1;
        for (const auto & role : selected_roles)
            EXPECT_TRUE(graph->registerSink(contextualSink({.node = source++, .path = 0}, role)));
        graph->seal();
        auto planner = SemanticRolePlanner::create(*graph, budget);
        expectCacheDependencyError(code, [&] { planner->seal(limits); });
        EXPECT_FALSE(planner->isSealed());
    };

    SemanticCacheDependencyDigestLimits invalid;
    invalid.maximum_input_roles = 0;
    expect_limit_error(std::span{roles}.first(1), invalid, SemanticCacheDependencyError::Code::InvalidConfiguration);

    SemanticCacheDependencyDigestLimits input_roles;
    input_roles.maximum_input_roles = 1;
    input_roles.maximum_distinct_roles = 1;
    expect_limit_error(roles, input_roles, SemanticCacheDependencyError::Code::LimitExceeded);

    SemanticCacheDependencyDigestLimits distinct_roles;
    distinct_roles.maximum_input_roles = 2;
    distinct_roles.maximum_distinct_roles = 1;
    expect_limit_error(roles, distinct_roles, SemanticCacheDependencyError::Code::LimitExceeded);

    const UInt64 arguments_bytes = first->getPersistedDescriptor().getCanonicalArgumentsEncoding().size();
    const UInt64 shape_bytes = first->getPhysicalType()->getName().size();
    ASSERT_GT(arguments_bytes, 1);
    ASSERT_GT(shape_bytes, 1);

    SemanticCacheDependencyDigestLimits single_arguments;
    single_arguments.maximum_single_arguments_bytes = arguments_bytes - 1;
    expect_limit_error(std::span{roles}.first(1), single_arguments, SemanticCacheDependencyError::Code::LimitExceeded);

    SemanticCacheDependencyDigestLimits single_shape;
    single_shape.maximum_single_shape_bytes = shape_bytes - 1;
    expect_limit_error(std::span{roles}.first(1), single_shape, SemanticCacheDependencyError::Code::LimitExceeded);

    SemanticCacheDependencyDigestLimits input_variable;
    input_variable.maximum_single_arguments_bytes = arguments_bytes;
    input_variable.maximum_single_shape_bytes = shape_bytes;
    input_variable.maximum_input_variable_bytes = 2 * (arguments_bytes + shape_bytes) - 1;
    expect_limit_error(roles, input_variable, SemanticCacheDependencyError::Code::LimitExceeded);

    constexpr UInt64 fixed_role_bytes = 2 * sizeof(CanonicalUUID) + sizeof(UInt64) + 2 * sizeof(Digest);
    const UInt64 one_role_canonical_bytes = sizeof(UInt16) + varUIntBytes(1) + fixed_role_bytes + varUIntBytes(arguments_bytes)
        + arguments_bytes + varUIntBytes(shape_bytes) + shape_bytes;
    SemanticCacheDependencyDigestLimits canonical_encoding;
    canonical_encoding.maximum_single_arguments_bytes = arguments_bytes;
    canonical_encoding.maximum_single_shape_bytes = shape_bytes;
    canonical_encoding.maximum_canonical_encoding_bytes = one_role_canonical_bytes - 1;
    expect_limit_error(std::span{roles}.first(1), canonical_encoding, SemanticCacheDependencyError::Code::LimitExceeded);

    SemanticCacheDependencyDigestLimits scratch;
    scratch.maximum_scratch_bytes = static_cast<UInt64>(roles.size()) * 2 * sizeof(const void *) - 1;
    expect_limit_error(roles, scratch, SemanticCacheDependencyError::Code::LimitExceeded);
}

}
