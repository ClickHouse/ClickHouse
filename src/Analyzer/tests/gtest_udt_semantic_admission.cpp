#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/UDT/QueryAnalysisState.h>
#include <Analyzer/UDT/QueryTreeSemanticRoleGraph.h>
#include <Analyzer/UDT/SemanticSinkRegistry.h>
#include <Analyzer/UDT/SemanticTransferRegistry.h>

#include <Common/Exception.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/UDT/CanonicalHash.h>
#include <DataTypes/UDT/IAuthorityAdapter.h>
#include <DataTypes/UDT/InstantiatedTypeDescriptor.h>
#include <DataTypes/UDT/ResourceAccounting.h>
#include <DataTypes/UDT/ResourceLimits.h>
#include <DataTypes/UDT/TableColumnTypeBindings.h>
#include <DataTypes/UDT/TemplateChecker.h>

#include <Storages/StorageDummy.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageSnapshot.h>

#include <Functions/IFunction.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <limits>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace DB::ErrorCodes
{
extern const int LIMIT_EXCEEDED;
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
}

namespace DB::UDT
{
namespace
{

UUID testUUID(UInt64 high, UInt64 low)
{
    UUID result{};
    UUIDHelpers::getHighBytes(result) = high;
    UUIDHelpers::getLowBytes(result) = low;
    return result;
}

Definition::Ptr checkedDefinition(UInt64 type_id, SemanticCapabilityMask capabilities, bool nullable_physical_type = false)
{
    DefinitionInput input;
    input.identity = {
        .database_uuid = testUUID(0x550e8400e29b41d4ULL, 0xa716446655440000ULL),
        .type_uuid = testUUID(0x123456789abcdef0ULL, type_id),
        .revision = 1,
    };
    input.normalized_name = "semantic_admission.Type" + std::to_string(type_id);
    input.normalized_local_name = "Type" + std::to_string(type_id);
    TemplateNode root;
    root.kind = TemplateNodeKind::BuiltIn;
    if (nullable_physical_type)
    {
        root.atom = "Nullable";
        root.children = {{.reference = 1, .label = {}}};
        TemplateNode nested;
        nested.kind = TemplateNodeKind::BuiltIn;
        nested.atom = "UInt64";
        input.nodes = {std::move(root), std::move(nested)};
    }
    else
    {
        root.atom = "UInt64";
        input.nodes.push_back(std::move(root));
    }
    input.semantic_capabilities = capabilities;
    if (capabilities)
    {
        input.policy_bearing = true;
        input.policy_semantic_hash = hashDomainSeparated("ClickHouse UDT semantic admission test V1", input.normalized_name);
    }
    return TemplateChecker::checkAll({std::move(input)}).front();
}

InstantiatedTypeDescriptor::Ptr descriptor(UInt64 type_id, SemanticCapabilityMask capabilities, bool nullable_physical_type = false)
{
    DataTypePtr physical_type = std::make_shared<DataTypeUInt64>();
    if (nullable_physical_type)
        physical_type = std::make_shared<DataTypeNullable>(physical_type);
    return InstantiatedTypeDescriptor::create(
        checkedDefinition(type_id, capabilities, nullable_physical_type),
        CanonicalTypeArguments::validate({}, {}),
        std::move(physical_type));
}

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

FunctionNodePtr resolvedCastFunction(const DataTypePtr & result_type)
{
    auto input = std::make_shared<ConstantNode>(Field(UInt64{7}), std::make_shared<DataTypeUInt64>());
    auto type_argument = std::make_shared<ConstantNode>(Field(String{"UInt64"}), std::make_shared<DataTypeString>());
    return resolvedFunction("CAST", result_type, {std::move(input), std::move(type_argument)});
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

SemanticSink sinkFor(
    SemanticSinkKind kind,
    const InstantiatedTypeDescriptor::Ptr & logical_descriptor,
    SemanticCapabilityMask object_capabilities,
    SemanticCapabilityMask selected_capabilities)
{
    return {
        .source = {.node = 1, .path = 0},
        .kind = kind,
        .object_semantic_capabilities = object_capabilities,
        .selected_semantic_capabilities = selected_capabilities,
        .observes_identity = false,
        .expected_role = SemanticExpectedRole{
            .role = LogicalRoleInput::fromDescriptor(
                *logical_descriptor, logical_descriptor->getPersistedDescriptor().getCanonicalPhysicalType()),
            .retained_descriptor = logical_descriptor,
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
void expectLogicalError(Callback && callback, std::string_view message_fragment)
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    EXPECT_DEATH(callback(), String(message_fragment));
#else
    try
    {
        callback();
        FAIL() << "expected a logical error containing: " << message_fragment;
    }
    catch (const Exception & error)
    {
        EXPECT_EQ(error.code(), ErrorCodes::LOGICAL_ERROR) << error.message();
        EXPECT_NE(error.message().find(message_fragment), String::npos) << error.message();
    }
#endif
}

template <typename Callback>
void expectExceptionCode(int code, Callback && callback, std::string_view message_fragment)
{
    try
    {
        callback();
        FAIL() << "expected DB::Exception code " << code << " containing: " << message_fragment;
    }
    catch (const Exception & error)
    {
        EXPECT_EQ(error.code(), code) << error.message();
        EXPECT_NE(error.message().find(message_fragment), String::npos) << error.message();
    }
}

TypeAuthorityCapabilities authorityCapabilities()
{
    return {
        .adapter_abi = 1,
        .mask = typeAuthorityCapabilityBit(TypeAuthorityCapability::TransientResolution)
            | typeAuthorityCapabilityBit(TypeAuthorityCapability::Limits)
            | typeAuthorityCapabilityBit(TypeAuthorityCapability::Templates),
        .limits = {
            .maximum_definitions = 32,
            .maximum_definition_bytes = 1ULL << 20,
            .maximum_template_nodes = 4'096,
            .maximum_direct_dependencies = 256,
            .maximum_transitive_dependencies = 32,
            .maximum_checker_work = 65'536,
        },
    };
}

EffectiveResourceLimits testResourceLimits()
{
    return calculateEffectiveResourceLimits(std::span<const ResourceLimitLayer>{});
}

struct PreboundColumnFixture
{
    InstantiatedTypeDescriptor::Ptr logical_descriptor;
    DataTypePtr physical_type;
    BoundObjectTypeReferences::Ptr references;
    std::shared_ptr<StorageInMemoryMetadata> metadata;
    StoragePtr storage;
    StorageSnapshotPtr snapshot;
    TableNodePtr table;
    ColumnNodePtr column;
};

PreboundColumnFixture preboundColumnFromTree(
    UInt64 fixture_id,
    InstantiatedTypeDescriptor::Ptr primary_descriptor,
    DataTypePtr physical_type,
    BoundDeclaredTypeTree::Ptr tree,
    std::vector<Definition::Ptr> definitions)
{
    const SchemaObjectID object{
        .kind = SchemaObjectKind::Table,
        .database_uuid = primary_descriptor->getDefinition()->getIdentity().database_uuid,
        .object_uuid = testUUID(0x723456789abcdef0ULL, fixture_id),
    };
    std::vector<TableColumnTypeBindingInput> columns;
    columns.push_back({"value", BoundDeclaredTypeResult::withLogicalTree(std::move(tree))});
    auto prepared = prepareTableColumnTypeBindings(object, 7, columns);
    if (!prepared.persisted_references || !prepared.bound_physical_schema || !prepared.sidecar_expectation)
        throw std::logic_error("prebound-column fixture produced a partial logical binding package");

    const auto authority = makeTransientAuthorityAdapter(
        primary_descriptor->getDefinition()->getIdentity().database_uuid, authorityCapabilities(), std::move(definitions));
    auto references
        = BoundObjectTypeReferences::bind(*prepared.persisted_references, std::move(*prepared.bound_physical_schema), *authority);

    const StorageID storage_id{"semantic_admission", "source", object.object_uuid};
    auto storage = std::make_shared<StorageDummy>(storage_id, ColumnsDescription(prepared.physical_columns));
    auto metadata = std::make_shared<StorageInMemoryMetadata>();
    metadata->setColumns(ColumnsDescription(prepared.physical_columns));
    auto snapshot = std::make_shared<StorageSnapshot>(*storage, StorageMetadataPtr{metadata});
    /// Construct the snapshot while it is physical-only. Publishing the bound
    /// package afterwards lets this isolated fixture exercise analyzer logic
    /// without entering the database-owned mapped-read gate.
    metadata->setColumnsAndBoundUDTReferences(ColumnsDescription(prepared.physical_columns), references, *prepared.sidecar_expectation);
    auto table = std::make_shared<TableNode>(storage, storage_id, TableLockHolder{}, snapshot);
    auto column = std::make_shared<ColumnNode>(NameAndTypePair{"value", physical_type}, table);
    return {
        .logical_descriptor = std::move(primary_descriptor),
        .physical_type = std::move(physical_type),
        .references = std::move(references),
        .metadata = std::move(metadata),
        .storage = std::move(storage),
        .snapshot = std::move(snapshot),
        .table = std::move(table),
        .column = std::move(column),
    };
}

PreboundColumnFixture preboundColumn(UInt64 type_id, bool array_element_role)
{
    const auto capabilities = semanticCapabilityBit(SemanticCapability::Input) | semanticCapabilityBit(SemanticCapability::ValueChecks)
        | semanticCapabilityBit(SemanticCapability::Default);
    auto logical_descriptor = descriptor(type_id, capabilities);
    DataTypePtr physical_type = logical_descriptor->getPhysicalType();
    BoundDeclaredTypeTree::Ptr tree;
    if (array_element_role)
    {
        physical_type = std::make_shared<DataTypeArray>(physical_type);
        tree = BoundDeclaredTypeTree::build(
            {
                {.type_child_ordinals = {}, .physical_type = physical_type},
                {.type_child_ordinals = {0}, .physical_type = logical_descriptor->getPhysicalType()},
            },
            {{.type_child_ordinals = {0}, .logical_descriptor = logical_descriptor, .logical_preorder = 0}},
            {});
    }
    else
        tree = directTree(logical_descriptor);

    return preboundColumnFromTree(
        type_id, logical_descriptor, std::move(physical_type), std::move(tree), {logical_descriptor->getDefinition()});
}

BoundObjectTypeReferences::Ptr storedCastReferences(
    UInt64 fixture_id,
    const InstantiatedTypeDescriptor::Ptr & logical_descriptor,
    std::vector<PersistedTypeOccurrencePath> paths,
    SemanticCapabilityMask selected_capabilities)
{
    PersistedTypeReferences references;
    references.format_version = persisted_type_references_format_version_v2;
    references.path_dictionary_version = persisted_type_path_dictionary_version_v2;
    references.object = {
        .kind = SchemaObjectKind::View,
        .database_uuid = logical_descriptor->getDefinition()->getIdentity().database_uuid,
        .object_uuid = testUUID(0x733456789abcdef0ULL, fixture_id),
    };
    references.object_schema_revision = 9;
    references.physical_schema_fingerprint = hashDomainSeparated("ClickHouse UDT stored CAST fixture V1", std::to_string(fixture_id));
    references.descriptors = {logical_descriptor->getPersistedDescriptor()};
    references.occurrence_paths = paths;

    BoundObjectPhysicalSchema physical_schema{
        .object = references.object,
        .object_schema_revision = references.object_schema_revision,
        .physical_schema_fingerprint = references.physical_schema_fingerprint,
        .occurrences = {},
    };
    references.uses.reserve(paths.size());
    physical_schema.occurrences.reserve(paths.size());
    for (size_t index = 0; index < paths.size(); ++index)
    {
        references.uses.push_back({.path_id = static_cast<UInt64>(index), .descriptor_id = 0});
        physical_schema.occurrences.push_back({
            .path = paths[index],
            .physical_type = logical_descriptor->getPhysicalType(),
            .runtime_owner_key = "stored-expression:" + std::to_string(paths[index].object_ordinal),
            .selected_semantic_capabilities = selected_capabilities,
        });
    }

    const auto authority = makeTransientAuthorityAdapter(
        logical_descriptor->getDefinition()->getIdentity().database_uuid, authorityCapabilities(), {logical_descriptor->getDefinition()});
    return BoundObjectTypeReferences::bind(references, std::move(physical_schema), *authority);
}

PersistedTypeOccurrencePath storedCastPath(UInt64 stored_expression_ordinal, UInt64 occurrence_ordinal, std::vector<UInt64> child_path)
{
    return {
        .section = PersistedTypePathSection::ViewExpression,
        .site = PersistedTypeOccurrenceSite::StoredExpression,
        .object_ordinal = stored_expression_ordinal,
        .occurrence_ordinal = occurrence_ordinal,
        .type_child_ordinals = std::move(child_path),
    };
}

ConstantNodePtr contextualLiteral(SemanticSinkKind kind)
{
    if (kind == SemanticSinkKind::HasAnyConstant)
    {
        return std::make_shared<ConstantNode>(
            Field(Array{Field(UInt64{7}), Field(UInt64{8})}), std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()));
    }
    return std::make_shared<ConstantNode>(Field(UInt64{7}), std::make_shared<DataTypeUInt64>());
}

}

TEST(UDTSemanticAdmission, ClosedSinkRegistryExposesOnlyFullyWiredKinds)
{
    using Underlying = std::underlying_type_t<SemanticSinkKind>;
    const std::array exposed{
        SemanticSinkKind::ExplicitUDTCast,
        SemanticSinkKind::EqualityConstant,
        SemanticSinkKind::InConstant,
        SemanticSinkKind::GlobalInConstant,
        SemanticSinkKind::HasConstant,
        SemanticSinkKind::HasAnyConstant,
    };

    for (Underlying raw = 0; raw < static_cast<Underlying>(SemanticSinkKind::Count); ++raw)
    {
        const auto kind = static_cast<SemanticSinkKind>(raw);
        const bool expected = std::ranges::find(exposed, kind) != exposed.end();
        EXPECT_EQ(SemanticSinkRegistry::find(kind) != nullptr, expected) << static_cast<UInt64>(raw);
        if (expected)
        {
            const auto * descriptor = SemanticSinkRegistry::find(kind);
            ASSERT_NE(descriptor, nullptr);
            EXPECT_TRUE(descriptor->requires_expected_role);
            EXPECT_TRUE(descriptor->allows_expected_role);
            EXPECT_FALSE(descriptor->allows_identity_observation);
        }
    }
    EXPECT_EQ(SemanticSinkRegistry::find(SemanticSinkKind::Count), nullptr);
    EXPECT_EQ(SemanticSinkRegistry::find(static_cast<SemanticSinkKind>(255)), nullptr);
}

TEST(UDTSemanticAdmission, CapabilitySubsetsAndStackedPolicyRolesFailClosed)
{
    const auto input = semanticCapabilityBit(SemanticCapability::Input);
    const auto output = semanticCapabilityBit(SemanticCapability::Output);
    const auto value_checks = semanticCapabilityBit(SemanticCapability::ValueChecks);
    const auto defaults = semanticCapabilityBit(SemanticCapability::Default);
    const auto logical_descriptor = descriptor(1, input | value_checks | defaults);

    auto candidate = sinkFor(SemanticSinkKind::ExplicitUDTCast, logical_descriptor, input | value_checks | defaults, input);
    EXPECT_TRUE(SemanticSinkRegistry::isEligible(candidate));

    candidate.selected_semantic_capabilities = input | value_checks;
    EXPECT_TRUE(SemanticSinkRegistry::isEligible(candidate));
    candidate.selected_semantic_capabilities = defaults;
    EXPECT_TRUE(SemanticSinkRegistry::isEligible(candidate));

    candidate.kind = SemanticSinkKind::EqualityConstant;
    EXPECT_FALSE(SemanticSinkRegistry::isEligible(candidate));
    candidate.selected_semantic_capabilities = value_checks;
    EXPECT_TRUE(SemanticSinkRegistry::isEligible(candidate));

    candidate.object_semantic_capabilities = input;
    candidate.selected_semantic_capabilities = input | value_checks;
    EXPECT_FALSE(SemanticSinkRegistry::isEligible(candidate));

    candidate.object_semantic_capabilities = all_semantic_capabilities;
    candidate.selected_semantic_capabilities = output;
    EXPECT_FALSE(SemanticSinkRegistry::isEligible(candidate));

    candidate.selected_semantic_capabilities = input;
    candidate.object_semantic_capabilities = static_cast<SemanticCapabilityMask>(all_semantic_capabilities | 0x80);
    EXPECT_FALSE(SemanticSinkRegistry::isEligible(candidate));
    candidate.object_semantic_capabilities = all_semantic_capabilities;
    candidate.selected_semantic_capabilities = static_cast<SemanticCapabilityMask>(input | 0x80);
    EXPECT_FALSE(SemanticSinkRegistry::isEligible(candidate));

    candidate.selected_semantic_capabilities = input;
    candidate.observes_identity = true;
    EXPECT_FALSE(SemanticSinkRegistry::isEligible(candidate));
    candidate.observes_identity = false;
    candidate.expected_role.reset();
    EXPECT_FALSE(SemanticSinkRegistry::isEligible(candidate));

    candidate = sinkFor(SemanticSinkKind::ExplicitUDTCast, logical_descriptor, input, input);
    candidate.expected_role->retained_descriptor = descriptor(2, input);
    EXPECT_FALSE(SemanticSinkRegistry::isEligible(candidate));
}

TEST(UDTSemanticAdmission, DirectCastTargetRequiresOneRootLogicalOccurrence)
{
    const auto input = semanticCapabilityBit(SemanticCapability::Input);
    const auto defaults = semanticCapabilityBit(SemanticCapability::Default);
    const auto logical_descriptor = descriptor(10, input | defaults);
    const auto direct = directTree(logical_descriptor);

    const auto target = QueryTreeSemanticRoleGraph::inspectDirectExplicitCastTarget(*direct);
    ASSERT_TRUE(target.retained_descriptor);
    EXPECT_EQ(target.selected_semantic_capabilities, input | defaults);
    EXPECT_TRUE(target.role.descriptor->hasSameInstantiation(logical_descriptor->getPersistedDescriptor()));
    EXPECT_EQ(target.role.shape.canonical_encoding, "UInt64");

    std::vector<BoundDeclaredTypeNodeInput> stacked_nodes{
        {.type_child_ordinals = {}, .physical_type = logical_descriptor->getPhysicalType()},
    };
    std::vector<BoundDeclaredTypeOccurrenceInput> stacked_occurrences{
        {.type_child_ordinals = {}, .logical_descriptor = logical_descriptor, .logical_preorder = 0},
        {.type_child_ordinals = {}, .logical_descriptor = logical_descriptor, .logical_preorder = 1},
    };
    const auto stacked = BoundDeclaredTypeTree::build(std::move(stacked_nodes), std::move(stacked_occurrences), {});
    expectGraphError(
        QueryTreeSemanticRoleGraphError::Code::UnsupportedCastShape,
        [&] { static_cast<void>(QueryTreeSemanticRoleGraph::inspectDirectExplicitCastTarget(*stacked)); });

    const auto array_type = std::make_shared<DataTypeArray>(logical_descriptor->getPhysicalType());
    std::vector<BoundDeclaredTypeNodeInput> nested_nodes{
        {.type_child_ordinals = {}, .physical_type = array_type},
        {.type_child_ordinals = {0}, .physical_type = logical_descriptor->getPhysicalType()},
    };
    std::vector<BoundDeclaredTypeOccurrenceInput> nested_occurrences{
        {.type_child_ordinals = {0}, .logical_descriptor = logical_descriptor, .logical_preorder = 0},
    };
    const auto nested = BoundDeclaredTypeTree::build(std::move(nested_nodes), std::move(nested_occurrences), {});
    expectGraphError(
        QueryTreeSemanticRoleGraphError::Code::UnsupportedCastShape,
        [&] { static_cast<void>(QueryTreeSemanticRoleGraph::inspectDirectExplicitCastTarget(*nested)); });
}

TEST(UDTSemanticAdmission, ResolvedDirectCastRegistersOnceAndFinalizes)
{
    const auto capabilities = semanticCapabilityBit(SemanticCapability::Input) | semanticCapabilityBit(SemanticCapability::ValueChecks)
        | semanticCapabilityBit(SemanticCapability::Default);
    const auto logical_descriptor = descriptor(30, capabilities);
    const auto target = directTree(logical_descriptor);
    const auto function = resolvedCastFunction(logical_descriptor->getPhysicalType());
    const auto authority_limits = authorityCapabilities().limits;
    const auto exact_limits = testResourceLimits();

    QueryAnalysisState state;
    EXPECT_TRUE(state.isDirectExplicitCastEligible(*target));
    ASSERT_TRUE(state.registerResolvedDirectExplicitCast(function, target, authority_limits, &exact_limits));
    EXPECT_TRUE(state.hasSemanticResourceBudget());
    EXPECT_TRUE(state.hasQueryTreeRegistrations());
    ASSERT_NE(state.semantic_role_graph, nullptr);
    EXPECT_EQ(state.next_semantic_node_id, 2);

    /// Re-registering the same resolved node is an idempotent analyzer callback,
    /// not a second semantic boundary.
    EXPECT_TRUE(state.registerResolvedDirectExplicitCast(function, target, authority_limits, &exact_limits));
    EXPECT_TRUE(state.hasQueryTreeRegistrations());
    EXPECT_EQ(state.next_semantic_node_id, 2);

    EXPECT_NO_THROW(state.finalizeSemanticAnalysis());
    EXPECT_TRUE(state.semantic_analysis_finalized);
    EXPECT_FALSE(state.hasQueryTreeRegistrations());
    EXPECT_EQ(state.semantic_role_graph, nullptr);
    EXPECT_EQ(state.semantic_role_planner, nullptr);
    EXPECT_NO_THROW(state.finalizeSemanticAnalysis());
    expectLogicalError(
        [&] { static_cast<void>(state.registerResolvedDirectExplicitCast(function, target, authority_limits, &exact_limits)); },
        "UDT semantic CAST reached analysis after the semantic graph was sealed");
}

TEST(UDTSemanticAdmission, ResolvedDirectCastFastNegativesDoNotAllocate)
{
    const auto authority_limits = authorityCapabilities().limits;
    for (const auto capabilities : {SemanticCapabilityMask{0}, semanticCapabilityBit(SemanticCapability::Output)})
    {
        SCOPED_TRACE(static_cast<UInt64>(capabilities));
        const auto logical_descriptor = descriptor(31 + capabilities, capabilities);
        const auto target = directTree(logical_descriptor);
        const auto function = resolvedCastFunction(logical_descriptor->getPhysicalType());

        QueryAnalysisState state;
        EXPECT_FALSE(state.isDirectExplicitCastEligible(*target));
        EXPECT_FALSE(state.registerResolvedDirectExplicitCast(function, target, authority_limits));
        EXPECT_FALSE(state.hasSemanticResourceBudget());
        EXPECT_FALSE(state.hasQueryTreeRegistrations());
        EXPECT_EQ(state.resource_ledger, nullptr);
        EXPECT_EQ(state.semantic_role_graph, nullptr);
    }

    /// The capability fast negative precedes direct-root shape validation.
    /// Nested and stacked output-only aliases therefore remain physical.
    const auto output_descriptor = descriptor(35, semanticCapabilityBit(SemanticCapability::Output));
    const auto array_type = std::make_shared<DataTypeArray>(output_descriptor->getPhysicalType());
    const auto nested_output = BoundDeclaredTypeTree::build(
        {
            {.type_child_ordinals = {}, .physical_type = array_type},
            {.type_child_ordinals = {0}, .physical_type = output_descriptor->getPhysicalType()},
        },
        {{.type_child_ordinals = {0}, .logical_descriptor = output_descriptor, .logical_preorder = 0}},
        {});
    const auto stacked_output = BoundDeclaredTypeTree::build(
        {{.type_child_ordinals = {}, .physical_type = output_descriptor->getPhysicalType()}},
        {
            {.type_child_ordinals = {}, .logical_descriptor = output_descriptor, .logical_preorder = 0},
            {.type_child_ordinals = {}, .logical_descriptor = output_descriptor, .logical_preorder = 1},
        },
        {});
    QueryAnalysisState output_state;
    EXPECT_FALSE(output_state.isDirectExplicitCastEligible(*nested_output));
    EXPECT_FALSE(output_state.isDirectExplicitCastEligible(*stacked_output));
    EXPECT_FALSE(output_state.registerResolvedDirectExplicitCast(resolvedCastFunction(array_type), nested_output, authority_limits));
    EXPECT_FALSE(output_state.registerResolvedDirectExplicitCast(
        resolvedCastFunction(output_descriptor->getPhysicalType()), stacked_output, authority_limits));
    EXPECT_FALSE(output_state.hasSemanticResourceBudget());
    EXPECT_FALSE(output_state.hasQueryTreeRegistrations());
}

TEST(UDTSemanticAdmission, ResolvedDirectCastRejectsIncompleteAndUnsupportedTargets)
{
    const auto capabilities = semanticCapabilityBit(SemanticCapability::Input) | semanticCapabilityBit(SemanticCapability::ValueChecks);
    const auto logical_descriptor = descriptor(40, capabilities);
    const auto direct = directTree(logical_descriptor);
    const auto function = resolvedCastFunction(logical_descriptor->getPhysicalType());
    const auto authority_limits = authorityCapabilities().limits;

    QueryAnalysisState incomplete_state;
    expectLogicalError(
        [&] { static_cast<void>(incomplete_state.registerResolvedDirectExplicitCast(FunctionNodePtr{}, direct, authority_limits)); },
        "UDT semantic CAST registration has no resolved function or exact target");
    expectLogicalError(
        [&]
        {
            static_cast<void>(
                incomplete_state.registerResolvedDirectExplicitCast(function, BoundDeclaredTypeTree::Ptr{}, authority_limits));
        },
        "UDT semantic CAST registration has no resolved function or exact target");
    EXPECT_FALSE(incomplete_state.hasSemanticResourceBudget());

    QueryAnalysisState malformed_state;
    const auto malformed_function = std::make_shared<FunctionNode>("CAST");
    expectLogicalError(
        [&] { static_cast<void>(malformed_state.registerResolvedDirectExplicitCast(malformed_function, direct, authority_limits)); },
        "Resolved UDT semantic CAST has an invalid argument list");
    EXPECT_FALSE(malformed_state.hasQueryTreeRegistrations());

    const auto array_type = std::make_shared<DataTypeArray>(logical_descriptor->getPhysicalType());
    const auto nested = BoundDeclaredTypeTree::build(
        {
            {.type_child_ordinals = {}, .physical_type = array_type},
            {.type_child_ordinals = {0}, .physical_type = logical_descriptor->getPhysicalType()},
        },
        {{.type_child_ordinals = {0}, .logical_descriptor = logical_descriptor, .logical_preorder = 0}},
        {});
    QueryAnalysisState nested_state;
    expectExceptionCode(
        ErrorCodes::NOT_IMPLEMENTED,
        [&] { static_cast<void>(nested_state.registerResolvedDirectExplicitCast(function, nested, authority_limits)); },
        "requires exactly one logical occurrence");
    EXPECT_FALSE(nested_state.hasSemanticResourceBudget());

    const auto stacked = BoundDeclaredTypeTree::build(
        {{.type_child_ordinals = {}, .physical_type = logical_descriptor->getPhysicalType()}},
        {
            {.type_child_ordinals = {}, .logical_descriptor = logical_descriptor, .logical_preorder = 0},
            {.type_child_ordinals = {}, .logical_descriptor = logical_descriptor, .logical_preorder = 1},
        },
        {});
    QueryAnalysisState stacked_state;
    expectExceptionCode(
        ErrorCodes::NOT_IMPLEMENTED,
        [&] { static_cast<void>(stacked_state.registerResolvedDirectExplicitCast(function, stacked, authority_limits)); },
        "requires exactly one logical occurrence");
    EXPECT_FALSE(stacked_state.hasSemanticResourceBudget());

    QueryAnalysisState finalized_state;
    finalized_state.finalizeSemanticAnalysis();
    expectLogicalError(
        [&] { static_cast<void>(finalized_state.registerResolvedDirectExplicitCast(function, direct, authority_limits)); },
        "UDT semantic CAST reached analysis after the semantic graph was sealed");

    const auto output_descriptor = descriptor(41, semanticCapabilityBit(SemanticCapability::Output));
    const auto output_target = directTree(output_descriptor);
    EXPECT_FALSE(finalized_state.registerResolvedDirectExplicitCast(
        resolvedCastFunction(output_descriptor->getPhysicalType()), output_target, authority_limits));
    EXPECT_FALSE(finalized_state.hasSemanticResourceBudget());
}

TEST(UDTSemanticAdmission, PreboundStoredCastSelectsExactDirectRoot)
{
    const auto capabilities = semanticCapabilityBit(SemanticCapability::Input) | semanticCapabilityBit(SemanticCapability::ValueChecks);
    const auto logical_descriptor = descriptor(50, capabilities);
    const auto references = storedCastReferences(50, logical_descriptor, {storedCastPath(3, 0, {})}, capabilities);

    const auto target = QueryAnalysisState::inspectPreboundStoredExplicitCast(references, 3);
    ASSERT_TRUE(target);
    EXPECT_EQ(target->getNodeCount(), 1);
    EXPECT_EQ(target->getOccurrenceCount(), 1);
    EXPECT_EQ(target->getSemanticCapabilities(), capabilities);
    EXPECT_TRUE(target->getPhysicalType()->equals(*logical_descriptor->getPhysicalType()));
    ASSERT_EQ(target->getDescriptors().size(), 1);
    EXPECT_TRUE(
        target->getDescriptors().front()->getPersistedDescriptor().hasSameInstantiation(logical_descriptor->getPersistedDescriptor()));

    EXPECT_EQ(QueryAnalysisState::inspectPreboundStoredExplicitCast(references, 4), nullptr);
    EXPECT_EQ(QueryAnalysisState::inspectPreboundStoredExplicitCast({}, 3), nullptr);

    const auto physical_only_references = storedCastReferences(51, logical_descriptor, {storedCastPath(3, 0, {})}, 0);
    EXPECT_EQ(QueryAnalysisState::inspectPreboundStoredExplicitCast(physical_only_references, 3), nullptr);
    const auto physical_only_nested_references = storedCastReferences(52, logical_descriptor, {storedCastPath(3, 0, {0})}, 0);
    EXPECT_EQ(QueryAnalysisState::inspectPreboundStoredExplicitCast(physical_only_nested_references, 3), nullptr);

    const auto output_descriptor = descriptor(51, semanticCapabilityBit(SemanticCapability::Output));
    const auto output_references
        = storedCastReferences(53, output_descriptor, {storedCastPath(3, 0, {})}, semanticCapabilityBit(SemanticCapability::Output));
    EXPECT_EQ(QueryAnalysisState::inspectPreboundStoredExplicitCast(output_references, 3), nullptr);
}

TEST(UDTSemanticAdmission, PreboundStoredCastRejectsMalformedNestedAndStackedUses)
{
    const auto capabilities = semanticCapabilityBit(SemanticCapability::Input) | semanticCapabilityBit(SemanticCapability::ValueChecks);
    const auto logical_descriptor = descriptor(60, capabilities);

    const auto table_fixture = preboundColumn(60, false);
    expectLogicalError(
        [&] { static_cast<void>(QueryAnalysisState::inspectPreboundStoredExplicitCast(table_fixture.references, 0)); },
        "A stored UDT CAST has no trusted bound View identity");

    const auto nested_references = storedCastReferences(61, logical_descriptor, {storedCastPath(3, 0, {0})}, capabilities);
    expectExceptionCode(
        ErrorCodes::NOT_IMPLEMENTED,
        [&] { static_cast<void>(QueryAnalysisState::inspectPreboundStoredExplicitCast(nested_references, 3)); },
        "exactly one direct-root logical occurrence");

    const auto wrong_occurrence_references = storedCastReferences(62, logical_descriptor, {storedCastPath(3, 1, {})}, capabilities);
    expectExceptionCode(
        ErrorCodes::NOT_IMPLEMENTED,
        [&] { static_cast<void>(QueryAnalysisState::inspectPreboundStoredExplicitCast(wrong_occurrence_references, 3)); },
        "exactly one direct-root logical occurrence");

    const auto stacked_references
        = storedCastReferences(63, logical_descriptor, {storedCastPath(3, 0, {}), storedCastPath(3, 1, {})}, capabilities);
    expectExceptionCode(
        ErrorCodes::NOT_IMPLEMENTED,
        [&] { static_cast<void>(QueryAnalysisState::inspectPreboundStoredExplicitCast(stacked_references, 3)); },
        "exactly one direct-root logical occurrence");
}

TEST(UDTSemanticAdmission, TransferRegistryPinsAmbiguityAndOuterJoinNullContracts)
{
    using Underlying = std::underlying_type_t<SemanticTransferKind>;
    EXPECT_EQ(SemanticTransferRegistry::find(SemanticTransferKind::Unregistered), nullptr);
    EXPECT_EQ(SemanticTransferRegistry::find(SemanticTransferKind::Count), nullptr);
    EXPECT_EQ(SemanticTransferRegistry::find(static_cast<SemanticTransferKind>(255)), nullptr);
    for (Underlying raw = static_cast<Underlying>(SemanticTransferKind::Identity);
         raw < static_cast<Underlying>(SemanticTransferKind::Count);
         ++raw)
    {
        const auto kind = static_cast<SemanticTransferKind>(raw);
        const auto * transfer = SemanticTransferRegistry::find(kind);
        ASSERT_NE(transfer, nullptr) << static_cast<UInt64>(raw);
        EXPECT_EQ(transfer->kind, kind);
        EXPECT_GE(transfer->maximum_inputs, transfer->minimum_inputs);
        EXPECT_GT(transfer->minimum_inputs, 0);
    }

    const auto * left_or_right_preserved = SemanticTransferRegistry::find(SemanticTransferKind::JoinDirectNonSynthesizing);
    ASSERT_NE(left_or_right_preserved, nullptr);
    EXPECT_EQ(left_or_right_preserved->policy, SemanticTransferPolicy::PreserveUnary);
    EXPECT_EQ(left_or_right_preserved->null_contract, SemanticNullContract::None);

    const auto * left_or_right_nullable = SemanticTransferRegistry::find(SemanticTransferKind::JoinDirectNullableLift);
    ASSERT_NE(left_or_right_nullable, nullptr);
    EXPECT_EQ(left_or_right_nullable->policy, SemanticTransferPolicy::ReshapeUnary);
    EXPECT_TRUE(left_or_right_nullable->requires_result_shape);
    EXPECT_EQ(left_or_right_nullable->null_contract, SemanticNullContract::OuterNullMapBypassesSemanticPrograms);

    for (const auto kind : {
             SemanticTransferKind::UnanimousBranch,
             SemanticTransferKind::UnanimousUnion,
             SemanticTransferKind::JoinUsingUnanimous,
         })
    {
        const auto * transfer = SemanticTransferRegistry::find(kind);
        ASSERT_NE(transfer, nullptr);
        EXPECT_EQ(transfer->policy, SemanticTransferPolicy::MeetUnanimous);
    }
    EXPECT_EQ(SemanticTransferRegistry::find(SemanticTransferKind::JoinUsingUnanimous)->minimum_inputs, 2);
}

TEST(UDTSemanticAdmission, PhysicalEqualityNeverCreatesLogicalIdentity)
{
    const auto input = semanticCapabilityBit(SemanticCapability::Input);
    const auto first = descriptor(20, input);
    const auto second = descriptor(21, input);
    ASSERT_TRUE(first->getPhysicalType()->equals(*second->getPhysicalType()));

    const auto first_role = LogicalRoleInput::fromDescriptor(*first, first->getPersistedDescriptor().getCanonicalPhysicalType());
    const auto second_role = LogicalRoleInput::fromDescriptor(*second, second->getPersistedDescriptor().getCanonicalPhysicalType());
    EXPECT_NE(first_role.descriptor, second_role.descriptor);
    EXPECT_FALSE(first_role.descriptor->hasSameInstantiation(*second_role.descriptor));
}

TEST(UDTSemanticAdmission, PreboundContextualShapesSelectOnlyTheirExactDeclarationUse)
{
    const std::array direct_kinds{
        SemanticSinkKind::EqualityConstant,
        SemanticSinkKind::InConstant,
        SemanticSinkKind::GlobalInConstant,
    };
    UInt64 type_id = 100;
    for (const auto kind : direct_kinds)
    {
        SCOPED_TRACE(static_cast<UInt64>(kind));
        const auto fixture = preboundColumn(type_id++, false);
        const auto candidate = QueryAnalysisState::inspectPreboundContextualConstant(*fixture.column, kind);
        ASSERT_TRUE(candidate);
        EXPECT_EQ(candidate->references, fixture.references);
        EXPECT_EQ(candidate->use_path.section, PersistedTypePathSection::ColumnType);
        EXPECT_EQ(candidate->use_path.site, PersistedTypeOccurrenceSite::Declaration);
        EXPECT_EQ(candidate->use_path.object_ordinal, 0);
        EXPECT_EQ(candidate->use_path.occurrence_ordinal, 0);
        EXPECT_TRUE(candidate->use_path.type_child_ordinals.empty());
        EXPECT_FALSE(candidate->effective_query_limits.has_value());
        EXPECT_FALSE(QueryAnalysisState::inspectPreboundContextualConstant(*fixture.column, SemanticSinkKind::HasConstant));
        EXPECT_FALSE(QueryAnalysisState::inspectPreboundContextualConstant(*fixture.column, SemanticSinkKind::HasAnyConstant));
    }

    const std::array array_kinds{SemanticSinkKind::HasConstant, SemanticSinkKind::HasAnyConstant};
    for (const auto kind : array_kinds)
    {
        SCOPED_TRACE(static_cast<UInt64>(kind));
        const auto fixture = preboundColumn(type_id++, true);
        const auto candidate = QueryAnalysisState::inspectPreboundContextualConstant(*fixture.column, kind);
        ASSERT_TRUE(candidate);
        EXPECT_EQ(candidate->references, fixture.references);
        EXPECT_EQ(candidate->use_path.section, PersistedTypePathSection::ColumnType);
        EXPECT_EQ(candidate->use_path.site, PersistedTypeOccurrenceSite::Declaration);
        EXPECT_EQ(candidate->use_path.object_ordinal, 0);
        EXPECT_EQ(candidate->use_path.occurrence_ordinal, 0);
        EXPECT_EQ(candidate->use_path.type_child_ordinals, std::vector<UInt64>({0}));
        EXPECT_FALSE(candidate->effective_query_limits.has_value());
        EXPECT_FALSE(QueryAnalysisState::inspectPreboundContextualConstant(*fixture.column, SemanticSinkKind::InConstant));
        EXPECT_FALSE(QueryAnalysisState::inspectPreboundContextualConstant(*fixture.column, SemanticSinkKind::GlobalInConstant));
    }
}

TEST(UDTSemanticAdmission, ContextualWrappersAndStaticPrefixesSelectExactRoles)
{
    const auto capabilities = semanticCapabilityBit(SemanticCapability::Input) | semanticCapabilityBit(SemanticCapability::ValueChecks);

    const auto nullable_descriptor = descriptor(200, capabilities);
    const DataTypePtr nullable_type = std::make_shared<DataTypeNullable>(nullable_descriptor->getPhysicalType());
    const auto nullable_tree = BoundDeclaredTypeTree::build(
        {
            {.type_child_ordinals = {}, .physical_type = nullable_type},
            {.type_child_ordinals = {0}, .physical_type = nullable_descriptor->getPhysicalType()},
        },
        {{.type_child_ordinals = {0}, .logical_descriptor = nullable_descriptor, .logical_preorder = 0}},
        {});
    const auto nullable_fixture
        = preboundColumnFromTree(200, nullable_descriptor, nullable_type, nullable_tree, {nullable_descriptor->getDefinition()});
    const auto nullable_candidate
        = QueryAnalysisState::inspectPreboundContextualConstant(*nullable_fixture.column, SemanticSinkKind::EqualityConstant);
    ASSERT_TRUE(nullable_candidate);
    EXPECT_EQ(nullable_candidate->use_path.type_child_ordinals, std::vector<UInt64>({0}));
    const std::array<UInt64, 1> nullable_prefix{0};
    const auto nullable_prefixed_candidate
        = QueryAnalysisState::inspectPreboundContextualConstant(*nullable_fixture.column, SemanticSinkKind::InConstant, nullable_prefix);
    ASSERT_TRUE(nullable_prefixed_candidate);
    EXPECT_EQ(nullable_prefixed_candidate->use_path.type_child_ordinals, std::vector<UInt64>({0}));

    const auto array_descriptor = descriptor(201, capabilities);
    const DataTypePtr array_element_type = std::make_shared<DataTypeNullable>(array_descriptor->getPhysicalType());
    const DataTypePtr array_type = std::make_shared<DataTypeArray>(array_element_type);
    const auto array_tree = BoundDeclaredTypeTree::build(
        {
            {.type_child_ordinals = {}, .physical_type = array_type},
            {.type_child_ordinals = {0}, .physical_type = array_element_type},
            {.type_child_ordinals = {0, 0}, .physical_type = array_descriptor->getPhysicalType()},
        },
        {{.type_child_ordinals = {0, 0}, .logical_descriptor = array_descriptor, .logical_preorder = 0}},
        {});
    const auto array_fixture = preboundColumnFromTree(201, array_descriptor, array_type, array_tree, {array_descriptor->getDefinition()});
    for (const auto kind : {SemanticSinkKind::HasConstant, SemanticSinkKind::HasAnyConstant})
    {
        SCOPED_TRACE(static_cast<UInt64>(kind));
        const auto candidate = QueryAnalysisState::inspectPreboundContextualConstant(*array_fixture.column, kind);
        ASSERT_TRUE(candidate);
        EXPECT_EQ(candidate->use_path.type_child_ordinals, std::vector<UInt64>({0, 0}));
    }
    const std::array<UInt64, 1> array_prefix{0};
    const auto array_prefixed_candidate
        = QueryAnalysisState::inspectPreboundContextualConstant(*array_fixture.column, SemanticSinkKind::GlobalInConstant, array_prefix);
    ASSERT_TRUE(array_prefixed_candidate);
    EXPECT_EQ(array_prefixed_candidate->use_path.type_child_ordinals, std::vector<UInt64>({0, 0}));

    const auto low_cardinality_descriptor = descriptor(202, capabilities);
    const DataTypePtr low_cardinality_type = std::make_shared<DataTypeLowCardinality>(low_cardinality_descriptor->getPhysicalType());
    const auto low_cardinality_tree = BoundDeclaredTypeTree::build(
        {
            {.type_child_ordinals = {}, .physical_type = low_cardinality_type},
            {.type_child_ordinals = {0}, .physical_type = low_cardinality_descriptor->getPhysicalType()},
        },
        {{.type_child_ordinals = {0}, .logical_descriptor = low_cardinality_descriptor, .logical_preorder = 0}},
        {});
    const auto low_cardinality_fixture = preboundColumnFromTree(
        202, low_cardinality_descriptor, low_cardinality_type, low_cardinality_tree, {low_cardinality_descriptor->getDefinition()});
    const auto low_cardinality_candidate
        = QueryAnalysisState::inspectPreboundContextualConstant(*low_cardinality_fixture.column, SemanticSinkKind::EqualityConstant);
    ASSERT_TRUE(low_cardinality_candidate);
    EXPECT_EQ(low_cardinality_candidate->use_path.type_child_ordinals, std::vector<UInt64>({0}));
    const std::array<UInt64, 1> low_cardinality_prefix{0};
    const auto low_cardinality_prefixed_candidate = QueryAnalysisState::inspectPreboundContextualConstant(
        *low_cardinality_fixture.column, SemanticSinkKind::EqualityConstant, low_cardinality_prefix);
    ASSERT_TRUE(low_cardinality_prefixed_candidate);
    EXPECT_EQ(low_cardinality_prefixed_candidate->use_path.type_child_ordinals, std::vector<UInt64>({0}));

    const auto tuple_descriptor = descriptor(203, capabilities);
    const DataTypePtr tuple_type
        = std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeString>(), tuple_descriptor->getPhysicalType()});
    const auto tuple_tree = BoundDeclaredTypeTree::build(
        {
            {.type_child_ordinals = {}, .physical_type = tuple_type},
            {.type_child_ordinals = {1}, .physical_type = tuple_descriptor->getPhysicalType()},
        },
        {{.type_child_ordinals = {1}, .logical_descriptor = tuple_descriptor, .logical_preorder = 0}},
        {});
    const auto tuple_fixture = preboundColumnFromTree(203, tuple_descriptor, tuple_type, tuple_tree, {tuple_descriptor->getDefinition()});
    const std::array<UInt64, 1> tuple_role_prefix{1};
    const auto tuple_candidate = QueryAnalysisState::inspectPreboundContextualConstant(
        *tuple_fixture.column, SemanticSinkKind::EqualityConstant, tuple_role_prefix);
    ASSERT_TRUE(tuple_candidate);
    EXPECT_EQ(tuple_candidate->use_path.type_child_ordinals, std::vector<UInt64>({1}));
    const std::array<UInt64, 1> physical_tuple_prefix{0};
    EXPECT_FALSE(
        QueryAnalysisState::inspectPreboundContextualConstant(
            *tuple_fixture.column, SemanticSinkKind::EqualityConstant, physical_tuple_prefix));
}

TEST(UDTSemanticAdmission, ContextualSelectionRejectsExcessiveOrInvalidPaths)
{
    const auto capabilities = semanticCapabilityBit(SemanticCapability::Input) | semanticCapabilityBit(SemanticCapability::ValueChecks);
    const auto logical_descriptor = descriptor(210, capabilities);
    const DataTypePtr tuple_type
        = std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeString>(), logical_descriptor->getPhysicalType()});
    const auto tree = BoundDeclaredTypeTree::build(
        {
            {.type_child_ordinals = {}, .physical_type = tuple_type},
            {.type_child_ordinals = {1}, .physical_type = logical_descriptor->getPhysicalType()},
        },
        {{.type_child_ordinals = {1}, .logical_descriptor = logical_descriptor, .logical_preorder = 0}},
        {});
    const auto fixture = preboundColumnFromTree(210, logical_descriptor, tuple_type, tree, {logical_descriptor->getDefinition()});

    const std::array<UInt64, 1> invalid_prefix{2};
    expectLogicalError(
        [&]
        {
            static_cast<void>(
                QueryAnalysisState::inspectPreboundContextualConstant(*fixture.column, SemanticSinkKind::EqualityConstant, invalid_prefix));
        },
        "UDT contextual static-child path disagrees with its storage type");

    const std::vector<UInt64> excessive_prefix(65, 0);
    expectExceptionCode(
        ErrorCodes::LIMIT_EXCEEDED,
        [&]
        {
            static_cast<void>(QueryAnalysisState::inspectPreboundContextualConstant(
                *fixture.column, SemanticSinkKind::EqualityConstant, excessive_prefix));
        },
        "UDT contextual static-child path exceeds its depth limit");

    expectLogicalError(
        [&]
        { static_cast<void>(QueryAnalysisState::inspectPreboundContextualConstant(*fixture.column, SemanticSinkKind::LogicalRendering)); },
        "Unknown UDT contextual-constant sink kind");

    const auto output_descriptor = descriptor(211, semanticCapabilityBit(SemanticCapability::Output));
    const auto output_fixture = preboundColumnFromTree(
        211, output_descriptor, output_descriptor->getPhysicalType(), directTree(output_descriptor), {output_descriptor->getDefinition()});
    EXPECT_FALSE(
        QueryAnalysisState::inspectPreboundContextualConstant(
            *output_fixture.column, SemanticSinkKind::EqualityConstant, excessive_prefix));
}

TEST(UDTSemanticAdmission, ContextualSelectionRejectsStackedAndCrossWrapperAmbiguity)
{
    const auto capabilities = semanticCapabilityBit(SemanticCapability::Input) | semanticCapabilityBit(SemanticCapability::ValueChecks);

    const auto first_descriptor = descriptor(220, capabilities);
    const auto second_descriptor = descriptor(221, capabilities);
    const auto stacked_tree = BoundDeclaredTypeTree::build(
        {{.type_child_ordinals = {}, .physical_type = first_descriptor->getPhysicalType()}},
        {
            {.type_child_ordinals = {}, .logical_descriptor = first_descriptor, .logical_preorder = 0},
            {.type_child_ordinals = {}, .logical_descriptor = second_descriptor, .logical_preorder = 1},
        },
        {});
    const auto stacked_fixture = preboundColumnFromTree(
        220,
        first_descriptor,
        first_descriptor->getPhysicalType(),
        stacked_tree,
        {first_descriptor->getDefinition(), second_descriptor->getDefinition()});
    expectExceptionCode(
        ErrorCodes::NOT_IMPLEMENTED,
        [&]
        {
            static_cast<void>(
                QueryAnalysisState::inspectPreboundContextualConstant(*stacked_fixture.column, SemanticSinkKind::EqualityConstant));
        },
        "multiple logical roles stacked");

    const auto outer_descriptor = descriptor(222, capabilities, true);
    const auto inner_descriptor = descriptor(223, capabilities);
    const auto cross_wrapper_tree = BoundDeclaredTypeTree::build(
        {
            {.type_child_ordinals = {}, .physical_type = outer_descriptor->getPhysicalType()},
            {.type_child_ordinals = {0}, .physical_type = inner_descriptor->getPhysicalType()},
        },
        {
            {.type_child_ordinals = {}, .logical_descriptor = outer_descriptor, .logical_preorder = 0},
            {.type_child_ordinals = {0}, .logical_descriptor = inner_descriptor, .logical_preorder = 1},
        },
        {});
    const auto cross_wrapper_fixture = preboundColumnFromTree(
        222,
        outer_descriptor,
        outer_descriptor->getPhysicalType(),
        cross_wrapper_tree,
        {outer_descriptor->getDefinition(), inner_descriptor->getDefinition()});
    expectExceptionCode(
        ErrorCodes::NOT_IMPLEMENTED,
        [&]
        {
            static_cast<void>(
                QueryAnalysisState::inspectPreboundContextualConstant(*cross_wrapper_fixture.column, SemanticSinkKind::EqualityConstant));
        },
        "crosses multiple eligible logical wrapper roles");
}

TEST(UDTSemanticAdmission, ContextualRegistrationFinalizesOnceAndRejectsLateRegistration)
{
    const std::array kinds{
        SemanticSinkKind::EqualityConstant,
        SemanticSinkKind::InConstant,
        SemanticSinkKind::GlobalInConstant,
        SemanticSinkKind::HasConstant,
        SemanticSinkKind::HasAnyConstant,
    };
    UInt64 type_id = 120;
    for (const auto kind : kinds)
    {
        SCOPED_TRACE(static_cast<UInt64>(kind));
        const bool array_element_role = kind == SemanticSinkKind::HasConstant || kind == SemanticSinkKind::HasAnyConstant;
        const auto fixture = preboundColumn(type_id++, array_element_role);
        auto candidate = QueryAnalysisState::inspectPreboundContextualConstant(*fixture.column, kind);
        ASSERT_TRUE(candidate);
        const auto literal = contextualLiteral(kind);
        const UInt64 literal_bytes = literal->getColumn()->byteSize();

        QueryAnalysisState state;
        expectLogicalError(
            [&] { static_cast<void>(state.registerPreboundContextualConstant(fixture.column, literal, kind, *candidate)); },
            "A contextual UDT constant has no exact owning-database resource limits");
        EXPECT_FALSE(state.hasSemanticResourceBudget());
        EXPECT_FALSE(state.hasQueryTreeRegistrations());

        candidate->effective_query_limits = testResourceLimits();
        ASSERT_TRUE(state.registerPreboundContextualConstant(fixture.column, literal, kind, *candidate));
        EXPECT_TRUE(state.hasSemanticResourceBudget());
        EXPECT_TRUE(state.hasQueryTreeRegistrations());
        EXPECT_EQ(state.semantic_literal_bytes, literal_bytes);
        ASSERT_TRUE(state.resource_ledger);
        EXPECT_EQ(state.resource_ledger->getUsage().get(ResourceLimit::ContextualLiteralBytesPerQuery), 0);

        EXPECT_NO_THROW(state.finalizeSemanticAnalysis());
        EXPECT_TRUE(state.semantic_analysis_finalized);
        EXPECT_FALSE(state.hasQueryTreeRegistrations());
        EXPECT_EQ(state.semantic_role_graph, nullptr);
        EXPECT_EQ(state.semantic_role_planner, nullptr);
        EXPECT_EQ(state.semantic_literal_bytes, 0);
        EXPECT_EQ(state.resource_ledger->getUsage().get(ResourceLimit::ContextualLiteralBytesPerQuery), literal_bytes);
        EXPECT_NO_THROW(state.finalizeSemanticAnalysis());
        expectLogicalError(
            [&] { static_cast<void>(state.registerPreboundContextualConstant(fixture.column, literal, kind, *candidate)); },
            "UDT contextual constant reached analysis after the semantic graph was sealed");
    }
}

TEST(UDTSemanticAdmission, EmptyAnalysisFinalizationIsIdempotentAndAllocationFree)
{
    QueryAnalysisState state;
    EXPECT_FALSE(state.semantic_analysis_finalized);
    EXPECT_FALSE(state.hasSemanticResourceBudget());
    EXPECT_FALSE(state.hasQueryTreeRegistrations());
    EXPECT_EQ(state.resource_ledger, nullptr);

    EXPECT_NO_THROW(state.finalizeSemanticAnalysis());
    EXPECT_TRUE(state.semantic_analysis_finalized);
    EXPECT_FALSE(state.hasSemanticResourceBudget());
    EXPECT_FALSE(state.hasQueryTreeRegistrations());
    EXPECT_EQ(state.resource_ledger, nullptr);
    EXPECT_NO_THROW(state.finalizeSemanticAnalysis());
}

TEST(UDTSemanticAdmission, ResolvedCastTargetRetentionRemapsAndExpiresAtFinalization)
{
    QueryAnalysisState state;
    auto function = std::make_shared<FunctionNode>("CAST");
    const auto target = directTree(descriptor(140, semanticCapabilityBit(SemanticCapability::Output)));
    state.rememberResolvedExplicitCastTarget(function.get(), target);
    state.rememberResolvedExplicitCastTarget(function.get(), target);
    EXPECT_EQ(state.findResolvedExplicitCastTarget(function.get()), target);
    EXPECT_TRUE(state.hasQueryTreeRegistrations());

    const auto conflicting_target = directTree(descriptor(141, semanticCapabilityBit(SemanticCapability::Output)));
    expectLogicalError(
        [&] { state.rememberResolvedExplicitCastTarget(function.get(), conflicting_target); },
        "One resolved CAST node maps to conflicting UDT targets");
    EXPECT_EQ(state.findResolvedExplicitCastTarget(function.get()), target);

    IQueryTreeNode::CloneNodeMapping clone_mapping;
    const auto replacement = function->cloneAndReplace({}, &clone_mapping);
    const auto * replacement_function = replacement->as<FunctionNode>();
    ASSERT_NE(replacement_function, nullptr);
    state.remapSemanticGenerationAfterQueryTreeReplacement(clone_mapping);
    EXPECT_EQ(state.findResolvedExplicitCastTarget(function.get()), nullptr);
    EXPECT_EQ(state.findResolvedExplicitCastTarget(replacement_function), target);

    state.finalizeSemanticAnalysis();
    EXPECT_FALSE(state.hasQueryTreeRegistrations());
    EXPECT_EQ(state.findResolvedExplicitCastTarget(replacement_function), nullptr);
    expectLogicalError(
        [&] { state.rememberResolvedExplicitCastTarget(replacement_function, target); },
        "Resolved UDT CAST target reached a finalized query analysis");
    expectLogicalError(
        [&] { state.remapSemanticGenerationAfterQueryTreeReplacement({}); },
        "A QueryTree generation replacement reached finalized UDT semantic analysis");
}

TEST(UDTSemanticAdmission, DiscoveryAccountingChargesOnlyMonotonicDeltasAfterBudgetActivation)
{
    ResourceLimitLayer query_limits(ResourceLimitLayerKind::QueryProfile);
    query_limits.set(ResourceLimit::NodePathStatesPerQuery, 2);
    query_limits.set(ResourceLimit::InspectedEdgesPerQuery, 3);
    query_limits.set(ResourceLimit::SemanticScratchBytesPerQuery, 4);
    const auto limits = calculateEffectiveResourceLimits(std::span<const ResourceLimitLayer>{&query_limits, 1});

    QueryAnalysisState state;
    state.chargeSemanticDiscoveryWork(1, 2, 3);
    EXPECT_EQ(state.charged_semantic_discovery_node_path_states, 0);
    EXPECT_EQ(state.charged_semantic_discovery_inspected_edges, 0);
    EXPECT_EQ(state.charged_semantic_discovery_scratch_bytes, 0);
    EXPECT_EQ(state.resource_ledger, nullptr);

    state.getOrCreateSemanticResourceBudget(limits);
    ASSERT_TRUE(state.resource_ledger);
    state.chargeSemanticDiscoveryWork(1, 2, 3);
    EXPECT_EQ(state.charged_semantic_discovery_node_path_states, 1);
    EXPECT_EQ(state.charged_semantic_discovery_inspected_edges, 2);
    EXPECT_EQ(state.charged_semantic_discovery_scratch_bytes, 3);
    EXPECT_EQ(state.resource_ledger->getUsage().get(ResourceLimit::NodePathStatesPerQuery), 1);
    EXPECT_EQ(state.resource_ledger->getUsage().get(ResourceLimit::InspectedEdgesPerQuery), 2);
    EXPECT_EQ(state.resource_ledger->getUsage().get(ResourceLimit::SemanticScratchBytesPerQuery), 3);

    state.chargeSemanticDiscoveryWork(1, 2, 3);
    EXPECT_EQ(state.resource_ledger->getUsage().get(ResourceLimit::NodePathStatesPerQuery), 1);
    EXPECT_EQ(state.resource_ledger->getUsage().get(ResourceLimit::InspectedEdgesPerQuery), 2);
    EXPECT_EQ(state.resource_ledger->getUsage().get(ResourceLimit::SemanticScratchBytesPerQuery), 3);

    expectLogicalError([&] { state.chargeSemanticDiscoveryWork(0, 2, 3); }, "UDT semantic discovery work totals are not monotonic");
    try
    {
        state.chargeSemanticDiscoveryWork(3, 2, 3);
        FAIL() << "expected the query-wide node/path-state limit to reject growth";
    }
    catch (const Exception & error)
    {
        EXPECT_EQ(error.code(), ErrorCodes::LIMIT_EXCEEDED) << error.message();
    }
    EXPECT_EQ(state.charged_semantic_discovery_node_path_states, 1);
    EXPECT_EQ(state.resource_ledger->getUsage().get(ResourceLimit::NodePathStatesPerQuery), 1);
}

}
