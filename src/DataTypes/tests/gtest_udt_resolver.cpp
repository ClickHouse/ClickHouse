#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/UDT/TemplateChecker.h>
#include <DataTypes/UDT/TypeResolver.h>

#include <Common/tests/gtest_global_register.h>

#include <Parsers/ASTDataType.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/ASTObjectTypeArgument.h>
#include <Parsers/ASTTupleDataType.h>
#include <Parsers/ParserDataType.h>
#include <Parsers/parseQuery.h>

#include <gtest/gtest.h>

#include <array>
#include <stdexcept>
#include <utility>
#include <vector>

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

const UUID database_uuid = testUUID(0x550e8400e29b41d4ULL, 0xa716446655440000ULL);

DefinitionIdentity identity(UInt64 ordinal)
{
    return {
        .database_uuid = database_uuid,
        .type_uuid = testUUID(0x123456789abcdef0ULL, ordinal),
        .revision = 1,
    };
}

TypeAuthorityCapabilities transientCapabilities()
{
    TypeAuthorityCapabilities capabilities;
    capabilities.mask = typeAuthorityCapabilityBit(TypeAuthorityCapability::TransientResolution)
        | typeAuthorityCapabilityBit(TypeAuthorityCapability::Limits) | typeAuthorityCapabilityBit(TypeAuthorityCapability::Templates);
    capabilities.limits = {
        .maximum_definitions = 1'024,
        .maximum_definition_bytes = 1ULL << 20,
        .maximum_template_nodes = 4'096,
        .maximum_direct_dependencies = 256,
        .maximum_transitive_dependencies = 1'024,
        .maximum_checker_work = 65'536,
    };
    return capabilities;
}

TemplateNode builtIn(String family)
{
    TemplateNode node;
    node.kind = TemplateNodeKind::BuiltIn;
    node.atom = std::move(family);
    return node;
}

Definition::Ptr definition(UInt64 ordinal, String family)
{
    DefinitionInput input;
    input.identity = identity(ordinal);
    input.normalized_name = "db.Type" + std::to_string(ordinal);
    input.normalized_local_name = "Type" + std::to_string(ordinal);
    input.nodes.push_back(builtIn(std::move(family)));
    auto checked = TemplateChecker::checkAll({std::move(input)});
    EXPECT_EQ(checked.size(), 1);
    return checked.front();
}

Definition::Ptr typeAliasDefinition(UInt64 ordinal)
{
    DefinitionInput input;
    input.identity = identity(ordinal);
    input.normalized_name = "db.TypeAlias" + std::to_string(ordinal);
    input.normalized_local_name = "TypeAlias" + std::to_string(ordinal);
    input.parameters = {{.normalized_name = "T", .kind = ParameterKind::Type}};
    TemplateNode formal;
    formal.kind = TemplateNodeKind::TypeParameter;
    formal.parameter = 0;
    input.nodes.push_back(std::move(formal));
    auto checked = TemplateChecker::checkAll({std::move(input)});
    EXPECT_EQ(checked.size(), 1);
    return checked.front();
}

Definition::Ptr nestedArrayDefinition(UInt64 ordinal, size_t depth)
{
    DefinitionInput input;
    input.identity = identity(ordinal);
    input.normalized_name = "db.NestedArray" + std::to_string(ordinal);
    input.normalized_local_name = "NestedArray" + std::to_string(ordinal);
    input.nodes.reserve(depth + 1);
    for (size_t index = 0; index < depth; ++index)
    {
        auto array = builtIn("Array");
        array.children.push_back({.reference = static_cast<UInt32>(index + 1), .label = {}});
        input.nodes.push_back(std::move(array));
    }
    input.nodes.push_back(builtIn("UInt64"));
    auto checked = TemplateChecker::checkAll({std::move(input)});
    EXPECT_EQ(checked.size(), 1);
    return checked.front();
}

Definition::Ptr aggregateFieldDefinition(UInt64 ordinal, Field parameter)
{
    DefinitionInput input;
    input.identity = identity(ordinal);
    input.normalized_name = "db.AggregateField" + std::to_string(ordinal);
    input.normalized_local_name = "AggregateField" + std::to_string(ordinal);
    TemplateNode function;
    function.kind = TemplateNodeKind::AggregateFunction;
    function.text = "sum";
    input.nodes = {
        builtIn("AggregateFunction"),
        std::move(function),
        builtIn("UInt64"),
    };
    input.nodes[0].children = {{.reference = 1, .label = {}}, {.reference = 2, .label = {}}};
    const Array parameters{std::move(parameter)};
    for (const TemplateNodeID root : appendCanonicalFieldValues(parameters, input.nodes))
        input.nodes[1].children.push_back({.reference = root, .label = {}});
    auto checked = TemplateChecker::checkAll({std::move(input)});
    EXPECT_EQ(checked.size(), 1);
    return checked.front();
}

Definition::Ptr conditionalTypeAliasDefinition(UInt64 ordinal)
{
    DefinitionInput input;
    input.identity = identity(ordinal);
    input.normalized_name = "db.ConditionalAlias" + std::to_string(ordinal);
    input.normalized_local_name = "ConditionalAlias" + std::to_string(ordinal);
    input.parameters = {
        {.normalized_name = "T", .kind = ParameterKind::Type},
        {.normalized_name = "N", .kind = ParameterKind::UInt64},
    };
    TemplateNode choose;
    choose.kind = TemplateNodeKind::TypeIfZero;
    choose.parameter = 1;
    choose.children = {{.reference = 1, .label = {}}, {.reference = 2, .label = {}}};
    auto fixed = builtIn("UInt64");
    TemplateNode formal;
    formal.kind = TemplateNodeKind::TypeParameter;
    formal.parameter = 0;
    input.nodes = {std::move(choose), std::move(fixed), std::move(formal)};
    auto checked = TemplateChecker::checkAll({std::move(input)});
    EXPECT_EQ(checked.size(), 1);
    return checked.front();
}

Definition::Ptr valueArgumentDefinition(UInt64 ordinal, ParameterKind kind)
{
    DefinitionInput input;
    input.identity = identity(ordinal);
    input.normalized_name = "db.ValueArgument" + std::to_string(ordinal);
    input.normalized_local_name = "ValueArgument" + std::to_string(ordinal);
    input.parameters = {{.normalized_name = "V", .kind = kind}};
    input.nodes.push_back(builtIn("UInt64"));
    auto checked = TemplateChecker::checkAll({std::move(input)});
    EXPECT_EQ(checked.size(), 1);
    return checked.front();
}

std::pair<Definition::Ptr, Definition::Ptr> transparentDependencyDefinitions()
{
    DefinitionInput inner;
    inner.identity = identity(10);
    inner.normalized_name = "db.Inner";
    inner.normalized_local_name = "Inner";
    inner.nodes.push_back(builtIn("UInt64"));

    DefinitionInput outer;
    outer.identity = identity(11);
    outer.normalized_name = "db.Outer";
    outer.normalized_local_name = "Outer";
    outer.dependencies.push_back(
        {.type_uuid = inner.identity.type_uuid, .revision = inner.identity.revision, .target_definition_hash = {}});
    TemplateNode call;
    call.kind = TemplateNodeKind::DefinitionCall;
    call.dependency_ordinal = 0;
    outer.nodes.push_back(std::move(call));

    auto checked = TemplateChecker::checkAll({std::move(inner), std::move(outer)});
    EXPECT_EQ(checked.size(), 2);
    return {checked[0], checked[1]};
}

CanonicalTypeArguments noArguments()
{
    return CanonicalTypeArguments::validate({}, {});
}

ASTPtr parseType(const String & text)
{
    tryRegisterAggregateFunctions();
    ParserDataType parser;
    return parseQuery(parser, text, "UDT resolver test", 0, 200, 0);
}

ASTPtr wrapInArrays(ASTPtr type, size_t depth)
{
    for (size_t index = 0; index < depth; ++index)
        type = makeASTDataType("Array", std::move(type));
    return type;
}

boost::intrusive_ptr<ASTDataType> marker(String name)
{
    auto result = make_intrusive<ASTDataType>();
    result->name = std::move(name);
    return result;
}

boost::intrusive_ptr<ASTNameTypePair> named(String name, ASTPtr type)
{
    auto result = make_intrusive<ASTNameTypePair>();
    result->name = std::move(name);
    result->type = std::move(type);
    result->children.push_back(result->type);
    return result;
}

DeclaredTypeReferenceInput reference(const ASTPtr & ast, const Definition::Ptr & target)
{
    return {
        .reference_node = ast.get(),
        .definition_identity = target->getIdentity(),
        .canonical_arguments = noArguments(),
        .type_argument_lineage = {},
    };
}

CanonicalTypeArguments oneTypeArgument(const Definition & target, const ASTPtr & physical)
{
    std::vector<CanonicalTypeArgumentValue> arguments;
    arguments.push_back(CanonicalTypeArgumentValue::type(physical));
    return CanonicalTypeArguments::validate(target.getParameters(), std::move(arguments));
}

CanonicalTypeArguments conditionalArguments(const Definition & target, const ASTPtr & physical, UInt64 selector)
{
    std::vector<CanonicalTypeArgumentValue> arguments;
    arguments.push_back(CanonicalTypeArgumentValue::type(physical));
    arguments.push_back(CanonicalTypeArgumentValue::unsignedInteger(ParameterKind::UInt64, selector));
    return CanonicalTypeArguments::validate(target.getParameters(), std::move(arguments));
}

DeclaredTypeReferenceInput referenceWithTypeArgument(
    const ASTPtr & ast,
    const Definition::Ptr & target,
    CanonicalTypeArguments arguments,
    const ASTPtr & nested,
    RelativePhysicalTypePath nested_path = {})
{
    DeclaredTypeReferenceInput result{
        .reference_node = ast.get(),
        .definition_identity = target->getIdentity(),
        .canonical_arguments = std::move(arguments),
        .type_argument_lineage = {},
    };
    result.type_argument_lineage.push_back({.parameter = 0, .path = std::move(nested_path), .reference_node = nested.get()});
    return result;
}

DeclaredTypeReferenceInput
referenceWithValueArgument(const ASTPtr & ast, const Definition::Ptr & target, CanonicalTypeArgumentValue value)
{
    std::vector<CanonicalTypeArgumentValue> values;
    values.push_back(std::move(value));
    return {
        .reference_node = ast.get(),
        .definition_identity = target->getIdentity(),
        .canonical_arguments = CanonicalTypeArguments::validate(target->getParameters(), std::move(values)),
        .type_argument_lineage = {},
    };
}

AuthorityAdapterPtr authority(std::vector<Definition::Ptr> definitions)
{
    return makeTransientAuthorityAdapter(database_uuid, transientCapabilities(), std::move(definitions));
}

void collectExactGenericTypes(const ASTPtr & root, std::string_view name, std::vector<ASTPtr> & output)
{
    if (const auto * type = root->as<ASTDataType>(); type && type->name == name)
        output.push_back(root);
    for (const auto & child : root->children)
        collectExactGenericTypes(child, name, output);
}

template <typename Function>
void expectResolverError(TypeResolverError::Code expected, Function && function)
{
    try
    {
        function();
        FAIL() << "expected a TypeResolverError";
    }
    catch (const TypeResolverError & error)
    {
        EXPECT_EQ(error.code, expected);
    }
}

class FailIfTouchedAuthority final : public IAuthorityAdapter
{
public:
    const TypeAuthorityCapabilities & getCapabilities() const noexcept override
    {
        ++capability_calls;
        return capabilities;
    }

    UUID getDatabaseUUID() const noexcept override
    {
        ++database_calls;
        return UUIDHelpers::Nil;
    }

    ResolutionSession beginResolutionSession() const override
    {
        ++session_calls;
        throw std::runtime_error("built-in fast path touched the UDT authority");
    }

    void requireCapabilities(TypeAuthorityCapabilityMask, std::string_view) const override
    {
        ++require_calls;
        throw std::runtime_error("built-in fast path checked UDT capabilities");
    }

    mutable UInt64 capability_calls = 0;
    mutable UInt64 database_calls = 0;
    mutable UInt64 session_calls = 0;
    mutable UInt64 require_calls = 0;

private:
    TypeAuthorityCapabilities capabilities;
};

}

TEST(UDTResolver, BuiltInFastPathIsTheUnchangedSingleFactoryCall)
{
    const auto ast = parseType("Tuple(UInt64, Array(String))");
    FailIfTouchedAuthority untouched;
    TypeResolverLimits deliberately_invalid_udt_limits;
    deliberately_invalid_udt_limits.maximum_input_references = 0;
    TypeResolverStatistics statistics;
    statistics.logical_occurrences = 999;

    const auto result = TypeResolver::resolve(ast, {}, untouched, deliberately_invalid_udt_limits, &statistics);

    ASSERT_FALSE(result.hasLogicalTree());
    EXPECT_EQ(result.getPhysicalType()->getName(), "Tuple(UInt64, Array(String))");
    EXPECT_EQ(statistics.physical_factory_calls, 1);
    EXPECT_EQ(statistics.logical_occurrences, 0);
    EXPECT_EQ(untouched.capability_calls, 0);
    EXPECT_EQ(untouched.database_calls, 0);
    EXPECT_EQ(untouched.session_calls, 0);
    EXPECT_EQ(untouched.require_calls, 0);
}

TEST(UDTResolver, InvalidCompositeLimitsFailBeforeAuthorityOrFactoryWork)
{
    const auto target = definition(37, "UInt64");
    ASTPtr root = marker("db.Logical");
    const std::array references{reference(root, target)};
    FailIfTouchedAuthority untouched;

    TypeResolverLimits invalid_specializer;
    invalid_specializer.specializer.maximum_work = 0;
    expectResolverError(
        TypeResolverError::Code::LimitExceeded,
        [&] { static_cast<void>(TypeResolver::resolve(root, references, untouched, invalid_specializer)); });

    TypeResolverLimits invalid_descriptor;
    invalid_descriptor.descriptors.maximum_nodes = 0;
    expectResolverError(
        TypeResolverError::Code::LimitExceeded,
        [&] { static_cast<void>(TypeResolver::resolve(root, references, untouched, invalid_descriptor)); });

    EXPECT_EQ(untouched.capability_calls, 0);
    EXPECT_EQ(untouched.database_calls, 0);
    EXPECT_EQ(untouched.session_calls, 0);
    EXPECT_EQ(untouched.require_calls, 0);
}

TEST(UDTResolver, FoldedBuiltInMarkerCollisionFailsBeforeAuthorityOrFactoryWork)
{
    const auto target = definition(208, "UInt64");
    ASTPtr root = marker("uInT64");
    const std::array references{reference(root, target)};
    FailIfTouchedAuthority untouched;

    expectResolverError(
        TypeResolverError::Code::InvalidReference, [&] { static_cast<void>(TypeResolver::resolve(root, references, untouched)); });

    EXPECT_EQ(untouched.capability_calls, 0);
    EXPECT_EQ(untouched.database_calls, 0);
    EXPECT_EQ(untouched.session_calls, 0);
    EXPECT_EQ(untouched.require_calls, 0);
}

TEST(UDTResolver, DeclarationPayloadIsBoundedBeforeAuthorityCloneOrFactoryWork)
{
    const auto target = definition(206, "UInt64");
    ASTPtr root = marker("db." + String(256, 'm'));
    const std::array references{reference(root, target)};
    FailIfTouchedAuthority untouched;
    TypeResolverLimits limits;
    limits.maximum_declaration_ast_syntax_bytes = 32;
    TypeResolverStatistics unchanged;
    unchanged.declaration_ast_syntax_bytes = 777;
    unchanged.physical_factory_calls = 777;

    expectResolverError(
        TypeResolverError::Code::LimitExceeded,
        [&] { static_cast<void>(TypeResolver::resolve(root, references, untouched, limits, &unchanged)); });

    EXPECT_EQ(unchanged.declaration_ast_syntax_bytes, 777);
    EXPECT_EQ(unchanged.physical_factory_calls, 777);
    EXPECT_EQ(untouched.capability_calls, 0);
    EXPECT_EQ(untouched.database_calls, 0);
    EXPECT_EQ(untouched.session_calls, 0);
    EXPECT_EQ(untouched.require_calls, 0);
}

TEST(UDTResolver, DeclarationHiddenAliasStateIsRejectedBeforeAuthorityOrClone)
{
    const auto target = valueArgumentDefinition(207, ParameterKind::String);
    auto literal = make_intrusive<ASTLiteral>(Field(String("x")));
    literal->alias = String(256, 'a');
    ASTPtr root = makeASTDataType("db.AliasedMarker", literal);
    const std::array references{referenceWithValueArgument(root, target, CanonicalTypeArgumentValue::string("x"))};
    FailIfTouchedAuthority untouched;
    TypeResolverStatistics unchanged;
    unchanged.declaration_ast_nodes = 777;
    unchanged.physical_factory_calls = 777;

    expectResolverError(
        TypeResolverError::Code::InvalidASTShape,
        [&] { static_cast<void>(TypeResolver::resolve(root, references, untouched, {}, &unchanged)); });

    EXPECT_EQ(unchanged.declaration_ast_nodes, 777);
    EXPECT_EQ(unchanged.physical_factory_calls, 777);
    EXPECT_EQ(untouched.capability_calls, 0);
    EXPECT_EQ(untouched.database_calls, 0);
    EXPECT_EQ(untouched.session_calls, 0);
    EXPECT_EQ(untouched.require_calls, 0);
}

TEST(UDTResolver, DeclarationHiddenIdentifierSemanticStateIsRejectedBeforeAuthorityOrClone)
{
    const auto target = definition(208, "UInt64");
    auto identifier = make_intrusive<ASTIdentifier>(std::vector<String>{String(256, 'h'), "setting"});
    identifier->setShortName("setting");
    ASSERT_FALSE(identifier->getParserIdentifierSemanticStringBytes().has_value());
    ASTPtr root = makeASTDataType("db.HiddenIdentifier", identifier);
    const std::array references{reference(root, target)};
    FailIfTouchedAuthority untouched;
    TypeResolverStatistics unchanged;
    unchanged.declaration_ast_nodes = 777;
    unchanged.physical_factory_calls = 777;

    expectResolverError(
        TypeResolverError::Code::InvalidASTShape,
        [&] { static_cast<void>(TypeResolver::resolve(root, references, untouched, {}, &unchanged)); });

    EXPECT_EQ(unchanged.declaration_ast_nodes, 777);
    EXPECT_EQ(unchanged.physical_factory_calls, 777);
    EXPECT_EQ(untouched.capability_calls, 0);
    EXPECT_EQ(untouched.database_calls, 0);
    EXPECT_EQ(untouched.session_calls, 0);
    EXPECT_EQ(untouched.require_calls, 0);
}

TEST(UDTResolver, ParserCompoundIdentifierStateRemainsValidOnTheActivatedPath)
{
    const auto target = definition(209, "UInt64");
    const auto adapter = authority({target});
    ASTPtr logical = marker("db.WithJSONSibling");
    ASTPtr json = parseType("JSON(SKIP hidden)");
    ASTPtr root = makeASTDataType("Tuple", logical, json);
    ASTPtr physical_root = makeASTDataType("Tuple", makeASTDataType("UInt64"), json->clone());
    const std::array references{reference(logical, target)};

    const auto result = TypeResolver::resolve(root, references, *adapter);
    const auto physical_only = TypeResolver::resolve(physical_root, {}, *adapter);

    EXPECT_TRUE(result.hasLogicalTree());
    EXPECT_FALSE(physical_only.hasLogicalTree());
    EXPECT_TRUE(result.getPhysicalType()->equals(*physical_only.getPhysicalType()));
    EXPECT_EQ(result.getPhysicalType()->getName(), physical_only.getPhysicalType()->getName());
}

TEST(UDTResolver, RootReferenceLowersOnceAndPublishesNoSessionState)
{
    const auto target = definition(1, "UInt64");
    ASTPtr root = marker("db.RootLogical");
    const std::array references{reference(root, target)};
    const auto adapter = authority({target});
    TypeResolverStatistics statistics;

    const auto result = TypeResolver::resolve(root, references, *adapter, {}, &statistics);

    ASSERT_TRUE(result.hasLogicalTree());
    EXPECT_EQ(result.getPhysicalType()->getName(), "UInt64");
    EXPECT_EQ(statistics.physical_factory_calls, 1);
    EXPECT_EQ(statistics.specializer.resolution_sessions, 1);
    EXPECT_EQ(statistics.logical_occurrences, 1);
    EXPECT_EQ(statistics.bound_nodes, 1);
    ASSERT_EQ(result.getLogicalTree()->getDescriptorIndices(0).size(), 1);
    EXPECT_EQ(result.getLogicalTree()->getDefinitionHandles().size(), 1);
}

TEST(UDTResolver, NestedNamedAndSpecializedTuplePathsUseBinaryChildOrdinals)
{
    const auto first_definition = definition(2, "UInt64");
    const auto second_definition = definition(3, "String");
    ASTPtr first = marker("db.FirstLogical");
    ASTPtr second = marker("db.SecondLogical");
    auto array = makeASTDataType("Array", second);
    ASTPtr root = makeASTDataType("Tuple", named("first", first), named("second", array));
    const std::array references{reference(first, first_definition), reference(second, second_definition)};
    const auto adapter = authority({first_definition, second_definition});

    const auto result = TypeResolver::resolve(root, references, *adapter);

    ASSERT_TRUE(result.hasLogicalTree());
    EXPECT_EQ(result.getPhysicalType()->getName(), "Tuple(first UInt64, second Array(String))");
    const std::array<UInt32, 1> first_path{0};
    const std::array<UInt32, 2> second_path{1, 0};
    const auto first_node = result.getLogicalTree()->findNode(first_path);
    const auto second_node = result.getLogicalTree()->findNode(second_path);
    ASSERT_TRUE(first_node);
    ASSERT_TRUE(second_node);
    EXPECT_EQ(result.getLogicalTree()->getDescriptorIndices(*first_node).size(), 1);
    EXPECT_EQ(result.getLogicalTree()->getDescriptorIndices(*second_node).size(), 1);

    /// ParserDataType's allocation-reduced specialized Tuple AST follows the
    /// same path contract as the generic named-pair representation above.
    ASTPtr specialized = parseType("Tuple(named UInt64, logical SecondLogical)");
    const auto * tuple = specialized->as<ASTTupleDataType>();
    ASSERT_NE(tuple, nullptr);
    ASSERT_NE(tuple->getArguments(), nullptr);
    ASTPtr specialized_marker = tuple->getArguments()->children[1];
    const std::array specialized_references{reference(specialized_marker, second_definition)};
    const auto specialized_result = TypeResolver::resolve(specialized, specialized_references, *adapter);
    const std::array<UInt32, 1> specialized_path{1};
    ASSERT_TRUE(specialized_result.getLogicalTree()->findNode(specialized_path));
}

TEST(UDTResolver, TransparentDependencyPreservesOuterThenInnerAtOnePhysicalPath)
{
    const auto [inner, outer] = transparentDependencyDefinitions();
    ASTPtr root = marker("db.OuterLogical");
    const std::array references{reference(root, outer)};
    const auto adapter = authority({inner, outer});

    const auto result = TypeResolver::resolve(root, references, *adapter);

    ASSERT_TRUE(result.hasLogicalTree());
    ASSERT_EQ(result.getLogicalTree()->getOccurrenceCount(), 2);
    const auto indices = result.getLogicalTree()->getDescriptorIndices(0);
    ASSERT_EQ(indices.size(), 2);
    const auto & descriptors = result.getLogicalTree()->getDescriptors();
    EXPECT_EQ(descriptors[indices[0]]->getDefinition()->getIdentity(), outer->getIdentity());
    EXPECT_EQ(descriptors[indices[1]]->getDefinition()->getIdentity(), inner->getIdentity());
}

TEST(UDTResolver, StableTopologyInventoryMatchesCompleteBinaryTypeChildren)
{
    const auto string_definition = definition(12, "String");
    const auto uint64_definition = definition(13, "UInt64");
    const auto float32_definition = definition(14, "Float32");
    ASTPtr root = parseType(
        "Tuple("
        "JSON(z UInt8, a LogicalString), "
        "Nested(first UInt8, second LogicalString), "
        "AggregateFunction(sum, LogicalUInt64), "
        "SimpleAggregateFunction(sum, LogicalUInt64), "
        "Map(String, LogicalUInt64), "
        "QBit(LogicalFloat32, 8), "
        "Nullable(LogicalUInt64), "
        "LowCardinality(LogicalString))");

    std::vector<ASTPtr> string_markers;
    std::vector<ASTPtr> uint64_markers;
    std::vector<ASTPtr> float32_markers;
    collectExactGenericTypes(root, "LogicalString", string_markers);
    collectExactGenericTypes(root, "LogicalUInt64", uint64_markers);
    collectExactGenericTypes(root, "LogicalFloat32", float32_markers);
    ASSERT_EQ(string_markers.size(), 3);
    ASSERT_EQ(uint64_markers.size(), 4);
    ASSERT_EQ(float32_markers.size(), 1);

    std::vector<DeclaredTypeReferenceInput> references;
    references.reserve(8);
    for (const auto & logical : string_markers)
        references.push_back(reference(logical, string_definition));
    for (const auto & logical : uint64_markers)
        references.push_back(reference(logical, uint64_definition));
    references.push_back(reference(float32_markers.front(), float32_definition));
    const auto adapter = authority({string_definition, uint64_definition, float32_definition});

    const auto result = TypeResolver::resolve(root, references, *adapter);

    EXPECT_EQ(result.getPhysicalType()->getTypeId(), TypeIndex::Tuple);
    const std::array<std::array<UInt32, 2>, 8> expected_paths{
        std::array<UInt32, 2>{0, 0}, /// JSON typed paths sort by binary path key: a precedes z.
        std::array<UInt32, 2>{1, 1}, /// Nested custom encoding exposes named elements, not Array/Tuple storage wrappers.
        std::array<UInt32, 2>{2, 0}, /// AggregateFunction skips function metadata before argument types.
        std::array<UInt32, 2>{3, 0}, /// SimpleAggregateFunction uses custom-name argument types.
        std::array<UInt32, 2>{4, 1}, /// Map value.
        std::array<UInt32, 2>{5, 0}, /// QBit element; dimensions are values, not type children.
        std::array<UInt32, 2>{6, 0}, /// Nullable nested type.
        std::array<UInt32, 2>{7, 0}, /// LowCardinality dictionary type.
    };
    const std::array<std::string_view, 8> expected_physical_names{
        "String", "String", "UInt64", "UInt64", "UInt64", "Float32", "UInt64", "String"};
    for (size_t index = 0; index < expected_paths.size(); ++index)
    {
        const auto node = result.getLogicalTree()->findNode(expected_paths[index]);
        ASSERT_TRUE(node);
        EXPECT_EQ(result.getLogicalTree()->getDescriptorIndices(*node).size(), 1);
        EXPECT_EQ(result.getLogicalTree()->getNode(*node).getPhysicalType()->getName(), expected_physical_names[index]);
    }
    EXPECT_EQ(result.getLogicalTree()->getOccurrenceCount(), 8);
    EXPECT_EQ(result.getLogicalTree()->getDescriptors().size(), 3);
}

TEST(UDTResolver, ZeroArgumentAggregateFunctionHasNoPhysicalTypeChild)
{
    const auto target = definition(36, "UInt64");
    const auto adapter = authority({target});
    ASTPtr logical = marker("db.Logical");
    ASTPtr root = makeASTDataType("Tuple", logical, parseType("AggregateFunction(count)"));
    const std::array references{reference(logical, target)};

    const auto result = TypeResolver::resolve(root, references, *adapter);

    EXPECT_EQ(result.getPhysicalType()->getName(), "Tuple(UInt64, AggregateFunction(count))");
    EXPECT_EQ(result.getLogicalTree()->getOccurrenceCount(), 1);
    const std::array<UInt32, 1> path{0};
    ASSERT_TRUE(result.getLogicalTree()->findNode(path));
}

TEST(UDTResolver, VariantRemapsSourceOrdinalToCanonicalFinalOrdinal)
{
    const auto target = definition(4, "String");
    ASTPtr logical = marker("db.StringLogical");
    ASTPtr root = makeASTDataType("Variant", makeASTDataType("UInt64"), logical);
    const std::array references{reference(logical, target)};
    const auto adapter = authority({target});
    TypeResolverStatistics statistics;

    const auto result = TypeResolver::resolve(root, references, *adapter, {}, &statistics);

    EXPECT_EQ(result.getPhysicalType()->getName(), "Variant(String, UInt64)");
    const std::array<UInt32, 1> normalized_string_path{0};
    const auto node = result.getLogicalTree()->findNode(normalized_string_path);
    ASSERT_TRUE(node);
    EXPECT_EQ(result.getLogicalTree()->getDescriptorIndices(*node).size(), 1);
    EXPECT_EQ(statistics.variant_branch_factory_calls, 2);
}

TEST(UDTResolver, VariantDropAndCollapseFailClosed)
{
    const auto nothing = definition(5, "Nothing");
    const auto uint64 = definition(6, "UInt64");
    const auto adapter = authority({nothing, uint64});

    ASTPtr dropped = marker("db.NothingLogical");
    ASTPtr dropped_root = makeASTDataType("Variant", makeASTDataType("String"), dropped);
    const std::array dropped_references{reference(dropped, nothing)};
    expectResolverError(
        TypeResolverError::Code::VariantBranchDropped,
        [&] { static_cast<void>(TypeResolver::resolve(dropped_root, dropped_references, *adapter)); });

    ASTPtr collapsed = marker("db.UInt64Logical");
    ASTPtr collapsed_root = makeASTDataType("Variant", makeASTDataType("UInt64"), collapsed);
    const std::array collapsed_references{reference(collapsed, uint64)};
    expectResolverError(
        TypeResolverError::Code::VariantBranchCollapsed,
        [&] { static_cast<void>(TypeResolver::resolve(collapsed_root, collapsed_references, *adapter)); });
}

TEST(UDTResolver, SideTableAndUnsubstitutedMarkerFailuresAreExactAndTransactional)
{
    const auto target = definition(7, "UInt64");
    const auto adapter = authority({target});
    ASTPtr present = marker("db.Present");
    ASTPtr absent = marker("db.Absent");
    ASTPtr root = makeASTDataType("Tuple", present, makeASTDataType("String"));

    std::array duplicate{reference(present, target), reference(present, target)};
    expectResolverError(
        TypeResolverError::Code::DuplicateReference, [&] { static_cast<void>(TypeResolver::resolve(root, duplicate, *adapter)); });

    ASTPtr malformed_marker = marker("db.MalformedShape");
    malformed_marker->children.push_back(make_intrusive<ASTLiteral>(Field(UInt64{1})));
    const std::array malformed_shape{reference(malformed_marker, target)};
    /// The complete declaration surface is validated before side-table
    /// lineage. A marker that directly owns a literal is therefore precisely
    /// an invalid ParserDataType AST shape, not malformed lineage metadata.
    expectResolverError(
        TypeResolverError::Code::InvalidASTShape,
        [&] { static_cast<void>(TypeResolver::resolve(malformed_marker, malformed_shape, *adapter)); });

    ASTPtr built_in_root = makeASTDataType("Tuple", makeASTDataType("UInt64"), makeASTDataType("String"));
    const std::array unreachable{reference(absent, target)};
    expectResolverError(
        TypeResolverError::Code::UnreachableReference,
        [&] { static_cast<void>(TypeResolver::resolve(built_in_root, unreachable, *adapter)); });

    ASTPtr unknown = marker("db.UnknownWithoutSideTableEntry");
    ASTPtr mixed_root = makeASTDataType("Tuple", present, unknown);
    const std::array only_present{reference(present, target)};
    TypeResolverStatistics unchanged;
    unchanged.logical_occurrences = 777;
    expectResolverError(
        TypeResolverError::Code::UnsubstitutedReference,
        [&] { static_cast<void>(TypeResolver::resolve(mixed_root, only_present, *adapter, {}, &unchanged)); });
    EXPECT_EQ(unchanged.logical_occurrences, 777);
    EXPECT_EQ(unchanged.physical_factory_calls, 0);
}

TEST(UDTResolver, VariantBranchFactoryCallsAreProspectivelyBounded)
{
    const auto target = definition(8, "String");
    const auto adapter = authority({target});
    ASTPtr logical = marker("db.StringLogical");
    ASTPtr root = makeASTDataType("Variant", makeASTDataType("UInt64"), logical);
    const std::array references{reference(logical, target)};
    TypeResolverLimits limits;
    limits.maximum_variant_branch_factory_calls = 1;

    expectResolverError(
        TypeResolverError::Code::LimitExceeded, [&] { static_cast<void>(TypeResolver::resolve(root, references, *adapter, limits)); });
}

TEST(UDTResolver, NestedTypeActualLineagePreservesSamePhysicalDifferentIdentities)
{
    const auto outer = typeAliasDefinition(20);
    const auto inner_first = definition(21, "UInt64");
    const auto inner_second = definition(22, "UInt64");
    ASTPtr first_inner_syntax = marker("db.InnerFirst");
    ASTPtr second_inner_syntax = marker("db.InnerSecond");
    ASTPtr first_outer_syntax = makeASTDataType("db.Outer", first_inner_syntax);
    ASTPtr second_outer_syntax = makeASTDataType("db.Outer", second_inner_syntax);
    ASTPtr root = makeASTDataType("Tuple", first_outer_syntax, second_outer_syntax);

    std::vector<DeclaredTypeReferenceInput> references;
    references.push_back(reference(first_inner_syntax, inner_first));
    references.push_back(reference(second_inner_syntax, inner_second));
    references.push_back(
        referenceWithTypeArgument(first_outer_syntax, outer, oneTypeArgument(*outer, parseType("UInt64")), first_inner_syntax));
    references.push_back(
        referenceWithTypeArgument(second_outer_syntax, outer, oneTypeArgument(*outer, parseType("UInt64")), second_inner_syntax));
    const auto adapter = authority({outer, inner_first, inner_second});
    TypeResolverStatistics statistics;

    const auto result = TypeResolver::resolve(root, references, *adapter, {}, &statistics);

    ASSERT_TRUE(result.hasLogicalTree());
    EXPECT_EQ(result.getPhysicalType()->getName(), "Tuple(UInt64, UInt64)");
    EXPECT_EQ(statistics.physical_factory_calls, 1);
    EXPECT_EQ(statistics.argument_validation_factory_calls, 2);
    EXPECT_EQ(statistics.logical_occurrences, 4);
    const auto & tree = *result.getLogicalTree();
    const auto & descriptors = tree.getDescriptors();
    for (UInt32 ordinal = 0; ordinal < 2; ++ordinal)
    {
        const std::array<UInt32, 1> path{ordinal};
        const auto node = tree.findNode(path);
        ASSERT_TRUE(node);
        const auto indices = tree.getDescriptorIndices(*node);
        ASSERT_EQ(indices.size(), 2);
        EXPECT_EQ(descriptors[indices[0]]->getDefinition()->getIdentity(), outer->getIdentity());
        EXPECT_EQ(
            descriptors[indices[1]]->getDefinition()->getIdentity(),
            ordinal == 0 ? inner_first->getIdentity() : inner_second->getIdentity());
    }
}

TEST(UDTResolver, TypeActualValidationWorkIsProspectivelyBounded)
{
    const auto outer = typeAliasDefinition(35);
    const auto adapter = authority({outer});
    ASTPtr first = makeASTDataType("db.Outer", makeASTDataType("UInt64"));
    ASTPtr second = makeASTDataType("db.Outer", makeASTDataType("UInt64"));
    ASTPtr root = makeASTDataType("Tuple", first, second);
    DeclaredTypeReferenceInput first_reference{
        .reference_node = first.get(),
        .definition_identity = outer->getIdentity(),
        .canonical_arguments = oneTypeArgument(*outer, parseType("UInt64")),
        .type_argument_lineage = {},
    };
    DeclaredTypeReferenceInput second_reference{
        .reference_node = second.get(),
        .definition_identity = outer->getIdentity(),
        .canonical_arguments = oneTypeArgument(*outer, parseType("UInt64")),
        .type_argument_lineage = {},
    };
    const std::array references{std::move(first_reference), std::move(second_reference)};
    TypeResolverLimits limits;
    limits.maximum_argument_validation_factory_calls = 1;
    expectResolverError(
        TypeResolverError::Code::LimitExceeded, [&] { static_cast<void>(TypeResolver::resolve(root, references, *adapter, limits)); });
}

TEST(UDTResolver, ArgumentLineageRejectsDuplicateMalformedAndCyclicGraphs)
{
    const auto outer = typeAliasDefinition(23);
    const auto inner = definition(24, "UInt64");
    const auto other = definition(25, "UInt64");
    const auto adapter = authority({outer, inner, other});

    ASTPtr inner_syntax = marker("db.Inner");
    ASTPtr outer_syntax = makeASTDataType("db.Outer", inner_syntax);
    auto duplicate_outer = referenceWithTypeArgument(outer_syntax, outer, oneTypeArgument(*outer, parseType("UInt64")), inner_syntax);
    duplicate_outer.type_argument_lineage.push_back(duplicate_outer.type_argument_lineage.front());
    const std::array duplicate_references{reference(inner_syntax, inner), std::move(duplicate_outer)};
    expectResolverError(
        TypeResolverError::Code::DuplicateArgumentLineage,
        [&] { static_cast<void>(TypeResolver::resolve(outer_syntax, duplicate_references, *adapter)); });

    ASTPtr other_syntax = marker("db.Other");
    auto malformed_outer = referenceWithTypeArgument(outer_syntax, outer, oneTypeArgument(*outer, parseType("UInt64")), other_syntax);
    const std::array malformed_references{reference(inner_syntax, inner), reference(other_syntax, other), std::move(malformed_outer)};
    expectResolverError(
        TypeResolverError::Code::InvalidArgumentLineage,
        [&] { static_cast<void>(TypeResolver::resolve(outer_syntax, malformed_references, *adapter)); });

    auto unknown_locator = referenceWithTypeArgument(outer_syntax, outer, oneTypeArgument(*outer, parseType("UInt64")), inner_syntax);
    unknown_locator.type_argument_lineage.front().path.push_back({static_cast<PhysicalTypeChildLocatorKind>(255), 0});
    const std::array unknown_locator_references{reference(inner_syntax, inner), std::move(unknown_locator)};
    expectResolverError(
        TypeResolverError::Code::InvalidArgumentLineage,
        [&] { static_cast<void>(TypeResolver::resolve(outer_syntax, unknown_locator_references, *adapter)); });

    auto cyclic_outer = referenceWithTypeArgument(outer_syntax, outer, oneTypeArgument(*outer, parseType("UInt64")), inner_syntax);
    auto cyclic_inner = reference(inner_syntax, inner);
    cyclic_inner.type_argument_lineage.push_back({.parameter = 0, .path = {}, .reference_node = outer_syntax.get()});
    const std::array cyclic_references{std::move(cyclic_inner), std::move(cyclic_outer)};
    expectResolverError(
        TypeResolverError::Code::ArgumentLineageCycle,
        [&] { static_cast<void>(TypeResolver::resolve(outer_syntax, cyclic_references, *adapter)); });
}

TEST(UDTResolver, TypeActualSyntaxMustMatchItsCanonicalPhysicalArgument)
{
    const auto outer = typeAliasDefinition(26);
    const auto inner = definition(27, "UInt64");
    const auto adapter = authority({outer, inner});

    ASTPtr string_actual = makeASTDataType("String");
    ASTPtr string_outer = makeASTDataType("db.Outer", string_actual);
    DeclaredTypeReferenceInput string_reference{
        .reference_node = string_outer.get(),
        .definition_identity = outer->getIdentity(),
        .canonical_arguments = oneTypeArgument(*outer, parseType("UInt64")),
        .type_argument_lineage = {},
    };
    const std::array string_references{std::move(string_reference)};
    expectResolverError(
        TypeResolverError::Code::CanonicalArgumentMismatch,
        [&] { static_cast<void>(TypeResolver::resolve(string_outer, string_references, *adapter)); });

    ASTPtr inner_syntax = marker("db.Inner");
    ASTPtr wrapped_actual = makeASTDataType("Array", inner_syntax);
    ASTPtr wrapped_outer = makeASTDataType("db.Outer", wrapped_actual);
    RelativePhysicalTypePath nested_path{{PhysicalTypeChildLocatorKind::StableOrdinal, 0}};
    const std::array wrapped_references{
        reference(inner_syntax, inner),
        referenceWithTypeArgument(wrapped_outer, outer, oneTypeArgument(*outer, parseType("UInt64")), inner_syntax, std::move(nested_path)),
    };
    expectResolverError(
        TypeResolverError::Code::CanonicalArgumentMismatch,
        [&] { static_cast<void>(TypeResolver::resolve(wrapped_outer, wrapped_references, *adapter)); });
}

TEST(UDTResolver, CompositeAggregateTypeActualPreservesEveryCanonicalFieldByte)
{
    const auto outer = typeAliasDefinition(101);
    const auto adapter = authority({outer});

    const auto resolve_parameter = [&](const String & parameter)
    {
        ASTPtr physical_actual = parseType("AggregateFunction(1, sumMapFiltered([1, 4, " + parameter + "]), Array(UInt64), Array(UInt64))");
        ASTPtr syntax = makeASTDataType("db.CompositeAggregate", physical_actual);
        DeclaredTypeReferenceInput input{
            .reference_node = syntax.get(),
            .definition_identity = outer->getIdentity(),
            .canonical_arguments = oneTypeArgument(*outer, physical_actual),
            .type_argument_lineage = {},
        };
        const std::array references{std::move(input)};
        return TypeResolver::resolve(syntax, references, *adapter);
    };

    const auto first = resolve_parameter("8");
    const auto different = resolve_parameter("9");
    ASSERT_TRUE(first.hasLogicalTree());
    ASSERT_TRUE(different.hasLogicalTree());
    EXPECT_EQ(first.getPhysicalType()->getName(), "AggregateFunction(1, sumMapFiltered([1, 4, 8]), Array(UInt64), Array(UInt64))");
    EXPECT_EQ(first.getLogicalTree()->getOccurrenceCount(), 1);
    EXPECT_EQ(different.getLogicalTree()->getOccurrenceCount(), 1);

    const auto & first_descriptor = first.getLogicalTree()->getDescriptors().front()->getPersistedDescriptor();
    const auto & different_descriptor = different.getLogicalTree()->getDescriptors().front()->getPersistedDescriptor();
    EXPECT_NE(first_descriptor.getCanonicalArgumentsEncoding(), different_descriptor.getCanonicalArgumentsEncoding());
    EXPECT_NE(first_descriptor.getInstantiationSemanticHash(), different_descriptor.getInstantiationSemanticHash());
    EXPECT_FALSE(first.getPhysicalType()->equals(*different.getPhysicalType()));
}

TEST(UDTResolver, EveryValueActualKindUsesExactLiteralSemanticsBeforeSpecialization)
{
    const auto boolean_definition = valueArgumentDefinition(32, ParameterKind::Bool);
    const auto signed_definition = valueArgumentDefinition(33, ParameterKind::Int8);
    const auto string_definition = valueArgumentDefinition(34, ParameterKind::String);
    const auto adapter = authority({boolean_definition, signed_definition, string_definition});
    ASTPtr boolean_syntax = makeASTDataType("db.Boolean", make_intrusive<ASTLiteral>(Field(true)));
    ASTPtr signed_syntax = makeASTDataType("db.Signed", make_intrusive<ASTLiteral>(Field(Int64{-7})));
    ASTPtr string_syntax = makeASTDataType("db.StringValue", make_intrusive<ASTLiteral>(Field(String("raw"))));
    ASTPtr root = makeASTDataType("Tuple", boolean_syntax, signed_syntax, string_syntax);
    const std::array references{
        referenceWithValueArgument(boolean_syntax, boolean_definition, CanonicalTypeArgumentValue::boolean(true)),
        referenceWithValueArgument(signed_syntax, signed_definition, CanonicalTypeArgumentValue::signedInteger(ParameterKind::Int8, -7)),
        referenceWithValueArgument(string_syntax, string_definition, CanonicalTypeArgumentValue::string("raw")),
    };
    const auto result = TypeResolver::resolve(root, references, *adapter);
    EXPECT_EQ(result.getPhysicalType()->getName(), "Tuple(UInt64, UInt64, UInt64)");
    EXPECT_EQ(result.getLogicalTree()->getOccurrenceCount(), 3);

    ASTPtr inexact_boolean = makeASTDataType("db.Boolean", make_intrusive<ASTLiteral>(Field(UInt64{1})));
    const std::array inexact_boolean_reference{
        referenceWithValueArgument(inexact_boolean, boolean_definition, CanonicalTypeArgumentValue::boolean(true))};
    expectResolverError(
        TypeResolverError::Code::CanonicalArgumentMismatch,
        [&] { static_cast<void>(TypeResolver::resolve(inexact_boolean, inexact_boolean_reference, *adapter)); });

    ASTPtr inexact_signed = makeASTDataType("db.Signed", make_intrusive<ASTLiteral>(Field(UInt64{7})));
    const std::array inexact_signed_reference{
        referenceWithValueArgument(inexact_signed, signed_definition, CanonicalTypeArgumentValue::signedInteger(ParameterKind::Int8, -7))};
    expectResolverError(
        TypeResolverError::Code::CanonicalArgumentMismatch,
        [&] { static_cast<void>(TypeResolver::resolve(inexact_signed, inexact_signed_reference, *adapter)); });

    ASTPtr inexact_string = makeASTDataType("db.StringValue", make_intrusive<ASTLiteral>(Field(String("other"))));
    const std::array inexact_string_reference{
        referenceWithValueArgument(inexact_string, string_definition, CanonicalTypeArgumentValue::string("raw"))};
    expectResolverError(
        TypeResolverError::Code::CanonicalArgumentMismatch,
        [&] { static_cast<void>(TypeResolver::resolve(inexact_string, inexact_string_reference, *adapter)); });
}

TEST(UDTResolver, ConditionalErasureFailsClosedAndSelectedActualPreservesLineage)
{
    const auto conditional = conditionalTypeAliasDefinition(28);
    const auto inner = definition(29, "UInt64");
    const auto adapter = authority({conditional, inner});

    ASTPtr mismatched_inner = marker("db.InnerMismatch");
    ASTPtr syntax_selector = make_intrusive<ASTLiteral>(Field(UInt64{3}));
    ASTPtr mismatched_outer = makeASTDataType("db.ConditionalMismatch", mismatched_inner, syntax_selector);
    auto mismatched_outer_reference = referenceWithTypeArgument(
        mismatched_outer, conditional, conditionalArguments(*conditional, parseType("UInt64"), 4), mismatched_inner);
    const std::array mismatched_references{reference(mismatched_inner, inner), std::move(mismatched_outer_reference)};
    expectResolverError(
        TypeResolverError::Code::CanonicalArgumentMismatch,
        [&] { static_cast<void>(TypeResolver::resolve(mismatched_outer, mismatched_references, *adapter)); });

    const auto run = [&](UInt64 selector)
    {
        ASTPtr inner_syntax = marker("db.Inner");
        ASTPtr selector_syntax = make_intrusive<ASTLiteral>(Field(selector));
        ASTPtr outer_syntax = makeASTDataType("db.Conditional", inner_syntax, selector_syntax);
        auto outer_reference = referenceWithTypeArgument(
            outer_syntax, conditional, conditionalArguments(*conditional, parseType("UInt64"), selector), inner_syntax);
        std::array references{reference(inner_syntax, inner), std::move(outer_reference)};
        return TypeResolver::resolve(outer_syntax, references, *adapter);
    };

    expectResolverError(TypeResolverError::Code::UnreachableArgumentLineage, [&] { static_cast<void>(run(0)); });

    const auto preserved = run(1);
    ASSERT_TRUE(preserved.hasLogicalTree());
    ASSERT_EQ(preserved.getLogicalTree()->getOccurrenceCount(), 2);
    const auto indices = preserved.getLogicalTree()->getDescriptorIndices(0);
    ASSERT_EQ(indices.size(), 2);
    const auto & descriptors = preserved.getLogicalTree()->getDescriptors();
    EXPECT_EQ(descriptors[indices[0]]->getDefinition()->getIdentity(), conditional->getIdentity());
    EXPECT_EQ(descriptors[indices[1]]->getDefinition()->getIdentity(), inner->getIdentity());
}

TEST(UDTResolver, SharedTypeActualASTCannotBypassParentSpecificLineageValidation)
{
    const auto outer = typeAliasDefinition(30);
    const auto inner = definition(31, "UInt64");
    const auto adapter = authority({outer, inner});
    ASTPtr shared_inner = marker("db.SharedInner");
    ASTPtr first_outer = makeASTDataType("db.FirstOuter", shared_inner);
    ASTPtr second_outer = makeASTDataType("db.SecondOuter", shared_inner);
    ASTPtr root = makeASTDataType("Tuple", first_outer, second_outer);
    auto first_reference = referenceWithTypeArgument(first_outer, outer, oneTypeArgument(*outer, parseType("UInt64")), shared_inner);
    DeclaredTypeReferenceInput incomplete_second{
        .reference_node = second_outer.get(),
        .definition_identity = outer->getIdentity(),
        .canonical_arguments = oneTypeArgument(*outer, parseType("UInt64")),
        .type_argument_lineage = {},
    };
    const std::array references{reference(shared_inner, inner), std::move(first_reference), std::move(incomplete_second)};

    expectResolverError(
        TypeResolverError::Code::UnsubstitutedReference, [&] { static_cast<void>(TypeResolver::resolve(root, references, *adapter)); });
}

TEST(UDTResolver, BinaryValidationStreamsWithinTheCanonicalBudgetForALargerEqualStateType)
{
    const auto outer = typeAliasDefinition(200);
    const auto adapter = authority({outer});
    ASTPtr canonical_ast = parseType("AggregateFunction(count, UInt8)");
    auto canonical_arguments = oneTypeArgument(*outer, canonical_ast);
    const auto & canonical = std::get<CanonicalTypeArgument>(canonical_arguments.values().front().value);

    String wide_actual_sql = "AggregateFunction(count, Tuple(";
    for (size_t index = 0; index < 64; ++index)
    {
        if (index != 0)
            wide_actual_sql += ", ";
        wide_actual_sql += "UInt8";
    }
    wide_actual_sql += "))";
    ASTPtr wide_actual = parseType(wide_actual_sql);
    const auto wide_physical = DataTypeFactory::instance().get(wide_actual);
    ASSERT_TRUE(wide_physical->equals(*canonical.getPhysicalType()));
    ASSERT_GT(encodeDataType(wide_physical).size(), canonical.getBinaryEncoding().size());
    const UInt64 canonical_binary_bytes = canonical.getBinaryEncoding().size();

    ASTPtr syntax = makeASTDataType("db.BinaryBound", wide_actual);
    DeclaredTypeReferenceInput input{
        .reference_node = syntax.get(),
        .definition_identity = outer->getIdentity(),
        .canonical_arguments = std::move(canonical_arguments),
        .type_argument_lineage = {},
    };
    const std::array references{std::move(input)};
    TypeResolverLimits limits;
    limits.maximum_argument_validation_binary_bytes = canonical_binary_bytes;
    TypeResolverStatistics unchanged;
    unchanged.argument_validation_binary_bytes = 777;

    expectResolverError(
        TypeResolverError::Code::CanonicalArgumentMismatch,
        [&] { static_cast<void>(TypeResolver::resolve(syntax, references, *adapter, limits, &unchanged)); });
    EXPECT_EQ(unchanged.argument_validation_binary_bytes, 777);
}

TEST(UDTResolver, PhysicalizedTypeActualIsBoundedAfterNestedMarkerExpansion)
{
    constexpr size_t expansion_depth = 6;
    const auto outer = typeAliasDefinition(201);
    const auto inner = nestedArrayDefinition(202, expansion_depth);
    const auto adapter = authority({outer, inner});
    ASTPtr canonical_physical = wrapInArrays(makeASTDataType("UInt64"), expansion_depth);
    ASTPtr inner_syntax = marker("db.DeepInner");
    ASTPtr outer_syntax = makeASTDataType("db.DeepOuter", inner_syntax);
    const std::array references{
        reference(inner_syntax, inner),
        referenceWithTypeArgument(outer_syntax, outer, oneTypeArgument(*outer, canonical_physical), inner_syntax),
    };
    TypeResolverLimits limits;
    limits.maximum_argument_validation_ast_nodes = 4;
    TypeResolverStatistics unchanged;
    unchanged.argument_validation_physical_ast_nodes = 777;

    expectResolverError(
        TypeResolverError::Code::LimitExceeded,
        [&] { static_cast<void>(TypeResolver::resolve(outer_syntax, references, *adapter, limits, &unchanged)); });
    EXPECT_EQ(unchanged.argument_validation_physical_ast_nodes, 777);
}

TEST(UDTResolver, CombinedPhysicalASTDepthIsCheckedAfterMarkerSplicing)
{
    const auto inner = nestedArrayDefinition(203, 3);
    const auto adapter = authority({inner});
    ASTPtr inner_syntax = marker("db.SpliceDepth");
    ASTPtr root = wrapInArrays(inner_syntax, 2);
    const std::array references{reference(inner_syntax, inner)};
    TypeResolverLimits limits;
    /// The source has raw AST depth 5 and the replacement has depth 7, but
    /// their composed five-array type has raw AST depth 11.
    limits.maximum_physical_ast_depth = 9;
    TypeResolverStatistics unchanged;
    unchanged.maximum_physical_ast_depth = 777;

    expectResolverError(
        TypeResolverError::Code::LimitExceeded,
        [&] { static_cast<void>(TypeResolver::resolve(root, references, *adapter, limits, &unchanged)); });
    EXPECT_EQ(unchanged.maximum_physical_ast_depth, 777);
}

TEST(UDTResolver, FinalPhysicalASTChargesEveryNestedLiteralFieldStringBeforeFactory)
{
    AggregateFunctionStateData state{
        .name = String(256, 'n'),
        .data = String(256, 'd'),
    };
    Object object;
    object.emplace(String(64, 'k'), Field(Array{Field(Tuple{Field(std::move(state))})}));
    const auto target = aggregateFieldDefinition(205, Field(std::move(object)));
    const auto adapter = authority({target});
    ASTPtr root = marker("db.CompositeFieldBytes");
    const std::array references{reference(root, target)};
    TypeResolverLimits limits;
    limits.maximum_physical_ast_syntax_bytes = 128;
    TypeResolverStatistics unchanged;
    unchanged.physical_factory_calls = 777;
    unchanged.literal_field_nodes = 777;
    unchanged.physical_ast_syntax_bytes = 777;

    expectResolverError(
        TypeResolverError::Code::LimitExceeded,
        [&] { static_cast<void>(TypeResolver::resolve(root, references, *adapter, limits, &unchanged)); });
    EXPECT_EQ(unchanged.physical_factory_calls, 777);
    EXPECT_EQ(unchanged.literal_field_nodes, 777);
    EXPECT_EQ(unchanged.physical_ast_syntax_bytes, 777);
}

TEST(UDTResolver, DescriptorDepthAndRetainedPathElementsAreProspectivelyBounded)
{
    const auto target = definition(204, "UInt64");
    const auto adapter = authority({target});

    ASTPtr deep_logical = marker("db.DeepPath");
    ASTPtr deep_root = wrapInArrays(deep_logical, 2);
    const std::array deep_references{reference(deep_logical, target)};
    TypeResolverLimits depth_limits;
    depth_limits.descriptors.maximum_path_depth = 1;
    expectResolverError(
        TypeResolverError::Code::LimitExceeded,
        [&] { static_cast<void>(TypeResolver::resolve(deep_root, deep_references, *adapter, depth_limits)); });

    ASTPtr child_logical = marker("db.PathElements");
    ASTPtr tuple_root = makeASTDataType("Tuple", child_logical);
    const std::array tuple_references{reference(child_logical, target)};
    TypeResolverLimits path_limits;
    /// One occurrence path element plus one retained child-node path element;
    /// the actual tree-edge count alone would still fit this limit.
    path_limits.descriptors.maximum_edges = 1;
    expectResolverError(
        TypeResolverError::Code::LimitExceeded,
        [&] { static_cast<void>(TypeResolver::resolve(tuple_root, tuple_references, *adapter, path_limits)); });
}

TEST(UDTResolver, WideJSONArgumentLineageUsesOneSortedTopologyCache)
{
    constexpr size_t path_count = 16;
    const auto outer = typeAliasDefinition(100);
    String physical_sql = "JSON(";
    for (size_t index = 0; index < path_count; ++index)
    {
        if (index != 0)
            physical_sql += ", ";
        physical_sql += "p" + (index < 10 ? String("0") : String()) + std::to_string(index) + " UInt64";
    }
    physical_sql += ')';
    ASTPtr physical_actual = parseType(physical_sql);
    ASTPtr syntax_actual = physical_actual->clone();
    auto * syntax_type = syntax_actual->as<ASTDataType>();
    ASSERT_NE(syntax_type, nullptr);
    auto * syntax_arguments = syntax_type->getArguments()->as<ASTExpressionList>();
    ASSERT_NE(syntax_arguments, nullptr);

    std::vector<Definition::Ptr> definitions{outer};
    std::vector<ASTPtr> nested_syntax;
    nested_syntax.reserve(path_count);
    for (size_t index = 0; index < path_count; ++index)
    {
        auto * object = syntax_arguments->children[index]->as<ASTObjectTypeArgument>();
        ASSERT_NE(object, nullptr);
        auto * typed = object->path_with_type->as<ASTObjectTypedPathArgument>();
        ASSERT_NE(typed, nullptr);
        ASTPtr nested = marker("db.JSONInner" + std::to_string(index));
        typed->type = nested;
        typed->children[0] = nested;
        nested_syntax.push_back(std::move(nested));
        definitions.push_back(definition(101 + index, "UInt64"));
    }
    ASTPtr outer_syntax = makeASTDataType("db.JSONOuter", syntax_actual);
    std::vector<DeclaredTypeReferenceInput> references;
    references.reserve(path_count + 1);
    for (size_t index = 0; index < path_count; ++index)
        references.push_back(reference(nested_syntax[index], definitions[index + 1]));
    DeclaredTypeReferenceInput outer_reference{
        .reference_node = outer_syntax.get(),
        .definition_identity = outer->getIdentity(),
        .canonical_arguments = oneTypeArgument(*outer, physical_actual),
        .type_argument_lineage = {},
    };
    for (size_t index = 0; index < path_count; ++index)
        outer_reference.type_argument_lineage.push_back(
            {.parameter = 0,
             .path = {{PhysicalTypeChildLocatorKind::StableOrdinal, static_cast<UInt32>(index)}},
             .reference_node = nested_syntax[index].get()});
    references.push_back(std::move(outer_reference));
    const auto adapter = authority(std::move(definitions));
    TypeResolverStatistics statistics;

    const auto result = TypeResolver::resolve(outer_syntax, references, *adapter, {}, &statistics);

    ASSERT_TRUE(result.hasLogicalTree());
    EXPECT_EQ(result.getLogicalTree()->getOccurrenceCount(), path_count + 1);
    EXPECT_EQ(statistics.argument_validation_factory_calls, 1);
    for (UInt32 index = 0; index < path_count; ++index)
    {
        const std::array<UInt32, 1> path{index};
        ASSERT_TRUE(result.getLogicalTree()->findNode(path));
    }
}

}
