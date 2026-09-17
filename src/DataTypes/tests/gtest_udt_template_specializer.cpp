#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/UDT/CanonicalHash.h>
#include <DataTypes/UDT/TemplateChecker.h>
#include <DataTypes/UDT/TemplateSpecializer.h>

#include <Common/Exception.h>
#include <Common/FieldBinaryEncoding.h>

#include <Core/Field.h>

#include <IO/WriteBufferFromString.h>

#include <Parsers/ASTDataType.h>
#include <Parsers/ASTEnumDataType.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTObjectTypeArgument.h>
#include <Parsers/ASTTupleDataType.h>

#include <gtest/gtest.h>

#include <Common/typeid_cast.h>

#include <algorithm>
#include <array>
#include <memory>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

namespace DB::ErrorCodes
{
extern const int UNKNOWN_AGGREGATE_FUNCTION;
extern const int UNEXPECTED_AST_STRUCTURE;
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

UUID databaseUUID()
{
    return testUUID(0x550e8400e29b41d4ULL, 0xa716446655440000ULL);
}

DefinitionIdentity identity(UInt64 low)
{
    return {.database_uuid = databaseUUID(), .type_uuid = testUUID(0x1000000000000000ULL, low), .revision = 1};
}

TypeAuthorityCapabilities capabilities(bool decreasing_recursion = true)
{
    TypeAuthorityCapabilities result;
    result.mask = typeAuthorityCapabilityBit(TypeAuthorityCapability::TransientResolution)
        | typeAuthorityCapabilityBit(TypeAuthorityCapability::Limits) | typeAuthorityCapabilityBit(TypeAuthorityCapability::Templates);
    if (decreasing_recursion)
        result.mask |= typeAuthorityCapabilityBit(TypeAuthorityCapability::DecreasingRecursion);
    result.limits = {
        .maximum_definitions = 1'024,
        .maximum_definition_bytes = 1ULL << 20,
        .maximum_template_nodes = 4'096,
        .maximum_direct_dependencies = 256,
        .maximum_transitive_dependencies = 1'024,
        .maximum_checker_work = 65'536,
    };
    return result;
}

TemplateNode node(TemplateNodeKind kind)
{
    TemplateNode result;
    result.kind = kind;
    return result;
}

TemplateNode builtIn(String name, std::vector<TemplateNodeChild> children = {})
{
    auto result = node(TemplateNodeKind::BuiltIn);
    result.atom = std::move(name);
    result.children = std::move(children);
    return result;
}

DefinitionInput scalarDefinition(String name, UInt64 id, String family)
{
    DefinitionInput result;
    result.identity = identity(id);
    result.normalized_name = std::move(name);
    result.nodes = {builtIn(std::move(family))};
    return result;
}

DefinitionInput typeAliasDefinition(String name, UInt64 id)
{
    DefinitionInput result;
    result.identity = identity(id);
    result.normalized_name = std::move(name);
    result.parameters = {{.normalized_name = "T", .kind = ParameterKind::Type}};
    auto formal = node(TemplateNodeKind::TypeParameter);
    result.nodes = {std::move(formal)};
    return result;
}

TemplateNode definitionCall(UInt16 dependency_ordinal, std::vector<TemplateNodeChild> arguments = {})
{
    auto result = node(TemplateNodeKind::DefinitionCall);
    result.dependency_ordinal = dependency_ordinal;
    result.children = std::move(arguments);
    return result;
}

DefinitionDependency dependencyFor(const Definition & target)
{
    return {
        .type_uuid = target.getIdentity().type_uuid,
        .revision = target.getIdentity().revision,
        .target_definition_hash = target.getDefinitionHash(),
    };
}

std::vector<Definition::Ptr> checkWithSingleDependency(DefinitionInput caller, DefinitionInput target)
{
    const auto target_seed = TemplateChecker::checkAll({target}).front();
    caller.dependencies = {dependencyFor(*target_seed)};
    return TemplateChecker::checkAll({std::move(caller), std::move(target)});
}

const Definition::Ptr &
byIdentity(const std::vector<Definition::Ptr> & definitions, const DefinitionIdentity & target)
{
    const auto found = std::find_if(
        definitions.begin(), definitions.end(), [&](const auto & definition) { return definition->getIdentity() == target; });
    if (found == definitions.end())
        throw std::logic_error("checked test definition is absent");
    return *found;
}

AuthorityAdapterPtr authority(std::vector<Definition::Ptr> definitions, bool decreasing = true)
{
    return makeTransientAuthorityAdapter(databaseUUID(), capabilities(decreasing), std::move(definitions));
}

CanonicalTypeArguments noArguments()
{
    return CanonicalTypeArguments::validate({}, {});
}

CanonicalTypeArguments oneTypeArgument(const Definition & definition, String family = "UInt64")
{
    auto ast = make_intrusive<ASTDataType>();
    ast->name = std::move(family);
    std::vector<CanonicalTypeArgumentValue> values;
    values.push_back(CanonicalTypeArgumentValue::type(ast));
    return CanonicalTypeArguments::validate(definition.getParameters(), std::move(values));
}

CanonicalTypeArguments recursiveArguments(const Definition & definition, UInt64 depth)
{
    auto ast = make_intrusive<ASTDataType>();
    ast->name = "UInt64";
    std::vector<CanonicalTypeArgumentValue> values;
    values.push_back(CanonicalTypeArgumentValue::type(ast));
    values.push_back(CanonicalTypeArgumentValue::unsignedInteger(ParameterKind::UInt64, depth));
    return CanonicalTypeArguments::validate(definition.getParameters(), std::move(values));
}

CanonicalTypeArguments oneUnsignedArgument(const Definition & definition, UInt64 value)
{
    std::vector<CanonicalTypeArgumentValue> values;
    values.push_back(CanonicalTypeArgumentValue::unsignedInteger(ParameterKind::UInt64, value));
    return CanonicalTypeArguments::validate(definition.getParameters(), std::move(values));
}

template <typename Function>
void expectSpecializerError(TemplateSpecializerError::Code expected, Function && function, std::string_view expected_message = {})
{
    try
    {
        function();
        FAIL() << "expected TemplateSpecializerError";
    }
    catch (const TemplateSpecializerError & error)
    {
        EXPECT_EQ(error.code, expected) << error.what();
        if (!expected_message.empty())
            EXPECT_NE(std::string_view(error.what()).find(expected_message), std::string_view::npos) << error.what();
    }
}

template <typename Function>
void expectDBError(int expected, Function && function)
{
    try
    {
        function();
        FAIL() << "expected DB::Exception";
    }
    catch (const Exception & error)
    {
        EXPECT_EQ(error.code(), expected) << error.message();
    }
}

class InvalidLimitsAuthority final : public IAuthorityAdapter
{
public:
    InvalidLimitsAuthority()
    {
        advertised.mask = typeAuthorityCapabilityBit(TypeAuthorityCapability::TransientResolution)
            | typeAuthorityCapabilityBit(TypeAuthorityCapability::Limits)
            | typeAuthorityCapabilityBit(TypeAuthorityCapability::Templates);
    }

    const TypeAuthorityCapabilities & getCapabilities() const noexcept override { return advertised; }
    UUID getDatabaseUUID() const noexcept override
    {
        ++database_calls;
        return databaseUUID();
    }
    ResolutionSession beginResolutionSession() const override
    {
        ++session_calls;
        throw std::runtime_error("invalid Limits authority opened a session");
    }
    void requireCapabilities(TypeAuthorityCapabilityMask, std::string_view) const override
    {
        ++require_calls;
        throw std::runtime_error("invalid Limits authority checked capabilities late");
    }

    mutable UInt64 database_calls = 0;
    mutable UInt64 session_calls = 0;
    mutable UInt64 require_calls = 0;

private:
    TypeAuthorityCapabilities advertised;
};

/// Deliberately advertises limits independently of the already-validated
/// backing generation. This models an arbitrary adapter implementation and
/// makes specialization responsible for enforcing the advertised contract.
class CountingLimitsAuthority final : public IAuthorityAdapter
{
public:
    CountingLimitsAuthority(TypeAuthorityCapabilities advertised_, std::vector<Definition::Ptr> definitions)
        : advertised(std::move(advertised_))
        , backing(authority(std::move(definitions)))
    {
    }

    const TypeAuthorityCapabilities & getCapabilities() const noexcept override { return advertised; }
    UUID getDatabaseUUID() const noexcept override
    {
        ++database_calls;
        return databaseUUID();
    }
    ResolutionSession beginResolutionSession() const override
    {
        ++session_calls;
        return backing->beginResolutionSession();
    }
    void requireCapabilities(TypeAuthorityCapabilityMask required, std::string_view) const override
    {
        ++require_calls;
        if (!advertised.containsAll(required))
            throw std::runtime_error("required capability is absent");
    }

    mutable UInt64 database_calls = 0;
    mutable UInt64 session_calls = 0;
    mutable UInt64 require_calls = 0;

private:
    TypeAuthorityCapabilities advertised;
    AuthorityAdapterPtr backing;
};

std::vector<Definition::Ptr> definitionsWithTwoDirectDependencies()
{
    auto first = scalarDefinition("FirstDirectDependency", 0x291, "UInt64");
    auto second = scalarDefinition("SecondDirectDependency", 0x292, "String");
    const auto checked_targets = TemplateChecker::checkAll({first, second});

    DefinitionInput caller;
    caller.identity = identity(0x293);
    caller.normalized_name = "TwoDirectDependencies";
    caller.nodes = {
        builtIn("Tuple", {{.reference = 1, .label = {}}, {.reference = 2, .label = {}}}),
        definitionCall(0),
        definitionCall(1),
    };
    caller.dependencies
        = {dependencyFor(*byIdentity(checked_targets, first.identity)), dependencyFor(*byIdentity(checked_targets, second.identity))};
    return TemplateChecker::checkAll({std::move(caller), std::move(first), std::move(second)});
}

void expectReturnedDefinitionLimitFailure(
    TypeAuthorityCapabilities advertised, Definition::Ptr definition, std::string_view expected_message)
{
    const DefinitionIdentity requested_identity = definition->getIdentity();
    std::weak_ptr<const Definition> observer = definition;
    auto adapter = std::make_shared<CountingLimitsAuthority>(std::move(advertised), std::vector{definition});
    definition.reset();

    auto attempt = TemplateSpecializer::Attempt::begin(*adapter);
    EXPECT_EQ(adapter->database_calls, 1);
    EXPECT_EQ(adapter->require_calls, 1);
    EXPECT_EQ(adapter->session_calls, 1);
    adapter.reset();
    EXPECT_FALSE(observer.expired());
    expectSpecializerError(
        TemplateSpecializerError::Code::LimitExceeded,
        [&] { static_cast<void>(attempt.specialize(requested_identity, noArguments())); },
        expected_message);
    /// The failed definition was never retained or committed. Poisoning drops
    /// the pinned session and every transient lookup result deterministically.
    EXPECT_TRUE(observer.expired());
    expectSpecializerError(TemplateSpecializerError::Code::InvalidAttemptState, [&] { static_cast<void>(attempt.finish()); });
}

void expectStableSpecializationPath(
    const RelativeLogicalTypeOccurrence & occurrence, TemplateSpecializationID specialization, std::span<const UInt32> ordinals)
{
    EXPECT_EQ(occurrence.kind, RelativeLogicalTypeOccurrenceKind::Specialization);
    EXPECT_EQ(occurrence.source_ordinal, specialization);
    ASSERT_EQ(occurrence.path.size(), ordinals.size());
    for (std::size_t index = 0; index < ordinals.size(); ++index)
    {
        EXPECT_EQ(occurrence.path[index].kind, PhysicalTypeChildLocatorKind::StableOrdinal);
        EXPECT_EQ(occurrence.path[index].source_ordinal, ordinals[index]);
    }
}

void expectStableTypeArgumentPath(const RelativeLogicalTypeOccurrence & occurrence, UInt16 parameter, std::span<const UInt32> ordinals)
{
    EXPECT_EQ(occurrence.kind, RelativeLogicalTypeOccurrenceKind::TypeArgument);
    EXPECT_EQ(occurrence.source_ordinal, parameter);
    ASSERT_EQ(occurrence.path.size(), ordinals.size());
    for (std::size_t index = 0; index < ordinals.size(); ++index)
    {
        EXPECT_EQ(occurrence.path[index].kind, PhysicalTypeChildLocatorKind::StableOrdinal);
        EXPECT_EQ(occurrence.path[index].source_ordinal, ordinals[index]);
    }
}

DefinitionInput transparentCaller(String name, UInt64 id, const DefinitionIdentity & target)
{
    DefinitionInput result;
    result.identity = identity(id);
    result.normalized_name = std::move(name);
    result.parameters = {{.normalized_name = "T", .kind = ParameterKind::Type}};
    result.nodes = {definitionCall(0, {{.reference = 0, .label = {}}})};
    result.dependencies = {{.type_uuid = target.type_uuid, .revision = target.revision}};
    return result;
}

DefinitionInput recursiveDefinition()
{
    DefinitionInput result;
    result.identity = identity(0x400);
    result.normalized_name = "RecursiveArray";
    result.parameters = {
        {.normalized_name = "T", .kind = ParameterKind::Type},
        {.normalized_name = "N", .kind = ParameterKind::UInt64},
    };
    result.decreasing_parameter = 1;
    result.checker_abi = 2;
    auto choose = node(TemplateNodeKind::TypeIfZero);
    choose.parameter = 1;
    choose.children = {{.reference = 1, .label = {}}, {.reference = 2, .label = {}}};
    auto formal = node(TemplateNodeKind::TypeParameter);
    auto array = builtIn("Array", {{.reference = 3, .label = {}}});
    auto self = node(TemplateNodeKind::SelfCall);
    self.parameter = 1;
    self.decrement = 1;
    result.nodes = {std::move(choose), std::move(formal), std::move(array), std::move(self)};
    return result;
}

DefinitionInput v3FactorySurfaceDefinition()
{
    DefinitionInput result;
    result.identity = identity(0x410);
    result.normalized_name = "V3FactorySurface";
    result.parameters = {{.normalized_name = "N", .kind = ParameterKind::UInt64}};

    auto value_parameter = node(TemplateNodeKind::ValueParameter);
    value_parameter.parameter = 0;
    /// The two uses are distinct syntactic occurrences at different depths;
    /// V3 structural references remain in strict parent-before-child order.
    auto dynamic_value_parameter = value_parameter;
    auto signed_literal = node(TemplateNodeKind::SignedLiteral);
    signed_literal.signed_literal = 2;
    auto string_literal = node(TemplateNodeKind::StringLiteral);
    string_literal.text = "UTC";
    auto identifier = node(TemplateNodeKind::Identifier);
    identifier.text = "UInt64";
    auto specialized_enum = node(TemplateNodeKind::SpecializedEnum);
    specialized_enum.specialized_enum_width = SpecializedEnumWidth::Enum8;
    specialized_enum.enum_entries = {{.name = "off", .value = -1}, {.name = "on", .value = 1}};
    auto dynamic_setting = node(TemplateNodeKind::DynamicSetting);
    dynamic_setting.text = "max_types";
    dynamic_setting.children = {{.reference = 12, .label = {}}};

    result.nodes = {
        builtIn(
            "Tuple",
            {
                {.reference = 1, .label = {}},
                {.reference = 3, .label = {}},
                {.reference = 5, .label = {}},
                {.reference = 7, .label = {}},
                {.reference = 9, .label = {}},
                {.reference = 10, .label = {}},
            }),
        builtIn("FixedString", {{.reference = 2, .label = {}}}),
        std::move(value_parameter),
        builtIn("Decimal32", {{.reference = 4, .label = {}}}),
        std::move(signed_literal),
        builtIn("DateTime", {{.reference = 6, .label = {}}}),
        std::move(string_literal),
        builtIn("Array", {{.reference = 8, .label = {}}}),
        std::move(identifier),
        std::move(specialized_enum),
        builtIn("Dynamic", {{.reference = 11, .label = {}}}),
        std::move(dynamic_setting),
        std::move(dynamic_value_parameter),
    };
    return result;
}

DefinitionInput booleanLiteralSurfaceDefinition()
{
    DefinitionInput result;
    result.identity = identity(0x411);
    result.normalized_name = "BooleanLiteralSurface";
    auto boolean_literal = node(TemplateNodeKind::BooleanLiteral);
    boolean_literal.boolean_literal = true;
    result.nodes = {builtIn("FixedString", {{.reference = 1, .label = {}}}), std::move(boolean_literal)};
    return result;
}

DefinitionInput fieldInventoryDefinition(const Array & values)
{
    DefinitionInput result;
    result.identity = identity(0x412);
    result.normalized_name = "CanonicalFieldInventory";
    auto function = node(TemplateNodeKind::AggregateFunction);
    function.text = "udtCanonicalFieldInventoryFunctionThatMustNotExist";
    result.nodes = {
        builtIn("AggregateFunction", {{.reference = 1, .label = {}}, {.reference = 2, .label = {}}}),
        std::move(function),
        builtIn("UInt64"),
    };
    for (const TemplateNodeID root : appendCanonicalFieldValues(values, result.nodes))
        result.nodes[1].children.push_back({.reference = root, .label = {}});
    return result;
}

}

TEST(UDTTemplateSpecializer, TransparentDefinitionCallPreservesEverySamePathOccurrence)
{
    auto target_input = typeAliasDefinition("InnerAlias", 0x200);
    auto caller_input = transparentCaller("OuterAlias", 0x100, target_input.identity);
    const auto definitions = checkWithSingleDependency(std::move(caller_input), std::move(target_input));
    const auto & caller = byIdentity(definitions, identity(0x100));
    auto adapter = authority(definitions);

    auto attempt = TemplateSpecializer::Attempt::begin(*adapter);
    const auto id = attempt.specialize(caller->getIdentity(), oneTypeArgument(*caller));
    ASSERT_EQ(id, 0U);
    EXPECT_EQ(attempt.getCanonicalPhysicalAST(id)->as<ASTDataType>()->name, "UInt64");
    const auto finished = attempt.finish();

    ASSERT_EQ(finished.specializations.size(), 2U);
    ASSERT_EQ(finished.specializations[0].relative_occurrences.size(), 3U);
    expectStableSpecializationPath(finished.specializations[0].relative_occurrences[0], 0, {});
    expectStableSpecializationPath(finished.specializations[0].relative_occurrences[1], 1, {});
    expectStableTypeArgumentPath(finished.specializations[0].relative_occurrences[2], 0, {});
    EXPECT_EQ(DataTypeFactory::instance().get(finished.specializations[0].canonical_physical_ast)->getName(), "UInt64");
    EXPECT_EQ(finished.statistics.resolution_sessions, 1U);
    EXPECT_EQ(finished.statistics.distinct_specializations, 2U);
    EXPECT_EQ(finished.statistics.definition_lookups, 2U);
    EXPECT_EQ(finished.definition_handles.size(), 2U);
    EXPECT_TRUE(
        std::is_sorted(
            finished.definition_handles.begin(),
            finished.definition_handles.end(),
            [](const auto & lhs, const auto & rhs)
            { return uuidToCanonicalBytes(lhs->getIdentity().type_uuid) < uuidToCanonicalBytes(rhs->getIdentity().type_uuid); }));
    expectSpecializerError(TemplateSpecializerError::Code::InvalidAttemptState, [&] { static_cast<void>(attempt.finish()); });
}

TEST(UDTTemplateSpecializer, RepeatedMemoOccurrenceRetainsBothPhysicalPaths)
{
    auto target_input = typeAliasDefinition("RepeatedInner", 0x220);
    DefinitionInput caller;
    caller.identity = identity(0x120);
    caller.normalized_name = "RepeatedOuter";
    caller.parameters = {{.normalized_name = "T", .kind = ParameterKind::Type}};
    caller.nodes = {
        builtIn("Tuple", {{.reference = 1, .label = {}}, {.reference = 1, .label = {}}}),
        definitionCall(0, {{.reference = 0, .label = {}}}),
    };
    caller.dependencies = {{.type_uuid = target_input.identity.type_uuid, .revision = 1}};
    const auto definitions = checkWithSingleDependency(std::move(caller), std::move(target_input));
    const auto & outer = byIdentity(definitions, identity(0x120));
    auto adapter = authority(definitions);

    auto attempt = TemplateSpecializer::Attempt::begin(*adapter);
    const auto id = attempt.specialize(outer->getIdentity(), oneTypeArgument(*outer));
    const auto finished = attempt.finish();

    ASSERT_EQ(finished.specializations[id].relative_occurrences.size(), 5U);
    expectStableSpecializationPath(finished.specializations[id].relative_occurrences[0], id, {});
    const std::array<UInt32, 1> zero{0};
    const std::array<UInt32, 1> one{1};
    expectStableSpecializationPath(finished.specializations[id].relative_occurrences[1], 1, zero);
    expectStableTypeArgumentPath(finished.specializations[id].relative_occurrences[2], 0, zero);
    expectStableSpecializationPath(finished.specializations[id].relative_occurrences[3], 1, one);
    expectStableTypeArgumentPath(finished.specializations[id].relative_occurrences[4], 0, one);
    EXPECT_EQ(finished.statistics.specialization_memo_hits, 1U);
    EXPECT_EQ(DataTypeFactory::instance().get(finished.specializations[id].canonical_physical_ast)->getName(), "Tuple(UInt64, UInt64)");
}

TEST(UDTTemplateSpecializer, NamedTupleUsesDirectBinaryChildOrdinals)
{
    auto target_input = scalarDefinition("PayloadText", 0x230, "String");
    DefinitionInput caller;
    caller.identity = identity(0x130);
    caller.normalized_name = "NamedRecord";
    caller.nodes = {
        builtIn("Tuple", {{.reference = 1, .label = "payload"}, {.reference = 2, .label = "id"}}),
        definitionCall(0),
        builtIn("UInt64"),
    };
    caller.dependencies = {{.type_uuid = target_input.identity.type_uuid, .revision = 1}};
    const auto definitions = checkWithSingleDependency(std::move(caller), std::move(target_input));
    const auto & outer = byIdentity(definitions, identity(0x130));
    auto adapter = authority(definitions);

    auto attempt = TemplateSpecializer::Attempt::begin(*adapter);
    const auto id = attempt.specialize(outer->getIdentity(), noArguments());
    const auto finished = attempt.finish();

    const auto * tuple = finished.specializations[id].canonical_physical_ast->as<ASTTupleDataType>();
    ASSERT_NE(tuple, nullptr);
    EXPECT_EQ(tuple->element_names, (Strings{"payload", "id"}));
    ASSERT_EQ(finished.specializations[id].relative_occurrences.size(), 2U);
    const std::array<UInt32, 1> payload_path{0};
    expectStableSpecializationPath(finished.specializations[id].relative_occurrences[1], 1, payload_path);
    EXPECT_EQ(
        DataTypeFactory::instance().get(finished.specializations[id].canonical_physical_ast)->getName(),
        "Tuple(payload String, id UInt64)");
}

TEST(UDTTemplateSpecializer, DecreasingRecursionExpandsFinitelyAndPreservesLineage)
{
    const auto definitions = TemplateChecker::checkAll({recursiveDefinition()});
    const auto & recursive = definitions.front();
    auto adapter = authority(definitions);

    auto attempt = TemplateSpecializer::Attempt::begin(*adapter);
    const auto id = attempt.specialize(recursive->getIdentity(), recursiveArguments(*recursive, 2));
    const auto finished = attempt.finish();

    ASSERT_EQ(finished.specializations.size(), 3U);
    ASSERT_EQ(finished.specializations[id].relative_occurrences.size(), 4U);
    expectStableSpecializationPath(finished.specializations[id].relative_occurrences[0], 0, {});
    const std::array<UInt32, 1> one_level{0};
    const std::array<UInt32, 2> two_levels{0, 0};
    expectStableSpecializationPath(finished.specializations[id].relative_occurrences[1], 1, one_level);
    expectStableSpecializationPath(finished.specializations[id].relative_occurrences[2], 2, two_levels);
    expectStableTypeArgumentPath(finished.specializations[id].relative_occurrences[3], 0, two_levels);
    EXPECT_EQ(DataTypeFactory::instance().get(finished.specializations[id].canonical_physical_ast)->getName(), "Array(Array(UInt64))");
    EXPECT_EQ(finished.statistics.maximum_specialization_depth, 3U);
}

TEST(UDTTemplateSpecializer, JSONRetainsCanonicalArgumentsAndUsesBinaryTypedPathOrdinals)
{
    auto target_input = scalarDefinition("JSONText", 0x250, "String");
    DefinitionInput caller;
    caller.identity = identity(0x150);
    caller.normalized_name = "JSONEnvelope";
    auto max_types = node(TemplateNodeKind::ObjectSetting);
    max_types.text = "max_dynamic_types";
    max_types.children = {{.reference = 9, .label = {}}};
    auto max_paths = node(TemplateNodeKind::ObjectSetting);
    max_paths.text = "max_dynamic_paths";
    max_paths.children = {{.reference = 10, .label = {}}};
    auto a_path = node(TemplateNodeKind::ObjectTypedPath);
    a_path.text = "a.path";
    a_path.children = {{.reference = 11, .label = {}}};
    auto z_path = node(TemplateNodeKind::ObjectTypedPath);
    z_path.text = "z.path";
    z_path.children = {{.reference = 11, .label = {}}};
    auto skip_a = node(TemplateNodeKind::ObjectSkipPath);
    skip_a.text = "ignored.a";
    auto skip_z = node(TemplateNodeKind::ObjectSkipPath);
    skip_z.text = "ignored.z";
    auto regexp_a = node(TemplateNodeKind::ObjectSkipRegexp);
    regexp_a.text = "^secret\\.";
    auto regexp_b = regexp_a;
    auto max_types_value = node(TemplateNodeKind::UnsignedLiteral);
    max_types_value.unsigned_literal = 4;
    auto max_paths_value = node(TemplateNodeKind::UnsignedLiteral);
    max_paths_value.unsigned_literal = 64;
    caller.nodes = {
        builtIn(
            "JSON",
            {
                {.reference = 1, .label = {}},
                {.reference = 2, .label = {}},
                {.reference = 3, .label = {}},
                {.reference = 4, .label = {}},
                {.reference = 5, .label = {}},
                {.reference = 6, .label = {}},
                {.reference = 7, .label = {}},
                {.reference = 8, .label = {}},
            }),
        std::move(max_types),
        std::move(max_paths),
        std::move(a_path),
        std::move(z_path),
        std::move(skip_a),
        std::move(skip_z),
        std::move(regexp_a),
        std::move(regexp_b),
        std::move(max_types_value),
        std::move(max_paths_value),
        definitionCall(0),
    };
    caller.dependencies = {{.type_uuid = target_input.identity.type_uuid, .revision = 1}};
    const auto definitions = checkWithSingleDependency(std::move(caller), std::move(target_input));
    const auto & outer = byIdentity(definitions, identity(0x150));
    auto adapter = authority(definitions);

    auto attempt = TemplateSpecializer::Attempt::begin(*adapter);
    const auto id = attempt.specialize(outer->getIdentity(), noArguments());
    const auto finished = attempt.finish();

    const auto * json = finished.specializations[id].canonical_physical_ast->as<ASTDataType>();
    ASSERT_NE(json, nullptr);
    const auto * arguments = json->getArguments()->as<ASTExpressionList>();
    ASSERT_NE(arguments, nullptr);
    ASSERT_EQ(arguments->children.size(), 8U);
    const auto * max_types_argument = arguments->children[0]->as<ASTObjectTypeArgument>();
    const auto * max_paths_argument = arguments->children[1]->as<ASTObjectTypeArgument>();
    ASSERT_NE(max_types_argument, nullptr);
    ASSERT_NE(max_paths_argument, nullptr);
    ASSERT_NE(max_types_argument->parameter, nullptr);
    ASSERT_NE(max_paths_argument->parameter, nullptr);
    const auto * first = arguments->children[2]->as<ASTObjectTypeArgument>();
    const auto * second = arguments->children[3]->as<ASTObjectTypeArgument>();
    ASSERT_NE(first, nullptr);
    ASSERT_NE(second, nullptr);
    ASSERT_NE(first->path_with_type, nullptr);
    ASSERT_NE(second->path_with_type, nullptr);
    const auto * first_typed_path = first->path_with_type->as<ASTObjectTypedPathArgument>();
    const auto * second_typed_path = second->path_with_type->as<ASTObjectTypedPathArgument>();
    ASSERT_NE(first_typed_path, nullptr);
    ASSERT_NE(second_typed_path, nullptr);
    EXPECT_EQ(first_typed_path->path, "a.path");
    EXPECT_EQ(second_typed_path->path, "z.path");
    ASSERT_EQ(finished.specializations[id].relative_occurrences.size(), 3U);
    const std::array<UInt32, 1> first_path{0};
    const std::array<UInt32, 1> second_path{1};
    expectStableSpecializationPath(finished.specializations[id].relative_occurrences[1], 1, first_path);
    expectStableSpecializationPath(finished.specializations[id].relative_occurrences[2], 1, second_path);
    EXPECT_EQ(finished.statistics.specialization_memo_hits, 1U);
    const auto physical = DataTypeFactory::instance().get(finished.specializations[id].canonical_physical_ast);
    const auto * object = typeid_cast<const DataTypeObject *>(physical.get());
    ASSERT_NE(object, nullptr);
    EXPECT_EQ(object->getMaxDynamicTypes(), 4U);
    EXPECT_EQ(object->getMaxDynamicPaths(), 64U);
    ASSERT_EQ(object->getPathRegexpsToSkip().size(), 2U);
    EXPECT_EQ(object->getPathRegexpsToSkip()[0], "^secret\\.");
    EXPECT_EQ(object->getPathRegexpsToSkip()[1], "^secret\\.");
}

TEST(UDTTemplateSpecializer, VariantDefersOnlyItsNormalizationBoundary)
{
    auto target_input = scalarDefinition("VariantText", 0x260, "String");
    DefinitionInput caller;
    caller.identity = identity(0x160);
    caller.normalized_name = "VariantEnvelope";
    caller.nodes = {
        builtIn("Variant", {{.reference = 1, .label = {}}, {.reference = 2, .label = {}}}),
        definitionCall(0),
        builtIn("UInt64"),
    };
    caller.dependencies = {{.type_uuid = target_input.identity.type_uuid, .revision = 1}};
    const auto definitions = checkWithSingleDependency(std::move(caller), std::move(target_input));
    const auto & outer = byIdentity(definitions, identity(0x160));
    auto adapter = authority(definitions);

    auto attempt = TemplateSpecializer::Attempt::begin(*adapter);
    const auto id = attempt.specialize(outer->getIdentity(), noArguments());
    const auto finished = attempt.finish();

    ASSERT_EQ(finished.specializations[id].relative_occurrences.size(), 2U);
    const auto & path = finished.specializations[id].relative_occurrences[1].path;
    ASSERT_EQ(path.size(), 1U);
    EXPECT_EQ(path.front().kind, PhysicalTypeChildLocatorKind::VariantNormalizedBranch);
    EXPECT_EQ(path.front().source_ordinal, 0U);
    EXPECT_EQ(DataTypeFactory::instance().get(finished.specializations[id].canonical_physical_ast)->getName(), "Variant(String, UInt64)");
}

TEST(UDTTemplateSpecializer, AggregateOrdinalsExcludeFunctionSurfaceArguments)
{
    auto target_input = scalarDefinition("AggregateValue", 0x270, "UInt64");
    DefinitionInput caller;
    caller.identity = identity(0x170);
    caller.normalized_name = "AggregateState";
    auto function = node(TemplateNodeKind::AggregateFunction);
    function.text = "sum";
    caller.nodes = {
        builtIn("AggregateFunction", {{.reference = 1, .label = {}}, {.reference = 2, .label = {}}}),
        std::move(function),
        definitionCall(0),
    };
    caller.dependencies = {{.type_uuid = target_input.identity.type_uuid, .revision = 1}};
    const auto definitions = checkWithSingleDependency(std::move(caller), std::move(target_input));
    const auto & outer = byIdentity(definitions, identity(0x170));
    auto adapter = authority(definitions);

    auto attempt = TemplateSpecializer::Attempt::begin(*adapter);
    const auto id = attempt.specialize(outer->getIdentity(), noArguments());
    const auto finished = attempt.finish();

    ASSERT_EQ(finished.specializations[id].relative_occurrences.size(), 2U);
    const std::array<UInt32, 1> argument_zero{0};
    expectStableSpecializationPath(finished.specializations[id].relative_occurrences[1], 1, argument_zero);
    const auto * type = finished.specializations[id].canonical_physical_ast->as<ASTDataType>();
    ASSERT_NE(type, nullptr);
    const auto * arguments = type->getArguments()->as<ASTExpressionList>();
    ASSERT_NE(arguments, nullptr);
    ASSERT_EQ(arguments->children.size(), 2U);
    EXPECT_NE(arguments->children[0]->as<ASTIdentifier>(), nullptr);
    EXPECT_NE(arguments->children[1]->as<ASTDataType>(), nullptr);
}

TEST(UDTTemplateSpecializer, CheckedCanonicalLiteralSurfacesReconstructFactoryAcceptedPhysicalTypes)
{
    const auto definitions = TemplateChecker::checkAll({v3FactorySurfaceDefinition()});
    const auto & definition = definitions.front();
    auto adapter = authority(definitions);

    auto attempt = TemplateSpecializer::Attempt::begin(*adapter);
    const auto id = attempt.specialize(definition->getIdentity(), oneUnsignedArgument(*definition, 7));
    const auto finished = attempt.finish();

    const ASTPtr & root = finished.specializations[id].canonical_physical_ast;
    const auto * tuple = root->as<ASTTupleDataType>();
    ASSERT_NE(tuple, nullptr);
    const auto * elements = tuple->getArguments()->as<ASTExpressionList>();
    ASSERT_NE(elements, nullptr);
    ASSERT_EQ(elements->children.size(), 6U);

    const auto * fixed = elements->children[0]->as<ASTDataType>();
    const auto * decimal = elements->children[1]->as<ASTDataType>();
    const auto * date_time = elements->children[2]->as<ASTDataType>();
    const auto * array = elements->children[3]->as<ASTDataType>();
    const auto * specialized_enum = elements->children[4]->as<ASTEnumDataType>();
    const auto * dynamic = elements->children[5]->as<ASTDataType>();
    ASSERT_NE(fixed, nullptr);
    ASSERT_NE(decimal, nullptr);
    ASSERT_NE(date_time, nullptr);
    ASSERT_NE(array, nullptr);
    ASSERT_NE(specialized_enum, nullptr);
    ASSERT_NE(dynamic, nullptr);

    const auto * fixed_arguments = fixed->getArguments()->as<ASTExpressionList>();
    const auto * decimal_arguments = decimal->getArguments()->as<ASTExpressionList>();
    const auto * date_time_arguments = date_time->getArguments()->as<ASTExpressionList>();
    const auto * array_arguments = array->getArguments()->as<ASTExpressionList>();
    const auto * dynamic_arguments = dynamic->getArguments()->as<ASTExpressionList>();
    ASSERT_NE(fixed_arguments, nullptr);
    ASSERT_NE(decimal_arguments, nullptr);
    ASSERT_NE(date_time_arguments, nullptr);
    ASSERT_NE(array_arguments, nullptr);
    ASSERT_NE(dynamic_arguments, nullptr);
    ASSERT_EQ(fixed_arguments->children.size(), 1U);
    ASSERT_EQ(decimal_arguments->children.size(), 1U);
    ASSERT_EQ(date_time_arguments->children.size(), 1U);
    ASSERT_EQ(array_arguments->children.size(), 1U);
    ASSERT_EQ(dynamic_arguments->children.size(), 1U);

    const auto * value_parameter = fixed_arguments->children[0]->as<ASTLiteral>();
    const auto * signed_literal = decimal_arguments->children[0]->as<ASTLiteral>();
    const auto * string_literal = date_time_arguments->children[0]->as<ASTLiteral>();
    const auto * identifier = array_arguments->children[0]->as<ASTIdentifier>();
    const auto * dynamic_setting = dynamic_arguments->children[0]->as<ASTFunction>();
    ASSERT_NE(value_parameter, nullptr);
    ASSERT_NE(signed_literal, nullptr);
    ASSERT_NE(string_literal, nullptr);
    ASSERT_NE(identifier, nullptr);
    ASSERT_NE(dynamic_setting, nullptr);
    EXPECT_EQ(value_parameter->value.getType(), Field::Types::UInt64);
    EXPECT_EQ(value_parameter->value.safeGet<UInt64>(), 7U);
    EXPECT_EQ(signed_literal->value.getType(), Field::Types::Int64);
    EXPECT_EQ(signed_literal->value.safeGet<Int64>(), 2);
    EXPECT_EQ(string_literal->value.getType(), Field::Types::String);
    EXPECT_EQ(string_literal->value.safeGet<String>(), "UTC");
    EXPECT_EQ(identifier->name(), "UInt64");
    EXPECT_EQ(specialized_enum->name, "Enum8");
    EXPECT_EQ(specialized_enum->values, (std::vector<std::pair<String, Int64>>{{"off", -1}, {"on", 1}}));
    EXPECT_EQ(dynamic_setting->name, "equals");
    ASSERT_NE(dynamic_setting->arguments, nullptr);
    ASSERT_EQ(dynamic_setting->arguments->children.size(), 2U);
    EXPECT_EQ(dynamic_setting->arguments->children[0]->as<ASTIdentifier>()->name(), "max_types");
    EXPECT_EQ(dynamic_setting->arguments->children[1]->as<ASTLiteral>()->value.safeGet<UInt64>(), 7U);

    /// This is the linked acceptance boundary: every checked V3 node above
    /// contributed to one physical type that the unmodified factory accepts.
    EXPECT_NO_THROW(static_cast<void>(DataTypeFactory::instance().get(root)));
}

TEST(UDTTemplateSpecializer, BooleanLiteralIsPreservedButNotPretendedToBeATypeArgument)
{
    const auto definitions = TemplateChecker::checkAll({booleanLiteralSurfaceDefinition()});
    auto adapter = authority(definitions);

    auto attempt = TemplateSpecializer::Attempt::begin(*adapter);
    const auto id = attempt.specialize(definitions.front()->getIdentity(), noArguments());
    const auto finished = attempt.finish();

    const ASTPtr & root = finished.specializations[id].canonical_physical_ast;
    const auto * fixed = root->as<ASTDataType>();
    ASSERT_NE(fixed, nullptr);
    const auto * arguments = fixed->getArguments()->as<ASTExpressionList>();
    ASSERT_NE(arguments, nullptr);
    ASSERT_EQ(arguments->children.size(), 1U);
    const auto * literal = arguments->children[0]->as<ASTLiteral>();
    ASSERT_NE(literal, nullptr);
    EXPECT_EQ(literal->value.getType(), Field::Types::Bool);
    EXPECT_TRUE(literal->value.safeGet<bool>());

    /// BooleanLiteral is representable in canonical V3 and must round-trip
    /// exactly. It is not currently a SQL-spellable physical data-type
    /// argument: registered numeric settings require UInt64, not Field::Bool.
    expectDBError(
        ErrorCodes::UNEXPECTED_AST_STRUCTURE,
        [&] { static_cast<void>(DataTypeFactory::instance().get(finished.specializations[id].canonical_physical_ast)); });
}

TEST(UDTTemplateSpecializer, CanonicalFieldInventoryPreservesEveryKindAndBinaryPayload)
{
    struct InventoryRow
    {
        std::string_view name;
        Field value;
        CanonicalFieldKind canonical_kind;
        bool sql_spellable;
    };

    Object object;
    object.emplace("a", Field(UInt64{1}));
    object.emplace("b", Field(String{"two"}));
    Map map{
        Field(Tuple{Field(UInt64{3}), Field(String{"three"})}),
        Field(Tuple{Field(UInt64{4}), Field(Object(object))}),
    };

    /// sql_spellable classifies the exact ParserDataType aggregate-parameter
    /// surface. Null, scalar CAST families, and Array are spellable. The two
    /// ordering sentinels, Tuple/Map/Object, and AggregateFunctionState are
    /// valid FieldBinaryEncoding values but have no parser spelling there.
    const std::array inventory{
        InventoryRow{"Null", Field(Null{}), CanonicalFieldKind::Null, true},
        InventoryRow{"NegativeInfinity sentinel", Field(NEGATIVE_INFINITY), CanonicalFieldKind::NegativeInfinity, false},
        InventoryRow{"PositiveInfinity sentinel", Field(POSITIVE_INFINITY), CanonicalFieldKind::PositiveInfinity, false},
        InventoryRow{"UInt64", Field(UInt64{42}), CanonicalFieldKind::UInt64, true},
        InventoryRow{"Int64", Field(Int64{-42}), CanonicalFieldKind::Int64, true},
        InventoryRow{"Float64", Field(Float64{-0.0}), CanonicalFieldKind::Float64, true},
        InventoryRow{"binary String", Field(String{"a\0b", 3}), CanonicalFieldKind::String, true},
        InventoryRow{"Bool", Field(true), CanonicalFieldKind::Bool, true},
        InventoryRow{"UInt128", Field(UInt128{42}), CanonicalFieldKind::UInt128, true},
        InventoryRow{"Int128", Field(Int128{-42}), CanonicalFieldKind::Int128, true},
        InventoryRow{"UInt256", Field(UInt256{42}), CanonicalFieldKind::UInt256, true},
        InventoryRow{"Int256", Field(Int256{-42}), CanonicalFieldKind::Int256, true},
        InventoryRow{"Decimal32", Field(DecimalField<Decimal32>(42, 3)), CanonicalFieldKind::Decimal32, true},
        InventoryRow{"Decimal64", Field(DecimalField<Decimal64>(42, 3)), CanonicalFieldKind::Decimal64, true},
        InventoryRow{"Decimal128", Field(DecimalField<Decimal128>(Int128{42}, 3)), CanonicalFieldKind::Decimal128, true},
        InventoryRow{"Decimal256", Field(DecimalField<Decimal256>(Int256{42}, 3)), CanonicalFieldKind::Decimal256, true},
        InventoryRow{"UUID", Field(UUID{42}), CanonicalFieldKind::UUID, true},
        InventoryRow{"IPv4", Field(IPv4{42}), CanonicalFieldKind::IPv4, true},
        InventoryRow{"IPv6", Field(IPv6{42}), CanonicalFieldKind::IPv6, true},
        InventoryRow{"Array", Field(Array{Field(UInt64{1}), Field(Null{})}), CanonicalFieldKind::Array, true},
        InventoryRow{"Tuple", Field(Tuple{Field(true), Field(DecimalField<Decimal64>(42, 3))}), CanonicalFieldKind::Tuple, false},
        InventoryRow{"Map", Field(map), CanonicalFieldKind::Map, false},
        InventoryRow{"Object", Field(object), CanonicalFieldKind::Object, false},
        InventoryRow{
            "AggregateFunctionState",
            Field(
                AggregateFunctionStateData{
                    .name = "AggregateFunction(sum, UInt64)",
                    .data = String{"x\0y", 3},
                }),
            CanonicalFieldKind::AggregateFunctionState,
            false},
    };

    Array values;
    values.reserve(inventory.size());
    std::array<bool, static_cast<std::size_t>(CanonicalFieldKind::AggregateFunctionState) + 1> covered{};
    std::size_t sql_spellable = 0;
    for (const auto & row : inventory)
    {
        const std::size_t kind_index = static_cast<std::size_t>(row.canonical_kind);
        ASSERT_LT(kind_index, covered.size()) << row.name;
        EXPECT_FALSE(covered[kind_index]) << row.name;
        covered[kind_index] = true;
        EXPECT_EQ(CanonicalFieldValue::fromField(row.value).kind, row.canonical_kind) << row.name;
        sql_spellable += row.sql_spellable;
        values.push_back(row.value);
    }
    EXPECT_FALSE(covered[static_cast<std::size_t>(CanonicalFieldKind::None)]);
    for (std::size_t index = 1; index < covered.size(); ++index)
        EXPECT_TRUE(covered[index]) << "missing CanonicalFieldKind ordinal " << index;
    EXPECT_EQ(sql_spellable, 18U);
    EXPECT_EQ(inventory.size() - sql_spellable, 6U);

    const auto definitions = TemplateChecker::checkAll({fieldInventoryDefinition(values)});
    auto adapter = authority(definitions);
    auto attempt = TemplateSpecializer::Attempt::begin(*adapter);
    const auto id = attempt.specialize(definitions.front()->getIdentity(), noArguments());
    const auto finished = attempt.finish();

    const ASTPtr & root = finished.specializations[id].canonical_physical_ast;
    const auto * aggregate_type = root->as<ASTDataType>();
    ASSERT_NE(aggregate_type, nullptr);
    const auto * type_arguments = aggregate_type->getArguments()->as<ASTExpressionList>();
    ASSERT_NE(type_arguments, nullptr);
    ASSERT_EQ(type_arguments->children.size(), 2U);
    const auto * function = type_arguments->children[0]->as<ASTFunction>();
    ASSERT_NE(function, nullptr);
    ASSERT_NE(function->arguments, nullptr);
    ASSERT_EQ(function->arguments->children.size(), inventory.size());
    for (std::size_t index = 0; index < inventory.size(); ++index)
    {
        const auto * literal = function->arguments->children[index]->as<ASTLiteral>();
        ASSERT_NE(literal, nullptr) << inventory[index].name;
        EXPECT_EQ(literal->value, inventory[index].value) << inventory[index].name;

        WriteBufferFromOwnString expected_bytes;
        WriteBufferFromOwnString reconstructed_bytes;
        encodeField(inventory[index].value, expected_bytes);
        encodeField(literal->value, reconstructed_bytes);
        EXPECT_EQ(reconstructed_bytes.str(), expected_bytes.str()) << inventory[index].name;
    }

    /// The factory parses every reconstructed literal before resolving the
    /// aggregate function. Reaching this precise final error proves the whole
    /// Field inventory crossed checker -> specializer -> factory intact.
    expectDBError(ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION, [&] { static_cast<void>(DataTypeFactory::instance().get(root)); });
}

TEST(UDTTemplateSpecializer, WrongArgumentKindsFailPreciselyAndPoisonTheAttempt)
{
    const auto definitions = TemplateChecker::checkAll({typeAliasDefinition("KindedAlias", 0x280)});
    auto adapter = authority(definitions);
    const std::vector<Parameter> wrong_formals{{.normalized_name = "N", .kind = ParameterKind::UInt64}};
    std::vector<CanonicalTypeArgumentValue> wrong_values;
    wrong_values.push_back(CanonicalTypeArgumentValue::unsignedInteger(ParameterKind::UInt64, 7));
    const auto wrong_arguments = CanonicalTypeArguments::validate(wrong_formals, std::move(wrong_values));

    auto attempt = TemplateSpecializer::Attempt::begin(*adapter);
    expectSpecializerError(
        TemplateSpecializerError::Code::InvalidArguments,
        [&] { static_cast<void>(attempt.specialize(definitions.front()->getIdentity(), wrong_arguments)); });
    expectSpecializerError(TemplateSpecializerError::Code::InvalidAttemptState, [&] { static_cast<void>(attempt.finish()); });
}

TEST(UDTTemplateSpecializer, LoweredOutputLimitRejectsAtProspectiveBuiltInPreflight)
{
    DefinitionInput input;
    input.identity = identity(0x285);
    input.normalized_name = "ProspectiveTuple";
    input.nodes = {
        builtIn("Tuple", {{.reference = 1, .label = {}}, {.reference = 2, .label = {}}}),
        builtIn("UInt64"),
        builtIn("String"),
    };
    const auto definitions = TemplateChecker::checkAll({std::move(input)});
    auto adapter = authority(definitions);
    TemplateSpecializerLimits limits;
    /// Tuple's two argument edges plus its expression-list edge cannot fit.
    /// The specializer must reject before allocating any child-sized scratch.
    limits.maximum_constructed_ast_edges = 2;

    auto attempt = TemplateSpecializer::Attempt::begin(*adapter, limits);
    expectSpecializerError(
        TemplateSpecializerError::Code::LimitExceeded,
        [&] { static_cast<void>(attempt.specialize(definitions.front()->getIdentity(), noArguments())); });
    expectSpecializerError(TemplateSpecializerError::Code::InvalidAttemptState, [&] { static_cast<void>(attempt.finish()); });
}

TEST(UDTTemplateSpecializer, EncodedArgumentsPreflightWorkBeforeDecode)
{
    const auto definitions = TemplateChecker::checkAll({scalarDefinition("EncodedPreflight", 0x284, "UInt64")});
    auto adapter = authority(definitions);
    TemplateSpecializerLimits limits;
    /// The initial pinned lookup fits. The unavoidable request, repeated
    /// lookup, and two one-byte envelope passes do not. The byte itself is
    /// intentionally malformed: seeing InvalidArguments would prove decode
    /// ran before the aggregate work preflight.
    limits.maximum_work = 3;

    auto attempt = TemplateSpecializer::Attempt::begin(*adapter, limits);
    expectSpecializerError(
        TemplateSpecializerError::Code::LimitExceeded,
        [&] { static_cast<void>(attempt.specializeEncoded(definitions.front()->getIdentity(), "x")); },
        "encoded specialization work");
    expectSpecializerError(TemplateSpecializerError::Code::InvalidAttemptState, [&] { static_cast<void>(attempt.finish()); });
}

TEST(UDTTemplateSpecializer, LoweredWorkRejectsMemoReplayBeforeDestinationAllocation)
{
    auto target_input = scalarDefinition("ReplayTarget", 0x286, "UInt64");
    DefinitionInput caller;
    caller.identity = identity(0x287);
    caller.normalized_name = "ReplayCaller";
    caller.nodes = {definitionCall(0)};
    caller.dependencies = {{.type_uuid = target_input.identity.type_uuid, .revision = 1}};
    const auto definitions = checkWithSingleDependency(std::move(caller), std::move(target_input));
    const auto & outer = byIdentity(definitions, identity(0x287));
    auto adapter = authority(definitions);
    TemplateSpecializerLimits limits;
    /// Twenty-nine work units reach the completed dependency memo entry. Copying
    /// its one lineage record requires one more and must be rejected by the
    /// aggregate prospective check before destination.reserve().
    limits.maximum_work = 29;

    auto attempt = TemplateSpecializer::Attempt::begin(*adapter, limits);
    expectSpecializerError(
        TemplateSpecializerError::Code::LimitExceeded,
        [&] { static_cast<void>(attempt.specialize(outer->getIdentity(), noArguments())); },
        "retained occurrence work");
    expectSpecializerError(TemplateSpecializerError::Code::InvalidAttemptState, [&] { static_cast<void>(attempt.finish()); });
}

TEST(UDTTemplateSpecializer, FinalPathNormalizationIsProspectivelyWorkBounded)
{
    auto target_input = scalarDefinition("NormalizedPathTarget", 0x288, "UInt64");
    DefinitionInput caller;
    caller.identity = identity(0x289);
    caller.normalized_name = "NormalizedPathCaller";
    caller.nodes = {builtIn("Array", {{.reference = 1, .label = {}}}), definitionCall(0)};
    caller.dependencies = {{.type_uuid = target_input.identity.type_uuid, .revision = 1}};
    const auto definitions = checkWithSingleDependency(std::move(caller), std::move(target_input));
    const auto & outer = byIdentity(definitions, identity(0x289));
    auto adapter = authority(definitions);

    auto successful_attempt = TemplateSpecializer::Attempt::begin(*adapter);
    static_cast<void>(successful_attempt.specialize(outer->getIdentity(), noArguments()));
    const auto successful = successful_attempt.finish();
    ASSERT_GT(successful.statistics.retained_path_components, 0U);
    ASSERT_GT(successful.statistics.charged_work, 0U);

    TemplateSpecializerLimits limits;
    /// Every preceding unit fits, but the final complete path-normalization
    /// pass is one unit short. Rejection therefore occurs before any retained
    /// path is reversed or any result is committed.
    limits.maximum_work = successful.statistics.charged_work - 1;
    auto limited_attempt = TemplateSpecializer::Attempt::begin(*adapter, limits);
    static_cast<void>(limited_attempt.specialize(outer->getIdentity(), noArguments()));
    expectSpecializerError(
        TemplateSpecializerError::Code::LimitExceeded, [&] { static_cast<void>(limited_attempt.finish()); }, "specialization work");
    expectSpecializerError(TemplateSpecializerError::Code::InvalidAttemptState, [&] { static_cast<void>(limited_attempt.finish()); });
}

TEST(UDTTemplateSpecializer, AuthorityTraversalLimitBoundsDistinctDefinitions)
{
    auto target_input = scalarDefinition("AuthorityTraversalTarget", 0x28a, "UInt64");
    DefinitionInput caller;
    caller.identity = identity(0x28b);
    caller.normalized_name = "AuthorityTraversalCaller";
    caller.nodes = {definitionCall(0)};
    caller.dependencies = {{.type_uuid = target_input.identity.type_uuid, .revision = 1}};
    const auto definitions = checkWithSingleDependency(std::move(caller), std::move(target_input));
    const auto & outer = byIdentity(definitions, identity(0x28b));

    auto restricted_capabilities = capabilities();
    /// The attempt-wide authority budget includes the requested root, so one
    /// slot cannot also admit its dependency.
    restricted_capabilities.limits.maximum_transitive_dependencies = 1;
    auto adapter = makeTransientAuthorityAdapter(databaseUUID(), restricted_capabilities, definitions);
    auto attempt = TemplateSpecializer::Attempt::begin(*adapter);
    expectSpecializerError(
        TemplateSpecializerError::Code::LimitExceeded,
        [&] { static_cast<void>(attempt.specialize(outer->getIdentity(), noArguments())); },
        "authority traversal limit");
    expectSpecializerError(TemplateSpecializerError::Code::InvalidAttemptState, [&] { static_cast<void>(attempt.finish()); });
}

TEST(UDTTemplateSpecializer, AuthorityTraversalLimitIsAttemptWideAcrossIndependentRoots)
{
    const auto definitions = TemplateChecker::checkAll(
        {scalarDefinition("IndependentFirst", 0x28c, "UInt64"), scalarDefinition("IndependentSecond", 0x28d, "String")});
    auto restricted_capabilities = capabilities();
    restricted_capabilities.limits.maximum_transitive_dependencies = 1;
    auto adapter = makeTransientAuthorityAdapter(databaseUUID(), restricted_capabilities, definitions);

    auto attempt = TemplateSpecializer::Attempt::begin(*adapter);
    EXPECT_NE(attempt.specialize(identity(0x28c), noArguments()), invalid_template_specialization_id);
    expectSpecializerError(
        TemplateSpecializerError::Code::LimitExceeded,
        [&] { static_cast<void>(attempt.specialize(identity(0x28d), noArguments())); },
        "authority traversal limit");
    expectSpecializerError(TemplateSpecializerError::Code::InvalidAttemptState, [&] { static_cast<void>(attempt.finish()); });
}

TEST(UDTTemplateSpecializer, AuthorityDefinitionCountRejectsBeforeLookupAccounting)
{
    auto definitions = TemplateChecker::checkAll(
        {scalarDefinition("DefinitionCountFirst", 0x294, "UInt64"), scalarDefinition("DefinitionCountSecond", 0x295, "String")});
    const DefinitionIdentity first_identity = identity(0x294);
    const DefinitionIdentity second_identity = identity(0x295);
    std::weak_ptr<const Definition> first_observer = byIdentity(definitions, first_identity);
    std::weak_ptr<const Definition> second_observer = byIdentity(definitions, second_identity);

    auto advertised = capabilities();
    advertised.limits.maximum_definitions = 1;
    auto adapter = std::make_shared<CountingLimitsAuthority>(advertised, definitions);
    definitions.clear();
    TemplateSpecializerLimits caller_limits;
    caller_limits.maximum_definition_lookups = 1;
    auto attempt = TemplateSpecializer::Attempt::begin(*adapter, caller_limits);
    adapter.reset();

    EXPECT_NE(attempt.specialize(first_identity, noArguments()), invalid_template_specialization_id);
    expectSpecializerError(
        TemplateSpecializerError::Code::LimitExceeded,
        [&] { static_cast<void>(attempt.specialize(second_identity, noArguments())); },
        "authority definition-count limit");
    /// If the second request reached lookup accounting, the deliberately exact
    /// caller lookup limit would have produced a different failure first.
    EXPECT_TRUE(first_observer.expired());
    EXPECT_TRUE(second_observer.expired());
    expectSpecializerError(TemplateSpecializerError::Code::InvalidAttemptState, [&] { static_cast<void>(attempt.finish()); });

    /// Poisoning is attempt-local: a fresh compliant generation can resolve
    /// the same identity and commit normally.
    const auto retry_definitions = TemplateChecker::checkAll({scalarDefinition("DefinitionCountSecond", 0x295, "String")});
    auto retry_adapter = authority(retry_definitions);
    auto retry = TemplateSpecializer::Attempt::begin(*retry_adapter);
    EXPECT_NE(retry.specialize(second_identity, noArguments()), invalid_template_specialization_id);
    EXPECT_EQ(retry.finish().definition_handles.size(), 1);
}

TEST(UDTTemplateSpecializer, CallerDefinitionHandleLimitRejectsBeforeLookupAccounting)
{
    const auto definitions = TemplateChecker::checkAll(
        {scalarDefinition("CallerHandleFirst", 0x299, "UInt64"), scalarDefinition("CallerHandleSecond", 0x29a, "String")});
    auto adapter = std::make_shared<CountingLimitsAuthority>(capabilities(), definitions);
    TemplateSpecializerLimits caller_limits;
    caller_limits.maximum_definition_handles = 1;
    caller_limits.maximum_definition_lookups = 1;
    auto attempt = TemplateSpecializer::Attempt::begin(*adapter, caller_limits);

    EXPECT_NE(attempt.specialize(identity(0x299), noArguments()), invalid_template_specialization_id);
    expectSpecializerError(
        TemplateSpecializerError::Code::LimitExceeded,
        [&] { static_cast<void>(attempt.specialize(identity(0x29a), noArguments())); },
        "definition handles exceed their limit");
    expectSpecializerError(TemplateSpecializerError::Code::InvalidAttemptState, [&] { static_cast<void>(attempt.finish()); });
}

TEST(UDTTemplateSpecializer, ArbitraryAuthorityTemplateNodeLimitIsEnforcedBeforeRetention)
{
    DefinitionInput input;
    input.identity = identity(0x296);
    input.normalized_name = "OversizedTemplate";
    input.nodes = {builtIn("Array", {{.reference = 1, .label = {}}}), builtIn("UInt64")};
    auto definition = TemplateChecker::checkAll({std::move(input)}).front();
    ASSERT_EQ(definition->getNodes().size(), 2);
    auto advertised = capabilities();
    advertised.limits.maximum_template_nodes = 1;
    expectReturnedDefinitionLimitFailure(std::move(advertised), std::move(definition), "template nodes");
}

TEST(UDTTemplateSpecializer, ArbitraryAuthorityDirectDependencyLimitIsEnforcedBeforeRetention)
{
    auto definitions = definitionsWithTwoDirectDependencies();
    auto definition = byIdentity(definitions, identity(0x293));
    ASSERT_EQ(definition->getDependencies().size(), 2);
    auto advertised = capabilities();
    advertised.limits.maximum_direct_dependencies = 1;

    const DefinitionIdentity requested_identity = definition->getIdentity();
    std::weak_ptr<const Definition> observer = definition;
    auto adapter = std::make_shared<CountingLimitsAuthority>(std::move(advertised), std::move(definitions));
    definition.reset();
    auto attempt = TemplateSpecializer::Attempt::begin(*adapter);
    adapter.reset();
    expectSpecializerError(
        TemplateSpecializerError::Code::LimitExceeded,
        [&] { static_cast<void>(attempt.specialize(requested_identity, noArguments())); },
        "direct dependencies");
    EXPECT_TRUE(observer.expired());
    expectSpecializerError(TemplateSpecializerError::Code::InvalidAttemptState, [&] { static_cast<void>(attempt.finish()); });
}

TEST(UDTTemplateSpecializer, ArbitraryAuthorityCheckerWorkLimitIsEnforcedBeforeRetention)
{
    auto definition = TemplateChecker::checkAll({scalarDefinition("OversizedCheckerWork", 0x297, "UInt64")}).front();
    ASSERT_GT(definition->getCertificate().charged_work, 1);
    auto advertised = capabilities();
    advertised.limits.maximum_checker_work = 1;
    expectReturnedDefinitionLimitFailure(std::move(advertised), std::move(definition), "checker work");
}

TEST(UDTTemplateSpecializer, ArbitraryAuthorityExactLogicalByteLimitIsEnforcedBeforeRetention)
{
    auto input = scalarDefinition(String(4ULL << 10, 'n'), 0x298, "UInt64");
    input.normalized_local_name = String(4ULL << 10, 'l');
    auto definition = TemplateChecker::checkAll({std::move(input)}).front();
    const auto exact = tryCountLogicalRetainedDefinitionBytes(*definition, std::numeric_limits<UInt64>::max());
    ASSERT_TRUE(exact.has_value());
    ASSERT_GT(*exact, 1);
    auto advertised = capabilities();
    advertised.limits.maximum_definition_bytes = *exact - 1;
    expectReturnedDefinitionLimitFailure(std::move(advertised), std::move(definition), "logical retained bytes");
}

TEST(UDTTemplateSpecializer, IndependentLimitsRejectBeforePartialCommit)
{
    const auto definitions = TemplateChecker::checkAll({recursiveDefinition()});
    auto adapter = authority(definitions);
    TemplateSpecializerLimits limits;
    limits.maximum_distinct_specializations = 2;

    auto attempt = TemplateSpecializer::Attempt::begin(*adapter, limits);
    expectSpecializerError(
        TemplateSpecializerError::Code::LimitExceeded,
        [&] { static_cast<void>(attempt.specialize(definitions.front()->getIdentity(), recursiveArguments(*definitions.front(), 2))); });
    expectSpecializerError(TemplateSpecializerError::Code::InvalidAttemptState, [&] { static_cast<void>(attempt.finish()); });
}

TEST(UDTTemplateSpecializer, FailureReleasesPinnedAuthorityAndRetainsNoPartialState)
{
    auto definition = TemplateChecker::checkAll({scalarDefinition("LifetimeValue", 0x290, "UInt64")}).front();
    std::weak_ptr<const Definition> observer = definition;
    AuthorityAdapterPtr adapter = authority({definition});
    definition.reset();

    auto attempt = TemplateSpecializer::Attempt::begin(*adapter);
    adapter.reset();
    EXPECT_FALSE(observer.expired());
    expectSpecializerError(
        TemplateSpecializerError::Code::DefinitionNotFound,
        [&] { static_cast<void>(attempt.specialize(identity(0xffff), noArguments())); });
    EXPECT_TRUE(observer.expired());
    expectSpecializerError(TemplateSpecializerError::Code::InvalidAttemptState, [&] { static_cast<void>(attempt.finish()); });
}

TEST(UDTTemplateSpecializer, MissingCapabilitiesFailBeforeOpeningASession)
{
    const auto unsupported = makeUnsupportedAuthorityAdapter();
    expectSpecializerError(
        TemplateSpecializerError::Code::MissingCapability, [&] { static_cast<void>(TemplateSpecializer::Attempt::begin(*unsupported)); });
}

TEST(UDTTemplateSpecializer, InvalidAdvertisedLimitsFailBeforeAuthorityOrSessionAccess)
{
    InvalidLimitsAuthority authority_with_zero_limits;
    expectSpecializerError(
        TemplateSpecializerError::Code::MissingCapability,
        [&] { static_cast<void>(TemplateSpecializer::Attempt::begin(authority_with_zero_limits)); },
        "invalid Limits");
    EXPECT_EQ(authority_with_zero_limits.database_calls, 0);
    EXPECT_EQ(authority_with_zero_limits.session_calls, 0);
    EXPECT_EQ(authority_with_zero_limits.require_calls, 0);
}

}
