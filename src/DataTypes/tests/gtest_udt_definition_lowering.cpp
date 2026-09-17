#include <DataTypes/UDT/DefinitionLowering.h>
#include <DataTypes/UDT/TemplateChecker.h>

#include <Core/UUID.h>

#include <IO/WriteHelpers.h>

#include <Parsers/ASTCreateTypeQuery.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTTupleDataType.h>
#include <Parsers/ParserCreateTypeQuery.h>
#include <Parsers/parseQuery.h>

#include <Common/Exception.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

namespace DB::UDT
{
namespace
{

UUID testUUID(UInt64 low)
{
    UUID result{};
    UUIDHelpers::getHighBytes(result) = 0x550e8400e29b41d4ULL;
    UUIDHelpers::getLowBytes(result) = low;
    return result;
}

DefinitionIdentity testIdentity(UInt64 low, UInt64 revision = 1)
{
    return {
        .database_uuid = testUUID(0xa716446655440000ULL),
        .type_uuid = testUUID(low),
        .revision = revision,
    };
}

ASTPtr parseTypeQuery(const String & text)
{
    ParserCreateTypeQuery parser;
    return parseQuery(parser, text, "definition lowering test", 0, 200, 0);
}

StructuredDefinitionName nameFor(std::string_view local)
{
    return {
        .normalized_database_name = "app",
        .normalized_qualified_name = "app." + String(local),
        .normalized_local_name = String(local),
    };
}

DefinitionInput lower(
    const ASTCreateTypeQuery & query,
    DefinitionIdentity identity,
    const StructuredDefinitionName & name,
    std::span<const AvailableDefinitionBinding> bindings = {},
    const DefinitionLoweringLimits & limits = {})
{
    return lowerCreateTypeQueryToDefinitionInput(
        query,
        {
            .identity = identity,
            .name = name,
            .available_bindings = bindings,
        },
        limits);
}

DefinitionInput lower(
    const String & text,
    DefinitionIdentity identity,
    const StructuredDefinitionName & name,
    std::span<const AvailableDefinitionBinding> bindings = {},
    const DefinitionLoweringLimits & limits = {})
{
    const ASTPtr ast = parseTypeQuery(text);
    const auto * query = ast->as<ASTCreateTypeQuery>();
    if (!query)
        throw std::logic_error("expected ASTCreateTypeQuery");
    return lower(*query, identity, name, bindings, limits);
}

AvailableDefinitionBinding
bindingFor(std::string_view local, DefinitionIdentity identity, std::initializer_list<ParameterKind> kinds, Digest hash = {})
{
    return {
        .name = nameFor(local),
        .identity = identity,
        .definition_hash = hash,
        .parameter_kinds = kinds,
    };
}

void expectInputEqual(const DefinitionInput & lhs, const DefinitionInput & rhs)
{
    EXPECT_EQ(lhs.identity, rhs.identity);
    EXPECT_EQ(lhs.normalized_name, rhs.normalized_name);
    EXPECT_EQ(lhs.normalized_local_name, rhs.normalized_local_name);
    EXPECT_EQ(lhs.parameters, rhs.parameters);
    EXPECT_EQ(lhs.decreasing_parameter, rhs.decreasing_parameter);
    EXPECT_EQ(lhs.nodes, rhs.nodes);
    EXPECT_EQ(lhs.root, rhs.root);
    EXPECT_EQ(lhs.policy_bearing, rhs.policy_bearing);
    EXPECT_EQ(lhs.semantic_capabilities, rhs.semantic_capabilities);
    EXPECT_EQ(lhs.checker_abi, rhs.checker_abi);
    EXPECT_EQ(lhs.checker_charge_abi, rhs.checker_charge_abi);
    EXPECT_EQ(lhs.policy_abi, rhs.policy_abi);
    EXPECT_EQ(lhs.function_registry_abi, rhs.function_registry_abi);
    EXPECT_EQ(lhs.policy_semantic_hash, rhs.policy_semantic_hash);
    EXPECT_EQ(lhs.dependencies, rhs.dependencies);
}

const Definition & byName(const std::vector<Definition::Ptr> & definitions, std::string_view name)
{
    const auto found = std::ranges::find_if(definitions, [&](const auto & definition) { return definition->getNormalizedName() == name; });
    if (found == definitions.end())
        throw std::logic_error("checked definition not found");
    return **found;
}

TEST(UDTDefinitionLowering, CreateAndAttachConvergeAfterAliasCanonicalization)
{
    const auto identity = testIdentity(0x10);
    const auto name = nameFor("Concrete");
    const String definition = "Tuple(amount DEC(18, 2), active boolean, code BINARY(4), happened date)";
    const auto fresh = lower("CREATE TYPE app.Concrete AS " + definition, identity, name);
    const auto recovered = lower(
        "ATTACH TYPE app.Concrete UUID '" + toString(identity.type_uuid) + "' REVISION 1 AS " + definition + " DEFINITION HASH '"
            + String(64, '0') + "' COMMENT 'recovered'",
        identity,
        name);
    expectInputEqual(fresh, recovered);

    std::vector<String> atoms;
    for (const auto & node : fresh.nodes)
        if (node.kind == TemplateNodeKind::BuiltIn)
            atoms.push_back(node.atom);
    EXPECT_NE(std::ranges::find(atoms, "Decimal"), atoms.end());
    EXPECT_NE(std::ranges::find(atoms, "Bool"), atoms.end());
    EXPECT_NE(std::ranges::find(atoms, "FixedString"), atoms.end());
    EXPECT_NE(std::ranges::find(atoms, "Date"), atoms.end());

    const auto fresh_checked = TemplateChecker::checkAll({fresh});
    const auto recovered_checked = TemplateChecker::checkAll({recovered});
    ASSERT_EQ(fresh_checked.size(), 1);
    ASSERT_EQ(recovered_checked.size(), 1);
    EXPECT_EQ(fresh_checked.front()->getCertificate(), recovered_checked.front()->getCertificate());
}

TEST(UDTDefinitionLowering, QualifiedAndUnqualifiedCallsForwardFormalsAndCanonicalizeDependencies)
{
    const auto box_identity = testIdentity(0x20);
    const auto wrapped_identity = testIdentity(0x21);
    const auto box = lower("CREATE TYPE app.Box(T TYPE, N UInt16) AS Tuple(value T, bytes FixedString(N))", box_identity, nameFor("Box"));
    const std::array bindings{bindingFor("Box", box_identity, {ParameterKind::Type, ParameterKind::UInt16})};
    const auto qualified
        = lower("CREATE TYPE app.Wrapped(X TYPE, M UInt16) AS app.Box(X, M)", wrapped_identity, nameFor("Wrapped"), bindings);
    const auto unqualified
        = lower("CREATE TYPE app.Wrapped(X TYPE, M UInt16) AS Box(X, M)", wrapped_identity, nameFor("Wrapped"), bindings);
    expectInputEqual(qualified, unqualified);
    ASSERT_EQ(qualified.dependencies.size(), 1);
    ASSERT_EQ(qualified.nodes.size(), 1);
    EXPECT_EQ(qualified.nodes.front().kind, TemplateNodeKind::DefinitionCall);
    ASSERT_EQ(qualified.nodes.front().children.size(), 2);
    EXPECT_EQ(qualified.nodes.front().children[0].reference, 0);
    EXPECT_EQ(qualified.nodes.front().children[1].reference, 1);

    const auto checked = TemplateChecker::checkAll({qualified, box});
    ASSERT_EQ(checked.size(), 2);
    EXPECT_NE(byName(checked, "app.Wrapped").getDependencies().front().target_definition_hash, Digest{});
}

TEST(UDTDefinitionLowering, StrictDecreasingRecursionUsesTheDedicatedNodes)
{
    const auto input = lower(
        "CREATE TYPE app.Chain(T TYPE, N UInt16) DECREASES N "
        "AS TYPE_IF(N = 0, T, Tuple(head T, tail app.Chain(T, N - 1)))",
        testIdentity(0x30),
        nameFor("Chain"));
    EXPECT_EQ(input.decreasing_parameter, 1);
    EXPECT_EQ(input.checker_abi, 2);
    EXPECT_EQ(std::ranges::count_if(input.nodes, [](const auto & node) { return node.kind == TemplateNodeKind::TypeIfZero; }), 1);
    EXPECT_EQ(std::ranges::count_if(input.nodes, [](const auto & node) { return node.kind == TemplateNodeKind::SelfCall; }), 1);
    EXPECT_NO_THROW(static_cast<void>(TemplateChecker::checkAll({input})));

    const auto unguarded = lower(
        "CREATE TYPE app.BadChain(T TYPE, N UInt16) DECREASES N AS app.BadChain(T, N - 1)", testIdentity(0x31), nameFor("BadChain"));
    EXPECT_THROW(static_cast<void>(TemplateChecker::checkAll({unguarded})), Exception);
}

TEST(UDTDefinitionLowering, CompleteTypedParserSurfacePassesTheChecker)
{
    const auto input = lower(
        "CREATE TYPE app.Rich(N UInt16) AS Tuple("
        "agg AggregateFunction(1, sumMapFiltered([1, 4, 8]), Array(UInt64), Array(UInt64)), "
        "simple SimpleAggregateFunction(sum(), UInt64), "
        "dyn Dynamic(max_types=N), "
        "obj JSON(SKIP REGEXP '^tmp', payload.value UInt64, max_dynamic_paths=9, SKIP private.path, max_dynamic_types=N), "
        "wide Enum('large' = 300, 'small' = -2), "
        "implicit Enum('first', 'second'))",
        testIdentity(0x40),
        nameFor("Rich"));
    const auto count
        = [&](TemplateNodeKind kind) { return std::ranges::count_if(input.nodes, [&](const auto & node) { return node.kind == kind; }); };
    EXPECT_EQ(count(TemplateNodeKind::AggregateFunction), 2);
    EXPECT_EQ(count(TemplateNodeKind::DynamicSetting), 1);
    EXPECT_EQ(count(TemplateNodeKind::ObjectSetting), 2);
    EXPECT_EQ(count(TemplateNodeKind::ObjectTypedPath), 1);
    EXPECT_EQ(count(TemplateNodeKind::ObjectSkipPath), 1);
    EXPECT_EQ(count(TemplateNodeKind::ObjectSkipRegexp), 1);
    EXPECT_EQ(count(TemplateNodeKind::SpecializedEnum), 2);
    EXPECT_GT(count(TemplateNodeKind::FieldValue), 1);
    EXPECT_NO_THROW(static_cast<void>(TemplateChecker::checkAll({input})));
}

TEST(UDTDefinitionLowering, ImplicitAndExplicitBuiltInSpellingsHaveByteIdenticalSemantics)
{
    const auto identity = testIdentity(0x50);
    const auto name = nameFor("Spellings");
    const auto implicit = lower(
        "CREATE TYPE app.Spellings AS Tuple("
        "a Dynamic, b DEC(9, 2), c AggregateFunction(sum, UInt64), d Enum('x', 'y'))",
        identity,
        name);
    const auto explicit_spelling = lower(
        "CREATE TYPE app.Spellings AS Tuple("
        "a Dynamic(), b Decimal(9, 2), c AggregateFunction(sum(), UInt64), d enum8('x' = 1, 'y' = 2))",
        identity,
        name);
    expectInputEqual(implicit, explicit_spelling);
    const auto first = TemplateChecker::checkAll({implicit});
    const auto second = TemplateChecker::checkAll({explicit_spelling});
    EXPECT_EQ(first.front()->getCertificate(), second.front()->getCertificate());
}

TEST(UDTDefinitionLowering, ImmutableDefinitionCopyIsExactAndRecheckable)
{
    const auto target_identity = testIdentity(0x60);
    const auto caller_identity = testIdentity(0x61);
    const auto target = lower("CREATE TYPE app.Target(T TYPE) AS Array(T)", target_identity, nameFor("Target"));
    const std::array bindings{bindingFor("Target", target_identity, {ParameterKind::Type})};
    const auto caller = lower("CREATE TYPE app.Caller(T TYPE) AS app.Target(T)", caller_identity, nameFor("Caller"), bindings);
    const auto checked = TemplateChecker::checkAll({caller, target});
    ASSERT_EQ(checked.size(), 2);

    std::vector<DefinitionInput> copied;
    copied.reserve(checked.size());
    for (const auto & definition : checked)
        copied.push_back(definitionInputFromCheckedDefinition(*definition));
    const auto rechecked = TemplateChecker::checkAll(copied);
    ASSERT_EQ(rechecked.size(), checked.size());
    for (const auto & original : checked)
        EXPECT_EQ(original->getCertificate(), byName(rechecked, original->getNormalizedName()).getCertificate());
}

TEST(UDTDefinitionLowering, StoredBindingAndAttachRecoveryMatchFreshDependentAdmission)
{
    const auto target_identity = testIdentity(0x68);
    const auto caller_identity = testIdentity(0x69);
    const auto target_input = lower("CREATE TYPE app.Stored(T TYPE) AS Nullable(T)", target_identity, nameFor("Stored"));
    const auto target_checked = TemplateChecker::checkAll({target_input});
    ASSERT_EQ(target_checked.size(), 1);
    const std::array bindings{bindingFor("Stored", target_identity, {ParameterKind::Type}, target_checked.front()->getDefinitionHash())};

    const auto fresh = lower("CREATE TYPE app.Dependent(T TYPE) AS app.Stored(T)", caller_identity, nameFor("Dependent"), bindings);
    const auto recovered = lower(
        "ATTACH TYPE app.Dependent(T TYPE) UUID '" + toString(caller_identity.type_uuid) + "' REVISION 1 AS app.Stored(T) DEFINITION HASH '"
            + String(64, '0') + "'",
        caller_identity,
        nameFor("Dependent"),
        bindings);
    expectInputEqual(fresh, recovered);
    ASSERT_EQ(fresh.dependencies.size(), 1);
    EXPECT_EQ(fresh.dependencies.front().target_definition_hash, target_checked.front()->getDefinitionHash());

    const auto existing = definitionInputFromCheckedDefinition(*target_checked.front());
    const auto fresh_checked = TemplateChecker::checkAll({fresh, existing});
    const auto recovered_checked = TemplateChecker::checkAll({recovered, existing});
    EXPECT_EQ(byName(fresh_checked, "app.Dependent").getCertificate(), byName(recovered_checked, "app.Dependent").getCertificate());
}

TEST(UDTDefinitionLowering, DefinitionDependencyCycleIsRejectedByWholeSetChecking)
{
    const auto first_identity = testIdentity(0x6a);
    const auto second_identity = testIdentity(0x6b);
    const std::array bindings{
        bindingFor("First", first_identity, {}),
        bindingFor("Second", second_identity, {}),
    };
    const auto first = lower("CREATE TYPE app.First AS app.Second", first_identity, nameFor("First"), bindings);
    const auto second = lower("CREATE TYPE app.Second AS app.First", second_identity, nameFor("Second"), bindings);
    EXPECT_THROW(static_cast<void>(TemplateChecker::checkAll({first, second})), Exception);
}

TEST(UDTDefinitionLowering, RejectsCrossDatabaseUnboundArityAndKindViolations)
{
    const auto target_identity = testIdentity(0x70);
    const std::array bindings{bindingFor("Target", target_identity, {ParameterKind::Type, ParameterKind::UInt16})};
    const auto caller_identity = testIdentity(0x71);
    const auto caller_name = nameFor("Caller");

    EXPECT_THROW(
        static_cast<void>(lower("CREATE TYPE app.Caller(T TYPE, N UInt16) AS other.Target(T, N)", caller_identity, caller_name, bindings)),
        Exception);
    EXPECT_THROW(
        static_cast<void>(lower("CREATE TYPE app.Caller(T TYPE, N UInt16) AS app.Missing(T, N)", caller_identity, caller_name, bindings)),
        Exception);
    EXPECT_THROW(
        static_cast<void>(lower("CREATE TYPE app.Caller(T TYPE, N UInt16) AS app.Target(T)", caller_identity, caller_name, bindings)),
        Exception);
    EXPECT_THROW(
        static_cast<void>(lower("CREATE TYPE app.Caller(T TYPE, N UInt16) AS app.Target(T, T)", caller_identity, caller_name, bindings)),
        Exception);
    EXPECT_THROW(
        static_cast<void>(lower("CREATE TYPE app.Caller(T TYPE, N UInt16) AS app.Target(T, 3)", caller_identity, caller_name, bindings)),
        Exception);

    auto cross_database_binding = bindings.front();
    cross_database_binding.name.normalized_database_name = "other";
    cross_database_binding.name.normalized_qualified_name = "other.Target";
    const std::array invalid_bindings{cross_database_binding};
    EXPECT_THROW(
        static_cast<void>(lower("CREATE TYPE app.Caller(T TYPE, N UInt16) AS UInt64", caller_identity, caller_name, invalid_bindings)),
        Exception);
}

TEST(UDTDefinitionLowering, RejectsInconsistentStructuredNames)
{
    const auto identity = testIdentity(0x78);
    auto request_name = nameFor("Named");
    request_name.normalized_qualified_name = "other.Named";
    EXPECT_THROW(static_cast<void>(lower("CREATE TYPE app.Named AS UInt64", identity, request_name)), Exception);

    auto inconsistent_binding = bindingFor("Target", testIdentity(0x79), {});
    inconsistent_binding.name.normalized_qualified_name = "app.Other";
    const std::array bindings{inconsistent_binding};
    EXPECT_THROW(static_cast<void>(lower("CREATE TYPE app.Named AS UInt64", identity, nameFor("Named"), bindings)), Exception);
}

TEST(UDTDefinitionLowering, RejectsCyclesSharedNodesAndAttachIdentityMismatch)
{
    const auto identity = testIdentity(0x80);
    const auto name = nameFor("Bad");

    {
        const ASTPtr ast = parseTypeQuery("CREATE TYPE app.Bad AS Tuple(UInt8, UInt16)");
        auto & query = ast->as<ASTCreateTypeQuery &>();
        auto & tuple = query.definition->as<ASTTupleDataType &>();
        auto & arguments = tuple.getArguments()->as<ASTExpressionList &>();
        const ASTPtr original = arguments.children[1];
        arguments.children[1] = arguments.children[0];
        EXPECT_THROW(static_cast<void>(lower(query, identity, name)), Exception);
        arguments.children[1] = original;
    }
    {
        const ASTPtr ast = parseTypeQuery("CREATE TYPE app.Bad AS Array(UInt8)");
        auto & query = ast->as<ASTCreateTypeQuery &>();
        auto & array = query.definition->as<ASTDataType &>();
        auto & arguments = array.getArguments()->as<ASTExpressionList &>();
        const ASTPtr original = arguments.children[0];
        arguments.children[0] = query.definition;
        EXPECT_THROW(static_cast<void>(lower(query, identity, name)), Exception);
        arguments.children[0] = original;
    }

    EXPECT_THROW(
        static_cast<void>(lower(
            "ATTACH TYPE app.Bad UUID '" + toString(testUUID(0x81)) + "' REVISION 1 AS UInt64 DEFINITION HASH '" + String(64, '0') + "'",
            identity,
            name)),
        Exception);
}

TEST(UDTDefinitionLowering, EveryResourceBoundaryFailsProspectively)
{
    const auto identity = testIdentity(0x90);
    const auto name = nameFor("Limited");

    {
        DefinitionLoweringLimits limits;
        limits.maximum_ast_nodes = 0;
        EXPECT_THROW(static_cast<void>(lower("CREATE TYPE app.Limited AS UInt64", identity, name, {}, limits)), Exception);
    }
    {
        DefinitionLoweringLimits limits;
        limits.maximum_ast_nodes = 2;
        EXPECT_THROW(static_cast<void>(lower("CREATE TYPE app.Limited AS Array(UInt64)", identity, name, {}, limits)), Exception);
    }
    {
        DefinitionLoweringLimits limits;
        limits.maximum_ast_edges = 2;
        EXPECT_THROW(static_cast<void>(lower("CREATE TYPE app.Limited AS Array(UInt64)", identity, name, {}, limits)), Exception);
    }
    {
        DefinitionLoweringLimits limits;
        limits.maximum_ast_depth = 3;
        EXPECT_THROW(static_cast<void>(lower("CREATE TYPE app.Limited AS Array(Array(UInt64))", identity, name, {}, limits)), Exception);
    }
    {
        DefinitionLoweringLimits limits;
        limits.maximum_formals = 1;
        EXPECT_THROW(static_cast<void>(lower("CREATE TYPE app.Limited(T TYPE, N UInt16) AS T", identity, name, {}, limits)), Exception);
    }
    {
        DefinitionLoweringLimits limits;
        limits.maximum_string_bytes = 4;
        EXPECT_THROW(static_cast<void>(lower("CREATE TYPE app.Limited AS UInt64", identity, name, {}, limits)), Exception);
    }
    {
        DefinitionLoweringLimits limits;
        limits.maximum_total_string_bytes = 12;
        EXPECT_THROW(static_cast<void>(lower("CREATE TYPE app.Limited AS UInt64", identity, name, {}, limits)), Exception);
    }
    {
        DefinitionLoweringLimits limits;
        limits.maximum_catalog_string_bytes = 8;
        const std::array bindings{bindingFor("Existing", testIdentity(0x99), {})};
        EXPECT_THROW(static_cast<void>(lower("CREATE TYPE app.Limited AS UInt64", identity, name, bindings, limits)), Exception);
    }
    {
        DefinitionLoweringLimits limits;
        limits.maximum_output_nodes = 1;
        EXPECT_THROW(static_cast<void>(lower("CREATE TYPE app.Limited AS Array(UInt64)", identity, name, {}, limits)), Exception);
    }
    {
        DefinitionLoweringLimits limits;
        limits.maximum_output_edges = 1;
        EXPECT_THROW(static_cast<void>(lower("CREATE TYPE app.Limited AS Tuple(UInt8, UInt16)", identity, name, {}, limits)), Exception);
    }
    {
        DefinitionLoweringLimits limits;
        limits.maximum_enum_entries = 1;
        EXPECT_THROW(static_cast<void>(lower("CREATE TYPE app.Limited AS Enum('a' = 1, 'b' = 2)", identity, name, {}, limits)), Exception);
    }
    {
        DefinitionLoweringLimits limits;
        limits.field_values.maximum_nodes = 1;
        EXPECT_THROW(
            static_cast<void>(lower("CREATE TYPE app.Limited AS AggregateFunction(sumMap([1, 2]), UInt64)", identity, name, {}, limits)),
            Exception);
    }
    {
        DefinitionLoweringLimits limits;
        limits.field_values.maximum_edges = 1;
        EXPECT_THROW(
            static_cast<void>(lower("CREATE TYPE app.Limited AS AggregateFunction(sumMap([1, 2]), UInt64)", identity, name, {}, limits)),
            Exception);
    }
    {
        DefinitionLoweringLimits limits;
        limits.field_values.maximum_entries = 1;
        EXPECT_THROW(
            static_cast<void>(lower("CREATE TYPE app.Limited AS AggregateFunction(sumMap([1, 2]), UInt64)", identity, name, {}, limits)),
            Exception);
    }
    {
        DefinitionLoweringLimits limits;
        limits.field_values.maximum_depth = 1;
        EXPECT_THROW(
            static_cast<void>(lower("CREATE TYPE app.Limited AS AggregateFunction(sumMap([1]), UInt64)", identity, name, {}, limits)),
            Exception);
    }
    {
        DefinitionLoweringLimits limits;
        limits.field_values.maximum_literal_bytes = 2;
        EXPECT_THROW(
            static_cast<void>(lower("CREATE TYPE app.Limited AS AggregateFunction(inventory('abc'), UInt64)", identity, name, {}, limits)),
            Exception);
    }
    {
        DefinitionLoweringLimits limits;
        limits.field_values.maximum_nodes = 2;
        EXPECT_NO_THROW(
            static_cast<void>(lower("CREATE TYPE app.Limited AS AggregateFunction(inventory(1, 2), UInt64)", identity, name, {}, limits)));
        limits.field_values.maximum_nodes = 1;
        EXPECT_THROW(
            static_cast<void>(lower("CREATE TYPE app.Limited AS AggregateFunction(inventory(1, 2), UInt64)", identity, name, {}, limits)),
            Exception);
    }
    {
        DefinitionLoweringLimits limits;
        limits.field_values.maximum_edges = 2;
        limits.field_values.maximum_entries = 2;
        EXPECT_NO_THROW(
            static_cast<void>(
                lower("CREATE TYPE app.Limited AS AggregateFunction(inventory([1], [2]), UInt64)", identity, name, {}, limits)));
        limits.field_values.maximum_edges = 1;
        limits.field_values.maximum_entries = 1;
        EXPECT_THROW(
            static_cast<void>(
                lower("CREATE TYPE app.Limited AS AggregateFunction(inventory([1], [2]), UInt64)", identity, name, {}, limits)),
            Exception);
    }
    {
        DefinitionLoweringLimits limits;
        limits.field_values.maximum_literal_bytes = 3;
        EXPECT_NO_THROW(
            static_cast<void>(
                lower("CREATE TYPE app.Limited AS AggregateFunction(inventory('a', 'b', 'c'), UInt64)", identity, name, {}, limits)));
        limits.field_values.maximum_literal_bytes = 2;
        EXPECT_THROW(
            static_cast<void>(
                lower("CREATE TYPE app.Limited AS AggregateFunction(inventory('a', 'b', 'c'), UInt64)", identity, name, {}, limits)),
            Exception);
    }
    {
        const std::array bindings{
            bindingFor("First", testIdentity(0x91), {}),
            bindingFor("Second", testIdentity(0x92), {}),
        };
        DefinitionLoweringLimits binding_limits;
        binding_limits.maximum_definitions = 1;
        EXPECT_THROW(static_cast<void>(lower("CREATE TYPE app.Limited AS UInt64", identity, name, bindings, binding_limits)), Exception);

        DefinitionLoweringLimits limits;
        limits.maximum_dependencies = 1;
        EXPECT_THROW(
            static_cast<void>(lower("CREATE TYPE app.Limited AS Tuple(app.First, app.Second)", identity, name, bindings, limits)),
            Exception);
    }
}

TEST(UDTDefinitionLowering, WholeDatabaseBindingLimitMatchesTheFrozenAuthorityProfile)
{
    std::vector<AvailableDefinitionBinding> bindings;
    bindings.reserve(10'000);
    for (UInt64 index = 0; index < 9'999; ++index)
        bindings.push_back(bindingFor("Bound" + toString(index), testIdentity(0x10'000 + index), {}));

    EXPECT_NO_THROW(static_cast<void>(lower("CREATE TYPE app.Limited AS UInt64", testIdentity(0x20'000), nameFor("Limited"), bindings)));

    bindings.push_back(bindingFor("Limited", testIdentity(0x20'000), {}));
    EXPECT_NO_THROW(static_cast<void>(lower("CREATE TYPE app.Limited AS UInt64", testIdentity(0x20'000), nameFor("Limited"), bindings)));

    EXPECT_THROW(
        static_cast<void>(lower("CREATE TYPE app.Overflow AS UInt64", testIdentity(0x30'000), nameFor("Overflow"), bindings)), Exception);

    auto mismatched_current = bindings.back();
    mismatched_current.name = nameFor("Other");
    bindings.back() = mismatched_current;
    EXPECT_THROW(
        static_cast<void>(lower("CREATE TYPE app.Limited AS UInt64", testIdentity(0x20'000), nameFor("Limited"), bindings)), Exception);
}

TEST(UDTDefinitionLowering, PreparedWholeAuthorityBindingsAreValidatedAndIndexedOnce)
{
    std::vector<AvailableDefinitionBinding> bindings;
    bindings.reserve(10'000);
    for (UInt64 index = 0; index < 10'000; ++index)
        bindings.push_back(bindingFor("Bound" + toString(index), testIdentity(0x40'000 + index), {}));

    auto prepared = prepareDefinitionLoweringBindings(testIdentity(1).database_uuid, "app", std::move(bindings));
    const auto preparation_statistics = prepared.getStatistics();
    EXPECT_EQ(
        preparation_statistics,
        (DefinitionLoweringBindingPreparationStatistics{
            .validated_bindings = 10'000,
            .catalog_string_bytes = preparation_statistics.catalog_string_bytes,
            .name_index_entries = 10'000,
            .identity_index_entries = 10'000,
        }));
    EXPECT_GT(preparation_statistics.catalog_string_bytes, 0);

    const auto lower_prepared = [&](UInt64 current, UInt64 dependency)
    {
        const String current_name = "Bound" + toString(current);
        const String dependency_name = "Bound" + toString(dependency);
        const ASTPtr ast = parseTypeQuery(
            "ATTACH TYPE app." + current_name + " UUID '" + toString(testIdentity(0x40'000 + current).type_uuid) + "' REVISION 1 AS app."
            + dependency_name + " DEFINITION HASH '" + String(64, '0') + "'");
        return lowerCreateTypeQueryToDefinitionInput(
            ast->as<ASTCreateTypeQuery &>(), testIdentity(0x40'000 + current), nameFor(current_name), prepared);
    };

    const auto first = lower_prepared(0, 9'999);
    const auto middle = lower_prepared(5'000, 0);
    const auto last = lower_prepared(9'999, 5'000);
    ASSERT_EQ(first.dependencies.size(), 1);
    ASSERT_EQ(middle.dependencies.size(), 1);
    ASSERT_EQ(last.dependencies.size(), 1);
    EXPECT_EQ(first.dependencies.front().type_uuid, testIdentity(0x40'000 + 9'999).type_uuid);
    EXPECT_EQ(middle.dependencies.front().type_uuid, testIdentity(0x40'000).type_uuid);
    EXPECT_EQ(last.dependencies.front().type_uuid, testIdentity(0x40'000 + 5'000).type_uuid);
    EXPECT_EQ(prepared.getStatistics(), preparation_statistics);
}

TEST(UDTDefinitionLowering, PreparedBindingsRejectAmbiguousOrCrossAuthorityCatalogs)
{
    const auto database_uuid = testIdentity(1).database_uuid;
    const auto first = bindingFor("First", testIdentity(0x50'000), {});
    auto duplicate_name = bindingFor("First", testIdentity(0x50'001), {});
    EXPECT_THROW(static_cast<void>(prepareDefinitionLoweringBindings(database_uuid, "app", {first, duplicate_name})), Exception);

    duplicate_name = bindingFor("Second", first.identity, {});
    EXPECT_THROW(static_cast<void>(prepareDefinitionLoweringBindings(database_uuid, "app", {first, duplicate_name})), Exception);

    auto cross_database = bindingFor("Second", testIdentity(0x50'002), {});
    cross_database.identity.database_uuid = testUUID(0xdead);
    EXPECT_THROW(static_cast<void>(prepareDefinitionLoweringBindings(database_uuid, "app", {first, cross_database})), Exception);

    auto cross_name = bindingFor("Second", testIdentity(0x50'003), {});
    cross_name.name.normalized_database_name = "other";
    cross_name.name.normalized_qualified_name = "other.Second";
    EXPECT_THROW(static_cast<void>(prepareDefinitionLoweringBindings(database_uuid, "app", {first, cross_name})), Exception);
}

}
}
