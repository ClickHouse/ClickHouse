#include <DataTypes/UDT/IAuthorityAdapter.h>
#include <DataTypes/UDT/TemplateChecker.h>

#include <Core/Field.h>
#include <Common/Exception.h>

#include <gtest/gtest.h>

#include <limits>
#include <optional>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
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

Definition::Ptr checkedDefinition(UUID database_uuid)
{
    DefinitionInput input;
    input.identity = {.database_uuid = database_uuid, .type_uuid = testUUID(100, 200), .revision = 1};
    input.normalized_name = "value";
    TemplateNode node;
    node.kind = TemplateNodeKind::BuiltIn;
    node.atom = "UInt64";
    input.nodes.push_back(std::move(node));

    auto definitions = TemplateChecker::checkAll({std::move(input)});
    return definitions.front();
}

Definition::Ptr checkedAggregateStateDefinition(UUID database_uuid, String state_name, String state_data)
{
    DefinitionInput input;
    input.identity = {.database_uuid = database_uuid, .type_uuid = testUUID(100, 201), .revision = 1};
    input.normalized_name = "aggregate_state_value";

    TemplateNode root;
    root.kind = TemplateNodeKind::BuiltIn;
    root.atom = "AggregateFunction";
    root.children = {{.reference = 1, .label = {}}, {.reference = 2, .label = {}}};

    TemplateNode function;
    function.kind = TemplateNodeKind::AggregateFunction;
    function.text = "udtAuthorityAccountingFunctionThatMustNotExist";
    function.children = {{.reference = 3, .label = {}}};

    TemplateNode argument_type;
    argument_type.kind = TemplateNodeKind::BuiltIn;
    argument_type.atom = "UInt64";

    TemplateNode field;
    field.kind = TemplateNodeKind::FieldValue;
    field.field_value
        = CanonicalFieldValue::fromField(Field(AggregateFunctionStateData{.name = std::move(state_name), .data = std::move(state_data)}));
    input.nodes = {std::move(root), std::move(function), std::move(argument_type), std::move(field)};

    auto definitions = TemplateChecker::checkAll({std::move(input)});
    return definitions.front();
}

UInt64 expectedAuthorityDefinitionBytes(const Definition & definition)
{
    UInt64 result = sizeof(Definition) + definition.getNormalizedName().size() + definition.getNormalizedLocalName().size();
    result += definition.getParameters().size() * sizeof(Parameter);
    for (const auto & parameter : definition.getParameters())
        result += parameter.normalized_name.size();
    result += definition.getNodes().size() * sizeof(TemplateNode);
    for (const auto & node : definition.getNodes())
    {
        result += node.atom.size();
        result += node.text.size();
        result += node.field_value.payload.size();
        result += node.field_value.name.size();
        result += node.enum_entries.size() * sizeof(SpecializedEnumEntry);
        for (const auto & entry : node.enum_entries)
            result += entry.name.size();
        result += node.children.size() * sizeof(TemplateNodeChild);
        for (const auto & child : node.children)
            result += child.label.size();
    }
    result += definition.getDependencies().size() * sizeof(DefinitionDependency);
    result += definition.getCertificate().canonical_template_ir.size();
    result += definition.getCertificate().encoded_certificate.size();
    return result;
}

template <typename Function>
void expectAuthorityError(int code, Function && function)
{
    try
    {
        function();
        FAIL() << "expected a transient authority error";
    }
    catch (const Exception & error)
    {
        EXPECT_EQ(error.code(), code);
    }
}

}

TEST(UDTAuthorityAdapter, UnsupportedAuthorityFailsClosed)
{
    const auto adapter = makeUnsupportedAuthorityAdapter();

    EXPECT_EQ(adapter->getDatabaseUUID(), UUIDHelpers::Nil);
    EXPECT_EQ(adapter->getCapabilities().mask, 0U);
    EXPECT_THROW(static_cast<void>(adapter->beginResolutionSession()), Exception);
    EXPECT_NO_THROW(adapter->requireCapabilities(0, "physical-only operation"));
    EXPECT_THROW(
        adapter->requireCapabilities(typeAuthorityCapabilityBit(TypeAuthorityCapability::TransientResolution), "binding"), Exception);
}

TEST(UDTAuthorityAdapter, TransientAuthorityAdvertisesOnlyImplementedContract)
{
    const auto capabilities = transientCapabilities();
    const auto database_uuid = testUUID(1, 2);
    const auto adapter = makeTransientAuthorityAdapter(database_uuid, capabilities, {});

    EXPECT_EQ(adapter->getDatabaseUUID(), database_uuid);
    EXPECT_TRUE(adapter->getCapabilities().contains(TypeAuthorityCapability::TransientResolution));
    EXPECT_NO_THROW({
        const auto session = adapter->beginResolutionSession();
        EXPECT_EQ(session.getGeneration(), 1U);
        EXPECT_FALSE(session.findByName("missing"));
        EXPECT_FALSE(
            session.findByIdentity(DefinitionIdentity{.database_uuid = database_uuid, .type_uuid = testUUID(9, 10), .revision = 1}));
    });
    EXPECT_NO_THROW(adapter->requireCapabilities(capabilities.mask, "synthetic binding"));
    EXPECT_THROW(
        adapter->requireCapabilities(typeAuthorityCapabilityBit(TypeAuthorityCapability::DurableAlias), "durable schema"), Exception);
}

TEST(UDTAuthorityAdapter, ResolutionSessionKeepsAuthorityStateAlive)
{
    const auto database_uuid = testUUID(10, 20);
    const auto definition = checkedDefinition(database_uuid);
    AuthorityAdapterPtr adapter
        = makeTransientAuthorityAdapter(database_uuid, transientCapabilities(), {definition});
    auto session = adapter->beginResolutionSession();

    adapter.reset();

    EXPECT_EQ(session.getGeneration(), 1U);
    EXPECT_EQ(session.findByIdentity(definition->getIdentity()), definition);
    EXPECT_EQ(session.findByName(definition->getNormalizedLocalName()), definition);
}

TEST(UDTAuthorityAdapter, TransientAuthorityCannotClaimDurabilityOrUseNilIdentity)
{
    auto capabilities = transientCapabilities();

    EXPECT_THROW(makeTransientAuthorityAdapter(UUIDHelpers::Nil, capabilities, {}), Exception);

    capabilities.mask |= typeAuthorityCapabilityBit(TypeAuthorityCapability::DurableAlias);
    EXPECT_THROW(makeTransientAuthorityAdapter(testUUID(3, 4), capabilities, {}), Exception);
}

TEST(UDTAuthorityAdapter, TransientAuthorityRequiresNonzeroLimits)
{
    auto capabilities = transientCapabilities();
    capabilities.limits.maximum_checker_work = 0;
    EXPECT_THROW(makeTransientAuthorityAdapter(testUUID(5, 6), capabilities, {}), Exception);
}

TEST(UDTAuthorityAdapter, TransientAuthorityChecksDefinitionLimitsBeforeCatalogBuild)
{
    const auto database_uuid = testUUID(30, 40);
    const auto definition = checkedDefinition(database_uuid);

    auto bytes = transientCapabilities();
    bytes.limits.maximum_definition_bytes = 1;
    EXPECT_THROW(makeTransientAuthorityAdapter(database_uuid, bytes, {definition}), Exception);

    auto work = transientCapabilities();
    work.limits.maximum_checker_work = 1;
    ASSERT_GT(definition->getCertificate().charged_work, work.limits.maximum_checker_work);
    EXPECT_THROW(makeTransientAuthorityAdapter(database_uuid, work, {definition}), Exception);

    EXPECT_THROW(makeTransientAuthorityAdapter(testUUID(31, 41), transientCapabilities(), {definition}), Exception);
}

TEST(UDTAuthorityAdapter, AggregateStateNameAndPayloadAreChargedBeforeAuthorityPublication)
{
    const auto database_uuid = testUUID(50, 60);
    const auto definition = checkedAggregateStateDefinition(database_uuid, String(8ULL << 10, 'n'), String(8ULL << 10, 'd'));
    const UInt64 exact_definition_bytes = expectedAuthorityDefinitionBytes(*definition);
    EXPECT_EQ(
        tryCountLogicalRetainedDefinitionBytes(*definition, std::numeric_limits<UInt64>::max()),
        std::optional<UInt64>(exact_definition_bytes));
    ASSERT_GT(exact_definition_bytes, definition->getCertificate().encoded_certificate.size());

    auto exact = transientCapabilities();
    exact.limits.maximum_definition_bytes = exact_definition_bytes;
    const auto adapter = makeTransientAuthorityAdapter(database_uuid, exact, {definition});
    const auto session = adapter->beginResolutionSession();
    EXPECT_EQ(session.findByIdentity(definition->getIdentity()), definition);

    auto one_byte_short = exact;
    one_byte_short.limits.maximum_definition_bytes = exact_definition_bytes - 1;
    expectAuthorityError(
        ErrorCodes::BAD_ARGUMENTS,
        [&] { static_cast<void>(makeTransientAuthorityAdapter(database_uuid, one_byte_short, {definition})); });

    EXPECT_EQ(session.findByIdentity(definition->getIdentity()), definition);
    EXPECT_EQ(session.getGeneration(), 1);
}

}
