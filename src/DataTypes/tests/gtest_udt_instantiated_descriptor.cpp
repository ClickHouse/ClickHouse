#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/UDT/CanonicalHash.h>
#include <DataTypes/UDT/InstantiatedTypeDescriptor.h>
#include <DataTypes/UDT/PhysicalTypeFingerprint.h>
#include <DataTypes/UDT/TemplateChecker.h>
#include <DataTypes/UDT/Catalog.h>

#include <Parsers/ParserDataType.h>
#include <Parsers/parseQuery.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <memory>
#include <span>
#include <string_view>
#include <type_traits>
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

String toHex(const Digest & digest)
{
    constexpr char digits[] = "0123456789abcdef";
    String result;
    result.reserve(digest.size() * 2);
    for (const CanonicalByte byte : digest)
    {
        result.push_back(digits[byte >> 4]);
        result.push_back(digits[byte & 0x0f]);
    }
    return result;
}

void appendUInt16LE(String & output, UInt16 value)
{
    output.push_back(static_cast<char>(value));
    output.push_back(static_cast<char>(value >> 8));
}

void appendUInt64LE(String & output, UInt64 value)
{
    for (size_t index = 0; index < sizeof(value); ++index)
        output.push_back(static_cast<char>(value >> (index * 8)));
}

void appendVarUInt(String & output, UInt64 value)
{
    while (value >= 0x80)
    {
        output.push_back(static_cast<char>(static_cast<UInt8>(value) | 0x80));
        value >>= 7;
    }
    output.push_back(static_cast<char>(value));
}

void appendFrame(String & output, std::string_view value)
{
    appendVarUInt(output, value.size());
    output.append(value);
}

void appendUUID(String & output, const UUID & value)
{
    const auto bytes = uuidToCanonicalBytes(value);
    output.append(reinterpret_cast<const char *>(bytes.data()), bytes.size());
}

void appendDigest(String & output, const Digest & value)
{
    output.append(reinterpret_cast<const char *>(value.data()), value.size());
}

DefinitionIdentity testIdentity(UInt64 type_id)
{
    return {
        .database_uuid = testUUID(0x550e8400e29b41d4ULL, 0xa716446655440000ULL),
        .type_uuid = testUUID(0x123456789abcdef0ULL, type_id),
        .revision = 1,
    };
}

TemplateNode builtIn(String family)
{
    TemplateNode result;
    result.kind = TemplateNodeKind::BuiltIn;
    result.atom = std::move(family);
    return result;
}

Definition::Ptr
checkedDefinition(String normalized_name, UInt64 type_id, String family = "UInt64", SemanticCapabilityMask capabilities = 0)
{
    DefinitionInput input;
    input.identity = testIdentity(type_id);
    const auto separator = normalized_name.find('.');
    input.normalized_local_name = separator == String::npos ? normalized_name : normalized_name.substr(separator + 1);
    input.normalized_name = std::move(normalized_name);
    input.nodes.push_back(builtIn(std::move(family)));
    input.semantic_capabilities = capabilities;
    if (capabilities != 0)
    {
        input.policy_bearing = true;
        input.policy_semantic_hash = hashDomainSeparated("ClickHouse UDT instantiated descriptor test policy V1", input.normalized_name);
    }
    auto checked = TemplateChecker::checkAll({std::move(input)});
    EXPECT_EQ(checked.size(), 1);
    return checked.front();
}

Definition::Ptr checkedTypeAlias(String normalized_name, UInt64 type_id, SemanticCapabilityMask capabilities = 0)
{
    DefinitionInput input;
    input.identity = testIdentity(type_id);
    const auto separator = normalized_name.find('.');
    input.normalized_local_name = separator == String::npos ? normalized_name : normalized_name.substr(separator + 1);
    input.normalized_name = std::move(normalized_name);
    input.parameters.push_back({.normalized_name = "T", .kind = ParameterKind::Type});
    TemplateNode root;
    root.kind = TemplateNodeKind::TypeParameter;
    input.nodes.push_back(std::move(root));
    input.semantic_capabilities = capabilities;
    if (capabilities != 0)
    {
        input.policy_bearing = true;
        input.policy_semantic_hash = hashDomainSeparated("ClickHouse UDT instantiated descriptor test policy V1", input.normalized_name);
    }
    auto checked = TemplateChecker::checkAll({std::move(input)});
    EXPECT_EQ(checked.size(), 1);
    return checked.front();
}

ASTPtr parseType(const String & text)
{
    ParserDataType parser;
    return parseQuery(parser, text, "instantiated descriptor test", 0, 150, 0);
}

CanonicalTypeArguments noArguments()
{
    return CanonicalTypeArguments::validate({}, {});
}

CanonicalTypeArguments oneTypeArgument(const String & type)
{
    const std::vector<Parameter> parameters{{.normalized_name = "T", .kind = ParameterKind::Type}};
    std::vector<CanonicalTypeArgumentValue> values;
    values.push_back(CanonicalTypeArgumentValue::type(parseType(type)));
    return CanonicalTypeArguments::validate(parameters, std::move(values));
}

InstantiatedTypeDescriptor::Ptr instantiate(const Definition::Ptr & definition, const DataTypePtr & physical_type)
{
    return InstantiatedTypeDescriptor::create(definition, noArguments(), physical_type);
}

template <typename Function>
void expectDescriptorError(DescriptorError::Code code, Function && function)
{
    try
    {
        function();
        FAIL() << "expected a user-defined type descriptor error";
    }
    catch (const DescriptorError & error)
    {
        EXPECT_EQ(error.code, code);
    }
}

bool hasDefinition(const std::vector<Definition::Ptr> & definitions, const DefinitionIdentity & expected)
{
    return std::any_of(
        definitions.begin(), definitions.end(), [&](const auto & definition) { return definition->getIdentity() == expected; });
}

}

static_assert(!std::is_copy_constructible_v<InstantiatedTypeDescriptor>);
static_assert(!std::is_move_constructible_v<InstantiatedTypeDescriptor>);
static_assert(!std::is_copy_constructible_v<BoundDeclaredTypeTree>);
static_assert(!std::is_move_constructible_v<BoundDeclaredTypeTree>);
static_assert(std::is_copy_constructible_v<PersistedTypeDescriptor>);
static_assert(!std::is_aggregate_v<PersistedTypeDescriptor>);

TEST(UDTInstantiatedDescriptor, UsesAdditiveCanonicalGoldenEncoding)
{
    EXPECT_EQ(instantiation_semantic_hash_domain, "ClickHouse UDT instantiation hash V2");
    const String canonical_arguments{"\x01\x00\x02\xff", 4};
    InstantiationSemanticHashInput input{
        .definition_identity{
            .database_uuid = testUUID(0x0102030405060708ULL, 0x1112131415161718ULL),
            .type_uuid = testUUID(0x2122232425262728ULL, 0x3132333435363738ULL),
            .revision = 0x4142434445464748ULL,
        },
        .canonical_arguments_encoding = canonical_arguments,
        .canonical_physical_type = "Tuple(UInt64)",
        .checker_abi = 0x5152,
        .checker_charge_abi = 0x5354,
        .policy_abi = 0x5556,
        .function_registry_abi = 0x5758,
        .semantic_capabilities = 0x0d,
    };
    for (size_t index = 0; index < input.definition_hash.size(); ++index)
    {
        input.definition_hash[index] = static_cast<CanonicalByte>(index);
        input.storage_fingerprint[index] = static_cast<CanonicalByte>(index + 32);
        input.policy_semantic_hash[index] = static_cast<CanonicalByte>(index + 64);
    }

    /// Independently assemble the frozen V2 frame. V1 is intentionally not
    /// reused: it committed checker-certificate serialization and had a
    /// different field set/order.
    String payload;
    appendUInt16LE(payload, 2);
    appendUUID(payload, input.definition_identity.database_uuid);
    appendUUID(payload, input.definition_identity.type_uuid);
    appendUInt64LE(payload, input.definition_identity.revision);
    appendDigest(payload, input.definition_hash);
    appendFrame(payload, input.canonical_arguments_encoding);
    appendFrame(payload, input.canonical_physical_type);
    appendDigest(payload, input.storage_fingerprint);
    appendUInt16LE(payload, input.checker_abi);
    appendUInt16LE(payload, input.checker_charge_abi);
    appendUInt16LE(payload, input.policy_abi);
    appendUInt16LE(payload, input.function_registry_abi);
    appendDigest(payload, input.policy_semantic_hash);
    payload.push_back(static_cast<char>(input.semantic_capabilities));
    appendVarUInt(payload, 0); /// resolver and catalog's semantically absent program set.

    ASSERT_GE(payload.size(), 2);
    EXPECT_EQ(static_cast<UInt8>(payload[0]), 2);
    EXPECT_EQ(static_cast<UInt8>(payload[1]), 0);
    const Digest independently_hashed = hashDomainSeparated(instantiation_semantic_hash_domain, payload);
    EXPECT_EQ(computeInstantiationSemanticHash(input), independently_hashed);
    EXPECT_EQ(toHex(independently_hashed), "ab4ff855016ee6d56637970dad096c809d1c70f48f1ec041faf4f2b8fd67726c");
}

TEST(UDTInstantiatedDescriptor, FreezesCanonicalIdentityWithoutRetainingRuntimeOwners)
{
    auto definition = checkedTypeAlias(
        "db.Generic", 1, semanticCapabilityBit(SemanticCapability::Input) | semanticCapabilityBit(SemanticCapability::ValueChecks));
    const auto physical = DataTypeFactory::instance().get("Tuple(UInt64, String)");
    auto first = InstantiatedTypeDescriptor::create(definition, oneTypeArgument("Tuple(UInt64, String)"), physical);
    auto second = InstantiatedTypeDescriptor::create(
        definition, oneTypeArgument("Tuple(UInt64, String)"), DataTypeFactory::instance().get("Tuple(UInt64, String)"));

    const auto & persisted = first->getPersistedDescriptor();
    EXPECT_EQ(persisted, second->getPersistedDescriptor());
    EXPECT_TRUE(persisted.hasSameInstantiation(second->getPersistedDescriptor()));
    EXPECT_EQ(persisted.getDefinitionIdentity(), definition->getIdentity());
    EXPECT_EQ(persisted.getDefinitionHash(), definition->getDefinitionHash());
    EXPECT_EQ(persisted.getCanonicalArgumentsEncoding(), first->getCanonicalArguments().encoded());
    EXPECT_EQ(persisted.getCanonicalPhysicalType(), "Tuple(UInt64, String)");
    EXPECT_EQ(persisted.getStorageFingerprint(), physicalTypeFingerprint(physical));
    EXPECT_NE(persisted.getInstantiationSemanticHash(), Digest{});
    EXPECT_EQ(persisted.getCheckerABI(), definition->getCheckerABI());
    EXPECT_EQ(persisted.getCheckerChargeABI(), definition->getCheckerChargeABI());
    EXPECT_EQ(persisted.getPolicyABI(), definition->getPolicyABI());
    EXPECT_EQ(persisted.getFunctionRegistryABI(), definition->getFunctionRegistryABI());
    EXPECT_EQ(persisted.getPolicySemanticHash(), definition->getPolicySemanticHash());
    EXPECT_EQ(persisted.getSemanticCapabilities(), definition->getSemanticCapabilities());

    PersistedTypeDescriptor detached = persisted;
    std::weak_ptr<const Definition> weak_definition = definition;
    first.reset();
    definition.reset();
    EXPECT_FALSE(weak_definition.expired());
    second.reset();
    EXPECT_TRUE(weak_definition.expired());
    EXPECT_EQ(detached.getCanonicalPhysicalType(), "Tuple(UInt64, String)");
    EXPECT_NE(detached.getInstantiationSemanticHash(), Digest{});
}

TEST(UDTInstantiatedDescriptor, OrderingIsDeterministicAndRenameIsDiagnosticOnly)
{
    const auto original = checkedDefinition("db.Original", 2);
    const auto renamed = checkedDefinition("db.Renamed", 2);
    ASSERT_EQ(original->getDefinitionHash(), renamed->getDefinitionHash());
    const auto physical = DataTypeFactory::instance().get("UInt64");
    const auto first = instantiate(original, physical);
    const auto second = instantiate(renamed, physical);

    EXPECT_TRUE(first->getPersistedDescriptor().hasSameInstantiation(second->getPersistedDescriptor()));
    EXPECT_NE(first->getPersistedDescriptor(), second->getPersistedDescriptor());
    EXPECT_FALSE(first->getPersistedDescriptor().stableLess(second->getPersistedDescriptor()));
    EXPECT_FALSE(second->getPersistedDescriptor().stableLess(first->getPersistedDescriptor()));

    const auto different = instantiate(checkedDefinition("db.Different", 3, "String"), DataTypeFactory::instance().get("String"));
    const bool forward = first->getPersistedDescriptor().stableLess(different->getPersistedDescriptor());
    const bool backward = different->getPersistedDescriptor().stableLess(first->getPersistedDescriptor());
    EXPECT_NE(forward, backward);
}

TEST(UDTInstantiatedDescriptor, RejectsMalformedInputsAndEveryDescriptorByteLimit)
{
    const auto physical = DataTypeFactory::instance().get("UInt64");
    expectDescriptorError(
        DescriptorError::Code::InvalidDefinition,
        [&] { static_cast<void>(InstantiatedTypeDescriptor::create(nullptr, noArguments(), physical)); });

    const auto generic = checkedTypeAlias("db.Generic", 4);
    expectDescriptorError(
        DescriptorError::Code::InvalidArguments,
        [&] { static_cast<void>(InstantiatedTypeDescriptor::create(generic, noArguments(), physical)); });

    const std::vector<Parameter> integer_parameter{{.normalized_name = "N", .kind = ParameterKind::UInt64}};
    std::vector<CanonicalTypeArgumentValue> integer_value;
    integer_value.push_back(CanonicalTypeArgumentValue::unsignedInteger(ParameterKind::UInt64, 7));
    auto wrong_kind = CanonicalTypeArguments::validate(integer_parameter, std::move(integer_value));
    expectDescriptorError(
        DescriptorError::Code::InvalidArguments,
        [&] { static_cast<void>(InstantiatedTypeDescriptor::create(generic, std::move(wrong_kind), physical)); });
    expectDescriptorError(
        DescriptorError::Code::InvalidPhysicalType,
        [&] { static_cast<void>(InstantiatedTypeDescriptor::create(generic, oneTypeArgument("UInt64"), nullptr)); });

    /// `FIXED` is a case-insensitive DataTypeFactory alias for Decimal, so a
    /// UDT with local name `Fixed` is correctly rejected by TemplateChecker.
    /// Keep this descriptor-limit fixture outside the frozen built-in namespace.
    const auto fixed = checkedDefinition("db.DescriptorFixed", 5);
    TypeDescriptorLimits argument_limit;
    argument_limit.maximum_canonical_arguments_bytes = noArguments().encoded().size() - 1;
    expectDescriptorError(
        DescriptorError::Code::LimitExceeded,
        [&] { static_cast<void>(InstantiatedTypeDescriptor::create(fixed, noArguments(), physical, argument_limit)); });

    TypeDescriptorLimits physical_limit;
    physical_limit.maximum_canonical_physical_type_bytes = physical->getName().size() - 1;
    expectDescriptorError(
        DescriptorError::Code::LimitExceeded,
        [&] { static_cast<void>(InstantiatedTypeDescriptor::create(fixed, noArguments(), physical, physical_limit)); });

    TypeDescriptorLimits name_limit;
    name_limit.maximum_qualified_name_bytes = fixed->getNormalizedName().size() - 1;
    expectDescriptorError(
        DescriptorError::Code::LimitExceeded,
        [&] { static_cast<void>(InstantiatedTypeDescriptor::create(fixed, noArguments(), physical, name_limit)); });

    TypeDescriptorLimits invalid_limit;
    invalid_limit.maximum_nodes = 0;
    expectDescriptorError(
        DescriptorError::Code::LimitExceeded,
        [&] { static_cast<void>(InstantiatedTypeDescriptor::create(fixed, noArguments(), physical, invalid_limit)); });
}

TEST(UDTBoundDeclaredTypeTree, NormalizesPhysicalTreeLogicalLeavesAndCapabilityAggregation)
{
    const auto tuple = DataTypeFactory::instance().get("Tuple(UInt64, String)");
    const auto uint64 = DataTypeFactory::instance().get("UInt64");
    const auto string = DataTypeFactory::instance().get("String");
    const auto input_definition = checkedDefinition("db.Input", 10, "UInt64", semanticCapabilityBit(SemanticCapability::Input));
    const auto output_definition = checkedDefinition(
        "db.Output",
        11,
        "String",
        semanticCapabilityBit(SemanticCapability::Output) | semanticCapabilityBit(SemanticCapability::ValueChecks));
    const auto transitive_definition = checkedDefinition("db.Transitive", 12);
    const auto input_descriptor = instantiate(input_definition, uint64);
    const auto output_descriptor = instantiate(output_definition, string);

    std::vector<BoundDeclaredTypeNodeInput> inputs{
        {.type_child_ordinals = {1}, .physical_type = string},
        {.type_child_ordinals = {}, .physical_type = tuple},
        {.type_child_ordinals = {0}, .physical_type = uint64},
    };
    std::vector<BoundDeclaredTypeOccurrenceInput> occurrences{
        {.type_child_ordinals = {1}, .logical_descriptor = output_descriptor, .logical_preorder = 1},
        {.type_child_ordinals = {0}, .logical_descriptor = input_descriptor, .logical_preorder = 0},
    };
    const auto tree = BoundDeclaredTypeTree::build(std::move(inputs), std::move(occurrences), {transitive_definition});
    std::vector<BoundDeclaredTypeNodeInput> differently_ordered_inputs{
        {.type_child_ordinals = {0}, .physical_type = uint64},
        {.type_child_ordinals = {1}, .physical_type = string},
        {.type_child_ordinals = {}, .physical_type = tuple},
    };
    std::vector<BoundDeclaredTypeOccurrenceInput> differently_ordered_occurrences{
        {.type_child_ordinals = {0}, .logical_descriptor = input_descriptor, .logical_preorder = 0},
        {.type_child_ordinals = {1}, .logical_descriptor = output_descriptor, .logical_preorder = 1},
    };
    const auto same_tree = BoundDeclaredTypeTree::build(
        std::move(differently_ordered_inputs), std::move(differently_ordered_occurrences), {transitive_definition});

    EXPECT_EQ(tree->getNodeCount(), 3);
    EXPECT_EQ(tree->getEdgeCount(), 2);
    ASSERT_EQ(same_tree->getNodeCount(), tree->getNodeCount());
    for (BoundDeclaredTypeNodeID node = 0; node < tree->getNodeCount(); ++node)
    {
        EXPECT_EQ(tree->getNode(node).getParent(), same_tree->getNode(node).getParent());
        EXPECT_EQ(tree->getNode(node).getChildOrdinal(), same_tree->getNode(node).getChildOrdinal());
        EXPECT_TRUE(std::ranges::equal(tree->getDescriptorIndices(node), same_tree->getDescriptorIndices(node)));
    }
    EXPECT_TRUE(tree->getPhysicalType()->equals(*tuple));
    EXPECT_EQ(tree->findNode(std::span<const UInt32>{}), 0);
    const std::array<UInt32, 1> first_path{0};
    const std::array<UInt32, 1> second_path{1};
    ASSERT_EQ(tree->findNode(first_path), 1);
    ASSERT_EQ(tree->findNode(second_path), 2);
    EXPECT_EQ(tree->getNode(1).getParent(), 0);
    EXPECT_EQ(tree->getNode(1).getChildOrdinal(), 0);
    EXPECT_EQ(tree->getNode(2).getChildOrdinal(), 1);
    EXPECT_EQ(tree->getNode(0).getOwnSemanticCapabilities(), 0);
    EXPECT_EQ(tree->getNode(1).getOwnSemanticCapabilities(), semanticCapabilityBit(SemanticCapability::Input));
    EXPECT_EQ(
        tree->getNode(2).getOwnSemanticCapabilities(),
        semanticCapabilityBit(SemanticCapability::Output) | semanticCapabilityBit(SemanticCapability::ValueChecks));
    EXPECT_EQ(
        tree->getSemanticCapabilities(),
        semanticCapabilityBit(SemanticCapability::Input) | semanticCapabilityBit(SemanticCapability::Output)
            | semanticCapabilityBit(SemanticCapability::ValueChecks));
    EXPECT_EQ(tree->getDescriptors().size(), 2);
    EXPECT_EQ(tree->getOccurrenceCount(), 2);
    ASSERT_EQ(tree->getDescriptors().size(), same_tree->getDescriptors().size());
    for (size_t index = 0; index < tree->getDescriptors().size(); ++index)
    {
        EXPECT_EQ(tree->getDescriptors()[index]->getPersistedDescriptor(), same_tree->getDescriptors()[index]->getPersistedDescriptor());
    }
    EXPECT_FALSE(tree->getDescriptors()[1]->getPersistedDescriptor().stableLess(tree->getDescriptors()[0]->getPersistedDescriptor()));
    EXPECT_EQ(tree->getDefinitionHandles().size(), 3);
    EXPECT_TRUE(hasDefinition(tree->getDefinitionHandles(), input_definition->getIdentity()));
    EXPECT_TRUE(hasDefinition(tree->getDefinitionHandles(), output_definition->getIdentity()));
    EXPECT_TRUE(hasDefinition(tree->getDefinitionHandles(), transitive_definition->getIdentity()));
    EXPECT_EQ(tree->getDefinitionHandles()[0]->getIdentity(), input_definition->getIdentity());
    EXPECT_EQ(tree->getDefinitionHandles()[1]->getIdentity(), output_definition->getIdentity());
    EXPECT_EQ(tree->getDefinitionHandles()[2]->getIdentity(), transitive_definition->getIdentity());

    const auto result = BoundDeclaredTypeResult::withLogicalTree(tree);
    EXPECT_TRUE(result.hasLogicalTree());
    EXPECT_EQ(result.getLogicalTree(), tree);
    EXPECT_TRUE(result.getPhysicalType()->equals(*tuple));
}

TEST(UDTBoundDeclaredTypeTree, SparseWideOrdinalsUseOneEdgeAndBinaryLookup)
{
    const auto uint64 = DataTypeFactory::instance().get("UInt64");
    const DataTypes elements(10'000, uint64);
    const DataTypePtr wide_tuple = std::make_shared<DataTypeTuple>(elements);
    const auto descriptor = instantiate(checkedDefinition("db.SparseLeaf", 20), uint64);
    std::vector<BoundDeclaredTypeNodeInput> inputs{
        {.type_child_ordinals = {9'999}, .physical_type = uint64},
        {.type_child_ordinals = {}, .physical_type = wide_tuple},
    };
    std::vector<BoundDeclaredTypeOccurrenceInput> occurrences{
        {.type_child_ordinals = {9'999}, .logical_descriptor = descriptor, .logical_preorder = 0},
    };
    const auto tree = BoundDeclaredTypeTree::build(std::move(inputs), std::move(occurrences), {});

    EXPECT_EQ(tree->getNodeCount(), 2);
    EXPECT_EQ(tree->getEdgeCount(), 1);
    EXPECT_EQ(tree->getNode(0).getChildCount(), 1);
    EXPECT_EQ(tree->getOccurrenceCount(), 1);
    const std::array<UInt32, 1> present{9'999};
    const std::array<UInt32, 1> absent{0};
    EXPECT_EQ(tree->findNode(present), 1);
    EXPECT_FALSE(tree->findNode(absent));
}

TEST(UDTBoundDeclaredTypeTree, RepeatedOccurrencesInternOneDescriptorAndDefinition)
{
    const auto uint64 = DataTypeFactory::instance().get("UInt64");
    const auto tuple = DataTypeFactory::instance().get("Tuple(UInt64, UInt64)");
    const auto old_name = checkedDefinition("db.AOldName", 30);
    const auto new_name = checkedDefinition("db.ZNewName", 30);
    ASSERT_EQ(old_name->getDefinitionHash(), new_name->getDefinitionHash());
    const auto first = instantiate(new_name, uint64);
    const auto second = instantiate(old_name, uint64);
    std::vector<BoundDeclaredTypeNodeInput> inputs{
        {.type_child_ordinals = {}, .physical_type = tuple},
        {.type_child_ordinals = {0}, .physical_type = uint64},
        {.type_child_ordinals = {1}, .physical_type = uint64},
    };
    std::vector<BoundDeclaredTypeOccurrenceInput> occurrences{
        {.type_child_ordinals = {0}, .logical_descriptor = first, .logical_preorder = 0},
        {.type_child_ordinals = {1}, .logical_descriptor = second, .logical_preorder = 1},
    };
    const auto tree = BoundDeclaredTypeTree::build(std::move(inputs), std::move(occurrences), {new_name, old_name});

    ASSERT_EQ(tree->getDescriptors().size(), 1);
    ASSERT_EQ(tree->getDefinitionHandles().size(), 1);
    EXPECT_EQ(tree->getDescriptors().front()->getPersistedDescriptor().getLastKnownQualifiedName(), "db.AOldName");
    EXPECT_EQ(tree->getDefinitionHandles().front()->getNormalizedName(), "db.AOldName");
    ASSERT_EQ(tree->getDescriptorIndices(1).size(), 1);
    ASSERT_EQ(tree->getDescriptorIndices(2).size(), 1);
    EXPECT_EQ(tree->getDescriptorIndices(1).front(), tree->getDescriptorIndices(2).front());
    EXPECT_EQ(tree->getOccurrenceCount(), 2);
}

TEST(UDTBoundDeclaredTypeTree, SameIdentityAndForgedHashCannotConflateDifferentCheckedBodies)
{
    const auto uint64 = DataTypeFactory::instance().get("UInt64");
    const auto first = checkedDefinition("db.First", 33, "UInt64");
    const auto conflicting = checkedDefinition("db.Conflicting", 33, "String");
    ASSERT_NE(first->getDefinitionHash(), conflicting->getDefinitionHash());

    /// Model a corrupt/untrusted catalog payload that forged only the headline
    /// digest. The underlying object was allocated non-const by TemplateChecker;
    /// its public handle is const so production cannot perform this mutation.
    /// The descriptor boundary must still compare the complete checked body and
    /// certificate before interning an immutable identity.
    auto & forged_certificate = const_cast<TemplateCheckerCertificate &>(conflicting->getCertificate());
    forged_certificate.definition_hash = first->getDefinitionHash();
    ASSERT_EQ(first->getDefinitionHash(), conflicting->getDefinitionHash());
    ASSERT_NE(first->getNodes(), conflicting->getNodes());

    const auto descriptor = instantiate(first, uint64);
    std::vector<BoundDeclaredTypeNodeInput> nodes{{.type_child_ordinals = {}, .physical_type = uint64}};
    std::vector<BoundDeclaredTypeOccurrenceInput> occurrences{
        {.type_child_ordinals = {}, .logical_descriptor = descriptor, .logical_preorder = 0},
    };
    expectDescriptorError(
        DescriptorError::Code::ConflictingIdentity,
        [&] { static_cast<void>(BoundDeclaredTypeTree::build(std::move(nodes), std::move(occurrences), {conflicting})); });

    const auto first_descriptor = instantiate(first, uint64);
    const auto conflicting_descriptor = instantiate(conflicting, uint64);
    ASSERT_TRUE(first_descriptor->getPersistedDescriptor().hasSameInstantiation(conflicting_descriptor->getPersistedDescriptor()));
    std::vector<BoundDeclaredTypeNodeInput> occurrence_nodes{{.type_child_ordinals = {}, .physical_type = uint64}};
    std::vector<BoundDeclaredTypeOccurrenceInput> conflicting_occurrences{
        {.type_child_ordinals = {}, .logical_descriptor = first_descriptor, .logical_preorder = 0},
        {.type_child_ordinals = {}, .logical_descriptor = conflicting_descriptor, .logical_preorder = 1},
    };
    expectDescriptorError(
        DescriptorError::Code::ConflictingIdentity,
        [&] { static_cast<void>(BoundDeclaredTypeTree::build(std::move(occurrence_nodes), std::move(conflicting_occurrences), {})); });
}

TEST(UDTBoundDeclaredTypeTree, SamePathPreservesOuterToInnerOccurrences)
{
    const auto uint64 = DataTypeFactory::instance().get("UInt64");
    const auto outer_definition = checkedDefinition("db.Outer", 31, "UInt64", semanticCapabilityBit(SemanticCapability::Input));
    const auto inner_definition = checkedDefinition("db.Inner", 32, "UInt64", semanticCapabilityBit(SemanticCapability::Output));
    const auto outer = instantiate(outer_definition, uint64);
    const auto inner = instantiate(inner_definition, uint64);

    std::vector<BoundDeclaredTypeNodeInput> nodes{{.type_child_ordinals = {}, .physical_type = uint64}};
    std::vector<BoundDeclaredTypeOccurrenceInput> occurrences{
        {.type_child_ordinals = {}, .logical_descriptor = inner, .logical_preorder = 11},
        {.type_child_ordinals = {}, .logical_descriptor = outer, .logical_preorder = 10},
        {.type_child_ordinals = {}, .logical_descriptor = outer, .logical_preorder = 12},
    };
    const auto tree = BoundDeclaredTypeTree::build(std::move(nodes), std::move(occurrences), {});

    ASSERT_EQ(tree->getNodeCount(), 1);
    ASSERT_EQ(tree->getDescriptors().size(), 2);
    ASSERT_EQ(tree->getOccurrenceCount(), 3);
    const auto descriptor_indices = tree->getDescriptorIndices(0);
    ASSERT_EQ(descriptor_indices.size(), 3);
    EXPECT_TRUE(
        tree->getDescriptors()[descriptor_indices[0]]->getPersistedDescriptor().hasSameInstantiation(outer->getPersistedDescriptor()));
    EXPECT_TRUE(
        tree->getDescriptors()[descriptor_indices[1]]->getPersistedDescriptor().hasSameInstantiation(inner->getPersistedDescriptor()));
    EXPECT_TRUE(
        tree->getDescriptors()[descriptor_indices[2]]->getPersistedDescriptor().hasSameInstantiation(outer->getPersistedDescriptor()));
    EXPECT_EQ(
        tree->getNode(0).getOwnSemanticCapabilities(),
        semanticCapabilityBit(SemanticCapability::Input) | semanticCapabilityBit(SemanticCapability::Output));
    EXPECT_EQ(tree->getNode(0).getOwnSemanticCapabilities(), tree->getSemanticCapabilities());
}

TEST(UDTBoundDeclaredTypeTree, PhysicalOnlyResultAllocatesNoLogicalTree)
{
    const auto physical = DataTypeFactory::instance().get("Array(UInt64)");
    const auto result = BoundDeclaredTypeResult::physicalOnly(physical);
    EXPECT_FALSE(result.hasLogicalTree());
    EXPECT_EQ(result.getLogicalTree(), nullptr);
    EXPECT_EQ(result.getPhysicalType(), physical);
    expectDescriptorError(
        DescriptorError::Code::InvalidPhysicalType,
        [] { static_cast<void>(BoundDeclaredTypeResult::physicalOnly(nullptr)); });
}

TEST(UDTBoundDeclaredTypeTree, RejectsMalformedPathsPhysicalMismatchAndLimits)
{
    const auto uint64 = DataTypeFactory::instance().get("UInt64");
    const auto string = DataTypeFactory::instance().get("String");
    const auto descriptor = instantiate(checkedDefinition("db.Node", 40), uint64);
    const auto root_occurrence = [&]
    {
        return std::vector<BoundDeclaredTypeOccurrenceInput>{
            {.type_child_ordinals = {}, .logical_descriptor = descriptor, .logical_preorder = 0},
        };
    };

    expectDescriptorError(
        DescriptorError::Code::InvalidPath, [&] { static_cast<void>(BoundDeclaredTypeTree::build({}, {}, {})); });
    expectDescriptorError(
        DescriptorError::Code::InvalidPath,
        [&]
        {
            std::vector<BoundDeclaredTypeNodeInput> duplicate{
                {.type_child_ordinals = {}, .physical_type = uint64},
                {.type_child_ordinals = {}, .physical_type = uint64},
            };
            static_cast<void>(BoundDeclaredTypeTree::build(std::move(duplicate), root_occurrence(), {}));
        });
    expectDescriptorError(
        DescriptorError::Code::InvalidPath,
        [&]
        {
            std::vector<BoundDeclaredTypeNodeInput> missing_parent{
                {.type_child_ordinals = {}, .physical_type = uint64},
                {.type_child_ordinals = {0, 1}, .physical_type = uint64},
            };
            static_cast<void>(BoundDeclaredTypeTree::build(std::move(missing_parent), root_occurrence(), {}));
        });
    expectDescriptorError(
        DescriptorError::Code::InvalidPhysicalType,
        [&]
        {
            std::vector<BoundDeclaredTypeNodeInput> mismatch{
                {.type_child_ordinals = {}, .physical_type = string},
            };
            static_cast<void>(BoundDeclaredTypeTree::build(std::move(mismatch), root_occurrence(), {}));
        });
    expectDescriptorError(
        DescriptorError::Code::InvalidDefinition,
        [&]
        {
            std::vector<BoundDeclaredTypeNodeInput> physical_only{
                {.type_child_ordinals = {}, .physical_type = uint64},
            };
            static_cast<void>(BoundDeclaredTypeTree::build(std::move(physical_only), {}, {}));
        });
    expectDescriptorError(
        DescriptorError::Code::InvalidDefinition,
        [&]
        {
            std::vector<BoundDeclaredTypeNodeInput> root{{.type_child_ordinals = {}, .physical_type = uint64}};
            std::vector<BoundDeclaredTypeOccurrenceInput> null_descriptor{
                {.type_child_ordinals = {}, .logical_descriptor = nullptr, .logical_preorder = 0},
            };
            static_cast<void>(BoundDeclaredTypeTree::build(std::move(root), std::move(null_descriptor), {}));
        });
    expectDescriptorError(
        DescriptorError::Code::InvalidPath,
        [&]
        {
            std::vector<BoundDeclaredTypeNodeInput> root{{.type_child_ordinals = {}, .physical_type = uint64}};
            std::vector<BoundDeclaredTypeOccurrenceInput> missing_node{
                {.type_child_ordinals = {7}, .logical_descriptor = descriptor, .logical_preorder = 0},
            };
            static_cast<void>(BoundDeclaredTypeTree::build(std::move(root), std::move(missing_node), {}));
        });
    expectDescriptorError(
        DescriptorError::Code::InvalidPath,
        [&]
        {
            std::vector<BoundDeclaredTypeNodeInput> root{{.type_child_ordinals = {}, .physical_type = uint64}};
            std::vector<BoundDeclaredTypeOccurrenceInput> duplicate_preorder{
                {.type_child_ordinals = {}, .logical_descriptor = descriptor, .logical_preorder = 8},
                {.type_child_ordinals = {}, .logical_descriptor = descriptor, .logical_preorder = 8},
            };
            static_cast<void>(BoundDeclaredTypeTree::build(std::move(root), std::move(duplicate_preorder), {}));
        });

    TypeDescriptorLimits node_limit;
    node_limit.maximum_nodes = 1;
    expectDescriptorError(
        DescriptorError::Code::LimitExceeded,
        [&]
        {
            std::vector<BoundDeclaredTypeNodeInput> too_many{
                {.type_child_ordinals = {}, .physical_type = uint64},
                {.type_child_ordinals = {0}, .physical_type = uint64},
            };
            static_cast<void>(BoundDeclaredTypeTree::build(std::move(too_many), root_occurrence(), {}, node_limit));
        });

    TypeDescriptorLimits depth_limit;
    depth_limit.maximum_path_depth = 1;
    expectDescriptorError(
        DescriptorError::Code::LimitExceeded,
        [&]
        {
            std::vector<BoundDeclaredTypeNodeInput> too_deep{
                {.type_child_ordinals = {}, .physical_type = uint64},
                {.type_child_ordinals = {0, 0}, .physical_type = uint64},
            };
            static_cast<void>(BoundDeclaredTypeTree::build(std::move(too_deep), root_occurrence(), {}, depth_limit));
        });

    TypeDescriptorLimits path_work_limit;
    path_work_limit.maximum_edges = 2;
    expectDescriptorError(
        DescriptorError::Code::LimitExceeded,
        [&]
        {
            const auto array = DataTypeFactory::instance().get("Array(UInt64)");
            const auto nested_array = DataTypeFactory::instance().get("Array(Array(UInt64))");
            std::vector<BoundDeclaredTypeNodeInput> too_many_path_elements{
                {.type_child_ordinals = {}, .physical_type = nested_array},
                {.type_child_ordinals = {0}, .physical_type = array},
                {.type_child_ordinals = {0, 0}, .physical_type = uint64},
            };
            static_cast<void>(BoundDeclaredTypeTree::build(std::move(too_many_path_elements), root_occurrence(), {}, path_work_limit));
        });

    TypeDescriptorLimits occurrence_limit;
    occurrence_limit.maximum_occurrences = 1;
    expectDescriptorError(
        DescriptorError::Code::LimitExceeded,
        [&]
        {
            std::vector<BoundDeclaredTypeNodeInput> root{{.type_child_ordinals = {}, .physical_type = uint64}};
            std::vector<BoundDeclaredTypeOccurrenceInput> too_many_occurrences{
                {.type_child_ordinals = {}, .logical_descriptor = descriptor, .logical_preorder = 0},
                {.type_child_ordinals = {}, .logical_descriptor = descriptor, .logical_preorder = 1},
            };
            static_cast<void>(BoundDeclaredTypeTree::build(std::move(root), std::move(too_many_occurrences), {}, occurrence_limit));
        });

    const auto other_definition = checkedDefinition("db.Other", 41);
    const auto other_descriptor = instantiate(other_definition, uint64);
    TypeDescriptorLimits descriptor_limit;
    descriptor_limit.maximum_descriptors = 1;
    expectDescriptorError(
        DescriptorError::Code::LimitExceeded,
        [&]
        {
            const auto tuple = DataTypeFactory::instance().get("Tuple(UInt64, UInt64)");
            std::vector<BoundDeclaredTypeNodeInput> too_many_descriptors{
                {.type_child_ordinals = {}, .physical_type = tuple},
                {.type_child_ordinals = {0}, .physical_type = uint64},
                {.type_child_ordinals = {1}, .physical_type = uint64},
            };
            std::vector<BoundDeclaredTypeOccurrenceInput> occurrences{
                {.type_child_ordinals = {0}, .logical_descriptor = descriptor, .logical_preorder = 0},
                {.type_child_ordinals = {1}, .logical_descriptor = other_descriptor, .logical_preorder = 1},
            };
            static_cast<void>(BoundDeclaredTypeTree::build(std::move(too_many_descriptors), std::move(occurrences), {}, descriptor_limit));
        });

    expectDescriptorError(
        DescriptorError::Code::InvalidDefinition,
        [&]
        {
            std::vector<BoundDeclaredTypeNodeInput> root{
                {.type_child_ordinals = {}, .physical_type = uint64},
            };
            static_cast<void>(BoundDeclaredTypeTree::build(std::move(root), root_occurrence(), {nullptr}));
        });

    TypeDescriptorLimits handle_limit;
    handle_limit.maximum_descriptors = 1;
    expectDescriptorError(
        DescriptorError::Code::LimitExceeded,
        [&]
        {
            std::vector<BoundDeclaredTypeNodeInput> root{
                {.type_child_ordinals = {}, .physical_type = uint64},
            };
            static_cast<void>(
                BoundDeclaredTypeTree::build(std::move(root), root_occurrence(), {other_definition, other_definition}, handle_limit));
        });
    expectDescriptorError(
        DescriptorError::Code::LimitExceeded,
        [&]
        {
            std::vector<BoundDeclaredTypeNodeInput> root{
                {.type_child_ordinals = {}, .physical_type = uint64},
            };
            static_cast<void>(BoundDeclaredTypeTree::build(std::move(root), root_occurrence(), {other_definition}, handle_limit));
        });
}

TEST(UDTBoundDeclaredTypeTree, RetainsIndependentDefinitionsButNeverCatalogRoot)
{
    auto definition = checkedDefinition("db.Lifetime", 50);
    std::vector<Definition::Ptr> definitions{definition};
    std::shared_ptr<const TypeCatalogRoot> catalog_root(TypeCatalogBuilder::build(1, definitions));
    std::weak_ptr<const TypeCatalogRoot> weak_root = catalog_root;
    std::weak_ptr<const Definition> weak_definition = definition;

    auto found = catalog_root->findByIdentity(definition->getIdentity());
    const auto physical = DataTypeFactory::instance().get("UInt64");
    auto descriptor = instantiate(found, physical);
    std::vector<BoundDeclaredTypeNodeInput> inputs{
        {.type_child_ordinals = {}, .physical_type = physical},
    };
    std::vector<BoundDeclaredTypeOccurrenceInput> occurrences{
        {.type_child_ordinals = {}, .logical_descriptor = descriptor, .logical_preorder = 0},
    };
    auto tree = BoundDeclaredTypeTree::build(std::move(inputs), std::move(occurrences), {});

    definitions.clear();
    found.reset();
    descriptor.reset();
    definition.reset();
    catalog_root.reset();
    EXPECT_TRUE(weak_root.expired());
    EXPECT_FALSE(weak_definition.expired());
    ASSERT_EQ(tree->getDefinitionHandles().size(), 1);

    tree.reset();
    EXPECT_TRUE(weak_definition.expired());
}

}
