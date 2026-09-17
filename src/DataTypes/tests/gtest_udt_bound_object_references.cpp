#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/UDT/BoundObjectTypeReferences.h>
#include <DataTypes/UDT/CanonicalHash.h>
#include <DataTypes/UDT/PhysicalTypeFingerprint.h>
#include <DataTypes/UDT/TemplateChecker.h>

#include <Parsers/ASTDataType.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

namespace DB::UDT
{
namespace
{

UUID uuid(UInt64 high, UInt64 low)
{
    UUID result{};
    UUIDHelpers::getHighBytes(result) = high;
    UUIDHelpers::getLowBytes(result) = low;
    return result;
}

UUID databaseUUID()
{
    return uuid(0x550e8400e29b41d4ULL, 0xa716446655440000ULL);
}

Digest digest(UInt8 seed)
{
    Digest result{};
    for (std::size_t index = 0; index < result.size(); ++index)
        result[index] = static_cast<UInt8>(seed + index);
    return result;
}

TypeAuthorityCapabilities capabilities()
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

Definition::Ptr checkedAlias()
{
    DefinitionInput input;
    input.identity = {
        .database_uuid = databaseUUID(),
        .type_uuid = uuid(0x123456789abcdef0ULL, 0x0102030405060708ULL),
        .revision = 1,
    };
    input.normalized_name = "app.Box";
    input.normalized_local_name = "Box";
    input.parameters = {{.normalized_name = "T", .kind = ParameterKind::Type}};
    TemplateNode root;
    root.kind = TemplateNodeKind::TypeParameter;
    input.nodes.push_back(std::move(root));
    input.semantic_capabilities = semanticCapabilityBit(SemanticCapability::Input) | semanticCapabilityBit(SemanticCapability::Default);
    input.policy_bearing = true;
    input.policy_semantic_hash = hashDomainSeparated("ClickHouse UDT bound-object test policy V1", input.normalized_name);
    return TemplateChecker::checkAll({std::move(input)}).front();
}

CanonicalTypeArguments uint64Arguments(const Definition & definition)
{
    auto type = make_intrusive<ASTDataType>();
    type->name = "UInt64";
    std::vector<CanonicalTypeArgumentValue> values;
    values.push_back(CanonicalTypeArgumentValue::type(type));
    return CanonicalTypeArguments::validate(definition.getParameters(), std::move(values));
}

PersistedTypeDescriptor descriptorFor(
    const Definition::Ptr & definition, const CanonicalTypeArguments & arguments, String diagnostic_name = "app.Box")
{
    const DataTypePtr physical_type = std::make_shared<DataTypeUInt64>();
    const auto fresh = InstantiatedTypeDescriptor::create(definition, arguments, physical_type)->getPersistedDescriptor();
    return PersistedTypeDescriptor::fromCanonicalPersistenceFields(
        fresh.getDefinitionIdentity(),
        fresh.getDefinitionHash(),
        fresh.getCanonicalArgumentsEncoding(),
        fresh.getCanonicalPhysicalType(),
        fresh.getInstantiationSemanticHash(),
        fresh.getStorageFingerprint(),
        fresh.getCheckerABI(),
        fresh.getCheckerChargeABI(),
        fresh.getPolicyABI(),
        fresh.getFunctionRegistryABI(),
        fresh.getPolicySemanticHash(),
        fresh.getSemanticCapabilities(),
        std::move(diagnostic_name));
}

PersistedTypeReferences references(const PersistedTypeDescriptor & descriptor)
{
    PersistedTypeReferences result;
    result.object = {
        .kind = SchemaObjectKind::SyntheticTestObject,
        .database_uuid = databaseUUID(),
        .object_uuid = uuid(0xfedcba9876543210ULL, 0x1020304050607080ULL),
    };
    result.object_schema_revision = 7;
    result.physical_schema_fingerprint = digest(0xa0);
    result.descriptors = {descriptor};
    result.occurrence_paths = {
        {
            .section = PersistedTypePathSection::SyntheticPayload,
            .object_ordinal = 0,
            .occurrence_ordinal = 0,
            .type_child_ordinals = {},
        },
        {
            .section = PersistedTypePathSection::SyntheticPayload,
            .object_ordinal = 1,
            .occurrence_ordinal = 0,
            .type_child_ordinals = {2, 3},
        },
    };
    result.uses = {{.path_id = 0, .descriptor_id = 0}, {.path_id = 1, .descriptor_id = 0}};
    return result;
}

BoundObjectPhysicalSchema
physicalSchema(const PersistedTypeReferences & sidecar, bool wrong_type = false, bool invalid_capabilities = false)
{
    BoundObjectPhysicalSchema result;
    result.object = sidecar.object;
    result.object_schema_revision = sidecar.object_schema_revision;
    result.physical_schema_fingerprint = sidecar.physical_schema_fingerprint;
    for (std::size_t index = 0; index < sidecar.occurrence_paths.size(); ++index)
    {
        result.occurrences.push_back({
            .path = sidecar.occurrence_paths[index],
            .physical_type = wrong_type ? DataTypePtr(std::make_shared<DataTypeString>()) : DataTypePtr(std::make_shared<DataTypeUInt64>()),
            .runtime_owner_key = "synthetic-payload-" + std::to_string(index),
            .selected_semantic_capabilities = invalid_capabilities ? semanticCapabilityBit(SemanticCapability::Output)
                : index == 0                                       ? semanticCapabilityBit(SemanticCapability::Input)
                                                                   : semanticCapabilityBit(SemanticCapability::Default),
        });
    }
    return result;
}

class CountingAuthority final : public IAuthorityAdapter
{
public:
    explicit CountingAuthority(std::vector<Definition::Ptr> definitions)
        : nested(makeTransientAuthorityAdapter(databaseUUID(), capabilities(), std::move(definitions)))
    {
    }

    const TypeAuthorityCapabilities & getCapabilities() const noexcept override { return nested->getCapabilities(); }
    UUID getDatabaseUUID() const noexcept override { return nested->getDatabaseUUID(); }
    ResolutionSession beginResolutionSession() const override
    {
        ++resolution_sessions;
        return nested->beginResolutionSession();
    }
    void requireCapabilities(TypeAuthorityCapabilityMask required, std::string_view operation) const override
    {
        nested->requireCapabilities(required, operation);
    }

    mutable UInt64 resolution_sessions = 0;

private:
    AuthorityAdapterPtr nested;
};

template <typename Callback>
void expectBoundError(BoundObjectTypeReferencesError::Code code, Callback && callback)
{
    try
    {
        callback();
        FAIL() << "expected a bound-object references error";
    }
    catch (const BoundObjectTypeReferencesError & error)
    {
        EXPECT_EQ(error.code, code) << error.what();
    }
}

}

TEST(UDTBoundObjectReferences, BindsEveryDistinctDescriptorOnceAgainstOneSnapshot)
{
    const auto definition = checkedAlias();
    const auto arguments = uint64Arguments(*definition);
    auto sidecar = references(descriptorFor(definition, arguments, "app.OldBox"));
    CountingAuthority authority({definition});

    const auto bound = BoundObjectTypeReferences::bind(sidecar, physicalSchema(sidecar), authority);
    ASSERT_TRUE(bound);
    EXPECT_EQ(authority.resolution_sessions, 1);
    EXPECT_EQ(bound->getObject(), sidecar.object);
    EXPECT_EQ(bound->getObjectSchemaRevision(), 7);
    EXPECT_EQ(bound->getPhysicalSchemaFingerprint(), sidecar.physical_schema_fingerprint);
    ASSERT_EQ(bound->getDescriptors().size(), 1);
    ASSERT_EQ(bound->getDefinitionHandles().size(), 1);
    ASSERT_EQ(bound->getUses().size(), 2);
    EXPECT_EQ(bound->getDescriptors().front()->getPersistedDescriptor().getLastKnownQualifiedName(), "app.Box");
    EXPECT_EQ(bound->getDefinitionHandles().front().get(), definition.get());
    EXPECT_EQ(
        bound->getSemanticCapabilities(),
        semanticCapabilityBit(SemanticCapability::Input) | semanticCapabilityBit(SemanticCapability::Default));
    ASSERT_NE(bound->findUse(sidecar.occurrence_paths[1]), nullptr);
    EXPECT_EQ(bound->findUse(sidecar.occurrence_paths[1])->getPhysicalType()->getName(), "UInt64");
    EXPECT_EQ(bound->findUse(sidecar.occurrence_paths[1])->getSemanticCapabilities(), semanticCapabilityBit(SemanticCapability::Default));

    auto absent = sidecar.occurrence_paths[1];
    absent.occurrence_ordinal = 9;
    EXPECT_EQ(bound->findUse(absent), nullptr);
}

TEST(UDTBoundObjectReferences, RejectsSchemaIdentityPathAndPhysicalTypeMismatchBeforePublication)
{
    const auto definition = checkedAlias();
    auto sidecar = references(descriptorFor(definition, uint64Arguments(*definition)));
    CountingAuthority authority({definition});

    auto wrong_object = physicalSchema(sidecar);
    wrong_object.object.object_uuid = uuid(1, 2);
    expectBoundError(
        BoundObjectTypeReferencesError::Code::ObjectMismatch,
        [&] { static_cast<void>(BoundObjectTypeReferences::bind(sidecar, std::move(wrong_object), authority)); });
    EXPECT_EQ(authority.resolution_sessions, 0);

    auto wrong_path = physicalSchema(sidecar);
    wrong_path.occurrences[1].path.type_child_ordinals.back() = 4;
    expectBoundError(
        BoundObjectTypeReferencesError::Code::PathMismatch,
        [&] { static_cast<void>(BoundObjectTypeReferences::bind(sidecar, std::move(wrong_path), authority)); });
    EXPECT_EQ(authority.resolution_sessions, 0);

    expectBoundError(
        BoundObjectTypeReferencesError::Code::PhysicalSchemaMismatch,
        [&] { static_cast<void>(BoundObjectTypeReferences::bind(sidecar, physicalSchema(sidecar, true), authority)); });
    EXPECT_EQ(authority.resolution_sessions, 1);

    expectBoundError(
        BoundObjectTypeReferencesError::Code::PhysicalSchemaMismatch,
        [&] { static_cast<void>(BoundObjectTypeReferences::bind(sidecar, physicalSchema(sidecar, false, true), authority)); });
    EXPECT_EQ(authority.resolution_sessions, 2);
}

TEST(UDTBoundObjectReferences, RejectsDescriptorDriftAndWrongCanonicalArguments)
{
    const auto definition = checkedAlias();
    const auto correct = descriptorFor(definition, uint64Arguments(*definition));
    auto sidecar = references(correct);

    const String no_arguments{"\x01\x00\x00", 3};
    const Digest wrong_instantiation_hash = computeInstantiationSemanticHash({
        .definition_identity = correct.getDefinitionIdentity(),
        .definition_hash = correct.getDefinitionHash(),
        .canonical_arguments_encoding = no_arguments,
        .canonical_physical_type = correct.getCanonicalPhysicalType(),
        .storage_fingerprint = correct.getStorageFingerprint(),
        .checker_abi = correct.getCheckerABI(),
        .checker_charge_abi = correct.getCheckerChargeABI(),
        .policy_abi = correct.getPolicyABI(),
        .function_registry_abi = correct.getFunctionRegistryABI(),
        .policy_semantic_hash = correct.getPolicySemanticHash(),
        .semantic_capabilities = correct.getSemanticCapabilities(),
    });
    sidecar.descriptors.front() = PersistedTypeDescriptor::fromCanonicalPersistenceFields(
        correct.getDefinitionIdentity(),
        correct.getDefinitionHash(),
        no_arguments,
        correct.getCanonicalPhysicalType(),
        wrong_instantiation_hash,
        correct.getStorageFingerprint(),
        correct.getCheckerABI(),
        correct.getCheckerChargeABI(),
        correct.getPolicyABI(),
        correct.getFunctionRegistryABI(),
        correct.getPolicySemanticHash(),
        correct.getSemanticCapabilities(),
        correct.getLastKnownQualifiedName());

    CountingAuthority authority({definition});
    expectBoundError(
        BoundObjectTypeReferencesError::Code::DescriptorMismatch,
        [&] { static_cast<void>(BoundObjectTypeReferences::bind(sidecar, physicalSchema(sidecar), authority)); });
    EXPECT_EQ(authority.resolution_sessions, 1);
}

TEST(UDTBoundObjectReferences, ProspectivePathLimitRejectsBeforeAuthoritySession)
{
    const auto definition = checkedAlias();
    auto sidecar = references(descriptorFor(definition, uint64Arguments(*definition)));
    CountingAuthority authority({definition});
    BoundObjectTypeReferencesLimits limits;
    limits.maximum_retained_path_components = 1;

    expectBoundError(
        BoundObjectTypeReferencesError::Code::LimitExceeded,
        [&] { static_cast<void>(BoundObjectTypeReferences::bind(sidecar, physicalSchema(sidecar), authority, limits)); });
    EXPECT_EQ(authority.resolution_sessions, 0);
}

TEST(UDTBoundObjectReferences, InvalidPersistedLimitProfilesRejectBeforeAuthoritySession)
{
    const auto definition = checkedAlias();
    auto sidecar = references(descriptorFor(definition, uint64Arguments(*definition)));
    CountingAuthority authority({definition});

    BoundObjectTypeReferencesLimits zero_limit;
    zero_limit.persisted.maximum_sidecar_bytes = 0;
    expectBoundError(
        BoundObjectTypeReferencesError::Code::InvalidConfiguration,
        [&] { static_cast<void>(BoundObjectTypeReferences::bind(sidecar, physicalSchema(sidecar), authority, zero_limit)); });
    EXPECT_EQ(authority.resolution_sessions, 0);

    BoundObjectTypeReferencesLimits excessive_limit;
    excessive_limit.persisted.maximum_sidecar_bytes = (16ULL << 20) + 1;
    expectBoundError(
        BoundObjectTypeReferencesError::Code::InvalidConfiguration,
        [&] { static_cast<void>(BoundObjectTypeReferences::bind(sidecar, physicalSchema(sidecar), authority, excessive_limit)); });
    EXPECT_EQ(authority.resolution_sessions, 0);
}

TEST(UDTBoundObjectReferences, InvalidTypeArgumentLimitProfilesRejectBeforeAuthoritySession)
{
    const auto definition = checkedAlias();
    auto sidecar = references(descriptorFor(definition, uint64Arguments(*definition)));
    CountingAuthority authority({definition});

    BoundObjectTypeReferencesLimits zero_limit;
    zero_limit.type_arguments.maximum_ast_nodes = 0;
    expectBoundError(
        BoundObjectTypeReferencesError::Code::InvalidConfiguration,
        [&] { static_cast<void>(BoundObjectTypeReferences::bind(sidecar, physicalSchema(sidecar), authority, zero_limit)); });
    EXPECT_EQ(authority.resolution_sessions, 0);

    BoundObjectTypeReferencesLimits excessive_limit;
    excessive_limit.type_arguments.maximum_ast_nodes = (1ULL << 20) + 1;
    expectBoundError(
        BoundObjectTypeReferencesError::Code::InvalidConfiguration,
        [&] { static_cast<void>(BoundObjectTypeReferences::bind(sidecar, physicalSchema(sidecar), authority, excessive_limit)); });
    EXPECT_EQ(authority.resolution_sessions, 0);
}

}
