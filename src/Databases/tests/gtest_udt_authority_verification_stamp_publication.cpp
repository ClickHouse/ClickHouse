#include <Databases/SchemaObjectDependencyGraph.h>
#include <Databases/UDT/AtomicAuthority.h>
#include <Databases/UDT/AuthorityRoot.h>
#include <Databases/UDT/AuthorityVerificationStampPublication.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/UDT/BoundObjectTypeReferences.h>
#include <DataTypes/UDT/IAuthorityAdapter.h>
#include <DataTypes/UDT/PhysicalTypeFingerprint.h>
#include <DataTypes/UDT/Record.h>
#include <DataTypes/UDT/SidecarExpectationRecord.h>
#include <DataTypes/UDT/TemplateChecker.h>

#include <Parsers/ASTDataType.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <memory>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace DB::UDT
{
namespace
{

UUID verificationUUID(UInt64 high, UInt64 low)
{
    UUID result{};
    UUIDHelpers::getHighBytes(result) = high;
    UUIDHelpers::getLowBytes(result) = low;
    return result;
}

Digest verificationDigest(UInt8 seed)
{
    Digest result{};
    for (size_t index = 0; index < result.size(); ++index)
        result[index] = static_cast<UInt8>(seed + index);
    return result;
}

Definition::Ptr checkedParameterizedAlias(UUID database_uuid)
{
    DefinitionInput input;
    input.identity = {
        .database_uuid = database_uuid,
        .type_uuid = verificationUUID(0x8100, 1),
        .revision = 1,
    };
    input.normalized_name = "db.Box";
    input.normalized_local_name = "Box";
    input.parameters = {{.normalized_name = "T", .kind = ParameterKind::Type}};
    TemplateNode root;
    root.kind = TemplateNodeKind::TypeParameter;
    root.parameter = 0;
    input.nodes.push_back(std::move(root));
    return TemplateChecker::checkAll({std::move(input)}).front();
}

Record verificationRecord(const Definition & definition)
{
    return makeRecord(
        definition,
        {
            .canonical_definition_sql = "CREATE TYPE db.Box(T TYPE) AS T",
            .canonical_physical_template_sql = "T",
            .owner_uuid = verificationUUID(0x8100, 2),
            .owner_display_name = "owner",
            .comment = {},
            .creation_time_us_utc = 1,
        });
}

CanonicalTypeArguments singleTypeArgument(const Definition & definition, std::string_view type_name)
{
    auto type = make_intrusive<ASTDataType>();
    type->name = String(type_name);
    std::vector<CanonicalTypeArgumentValue> values;
    values.push_back(CanonicalTypeArgumentValue::type(type));
    return CanonicalTypeArguments::validate(definition.getParameters(), std::move(values));
}

struct DescriptorAndPhysicalType
{
    PersistedTypeDescriptor descriptor;
    DataTypePtr physical_type;
};

DescriptorAndPhysicalType specialization(const Definition::Ptr & definition, CanonicalTypeArguments arguments, DataTypePtr physical_type)
{
    const auto descriptor = InstantiatedTypeDescriptor::create(definition, std::move(arguments), physical_type)->getPersistedDescriptor();
    return {descriptor, std::move(physical_type)};
}

AuthorityInventorySummary verificationInventory(const Record & record, const SidecarExpectationRecord & expectation)
{
    std::vector<AuthorityInventoryLeaf> leaves{
        {
            .key = {
                .record_kind = AuthorityInventoryRecordKind::TypeDefinition,
                .object_uuid = record.identity.type_uuid,
            },
            .object_revision = record.identity.revision,
            .canonical_record_hash = computeRecordHash(record),
        },
        {
            .key = {
                .record_kind = AuthorityInventoryRecordKind::SidecarExpectation,
                .object_uuid = expectation.object.object_uuid,
            },
            .object_revision = expectation.object_schema_revision,
            .canonical_record_hash = computeSidecarExpectationRecordHash(expectation),
        },
    };
    std::sort(leaves.begin(), leaves.end(), authorityInventoryLeafLess);
    return buildAuthorityInventorySummary(leaves);
}

struct VerificationStampDecisionFixture
{
    UUID database_uuid = verificationUUID(0x8120, 1);
    AuthorityRootIdentity root{
        .database_uuid = database_uuid,
        .database_catalog_epoch = 7,
        .authority_anchor = verificationDigest(0x10),
    };
    DefinitionIdentity definition{
        .database_uuid = database_uuid,
        .type_uuid = verificationUUID(0x8121, 1),
        .revision = 3,
    };
    AuthorityObjectImageIdentity object{
        .object = {
            .kind = SchemaObjectKind::SyntheticTestObject,
            .database_uuid = database_uuid,
            .object_uuid = verificationUUID(0x8122, 1),
        },
        .object_schema_revision = 11,
        .sidecar_hash = verificationDigest(0x30),
        .physical_schema_fingerprint = verificationDigest(0x50),
    };
    AuthorityVerificationStamp::Ptr stamp;

    VerificationStampDecisionFixture()
    {
        const std::array definitions{definition};
        const VerifiedAuthorityObjectIntegrity verification{
            .database_uuid = database_uuid,
            .database_catalog_epoch = root.database_catalog_epoch,
            .authority_anchor = root.authority_anchor,
            .object = object.object,
            .object_schema_revision = object.object_schema_revision,
            .sidecar_hash = object.sidecar_hash,
            .physical_schema_fingerprint = object.physical_schema_fingerprint,
            .required_definitions_digest
            = computeVerifiedRequiredDefinitionsDigest(definitions, AuthorityVerificationStampLimits{}.maximum_required_definitions),
            .required_definition_count = 1,
            .statistics = {},
        };
        stamp = AuthorityVerificationStamp::create(verification, definitions, root);
    }

    AuthorityRootPublicationProofView transition(std::span<const AuthorityInventoryKey> touched = {}) const
    {
        return {
            .transition = {
                .previous = root,
                .next = {
                    .database_uuid = database_uuid,
                    .database_catalog_epoch = root.database_catalog_epoch + 1,
                    .authority_anchor = verificationDigest(0x70),
                },
            },
            .sorted_unique_touched_authority_keys = touched,
        };
    }
};

template <typename Callback>
void expectStampError(AuthorityVerificationStampError::Code code, Callback && callback)
{
    try
    {
        callback();
        FAIL() << "Expected AuthorityVerificationStampError";
    }
    catch (const AuthorityVerificationStampError & error)
    {
        EXPECT_EQ(error.code, code);
    }
}

TEST(UDTAuthorityVerificationStamp, ConstructionRejectsUnrootedMismatchedAndNonCanonicalEvidence)
{
    VerificationStampDecisionFixture fixture;
    const std::array definitions{fixture.definition};
    VerifiedAuthorityObjectIntegrity verification{
        .database_uuid = fixture.database_uuid,
        .database_catalog_epoch = fixture.root.database_catalog_epoch,
        .authority_anchor = fixture.root.authority_anchor,
        .object = fixture.object.object,
        .object_schema_revision = fixture.object.object_schema_revision,
        .sidecar_hash = fixture.object.sidecar_hash,
        .physical_schema_fingerprint = fixture.object.physical_schema_fingerprint,
        .required_definitions_digest
        = computeVerifiedRequiredDefinitionsDigest(definitions, AuthorityVerificationStampLimits{}.maximum_required_definitions),
        .required_definition_count = 1,
        .statistics = {},
    };

    auto invalid_root = fixture.root;
    invalid_root.authority_anchor = {};
    expectStampError(
        AuthorityVerificationStampError::Code::InvalidRootIdentity,
        [&] { static_cast<void>(AuthorityVerificationStamp::create(verification, definitions, invalid_root)); });

    verification.authority_anchor = verificationDigest(0x90);
    expectStampError(
        AuthorityVerificationStampError::Code::InvalidVerification,
        [&] { static_cast<void>(AuthorityVerificationStamp::create(verification, definitions, fixture.root)); });
    verification.authority_anchor = fixture.root.authority_anchor;

    const std::array duplicate_definitions{fixture.definition, fixture.definition};
    verification.required_definition_count = duplicate_definitions.size();
    verification.required_definitions_digest
        = computeVerifiedRequiredDefinitionsDigest(duplicate_definitions, AuthorityVerificationStampLimits{}.maximum_required_definitions);
    expectStampError(
        AuthorityVerificationStampError::Code::NonCanonicalDefinitionClosure,
        [&] { static_cast<void>(AuthorityVerificationStamp::create(verification, duplicate_definitions, fixture.root)); });

    AuthorityVerificationStampLimits invalid_limits;
    invalid_limits.maximum_required_definitions = 0;
    expectStampError(
        AuthorityVerificationStampError::Code::InvalidConfiguration,
        [&] { static_cast<void>(AuthorityVerificationStamp::create(verification, duplicate_definitions, fixture.root, invalid_limits)); });
}

TEST(UDTAuthorityVerificationStamp, ReuseDecisionReportsEveryExactImageAndClosureMismatch)
{
    VerificationStampDecisionFixture fixture;
    const std::array definitions{fixture.definition};
    auto proof = fixture.transition();
    auto decide = [&](const AuthorityObjectImageIdentity & object, std::span<const DefinitionIdentity> current_definitions)
    { return decideAuthorityVerificationStampReuse(*fixture.stamp, proof, object, current_definitions); };
    EXPECT_EQ(decide(fixture.object, definitions).status, AuthorityVerificationStampReuseStatus::Reusable);

    proof.transition.next.database_catalog_epoch = fixture.root.database_catalog_epoch;
    EXPECT_EQ(decide(fixture.object, definitions).status, AuthorityVerificationStampReuseStatus::RootTransitionUnproven);
    proof = fixture.transition();

    proof.transition.next.database_uuid = verificationUUID(0x8120, 2);
    EXPECT_EQ(decide(fixture.object, definitions).status, AuthorityVerificationStampReuseStatus::DatabaseChanged);
    proof = fixture.transition();

    auto changed_object = fixture.object;
    changed_object.object.object_uuid = verificationUUID(0x8122, 2);
    EXPECT_EQ(decide(changed_object, definitions).status, AuthorityVerificationStampReuseStatus::ObjectChanged);
    changed_object = fixture.object;
    ++changed_object.object_schema_revision;
    EXPECT_EQ(decide(changed_object, definitions).status, AuthorityVerificationStampReuseStatus::ObjectSchemaRevisionChanged);
    changed_object = fixture.object;
    changed_object.sidecar_hash = verificationDigest(0xa0);
    EXPECT_EQ(decide(changed_object, definitions).status, AuthorityVerificationStampReuseStatus::SidecarChanged);
    changed_object = fixture.object;
    changed_object.physical_schema_fingerprint = verificationDigest(0xb0);
    EXPECT_EQ(decide(changed_object, definitions).status, AuthorityVerificationStampReuseStatus::PhysicalSchemaChanged);

    const std::array changed_definitions{DefinitionIdentity{
        .database_uuid = fixture.database_uuid,
        .type_uuid = verificationUUID(0x8121, 2),
        .revision = 1,
    }};
    EXPECT_EQ(decide(fixture.object, changed_definitions).status, AuthorityVerificationStampReuseStatus::DependencyClosureUnproven);

    const AuthorityInventoryKey definition_key{
        .record_kind = AuthorityInventoryRecordKind::TypeDefinition,
        .object_uuid = fixture.definition.type_uuid,
    };
    const std::array duplicate_touches{definition_key, definition_key};
    proof = fixture.transition(duplicate_touches);
    EXPECT_EQ(decide(fixture.object, definitions).status, AuthorityVerificationStampReuseStatus::TouchedAuthorityKeysUnproven);

    const std::array definition_touch{definition_key};
    proof = fixture.transition(definition_touch);
    EXPECT_EQ(decide(fixture.object, definitions).status, AuthorityVerificationStampReuseStatus::CoveredAuthorityKeyTouched);

    const std::array expectation_touch{AuthorityInventoryKey{
        .record_kind = AuthorityInventoryRecordKind::SidecarExpectation,
        .object_uuid = fixture.object.object.object_uuid,
    }};
    proof = fixture.transition(expectation_touch);
    EXPECT_EQ(decide(fixture.object, definitions).status, AuthorityVerificationStampReuseStatus::CoveredAuthorityKeyTouched);

    const std::array unrelated_touch{AuthorityInventoryKey{
        .record_kind = AuthorityInventoryRecordKind::SidecarExpectation,
        .object_uuid = verificationUUID(0x8122, 3),
    }};
    proof = fixture.transition(unrelated_touch);
    EXPECT_EQ(decide(fixture.object, definitions).status, AuthorityVerificationStampReuseStatus::Reusable);
}

TEST(UDTAuthorityVerificationStampPublication, RebaseDoesNotZipSpecializationsWithDefinitionHandles)
{
    const UUID database_uuid = verificationUUID(0x8100, 3);
    const auto definition = checkedParameterizedAlias(database_uuid);
    const auto record = verificationRecord(*definition);
    const SchemaObjectID object{
        .kind = SchemaObjectKind::SyntheticTestObject,
        .database_uuid = database_uuid,
        .object_uuid = verificationUUID(0x8100, 4),
    };

    std::vector<DescriptorAndPhysicalType> specializations;
    specializations.push_back(specialization(definition, singleTypeArgument(*definition, "UInt64"), std::make_shared<DataTypeUInt64>()));
    specializations.push_back(specialization(definition, singleTypeArgument(*definition, "String"), std::make_shared<DataTypeString>()));
    std::sort(
        specializations.begin(),
        specializations.end(),
        [](const auto & lhs, const auto & rhs) { return lhs.descriptor.stableLess(rhs.descriptor); });

    PersistedTypeReferences references;
    references.object = object;
    references.object_schema_revision = 7;
    references.physical_schema_fingerprint = verificationDigest(0x40);
    for (auto & entry : specializations)
        references.descriptors.push_back(std::move(entry.descriptor));
    references.occurrence_paths = {
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
            .type_child_ordinals = {},
        },
    };
    references.uses = {{.path_id = 0, .descriptor_id = 0}, {.path_id = 1, .descriptor_id = 1}};

    BoundObjectPhysicalSchema physical_schema{
        .object = object,
        .object_schema_revision = references.object_schema_revision,
        .physical_schema_fingerprint = references.physical_schema_fingerprint,
        .occurrences = {},
    };
    for (size_t index = 0; index < specializations.size(); ++index)
    {
        physical_schema.occurrences.push_back({
            .path = references.occurrence_paths[index],
            .physical_type = std::move(specializations[index].physical_type),
            .runtime_owner_key = "payload_" + std::to_string(index),
            .selected_semantic_capabilities = 0,
        });
    }

    auto transient_capabilities = atomicDatabaseAuthorityCapabilities();
    transient_capabilities.mask &= ~typeAuthorityCapabilityBit(TypeAuthorityCapability::DurableAlias);
    auto authority = makeTransientAuthorityAdapter(database_uuid, transient_capabilities, std::vector<Definition::Ptr>{definition});
    const auto bound = BoundObjectTypeReferences::bind(references, std::move(physical_schema), *authority);
    ASSERT_EQ(bound->getDescriptors().size(), 2);
    ASSERT_EQ(bound->getDefinitionHandles().size(), 1);
    const auto required_definitions = collectAuthorityVerificationRequiredDefinitions(*bound, 1);
    ASSERT_EQ(required_definitions.size(), 1);
    EXPECT_EQ(required_definitions.front(), definition->getIdentity());

    const String canonical_sidecar = encodePersistedTypeReferences(references);
    const SidecarExpectationRecord expectation{
        .object = object,
        .object_schema_revision = references.object_schema_revision,
        .sidecar_hash = computePersistedTypeReferencesSidecarHash(references),
        .physical_schema_fingerprint = references.physical_schema_fingerprint,
    };
    const SchemaObjectID definition_object{
        .kind = SchemaObjectKind::TypeDefinition,
        .database_uuid = database_uuid,
        .object_uuid = definition->getIdentity().type_uuid,
    };
    const SchemaObjectDependencyEdge edge{
        .dependent = object,
        .dependency = definition_object,
        .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
    };
    const std::vector graph_nodes{object, definition_object};
    const std::vector graph_edges{edge};
    auto graph = SchemaObjectDependencyGraph::build(database_uuid, graph_nodes, graph_edges);
    const auto inventory = verificationInventory(record, expectation);
    const auto state = makeAuthorityState(
        database_uuid,
        11,
        dependent_object_authority_capability_mask,
        inventory.leaf_count,
        inventory.merkle_radix_root,
        graph->computeRoot());
    const std::vector<Definition::Ptr> definitions{definition};
    const std::vector records{record};
    const std::vector expectations{expectation};
    const std::vector dependent_objects{AuthorityDependentObjectResourceImage{
        .object = object,
        .canonical_metadata_bytes = "synthetic-metadata",
        .canonical_sidecar_bytes = canonical_sidecar,
        .canonical_installation_record_bytes = {},
    }};
    const auto root = AuthorityRootBuilder::build(
        state, 1, definitions, records, expectations, std::move(graph), AuthorityRootBuildLimits{}, dependent_objects);
    AuthorityVerificationStampLimits stamp_limits;
    stamp_limits.maximum_required_definitions = 1;
    const auto stamp = verifyAndCreateAuthorityVerificationStamp(*root, expectation, canonical_sidecar, *bound, {}, stamp_limits);

    auto mismatched_references = references;
    mismatched_references.physical_schema_fingerprint = verificationDigest(0xc0);
    const String damaged_sidecar = encodePersistedTypeReferences(mismatched_references);
    EXPECT_THROW(
        static_cast<void>(verifyAndCreateAuthorityVerificationStamp(*root, expectation, damaged_sidecar, *bound, {}, stamp_limits)),
        AuthorityIntegrityVerifierError);
    auto mismatched_expectation = expectation;
    ++mismatched_expectation.object_schema_revision;
    EXPECT_THROW(
        static_cast<void>(
            verifyAndCreateAuthorityVerificationStamp(*root, mismatched_expectation, canonical_sidecar, *bound, {}, stamp_limits)),
        AuthorityVerificationStampError);

    const auto replacement_root = root->cloneForExactRepair();
    const auto rebased = validateAndRebaseAuthorityVerificationStamp(*replacement_root, expectation, *bound, *stamp, stamp_limits);
    ASSERT_TRUE(rebased);
    EXPECT_EQ(rebased->getVerifiedObject(), stamp->getVerifiedObject());
    EXPECT_TRUE(std::ranges::equal(rebased->getRequiredDefinitions(), stamp->getRequiredDefinitions()));
    const auto & replacement_state = replacement_root->getAuthorityState();
    const AuthorityRootIdentity expected_root{
        .database_uuid = replacement_state.database_uuid,
        .database_catalog_epoch = replacement_state.database_catalog_epoch,
        .authority_anchor = replacement_state.anchor_hash,
    };
    EXPECT_EQ(rebased->getVerifiedRoot(), expected_root);
    EXPECT_NE(rebased->getVerifiedRoot(), stamp->getVerifiedRoot());
}

}
}
