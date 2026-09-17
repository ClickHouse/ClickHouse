#include <Databases/DatabaseSchemaWAL.h>
#include <Databases/UDT/AtomicAuthority.h>
#include <Databases/UDT/AuthorityRepairAudit.h>
#include <Databases/UDT/AuthorityRepairPlan.h>
#include <Databases/tests/UDTTestResourceImages.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/UDT/DependentObjectMetadataInstallationRecord.h>
#include <DataTypes/UDT/TemplateChecker.h>

#include <IO/WriteHelpers.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace DB::UDT
{
namespace
{

UUID repairUUID(UInt64 high, UInt64 low)
{
    UUID result{};
    UUIDHelpers::getHighBytes(result) = high;
    UUIDHelpers::getLowBytes(result) = low;
    return result;
}

String digestHex(const Digest & value)
{
    static constexpr char digits[] = "0123456789abcdef";
    String result;
    result.reserve(value.size() * 2);
    for (const UInt8 byte : value)
    {
        result.push_back(digits[byte >> 4]);
        result.push_back(digits[byte & 0x0f]);
    }
    return result;
}

struct RepairFixture
{
    UUID database_uuid = repairUUID(0x8300, 1);
    UUID type_uuid = repairUUID(0x8301, 1);
    Definition::Ptr definition;
    Record record;
    String canonical_record;
    AuthorityInventoryKey key;
    SchemaObjectID object;
    std::unique_ptr<AtomicAuthority> authority;

    RepairFixture()
    {
        DefinitionInput input;
        input.identity = {.database_uuid = database_uuid, .type_uuid = type_uuid, .revision = 1};
        input.normalized_name = "authority_repair.Value";
        input.normalized_local_name = "Value";
        TemplateNode node;
        node.kind = TemplateNodeKind::BuiltIn;
        node.atom = "UInt64";
        input.nodes.push_back(std::move(node));
        definition = TemplateChecker::checkAll({std::move(input)}).front();
        record = makeRecord(
            *definition,
            {
                .canonical_definition_sql = "ATTACH TYPE authority_repair.Value UUID '" + toString(type_uuid)
                    + "' REVISION 1 AS UInt64 DEFINITION HASH '" + digestHex(definition->getDefinitionHash()) + "'",
                .canonical_physical_template_sql = "UInt64",
                .owner_uuid = repairUUID(0x8302, 1),
                .owner_display_name = "owner",
                .comment = "authority repair",
                .creation_time_us_utc = 1,
            });
        canonical_record = encodeRecord(record);
        key = {
            .record_kind = AuthorityInventoryRecordKind::TypeDefinition,
            .object_uuid = type_uuid,
        };
        object = {
            .kind = SchemaObjectKind::TypeDefinition,
            .database_uuid = database_uuid,
            .object_uuid = type_uuid,
        };
        const AuthorityInventoryLeaf leaf{
            .key = key,
            .object_revision = 1,
            .canonical_record_hash = computeRecordHash(record),
        };
        const auto inventory = buildAuthorityInventorySummary({&leaf, 1});
        auto graph = SchemaObjectDependencyGraph::build(database_uuid, {&object, 1}, {});
        const auto state = makeAuthorityState(
            database_uuid,
            1,
            definition_authority_capability_mask,
            inventory.leaf_count,
            inventory.merkle_radix_root,
            graph->computeRoot());
        const std::array definitions{definition};
        const std::array records{record};
        const std::vector<SidecarExpectationRecord> expectations;
        auto root = AuthorityRootBuilder::buildInitialAdmission(state, 1, definitions, records, expectations, std::move(graph));
        authority = std::make_unique<AtomicAuthority>(database_uuid, atomicDatabaseAuthorityCapabilities(), std::move(root));
    }

    AuthorityRepairAudit::Ptr missingDefinitionAudit()
    {
        const AuthorityRepairObservation observation{
            .artifact_kind = AuthorityRepairAuditArtifactKind::DefinitionRecord,
            .authority_key = key,
            .state = AuthorityRepairObservationState::Missing,
            .artifact_bytes = {},
            .object = {},
        };
        return AuthorityRepairAudit::build(authority->acquireCurrentRoot(), {&observation, 1});
    }

    AuthorityRepairCandidate candidate(const AuthorityRepairTarget & target, AuthorityRepairSource source, std::string_view bytes) const
    {
        return {
            .source = source,
            .artifact_kind = target.artifact_kind,
            .object = target.object,
            .authority_key = target.authority_key,
            .object_revision = target.object_revision,
            .physical_schema_fingerprint = target.physical_schema_fingerprint,
            .canonical_bytes = bytes,
            .source_reference = source == AuthorityRepairSource::LocalSchemaWAL ? "wal:1"
                : source == AuthorityRepairSource::ReplicatedAuthority          ? "replica:1"
                                                                                : "backup:1",
        };
    }
};

Digest repairDigest(UInt8 seed)
{
    Digest result{};
    for (size_t index = 0; index < result.size(); ++index)
        result[index] = static_cast<UInt8>(seed + index);
    return result;
}

struct CompleteRepairAuditFixture
{
    UUID database_uuid = repairUUID(0x8310, 1);
    Definition::Ptr definition;
    Record record;
    String canonical_record;
    SchemaObjectID definition_object;
    SchemaObjectID dependent_object;
    PersistedTypeReferences references;
    SidecarExpectationRecord expectation;
    DependentObjectMetadataInstallationRecord installation;
    String canonical_expectation;
    String canonical_sidecar;
    String canonical_installation;
    String canonical_metadata = "ATTACH TABLE authority_repair.events (value UInt64)";
    AuthorityInventoryKey definition_key;
    AuthorityInventoryKey expectation_key;
    std::unique_ptr<AtomicAuthority> authority;

    CompleteRepairAuditFixture()
    {
        DefinitionInput input;
        input.identity = {.database_uuid = database_uuid, .type_uuid = repairUUID(0x8311, 1), .revision = 1};
        input.normalized_name = "authority_repair.Value";
        input.normalized_local_name = "Value";
        TemplateNode node;
        node.kind = TemplateNodeKind::BuiltIn;
        node.atom = "UInt64";
        input.nodes.push_back(std::move(node));
        definition = TemplateChecker::checkAll({std::move(input)}).front();
        record = makeRecord(
            *definition,
            {
                .canonical_definition_sql = "CREATE TYPE authority_repair.Value AS UInt64",
                .canonical_physical_template_sql = "UInt64",
                .owner_uuid = repairUUID(0x8312, 1),
                .owner_display_name = "owner",
                .comment = {},
                .creation_time_us_utc = 1,
            });
        canonical_record = encodeRecord(record);
        definition_object = {
            .kind = SchemaObjectKind::TypeDefinition,
            .database_uuid = database_uuid,
            .object_uuid = definition->getIdentity().type_uuid,
        };
        dependent_object = {
            .kind = SchemaObjectKind::Table,
            .database_uuid = database_uuid,
            .object_uuid = repairUUID(0x8313, 1),
        };
        references = Test::singleDefinitionPersistedTypeReferences(
            dependent_object, 7, repairDigest(0x40), definition, std::make_shared<DataTypeUInt64>(), PersistedTypePathSection::ColumnType);
        installation = {
            .object = dependent_object,
            .object_schema_revision = references.object_schema_revision,
            .object_name = "events",
            .metadata_artifact_hash
            = computeDatabaseSchemaWALStagedArtifactHash(DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, canonical_metadata),
        };
        expectation = Test::sidecarExpectationFor(references);
        expectation.installation_record_hash = computeDependentObjectMetadataInstallationRecordHash(installation);
        canonical_expectation = encodeSidecarExpectationRecord(expectation);
        canonical_sidecar = encodePersistedTypeReferences(references);
        canonical_installation = encodeDependentObjectMetadataInstallationRecord(installation);
        definition_key = {
            .record_kind = AuthorityInventoryRecordKind::TypeDefinition,
            .object_uuid = definition->getIdentity().type_uuid,
        };
        expectation_key = {
            .record_kind = AuthorityInventoryRecordKind::SidecarExpectation,
            .object_uuid = dependent_object.object_uuid,
        };
        std::vector<AuthorityInventoryLeaf> leaves{
            {
                .key = definition_key,
                .object_revision = definition->getIdentity().revision,
                .canonical_record_hash = computeRecordHash(record),
            },
            {
                .key = expectation_key,
                .object_revision = expectation.object_schema_revision,
                .canonical_record_hash = computeSidecarExpectationRecordHash(expectation),
            },
        };
        std::sort(leaves.begin(), leaves.end(), authorityInventoryLeafLess);
        const auto inventory = buildAuthorityInventorySummary(leaves);
        const std::array graph_nodes{definition_object, dependent_object};
        const std::array graph_edges{SchemaObjectDependencyEdge{
            .dependent = dependent_object,
            .dependency = definition_object,
            .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
        }};
        auto graph = SchemaObjectDependencyGraph::build(database_uuid, graph_nodes, graph_edges);
        const auto state = makeAuthorityState(
            database_uuid,
            2,
            dependent_object_authority_capability_mask,
            inventory.leaf_count,
            inventory.merkle_radix_root,
            graph->computeRoot());
        const std::array definitions{definition};
        const std::array records{record};
        const std::array expectations{expectation};
        std::vector<Test::DependentObjectResourceImageInput> image_inputs{
            {
                .canonical_metadata_bytes = canonical_metadata,
                .references = references,
                .canonical_installation_record_bytes = canonical_installation,
            },
        };
        Test::DependentObjectResourceImageBatch images(expectations, std::move(image_inputs));
        auto root = AuthorityRootBuilder::buildInitialAdmission(
            state, 1, definitions, records, expectations, std::move(graph), AuthorityRootBuildLimits{}, images.get());
        authority = std::make_unique<AtomicAuthority>(database_uuid, atomicDatabaseAuthorityCapabilities(), std::move(root));
    }

    std::vector<AuthorityRepairObservation> cleanObservations() const
    {
        return {
            {
                .artifact_kind = AuthorityRepairAuditArtifactKind::DefinitionRecord,
                .authority_key = definition_key,
                .state = AuthorityRepairObservationState::Present,
                .artifact_bytes = canonical_record,
                .object = {},
                .object_schema_revision = 0,
                .physical_schema_fingerprint = {},
            },
            {
                .artifact_kind = AuthorityRepairAuditArtifactKind::SidecarExpectationRecord,
                .authority_key = expectation_key,
                .state = AuthorityRepairObservationState::Present,
                .artifact_bytes = canonical_expectation,
                .object = {},
                .object_schema_revision = 0,
                .physical_schema_fingerprint = {},
            },
            {
                .artifact_kind = AuthorityRepairAuditArtifactKind::PersistedTypeReferencesSidecar,
                .authority_key = expectation_key,
                .state = AuthorityRepairObservationState::Present,
                .artifact_bytes = canonical_sidecar,
                .object = {},
                .object_schema_revision = 0,
                .physical_schema_fingerprint = {},
            },
            {
                .artifact_kind = AuthorityRepairAuditArtifactKind::DependentObjectMetadataInstallationRecord,
                .authority_key = expectation_key,
                .state = AuthorityRepairObservationState::Present,
                .artifact_bytes = canonical_installation,
                .object = {},
                .object_schema_revision = 0,
                .physical_schema_fingerprint = {},
            },
            {
                .artifact_kind = AuthorityRepairAuditArtifactKind::DependentObjectMetadata,
                .authority_key = expectation_key,
                .state = AuthorityRepairObservationState::Present,
                .artifact_bytes = canonical_metadata,
                .object = {},
                .object_schema_revision = 0,
                .physical_schema_fingerprint = {},
            },
            {
                .artifact_kind = AuthorityRepairAuditArtifactKind::StoredObjectImage,
                .authority_key = expectation_key,
                .state = AuthorityRepairObservationState::Present,
                .artifact_bytes = {},
                .object = dependent_object,
                .object_schema_revision = expectation.object_schema_revision,
                .physical_schema_fingerprint = expectation.physical_schema_fingerprint,
            },
        };
    }
};

template <typename Callback>
void expectPlanError(AuthorityRepairPlanError::Code code, Callback && callback)
{
    try
    {
        callback();
        FAIL() << "Expected AuthorityRepairPlanError";
    }
    catch (const AuthorityRepairPlanError & error)
    {
        EXPECT_EQ(error.code, code);
    }
}

template <typename Callback>
void expectAuditError(AuthorityRepairAuditError::Code code, Callback && callback)
{
    try
    {
        callback();
        FAIL() << "Expected AuthorityRepairAuditError";
    }
    catch (const AuthorityRepairAuditError & error)
    {
        EXPECT_EQ(error.code, code);
    }
}

enum class RepairAuditMismatch : UInt8
{
    Identity,
    Revision,
    PhysicalSchema,
    CanonicalHash,
    Malformed,
    AuthenticationUnavailable,
};

struct RepairAuditCase
{
    std::string_view name;
    AuthorityRepairAuditArtifactKind artifact_kind;
    RepairAuditMismatch mismatch;
    AuthorityRepairFindingKind expected_finding;
    size_t expected_finding_count;
    bool repairable;
};

constexpr std::array repair_audit_cases{
    RepairAuditCase{
        "definition identity",
        AuthorityRepairAuditArtifactKind::DefinitionRecord,
        RepairAuditMismatch::Identity,
        AuthorityRepairFindingKind::IdentityMismatch,
        1,
        true},
    RepairAuditCase{
        "definition revision",
        AuthorityRepairAuditArtifactKind::DefinitionRecord,
        RepairAuditMismatch::Revision,
        AuthorityRepairFindingKind::RevisionMismatch,
        1,
        true},
    RepairAuditCase{
        "definition hash",
        AuthorityRepairAuditArtifactKind::DefinitionRecord,
        RepairAuditMismatch::CanonicalHash,
        AuthorityRepairFindingKind::CanonicalHashMismatch,
        1,
        true},
    RepairAuditCase{
        "definition malformed",
        AuthorityRepairAuditArtifactKind::DefinitionRecord,
        RepairAuditMismatch::Malformed,
        AuthorityRepairFindingKind::Malformed,
        1,
        true},
    RepairAuditCase{
        "expectation identity",
        AuthorityRepairAuditArtifactKind::SidecarExpectationRecord,
        RepairAuditMismatch::Identity,
        AuthorityRepairFindingKind::IdentityMismatch,
        1,
        true},
    RepairAuditCase{
        "expectation revision",
        AuthorityRepairAuditArtifactKind::SidecarExpectationRecord,
        RepairAuditMismatch::Revision,
        AuthorityRepairFindingKind::RevisionMismatch,
        1,
        true},
    RepairAuditCase{
        "expectation physical schema",
        AuthorityRepairAuditArtifactKind::SidecarExpectationRecord,
        RepairAuditMismatch::PhysicalSchema,
        AuthorityRepairFindingKind::PhysicalSchemaMismatch,
        1,
        true},
    RepairAuditCase{
        "expectation hash",
        AuthorityRepairAuditArtifactKind::SidecarExpectationRecord,
        RepairAuditMismatch::CanonicalHash,
        AuthorityRepairFindingKind::CanonicalHashMismatch,
        1,
        true},
    RepairAuditCase{
        "expectation malformed",
        AuthorityRepairAuditArtifactKind::SidecarExpectationRecord,
        RepairAuditMismatch::Malformed,
        AuthorityRepairFindingKind::Malformed,
        1,
        true},
    RepairAuditCase{
        "sidecar identity",
        AuthorityRepairAuditArtifactKind::PersistedTypeReferencesSidecar,
        RepairAuditMismatch::Identity,
        AuthorityRepairFindingKind::IdentityMismatch,
        1,
        true},
    RepairAuditCase{
        "sidecar revision",
        AuthorityRepairAuditArtifactKind::PersistedTypeReferencesSidecar,
        RepairAuditMismatch::Revision,
        AuthorityRepairFindingKind::RevisionMismatch,
        1,
        true},
    RepairAuditCase{
        "sidecar physical schema",
        AuthorityRepairAuditArtifactKind::PersistedTypeReferencesSidecar,
        RepairAuditMismatch::PhysicalSchema,
        AuthorityRepairFindingKind::PhysicalSchemaMismatch,
        1,
        true},
    RepairAuditCase{
        "sidecar hash",
        AuthorityRepairAuditArtifactKind::PersistedTypeReferencesSidecar,
        RepairAuditMismatch::CanonicalHash,
        AuthorityRepairFindingKind::CanonicalHashMismatch,
        1,
        true},
    RepairAuditCase{
        "sidecar malformed",
        AuthorityRepairAuditArtifactKind::PersistedTypeReferencesSidecar,
        RepairAuditMismatch::Malformed,
        AuthorityRepairFindingKind::Malformed,
        1,
        true},
    RepairAuditCase{
        "installation identity",
        AuthorityRepairAuditArtifactKind::DependentObjectMetadataInstallationRecord,
        RepairAuditMismatch::Identity,
        AuthorityRepairFindingKind::IdentityMismatch,
        2,
        false},
    RepairAuditCase{
        "installation revision",
        AuthorityRepairAuditArtifactKind::DependentObjectMetadataInstallationRecord,
        RepairAuditMismatch::Revision,
        AuthorityRepairFindingKind::RevisionMismatch,
        2,
        false},
    RepairAuditCase{
        "installation hash",
        AuthorityRepairAuditArtifactKind::DependentObjectMetadataInstallationRecord,
        RepairAuditMismatch::CanonicalHash,
        AuthorityRepairFindingKind::CanonicalHashMismatch,
        2,
        false},
    RepairAuditCase{
        "installation malformed",
        AuthorityRepairAuditArtifactKind::DependentObjectMetadataInstallationRecord,
        RepairAuditMismatch::Malformed,
        AuthorityRepairFindingKind::Malformed,
        2,
        false},
    RepairAuditCase{
        "metadata authentication unavailable",
        AuthorityRepairAuditArtifactKind::DependentObjectMetadata,
        RepairAuditMismatch::AuthenticationUnavailable,
        AuthorityRepairFindingKind::AuthenticationUnavailable,
        2,
        false},
    RepairAuditCase{
        "metadata malformed",
        AuthorityRepairAuditArtifactKind::DependentObjectMetadata,
        RepairAuditMismatch::Malformed,
        AuthorityRepairFindingKind::Malformed,
        1,
        false},
    RepairAuditCase{
        "metadata hash",
        AuthorityRepairAuditArtifactKind::DependentObjectMetadata,
        RepairAuditMismatch::CanonicalHash,
        AuthorityRepairFindingKind::CanonicalHashMismatch,
        1,
        false},
    RepairAuditCase{
        "stored image identity",
        AuthorityRepairAuditArtifactKind::StoredObjectImage,
        RepairAuditMismatch::Identity,
        AuthorityRepairFindingKind::IdentityMismatch,
        1,
        false},
    RepairAuditCase{
        "stored image revision",
        AuthorityRepairAuditArtifactKind::StoredObjectImage,
        RepairAuditMismatch::Revision,
        AuthorityRepairFindingKind::RevisionMismatch,
        1,
        false},
    RepairAuditCase{
        "stored image physical schema",
        AuthorityRepairAuditArtifactKind::StoredObjectImage,
        RepairAuditMismatch::PhysicalSchema,
        AuthorityRepairFindingKind::PhysicalSchemaMismatch,
        1,
        false},
};

TEST(UDTAuthorityRepair, CompleteMissingDefinitionAuditAnchorsTargetAndQuarantine)
{
    RepairFixture fixture;
    auto audit = fixture.missingDefinitionAudit();
    ASSERT_TRUE(audit->hasDamage());
    ASSERT_TRUE(audit->hasCompleteRepairTargetSet());
    ASSERT_EQ(audit->getFindings().size(), 1);
    ASSERT_EQ(audit->getCompleteRepairTargets().size(), 1);
    ASSERT_NE(audit->getQuarantinePlan(), nullptr);
    EXPECT_TRUE(audit->getQuarantinePlan()->contains(fixture.object));
    EXPECT_EQ(audit->getCompleteRepairTargets().front().expected_canonical_hash, computeRecordHash(fixture.record));
    EXPECT_TRUE(AuthorityRepairAudit::requiresFullClosureReverificationBeforeRelease());
}

TEST(UDTAuthorityRepairAudit, CleanAuditConsumesAllSixRootDerivedArtifactKinds)
{
    CompleteRepairAuditFixture fixture;
    const auto observations = fixture.cleanObservations();
    auto audit = AuthorityRepairAudit::build(fixture.authority->acquireCurrentRoot(), observations);
    ASSERT_NE(audit, nullptr);
    EXPECT_FALSE(audit->hasDamage());
    EXPECT_FALSE(audit->hasCompleteRepairTargetSet());
    EXPECT_TRUE(audit->getFindings().empty());
    EXPECT_EQ(audit->getQuarantinePlan(), nullptr);
    EXPECT_EQ(audit->getStatistics().inventory_leaves, 2);
    EXPECT_EQ(audit->getStatistics().expected_artifacts, 6);
    EXPECT_EQ(audit->getStatistics().observed_artifacts, 6);
    EXPECT_EQ(audit->getStatistics().clean_artifacts, 6);
}

TEST(UDTAuthorityRepairAudit, MissingObservationForEveryArtifactKindProducesAnExactFinding)
{
    constexpr std::array artifact_kinds{
        AuthorityRepairAuditArtifactKind::DefinitionRecord,
        AuthorityRepairAuditArtifactKind::SidecarExpectationRecord,
        AuthorityRepairAuditArtifactKind::PersistedTypeReferencesSidecar,
        AuthorityRepairAuditArtifactKind::DependentObjectMetadataInstallationRecord,
        AuthorityRepairAuditArtifactKind::DependentObjectMetadata,
        AuthorityRepairAuditArtifactKind::StoredObjectImage,
    };
    for (size_t missing_index = 0; missing_index < artifact_kinds.size(); ++missing_index)
    {
        CompleteRepairAuditFixture fixture;
        auto observations = fixture.cleanObservations();
        auto & missing = observations[missing_index];
        missing.state = AuthorityRepairObservationState::Missing;
        missing.artifact_bytes = {};
        missing.object = {};
        missing.object_schema_revision = 0;
        missing.physical_schema_fingerprint = {};

        auto audit = AuthorityRepairAudit::build(fixture.authority->acquireCurrentRoot(), observations);
        ASSERT_TRUE(audit->hasDamage()) << missing_index;
        const auto finding = std::ranges::find(audit->getFindings(), artifact_kinds[missing_index], &AuthorityRepairFinding::artifact_kind);
        ASSERT_NE(finding, audit->getFindings().end()) << missing_index;
        EXPECT_EQ(finding->finding_kind, AuthorityRepairFindingKind::Missing);
        EXPECT_EQ(finding->observed_state, AuthorityRepairObservationState::Missing);
        const bool repairable = missing_index <= 2;
        EXPECT_EQ(finding->repair_target.has_value(), repairable);
        ASSERT_NE(audit->getQuarantinePlan(), nullptr);
        EXPECT_TRUE(audit->getQuarantinePlan()->contains(missing_index == 0 ? fixture.definition_object : fixture.dependent_object));
    }
}

TEST(UDTAuthorityRepairAudit, RejectsPartialExtraAndMisorderedObservationBatches)
{
    CompleteRepairAuditFixture fixture;
    auto observations = fixture.cleanObservations();

    auto partial = observations;
    partial.pop_back();
    expectAuditError(
        AuthorityRepairAuditError::Code::NonCanonicalObservationSet,
        [&] { static_cast<void>(AuthorityRepairAudit::build(fixture.authority->acquireCurrentRoot(), partial)); });

    auto extra = observations;
    extra.push_back(extra.back());
    expectAuditError(
        AuthorityRepairAuditError::Code::NonCanonicalObservationSet,
        [&] { static_cast<void>(AuthorityRepairAudit::build(fixture.authority->acquireCurrentRoot(), extra)); });

    std::swap(observations[1], observations[2]);
    expectAuditError(
        AuthorityRepairAuditError::Code::NonCanonicalObservationSet,
        [&] { static_cast<void>(AuthorityRepairAudit::build(fixture.authority->acquireCurrentRoot(), observations)); });
}

TEST(UDTAuthorityRepairAudit, RepairFindingMatrixCoversEveryApplicableArtifactMismatch)
{
    for (const auto & test_case : repair_audit_cases)
    {
        SCOPED_TRACE(test_case.name);
        CompleteRepairAuditFixture fixture;
        auto observations = fixture.cleanObservations();
        String mutated_bytes;

        switch (test_case.artifact_kind)
        {
            case AuthorityRepairAuditArtifactKind::DefinitionRecord: {
                if (test_case.mismatch == RepairAuditMismatch::Malformed)
                    mutated_bytes = "malformed definition record";
                else
                {
                    auto observed = fixture.record;
                    if (test_case.mismatch == RepairAuditMismatch::Identity)
                        observed.identity.type_uuid = repairUUID(0x8390, 1);
                    else if (test_case.mismatch == RepairAuditMismatch::Revision)
                        ++observed.identity.revision;
                    else if (test_case.mismatch == RepairAuditMismatch::CanonicalHash)
                        observed.comment = "different definition metadata";
                    else
                        FAIL() << "Unsupported definition-record mismatch";
                    mutated_bytes = encodeRecord(observed);
                }
                observations[0].artifact_bytes = mutated_bytes;
                break;
            }
            case AuthorityRepairAuditArtifactKind::SidecarExpectationRecord: {
                if (test_case.mismatch == RepairAuditMismatch::Malformed)
                    mutated_bytes = "malformed sidecar expectation";
                else
                {
                    auto observed = fixture.expectation;
                    if (test_case.mismatch == RepairAuditMismatch::Identity)
                        observed.object.object_uuid = repairUUID(0x8390, 2);
                    else if (test_case.mismatch == RepairAuditMismatch::Revision)
                        ++observed.object_schema_revision;
                    else if (test_case.mismatch == RepairAuditMismatch::PhysicalSchema)
                        observed.physical_schema_fingerprint = repairDigest(0x90);
                    else if (test_case.mismatch == RepairAuditMismatch::CanonicalHash)
                        observed.sidecar_hash = repairDigest(0xa0);
                    else
                        FAIL() << "Unsupported sidecar-expectation mismatch";
                    mutated_bytes = encodeSidecarExpectationRecord(observed);
                }
                observations[1].artifact_bytes = mutated_bytes;
                break;
            }
            case AuthorityRepairAuditArtifactKind::PersistedTypeReferencesSidecar: {
                if (test_case.mismatch == RepairAuditMismatch::Malformed)
                    mutated_bytes = "malformed persisted sidecar";
                else
                {
                    auto observed = fixture.references;
                    if (test_case.mismatch == RepairAuditMismatch::Identity)
                        observed.object.object_uuid = repairUUID(0x8390, 3);
                    else if (test_case.mismatch == RepairAuditMismatch::Revision)
                        ++observed.object_schema_revision;
                    else if (test_case.mismatch == RepairAuditMismatch::PhysicalSchema)
                        observed.physical_schema_fingerprint = repairDigest(0xb0);
                    else if (test_case.mismatch == RepairAuditMismatch::CanonicalHash)
                    {
                        ASSERT_FALSE(observed.occurrence_paths.empty());
                        ++observed.occurrence_paths.front().occurrence_ordinal;
                    }
                    else
                        FAIL() << "Unsupported persisted-sidecar mismatch";
                    mutated_bytes = encodePersistedTypeReferences(observed);
                }
                observations[2].artifact_bytes = mutated_bytes;
                break;
            }
            case AuthorityRepairAuditArtifactKind::DependentObjectMetadataInstallationRecord: {
                auto observed = fixture.installation;
                if (test_case.mismatch == RepairAuditMismatch::Identity)
                    observed.object.object_uuid = repairUUID(0x8390, 4);
                else if (test_case.mismatch == RepairAuditMismatch::Revision)
                    ++observed.object_schema_revision;
                else if (test_case.mismatch == RepairAuditMismatch::CanonicalHash)
                    observed.object_name = "events_changed";
                else if (test_case.mismatch == RepairAuditMismatch::Malformed)
                    observed.metadata_artifact_hash = {};
                else
                    FAIL() << "Unsupported installation-record mismatch";
                mutated_bytes = encodeDependentObjectMetadataInstallationRecord(observed);
                observations[3].artifact_bytes = mutated_bytes;
                break;
            }
            case AuthorityRepairAuditArtifactKind::DependentObjectMetadata: {
                if (test_case.mismatch == RepairAuditMismatch::AuthenticationUnavailable)
                {
                    observations[3].state = AuthorityRepairObservationState::Missing;
                    observations[3].artifact_bytes = {};
                }
                else if (test_case.mismatch == RepairAuditMismatch::Malformed)
                {
                    mutated_bytes.clear();
                    observations[4].artifact_bytes = mutated_bytes;
                }
                else if (test_case.mismatch == RepairAuditMismatch::CanonicalHash)
                {
                    mutated_bytes = "different canonical metadata";
                    observations[4].artifact_bytes = mutated_bytes;
                }
                else
                    FAIL() << "Unsupported metadata mismatch";
                break;
            }
            case AuthorityRepairAuditArtifactKind::StoredObjectImage: {
                if (test_case.mismatch == RepairAuditMismatch::Identity)
                    observations[5].object.object_uuid = repairUUID(0x8390, 5);
                else if (test_case.mismatch == RepairAuditMismatch::Revision)
                    ++observations[5].object_schema_revision;
                else if (test_case.mismatch == RepairAuditMismatch::PhysicalSchema)
                    observations[5].physical_schema_fingerprint = repairDigest(0xc0);
                else
                    FAIL() << "Unsupported stored-object mismatch";
                break;
            }
        }

        auto audit = AuthorityRepairAudit::build(fixture.authority->acquireCurrentRoot(), observations);
        ASSERT_EQ(audit->getFindings().size(), test_case.expected_finding_count);
        const auto finding = std::ranges::find(audit->getFindings(), test_case.artifact_kind, &AuthorityRepairFinding::artifact_kind);
        ASSERT_NE(finding, audit->getFindings().end());
        EXPECT_EQ(finding->finding_kind, test_case.expected_finding);
        EXPECT_EQ(finding->observed_state, AuthorityRepairObservationState::Present);
        EXPECT_EQ(finding->repair_target.has_value(), test_case.repairable);
        EXPECT_EQ(audit->hasCompleteRepairTargetSet(), test_case.repairable);
        ASSERT_NE(audit->getQuarantinePlan(), nullptr);
        EXPECT_TRUE(audit->getQuarantinePlan()->contains(
            test_case.artifact_kind == AuthorityRepairAuditArtifactKind::DefinitionRecord ? fixture.definition_object
                                                                                          : fixture.dependent_object));

        if (test_case.artifact_kind == AuthorityRepairAuditArtifactKind::DependentObjectMetadataInstallationRecord)
        {
            const auto metadata_finding = std::ranges::find(
                audit->getFindings(), AuthorityRepairAuditArtifactKind::DependentObjectMetadata, &AuthorityRepairFinding::artifact_kind);
            ASSERT_NE(metadata_finding, audit->getFindings().end());
            EXPECT_EQ(metadata_finding->finding_kind, AuthorityRepairFindingKind::AuthenticationUnavailable);
        }
        if (test_case.mismatch == RepairAuditMismatch::AuthenticationUnavailable)
        {
            const auto installation_finding = std::ranges::find(
                audit->getFindings(),
                AuthorityRepairAuditArtifactKind::DependentObjectMetadataInstallationRecord,
                &AuthorityRepairFinding::artifact_kind);
            ASSERT_NE(installation_finding, audit->getFindings().end());
            EXPECT_EQ(installation_finding->finding_kind, AuthorityRepairFindingKind::Missing);
        }
    }
}

TEST(UDTAuthorityRepair, ExactSourceHierarchyFallsBackAndAlwaysSelectsHighestAvailablePriority)
{
    for (const auto available_source :
         {AuthorityRepairSource::LocalSchemaWAL, AuthorityRepairSource::ReplicatedAuthority, AuthorityRepairSource::VerifiedBackup})
    {
        RepairFixture fixture;
        auto audit = fixture.missingDefinitionAudit();
        const auto & target = audit->getCompleteRepairTargets().front();
        const std::array candidates{fixture.candidate(target, available_source, fixture.canonical_record)};
        auto plan = AuthorityRepairPlan::build(*audit, candidates);
        ASSERT_EQ(plan->getSelections().size(), 1);
        EXPECT_EQ(plan->getSelections().front().source, available_source);
        EXPECT_EQ(plan->getSelections().front().canonical_bytes, fixture.canonical_record);
        EXPECT_EQ(plan->getDamagedArtifactManifestDigest(), audit->getDamagedArtifactManifestDigest());
    }

    RepairFixture fixture;
    auto audit = fixture.missingDefinitionAudit();
    const auto & target = audit->getCompleteRepairTargets().front();
    const std::array all_candidates{
        fixture.candidate(target, AuthorityRepairSource::LocalSchemaWAL, fixture.canonical_record),
        fixture.candidate(target, AuthorityRepairSource::ReplicatedAuthority, fixture.canonical_record),
        fixture.candidate(target, AuthorityRepairSource::VerifiedBackup, fixture.canonical_record),
    };
    auto plan = AuthorityRepairPlan::build(*audit, all_candidates);
    ASSERT_EQ(plan->getSelections().size(), 1);
    EXPECT_EQ(plan->getSelections().front().source, AuthorityRepairSource::LocalSchemaWAL);
}

TEST(UDTAuthorityRepair, MismatchedAndConflictingSourcesCannotProduceARepairPlan)
{
    RepairFixture fixture;
    auto audit = fixture.missingDefinitionAudit();
    const auto & target = audit->getCompleteRepairTargets().front();

    Record mismatched_record = fixture.record;
    mismatched_record.comment = "different canonical bytes";
    const String mismatched_bytes = encodeRecord(mismatched_record);
    const std::array mismatched_candidates{
        fixture.candidate(target, AuthorityRepairSource::LocalSchemaWAL, mismatched_bytes),
    };
    expectPlanError(
        AuthorityRepairPlanError::Code::ExactSourceMissing,
        [&] { static_cast<void>(AuthorityRepairPlan::build(*audit, mismatched_candidates)); });

    const std::array conflicting_candidates{
        fixture.candidate(target, AuthorityRepairSource::LocalSchemaWAL, fixture.canonical_record),
        fixture.candidate(target, AuthorityRepairSource::LocalSchemaWAL, mismatched_bytes),
    };
    expectPlanError(
        AuthorityRepairPlanError::Code::ConflictingCandidate,
        [&] { static_cast<void>(AuthorityRepairPlan::build(*audit, conflicting_candidates)); });
}

TEST(UDTAuthorityRepair, CooperativePlanContinuationSealsProgressWithoutChangingSelection)
{
    RepairFixture fixture;
    auto audit = fixture.missingDefinitionAudit();
    const auto & target = audit->getCompleteRepairTargets().front();
    const std::array candidates{
        fixture.candidate(target, AuthorityRepairSource::ReplicatedAuthority, fixture.canonical_record),
    };

    AuthorityRepairPlanBuildContinuation continuation;
    AuthorityRepairPlan::Ptr plan;
    for (UInt64 invocation = 0; invocation < 128 && !plan; ++invocation)
        plan = AuthorityRepairPlan::resumeExactCandidateSet(continuation, *audit, candidates, 1);
    ASSERT_NE(plan, nullptr);
    ASSERT_EQ(plan->getSelections().size(), 1);
    EXPECT_EQ(plan->getSelections().front().source, AuthorityRepairSource::ReplicatedAuthority);
}

}
}
