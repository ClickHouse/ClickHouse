#include <Databases/UDT/PhysicalizationApplyCoordinator.h>
#include <Databases/UDT/SyntheticObjectMetadata.h>
#include <Databases/tests/UDTTestResourceImages.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/UDT/PhysicalTypeFingerprint.h>
#include <DataTypes/UDT/TemplateChecker.h>

#include <IO/WriteHelpers.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <map>
#include <optional>
#include <span>
#include <stdexcept>
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

String toHex(const Digest & value)
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

SchemaObjectID objectID(SchemaObjectKind kind, UUID database_uuid, UUID object_uuid)
{
    return {.kind = kind, .database_uuid = database_uuid, .object_uuid = object_uuid};
}

DefinitionInput definitionInput(UUID database_uuid, UUID type_uuid)
{
    DefinitionInput input;
    input.identity = {.database_uuid = database_uuid, .type_uuid = type_uuid, .revision = 1};
    input.normalized_name = "db.Value";
    input.normalized_local_name = "Value";
    TemplateNode root;
    root.kind = TemplateNodeKind::BuiltIn;
    root.atom = "UInt64";
    input.nodes.push_back(std::move(root));
    input.semantic_capabilities = semanticCapabilityBit(SemanticCapability::Input);
    return input;
}

Record definitionRecord(const Definition & definition)
{
    return makeRecord(
        definition,
        {
            .canonical_definition_sql = "ATTACH TYPE db.Value UUID '" + toString(definition.getIdentity().type_uuid)
                + "' REVISION 1 AS UInt64 DEFINITION HASH '" + toHex(definition.getDefinitionHash()) + "'",
            .canonical_physical_template_sql = "UInt64",
            .owner_uuid = uuid(0x9000, 1),
            .owner_display_name = "owner",
            .comment = {},
            .creation_time_us_utc = 1,
        });
}

String noArguments()
{
    constexpr std::array<char, 3> bytes{1, 0, 0};
    return {bytes.data(), bytes.size()};
}

PersistedTypeDescriptor descriptor(const Record & definition)
{
    const String arguments = noArguments();
    const auto physical_type = DataTypeFactory::instance().get("UInt64");
    const String canonical_physical_type = physical_type->getName();
    const Digest storage_fingerprint = physicalTypeFingerprint(physical_type);
    const Digest instantiation_hash = computeInstantiationSemanticHash({
        .definition_identity = definition.identity,
        .definition_hash = definition.definition_hash,
        .canonical_arguments_encoding = arguments,
        .canonical_physical_type = canonical_physical_type,
        .storage_fingerprint = storage_fingerprint,
        .checker_abi = definition.checker_abi,
        .checker_charge_abi = definition.checker_charge_abi,
        .policy_abi = definition.policy_abi,
        .function_registry_abi = definition.function_registry_abi,
        .policy_semantic_hash = definition.policy_semantic_hash,
        .semantic_capabilities = definition.semantic_capabilities,
    });
    return PersistedTypeDescriptor::fromCanonicalPersistenceFields(
        definition.identity,
        definition.definition_hash,
        arguments,
        canonical_physical_type,
        instantiation_hash,
        storage_fingerprint,
        definition.checker_abi,
        definition.checker_charge_abi,
        definition.policy_abi,
        definition.function_registry_abi,
        definition.policy_semantic_hash,
        definition.semantic_capabilities,
        definition.normalized_name);
}

class Provider final : public IPhysicalizationObjectProvider
{
public:
    PhysicalizationObject load(const SidecarExpectationRecord & expectation) const override
    {
        ++calls;
        return objects.at(expectation.object);
    }

    mutable UInt64 calls = 0;
    std::map<SchemaObjectID, PhysicalizationObject> objects;
};

class RewriteAdapter final : public IPhysicalizationRewriteAdapter
{
public:
    std::vector<PhysicalizationRewriteImage>
    prepareRewriteImages(const PhysicalizationPlan & plan) const override
    {
        ++calls;
        std::vector<PhysicalizationRewriteImage> result;
        result.reserve(plan.getObjects().size());
        for (const auto & object : plan.getObjects())
            result.push_back(images.at(object.object));
        return result;
    }

    mutable UInt64 calls = 0;
    std::map<SchemaObjectID, PhysicalizationRewriteImage> images;
};

class Authorization final : public IPhysicalizationApplyAuthorization
{
public:
    void requireObjectRewrite(const PhysicalizationManifestObject &) const override
    {
        ++object_checks;
        if (deny_objects)
            throw std::runtime_error("denied");
    }

    void requireDefinitionDrop(const PhysicalizationManifestDefinition &) const override
    {
        ++definition_checks;
        if (deny_definitions)
            throw std::runtime_error("denied");
    }

    bool deny_objects = false;
    bool deny_definitions = false;
    mutable UInt64 object_checks = 0;
    mutable UInt64 definition_checks = 0;
};

class DurableStorage final : public IDatabaseSchemaMutationDurableStorage
{
public:
    enum class Failure
    {
        None,
        FinishStaging,
        Commit,
    };

    explicit DurableStorage(AuthorityState current_state_, UInt64 predecessor_)
        : current_state(std::move(current_state_))
        , predecessor(predecessor_)
    {
    }

    void validateMutationGuardAndDurablePredecessor(
        const DatabaseSchemaMutationGuard & guard,
        const std::optional<AuthorityState> & expected_preceding_authority_state,
        UInt64 transaction_id) override
    {
        events.emplace_back("validate");
        if (guard.getDatabaseUUID() != current_state.database_uuid || guard.getDurablePredecessorTransactionID() != predecessor
            || expected_preceding_authority_state != current_state || transaction_id <= predecessor)
            throw std::runtime_error("invalid predecessor");
    }

    void markMutationRecoveryRequired(
        const DatabaseSchemaMutationGuard &, UInt64 transaction_id, DatabaseSchemaMutationDurabilityPhase phase) noexcept override
    {
        recovery_required = true;
        recovery_transaction_id = transaction_id;
        recovery_phase = phase;
    }

    void validateRecoveryGuard(const DatabaseSchemaMutationGuard &, UInt64) override { throw std::runtime_error("unused recovery"); }

    void
    stageArtifact(const DatabaseSchemaWALStagedArtifactLocator &, const DatabaseSchemaWALStagedArtifactRef &, std::string_view) override
    {
        events.emplace_back("stage");
    }

    void finishStaging(UUID, UInt64) override
    {
        events.emplace_back("finish-staging");
        if (failure == Failure::FinishStaging)
            throw std::runtime_error("finish staging failed");
    }

    void persistPrepare(UInt64, std::string_view canonical_prepare) override
    {
        events.emplace_back("prepare");
        prepare = decodeDatabaseSchemaWALPrepare(canonical_prepare);
    }

    void installArtifact(const DatabaseSchemaWALStagedArtifactRef & artifact, std::string_view canonical_bytes) override
    {
        events.emplace_back("install");
        if (artifact.kind == DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata)
            installed_dependent_metadata = canonical_bytes;
    }

    void removeArtifact(DatabaseSchemaWALStagedArtifactKind, const SchemaObjectID &) override { events.emplace_back("remove"); }

    void finishInstallation(UUID, UInt64) override { events.emplace_back("finish-installation"); }

    void persistCommit(UInt64, std::string_view canonical_commit) override
    {
        events.emplace_back("commit");
        if (failure == Failure::Commit)
            throw std::runtime_error("commit failed");
        commit = decodeDatabaseSchemaWALCommit(canonical_commit);
        if (!prepare)
            throw std::runtime_error("commit before prepare");
        current_state = prepare->after_authority_state;
    }

    void finishRecovery(UUID, UInt64, DatabaseSchemaWALRecoveryDecision) override { throw std::runtime_error("unused recovery"); }

    void discardUnpreparedStaging(const DatabaseSchemaMutationGuard &, UInt64) override
    {
        events.emplace_back("discard");
        discarded = true;
    }

    void retireRolledBackTransaction(const DatabaseSchemaMutationGuard &, UInt64) override
    {
        throw std::runtime_error("unused retirement");
    }

    void persistValidatedCheckpoint(
        const DatabaseSchemaMutationGuard &,
        const DatabaseSchemaWALCheckpoint &,
        std::string_view,
        std::string_view,
        std::string_view) override
    {
        throw std::runtime_error("unused checkpoint");
    }

    void compactThroughValidatedCheckpoint(const DatabaseSchemaMutationGuard &, const DatabaseSchemaWALCheckpoint &) override
    {
        throw std::runtime_error("unused checkpoint");
    }

    AuthorityState current_state;
    UInt64 predecessor;
    Failure failure = Failure::None;
    std::vector<String> events;
    std::optional<DatabaseSchemaWALPrepare> prepare;
    std::optional<DatabaseSchemaWALCommit> commit;
    String installed_dependent_metadata;
    bool discarded = false;
    bool recovery_required = false;
    UInt64 recovery_transaction_id = 0;
    DatabaseSchemaMutationDurabilityPhase recovery_phase = DatabaseSchemaMutationDurabilityPhase::PrepareMarker;
};

struct Fixture
{
    UUID database_uuid = uuid(1, 2);
    SchemaObjectID type = objectID(SchemaObjectKind::TypeDefinition, database_uuid, uuid(3, 4));
    SchemaObjectID object = objectID(SchemaObjectKind::SyntheticTestObject, database_uuid, uuid(5, 6));
    SchemaObjectID second_object = objectID(SchemaObjectKind::SyntheticTestObject, database_uuid, uuid(5, 7));
    Definition::Ptr definition = TemplateChecker::checkAll({definitionInput(database_uuid, type.object_uuid)}).front();
    Record record = definitionRecord(*definition);
    Provider provider;
    RewriteAdapter rewrite;
    SidecarExpectationRecord expectation;
    AuthorityRoot::Ptr root;

    AuthorityRoot::Ptr
    buildRoot(AuthorityState state, std::span<const SidecarExpectationRecord> expectations, SchemaObjectDependencyGraph::Ptr graph) const
    {
        std::vector<Test::DependentObjectResourceImageInput> inputs;
        inputs.reserve(expectations.size());
        for (const auto & current_expectation : expectations)
        {
            inputs.push_back({
                .canonical_metadata_bytes = rewrite.images.at(current_expectation.object).before_canonical_metadata_bytes,
                .references = provider.objects.at(current_expectation.object).references,
                .canonical_installation_record_bytes = {},
            });
        }
        const Test::DependentObjectResourceImageBatch dependent_objects(expectations, std::move(inputs));
        return AuthorityRootBuilder::build(
            std::move(state), 1, std::array{definition}, std::array{record}, expectations, std::move(graph), {}, dependent_objects.get());
    }

    Fixture()
    {
        const auto persisted_descriptor = descriptor(record);
        const PersistedTypeOccurrencePath path{
            .section = PersistedTypePathSection::SyntheticPayload,
            .object_ordinal = 0,
            .occurrence_ordinal = 0,
            .type_child_ordinals = {},
        };
        const std::vector<SyntheticObjectPhysicalOccurrence> before_occurrences{{
            .path = path,
            .canonical_physical_type = "UInt64",
            .storage_fingerprint = persisted_descriptor.getStorageFingerprint(),
            .selected_semantic_capabilities = semanticCapabilityBit(SemanticCapability::Input),
        }};
        auto after_occurrences = before_occurrences;
        after_occurrences[0].selected_semantic_capabilities = 0;
        const auto before_metadata = makeSyntheticObjectMetadata(object, 1, "synthetic", before_occurrences);
        const auto after_metadata = makeSyntheticObjectMetadata(object, 2, "synthetic", after_occurrences);
        EXPECT_EQ(before_metadata.physical_schema_fingerprint, after_metadata.physical_schema_fingerprint);
        EXPECT_NE(before_metadata.canonical_record_hash, after_metadata.canonical_record_hash);
        const auto bound_before = makeSyntheticBoundPhysicalSchema(before_metadata);
        EXPECT_EQ(bound_before.occurrences.size(), 1);
        EXPECT_EQ(bound_before.occurrences[0].selected_semantic_capabilities, semanticCapabilityBit(SemanticCapability::Input));
        const String before_bytes = encodeSyntheticObjectMetadata(before_metadata);
        const String after_bytes = encodeSyntheticObjectMetadata(after_metadata);

        PersistedTypeReferences references;
        references.object = object;
        references.object_schema_revision = 1;
        references.physical_schema_fingerprint = before_metadata.physical_schema_fingerprint;
        references.descriptors = {persisted_descriptor};
        references.occurrence_paths = {path};
        references.uses = {{.path_id = 0, .descriptor_id = 0}};
        expectation = SidecarExpectationRecord{
            .object = object,
            .object_schema_revision = 1,
            .sidecar_hash = computePersistedTypeReferencesSidecarHash(references),
            .physical_schema_fingerprint = before_metadata.physical_schema_fingerprint,
        };
        provider.objects.emplace(
            object,
            PhysicalizationObject{
                .object = object,
                .object_schema_revision = 1,
                .diagnostic_name = "synthetic",
                .canonical_metadata_hash
                = computeDatabaseSchemaWALStagedArtifactHash(DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, before_bytes),
                .references = references,
                .selected_semantic_capabilities = {semanticCapabilityBit(SemanticCapability::Input)},
            });
        rewrite.images.emplace(
            object,
            PhysicalizationRewriteImage{
                .object = object,
                .before_object_schema_revision = 1,
                .after_object_schema_revision = 2,
                .before_canonical_metadata_bytes = before_bytes,
                .after_canonical_metadata_bytes = after_bytes,
                .before_physical_schema_fingerprint = before_metadata.physical_schema_fingerprint,
                .after_physical_schema_fingerprint = after_metadata.physical_schema_fingerprint,
            });

        std::vector<AuthorityInventoryLeaf> leaves{
            {
                .key = {.record_kind = AuthorityInventoryRecordKind::TypeDefinition, .object_uuid = type.object_uuid},
                .object_revision = 1,
                .canonical_record_hash = computeRecordHash(record),
            },
            {
                .key = {.record_kind = AuthorityInventoryRecordKind::SidecarExpectation, .object_uuid = object.object_uuid},
                .object_revision = 1,
                .canonical_record_hash = computeSidecarExpectationRecordHash(expectation),
            },
        };
        std::sort(leaves.begin(), leaves.end(), authorityInventoryLeafLess);
        const auto summary = buildAuthorityInventorySummary(leaves);
        auto graph = SchemaObjectDependencyGraph::build(
            database_uuid,
            std::array{type, object},
            std::array{SchemaObjectDependencyEdge{
                .dependent = object,
                .dependency = type,
                .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
            }});
        const auto state = makeAuthorityState(
            database_uuid, 2, dependent_object_authority_capability_mask, summary.leaf_count, summary.merkle_radix_root, graph->computeRoot());
        const std::array expectation_records{expectation};
        root = buildRoot(state, expectation_records, std::move(graph));
    }

    void replaceRootWithCyclicObjectDependencies()
    {
        const auto persisted_descriptor = descriptor(record);
        const PersistedTypeOccurrencePath path{
            .section = PersistedTypePathSection::SyntheticPayload,
            .object_ordinal = 0,
            .occurrence_ordinal = 0,
            .type_child_ordinals = {},
        };
        const std::vector<SyntheticObjectPhysicalOccurrence> before_occurrences{{
            .path = path,
            .canonical_physical_type = persisted_descriptor.getCanonicalPhysicalType(),
            .storage_fingerprint = persisted_descriptor.getStorageFingerprint(),
            .selected_semantic_capabilities = semanticCapabilityBit(SemanticCapability::Input),
        }};
        auto after_occurrences = before_occurrences;
        after_occurrences[0].selected_semantic_capabilities = 0;
        const auto before_metadata = makeSyntheticObjectMetadata(second_object, 1, "synthetic-second", before_occurrences);
        const auto after_metadata = makeSyntheticObjectMetadata(second_object, 2, "synthetic-second", after_occurrences);
        const String before_bytes = encodeSyntheticObjectMetadata(before_metadata);
        const String after_bytes = encodeSyntheticObjectMetadata(after_metadata);

        PersistedTypeReferences references;
        references.object = second_object;
        references.object_schema_revision = 1;
        references.physical_schema_fingerprint = before_metadata.physical_schema_fingerprint;
        references.descriptors = {persisted_descriptor};
        references.occurrence_paths = {path};
        references.uses = {{.path_id = 0, .descriptor_id = 0}};
        const SidecarExpectationRecord second_expectation{
            .object = second_object,
            .object_schema_revision = 1,
            .sidecar_hash = computePersistedTypeReferencesSidecarHash(references),
            .physical_schema_fingerprint = before_metadata.physical_schema_fingerprint,
        };
        provider.objects.emplace(
            second_object,
            PhysicalizationObject{
                .object = second_object,
                .object_schema_revision = 1,
                .diagnostic_name = "synthetic-second",
                .canonical_metadata_hash
                = computeDatabaseSchemaWALStagedArtifactHash(DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, before_bytes),
                .references = references,
                .selected_semantic_capabilities = {semanticCapabilityBit(SemanticCapability::Input)},
            });
        rewrite.images.emplace(
            second_object,
            PhysicalizationRewriteImage{
                .object = second_object,
                .before_object_schema_revision = 1,
                .after_object_schema_revision = 2,
                .before_canonical_metadata_bytes = before_bytes,
                .after_canonical_metadata_bytes = after_bytes,
                .before_physical_schema_fingerprint = before_metadata.physical_schema_fingerprint,
                .after_physical_schema_fingerprint = after_metadata.physical_schema_fingerprint,
            });

        std::vector<AuthorityInventoryLeaf> leaves{
            {
                .key = {.record_kind = AuthorityInventoryRecordKind::TypeDefinition, .object_uuid = type.object_uuid},
                .object_revision = 1,
                .canonical_record_hash = computeRecordHash(record),
            },
            {
                .key = {.record_kind = AuthorityInventoryRecordKind::SidecarExpectation, .object_uuid = object.object_uuid},
                .object_revision = 1,
                .canonical_record_hash = computeSidecarExpectationRecordHash(expectation),
            },
            {
                .key = {.record_kind = AuthorityInventoryRecordKind::SidecarExpectation, .object_uuid = second_object.object_uuid},
                .object_revision = 1,
                .canonical_record_hash = computeSidecarExpectationRecordHash(second_expectation),
            },
        };
        std::sort(leaves.begin(), leaves.end(), authorityInventoryLeafLess);
        const auto summary = buildAuthorityInventorySummary(leaves);
        auto graph = SchemaObjectDependencyGraph::build(
            database_uuid,
            std::array{type, object, second_object},
            std::array{
                SchemaObjectDependencyEdge{
                    .dependent = object,
                    .dependency = type,
                    .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
                },
                SchemaObjectDependencyEdge{
                    .dependent = second_object,
                    .dependency = type,
                    .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
                },
                SchemaObjectDependencyEdge{
                    .dependent = object,
                    .dependency = second_object,
                    .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnObject,
                },
                SchemaObjectDependencyEdge{
                    .dependent = second_object,
                    .dependency = object,
                    .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnObject,
                },
            });
        const auto state = makeAuthorityState(
            database_uuid, 2, dependent_object_authority_capability_mask, summary.leaf_count, summary.merkle_radix_root, graph->computeRoot());
        const std::array expectation_records{expectation, second_expectation};
        root = buildRoot(state, expectation_records, std::move(graph));
    }

    PhysicalizationPlan buildPlan(const AuthorityRoot & current_root) const
    {
        return PhysicalizationPlanner::build(
            current_root,
            {
                .scope = PhysicalizationScope::Object,
                .object = object,
                .drop_unused_types = false,
            },
            provider);
    }
};

TEST(PhysicalizationApplyCoordinator, ExactTokenCommitsAndPublishesOnce)
{
    Fixture fixture;
    AtomicAuthority authority(
        fixture.database_uuid, atomicDatabaseAuthorityCapabilities(), std::move(fixture.root));
    auto snapshot = authority.acquireCurrentRoot();
    ASSERT_TRUE(snapshot);
    const auto plan = fixture.buildPlan(snapshot.get());
    PhysicalizationTokenStore tokens(fixture.database_uuid);
    const UUID principal = uuid(7, 8);
    const String token = tokens.issue(plan, principal, 100, 100);
    Authorization authorization;
    DurableStorage storage(snapshot->getAuthorityState(), 40);
    auto guard = DatabaseSchemaMutationGuard::issue(fixture.database_uuid, 1, 40);

    const auto commit = PhysicalizationApplyCoordinator::apply(
        snapshot.get(), authority, storage, guard, tokens, token, principal, 101, 41, fixture.provider, fixture.rewrite, authorization);

    EXPECT_EQ(commit.transaction_id, 41);
    EXPECT_EQ(authorization.object_checks, 1);
    EXPECT_EQ(authorization.definition_checks, 0);
    EXPECT_EQ(fixture.rewrite.calls, 1);
    EXPECT_EQ(tokens.getOutstandingTokenCount(), 0);
    EXPECT_EQ(guard.getState(), DatabaseSchemaMutationGuard::State::Finished);
    EXPECT_FALSE(storage.discarded);
    ASSERT_TRUE(storage.commit);
    ASSERT_FALSE(storage.installed_dependent_metadata.empty());
    const auto installed_metadata = decodeSyntheticObjectMetadata(storage.installed_dependent_metadata);
    const auto installed_physical = makeSyntheticBoundPhysicalSchema(installed_metadata);
    ASSERT_EQ(installed_physical.occurrences.size(), 1);
    EXPECT_EQ(installed_physical.occurrences[0].selected_semantic_capabilities, 0);
    EXPECT_EQ(installed_physical.physical_schema_fingerprint, plan.getObjects()[0].physical_schema_fingerprint);
    auto after = authority.acquireCurrentRoot();
    ASSERT_TRUE(after);
    EXPECT_EQ(after->getDatabaseCatalogEpoch(), 3);
    EXPECT_TRUE(after->getExpectationRecords().empty());
    EXPECT_FALSE(after->getSchemaObjectDependencyGraph().containsEdge({
        .dependent = fixture.object,
        .dependency = fixture.type,
        .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
    }));
}

TEST(PhysicalizationApplyCoordinator, AuthorizationFailureHasNoRewriteTokenConsumptionOrStorageCall)
{
    Fixture fixture;
    AtomicAuthority authority(
        fixture.database_uuid, atomicDatabaseAuthorityCapabilities(), std::move(fixture.root));
    auto snapshot = authority.acquireCurrentRoot();
    const auto plan = fixture.buildPlan(snapshot.get());
    PhysicalizationTokenStore tokens(fixture.database_uuid);
    const UUID principal = uuid(9, 10);
    const String token = tokens.issue(plan, principal, 100, 100);
    Authorization authorization;
    authorization.deny_objects = true;
    DurableStorage storage(snapshot->getAuthorityState(), 50);
    auto guard = DatabaseSchemaMutationGuard::issue(fixture.database_uuid, 2, 50);

    EXPECT_THROW(
        static_cast<void>(PhysicalizationApplyCoordinator::apply(
            snapshot.get(),
            authority,
            storage,
            guard,
            tokens,
            token,
            principal,
            101,
            51,
            fixture.provider,
            fixture.rewrite,
            authorization)),
        std::runtime_error);
    EXPECT_EQ(tokens.getOutstandingTokenCount(), 1);
    EXPECT_EQ(fixture.rewrite.calls, 0);
    EXPECT_TRUE(storage.events.empty());
    EXPECT_EQ(guard.getState(), DatabaseSchemaMutationGuard::State::Ready);
}

TEST(PhysicalizationApplyCoordinator, DropUnusedDefinitionRequiresAndUsesExactAuthorization)
{
    Fixture fixture;
    AtomicAuthority authority(
        fixture.database_uuid, atomicDatabaseAuthorityCapabilities(), std::move(fixture.root));
    auto snapshot = authority.acquireCurrentRoot();
    ASSERT_TRUE(snapshot);
    const auto plan = PhysicalizationPlanner::build(
        snapshot.get(),
        {
            .scope = PhysicalizationScope::Object,
            .object = fixture.object,
            .drop_unused_types = true,
        },
        fixture.provider);
    ASSERT_EQ(plan.getDefinitions().size(), 1);
    ASSERT_TRUE(plan.getDefinitions().front().selected_for_drop);
    PhysicalizationTokenStore tokens(fixture.database_uuid);
    const UUID principal = uuid(9, 12);
    const String token = tokens.issue(plan, principal, 100, 100);
    Authorization authorization;
    DurableStorage storage(snapshot->getAuthorityState(), 57);
    auto guard = DatabaseSchemaMutationGuard::issue(fixture.database_uuid, 23, 57);

    static_cast<void>(PhysicalizationApplyCoordinator::apply(
        snapshot.get(), authority, storage, guard, tokens, token, principal, 101, 58, fixture.provider, fixture.rewrite, authorization));

    EXPECT_EQ(authorization.object_checks, 1);
    EXPECT_EQ(authorization.definition_checks, 1);
    EXPECT_EQ(tokens.getOutstandingTokenCount(), 0);
    auto after = authority.acquireCurrentRoot();
    ASSERT_TRUE(after);
    EXPECT_TRUE(after->getDefinitionRecords().empty());
}

TEST(PhysicalizationApplyCoordinator, DefinitionDropDenialRetainsTokenAndSkipsRewriteAndStorage)
{
    Fixture fixture;
    AtomicAuthority authority(
        fixture.database_uuid, atomicDatabaseAuthorityCapabilities(), std::move(fixture.root));
    auto snapshot = authority.acquireCurrentRoot();
    ASSERT_TRUE(snapshot);
    const auto plan = PhysicalizationPlanner::build(
        snapshot.get(),
        {
            .scope = PhysicalizationScope::Object,
            .object = fixture.object,
            .drop_unused_types = true,
        },
        fixture.provider);
    PhysicalizationTokenStore tokens(fixture.database_uuid);
    const UUID principal = uuid(9, 13);
    const String token = tokens.issue(plan, principal, 100, 100);
    Authorization authorization;
    authorization.deny_definitions = true;
    DurableStorage storage(snapshot->getAuthorityState(), 59);
    auto guard = DatabaseSchemaMutationGuard::issue(fixture.database_uuid, 24, 59);

    EXPECT_THROW(
        static_cast<void>(PhysicalizationApplyCoordinator::apply(
            snapshot.get(),
            authority,
            storage,
            guard,
            tokens,
            token,
            principal,
            101,
            60,
            fixture.provider,
            fixture.rewrite,
            authorization)),
        std::runtime_error);
    EXPECT_EQ(authorization.object_checks, 1);
    EXPECT_EQ(authorization.definition_checks, 1);
    EXPECT_EQ(tokens.getOutstandingTokenCount(), 1);
    EXPECT_EQ(fixture.rewrite.calls, 0);
    EXPECT_TRUE(storage.events.empty());
    EXPECT_EQ(guard.getState(), DatabaseSchemaMutationGuard::State::Ready);
}

TEST(PhysicalizationApplyCoordinator, TokenExpiryIsRecheckedAtPointOfNoReturn)
{
    Fixture fixture;
    AtomicAuthority authority(
        fixture.database_uuid, atomicDatabaseAuthorityCapabilities(), std::move(fixture.root));
    auto snapshot = authority.acquireCurrentRoot();
    ASSERT_TRUE(snapshot);
    const auto plan = fixture.buildPlan(snapshot.get());
    PhysicalizationTokenStore tokens(fixture.database_uuid);
    const UUID principal = uuid(9, 130);
    const String token = tokens.issue(plan, principal, 100, 10);
    Authorization authorization;
    DurableStorage storage(snapshot->getAuthorityState(), 130);
    auto guard = DatabaseSchemaMutationGuard::issue(fixture.database_uuid, 130, 130);
    UInt64 clock_calls = 0;

    EXPECT_THROW(
        static_cast<void>(PhysicalizationApplyCoordinator::apply(
            snapshot.get(),
            authority,
            storage,
            guard,
            tokens,
            token,
            principal,
            101,
            131,
            fixture.provider,
            fixture.rewrite,
            authorization,
            {},
            [&]
            {
                ++clock_calls;
                return 110;
            })),
        PhysicalizationTokenStoreError);

    EXPECT_EQ(clock_calls, 1);
    EXPECT_EQ(tokens.getOutstandingTokenCount(), 0);
    EXPECT_EQ(fixture.rewrite.calls, 1);
    EXPECT_EQ(storage.events, std::vector<String>{"validate"});
    EXPECT_EQ(guard.getState(), DatabaseSchemaMutationGuard::State::Ready);
    auto after = authority.acquireCurrentRoot();
    ASSERT_TRUE(after);
    EXPECT_EQ(after->getDatabaseCatalogEpoch(), 2);
}

TEST(PhysicalizationApplyCoordinator, InvalidGuardIsRejectedBeforeTokenConsumptionOrStorage)
{
    Fixture fixture;
    AtomicAuthority authority(
        fixture.database_uuid, atomicDatabaseAuthorityCapabilities(), std::move(fixture.root));
    auto snapshot = authority.acquireCurrentRoot();
    ASSERT_TRUE(snapshot);
    const auto plan = fixture.buildPlan(snapshot.get());
    PhysicalizationTokenStore tokens(fixture.database_uuid);
    const UUID principal = uuid(9, 14);
    const String token = tokens.issue(plan, principal, 100, 100);
    Authorization authorization;
    DurableStorage storage(snapshot->getAuthorityState(), 61);
    auto guard = DatabaseSchemaMutationGuard::issue(uuid(0xdead, 0xbeef), 25, 61);

    EXPECT_THROW(
        static_cast<void>(PhysicalizationApplyCoordinator::apply(
            snapshot.get(),
            authority,
            storage,
            guard,
            tokens,
            token,
            principal,
            101,
            62,
            fixture.provider,
            fixture.rewrite,
            authorization)),
        std::logic_error);
    EXPECT_EQ(tokens.getOutstandingTokenCount(), 1);
    EXPECT_TRUE(storage.events.empty());
    EXPECT_EQ(guard.getState(), DatabaseSchemaMutationGuard::State::Ready);
}

TEST(PhysicalizationApplyCoordinator, StaleDurablePredecessorIsRejectedBeforeTokenConsumptionOrStaging)
{
    Fixture fixture;
    AtomicAuthority authority(
        fixture.database_uuid, atomicDatabaseAuthorityCapabilities(), std::move(fixture.root));
    auto snapshot = authority.acquireCurrentRoot();
    ASSERT_TRUE(snapshot);
    const auto plan = fixture.buildPlan(snapshot.get());
    PhysicalizationTokenStore tokens(fixture.database_uuid);
    const UUID principal = uuid(9, 15);
    const String token = tokens.issue(plan, principal, 100, 100);
    Authorization authorization;
    DurableStorage storage(snapshot->getAuthorityState(), 63);
    auto stale_guard = DatabaseSchemaMutationGuard::issue(fixture.database_uuid, 26, 62);

    EXPECT_THROW(
        static_cast<void>(PhysicalizationApplyCoordinator::apply(
            snapshot.get(),
            authority,
            storage,
            stale_guard,
            tokens,
            token,
            principal,
            101,
            64,
            fixture.provider,
            fixture.rewrite,
            authorization)),
        std::runtime_error);
    EXPECT_EQ(tokens.getOutstandingTokenCount(), 1);
    EXPECT_EQ(storage.events, std::vector<String>{"validate"});
    EXPECT_FALSE(storage.discarded);
    EXPECT_EQ(stale_guard.getState(), DatabaseSchemaMutationGuard::State::Ready);
}

TEST(PhysicalizationApplyCoordinator, DependencyCycleFailsBeforeTokenConsumptionOrStorageCall)
{
    Fixture fixture;
    fixture.replaceRootWithCyclicObjectDependencies();
    AtomicAuthority authority(
        fixture.database_uuid, atomicDatabaseAuthorityCapabilities(), std::move(fixture.root));
    auto snapshot = authority.acquireCurrentRoot();
    ASSERT_TRUE(snapshot);
    const auto plan = PhysicalizationPlanner::build(
        snapshot.get(),
        {
            .scope = PhysicalizationScope::Database,
            .object = std::nullopt,
            .drop_unused_types = false,
        },
        fixture.provider);
    PhysicalizationTokenStore tokens(fixture.database_uuid);
    const UUID principal = uuid(9, 11);
    const String token = tokens.issue(plan, principal, 100, 100);
    Authorization authorization;
    DurableStorage storage(snapshot->getAuthorityState(), 55);
    auto guard = DatabaseSchemaMutationGuard::issue(fixture.database_uuid, 22, 55);

    EXPECT_THROW(
        static_cast<void>(PhysicalizationApplyCoordinator::apply(
            snapshot.get(),
            authority,
            storage,
            guard,
            tokens,
            token,
            principal,
            101,
            56,
            fixture.provider,
            fixture.rewrite,
            authorization)),
        std::logic_error);
    EXPECT_EQ(tokens.getOutstandingTokenCount(), 1);
    EXPECT_EQ(fixture.rewrite.calls, 1);
    EXPECT_TRUE(storage.events.empty());
    EXPECT_EQ(guard.getState(), DatabaseSchemaMutationGuard::State::Ready);
    auto after = authority.acquireCurrentRoot();
    ASSERT_TRUE(after);
    EXPECT_EQ(after->getDatabaseCatalogEpoch(), 2);
}

TEST(PhysicalizationApplyCoordinator, PrePrepareFailureConsumesTokenAndDiscardsStaging)
{
    Fixture fixture;
    AtomicAuthority authority(
        fixture.database_uuid, atomicDatabaseAuthorityCapabilities(), std::move(fixture.root));
    auto snapshot = authority.acquireCurrentRoot();
    const auto plan = fixture.buildPlan(snapshot.get());
    PhysicalizationTokenStore tokens(fixture.database_uuid);
    const UUID principal = uuid(11, 12);
    const String token = tokens.issue(plan, principal, 100, 100);
    Authorization authorization;
    DurableStorage storage(snapshot->getAuthorityState(), 60);
    storage.failure = DurableStorage::Failure::FinishStaging;
    auto guard = DatabaseSchemaMutationGuard::issue(fixture.database_uuid, 3, 60);

    EXPECT_THROW(
        static_cast<void>(PhysicalizationApplyCoordinator::apply(
            snapshot.get(),
            authority,
            storage,
            guard,
            tokens,
            token,
            principal,
            101,
            61,
            fixture.provider,
            fixture.rewrite,
            authorization)),
        std::runtime_error);
    EXPECT_EQ(tokens.getOutstandingTokenCount(), 0);
    EXPECT_TRUE(storage.discarded);
    EXPECT_FALSE(storage.recovery_required);
    EXPECT_EQ(guard.getState(), DatabaseSchemaMutationGuard::State::Finished);
    auto after = authority.acquireCurrentRoot();
    ASSERT_TRUE(after);
    EXPECT_EQ(after->getDatabaseCatalogEpoch(), 2);
}

TEST(PhysicalizationApplyCoordinator, CommitAmbiguityFailStopsWithoutPublication)
{
    Fixture fixture;
    AtomicAuthority authority(
        fixture.database_uuid, atomicDatabaseAuthorityCapabilities(), std::move(fixture.root));
    auto snapshot = authority.acquireCurrentRoot();
    const auto plan = fixture.buildPlan(snapshot.get());
    PhysicalizationTokenStore tokens(fixture.database_uuid);
    const UUID principal = uuid(13, 14);
    const String token = tokens.issue(plan, principal, 100, 100);
    Authorization authorization;
    DurableStorage storage(snapshot->getAuthorityState(), 70);
    storage.failure = DurableStorage::Failure::Commit;
    auto guard = DatabaseSchemaMutationGuard::issue(fixture.database_uuid, 4, 70);

    EXPECT_THROW(
        static_cast<void>(PhysicalizationApplyCoordinator::apply(
            snapshot.get(),
            authority,
            storage,
            guard,
            tokens,
            token,
            principal,
            101,
            71,
            fixture.provider,
            fixture.rewrite,
            authorization)),
        DatabaseSchemaMutationIndeterminateDurabilityError);
    EXPECT_EQ(tokens.getOutstandingTokenCount(), 0);
    EXPECT_FALSE(storage.discarded);
    EXPECT_TRUE(storage.recovery_required);
    EXPECT_EQ(storage.recovery_transaction_id, 71);
    EXPECT_EQ(storage.recovery_phase, DatabaseSchemaMutationDurabilityPhase::CommitMarker);
    EXPECT_EQ(guard.getState(), DatabaseSchemaMutationGuard::State::RecoveryRequired);
    auto after = authority.acquireCurrentRoot();
    ASSERT_TRUE(after);
    EXPECT_EQ(after->getDatabaseCatalogEpoch(), 2);
}

TEST(PhysicalizationApplyCoordinator, DependentObjectActivationUsesTheSameDurableThenPublishOrder)
{
    Fixture fixture;
    std::vector<AuthorityInventoryLeaf> leaves{{
        .key = {.record_kind = AuthorityInventoryRecordKind::TypeDefinition, .object_uuid = fixture.type.object_uuid},
        .object_revision = fixture.record.identity.revision,
        .canonical_record_hash = computeRecordHash(fixture.record),
    }};
    const auto summary = buildAuthorityInventorySummary(leaves);
    auto graph = SchemaObjectDependencyGraph::build(fixture.database_uuid, std::array{fixture.type}, {});
    const auto state = makeAuthorityState(
        fixture.database_uuid,
        1,
        definition_authority_capability_mask,
        summary.leaf_count,
        summary.merkle_radix_root,
        graph->computeRoot());
    auto root = AuthorityRootBuilder::build(
        state, 1, std::array{fixture.definition}, std::array{fixture.record}, {}, std::move(graph));
    AtomicAuthority authority(
        fixture.database_uuid, atomicDatabaseAuthorityCapabilities(), std::move(root));
    auto snapshot = authority.acquireCurrentRoot();
    ASSERT_TRUE(snapshot);
    auto prepared = DependentObjectActivationPlanner::plan(snapshot.get(), 81, 1);
    DurableStorage storage(snapshot->getAuthorityState(), 80);
    auto guard = DatabaseSchemaMutationGuard::issue(fixture.database_uuid, 5, 80);

    const auto commit = PhysicalizationApplyCoordinator::activateDependentObjectAuthority(authority, storage, guard, std::move(prepared));

    EXPECT_EQ(commit.transaction_id, 81);
    EXPECT_EQ(guard.getState(), DatabaseSchemaMutationGuard::State::Finished);
    ASSERT_TRUE(storage.prepare);
    EXPECT_TRUE(storage.prepare->staged_artifacts.empty());
    auto after = authority.acquireCurrentRoot();
    ASSERT_TRUE(after);
    EXPECT_EQ(after->getDatabaseCatalogEpoch(), 2);
    EXPECT_EQ(after->getPersistentCapabilityMask(), dependent_object_authority_capability_mask);
    EXPECT_TRUE(snapshot->sharesContentPayloadWith(after.get()));
}

}
}
