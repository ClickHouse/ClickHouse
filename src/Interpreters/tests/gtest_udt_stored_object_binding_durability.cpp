#include <Databases/DatabaseSchemaMutationTransaction.h>
#include <Databases/UDT/PhysicalizationApplyCoordinator.h>
#include <Databases/UDT/StoredObjectUDTPublicationCoordinator.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/UDT/CanonicalTypeArguments.h>
#include <DataTypes/UDT/IAuthorityAdapter.h>
#include <DataTypes/UDT/TemplateChecker.h>

#include <Interpreters/UDT/DictionaryAttributeTypeBindings.h>
#include <Interpreters/UDT/StoredObjectTypeBindingAdmission.h>
#include <Interpreters/UDT/ViewOutputTypeBindings.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>
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

SchemaObjectID objectID(SchemaObjectKind kind, UUID database_uuid, UUID object_uuid)
{
    return {.kind = kind, .database_uuid = database_uuid, .object_uuid = object_uuid};
}

UUID databaseUUID()
{
    return uuid(0x550e8400e29b41d4ULL, 0xa716446655440000ULL);
}

SchemaObjectID viewObject()
{
    return objectID(SchemaObjectKind::View, databaseUUID(), uuid(0x123456789abcdef0ULL, 0x0102030405060708ULL));
}

SchemaObjectID dictionaryObject()
{
    return objectID(SchemaObjectKind::Dictionary, databaseUUID(), uuid(0x123456789abcdef0ULL, 0x1112131415161718ULL));
}

Definition::Ptr checkedAlias()
{
    DefinitionInput input;
    input.identity = {
        .database_uuid = databaseUUID(),
        .type_uuid = uuid(0xabcdef0123456789ULL, 1),
        .revision = 1,
    };
    input.normalized_name = "app.UserId";
    input.normalized_local_name = "UserId";
    TemplateNode root;
    root.kind = TemplateNodeKind::BuiltIn;
    root.atom = "UInt64";
    input.nodes.push_back(std::move(root));
    return TemplateChecker::checkAll({std::move(input)}).front();
}

InstantiatedTypeDescriptor::Ptr descriptor(const Definition::Ptr & definition)
{
    return InstantiatedTypeDescriptor::create(
        definition, CanonicalTypeArguments::validate(definition->getParameters(), {}), std::make_shared<DataTypeUInt64>());
}

BoundDeclaredTypeResult rootAlias(const Definition::Ptr & definition)
{
    const auto logical_descriptor = descriptor(definition);
    auto tree = BoundDeclaredTypeTree::build(
        {{.type_child_ordinals = {}, .physical_type = logical_descriptor->getPhysicalType()}},
        {{.type_child_ordinals = {}, .logical_descriptor = logical_descriptor, .logical_preorder = 0}},
        {definition});
    return BoundDeclaredTypeResult::withLogicalTree(std::move(tree));
}

PreparedViewOutputTypeBindings viewBindings(const Definition::Ptr & definition)
{
    std::vector<ViewOutputTypeBindingInput> outputs;
    outputs.push_back({"label", BoundDeclaredTypeResult::physicalOnly(std::make_shared<DataTypeString>())});
    outputs.push_back({"user_id", rootAlias(definition)});
    outputs.push_back({"backup_id", rootAlias(definition)});
    return prepareViewOutputTypeBindings(viewObject(), 1, outputs);
}

PreparedDictionaryAttributeTypeBindings dictionaryBindings(const Definition::Ptr & definition)
{
    std::vector<DictionaryAttributeTypeBindingInput> attributes;
    attributes.push_back({"key", BoundDeclaredTypeResult::physicalOnly(std::make_shared<DataTypeUInt64>())});
    attributes.push_back({"value", rootAlias(definition)});
    return prepareDictionaryAttributeTypeBindings(dictionaryObject(), 1, attributes);
}

ASTPtr parseCreate(const String & sql)
{
    ParserCreateQuery parser;
    return parseQuery(parser, sql, "Durable stored-object binding test", 0, 512, 1'000'000);
}

ASTCreateQuery & asCreate(const ASTPtr & ast)
{
    auto * create = ast ? ast->as<ASTCreateQuery>() : nullptr;
    if (!create)
        throw std::logic_error("test input is not a CREATE query");
    return *create;
}

Record definitionRecord(const Definition & definition)
{
    return makeRecord(
        definition,
        {
            .canonical_definition_sql = "ATTACH TYPE app.UserId AS UInt64",
            .canonical_physical_template_sql = "UInt64",
            .owner_uuid = uuid(0x9000, 1),
            .owner_display_name = "owner",
            .comment = "Durable stored-object binding fixture",
            .creation_time_us_utc = 123,
        });
}

SchemaObjectID definitionObject(const Definition & definition)
{
    return objectID(SchemaObjectKind::TypeDefinition, definition.getIdentity().database_uuid, definition.getIdentity().type_uuid);
}

AuthorityInventorySummary inventorySummary(std::span<const Record> records)
{
    std::vector<AuthorityInventoryLeaf> leaves;
    leaves.reserve(records.size());
    for (const auto & record : records)
    {
        leaves.push_back({
            .key = {
                .record_kind = AuthorityInventoryRecordKind::TypeDefinition,
                .object_uuid = record.identity.type_uuid,
            },
            .object_revision = record.identity.revision,
            .canonical_record_hash = computeRecordHash(record),
        });
    }
    std::sort(leaves.begin(), leaves.end(), authorityInventoryLeafLess);
    return buildAuthorityInventorySummary(leaves);
}

AuthorityRoot::Ptr buildRoot(const Definition::Ptr & definition)
{
    std::vector<Definition::Ptr> definitions{definition};
    std::vector<Record> records{definitionRecord(*definition)};
    const std::vector<SchemaObjectID> graph_nodes{definitionObject(*definition)};
    auto graph = SchemaObjectDependencyGraph::build(databaseUUID(), graph_nodes, {});
    const auto inventory = inventorySummary(records);
    auto state = makeAuthorityState(
        databaseUUID(),
        7,
        dependent_object_authority_capability_mask,
        inventory.leaf_count,
        inventory.merkle_radix_root,
        graph->computeRoot());
    return AuthorityRootBuilder::build(state, 3, definitions, records, {}, std::move(graph));
}

AuthorityAdapterPtr transientAuthority(const Definition::Ptr & definition)
{
    auto capabilities = atomicDatabaseAuthorityCapabilities();
    capabilities.mask &= ~typeAuthorityCapabilityBit(TypeAuthorityCapability::DurableAlias);
    return makeTransientAuthorityAdapter(databaseUUID(), capabilities, {definition});
}

template <typename Error, typename Callback>
void expectError(typename Error::Code code, Callback && callback)
{
    try
    {
        callback();
        FAIL() << "expected a stored-object binding component error";
    }
    catch (const Error & error)
    {
        EXPECT_EQ(error.code, code) << error.what();
    }
}

class UnusedPhysicalizationProvider final : public IPhysicalizationObjectProvider
{
public:
    PhysicalizationObject load(const SidecarExpectationRecord &) const override
    {
        throw std::logic_error("admission-only provider must not load an object");
    }
};

class UnusedPhysicalizationRewriteAdapter final : public IPhysicalizationRewriteAdapter
{
public:
    std::vector<PhysicalizationRewriteImage> prepareRewriteImages(const PhysicalizationPlan &) const override
    {
        throw std::logic_error("admission-only rewrite adapter must not prepare an image");
    }
};

StoredObjectPhysicalizationAdapterRegistry
makeRegistry(const IPhysicalizationObjectProvider & provider, const IPhysicalizationRewriteAdapter & rewrite_adapter)
{
    const std::array registrations{
        StoredObjectPhysicalizationAdapterRegistration{
            .object_kind = StoredObjectKind::View,
            .schema_object_kind = SchemaObjectKind::View,
            .source_modes = storedObjectSourceModeMask(StoredObjectSourceMode::AsSelect),
            .occurrence_sites = storedObjectOccurrenceSiteMask(StoredObjectOccurrenceSite::ViewOutputDeclaration),
            .object_provider = &provider,
            .rewrite_adapter = &rewrite_adapter,
        },
        StoredObjectPhysicalizationAdapterRegistration{
            .object_kind = StoredObjectKind::Dictionary,
            .schema_object_kind = SchemaObjectKind::Dictionary,
            .source_modes = storedObjectSourceModeMask(StoredObjectSourceMode::ObjectDefinition),
            .occurrence_sites = storedObjectOccurrenceSiteMask(StoredObjectOccurrenceSite::DictionaryAttribute),
            .object_provider = &provider,
            .rewrite_adapter = &rewrite_adapter,
        },
    };
    return StoredObjectPhysicalizationAdapterRegistry::create(registrations);
}

class ExactStoredObjectMetadataValidator final : public IStoredObjectUDTMetadataValidator
{
public:
    ExactStoredObjectMetadataValidator(String accepted_metadata_, String object_name_)
        : accepted_metadata(std::move(accepted_metadata_))
        , object_name(std::move(object_name_))
    {
    }

protected:
    DecodedMetadata decodeAndCanonicalize(
        std::string_view candidate_metadata_bytes,
        std::string_view canonical_sidecar_bytes,
        const StoredObjectUDTMetadataValidationLimits &) const override
    {
        if (candidate_metadata_bytes != accepted_metadata)
            throw std::runtime_error("metadata is not the exact accepted stored-object image");
        const auto references = decodePersistedTypeReferences(canonical_sidecar_bytes);
        return {
            .object = references.object,
            .object_schema_revision = references.object_schema_revision,
            .object_name = object_name,
            .sidecar_hash = computePersistedTypeReferencesSidecarHash(references),
            .physical_schema_fingerprint = references.physical_schema_fingerprint,
            .canonical_metadata_bytes = accepted_metadata,
        };
    }

private:
    String accepted_metadata;
    String object_name;
};

class DurableStorage final : public IDatabaseSchemaMutationDurableStorage
{
public:
    enum class Failure
    {
        None,
        Install,
        CommitAfter,
    };

    DurableStorage(AuthorityState current_state_, UInt64 predecessor_)
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
            throw std::runtime_error("invalid durable predecessor");
    }

    void markMutationRecoveryRequired(
        const DatabaseSchemaMutationGuard &, UInt64 transaction_id, DatabaseSchemaMutationDurabilityPhase phase) noexcept override
    {
        recovery_required = true;
        recovery_transaction_id = transaction_id;
        recovery_phase = phase;
    }

    void validateRecoveryGuard(const DatabaseSchemaMutationGuard &, UInt64 transaction_id) override
    {
        events.emplace_back("validate-recovery");
        if (!prepare || prepare->transaction_id != transaction_id)
            throw std::runtime_error("invalid recovery transaction");
    }

    void
    stageArtifact(const DatabaseSchemaWALStagedArtifactLocator &, const DatabaseSchemaWALStagedArtifactRef &, std::string_view) override
    {
        events.emplace_back("stage");
    }

    void finishStaging(UUID, UInt64) override { events.emplace_back("finish-staging"); }

    void persistPrepare(UInt64, std::string_view canonical_prepare) override
    {
        events.emplace_back("prepare");
        prepare = decodeDatabaseSchemaWALPrepare(canonical_prepare);
    }

    void installArtifact(const DatabaseSchemaWALStagedArtifactRef & artifact, std::string_view) override
    {
        events.emplace_back("install");
        if (failure == Failure::Install)
            throw std::runtime_error("install failed");
        installed_kinds.push_back(artifact.kind);
    }

    void removeArtifact(DatabaseSchemaWALStagedArtifactKind, const SchemaObjectID &) override { events.emplace_back("remove"); }

    void finishInstallation(UUID, UInt64) override { events.emplace_back("finish-installation"); }

    void persistCommit(UInt64, std::string_view canonical_commit) override
    {
        events.emplace_back("commit");
        commit = decodeDatabaseSchemaWALCommit(canonical_commit);
        if (!prepare)
            throw std::runtime_error("commit was written before Prepare");
        current_state = prepare->after_authority_state;
        if (failure == Failure::CommitAfter)
            throw std::runtime_error("durable Commit completion was not observed");
    }

    void finishRecovery(UUID, UInt64 transaction_id, DatabaseSchemaWALRecoveryDecision decision) override
    {
        events.emplace_back("finish-recovery");
        if (!prepare || prepare->transaction_id != transaction_id)
            throw std::runtime_error("invalid recovery completion");
        recovery_decision = decision;
        current_state = decision == DatabaseSchemaWALRecoveryDecision::CompleteCommitted ? prepare->after_authority_state
                                                                                         : *prepare->before_authority_state;
        recovery_required = false;
    }

    void discardUnpreparedStaging(const DatabaseSchemaMutationGuard &, UInt64) override
    {
        events.emplace_back("discard");
        discarded = true;
    }

    void retireRolledBackTransaction(const DatabaseSchemaMutationGuard &, UInt64) override
    {
        events.emplace_back("retire-rollback");
        retired_rollback = true;
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
    std::vector<DatabaseSchemaWALStagedArtifactKind> installed_kinds;
    std::optional<DatabaseSchemaWALPrepare> prepare;
    std::optional<DatabaseSchemaWALCommit> commit;
    std::optional<DatabaseSchemaWALRecoveryDecision> recovery_decision;
    bool discarded = false;
    bool retired_rollback = false;
    bool recovery_required = false;
    UInt64 recovery_transaction_id = 0;
    DatabaseSchemaMutationDurabilityPhase recovery_phase = DatabaseSchemaMutationDurabilityPhase::PrepareMarker;
};

class CountingPublicationObserver final : public IAtomicAuthorityPublicationObserver
{
public:
    class Transition final : public PreparedTransition
    {
    public:
        explicit Transition(CountingPublicationObserver & owner_)
            : owner(owner_)
        {
        }

        void publish() noexcept override { ++owner.publishes; }

    private:
        CountingPublicationObserver & owner;
    };

    std::unique_ptr<PreparedTransition> prepareAuthorityPublication(const AuthorityRoot &, const AuthorityRoot &) override
    {
        ++prepares;
        return std::make_unique<Transition>(*this);
    }

    UInt64 prepares = 0;
    UInt64 publishes = 0;
};

struct PublicationFixture
{
    static constexpr UInt64 predecessor = 8'000;
    static constexpr UInt64 transaction_id = predecessor + 1;

    Definition::Ptr definition = checkedAlias();
    AuthorityRoot::Ptr root = buildRoot(definition);
    UnusedPhysicalizationProvider provider;
    UnusedPhysicalizationRewriteAdapter rewrite_adapter;
    String metadata = "ATTACH VIEW app.v UUID '12345678-9abc-def0-0102-030405060708' "
                      "(label String, user_id UInt64, backup_id UInt64) AS SELECT '' AS label, 1 AS user_id, 2 AS backup_id";
    ExactStoredObjectMetadataValidator metadata_validator{metadata, "v"};

    StoredObjectPhysicalizationAdapterRegistry registry() const { return makeRegistry(provider, rewrite_adapter); }

    PreparedStoredObjectUDTPublicationCommit prepare(
        AtomicAuthority & authority,
        DurableStorage & storage,
        DatabaseSchemaMutationGuard & guard,
        bool corrupt_physical_fingerprint = false) const
    {
        auto bindings = viewBindings(definition);
        auto ast = parseCreate(
            "CREATE VIEW app.v UUID '12345678-9abc-def0-0102-030405060708' "
            "(label String, user_id UInt64, backup_id UInt64) AS "
            "SELECT '' AS label, 1 AS user_id, 2 AS backup_id");
        auto adapter_registry = registry();
        auto proof = authorizePreparedViewOutputTypeBindings(StoredObjectKind::View, asCreate(ast), bindings, adapter_registry, false);
        String canonical_sidecar = encodePersistedTypeReferences(*bindings.persisted_references);
        auto physical_schema = std::move(*bindings.bound_physical_schema);
        if (corrupt_physical_fingerprint)
            physical_schema.physical_schema_fingerprint.front() ^= 0xff;
        const auto expectation = *bindings.sidecar_expectation;
        auto planning_root = authority.acquireCurrentRoot();
        return StoredObjectUDTPublicationCoordinator::prepareCreateCommit(
            std::move(planning_root),
            authority,
            storage,
            guard,
            transaction_id,
            std::move(proof),
            std::move(physical_schema),
            metadata,
            std::move(canonical_sidecar),
            expectation,
            {},
            metadata_validator);
    }
};

AuthorityState currentState(const AtomicAuthority & authority)
{
    auto root = authority.acquireCurrentRoot();
    if (!root)
        throw std::logic_error("test authority has no current root");
    return root->getAuthorityState();
}

UInt64 countEvent(const DurableStorage & storage, std::string_view event)
{
    return static_cast<UInt64>(std::count(storage.events.begin(), storage.events.end(), event));
}

}

TEST(UDTStoredObjectBindingDurability, ViewExplicitBindingsEncodeDecodeReconstructAndBindExactly)
{
    const auto definition = checkedAlias();
    const auto prepared = viewBindings(definition);
    ASSERT_TRUE(prepared.persisted_references);
    ASSERT_TRUE(prepared.bound_physical_schema);
    ASSERT_TRUE(prepared.sidecar_expectation);

    const String encoded = encodePersistedTypeReferences(*prepared.persisted_references);
    const auto decoded = decodePersistedTypeReferences(encoded);
    EXPECT_EQ(encodePersistedTypeReferences(decoded), encoded);
    EXPECT_EQ(decoded.object, viewObject());
    ASSERT_EQ(decoded.occurrence_paths.size(), 2u);
    EXPECT_EQ(decoded.occurrence_paths[0].section, PersistedTypePathSection::ViewExpression);
    EXPECT_EQ(decoded.occurrence_paths[0].object_ordinal, 1u);
    EXPECT_EQ(decoded.occurrence_paths[1].object_ordinal, 2u);
    EXPECT_EQ(computePersistedTypeReferencesSidecarHash(decoded), prepared.sidecar_expectation->sidecar_hash);

    auto reconstructed = reconstructViewOutputPhysicalSchema(viewObject(), 1, prepared.physical_outputs, decoded);
    const auto authority = transientAuthority(definition);
    const auto bound = BoundObjectTypeReferences::bind(decoded, std::move(reconstructed), *authority);
    ASSERT_TRUE(bound);
    EXPECT_EQ(bound->getObject(), viewObject());
    EXPECT_EQ(bound->getSidecarHash(), prepared.sidecar_expectation->sidecar_hash);
    ASSERT_EQ(bound->getUses().size(), 2u);
    EXPECT_EQ(bound->getUses()[0].getRuntimeOwnerKey(), "user_id");
    EXPECT_EQ(bound->getUses()[1].getRuntimeOwnerKey(), "backup_id");
    EXPECT_EQ(bound->getUses()[0].getPhysicalType()->getName(), "UInt64");
    const auto lookup = bound->findUniqueRuntimeUse(PersistedTypePathSection::ViewExpression, "backup_id", {});
    ASSERT_NE(lookup.use, nullptr);
    EXPECT_FALSE(lookup.ambiguous);
    EXPECT_EQ(lookup.use->getPath(), decoded.occurrence_paths[1]);
}

TEST(UDTStoredObjectBindingDurability, DictionaryExplicitBindingsEncodeDecodeReconstructAndBindExactly)
{
    const auto definition = checkedAlias();
    const auto prepared = dictionaryBindings(definition);
    ASSERT_TRUE(prepared.persisted_references);
    ASSERT_TRUE(prepared.bound_physical_schema);
    ASSERT_TRUE(prepared.sidecar_expectation);

    const String encoded = encodePersistedTypeReferences(*prepared.persisted_references);
    const auto decoded = decodePersistedTypeReferences(encoded);
    EXPECT_EQ(encodePersistedTypeReferences(decoded), encoded);
    EXPECT_EQ(decoded.object, dictionaryObject());
    ASSERT_EQ(decoded.occurrence_paths.size(), 1u);
    EXPECT_EQ(decoded.occurrence_paths.front().section, PersistedTypePathSection::DictionaryAttribute);
    EXPECT_EQ(decoded.occurrence_paths.front().object_ordinal, 1u);
    EXPECT_EQ(computePersistedTypeReferencesSidecarHash(decoded), prepared.sidecar_expectation->sidecar_hash);

    auto reconstructed = reconstructDictionaryAttributePhysicalSchema(dictionaryObject(), 1, prepared.physical_attributes, decoded);
    const auto authority = transientAuthority(definition);
    const auto bound = BoundObjectTypeReferences::bind(decoded, std::move(reconstructed), *authority);
    ASSERT_TRUE(bound);
    EXPECT_EQ(bound->getObject(), dictionaryObject());
    ASSERT_EQ(bound->getUses().size(), 1u);
    EXPECT_EQ(bound->getUses().front().getRuntimeOwnerKey(), "value");
    const auto lookup = bound->findUniqueRuntimeUse(PersistedTypePathSection::DictionaryAttribute, "value", {});
    ASSERT_NE(lookup.use, nullptr);
    EXPECT_FALSE(lookup.ambiguous);
    EXPECT_EQ(lookup.use->getPhysicalType()->getName(), "UInt64");
}

TEST(UDTStoredObjectBindingDurability, ViewReconstructionRejectsWrongPathOrdinalFingerprintAndLimits)
{
    const auto definition = checkedAlias();
    const auto prepared = viewBindings(definition);
    ASSERT_TRUE(prepared.persisted_references);

    auto wrong_path = *prepared.persisted_references;
    wrong_path.occurrence_paths.front().type_child_ordinals = {0};
    expectError<ViewOutputTypeBindingError>(
        ViewOutputTypeBindingError::Code::PathMismatch,
        [&] { static_cast<void>(reconstructViewOutputPhysicalSchema(viewObject(), 1, prepared.physical_outputs, wrong_path)); });

    auto wrong_object_ordinal = *prepared.persisted_references;
    wrong_object_ordinal.occurrence_paths.back().object_ordinal = prepared.physical_outputs.size();
    expectError<ViewOutputTypeBindingError>(
        ViewOutputTypeBindingError::Code::PathMismatch,
        [&] { static_cast<void>(reconstructViewOutputPhysicalSchema(viewObject(), 1, prepared.physical_outputs, wrong_object_ordinal)); });

    auto wrong_occurrence_ordinal = *prepared.persisted_references;
    wrong_occurrence_ordinal.occurrence_paths.front().occurrence_ordinal = 1;
    expectError<ViewOutputTypeBindingError>(
        ViewOutputTypeBindingError::Code::PathMismatch,
        [&]
        { static_cast<void>(reconstructViewOutputPhysicalSchema(viewObject(), 1, prepared.physical_outputs, wrong_occurrence_ordinal)); });

    auto wrong_fingerprint = *prepared.persisted_references;
    wrong_fingerprint.physical_schema_fingerprint.front() ^= 0xff;
    expectError<ViewOutputTypeBindingError>(
        ViewOutputTypeBindingError::Code::PhysicalSchemaMismatch,
        [&] { static_cast<void>(reconstructViewOutputPhysicalSchema(viewObject(), 1, prepared.physical_outputs, wrong_fingerprint)); });

    ViewOutputTypeBindingLimits limits;
    limits.maximum_descriptor_occurrences = 1;
    expectError<ViewOutputTypeBindingError>(
        ViewOutputTypeBindingError::Code::LimitExceeded,
        [&]
        {
            static_cast<void>(
                reconstructViewOutputPhysicalSchema(viewObject(), 1, prepared.physical_outputs, *prepared.persisted_references, limits));
        });
}

TEST(UDTStoredObjectBindingDurability, DictionaryReconstructionRejectsForeignIdentityAndPhysicalFingerprint)
{
    const auto definition = checkedAlias();
    const auto prepared = dictionaryBindings(definition);
    ASSERT_TRUE(prepared.persisted_references);

    auto wrong_identity = *prepared.persisted_references;
    wrong_identity.object.object_uuid = uuid(0xdead, 0xbeef);
    expectError<DictionaryAttributeTypeBindingError>(
        DictionaryAttributeTypeBindingError::Code::SidecarMismatch,
        [&]
        {
            static_cast<void>(
                reconstructDictionaryAttributePhysicalSchema(dictionaryObject(), 1, prepared.physical_attributes, wrong_identity));
        });

    auto wrong_fingerprint = *prepared.persisted_references;
    wrong_fingerprint.physical_schema_fingerprint.front() ^= 0xff;
    expectError<DictionaryAttributeTypeBindingError>(
        DictionaryAttributeTypeBindingError::Code::PhysicalSchemaMismatch,
        [&]
        {
            static_cast<void>(
                reconstructDictionaryAttributePhysicalSchema(dictionaryObject(), 1, prepared.physical_attributes, wrong_fingerprint));
        });
}

TEST(UDTStoredObjectBindingDurability, PublicationPreparesCommitsAndPublishesOneExactReplacement)
{
    PublicationFixture fixture;
    AtomicAuthority authority(databaseUUID(), atomicDatabaseAuthorityCapabilities(), std::move(fixture.root));
    CountingPublicationObserver observer;
    authority.setPublicationObserver(&observer);
    DurableStorage storage(currentState(authority), PublicationFixture::predecessor);
    auto guard = DatabaseSchemaMutationGuard::issue(databaseUUID(), 41, PublicationFixture::predecessor);

    auto prepared = fixture.prepare(authority, storage, guard);
    EXPECT_EQ(storage.events, std::vector<String>({"validate"}));
    EXPECT_EQ(observer.prepares, 1u);
    EXPECT_EQ(observer.publishes, 0u);
    EXPECT_EQ(prepared.getTransactionID(), PublicationFixture::transaction_id);
    ASSERT_TRUE(prepared.getBoundUDTReferences());
    ASSERT_TRUE(prepared.getVerificationStamp());
    EXPECT_EQ(prepared.getBoundUDTReferences()->getObject(), viewObject());
    {
        auto before = authority.acquireCurrentRoot();
        ASSERT_TRUE(before);
        EXPECT_EQ(before->findExpectationRecord(viewObject()), nullptr);
    }

    auto durable = StoredObjectUDTPublicationCoordinator::commitPreparedCreateDurably(storage, guard, std::move(prepared));
    EXPECT_EQ(durable.getCommit().transaction_id, PublicationFixture::transaction_id);
    EXPECT_EQ(countEvent(storage, "commit"), 1u);
    EXPECT_EQ(observer.publishes, 0u);
    {
        auto before_publication = authority.acquireCurrentRoot();
        ASSERT_TRUE(before_publication);
        EXPECT_EQ(before_publication->findExpectationRecord(viewObject()), nullptr);
    }

    auto committed = StoredObjectUDTPublicationCoordinator::publishDurablyCommittedCreate(authority, std::move(durable));
    EXPECT_EQ(observer.prepares, 1u);
    EXPECT_EQ(observer.publishes, 1u);
    EXPECT_EQ(committed.getCommit().transaction_id, PublicationFixture::transaction_id);
    EXPECT_EQ(committed.getPersistedReferences().object, viewObject());
    EXPECT_EQ(committed.getExpectationRecord().sidecar_hash, committed.getBoundUDTReferences()->getSidecarHash());
    EXPECT_EQ(committed.getPackageStatistics().descriptors_validated, 1u);
    EXPECT_EQ(committed.getPackageStatistics().occurrences_validated, 2u);
    EXPECT_EQ(guard.getState(), DatabaseSchemaMutationGuard::State::Finished);
    {
        auto after = authority.acquireCurrentRoot();
        ASSERT_TRUE(after);
        const auto * expectation = after->findExpectationRecord(viewObject());
        ASSERT_NE(expectation, nullptr);
        EXPECT_EQ(*expectation, committed.getExpectationRecord());
        EXPECT_EQ(after->getDatabaseCatalogEpoch(), 8u);
    }
    authority.setPublicationObserver(nullptr);
}

TEST(UDTStoredObjectBindingDurability, MismatchIsRejectedBeforePrepareMarkerOrAuthorityPublication)
{
    PublicationFixture fixture;
    AtomicAuthority authority(databaseUUID(), atomicDatabaseAuthorityCapabilities(), std::move(fixture.root));
    DurableStorage storage(currentState(authority), PublicationFixture::predecessor);
    auto guard = DatabaseSchemaMutationGuard::issue(databaseUUID(), 42, PublicationFixture::predecessor);

    expectError<StoredObjectUDTPublicationPackageError>(
        StoredObjectUDTPublicationPackageError::Code::IntegrityMismatch,
        [&] { static_cast<void>(fixture.prepare(authority, storage, guard, true)); });
    EXPECT_TRUE(storage.events.empty());
    EXPECT_FALSE(storage.prepare);
    EXPECT_FALSE(storage.commit);
    EXPECT_EQ(guard.getState(), DatabaseSchemaMutationGuard::State::Ready);
    auto after = authority.acquireCurrentRoot();
    ASSERT_TRUE(after);
    EXPECT_EQ(after->getDatabaseCatalogEpoch(), 7u);
    EXPECT_EQ(after->findExpectationRecord(viewObject()), nullptr);
}

TEST(UDTStoredObjectBindingDurability, PreparedFailureRecoversByRollbackWithoutPublication)
{
    PublicationFixture fixture;
    AtomicAuthority authority(databaseUUID(), atomicDatabaseAuthorityCapabilities(), std::move(fixture.root));
    DurableStorage storage(currentState(authority), PublicationFixture::predecessor);
    auto guard = DatabaseSchemaMutationGuard::issue(databaseUUID(), 43, PublicationFixture::predecessor);
    auto prepared = fixture.prepare(authority, storage, guard);
    storage.failure = DurableStorage::Failure::Install;

    EXPECT_THROW(
        static_cast<void>(StoredObjectUDTPublicationCoordinator::commitPreparedCreateDurably(storage, guard, std::move(prepared))),
        DatabaseSchemaMutationIndeterminateDurabilityError);
    EXPECT_EQ(guard.getState(), DatabaseSchemaMutationGuard::State::RecoveryRequired);
    EXPECT_TRUE(storage.recovery_required);
    EXPECT_EQ(storage.recovery_phase, DatabaseSchemaMutationDurabilityPhase::AfterImage);
    EXPECT_FALSE(storage.commit);

    storage.failure = DurableStorage::Failure::None;
    auto recovered = StoredObjectUDTPublicationCoordinator::recoverPreparedCreateDurably(storage, guard, std::move(prepared), std::nullopt);
    EXPECT_FALSE(recovered);
    ASSERT_TRUE(storage.recovery_decision);
    EXPECT_EQ(*storage.recovery_decision, DatabaseSchemaWALRecoveryDecision::RollBackPrepared);
    EXPECT_TRUE(storage.retired_rollback);
    EXPECT_EQ(guard.getState(), DatabaseSchemaMutationGuard::State::Finished);
    auto after = authority.acquireCurrentRoot();
    ASSERT_TRUE(after);
    EXPECT_EQ(after->getDatabaseCatalogEpoch(), 7u);
    EXPECT_EQ(after->findExpectationRecord(viewObject()), nullptr);
}

TEST(UDTStoredObjectBindingDurability, DurableCommitCompletionFailureRecoversAndPublishesAfterImage)
{
    PublicationFixture fixture;
    AtomicAuthority authority(databaseUUID(), atomicDatabaseAuthorityCapabilities(), std::move(fixture.root));
    DurableStorage storage(currentState(authority), PublicationFixture::predecessor);
    auto guard = DatabaseSchemaMutationGuard::issue(databaseUUID(), 44, PublicationFixture::predecessor);
    auto prepared = fixture.prepare(authority, storage, guard);
    storage.failure = DurableStorage::Failure::CommitAfter;

    EXPECT_THROW(
        static_cast<void>(StoredObjectUDTPublicationCoordinator::commitPreparedCreateDurably(storage, guard, std::move(prepared))),
        DatabaseSchemaMutationIndeterminateDurabilityError);
    EXPECT_EQ(guard.getState(), DatabaseSchemaMutationGuard::State::RecoveryRequired);
    EXPECT_EQ(storage.recovery_phase, DatabaseSchemaMutationDurabilityPhase::CommitMarker);
    ASSERT_TRUE(storage.commit);
    EXPECT_EQ(storage.commit->transaction_id, PublicationFixture::transaction_id);
    {
        auto before_recovery = authority.acquireCurrentRoot();
        ASSERT_TRUE(before_recovery);
        EXPECT_EQ(before_recovery->findExpectationRecord(viewObject()), nullptr);
    }

    storage.failure = DurableStorage::Failure::None;
    auto recovered
        = StoredObjectUDTPublicationCoordinator::recoverPreparedCreateDurably(storage, guard, std::move(prepared), storage.commit);
    ASSERT_TRUE(recovered);
    ASSERT_TRUE(storage.recovery_decision);
    EXPECT_EQ(*storage.recovery_decision, DatabaseSchemaWALRecoveryDecision::CompleteCommitted);
    auto committed = StoredObjectUDTPublicationCoordinator::publishDurablyCommittedCreate(authority, std::move(*recovered));
    EXPECT_EQ(committed.getCommit().transaction_id, PublicationFixture::transaction_id);
    EXPECT_EQ(guard.getState(), DatabaseSchemaMutationGuard::State::Finished);
    auto after = authority.acquireCurrentRoot();
    ASSERT_TRUE(after);
    ASSERT_NE(after->findExpectationRecord(viewObject()), nullptr);
    EXPECT_EQ(*after->findExpectationRecord(viewObject()), committed.getExpectationRecord());
}

}
