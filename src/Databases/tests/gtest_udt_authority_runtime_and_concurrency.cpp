#include <Databases/UDT/AtomicAuthority.h>
#include <Databases/UDT/AuthorityQuarantinePlan.h>
#include <Databases/UDT/AuthorityVerificationRuntimeState.h>
#include <Databases/tests/UDTTestResourceImages.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/UDT/TemplateChecker.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

namespace DB::UDT
{
namespace
{

UUID runtimeUUID(UInt64 high, UInt64 low)
{
    UUID result{};
    UUIDHelpers::getHighBytes(result) = high;
    UUIDHelpers::getLowBytes(result) = low;
    return result;
}

Digest runtimeDigest(UInt8 seed)
{
    Digest result{};
    for (size_t index = 0; index < result.size(); ++index)
        result[index] = static_cast<UInt8>(seed + index);
    return result;
}

struct RuntimeQuarantineFixture
{
    UUID database_uuid = runtimeUUID(0x8210, 1);
    Definition::Ptr definition;
    SchemaObjectID definition_object;
    SchemaObjectID table;
    SchemaObjectID view;
    SchemaObjectID dictionary;
    SchemaObjectID unaffected;
    AuthorityRoot::Ptr root;
    AuthorityQuarantinePlan::Ptr quarantine;
    std::vector<SidecarExpectationRecord> expectations;

    RuntimeQuarantineFixture()
    {
        DefinitionInput input;
        input.identity = {.database_uuid = database_uuid, .type_uuid = runtimeUUID(0x8211, 1), .revision = 1};
        input.normalized_name = "authority_runtime.Value";
        input.normalized_local_name = "Value";
        TemplateNode node;
        node.kind = TemplateNodeKind::BuiltIn;
        node.atom = "UInt64";
        input.nodes.push_back(std::move(node));
        definition = TemplateChecker::checkAll({std::move(input)}).front();
        definition_object = {
            .kind = SchemaObjectKind::TypeDefinition,
            .database_uuid = database_uuid,
            .object_uuid = definition->getIdentity().type_uuid,
        };
        table = {.kind = SchemaObjectKind::Table, .database_uuid = database_uuid, .object_uuid = runtimeUUID(0x8212, 1)};
        view = {.kind = SchemaObjectKind::View, .database_uuid = database_uuid, .object_uuid = runtimeUUID(0x8212, 2)};
        dictionary = {
            .kind = SchemaObjectKind::Dictionary,
            .database_uuid = database_uuid,
            .object_uuid = runtimeUUID(0x8212, 3),
        };
        unaffected = {
            .kind = SchemaObjectKind::SyntheticTestObject,
            .database_uuid = database_uuid,
            .object_uuid = runtimeUUID(0x8212, 4),
        };

        struct ObjectReference
        {
            SchemaObjectID object;
            PersistedTypeReferences references;
        };
        std::vector<ObjectReference> object_references;
        const auto add_references = [&](const SchemaObjectID & object, PersistedTypePathSection section, UInt8 digest_seed)
        {
            object_references.push_back({
                object,
                Test::singleDefinitionPersistedTypeReferences(
                    object, 1, runtimeDigest(digest_seed), definition, std::make_shared<DataTypeUInt64>(), section),
            });
        };
        add_references(table, PersistedTypePathSection::ColumnType, 0x10);
        add_references(view, PersistedTypePathSection::ViewExpression, 0x20);
        add_references(dictionary, PersistedTypePathSection::DictionaryAttribute, 0x30);
        add_references(unaffected, PersistedTypePathSection::SyntheticPayload, 0x40);
        std::sort(
            object_references.begin(), object_references.end(), [](const auto & lhs, const auto & rhs) { return lhs.object < rhs.object; });

        std::vector<PersistedTypeReferences> references;
        references.reserve(object_references.size());
        expectations.reserve(object_references.size());
        for (auto & item : object_references)
        {
            expectations.push_back(Test::sidecarExpectationFor(item.references));
            references.push_back(std::move(item.references));
        }
        Test::DependentObjectResourceImageBatch images(expectations, Test::dependentObjectResourceImageInputs(references));

        const Record record = makeRecord(
            *definition,
            {
                .canonical_definition_sql = "CREATE TYPE authority_runtime.Value AS UInt64",
                .canonical_physical_template_sql = "UInt64",
                .owner_uuid = runtimeUUID(0x8213, 1),
                .owner_display_name = "owner",
                .comment = {},
                .creation_time_us_utc = 1,
            });
        std::vector<AuthorityInventoryLeaf> leaves{
            {
                .key = {
                    .record_kind = AuthorityInventoryRecordKind::TypeDefinition,
                    .object_uuid = definition->getIdentity().type_uuid,
                },
                .object_revision = 1,
                .canonical_record_hash = computeRecordHash(record),
            },
        };
        for (const auto & expectation : expectations)
        {
            leaves.push_back({
                .key = {
                    .record_kind = AuthorityInventoryRecordKind::SidecarExpectation,
                    .object_uuid = expectation.object.object_uuid,
                },
                .object_revision = expectation.object_schema_revision,
                .canonical_record_hash = computeSidecarExpectationRecordHash(expectation),
            });
        }
        std::sort(leaves.begin(), leaves.end(), authorityInventoryLeafLess);
        const auto inventory = buildAuthorityInventorySummary(leaves);

        std::vector<SchemaObjectID> graph_nodes{definition_object, table, view, dictionary, unaffected};
        std::vector<SchemaObjectDependencyEdge> graph_edges;
        for (const auto & object : std::array{table, view, dictionary, unaffected})
        {
            graph_edges.push_back({
                .dependent = object,
                .dependency = definition_object,
                .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
            });
        }
        graph_edges.push_back({
            .dependent = view,
            .dependency = table,
            .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnObject,
        });
        graph_edges.push_back({
            .dependent = dictionary,
            .dependency = view,
            .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnObject,
        });
        auto graph = SchemaObjectDependencyGraph::build(database_uuid, graph_nodes, graph_edges);
        const auto state = makeAuthorityState(
            database_uuid,
            1,
            dependent_object_authority_capability_mask,
            inventory.leaf_count,
            inventory.merkle_radix_root,
            graph->computeRoot());
        const std::array definitions{definition};
        const std::array records{record};
        root = AuthorityRootBuilder::buildInitialAdmission(
            state, 1, definitions, records, expectations, std::move(graph), AuthorityRootBuildLimits{}, images.get());
        quarantine = AuthorityQuarantinePlan::build(*root, {&table, 1});
    }

    const SidecarExpectationRecord & expectationFor(const SchemaObjectID & object) const
    {
        const auto found = std::ranges::find(expectations, object, &SidecarExpectationRecord::object);
        if (found == expectations.end())
            throw std::logic_error("missing runtime quarantine expectation");
        return *found;
    }

    AuthorityVerificationStamp::Ptr stampFor(
        const SchemaObjectID & object,
        std::span<const DefinitionIdentity> required_definitions,
        AuthorityRootIdentity root_identity = {}) const
    {
        if (root_identity.database_uuid == UUIDHelpers::Nil)
            root_identity = quarantine->getRoot().authority_root;
        const auto & expectation = expectationFor(object);
        const VerifiedAuthorityObjectIntegrity verification{
            .database_uuid = root_identity.database_uuid,
            .database_catalog_epoch = root_identity.database_catalog_epoch,
            .authority_anchor = root_identity.authority_anchor,
            .object = object,
            .object_schema_revision = expectation.object_schema_revision,
            .sidecar_hash = expectation.sidecar_hash,
            .physical_schema_fingerprint = expectation.physical_schema_fingerprint,
            .required_definitions_digest = computeVerifiedRequiredDefinitionsDigest(
                required_definitions, AuthorityVerificationStampLimits{}.maximum_required_definitions),
            .required_definition_count = static_cast<UInt64>(required_definitions.size()),
            .statistics = {},
        };
        return AuthorityVerificationStamp::create(verification, required_definitions, root_identity);
    }

    AuthorityObjectImageIdentity imageFor(const SchemaObjectID & object) const
    {
        const auto & expectation = expectationFor(object);
        return {
            .object = object,
            .object_schema_revision = expectation.object_schema_revision,
            .sidecar_hash = expectation.sidecar_hash,
            .physical_schema_fingerprint = expectation.physical_schema_fingerprint,
        };
    }
};

AuthorityRoot::Ptr makeRuntimeRoot(UUID database_uuid)
{
    const auto inventory = buildAuthorityInventorySummary({});
    auto graph = SchemaObjectDependencyGraph::createEmpty(database_uuid);
    const auto state = makeAuthorityState(
        database_uuid, 1, definition_authority_capability_mask, inventory.leaf_count, inventory.merkle_radix_root, graph->computeRoot());
    const std::vector<Definition::Ptr> definitions;
    const std::vector<Record> records;
    const std::vector<SidecarExpectationRecord> expectations;
    return AuthorityRootBuilder::buildInitialAdmission(state, 1, definitions, records, expectations, std::move(graph));
}

AuthorityVerificationRuntimeState makeRuntime(UUID database_uuid, const AuthorityVerificationRuntimeStateLimits & limits = {})
{
    return AuthorityVerificationRuntimeState(database_uuid, makeAuthorityVerificationScheduleCursor(database_uuid), limits);
}

AuthorityQuarantineOperationView newReadOperation()
{
    return {
        .kind = AuthorityQuarantineOperationKind::Read,
        .timing = AuthorityQuarantineOperationTiming::New,
        .pinned_root = {},
        .touch_set_is_complete = true,
        .sorted_unique_touched_objects = {},
        .continuation_proof_set_is_complete = true,
        .sorted_unique_continuation_proofs = {},
    };
}

template <typename Callback>
void expectRuntimeError(AuthorityVerificationRuntimeStateError::Code code, Callback && callback)
{
    try
    {
        callback();
        FAIL() << "Expected AuthorityVerificationRuntimeStateError";
    }
    catch (const AuthorityVerificationRuntimeStateError & error)
    {
        EXPECT_EQ(error.code, code);
    }
}

template <typename Predicate>
bool waitUntil(Predicate && predicate, std::chrono::steady_clock::duration timeout)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!predicate())
    {
        if (std::chrono::steady_clock::now() >= deadline)
            return predicate();
        std::this_thread::yield();
    }
    return true;
}

TEST(UDTAuthorityVerificationRuntime, InitialSnapshotIsExactAndPhysicalAdmissionIsNoThrow)
{
    const UUID database_uuid = runtimeUUID(0x8200, 1);
    auto runtime = makeRuntime(database_uuid);
    auto snapshot = runtime.acquireSnapshot();
    EXPECT_EQ(snapshot.getRevision(), 1);
    EXPECT_EQ(snapshot.getCursor(), makeAuthorityVerificationScheduleCursor(database_uuid));
    EXPECT_FALSE(snapshot.getQuarantine());
    EXPECT_FALSE(snapshot.isFailClosed());
    EXPECT_EQ(snapshot.getLastErrorKind(), AuthorityVerificationRuntimeLastErrorKind::None);

    const auto decision = runtime.decideOperation(newReadOperation());
    EXPECT_EQ(decision.status, AuthorityQuarantineAdmissionStatus::AllowedUnaffected);
    EXPECT_TRUE(decision.isAllowed());
}

TEST(UDTQuarantine, ReverseClosureIncludesTransitiveDependentsAndHonorsBounds)
{
    RuntimeQuarantineFixture fixture;
    ASSERT_NE(fixture.quarantine, nullptr);
    EXPECT_TRUE(fixture.quarantine->contains(fixture.table));
    EXPECT_TRUE(fixture.quarantine->contains(fixture.view));
    EXPECT_TRUE(fixture.quarantine->contains(fixture.dictionary));
    EXPECT_FALSE(fixture.quarantine->contains(fixture.definition_object));
    EXPECT_FALSE(fixture.quarantine->contains(fixture.unaffected));
    EXPECT_EQ(fixture.quarantine->getFailingSeeds().size(), 1);
    EXPECT_EQ(fixture.quarantine->getQuarantinedObjects().size(), 3);
    EXPECT_EQ(fixture.quarantine->getStatistics().closure_objects, 3);
    EXPECT_EQ(fixture.quarantine->getStatistics().reverse_edges_inspected, 2);

    AuthorityQuarantinePlanLimits limits;
    limits.maximum_closure_objects = 2;
    EXPECT_THROW(
        static_cast<void>(AuthorityQuarantinePlan::build(*fixture.root, {&fixture.table, 1}, limits)), AuthorityQuarantinePlanError);

    const std::array duplicate_seeds{fixture.table, fixture.table};
    EXPECT_THROW(static_cast<void>(AuthorityQuarantinePlan::build(*fixture.root, duplicate_seeds)), AuthorityQuarantinePlanError);
}

TEST(UDTQuarantine, AdmissionDecisionMatrixSeparatesUnaffectedNewAndPreexistingOperations)
{
    RuntimeQuarantineFixture fixture;
    const std::array affected{fixture.table};
    const std::array unaffected{fixture.unaffected};

    AuthorityQuarantineOperationView operation{
        .kind = AuthorityQuarantineOperationKind::Read,
        .timing = AuthorityQuarantineOperationTiming::New,
        .pinned_root = fixture.quarantine->getRoot(),
        .touch_set_is_complete = true,
        .sorted_unique_touched_objects = unaffected,
        .continuation_proof_set_is_complete = false,
        .sorted_unique_continuation_proofs = {},
    };
    EXPECT_EQ(
        decideAuthorityQuarantineAdmission(*fixture.quarantine, operation).status, AuthorityQuarantineAdmissionStatus::AllowedUnaffected);

    operation.sorted_unique_touched_objects = affected;
    for (const auto kind : {
             AuthorityQuarantineOperationKind::Read,
             AuthorityQuarantineOperationKind::Write,
             AuthorityQuarantineOperationKind::Mutation,
             AuthorityQuarantineOperationKind::DDL,
             AuthorityQuarantineOperationKind::Attach,
         })
    {
        operation.kind = kind;
        EXPECT_EQ(
            decideAuthorityQuarantineAdmission(*fixture.quarantine, operation).status,
            AuthorityQuarantineAdmissionStatus::NewOperationTouchesQuarantine);
    }

    operation.timing = AuthorityQuarantineOperationTiming::StartedBeforeQuarantine;
    operation.kind = AuthorityQuarantineOperationKind::Write;
    EXPECT_EQ(
        decideAuthorityQuarantineAdmission(*fixture.quarantine, operation).status,
        AuthorityQuarantineAdmissionStatus::NonReadContinuationRejected);
    operation.kind = AuthorityQuarantineOperationKind::Read;
    EXPECT_EQ(
        decideAuthorityQuarantineAdmission(*fixture.quarantine, operation).status,
        AuthorityQuarantineAdmissionStatus::IncompleteContinuationEvidence);

    operation.touch_set_is_complete = false;
    EXPECT_EQ(
        decideAuthorityQuarantineAdmission(*fixture.quarantine, operation).status, AuthorityQuarantineAdmissionStatus::IncompleteTouchSet);
    operation.touch_set_is_complete = true;
    operation.pinned_root.authority_root.authority_anchor = runtimeDigest(0xe0);
    EXPECT_EQ(
        decideAuthorityQuarantineAdmission(*fixture.quarantine, operation).status,
        AuthorityQuarantineAdmissionStatus::OperationRootMismatch);

    operation.pinned_root = fixture.quarantine->getRoot();
    operation.kind = static_cast<AuthorityQuarantineOperationKind>(0xff);
    EXPECT_EQ(
        decideAuthorityQuarantineAdmission(*fixture.quarantine, operation).status,
        AuthorityQuarantineAdmissionStatus::InvalidOperationKind);
    operation.kind = AuthorityQuarantineOperationKind::Read;
    operation.timing = static_cast<AuthorityQuarantineOperationTiming>(0xff);
    EXPECT_EQ(
        decideAuthorityQuarantineAdmission(*fixture.quarantine, operation).status,
        AuthorityQuarantineAdmissionStatus::InvalidOperationTiming);
}

TEST(UDTQuarantine, ExactReadContinuationRejectsEveryMismatchedOrNonCanonicalProof)
{
    RuntimeQuarantineFixture fixture;
    const std::array touched{fixture.table};
    const std::array required_definitions{fixture.definition->getIdentity()};
    const auto stamp = fixture.stampFor(fixture.table, required_definitions);
    AuthorityReadContinuationObjectProofView proof{
        .current_object = fixture.imageFor(fixture.table),
        .last_verification_stamp = stamp.get(),
        .sorted_unique_current_required_definitions = required_definitions,
    };
    std::array proofs{proof};
    AuthorityQuarantineOperationView operation{
        .kind = AuthorityQuarantineOperationKind::Read,
        .timing = AuthorityQuarantineOperationTiming::StartedBeforeQuarantine,
        .pinned_root = fixture.quarantine->getRoot(),
        .touch_set_is_complete = true,
        .sorted_unique_touched_objects = touched,
        .continuation_proof_set_is_complete = true,
        .sorted_unique_continuation_proofs = proofs,
    };
    EXPECT_EQ(
        decideAuthorityQuarantineAdmission(*fixture.quarantine, operation).status,
        AuthorityQuarantineAdmissionStatus::AllowedReadContinuation);

    proofs.front().last_verification_stamp = nullptr;
    EXPECT_EQ(
        decideAuthorityQuarantineAdmission(*fixture.quarantine, operation).status,
        AuthorityQuarantineAdmissionStatus::IncompleteContinuationEvidence);
    proofs.front().last_verification_stamp = stamp.get();

    proofs.front().current_object.object_schema_revision += 1;
    EXPECT_EQ(
        decideAuthorityQuarantineAdmission(*fixture.quarantine, operation).status,
        AuthorityQuarantineAdmissionStatus::ContinuationStampMismatch);
    proofs.front().current_object = fixture.imageFor(fixture.table);

    auto alternate_root = fixture.quarantine->getRoot().authority_root;
    alternate_root.authority_anchor = runtimeDigest(0xf0);
    const auto alternate_stamp = fixture.stampFor(fixture.table, required_definitions, alternate_root);
    proofs.front().last_verification_stamp = alternate_stamp.get();
    EXPECT_EQ(
        decideAuthorityQuarantineAdmission(*fixture.quarantine, operation).status,
        AuthorityQuarantineAdmissionStatus::ContinuationStampMismatch);
    proofs.front().last_verification_stamp = stamp.get();

    const std::array different_definitions{DefinitionIdentity{
        .database_uuid = fixture.database_uuid,
        .type_uuid = runtimeUUID(0x8211, 2),
        .revision = 1,
    }};
    proofs.front().sorted_unique_current_required_definitions = different_definitions;
    EXPECT_EQ(
        decideAuthorityQuarantineAdmission(*fixture.quarantine, operation).status,
        AuthorityQuarantineAdmissionStatus::ContinuationDependencyMismatch);

    const std::array duplicate_definitions{required_definitions.front(), required_definitions.front()};
    proofs.front().sorted_unique_current_required_definitions = duplicate_definitions;
    EXPECT_EQ(
        decideAuthorityQuarantineAdmission(*fixture.quarantine, operation).status,
        AuthorityQuarantineAdmissionStatus::NonCanonicalContinuationEvidence);

    const std::array two_definitions{
        required_definitions.front(),
        DefinitionIdentity{
            .database_uuid = fixture.database_uuid,
            .type_uuid = runtimeUUID(0x8211, 2),
            .revision = 1,
        },
    };
    proofs.front().sorted_unique_current_required_definitions = two_definitions;
    AuthorityQuarantineAdmissionLimits limits;
    limits.maximum_required_definitions_per_proof = 1;
    EXPECT_EQ(
        decideAuthorityQuarantineAdmission(*fixture.quarantine, operation, limits).status,
        AuthorityQuarantineAdmissionStatus::LimitExceeded);

    proofs.front().sorted_unique_current_required_definitions = required_definitions;
    limits = {};
    limits.maximum_evidence_canonical_bytes = 1;
    EXPECT_EQ(
        decideAuthorityQuarantineAdmission(*fixture.quarantine, operation, limits).status,
        AuthorityQuarantineAdmissionStatus::LimitExceeded);

    limits = {};
    limits.maximum_touched_objects = 0;
    EXPECT_EQ(
        decideAuthorityQuarantineAdmission(*fixture.quarantine, operation, limits).status,
        AuthorityQuarantineAdmissionStatus::InvalidConfiguration);
}

TEST(UDTQuarantine, TouchAndContinuationSetsMustBeStrictlyCanonicalAndComplete)
{
    RuntimeQuarantineFixture fixture;
    const std::array duplicate_touches{fixture.table, fixture.table};
    AuthorityQuarantineOperationView operation{
        .kind = AuthorityQuarantineOperationKind::Read,
        .timing = AuthorityQuarantineOperationTiming::New,
        .pinned_root = fixture.quarantine->getRoot(),
        .touch_set_is_complete = true,
        .sorted_unique_touched_objects = duplicate_touches,
        .continuation_proof_set_is_complete = true,
        .sorted_unique_continuation_proofs = {},
    };
    EXPECT_EQ(
        decideAuthorityQuarantineAdmission(*fixture.quarantine, operation).status,
        AuthorityQuarantineAdmissionStatus::NonCanonicalTouchSet);

    const std::array touched{fixture.table};
    const std::array required_definitions{fixture.definition->getIdentity()};
    const auto stamp = fixture.stampFor(fixture.view, required_definitions);
    const std::array wrong_object_proof{AuthorityReadContinuationObjectProofView{
        .current_object = fixture.imageFor(fixture.view),
        .last_verification_stamp = stamp.get(),
        .sorted_unique_current_required_definitions = required_definitions,
    }};
    operation.timing = AuthorityQuarantineOperationTiming::StartedBeforeQuarantine;
    operation.sorted_unique_touched_objects = touched;
    operation.sorted_unique_continuation_proofs = wrong_object_proof;
    EXPECT_EQ(
        decideAuthorityQuarantineAdmission(*fixture.quarantine, operation).status,
        AuthorityQuarantineAdmissionStatus::IncompleteContinuationEvidence);

    operation.continuation_proof_set_is_complete = false;
    EXPECT_EQ(
        decideAuthorityQuarantineAdmission(*fixture.quarantine, operation).status,
        AuthorityQuarantineAdmissionStatus::IncompleteContinuationEvidence);
}

TEST(UDTAuthorityVerificationRuntime, HazardExhaustionFailsClosedAndShutdownIsSticky)
{
    const UUID database_uuid = runtimeUUID(0x8200, 2);
    AuthorityVerificationRuntimeStateLimits limits;
    limits.hazard_slot_count = 1;
    limits.maximum_retired_snapshot_count = 2;
    auto runtime = makeRuntime(database_uuid, limits);

    std::optional<AuthorityVerificationRuntimeState::Snapshot> only_slot;
    only_slot.emplace(runtime.acquireSnapshot());
    expectRuntimeError(
        AuthorityVerificationRuntimeStateError::Code::HazardSlotsExhausted, [&] { static_cast<void>(runtime.acquireSnapshot()); });
    EXPECT_EQ(runtime.decideOperation(newReadOperation()).status, AuthorityQuarantineAdmissionStatus::RuntimeFailClosed);

    only_slot.reset();
    runtime.shutdownAndDrain();
    EXPECT_TRUE(runtime.isShutdown());
    expectRuntimeError(AuthorityVerificationRuntimeStateError::Code::Shutdown, [&] { static_cast<void>(runtime.acquireSnapshot()); });
    EXPECT_EQ(runtime.decideOperation(newReadOperation()).status, AuthorityQuarantineAdmissionStatus::RuntimeFailClosed);
}

TEST(UDTAuthorityVerificationRuntime, InvalidCursorAndBoundDomainsFailClosedAtConstruction)
{
    const UUID database_uuid = runtimeUUID(0x8200, 3);
    auto cursor = makeAuthorityVerificationScheduleCursor(database_uuid);
    cursor.database_uuid = runtimeUUID(0x8200, 4);
    expectRuntimeError(
        AuthorityVerificationRuntimeStateError::Code::InvalidCursor,
        [&]
        {
            AuthorityVerificationRuntimeState invalid_runtime(database_uuid, cursor);
            static_cast<void>(invalid_runtime);
        });

    AuthorityVerificationRuntimeStateLimits no_slots;
    no_slots.hazard_slot_count = 0;
    expectRuntimeError(
        AuthorityVerificationRuntimeStateError::Code::InvalidConfiguration,
        [&]
        {
            AuthorityVerificationRuntimeState invalid_runtime(
                database_uuid, makeAuthorityVerificationScheduleCursor(database_uuid), no_slots);
            static_cast<void>(invalid_runtime);
        });
}

TEST(UDTAuthorityVerificationRuntime, RootAndRuntimeReadersRemainCoherentAcrossContentNeutralPublication)
{
    const UUID database_uuid = runtimeUUID(0x8200, 5);
    AtomicAuthority authority(database_uuid, atomicDatabaseAuthorityCapabilities(), makeRuntimeRoot(database_uuid));
    auto runtime = makeRuntime(database_uuid);
    authority.setPublicationObserver(&runtime);

    std::optional<AtomicAuthority::RootSnapshot> old_root;
    old_root.emplace(authority.acquireCurrentRoot());
    std::optional<AuthorityVerificationRuntimeState::Snapshot> old_runtime;
    old_runtime.emplace(runtime.acquireSnapshot());
    const UInt64 old_epoch = old_root->get().getDatabaseCatalogEpoch();
    const UInt64 old_runtime_revision = old_runtime->getRevision();

    AuthorityRoot::Ptr replacement = old_root->get().cloneForExactRepair();
    auto prepared = authority.preparePublication(std::move(replacement));
    authority.publish(std::move(prepared));

    auto current_root = authority.acquireCurrentRoot();
    auto current_runtime = runtime.acquireSnapshot();
    EXPECT_EQ(current_root->getDatabaseCatalogEpoch(), old_epoch + 1);
    EXPECT_EQ(old_root->get().getDatabaseCatalogEpoch(), old_epoch);
    EXPECT_EQ(current_runtime.getRevision(), old_runtime_revision);
    EXPECT_FALSE(current_runtime.getQuarantine());
    EXPECT_EQ(authority.scanRetired().retired_root_count, 1);

    old_root.reset();
    EXPECT_EQ(authority.scanRetired().retired_root_count, 0);
    old_runtime.reset();
    runtime.scanRetired();
    authority.setPublicationObserver(nullptr);
}

TEST(UDTAuthorityVerificationRuntime, ConcurrentAdmissionAndSnapshotAcquisitionNeverBypassShutdown)
{
    const UUID database_uuid = runtimeUUID(0x8200, 6);
    auto runtime = makeRuntime(database_uuid);
    std::atomic<bool> stop = false;
    std::atomic<UInt64> allowed = 0;
    std::atomic<UInt64> fail_closed = 0;
    std::atomic<UInt64> snapshots_acquired = 0;
    std::atomic<UInt64> snapshots_fail_closed = 0;
    std::atomic<UInt64> snapshots_rejected_at_shutdown = 0;
    std::atomic<UInt64> unexpected = 0;
    std::vector<std::thread> readers;
    readers.reserve(8);
    for (std::size_t index = 0; index < 8; ++index)
    {
        readers.emplace_back(
            [&]
            {
                while (!stop.load(std::memory_order_acquire))
                {
                    try
                    {
                        auto snapshot = runtime.acquireSnapshot();
                        if (snapshot.isFailClosed())
                            snapshots_fail_closed.fetch_add(1, std::memory_order_relaxed);
                        else if (snapshot.getRevision() == 1 && !snapshot.getQuarantine())
                            snapshots_acquired.fetch_add(1, std::memory_order_relaxed);
                        else
                            unexpected.fetch_add(1, std::memory_order_relaxed);
                    }
                    catch (const AuthorityVerificationRuntimeStateError & error)
                    {
                        if (error.code == AuthorityVerificationRuntimeStateError::Code::Shutdown)
                            snapshots_rejected_at_shutdown.fetch_add(1, std::memory_order_relaxed);
                        else
                            unexpected.fetch_add(1, std::memory_order_relaxed);
                    }
                    catch (...)
                    {
                        unexpected.fetch_add(1, std::memory_order_relaxed);
                    }

                    const auto decision = runtime.decideOperation(newReadOperation());
                    if (decision.status == AuthorityQuarantineAdmissionStatus::AllowedUnaffected)
                        allowed.fetch_add(1, std::memory_order_relaxed);
                    else if (decision.status == AuthorityQuarantineAdmissionStatus::RuntimeFailClosed)
                        fail_closed.fetch_add(1, std::memory_order_relaxed);
                    else
                        unexpected.fetch_add(1, std::memory_order_relaxed);
                }
            });
    }

    const bool exercised_before_shutdown = waitUntil(
        [&] { return allowed.load(std::memory_order_acquire) >= 100 && snapshots_acquired.load(std::memory_order_acquire) >= 100; },
        std::chrono::seconds(5));
    runtime.shutdownAndDrain();
    const bool exercised_after_shutdown = waitUntil(
        [&]
        { return fail_closed.load(std::memory_order_acquire) > 0 && snapshots_rejected_at_shutdown.load(std::memory_order_acquire) > 0; },
        std::chrono::seconds(5));
    for (std::size_t index = 0; index < 100; ++index)
    {
        EXPECT_EQ(runtime.decideOperation(newReadOperation()).status, AuthorityQuarantineAdmissionStatus::RuntimeFailClosed);
        fail_closed.fetch_add(1, std::memory_order_relaxed);
    }
    stop.store(true, std::memory_order_release);
    for (auto & reader : readers)
        reader.join();

    EXPECT_TRUE(exercised_before_shutdown);
    EXPECT_TRUE(exercised_after_shutdown);
    EXPECT_GT(allowed.load(std::memory_order_relaxed), 0);
    EXPECT_GT(fail_closed.load(std::memory_order_relaxed), 0);
    EXPECT_GT(snapshots_acquired.load(std::memory_order_relaxed), 0);
    EXPECT_GT(snapshots_fail_closed.load(std::memory_order_relaxed) + snapshots_rejected_at_shutdown.load(std::memory_order_relaxed), 0);
    EXPECT_EQ(unexpected.load(std::memory_order_relaxed), 0);
}

}
}
