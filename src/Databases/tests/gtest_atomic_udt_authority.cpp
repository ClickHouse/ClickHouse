#include <Databases/UDT/AtomicAuthority.h>
#include <Databases/UDT/ILifecycleAdapter.h>

#include <DataTypes/UDT/AuthorityInventory.h>
#include <DataTypes/UDT/Record.h>
#include <DataTypes/UDT/TemplateChecker.h>

#include <Common/Exception.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
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

TypeAuthorityCapabilities atomicCapabilities()
{
    return atomicDatabaseAuthorityCapabilities();
}

struct DefinitionSpec
{
    std::string_view local_name;
    UInt64 type_tag;
    std::string_view built_in_type = "UInt64";
    std::string_view comment;
};

DefinitionInput definitionInput(UUID database_uuid, const DefinitionSpec & spec)
{
    DefinitionInput input;
    input.identity = {.database_uuid = database_uuid, .type_uuid = testUUID(0x1000, spec.type_tag), .revision = 1};
    input.normalized_name = "db." + String(spec.local_name);
    input.normalized_local_name = spec.local_name;
    TemplateNode node;
    node.kind = TemplateNodeKind::BuiltIn;
    node.atom = spec.built_in_type;
    input.nodes.push_back(std::move(node));
    return input;
}

Record definitionRecord(const Definition & definition, const DefinitionSpec & spec)
{
    return makeRecord(
        definition,
        {
            .canonical_definition_sql = "CREATE TYPE " + definition.getNormalizedName() + " AS " + String(spec.built_in_type),
            .canonical_physical_template_sql = String(spec.built_in_type),
            .owner_uuid = testUUID(0x9000, 1),
            .owner_display_name = "owner",
            .comment = String(spec.comment),
            .creation_time_us_utc = 1,
        });
}

AuthorityRoot::Ptr makeRoot(
    UUID database_uuid,
    UInt64 database_catalog_epoch,
    UInt64 type_index_generation,
    UInt64 persistent_capability_mask,
    std::span<const DefinitionSpec> specs)
{
    std::vector<DefinitionInput> inputs;
    inputs.reserve(specs.size());
    for (const auto & spec : specs)
        inputs.push_back(definitionInput(database_uuid, spec));
    auto definitions = TemplateChecker::checkAll(inputs);

    std::vector<Record> records;
    std::vector<AuthorityInventoryLeaf> leaves;
    std::vector<SchemaObjectID> graph_nodes;
    records.reserve(definitions.size());
    leaves.reserve(definitions.size());
    graph_nodes.reserve(definitions.size());
    for (std::size_t index = 0; index < definitions.size(); ++index)
    {
        records.push_back(definitionRecord(*definitions[index], specs[index]));
        leaves.push_back({
            .key = {
                .record_kind = AuthorityInventoryRecordKind::TypeDefinition,
                .object_uuid = records.back().identity.type_uuid,
            },
            .object_revision = records.back().identity.revision,
            .canonical_record_hash = computeRecordHash(records.back()),
        });
        graph_nodes.push_back({
            .kind = SchemaObjectKind::TypeDefinition,
            .database_uuid = database_uuid,
            .object_uuid = records.back().identity.type_uuid,
        });
    }

    std::sort(leaves.begin(), leaves.end(), authorityInventoryLeafLess);
    const auto inventory = buildAuthorityInventorySummary(leaves);
    auto graph = SchemaObjectDependencyGraph::build(database_uuid, std::move(graph_nodes), {});
    const auto state = makeAuthorityState(
        database_uuid,
        database_catalog_epoch,
        persistent_capability_mask,
        inventory.leaf_count,
        inventory.merkle_radix_root,
        graph->computeRoot());
    const std::vector<SidecarExpectationRecord> expectations;
    return AuthorityRootBuilder::build(state, type_index_generation, definitions, records, expectations, std::move(graph));
}

AuthorityRoot::Ptr makeRoot(
    UUID database_uuid,
    UInt64 database_catalog_epoch,
    UInt64 type_index_generation,
    UInt64 persistent_capability_mask,
    std::string_view local_name = {},
    UInt64 type_tag = 1,
    std::string_view comment = {},
    std::string_view built_in_type = "UInt64")
{
    if (local_name.empty())
        return makeRoot(
            database_uuid, database_catalog_epoch, type_index_generation, persistent_capability_mask, std::span<const DefinitionSpec>{});
    const DefinitionSpec spec{
        .local_name = local_name,
        .type_tag = type_tag,
        .built_in_type = built_in_type,
        .comment = comment,
    };
    return makeRoot(database_uuid, database_catalog_epoch, type_index_generation, persistent_capability_mask, std::span(&spec, 1));
}

template <typename Callback>
void expectAuthorityError(AtomicAuthorityError::Code code, Callback && callback)
{
    try
    {
        callback();
        FAIL() << "Expected AtomicAuthorityError";
    }
    catch (const AtomicAuthorityError & error)
    {
        EXPECT_EQ(error.code, code);
    }
}

template <typename Callback>
void expectRootError(AuthorityRootError::Code code, Callback && callback)
{
    try
    {
        callback();
        FAIL() << "Expected AuthorityRootError";
    }
    catch (const AuthorityRootError & error)
    {
        EXPECT_EQ(error.code, code);
    }
}

TEST(AtomicAuthority, DatabaseAtomicProfileIsTheReviewedFullTier)
{
    constexpr auto capabilities = atomicDatabaseAuthorityCapabilities();
    EXPECT_EQ(capabilities.adapter_abi, 1);
    EXPECT_TRUE(capabilities.contains(TypeAuthorityCapability::TransientResolution));
    EXPECT_TRUE(capabilities.contains(TypeAuthorityCapability::DurableAlias));
    EXPECT_TRUE(capabilities.contains(TypeAuthorityCapability::Limits));
    EXPECT_TRUE(capabilities.contains(TypeAuthorityCapability::Templates));
    EXPECT_TRUE(capabilities.contains(TypeAuthorityCapability::DecreasingRecursion));
    EXPECT_FALSE(capabilities.contains(TypeAuthorityCapability::Policy));
    EXPECT_FALSE(capabilities.contains(TypeAuthorityCapability::Replication));
    EXPECT_FALSE(capabilities.contains(TypeAuthorityCapability::BackupRecovery));
    EXPECT_FALSE(capabilities.contains(TypeAuthorityCapability::Manifest));
    EXPECT_EQ(capabilities.limits.maximum_definitions, 100'000);
    EXPECT_EQ(capabilities.limits.maximum_definition_bytes, 256ULL << 10);
    EXPECT_EQ(capabilities.limits.maximum_template_nodes, 4'096);
    EXPECT_EQ(capabilities.limits.maximum_direct_dependencies, 256);
    EXPECT_EQ(capabilities.limits.maximum_transitive_dependencies, 1'024);
    EXPECT_EQ(capabilities.limits.maximum_checker_work, 65'536);
}

TEST(AtomicAuthority, InitializedEmptySnapshotIsDeterministic)
{
    const UUID database_uuid = testUUID(1, 2);
    AtomicAuthority authority(database_uuid, atomicCapabilities());
    EXPECT_FALSE(authority.acquireCurrentRoot());

    auto session = authority.beginResolutionSession();
    EXPECT_EQ(session.getGeneration(), 0);
    EXPECT_FALSE(session.findByName("missing"));
    EXPECT_FALSE(session.findByIdentity({.database_uuid = database_uuid, .type_uuid = testUUID(3, 4), .revision = 1}));
    EXPECT_FALSE(authority.isFirstPublicationReadyForActivation());
    EXPECT_EQ(authority.getRetirementState(), AtomicAuthorityRetirementState{});
}

TEST(AtomicAuthority, StaticEmptySessionMayOutliveTheAuthority)
{
    const UUID database_uuid = testUUID(3, 4);
    std::optional<IAuthorityAdapter::ResolutionSession> session;
    {
        auto authority = std::make_unique<AtomicAuthority>(database_uuid, atomicCapabilities());
        session.emplace(authority->beginResolutionSession());
        authority.reset();
    }

    EXPECT_EQ(session->getGeneration(), 0);
    EXPECT_FALSE(session->findByName("missing"));
}

TEST(AtomicAuthority, InitialPublicationAndReplacementUseOneCompositeSnapshot)
{
    const UUID database_uuid = testUUID(10, 20);
    AtomicAuthority authority(database_uuid, atomicCapabilities());

    auto initial = makeRoot(database_uuid, 1, 1, definition_authority_capability_mask, "Alpha", 1);
    const auto alpha_identity = initial->getDefinitionRecords().front().identity;
    auto initial_publication = authority.preparePublication(std::move(initial));
    authority.publish(std::move(initial_publication));
    EXPECT_TRUE(authority.isFirstPublicationReadyForActivation());

    auto old_session = authority.beginResolutionSession();
    ASSERT_TRUE(old_session.findByName("Alpha"));
    EXPECT_EQ(old_session.getGeneration(), 1);

    auto replacement = makeRoot(database_uuid, 2, 2, definition_authority_capability_mask, "Beta", 2);
    const auto beta_identity = replacement->getDefinitionRecords().front().identity;
    auto publication = authority.preparePublication(std::move(replacement));
    authority.publish(std::move(publication));
    EXPECT_FALSE(authority.isFirstPublicationReadyForActivation());

    auto new_session = authority.beginResolutionSession();
    EXPECT_EQ(new_session.getGeneration(), 2);
    EXPECT_TRUE(new_session.findByIdentity(beta_identity));
    EXPECT_FALSE(new_session.findByIdentity(alpha_identity));
    EXPECT_TRUE(new_session.findByName("Beta"));

    EXPECT_EQ(old_session.getGeneration(), 1);
    EXPECT_TRUE(old_session.findByIdentity(alpha_identity));
    EXPECT_FALSE(old_session.findByIdentity(beta_identity));
    EXPECT_TRUE(old_session.findByName("Alpha"));
    EXPECT_EQ(authority.getRetirementState().retired_root_count, 1);
}

TEST(AtomicAuthority, LifecycleProjectionStaysBoundToItsHazardedRootAfterPublication)
{
    const UUID database_uuid = testUUID(31, 32);
    AtomicAuthority authority(
        database_uuid,
        atomicCapabilities(),
        makeRoot(database_uuid, 1, 1, definition_authority_capability_mask, "Stable", 7, {}, "UInt64"));

    auto old_snapshot = authority.acquireLifecycleSnapshot();
    ASSERT_EQ(old_snapshot->getDefinitionRecords().size(), 1);
    const DefinitionIdentity identity = old_snapshot->getDefinitionRecords().front().identity;
    const auto old_projection_before = old_snapshot->getMonomorphicProjection(identity);
    ASSERT_TRUE(old_projection_before);
    EXPECT_EQ(old_projection_before->canonical_physical_type, "UInt64");
    EXPECT_EQ(old_snapshot->findCheckedDefinitionByIdentity(identity)->getIdentity(), identity);

    auto publication
        = authority.preparePublication(makeRoot(database_uuid, 2, 2, definition_authority_capability_mask, "Stable", 7, {}, "UInt32"));
    authority.publish(std::move(publication));

    const auto old_projection_after = old_snapshot->getMonomorphicProjection(identity);
    ASSERT_TRUE(old_projection_after);
    EXPECT_EQ(old_projection_after, old_projection_before);

    auto new_snapshot = authority.acquireLifecycleSnapshot();
    const auto new_projection = new_snapshot->getMonomorphicProjection(identity);
    ASSERT_TRUE(new_projection);
    EXPECT_EQ(new_projection->canonical_physical_type, "UInt32");
    EXPECT_NE(new_projection->storage_fingerprint, old_projection_after->storage_fingerprint);
}

TEST(AtomicAuthority, ConstructorAcceptsARecoveredDependentObjectRootAtItsPersistedEpoch)
{
    const UUID database_uuid = testUUID(41, 42);
    auto recovered = makeRoot(database_uuid, 37, 12, dependent_object_authority_capability_mask, "Recovered", 14);
    AtomicAuthority authority(database_uuid, atomicCapabilities(), std::move(recovered));

    auto current = authority.acquireCurrentRoot();
    ASSERT_TRUE(current);
    EXPECT_EQ(current->getDatabaseCatalogEpoch(), 37);
    EXPECT_EQ(current->getPersistentCapabilityMask(), dependent_object_authority_capability_mask);
    auto session = authority.beginResolutionSession();
    EXPECT_EQ(session.getGeneration(), 12);
    EXPECT_TRUE(session.findByName("Recovered"));
}

TEST(AtomicAuthority, ContentNeutralChangesMayKeepTheTypeIndexGeneration)
{
    const UUID database_uuid = testUUID(50, 60);
    auto initial = makeRoot(database_uuid, 1, 9, definition_authority_capability_mask, "Stable", 4, "before");
    AtomicAuthority authority(database_uuid, atomicCapabilities(), std::move(initial));

    auto changed_comment = makeRoot(database_uuid, 2, 9, definition_authority_capability_mask, "Stable", 4, "after");
    auto comment_publication = authority.preparePublication(std::move(changed_comment));
    authority.publish(std::move(comment_publication));

    {
        auto current = authority.acquireCurrentRoot();
        ASSERT_TRUE(current);
        EXPECT_EQ(current->getTypeIndexGeneration(), 9);
        ASSERT_EQ(current->getDefinitionRecords().size(), 1);
        EXPECT_EQ(current->getDefinitionRecords().front().comment, "after");
    }

    AuthorityRoot::Ptr activated;
    {
        auto current = authority.acquireCurrentRoot();
        ASSERT_TRUE(current);
        activated = current->cloneWithAuthorityState(activateDependentObjectAuthority(current->getAuthorityState()));
    }
    auto activation_publication = authority.preparePublication(std::move(activated));
    authority.publish(std::move(activation_publication));

    auto activated_root = authority.acquireCurrentRoot();
    ASSERT_TRUE(activated_root);
    EXPECT_EQ(activated_root->getPersistentCapabilityMask(), dependent_object_authority_capability_mask);
    EXPECT_EQ(activated_root->getTypeIndexGeneration(), 9);
}

TEST(AtomicAuthority, PreparationRejectsInvalidRootTransitions)
{
    const UUID database_uuid = testUUID(70, 80);
    const UUID other_database_uuid = testUUID(71, 81);
    auto initial = makeRoot(database_uuid, 1, 5, definition_authority_capability_mask, "First", 5);
    AtomicAuthority authority(database_uuid, atomicCapabilities(), std::move(initial));

    expectAuthorityError(AtomicAuthorityError::Code::InvalidConfiguration, [&] { static_cast<void>(authority.preparePublication({})); });
    expectAuthorityError(
        AtomicAuthorityError::Code::DatabaseMismatch,
        [&]
        {
            static_cast<void>(
                authority.preparePublication(makeRoot(other_database_uuid, 2, 6, definition_authority_capability_mask, "Other", 6)));
        });
    expectAuthorityError(
        AtomicAuthorityError::Code::EpochMismatch,
        [&]
        {
            static_cast<void>(
                authority.preparePublication(makeRoot(database_uuid, 3, 6, definition_authority_capability_mask, "Second", 6)));
        });
    expectAuthorityError(
        AtomicAuthorityError::Code::GenerationMismatch,
        [&]
        {
            static_cast<void>(
                authority.preparePublication(makeRoot(database_uuid, 2, 4, definition_authority_capability_mask, "First", 5)));
        });
    expectAuthorityError(
        AtomicAuthorityError::Code::GenerationMismatch,
        [&]
        {
            static_cast<void>(
                authority.preparePublication(makeRoot(database_uuid, 2, 5, definition_authority_capability_mask, "Renamed", 5)));
        });
    expectAuthorityError(
        AtomicAuthorityError::Code::GenerationMismatch,
        [&]
        {
            static_cast<void>(authority.preparePublication(
                makeRoot(database_uuid, 2, 5, definition_authority_capability_mask, "First", 5, {}, "UInt32")));
        });

    expectAuthorityError(
        AtomicAuthorityError::Code::CapabilityMismatch,
        [&]
        {
            static_cast<void>(
                authority.preparePublication(makeRoot(database_uuid, 2, 5, dependent_object_authority_capability_mask, "First", 5)));
        });

    AuthorityRoot::Ptr activated_root;
    {
        auto current = authority.acquireCurrentRoot();
        ASSERT_TRUE(current);
        activated_root = current->cloneWithAuthorityState(activateDependentObjectAuthority(current->getAuthorityState()));
    }
    auto activation = authority.preparePublication(std::move(activated_root));
    authority.publish(std::move(activation));
    expectAuthorityError(
        AtomicAuthorityError::Code::CapabilityMismatch,
        [&]
        {
            static_cast<void>(
                authority.preparePublication(makeRoot(database_uuid, 3, 5, definition_authority_capability_mask, "First", 5)));
        });
}

TEST(AtomicAuthority, DependentObjectActivationTransfersOnePayloadChargeAndRetiresOnlyTheOldWrapper)
{
    const UUID database_uuid = testUUID(81, 82);
    AtomicAuthority authority(
        database_uuid, atomicCapabilities(), makeRoot(database_uuid, 1, 6, definition_authority_capability_mask, "Stable", 8));

    const auto initial_state = authority.getRetirementState();
    const UInt64 complete_charge = initial_state.current_root_charged_bytes;
    ASSERT_GT(complete_charge, AuthorityRoot::getWrapperLogicalCharge());
    EXPECT_EQ(initial_state.retired_root_count, 0);

    std::optional<AtomicAuthority::RootSnapshot> old_snapshot;
    old_snapshot.emplace(authority.acquireCurrentRoot());
    ASSERT_TRUE(*old_snapshot);
    EXPECT_EQ(complete_charge, AuthorityRoot::getWrapperLogicalCharge() + old_snapshot->get().getContentPayloadLogicalCharge());
    const UInt64 generation = old_snapshot->get().getTypeIndexGeneration();
    const Digest content_digest = old_snapshot->get().getTypeIndexContentDigest();
    const auto definition_data = old_snapshot->get().getDefinitionRecords().data();
    auto activated = old_snapshot->get().cloneWithAuthorityState(activateDependentObjectAuthority(old_snapshot->get().getAuthorityState()));
    ASSERT_TRUE(activated->sharesContentPayloadWith(old_snapshot->get()));

    auto activation = authority.preparePublication(std::move(activated));
    authority.publish(std::move(activation));

    {
        auto current = authority.acquireCurrentRoot();
        ASSERT_TRUE(current);
        EXPECT_TRUE(current->sharesContentPayloadWith(old_snapshot->get()));
        EXPECT_EQ(current->getDefinitionRecords().data(), definition_data);
        EXPECT_EQ(current->getTypeIndexGeneration(), generation);
        EXPECT_EQ(current->getTypeIndexContentDigest(), content_digest);
    }

    const auto pinned = authority.scanRetired();
    EXPECT_EQ(pinned.retired_root_count, 1);
    EXPECT_EQ(pinned.retired_root_charged_bytes, AuthorityRoot::getWrapperLogicalCharge());
    EXPECT_EQ(pinned.current_root_charged_bytes, complete_charge);

    auto replacement = authority.preparePublication(makeRoot(database_uuid, 3, 6, dependent_object_authority_capability_mask, "Stable", 8));
    authority.publish(std::move(replacement));
    const auto shared_payload_owner_pinned = authority.scanRetired();
    EXPECT_EQ(shared_payload_owner_pinned.retired_root_count, 2);
    EXPECT_EQ(shared_payload_owner_pinned.retired_root_charged_bytes, complete_charge + AuthorityRoot::getWrapperLogicalCharge());
    EXPECT_EQ(shared_payload_owner_pinned.current_root_charged_bytes, complete_charge);

    old_snapshot.reset();
    const auto reclaimed = authority.scanRetired();
    EXPECT_EQ(reclaimed.retired_root_count, 0);
    EXPECT_EQ(reclaimed.retired_root_charged_bytes, 0);
    EXPECT_EQ(reclaimed.current_root_charged_bytes, complete_charge);
}

TEST(AtomicAuthority, ConfigurationRecoveryAndAdvertisedLimitsFailClosed)
{
    const UUID database_uuid = testUUID(90, 100);
    expectAuthorityError(
        AtomicAuthorityError::Code::InvalidConfiguration, [&] { AtomicAuthority authority(UUIDHelpers::Nil, atomicCapabilities()); });

    auto missing_durable = atomicCapabilities();
    missing_durable.mask &= ~typeAuthorityCapabilityBit(TypeAuthorityCapability::DurableAlias);
    expectAuthorityError(
        AtomicAuthorityError::Code::InvalidConfiguration, [&] { AtomicAuthority authority(database_uuid, missing_durable); });

    AtomicAuthorityPublicationLimits no_slots;
    no_slots.hazard_slot_count = 0;
    expectAuthorityError(
        AtomicAuthorityError::Code::InvalidConfiguration,
        [&] { AtomicAuthority authority(database_uuid, atomicCapabilities(), {}, no_slots); });

    expectAuthorityError(
        AtomicAuthorityError::Code::EpochMismatch,
        [&]
        {
            AtomicAuthority authority(
                database_uuid,
                atomicCapabilities(),
                makeRoot(database_uuid, 1, 1, dependent_object_authority_capability_mask, "Impossible", 1));
        });

    expectRootError(
        AuthorityRootError::Code::InvalidConfiguration,
        [&] { static_cast<void>(makeRoot(database_uuid, 1, 0, definition_authority_capability_mask, "Zero", 2)); });

    auto one_definition_cap = atomicCapabilities();
    one_definition_cap.limits.maximum_definitions = 1;
    const std::vector specs{
        DefinitionSpec{.local_name = "First", .type_tag = 10, .built_in_type = "UInt64", .comment = {}},
        DefinitionSpec{.local_name = "Second", .type_tag = 11, .built_in_type = "UInt64", .comment = {}},
    };
    expectAuthorityError(
        AtomicAuthorityError::Code::LimitExceeded,
        [&]
        {
            AtomicAuthority authority(
                database_uuid,
                one_definition_cap,
                makeRoot(database_uuid, 1, 1, definition_authority_capability_mask, std::span<const DefinitionSpec>(specs)));
        });

    auto tiny_definition_cap = atomicCapabilities();
    tiny_definition_cap.limits.maximum_definition_bytes = 1;
    expectAuthorityError(
        AtomicAuthorityError::Code::LimitExceeded,
        [&]
        {
            AtomicAuthority authority(
                database_uuid, tiny_definition_cap, makeRoot(database_uuid, 1, 1, definition_authority_capability_mask, "TooLarge", 12));
        });
}

TEST(AtomicAuthority, ReaderReleaseNeverReclaimsARetiredRoot)
{
    const UUID database_uuid = testUUID(110, 120);
    AtomicAuthority authority(
        database_uuid, atomicCapabilities(), makeRoot(database_uuid, 1, 1, definition_authority_capability_mask, "Old", 9));

    std::weak_ptr<const Definition> old_definition;
    {
        auto snapshot = authority.acquireCurrentRoot();
        auto definition = snapshot->findByName("Old");
        ASSERT_TRUE(definition);
        old_definition = definition;
    }

    {
        auto old_session = authority.beginResolutionSession();
        auto publication = authority.preparePublication(makeRoot(database_uuid, 2, 2, definition_authority_capability_mask, "New", 10));
        authority.publish(std::move(publication));

        EXPECT_FALSE(old_definition.expired());
        const auto still_hazarded = authority.scanRetired();
        EXPECT_EQ(still_hazarded.retired_root_count, 1);
        EXPECT_EQ(still_hazarded.active_hazard_slots, 1);
    }

    const auto released_not_reclaimed = authority.getRetirementState();
    EXPECT_EQ(released_not_reclaimed.retired_root_count, 1);
    EXPECT_EQ(released_not_reclaimed.active_hazard_slots, 0);
    EXPECT_FALSE(old_definition.expired());

    const auto reclaimed = authority.scanRetired();
    EXPECT_EQ(reclaimed.retired_root_count, 0);
    EXPECT_EQ(reclaimed.retired_root_charged_bytes, 0);
    EXPECT_TRUE(old_definition.expired());
}

TEST(AtomicAuthority, RetirementBackpressureIsCheckedBeforePublication)
{
    const UUID database_uuid = testUUID(121, 122);
    AtomicAuthorityPublicationLimits publication_limits;
    publication_limits.hazard_slot_count = 2;
    publication_limits.maximum_retired_root_count = 1;
    publication_limits.maximum_retired_root_charged_bytes = 1ULL << 30;
    AtomicAuthority authority(
        database_uuid,
        atomicCapabilities(),
        makeRoot(database_uuid, 1, 1, definition_authority_capability_mask, "First", 1),
        publication_limits);

    {
        auto old_session = authority.beginResolutionSession();
        auto second = authority.preparePublication(makeRoot(database_uuid, 2, 2, definition_authority_capability_mask, "Second", 2));
        authority.publish(std::move(second));

        expectAuthorityError(
            AtomicAuthorityError::Code::LimitExceeded,
            [&]
            {
                static_cast<void>(
                    authority.preparePublication(makeRoot(database_uuid, 3, 3, definition_authority_capability_mask, "Third", 3)));
            });
    }

    EXPECT_EQ(authority.scanRetired().retired_root_count, 0);
    auto third = authority.preparePublication(makeRoot(database_uuid, 3, 3, definition_authority_capability_mask, "Third", 3));
    authority.publish(std::move(third));
}

TEST(AtomicAuthority, HazardSlotExhaustionFailsClosed)
{
    const UUID database_uuid = testUUID(130, 140);
    AtomicAuthorityPublicationLimits publication_limits;
    publication_limits.hazard_slot_count = 1;
    AtomicAuthority authority(
        database_uuid,
        atomicCapabilities(),
        makeRoot(database_uuid, 1, 1, definition_authority_capability_mask, "Only", 1),
        publication_limits);

    auto session = authority.beginResolutionSession();
    expectAuthorityError(AtomicAuthorityError::Code::HazardSlotsExhausted, [&] { static_cast<void>(authority.beginResolutionSession()); });
    EXPECT_TRUE(session.findByName("Only"));
}

TEST(AtomicAuthority, MovingASessionReturnsItsHazardSlotExactlyOnce)
{
    const UUID database_uuid = testUUID(131, 141);
    AtomicAuthorityPublicationLimits publication_limits;
    publication_limits.hazard_slot_count = 1;
    AtomicAuthority authority(
        database_uuid,
        atomicCapabilities(),
        makeRoot(database_uuid, 1, 1, definition_authority_capability_mask, "Only", 1),
        publication_limits);

    {
        auto original = authority.beginResolutionSession();
        auto moved = std::move(original);
        EXPECT_TRUE(moved.findByName("Only"));
        expectAuthorityError(
            AtomicAuthorityError::Code::HazardSlotsExhausted, [&] { static_cast<void>(authority.beginResolutionSession()); });
    }

    EXPECT_EQ(authority.getRetirementState().active_hazard_slots, 0);
    auto next = authority.beginResolutionSession();
    EXPECT_TRUE(next.findByName("Only"));
    EXPECT_EQ(authority.getRetirementState().active_hazard_slots, 1);
}

TEST(AtomicAuthority, ConcurrentReadersNeverMixCompositeGenerations)
{
    const UUID database_uuid = testUUID(141, 142);
    auto initial = makeRoot(database_uuid, 1, 1, definition_authority_capability_mask, "Old", 21);
    const auto old_identity = initial->getDefinitionRecords().front().identity;
    AtomicAuthority authority(database_uuid, atomicCapabilities(), std::move(initial));

    auto replacement = makeRoot(database_uuid, 2, 2, definition_authority_capability_mask, "New", 22);
    const auto new_identity = replacement->getDefinitionRecords().front().identity;
    auto publication = authority.preparePublication(std::move(replacement));

    constexpr UInt64 reader_count = 8;
    std::atomic<UInt64> ready{0};
    std::atomic<bool> published{false};
    std::atomic<UInt64> failures{0};
    std::vector<std::thread> readers;
    readers.reserve(reader_count);
    for (UInt64 reader = 0; reader < reader_count; ++reader)
    {
        readers.emplace_back(
            [&]
            {
                auto old_session = authority.beginResolutionSession();
                ready.fetch_add(1, std::memory_order_release);
                while (!published.load(std::memory_order_acquire))
                    std::this_thread::yield();

                if (old_session.getGeneration() != 1 || !old_session.findByIdentity(old_identity)
                    || old_session.findByIdentity(new_identity) || !old_session.findByName("Old") || old_session.findByName("New"))
                    failures.fetch_add(1, std::memory_order_relaxed);

                auto new_session = authority.beginResolutionSession();
                if (new_session.getGeneration() != 2 || new_session.findByIdentity(old_identity)
                    || !new_session.findByIdentity(new_identity) || new_session.findByName("Old") || !new_session.findByName("New"))
                    failures.fetch_add(1, std::memory_order_relaxed);
            });
    }

    while (ready.load(std::memory_order_acquire) != reader_count)
        std::this_thread::yield();
    authority.publish(std::move(publication));
    published.store(true, std::memory_order_release);
    for (auto & reader : readers)
        reader.join();

    EXPECT_EQ(failures.load(std::memory_order_relaxed), 0);
    EXPECT_EQ(authority.scanRetired().retired_root_count, 0);
    auto current = authority.acquireCurrentRoot();
    ASSERT_TRUE(current);
    EXPECT_EQ(current->getDatabaseCatalogEpoch(), 2);
}

TEST(AtomicAuthority, ConcurrentAcquirePublishAndRetirementScanPreserveLiveRoots)
{
    const UUID database_uuid = testUUID(143, 144);
    constexpr UInt64 publication_count = 64;
    constexpr UInt64 reader_count = 8;

    AtomicAuthorityPublicationLimits publication_limits;
    publication_limits.hazard_slot_count = 32;
    publication_limits.maximum_retired_root_count = publication_count;
    publication_limits.maximum_retired_root_charged_bytes = 1ULL << 30;
    AtomicAuthority authority(
        database_uuid,
        atomicCapabilities(),
        makeRoot(database_uuid, 1, 1, definition_authority_capability_mask, "Type1", 1),
        publication_limits);

    std::vector<AuthorityRoot::Ptr> replacements;
    replacements.reserve(publication_count - 1);
    for (UInt64 generation = 2; generation <= publication_count; ++generation)
    {
        const String name = "Type" + std::to_string(generation);
        replacements.push_back(makeRoot(database_uuid, generation, generation, definition_authority_capability_mask, name, generation));
    }

    std::atomic<UInt64> ready{0};
    std::atomic<bool> start{false};
    std::atomic<bool> finished{false};
    std::atomic<UInt64> failures{0};
    std::vector<std::thread> readers;
    readers.reserve(reader_count);
    for (UInt64 reader = 0; reader < reader_count; ++reader)
    {
        readers.emplace_back(
            [&]
            {
                ready.fetch_add(1, std::memory_order_release);
                while (!start.load(std::memory_order_acquire))
                    std::this_thread::yield();
                while (!finished.load(std::memory_order_acquire))
                {
                    auto session = authority.beginResolutionSession();
                    const UInt64 generation = session.getGeneration();
                    const String name = "Type" + std::to_string(generation);
                    const DefinitionIdentity expected_identity{
                        .database_uuid = database_uuid,
                        .type_uuid = testUUID(0x1000, generation),
                        .revision = 1,
                    };
                    const auto by_name = session.findByName(name);
                    const auto by_identity = session.findByIdentity(expected_identity);
                    if (generation == 0 || generation > publication_count || !by_name || !by_identity
                        || by_name->getIdentity() != expected_identity || by_identity->getNormalizedLocalName() != name)
                        failures.fetch_add(1, std::memory_order_relaxed);
                }
            });
    }

    std::thread scanner(
        [&]
        {
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire))
                std::this_thread::yield();
            while (!finished.load(std::memory_order_acquire))
            {
                static_cast<void>(authority.scanRetired());
                std::this_thread::yield();
            }
            static_cast<void>(authority.scanRetired());
        });

    while (ready.load(std::memory_order_acquire) != reader_count + 1)
        std::this_thread::yield();
    start.store(true, std::memory_order_release);
    for (auto & replacement : replacements)
    {
        auto publication = authority.preparePublication(std::move(replacement));
        authority.publish(std::move(publication));
        std::this_thread::yield();
    }
    finished.store(true, std::memory_order_release);

    for (auto & reader : readers)
        reader.join();
    scanner.join();

    EXPECT_EQ(failures.load(std::memory_order_relaxed), 0);
    EXPECT_EQ(authority.scanRetired().retired_root_count, 0);
    auto current = authority.acquireCurrentRoot();
    ASSERT_TRUE(current);
    EXPECT_EQ(current->getDatabaseCatalogEpoch(), publication_count);
    EXPECT_EQ(current->getTypeIndexGeneration(), publication_count);
}

TEST(AtomicAuthority, ShutdownClosesAdmissionAndWaitsForLiveSessions)
{
    const UUID database_uuid = testUUID(150, 160);
    AtomicAuthority authority(
        database_uuid, atomicCapabilities(), makeRoot(database_uuid, 1, 1, definition_authority_capability_mask, "Pinned", 1));
    std::optional<IAuthorityAdapter::ResolutionSession> session;
    session.emplace(authority.beginResolutionSession());
    std::optional<AtomicAuthority::RootSnapshot> root_snapshot;
    root_snapshot.emplace(authority.acquireCurrentRoot());
    auto lifecycle_snapshot = authority.acquireLifecycleSnapshot();

    std::atomic<bool> drained{false};
    std::thread shutdown_thread(
        [&]
        {
            authority.shutdownAndDrain();
            drained.store(true, std::memory_order_release);
        });

    while (!authority.isShutdown())
        std::this_thread::yield();
    EXPECT_FALSE(drained.load(std::memory_order_acquire));
    const auto shutdown_state = authority.getRetirementState();
    EXPECT_TRUE(shutdown_state.shutdown);
    EXPECT_EQ(shutdown_state.active_hazard_slots, 3u);
    expectAuthorityError(AtomicAuthorityError::Code::Shutdown, [&] { static_cast<void>(authority.beginResolutionSession()); });
    expectAuthorityError(AtomicAuthorityError::Code::Shutdown, [&] { static_cast<void>(authority.acquireCurrentRoot()); });
    expectAuthorityError(AtomicAuthorityError::Code::Shutdown, [&] { static_cast<void>(authority.acquireLifecycleSnapshot()); });

    session.reset();
    root_snapshot.reset();
    lifecycle_snapshot.reset();
    shutdown_thread.join();
    EXPECT_TRUE(drained.load(std::memory_order_acquire));
    EXPECT_EQ(authority.getRetirementState().retired_root_count, 0);
    EXPECT_EQ(authority.getRetirementState().active_hazard_slots, 0);
}

TEST(AtomicAuthority, PublishingAStalePreparedRootIsAnInvariantFailure)
{
    const UUID database_uuid = testUUID(170, 180);
    AtomicAuthority authority(
        database_uuid, atomicCapabilities(), makeRoot(database_uuid, 1, 1, definition_authority_capability_mask, "Old", 11));
    auto winner = authority.preparePublication(makeRoot(database_uuid, 2, 2, definition_authority_capability_mask, "Winner", 12));
    auto stale = authority.preparePublication(makeRoot(database_uuid, 2, 2, definition_authority_capability_mask, "Stale", 13));
    authority.publish(std::move(winner));

    EXPECT_DEATH({ authority.publish(std::move(stale)); }, "");
}

}
}
