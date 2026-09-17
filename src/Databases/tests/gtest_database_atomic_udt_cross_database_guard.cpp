#include <Databases/DatabaseSchemaMutationTransaction.h>
#include <Databases/UDT/AtomicCrossDatabaseGuard.h>
#include <Databases/tests/UDTTestResourceImages.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/UDT/TemplateChecker.h>

#include <Core/NamesAndTypes.h>

#include <Storages/ColumnsDescription.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <Common/Exception.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

namespace DB::ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

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

Digest digest(UInt8 tag)
{
    Digest result{};
    result.front() = tag;
    result.back() = static_cast<UInt8>(tag + 1);
    return result;
}

struct AuthorityFixture
{
    UUID database_uuid;
    SchemaObjectID expected_table;
    SchemaObjectID definition_object;
    AuthorityRoot::Ptr root;

    AuthorityFixture(UUID database_uuid_, UUID expected_table_uuid)
        : database_uuid(database_uuid_)
        , expected_table{
              .kind = SchemaObjectKind::Table,
              .database_uuid = database_uuid,
              .object_uuid = expected_table_uuid,
          }
    {
        DefinitionInput definition_input;
        definition_input.identity = {
            .database_uuid = database_uuid,
            .type_uuid = uuid(UUIDHelpers::getHighBytes(database_uuid), 0x100),
            .revision = 1,
        };
        definition_input.normalized_name = "guard_db.Alias";
        definition_input.normalized_local_name = "Alias";
        TemplateNode template_root;
        template_root.kind = TemplateNodeKind::BuiltIn;
        template_root.atom = "UInt64";
        definition_input.nodes.push_back(std::move(template_root));
        auto definitions = TemplateChecker::checkAll({std::move(definition_input)});

        const Record definition_record = makeRecord(
            *definitions.front(),
            {
                .canonical_definition_sql = "ATTACH TYPE guard_db.Alias AS UInt64",
                .canonical_physical_template_sql = "UInt64",
                .owner_uuid = uuid(0x9000, 1),
                .owner_display_name = "owner",
                .comment = "cross-database guard fixture",
                .creation_time_us_utc = 1,
            });
        const auto references = Test::singleDefinitionPersistedTypeReferences(
            expected_table, 1, digest(2), definitions.front(), std::make_shared<DataTypeUInt64>(), PersistedTypePathSection::ColumnType);
        const SidecarExpectationRecord expectation = Test::sidecarExpectationFor(references);
        definition_object = {
            .kind = SchemaObjectKind::TypeDefinition,
            .database_uuid = database_uuid,
            .object_uuid = definitions.front()->getIdentity().type_uuid,
        };
        const SchemaObjectDependencyEdge edge{
            .dependent = expected_table,
            .dependency = definition_object,
            .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
        };
        auto graph = SchemaObjectDependencyGraph::build(database_uuid, std::vector{definition_object, expected_table}, std::vector{edge});

        std::vector<AuthorityInventoryLeaf> leaves{
            {
                .key = {
                    .record_kind = AuthorityInventoryRecordKind::TypeDefinition,
                    .object_uuid = definition_record.identity.type_uuid,
                },
                .object_revision = definition_record.identity.revision,
                .canonical_record_hash = computeRecordHash(definition_record),
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
        const auto inventory = buildAuthorityInventorySummary(leaves);
        const auto state = makeAuthorityState(
            database_uuid,
            7,
            dependent_object_authority_capability_mask,
            inventory.leaf_count,
            inventory.merkle_radix_root,
            graph->computeRoot());
        const std::vector definition_records{definition_record};
        const std::vector expectations{expectation};
        const std::vector reference_images{references};
        const Test::DependentObjectResourceImageBatch dependent_objects(
            expectations, Test::dependentObjectResourceImageInputs(reference_images));
        root = AuthorityRootBuilder::build(
            state, 1, definitions, definition_records, expectations, std::move(graph), AuthorityRootBuildLimits{}, dependent_objects.get());
    }

    AtomicCrossDatabaseAuthorityView view(std::optional<UInt64> recovery_required_transaction_id = std::nullopt) const
    {
        return {
            .database_uuid = database_uuid,
            .published_root = root.get(),
            .durable_storage_present = true,
            .durable_state = root->getAuthorityState(),
            .recovery_required_transaction_id = recovery_required_transaction_id,
            .durable_authority_marker = true,
        };
    }
};

AtomicCrossDatabaseAuthorityView emptyAuthorityView(UUID database_uuid)
{
    return {
        .database_uuid = database_uuid,
        .published_root = nullptr,
        .durable_storage_present = false,
        .durable_state = std::nullopt,
        .recovery_required_transaction_id = std::nullopt,
        .durable_authority_marker = false,
    };
}

template <typename Callback>
void expectDBException(int code, std::string_view message_fragment, Callback && callback)
{
    try
    {
        callback();
        FAIL() << "expected a DB::Exception";
    }
    catch (const Exception & error)
    {
        EXPECT_EQ(error.code(), code) << error.message();
        EXPECT_NE(error.message().find(message_fragment), String::npos) << error.message();
    }
}

template <typename Callback>
void expectReplayConflict(std::string_view message_fragment, Callback && callback)
{
    try
    {
        callback();
        FAIL() << "expected a schema replay conflict";
    }
    catch (const DatabaseSchemaMutationReplayConflictError & error)
    {
        EXPECT_NE(String(error.what()).find(message_fragment), String::npos) << error.what();
    }
}

}

TEST(DatabaseAtomicCrossDatabaseGuard, SourceDatabaseExpectationBlocksPhysicalOnlyLazyProxy)
{
    const UUID table_uuid = uuid(0x10, 1);
    AuthorityFixture source(uuid(0x01, 1), table_uuid);

    /// A lazy proxy is allowed to retain only the physical cache. The
    /// database-owned expectation must still prevent its source table moving.
    StorageInMemoryMetadata lazy_proxy_metadata;
    lazy_proxy_metadata.setColumns(ColumnsDescription(NamesAndTypesList{{"id", std::make_shared<DataTypeUInt64>()}}));
    ASSERT_FALSE(lazy_proxy_metadata.getBoundUDTReferences());
    ASSERT_FALSE(lazy_proxy_metadata.getBoundUDTExpectation());

    const bool database_owned = hasDatabaseOwnedTableExpectationForCrossDatabaseMove(source.view(), table_uuid);
    ASSERT_TRUE(database_owned);
    expectDBException(
        ErrorCodes::NOT_IMPLEMENTED,
        "cross-database UDT authority transfer is not implemented",
        [&]
        {
            assertTableCanLeaveAtomicDatabase(
                static_cast<bool>(lazy_proxy_metadata.getBoundUDTReferences()) || database_owned, "source.events");
        });
}

TEST(DatabaseAtomicCrossDatabaseGuard, TargetExpectationCollisionBlocksIncomingTableUUID)
{
    const UUID incoming_table_uuid = uuid(0x20, 1);
    AuthorityFixture target(uuid(0x02, 1), incoming_table_uuid);

    const bool collision = hasDatabaseOwnedTableExpectationForCrossDatabaseMove(target.view(), incoming_table_uuid);
    ASSERT_TRUE(collision);
    expectDBException(
        ErrorCodes::NOT_IMPLEMENTED,
        "already owns a user-defined type expectation for its UUID",
        [&] { assertTableCanEnterAtomicDatabase(collision, "source.events", "target"); });
}

TEST(DatabaseAtomicCrossDatabaseGuard, ExchangeChecksReverseLeaveAndReverseEnterDirections)
{
    const UUID source_database_uuid = uuid(0x03, 1);
    const UUID target_database_uuid = uuid(0x04, 1);
    const UUID source_table_uuid = uuid(0x30, 1);
    const UUID target_table_uuid = uuid(0x40, 1);

    /// Forward source-leave/target-enter checks both pass, then the target's
    /// resident table is checked before it leaves in the reverse direction.
    AuthorityFixture target_owns_resident(target_database_uuid, target_table_uuid);
    EXPECT_FALSE(hasDatabaseOwnedTableExpectationForCrossDatabaseMove(emptyAuthorityView(source_database_uuid), source_table_uuid));
    EXPECT_FALSE(hasDatabaseOwnedTableExpectationForCrossDatabaseMove(target_owns_resident.view(), source_table_uuid));
    const bool target_resident_is_owned
        = hasDatabaseOwnedTableExpectationForCrossDatabaseMove(target_owns_resident.view(), target_table_uuid);
    ASSERT_TRUE(target_resident_is_owned);
    expectDBException(
        ErrorCodes::NOT_IMPLEMENTED,
        "cross-database UDT authority transfer is not implemented",
        [&] { assertTableCanLeaveAtomicDatabase(target_resident_is_owned, "target.resident"); });

    /// The symmetric case reaches the final source-enter check for the table
    /// arriving from the target database.
    AuthorityFixture source_owns_incoming(source_database_uuid, target_table_uuid);
    EXPECT_FALSE(hasDatabaseOwnedTableExpectationForCrossDatabaseMove(source_owns_incoming.view(), source_table_uuid));
    EXPECT_FALSE(hasDatabaseOwnedTableExpectationForCrossDatabaseMove(emptyAuthorityView(target_database_uuid), source_table_uuid));
    EXPECT_FALSE(hasDatabaseOwnedTableExpectationForCrossDatabaseMove(emptyAuthorityView(target_database_uuid), target_table_uuid));
    const bool source_incoming_collision
        = hasDatabaseOwnedTableExpectationForCrossDatabaseMove(source_owns_incoming.view(), target_table_uuid);
    ASSERT_TRUE(source_incoming_collision);
    expectDBException(
        ErrorCodes::NOT_IMPLEMENTED,
        "already owns a user-defined type expectation for its UUID",
        [&] { assertTableCanEnterAtomicDatabase(source_incoming_collision, "target.resident", "source"); });
}

TEST(DatabaseAtomicCrossDatabaseGuard, RecoveryRequiredFailsClosedBeforeExpectationLookup)
{
    AuthorityFixture source(uuid(0x05, 1), uuid(0x50, 1));
    const UUID unrelated_table_uuid = uuid(0x51, 1);

    expectReplayConflict(
        "pending user-defined type schema recovery",
        [&]
        {
            static_cast<void>(hasDatabaseOwnedTableExpectationForCrossDatabaseMove(
                source.view(/*recovery_required_transaction_id=*/42), unrelated_table_uuid));
        });
}

TEST(DatabaseAtomicCrossDatabaseGuard, PublishedAuthorityWithoutActivationMarkerFailsClosed)
{
    AuthorityFixture source(uuid(0x07, 1), uuid(0x70, 1));
    auto inconsistent = source.view();
    inconsistent.durable_authority_marker = false;

    expectReplayConflict(
        "without its durable activation marker",
        [&] { static_cast<void>(hasDatabaseOwnedTableExpectationForCrossDatabaseMove(inconsistent, uuid(0x70, 1))); });
}

TEST(DatabaseAtomicCrossDatabaseGuard, UnpublishedDurableStateWithoutMarkerFailsClosed)
{
    const UUID database_uuid = uuid(0x06, 1);
    AuthorityFixture durable(database_uuid, uuid(0x60, 1));
    auto inconsistent = durable.view();
    inconsistent.published_root = nullptr;
    inconsistent.durable_authority_marker = false;

    expectReplayConflict(
        "inconsistent unpublished durable user-defined type authority state",
        [&] { static_cast<void>(hasDatabaseOwnedTableExpectationForCrossDatabaseMove(inconsistent, uuid(0x61, 1))); });
}

TEST(DatabaseAtomicCrossDatabaseGuard, UnpublishedStorageWithoutAHeadStillFailsClosed)
{
    auto unpublished = emptyAuthorityView(uuid(0x08, 1));
    unpublished.durable_storage_present = true;

    EXPECT_TRUE(hasDatabaseOwnedTableExpectationForCrossDatabaseMove(unpublished, uuid(0x80, 1)));
}

}
