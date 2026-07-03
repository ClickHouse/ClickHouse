#include <gtest/gtest.h>

#include <Storages/StreamingStorageRegistry.h>
#include <Core/UUID.h>
#include <base/sanitizer_defs.h>

using namespace DB;

/// Each test generates fresh UUIDs, so concurrent or sequential tests
/// do not collide in the singleton registry.
class StreamingStorageRegistryTest : public ::testing::Test
{
protected:
    StreamingStorageRegistry & registry = StreamingStorageRegistry::instance();

    UUID uuid1 = UUIDHelpers::generateV4();
    UUID uuid2 = UUIDHelpers::generateV4();
};

TEST_F(StreamingStorageRegistryTest, RegisterAndUnregister)
{
    StorageID table("test_db", "table_reg", uuid1);

    ASSERT_NO_THROW(registry.registerTable(table));
    ASSERT_NO_THROW(registry.unregisterTable(table));
}

TEST_F(StreamingStorageRegistryTest, DuplicateRegisterThrows)
{
    StorageID table("test_db", "table_dup", uuid1);

    registry.registerTable(table);
    EXPECT_THROW(registry.registerTable(table), Exception);

    registry.unregisterTable(table);
}

/// LOGICAL_ERROR aborts in debug/sanitizer builds, so skip this test there.
#ifndef DEBUG_OR_SANITIZER_BUILD
TEST_F(StreamingStorageRegistryTest, UnregisterNonExistentThrows)
{
    StorageID table("test_db", "table_missing", uuid1);
    EXPECT_THROW(registry.unregisterTable(table), Exception);
}
#endif

TEST_F(StreamingStorageRegistryTest, SimpleRename)
{
    StorageID from("test_db", "old_name", uuid1);
    StorageID to("test_db", "new_name", uuid1);

    registry.registerTable(from);
    ASSERT_NO_THROW(registry.renameTable(from, to));
    ASSERT_NO_THROW(registry.unregisterTable(to));
}

/// LOGICAL_ERROR aborts in debug/sanitizer builds, so skip this test there.
#ifndef DEBUG_OR_SANITIZER_BUILD
TEST_F(StreamingStorageRegistryTest, RenameNonExistentThrows)
{
    StorageID from("test_db", "no_such_table", uuid1);
    StorageID to("test_db", "dest", uuid1);

    EXPECT_THROW(registry.renameTable(from, to), Exception);
}
#endif

/// Regression test for the A↔B batch-rename scenario.
///
/// SharedDatabaseCatalog::applyState renames streaming tables in sequence.
/// When two S3Queue tables swap names (e.g., during a dbt RENAME+EXCHANGE
/// workflow), the rename sequence is:
///   1. rename A → B   (table uuid1 gets name B)
///   2. rename B → A   (table uuid2 gets name A)
///
/// Before the UUID-based identity fix, the registry matched entries by name
/// only, so step 1 would silently collide with the existing entry for B.
/// After step 2 completed, only one of the two tables remained registered,
/// causing LOGICAL_ERROR on subsequent applyState retries.
///
/// With UUID-based tracking, each table is identified by its UUID, so both
/// survive the swap regardless of intermediate name collisions.
TEST_F(StreamingStorageRegistryTest, BatchRenameSwapPreservesBothTables)
{
    StorageID table_a("test_db", "queue_a", uuid1);
    StorageID table_b("test_db", "queue_b", uuid2);

    registry.registerTable(table_a);
    registry.registerTable(table_b);

    /// Step 1: rename A → B (table uuid1 now has name "queue_b")
    StorageID a_with_new_name("test_db", "queue_b", uuid1);
    ASSERT_NO_THROW(registry.renameTable(table_a, a_with_new_name));

    /// Step 2: rename B → A (table uuid2 now has name "queue_a")
    StorageID b_with_new_name("test_db", "queue_a", uuid2);
    ASSERT_NO_THROW(registry.renameTable(table_b, b_with_new_name));

    /// Both tables must still be registered — unregister must not throw.
    EXPECT_NO_THROW(registry.unregisterTable(a_with_new_name));
    EXPECT_NO_THROW(registry.unregisterTable(b_with_new_name));
}

/// Same swap but across different databases, which is what dbt does when it
/// renames __new → production and production → __backup in different schemas.
TEST_F(StreamingStorageRegistryTest, BatchRenameCrossDatabaseSwap)
{
    StorageID table_a("db_prod", "queue", uuid1);
    StorageID table_b("db_staging", "queue", uuid2);

    registry.registerTable(table_a);
    registry.registerTable(table_b);

    StorageID a_moved("db_staging", "queue", uuid1);
    ASSERT_NO_THROW(registry.renameTable(table_a, a_moved));

    StorageID b_moved("db_prod", "queue", uuid2);
    ASSERT_NO_THROW(registry.renameTable(table_b, b_moved));

    EXPECT_NO_THROW(registry.unregisterTable(a_moved));
    EXPECT_NO_THROW(registry.unregisterTable(b_moved));
}
