#include <gtest/gtest.h>
#include <Access/RolesOrUsersSet.h>
#include <Core/UUID.h>
#include <unordered_set>

using namespace DB;

/// Proof test for iterator invalidation bug in RolesOrUsersSet::removeDependencies.
/// At line 384 in RolesOrUsersSet.cpp, `except_ids.erase(it)` should be
/// `it = except_ids.erase(it)` to update the iterator after erasure.
/// The `except_ids` member is a `boost::container::flat_set<UUID>` (vector-backed).
/// This test triggers the bug by removing multiple UUIDs from except_ids.
/// ASan should detect the invalid iterator access after erasure.

TEST(RolesOrUsersSet, RemoveDependenciesFromExceptIds)
{
    /// Create UUIDs that will be added to except_ids
    UUID id1 = UUIDHelpers::generateV4();
    UUID id2 = UUIDHelpers::generateV4();
    UUID id3 = UUIDHelpers::generateV4();
    UUID id4 = UUIDHelpers::generateV4();

    /// Create a RolesOrUsersSet with all=true and populate except_ids
    RolesOrUsersSet set{RolesOrUsersSet::AllTag{}};
    set.except_ids.insert(id1);
    set.except_ids.insert(id2);
    set.except_ids.insert(id3);
    set.except_ids.insert(id4);

    ASSERT_EQ(set.except_ids.size(), 4);

    /// Create a dependency set that matches ALL except_ids
    /// This forces multiple erasures in the buggy loop
    std::unordered_set<UUID> deps_to_remove;
    deps_to_remove.insert(id1);
    deps_to_remove.insert(id2);
    deps_to_remove.insert(id3);
    deps_to_remove.insert(id4);

    /// This call should trigger the iterator invalidation bug under ASan
    /// The bug: except_ids.erase(it) doesn't update it, so ++it uses invalid iterator
    set.removeDependencies(deps_to_remove);

    /// After removal, except_ids should be empty
    /// Without the fix, this may crash, produce wrong results, or trigger ASan
    EXPECT_EQ(set.except_ids.size(), 0);
}

TEST(RolesOrUsersSet, RemoveDependenciesFromExceptIdsPartial)
{
    /// Test partial removal to exercise both branches (erase and ++it)
    UUID id1 = UUIDHelpers::generateV4();
    UUID id2 = UUIDHelpers::generateV4();
    UUID id3 = UUIDHelpers::generateV4();
    UUID id4 = UUIDHelpers::generateV4();

    RolesOrUsersSet set{RolesOrUsersSet::AllTag{}};
    set.except_ids.insert(id1);
    set.except_ids.insert(id2);
    set.except_ids.insert(id3);
    set.except_ids.insert(id4);

    ASSERT_EQ(set.except_ids.size(), 4);

    /// Only remove some - forces interleaved erase/skip operations
    std::unordered_set<UUID> deps_to_remove;
    deps_to_remove.insert(id1);
    deps_to_remove.insert(id3);

    set.removeDependencies(deps_to_remove);

    /// Should have 2 remaining
    EXPECT_EQ(set.except_ids.size(), 2);
    EXPECT_TRUE(set.except_ids.contains(id2));
    EXPECT_TRUE(set.except_ids.contains(id4));
}

TEST(RolesOrUsersSet, RemoveDependenciesFromExceptIdsMany)
{
    /// Create many UUIDs and remove all of them
    /// This maximizes the chance of hitting the invalidation bug
    constexpr size_t count = 32;
    std::vector<UUID> uuids;
    uuids.reserve(count);
    for (size_t i = 0; i < count; ++i)
        uuids.push_back(UUIDHelpers::generateV4());

    RolesOrUsersSet set{RolesOrUsersSet::AllTag{}};
    for (const auto & id : uuids)
        set.except_ids.insert(id);

    ASSERT_EQ(set.except_ids.size(), count);

    /// Remove all UUIDs - triggers consecutive erasures
    std::unordered_set<UUID> deps_to_remove;
    for (const auto & id : uuids)
        deps_to_remove.insert(id);

    set.removeDependencies(deps_to_remove);

    EXPECT_EQ(set.except_ids.size(), 0);
}
