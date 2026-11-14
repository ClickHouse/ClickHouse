#include <Access/AccessRights.h>
#include <Access/EffectiveAccessRightsCache.h>
#include <Access/EnabledRolesInfo.h>
#include <Access/User.h>
#include <gtest/gtest.h>
#include <atomic>
#include <barrier>
#include <thread>
#include <vector>

using namespace DB;

namespace
{
struct Snapshot
{
    UserPtr user = std::make_shared<User>();
    std::shared_ptr<const EnabledRolesInfo> roles = std::make_shared<EnabledRolesInfo>();
    std::shared_ptr<const AccessRights> access = std::make_shared<AccessRights>();
    std::shared_ptr<const AccessRights> implicit = std::make_shared<AccessRights>();

    void store(const EffectiveAccessRightsCache::Handle & handle, const ImplicitExpansionSettings & settings = {}) const
    {
        handle.store(user, roles, settings, access, implicit);
    }
};

const UUID user_id{UInt128{1}};
}

/// SQL sessions can retain several `ContextAccess` objects in the existing context cache.
/// Exercise exact registration lifetimes here; functional tests cover the resulting permissions.
TEST(EffectiveAccessRightsCache, ActivatesAndDeactivatesWithCompatibleContexts)
{
    std::mutex subscription_mutex;
    EffectiveAccessRightsCache cache{subscription_mutex};
    std::lock_guard lock{cache.getMutex()};
    Snapshot snapshot;
    auto first = cache.registerContext(user_id);
    EXPECT_FALSE(first.isShared());
    snapshot.store(first);
    EXPECT_FALSE(first.find(snapshot.user, snapshot.roles, {}));

    {
        auto second = cache.registerContext(user_id);
        EXPECT_TRUE(first.isShared());
        EXPECT_TRUE(second.isShared());
        /// The sole context did not publish a result before there was a sharing opportunity.
        EXPECT_FALSE(second.find(snapshot.user, snapshot.roles, {}));
        snapshot.store(first);
        auto found = second.find(snapshot.user, snapshot.roles, {});
        ASSERT_TRUE(found);
        EXPECT_EQ(found->access, snapshot.access);
        EXPECT_EQ(found->access_with_implicit, snapshot.implicit);
    }

    EXPECT_FALSE(first.isShared());
    EXPECT_FALSE(first.find(snapshot.user, snapshot.roles, {}));
    auto second = cache.registerContext(user_id);
    EXPECT_TRUE(first.isShared());
    EXPECT_TRUE(second.find(snapshot.user, snapshot.roles, {}));
}

TEST(EffectiveAccessRightsCache, UnrelatedUsersAndRoleSetsDoNotActivateSharing)
{
    std::mutex mutex_a;
    std::mutex mutex_b;
    EffectiveAccessRightsCache roles_a{mutex_a};
    EffectiveAccessRightsCache roles_b{mutex_b};
    std::lock_guard lock_a{roles_a.getMutex()};
    std::lock_guard lock_b{roles_b.getMutex()};
    auto first = roles_a.registerContext(user_id);
    auto other_user = roles_a.registerContext(UUID{UInt128{2}});
    auto other_roles = roles_b.registerContext(user_id);
    EXPECT_FALSE(first.isShared());
    EXPECT_FALSE(other_user.isShared());
    EXPECT_FALSE(other_roles.isShared());

    auto peer = roles_a.registerContext(user_id);
    EXPECT_TRUE(first.isShared());
    EXPECT_FALSE(other_user.isShared());
    EXPECT_FALSE(other_roles.isShared());
}

TEST(EffectiveAccessRightsCache, ValidatesUserRoleAndSettingsSnapshots)
{
    std::mutex subscription_mutex;
    EffectiveAccessRightsCache cache{subscription_mutex};
    std::lock_guard lock{cache.getMutex()};
    Snapshot snapshot;
    auto first = cache.registerContext(user_id);
    auto second = cache.registerContext(user_id);
    snapshot.store(first);
    EXPECT_FALSE(second.find(std::make_shared<User>(), snapshot.roles, {}));
    EXPECT_FALSE(second.find(snapshot.user, std::make_shared<EnabledRolesInfo>(), {}));

    for (unsigned bits = 0; bits < 16; ++bits)
    {
        ImplicitExpansionSettings settings{bool(bits & 1), bool(bits & 2), bool(bits & 4), bool(bits & 8)};
        snapshot.store(first, settings);
        EXPECT_TRUE(second.find(snapshot.user, snapshot.roles, settings));
        settings.table_engines_require_grant = !settings.table_engines_require_grant;
        EXPECT_FALSE(second.find(snapshot.user, snapshot.roles, settings));
    }
}

TEST(EffectiveAccessRightsCache, MovingHandlesPreservesRegistrationCounts)
{
    std::mutex subscription_mutex;
    EffectiveAccessRightsCache cache{subscription_mutex};
    std::lock_guard lock{cache.getMutex()};
    auto first = cache.registerContext(user_id);
    auto second = cache.registerContext(user_id);
    auto moved = std::move(second);
    EXPECT_FALSE(second.isShared());
    EXPECT_TRUE(first.isShared());
    EXPECT_TRUE(moved.isShared());
    moved = std::move(first);
    EXPECT_FALSE(first.isShared());
    EXPECT_FALSE(moved.isShared());
    first = cache.registerContext(user_id);
    EXPECT_TRUE(moved.isShared());
    first = {};
    EXPECT_FALSE(moved.isShared());
}

TEST(EffectiveAccessRightsCache, ResultsDoNotOutliveTheirOwners)
{
    std::mutex subscription_mutex;
    EffectiveAccessRightsCache cache{subscription_mutex};
    std::lock_guard lock{cache.getMutex()};
    auto first = cache.registerContext(user_id);
    auto second = cache.registerContext(user_id);
    std::weak_ptr<const AccessRights> weak_access;
    {
        Snapshot snapshot;
        weak_access = snapshot.access;
        snapshot.store(first);
    }
    EXPECT_TRUE(weak_access.expired());
    Snapshot fresh;
    EXPECT_FALSE(second.find(fresh.user, fresh.roles, {}));
}

TEST(EffectiveAccessRightsCache, RegistrationsCanOutliveRegistry)
{
    EffectiveAccessRightsCache::Handle first;
    EffectiveAccessRightsCache::Handle second;
    Snapshot snapshot;
    {
        std::mutex subscription_mutex;
        EffectiveAccessRightsCache cache{subscription_mutex};
        std::lock_guard lock{cache.getMutex()};
        first = cache.registerContext(user_id);
        second = cache.registerContext(user_id);
        snapshot.store(first);
    }
    EXPECT_TRUE(second.find(snapshot.user, snapshot.roles, {}));
    first = {};
    EXPECT_FALSE(second.isShared());
}

TEST(EffectiveAccessRightsCache, ConcurrentVersionsCannotMixResults)
{
    std::mutex subscription_mutex;
    EffectiveAccessRightsCache cache{subscription_mutex};
    EffectiveAccessRightsCache::Handle keeper;
    {
        std::lock_guard lock{cache.getMutex()};
        keeper = cache.registerContext(user_id);
    }
    constexpr size_t threads_count = 4;
    constexpr size_t iterations = 10000;
    std::barrier<> start(threads_count);
    std::atomic<bool> valid = true;
    std::vector<std::thread> threads;
    for (size_t i = 0; i < threads_count; ++i)
    {
        threads.emplace_back([&]
        {
            Snapshot snapshot;
            start.arrive_and_wait();
            for (size_t j = 0; j < iterations; ++j)
            {
                EffectiveAccessRightsCache::Handle handle;
                {
                    std::lock_guard lock{cache.getMutex()};
                    handle = cache.registerContext(user_id);
                }
                snapshot.store(handle);
                auto found = handle.find(snapshot.user, snapshot.roles, {});
                if (found && (found->access != snapshot.access || found->access_with_implicit != snapshot.implicit))
                    valid = false;
            }
        });
    }
    for (auto & thread : threads)
        thread.join();
    EXPECT_TRUE(valid);
    EXPECT_FALSE(keeper.isShared());
}
