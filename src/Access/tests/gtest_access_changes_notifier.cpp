#include <gtest/gtest.h>

#include <Access/AccessChangesNotifier.h>
#include <Access/Common/AccessEntityType.h>
#include <Core/UUID.h>
#include <Common/Exception.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int UNFINISHED;
}

/// The per-entity caches (RowPolicyCache, RoleCache, SettingsProfilesCache, QuotaCache) rely on a whole
/// notification batch being delivered to a by-type handler in a single call, so that they patch their
/// per-entity maps for each change and run the expensive O(enabled sets * entities) rebuild only once per
/// batch (a full refresh delivers one notification per entity, but needs a single rebuild).
TEST(AccessChangesNotifier, BatchedChangesDeliveredOncePerBatch)
{
    AccessChangesNotifier notifier;

    size_t handler_calls = 0;
    size_t total_changes = 0;

    auto subscription = notifier.subscribeForChanges(
        AccessEntityType::ROW_POLICY,
        [&](const std::vector<AccessChangesNotifier::Change> & changes)
        {
            ++handler_calls;
            total_changes += changes.size();
        });

    /// A refresh that touches many entities: one notification per entity, drained in a single batch.
    static constexpr size_t num_entities = 16;
    for (size_t i = 0; i < num_entities; ++i)
        notifier.onEntityRemoved(UUIDHelpers::generateV4(), AccessEntityType::ROW_POLICY);

    notifier.sendNotifications();

    /// The whole batch is delivered in a single call - this is the coalescing the caches depend on (without
    /// it the handler, and so the rebuild, would run num_entities times).
    EXPECT_EQ(handler_calls, 1u);
    EXPECT_EQ(total_changes, num_entities);

    /// A single DDL delivers one change in one call.
    notifier.onEntityRemoved(UUIDHelpers::generateV4(), AccessEntityType::ROW_POLICY);
    notifier.sendNotifications();
    EXPECT_EQ(handler_calls, 2u);
    EXPECT_EQ(total_changes, num_entities + 1);

    /// A by-type handler is called even on an empty batch (with no changes): real handlers early-return on
    /// their flag, so this is cheap, and it must keep firing so a rebuild that previously threw is retried
    /// without waiting for a fresh access change (see ThrowingRebuildRetried).
    notifier.sendNotifications();
    EXPECT_EQ(handler_calls, 3u);
    EXPECT_EQ(total_changes, num_entities + 1); /// unchanged - the batch was empty
}

/// A batch handler that throws must not lose the pending work: the caches clear their "needs rebuild" flag
/// only on success, so the notifier has to keep invoking by-type handlers on later sendNotifications() calls
/// - including ones that drain no events - otherwise the derived state stays stale until some unrelated
/// access change happens to queue a notification (and an up-to-date SYSTEM RELOAD USERS, which diffs to
/// zero, would never trigger the retry).
TEST(AccessChangesNotifier, ThrowingRebuildRetried)
{
    AccessChangesNotifier notifier;

    bool need_rebuild = false; /// mirrors RowPolicyCache::need_mix_filters and friends
    size_t rebuilds = 0;
    bool fail_next_rebuild = true;

    auto subscription = notifier.subscribeForChanges(
        AccessEntityType::ROW_POLICY,
        [&](const std::vector<AccessChangesNotifier::Change> & changes)
        {
            if (!changes.empty())
                need_rebuild = true;
            if (!need_rebuild)
                return;
            if (fail_next_rebuild)
            {
                fail_next_rebuild = false;
                throw Exception(ErrorCodes::UNFINISHED, "rebuild failed"); /// caught and logged by sendNotifications()
            }
            ++rebuilds;
            need_rebuild = false; /// cleared only after a successful rebuild
        });

    notifier.onEntityRemoved(UUIDHelpers::generateV4(), AccessEntityType::ROW_POLICY);
    notifier.sendNotifications(); /// first attempt throws; the work stays pending
    EXPECT_EQ(rebuilds, 0u);

    /// No new event is queued, yet the by-type handler is still called and retries the pending rebuild.
    notifier.sendNotifications();
    EXPECT_EQ(rebuilds, 1u);

    /// Once it has succeeded an empty flush is a no-op, guarded by the handler's own flag.
    notifier.sendNotifications();
    EXPECT_EQ(rebuilds, 1u);
}

/// A by-id subscription is targeted: it is called only when its own entity changed in the batch (with all
/// of that entity's changes), so per-session ContextAccess subscriptions are not woken on every call.
TEST(AccessChangesNotifier, ByIdHandlerOnlyCalledWhenItsEntityChanged)
{
    AccessChangesNotifier notifier;

    auto tracked_id = UUIDHelpers::generateV4();
    auto other_id = UUIDHelpers::generateV4();
    size_t handler_calls = 0;
    size_t total_changes = 0;

    auto subscription = notifier.subscribeForChanges(
        tracked_id,
        [&](const std::vector<AccessChangesNotifier::Change> & changes)
        {
            ++handler_calls;
            total_changes += changes.size();
        });

    /// A batch that does not touch the tracked id must not call its handler.
    notifier.onEntityRemoved(other_id, AccessEntityType::ROW_POLICY);
    notifier.sendNotifications();
    EXPECT_EQ(handler_calls, 0u);

    /// A batch that touches the tracked id calls its handler once with that id's changes.
    notifier.onEntityRemoved(tracked_id, AccessEntityType::ROW_POLICY);
    notifier.onEntityRemoved(other_id, AccessEntityType::ROW_POLICY);
    notifier.sendNotifications();
    EXPECT_EQ(handler_calls, 1u);
    EXPECT_EQ(total_changes, 1u);
}

/// A handler may enqueue more changes while sendNotifications() runs: LDAP role mapping reacts to a Role
/// change by updating the mapped users through this same notifier. Those handler-generated changes must be
/// delivered before sendNotifications() returns (it drains until the queue is empty), otherwise a session's
/// by-id subscription would keep stale access until some unrelated change flushes the queue.
TEST(AccessChangesNotifier, HandlerEnqueuedChangesDeliveredInSameCall)
{
    AccessChangesNotifier notifier;

    auto user_id = UUIDHelpers::generateV4();
    bool user_change_injected = false;
    size_t user_changes_delivered = 0;

    /// Reacts to a Role change by enqueueing a User change, the way LDAP role mapping updates mapped users.
    auto role_subscription = notifier.subscribeForChanges(
        AccessEntityType::ROLE,
        [&](const std::vector<AccessChangesNotifier::Change> & changes)
        {
            if (!changes.empty() && !user_change_injected)
            {
                user_change_injected = true;
                notifier.onEntityRemoved(user_id, AccessEntityType::USER);
            }
        });

    /// Models a per-session ContextAccess subscription on that user.
    auto user_subscription = notifier.subscribeForChanges(
        user_id,
        [&](const std::vector<AccessChangesNotifier::Change> & changes) { user_changes_delivered += changes.size(); });

    notifier.onEntityRemoved(UUIDHelpers::generateV4(), AccessEntityType::ROLE);
    notifier.sendNotifications();

    /// The User change the Role handler enqueued is delivered within this single sendNotifications() call.
    EXPECT_TRUE(user_change_injected);
    EXPECT_EQ(user_changes_delivered, 1u);
}
