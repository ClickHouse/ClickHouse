#include <gtest/gtest.h>

#include <Access/AccessChangesNotifier.h>
#include <Access/Common/AccessEntityType.h>
#include <Core/UUID.h>

using namespace DB;

/// The per-entity caches (RowPolicyCache, RoleCache, SettingsProfilesCache, QuotaCache) rely on
/// subscribeForBatchFinished firing exactly once after a whole notification batch is drained, so that
/// they patch their per-entity maps in the cheap per-entity handlers and run the expensive O(enabled
/// sets * entities) rebuild only once per batch (a full refresh delivers one notification per entity).
TEST(AccessChangesNotifier, BatchFinishedCoalescesRecompute)
{
    AccessChangesNotifier notifier;

    size_t per_entity_calls = 0;
    size_t batch_calls = 0;
    size_t per_entity_calls_seen_by_last_batch = 0;

    auto entity_subscription = notifier.subscribeForChanges(
        AccessEntityType::ROW_POLICY,
        [&](const UUID &, const AccessEntityPtr &) { ++per_entity_calls; });

    auto batch_subscription = notifier.subscribeForBatchFinished(
        [&]
        {
            ++batch_calls;
            per_entity_calls_seen_by_last_batch = per_entity_calls;
        });

    /// A refresh that touches many entities: one notification per entity, drained in a single batch.
    static constexpr size_t num_entities = 16;
    for (size_t i = 0; i < num_entities; ++i)
        notifier.onEntityRemoved(UUIDHelpers::generateV4(), AccessEntityType::ROW_POLICY);

    notifier.sendNotifications();

    /// Every entity is dispatched, but the batch-finished hook runs exactly once - this is the
    /// coalescing the caches depend on (without it the rebuild would run num_entities times).
    EXPECT_EQ(per_entity_calls, num_entities);
    EXPECT_EQ(batch_calls, 1u);
    /// The batch hook runs after all per-entity notifications within the same sendNotifications() call,
    /// so the derived state is current once sendNotifications() returns.
    EXPECT_EQ(per_entity_calls_seen_by_last_batch, num_entities);

    /// A single DDL still recomputes exactly once.
    notifier.onEntityRemoved(UUIDHelpers::generateV4(), AccessEntityType::ROW_POLICY);
    notifier.sendNotifications();
    EXPECT_EQ(per_entity_calls, num_entities + 1);
    EXPECT_EQ(batch_calls, 2u);

    /// An empty queue still invokes the batch hook: real handlers early-return on their per-entity
    /// flag, so this is cheap, and it must keep firing so a rebuild that previously threw is retried
    /// without waiting for a fresh access change (see BatchFinishedRetriedAfterThrow).
    notifier.sendNotifications();
    EXPECT_EQ(batch_calls, 3u);
}

/// A per-batch rebuild that throws must not lose the pending work: the caches clear their "needs
/// rebuild" flag only on success, so the notifier has to keep invoking batch-finished handlers on
/// later sendNotifications() calls - including ones that drain no events - otherwise the derived
/// state stays stale until some unrelated access change happens to queue a notification (and an
/// up-to-date SYSTEM RELOAD USERS, which diffs to zero, would never trigger the retry).
TEST(AccessChangesNotifier, BatchFinishedRetriedAfterThrow)
{
    AccessChangesNotifier notifier;

    bool need_rebuild = false; /// mirrors RowPolicyCache::need_mix_filters and friends
    size_t rebuilds = 0;
    bool fail_next_rebuild = true;

    auto entity_subscription = notifier.subscribeForChanges(
        AccessEntityType::ROW_POLICY,
        [&](const UUID &, const AccessEntityPtr &) { need_rebuild = true; });

    auto batch_subscription = notifier.subscribeForBatchFinished(
        [&]
        {
            if (!need_rebuild)
                return;
            if (fail_next_rebuild)
            {
                fail_next_rebuild = false;
                throw std::runtime_error("rebuild failed"); /// caught and logged by sendNotifications()
            }
            ++rebuilds;
            need_rebuild = false; /// cleared only after a successful rebuild
        });

    notifier.onEntityRemoved(UUIDHelpers::generateV4(), AccessEntityType::ROW_POLICY);
    notifier.sendNotifications(); /// first attempt throws; the work stays pending
    EXPECT_EQ(rebuilds, 0u);

    /// No new event is queued, yet the pending rebuild must still be retried and now succeed.
    notifier.sendNotifications();
    EXPECT_EQ(rebuilds, 1u);

    /// Once it has succeeded an empty flush is a no-op, guarded by the handler's own flag.
    notifier.sendNotifications();
    EXPECT_EQ(rebuilds, 1u);
}
