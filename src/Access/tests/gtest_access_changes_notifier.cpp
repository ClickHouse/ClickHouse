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

    /// An empty queue must not invoke the batch hook (nothing changed, nothing to rebuild).
    notifier.sendNotifications();
    EXPECT_EQ(batch_calls, 2u);
}
