#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefProtocol.h>
#include <Common/Exception.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

/// Task 1: ref-lane exception-safety. A queue leader that throws BEFORE carving its compatible batch
/// must not leave its own enqueued item stranded in `rt->pending`. If it does, a later leader (a woken
/// follower) carves the stranded item and runs its `build_ops` closure long after the original caller's
/// stack -- which the production `[&]` closures capture by reference -- has unwound: a use-after-free.
///
/// These tests drive the fault through the SAME pre-carve injection point production leaders pass
/// (`setRefPreCarveHookForTest`, invoked inside `flushRefBatch` immediately before the batch is carved).
/// The suite name is prefixed `RefWriter` so it is covered by the `RefWriter*` unit-test gate filter.

namespace DB::ErrorCodes
{
extern const int CORRUPTED_DATA;
}

using namespace DB::Cas;

namespace
{

PoolPtr openPoolForRefLane(const BackendPtr & backend)
{
    return Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
}

}

/// A SOLO faulted caller must not leave its own item behind in the pending queue. Before the fix, the
/// leader's `appendRefOps` catch reset `leader_active` and rethrew but never completed / de-pended the
/// leader's own item, so it was stranded in `rt->pending` with `done == false` forever (nothing left to
/// carve it) -- the deterministic, sanitizer-independent shape of the stranded-item defect.
TEST(CASRefWriterLaneExceptionSafety, SoloLeaderThrowBeforeCarveDrainsOwnItem)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForRefLane(backend);
    const RootNamespace ns{"srv1/reflane_solo"};

    std::atomic<int> fault_armed{1};
    store->setRefPreCarveHookForTest([&]
    {
        if (fault_armed.exchange(0) == 1)
            throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "injected pre-carve fault");
    });

    bool threw = false;
    try
    {
        store->appendRefOps(ns, MutationScope::ref("ref_solo"),
            [](const RefTableState &) -> std::vector<RefOp> { return {}; },
            RootMutationOrigin::Writer, RootMutationKind::Publish);
    }
    catch (const DB::Exception &)
    {
        threw = true;
    }
    store->setRefPreCarveHookForTest(nullptr);

    EXPECT_TRUE(threw) << "the faulted solo caller must observe the injected error";
    EXPECT_EQ(store->refQueuePendingForTest(ns), 0u)
        << "the leader's own item was left stranded in rt->pending after it threw before carving";
}

/// Two concurrent callers on one namespace. The first flush's leader throws before carving; a woken
/// follower then leads. Before the fix, the follower carved the faulted leader's STILL-pending item and
/// ran its `build_ops` closure -- the use-after-free window. This asserts, sanitizer-independently, that
/// the follower never invokes the faulted caller's closure, that the queue drains, and that the
/// non-faulted caller still completes.
TEST(CASRefWriterLaneExceptionSafety, FollowerNeverRunsStrandedLeaderClosure)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForRefLane(backend);
    const RootNamespace ns{"srv1/reflane_follower"};

    std::atomic<int> fault_armed{1};
    /// The leader parks HERE, at the pre-carve point, until the main thread has queued the follower
    /// behind it -- and only then throws. Parking (rather than letting the leader race ahead while the
    /// main thread polls the queue depth) is what makes the interleaving this test is about --
    /// "the leader throws WHILE a follower is waiting for the baton" -- deterministic. The previous
    /// formulation polled `refQueuePendingForTest(ns) >= 1` from the main thread AFTER starting t1,
    /// which loses a race the scheduler decides: t1 could enqueue, take the baton, throw, and have its
    /// item erased by `completeOwnedItemsAndReleaseLeadership` before the main thread was ever
    /// scheduled to sample -- after which `pending` is 0 forever and the poll spins until the harness
    /// is killed. Invisible on an idle 32-core box (10/10 green) and a guaranteed hang under
    /// contention (8/8 when pinned to one CPU with `taskset -c 3`).
    std::mutex hook_mutex;
    std::condition_variable hook_cv;
    bool leader_parked_at_precarve = false;   /// guarded by hook_mutex
    bool release_leader = false;              /// guarded by hook_mutex
    store->setRefPreCarveHookForTest([&]
    {
        /// Park+throw only on the FIRST leader flush, so the follower (or a re-drive) can proceed.
        if (fault_armed.exchange(0) != 1)
            return;
        {
            std::lock_guard announce(hook_mutex);
            leader_parked_at_precarve = true;
        }
        hook_cv.notify_all();
        {
            std::unique_lock wait_for_follower(hook_mutex);
            hook_cv.wait(wait_for_follower, [&] { return release_leader; });
        }
        throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "injected pre-carve fault");
    });

    /// Set by the faulted caller's own closure iff a DIFFERENT thread (a follower leader) ever runs it --
    /// i.e. the stranded item was carved by someone other than its owner. This is the direct, portable
    /// signature of the use-after-free the fix prevents.
    std::atomic<std::thread::id> faulted_owner{};
    std::atomic<bool> faulted_closure_ran_on_follower{false};

    std::atomic<int> ok{0};
    auto caller = [&](int seq, bool is_faulted)
    {
        try
        {
            store->appendRefOps(ns, MutationScope::ref("ref_" + std::to_string(seq)),
                [&, is_faulted](const RefTableState &) -> std::vector<RefOp>
                {
                    if (is_faulted && std::this_thread::get_id() != faulted_owner.load())
                        faulted_closure_ran_on_follower.store(true);
                    return {};
                },
                RootMutationOrigin::Writer, RootMutationKind::Publish);
            ok.fetch_add(1);
        }
        catch (const DB::Exception &) // NOLINT(bugprone-empty-catch)
        {
            /// The faulted caller may see the injected error; that is expected.
        }
    };

    /// Serialize the two callers so the fault deterministically lands on the FIRST one to lead: t1
    /// enqueues, takes the baton, and PARKS at the pre-carve hook; t2 then queues behind it as a
    /// follower; only then is t1 released to throw.
    std::thread t1([&]
    {
        faulted_owner.store(std::this_thread::get_id());
        caller(1, /*is_faulted=*/true);
    });
    {
        std::unique_lock wait_for_leader(hook_mutex);
        hook_cv.wait(wait_for_leader, [&] { return leader_parked_at_precarve; });
    }
    std::thread t2([&] { caller(2, /*is_faulted=*/false); });
    /// The parked leader cannot drain anything, so this poll cannot miss its window: t1's own item is
    /// already in `pending`, and the count reaches 2 as soon as t2 has enqueued.
    while (store->refQueuePendingForTest(ns) < 2)
        std::this_thread::yield();
    {
        std::lock_guard release(hook_mutex);
        release_leader = true;
    }
    hook_cv.notify_all();

    t1.join();
    t2.join();
    store->setRefPreCarveHookForTest(nullptr);

    EXPECT_FALSE(faulted_closure_ran_on_follower.load())
        << "a follower leader carved and ran the stranded faulted caller's build_ops closure (use-after-free)";
    EXPECT_EQ(store->refQueuePendingForTest(ns), 0u) << "an item was stranded in rt->pending";
    EXPECT_GE(ok.load(), 1) << "the non-faulted caller must complete cleanly";
}

/// codex stage-1 review (Important): an allocation exception at the PRE-TENURE point -- the first
/// allocation that builds the leader's responsibility set, BEFORE `leader_active` is published -- must
/// not permanently strand the append-lane baton. Before the fix the throwing allocation fired AFTER
/// `leader_active = true` (and after the queue mutex was released), leaving the baton held with no live
/// leader and the caller's item stuck in `pending`: every later writer on the namespace would wait
/// forever at the leader-election cv, and shutdown draining could only time out. This drives the fault
/// through the dedicated pre-tenure seam and asserts, deterministically (no hang), that the lane is left
/// idle: the item is un-enqueued and the baton is un-taken.
TEST(CASRefWriterLaneExceptionSafety, PreTenureAllocFailureReleasesBaton)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForRefLane(backend);
    const RootNamespace ns{"srv1/reflane_pretenure"};

    std::atomic<int> fault_armed{1};
    store->setRefPreTenureHookForTest([&]
    {
        if (fault_armed.exchange(0) == 1)
            throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "injected pre-tenure fault");
    });

    bool threw = false;
    try
    {
        store->appendRefOps(ns, MutationScope::ref("ref_pretenure"),
            [](const RefTableState &) -> std::vector<RefOp> { return {}; },
            RootMutationOrigin::Writer, RootMutationKind::Publish);
    }
    catch (const DB::Exception &)
    {
        threw = true;
    }
    store->setRefPreTenureHookForTest(nullptr);

    EXPECT_TRUE(threw) << "the faulted caller must observe the injected error";
    EXPECT_EQ(store->refQueuePendingForTest(ns), 0u)
        << "a pre-tenure allocation failure left the caller's item stranded in rt->pending";
    EXPECT_FALSE(store->refLeaderActiveForTest(ns))
        << "a pre-tenure allocation failure left the append-lane baton held with no live leader";
}
