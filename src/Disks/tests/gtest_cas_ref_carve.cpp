#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefLedger.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefProtocol.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefLogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFormat.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <future>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <thread>
#include <vector>

/// Task 7 (stage-1 §2): the ref-flush two-phase carve and the validation loop's publish ordering.
///
/// Two exception-safety windows are pinned here, both driven through the `setCarveHookForTest` fault
/// seam (which fires `std::bad_alloc` at named carve/validation phase points):
///
///  - The carve must PLAN (scan `pending` without popping; build the selection and every reservation)
///    and only then PUBLISH (pop + append under the same continuous `ref_queue_mutex` hold, using only
///    non-throwing moves/copies). A throw anywhere in the plan must leave the queue byte-for-byte intact
///    so no already-selected item is stranded (removed from `pending` yet never completed) and no waiter
///    hangs. The pre-fix carve interleaved pops with the allocating `seen_refs`/`batch` growth, so a
///    throw after the first pop stranded popped items and hung their waiters forever — the behavioural
///    signature this suite demonstrates.
///  - The per-item validation loop must reserve `final_ops`/`survivors` growth BEFORE applying the item
///    to `working`, and publish only past all throwing points. The pre-fix loop moved `working` before
///    those allocations, so a failure there left a failed item's effects in `working` and — when the
///    throw fell between the two accumulator writes — its ops already in the durably-committed
///    transaction while its own caller was told the append failed.
///
/// The suite name is prefixed `RefWriter` so it is covered by the `RefWriter*` unit-test gate filter.

using namespace DB::Cas;
using Phase = CasRefLedger::CarvePhaseForTest;

namespace
{

PoolPtr openPool(const BackendPtr & backend)
{
    /// A fresh pool with no residue: `seedPoolMetaForRestart` is idempotent and a no-op here (mirrors the
    /// ref-writer suite's `openPool`), it just lets `beginPartWrite` bootstrap over a valid `_pool_meta`.
    DB::Cas::tests::seedPoolMetaForRestart(*backend);
    return Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
}

/// A legal blob-free part: stage an empty manifest, precommit, promote — enough to leave one committed
/// ref in `ns` that a later `dropRef` can co-batch. Mirrors the ref-writer suite's `publishEmptyPart`.
///
/// Stage B (Task 4-C): pin `ns` to the sentinel before the first real touch, mirroring the same fix in
/// `gtest_cas_ref_writer.cpp`'s `startBuildFor` and `gtest_cas_ref_chunked_flush.cpp`'s
/// `publishEmptyPart` -- every test in this file births its namespace here before any fault
/// injection/verification that separately computes a key via `DB::Cas::tests::fixture::fixtureLife(ns)`.
void publishEmptyPart(const PoolPtr & s, const RootNamespace & ns, const String & ref)
{
    DB::Cas::tests::casAdmitRecoverableEntry(s->backend(), s->layout(), ns, s->liveWriterEpoch());
    PartWriteInfo info;
    info.intended_namespace = ns;
    info.intended_ref = ns.string() + "/" + ref;
    auto build = s->beginPartWrite(info);
    const ManifestId id = build->stageManifest({});
    build->precommitAdd(ns, ref, id);
    build->promote(ns, ref, build->buildId(), id);
}

/// Shared, heap-backed synchronisation state for one case. Heap-backing (captured by `shared_ptr` into
/// the hooks and caller threads) is what makes a leaked/detached hung thread safe on the RED path: the
/// thread keeps its own references alive, so nothing it touches is destroyed underneath it.
struct CaseSync
{
    std::mutex m;
    std::condition_variable cv;
    bool entered = false;                        /// guarded by m: the first flush reached the pre-carve hook
    /// Per-`CarvePhaseForTest` invocation counter, indexed by `static_cast<int>(phase)`. Sized off the
    /// enum's last enumerator rather than a literal: the array was already undersized once (it predates
    /// `ChunkReseed`; `PostDurableInstall` and `PostInstallPreAck` followed), and it is out of bounds only
    /// because every call site happens to filter to a lower-numbered phase first -- a trap for the next
    /// phase added.
    std::atomic<int> phase_hits[static_cast<size_t>(CasRefLedger::CarvePhaseForTest::PostInstallPreAck) + 1] = {};
};

/// The newest `_log/` transaction currently present for `ns`, decoded from the backend directly (no Pool
/// cache). Used to inspect exactly what a flush durably committed.
std::optional<RefLogTxn> newestLogTxn(DB::Cas::Backend & backend, const DB::Cas::Layout & layout, const RootNamespace & ns)
{
    std::optional<RefTxnId> newest;
    String cursor;
    for (;;)
    {
        const ListPage page = backend.list(layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)), cursor, 1000);
        for (const ListedKey & lk : page.keys)
        {
            const auto parsed = layout.parseRefObjectKey(lk.key);
            if (parsed && parsed->life_id == DB::Cas::tests::fixture::fixtureLife(ns).incarnation
                && parsed->kind == RefObjectKind::Log
                && (!newest || *newest < parsed->txn_id))
                newest = parsed->txn_id;
        }
        if (page.next_cursor.empty())
            break;
        cursor = page.next_cursor;
    }
    if (!newest)
        return std::nullopt;
    const auto got = backend.get(layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), *newest));
    if (!got)
        return std::nullopt;
    return decodeRefLogTxn(openObject(FormatId::RefLog, got->bytes), ns.string(), *newest);
}

/// Counts `OwnerTransition` removal ops (old binding present, no new binding) naming `ref_name` across
/// EVERY committed `_log/` transaction of `ns`.
size_t committedRemovalCountForRef(DB::Cas::Backend & backend, const DB::Cas::Layout & layout,
                                   const RootNamespace & ns, const String & ref_name)
{
    size_t count = 0;
    String cursor;
    for (;;)
    {
        const ListPage page = backend.list(layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)), cursor, 1000);
        for (const ListedKey & lk : page.keys)
        {
            const auto parsed = layout.parseRefObjectKey(lk.key);
            if (!parsed || parsed->life_id != DB::Cas::tests::fixture::fixtureLife(ns).incarnation
                || parsed->kind != RefObjectKind::Log)
                continue;
            const auto got = backend.get(layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), parsed->txn_id));
            if (!got)
                continue;
            const RefLogTxn txn = decodeRefLogTxn(openObject(FormatId::RefLog, got->bytes), ns.string(), parsed->txn_id);
            for (const RefOp & op : txn.ops)
                if (op.kind == RefOpKind::OwnerTransition && op.old_binding.has_value()
                    && !op.new_binding.has_value() && op.old_binding->ref_name == ref_name)
                    ++count;
        }
        if (page.next_cursor.empty())
            break;
        cursor = page.next_cursor;
    }
    return count;
}

/// One queued append driven on its own thread, with a future that becomes ready only when the caller's
/// `dropRef` RETURNS (normally or by throwing). A caller whose item was stranded never returns, so its
/// future stays not-ready — a bounded `wait_for` on it is the hung-waiter detector.
struct Caller
{
    std::thread t;
    std::future<std::exception_ptr> fut;
    String ref;
};

Caller launchDrop(const PoolPtr & store, const RootNamespace & ns, const String & ref)
{
    auto prom = std::make_shared<std::promise<std::exception_ptr>>();
    std::future<std::exception_ptr> fut = prom->get_future();
    std::thread t([store, ns, ref, prom]
    {
        std::exception_ptr err;
        try { store->dropRef(ns, ref); }
        catch (...) { err = std::current_exception(); }
        prom->set_value(err);
    });
    return Caller{std::move(t), std::move(fut), ref};
}

/// Stages three compatible drops (leader "a" plus followers "b","c") into one carve, injects
/// `std::bad_alloc` at `target_phase` on its `target_ordinal`-th firing, then asserts that no caller
/// hangs and the queue drains. Returns true iff a caller hung (the stranded-item signature).
///
/// On the fixed (two-phase) carve, a plan-phase throw pops nothing: the leader's own item is failed by
/// the leadership-exit guard and the untouched followers commit on the next leader's retry. On the
/// pre-fix interleaved carve, the same throw strands already-popped followers, whose waiters hang.
bool runPlanPointCase(Phase target_phase, int target_ordinal, const char * label)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{String("srv1/carve_") + label};
    publishEmptyPart(store, ns, "a");
    publishEmptyPart(store, ns, "b");
    publishEmptyPart(store, ns, "c");

    auto sync = std::make_shared<CaseSync>();
    /// Block the first flush's leader in the pre-carve window until all three items are queued, forcing a
    /// deterministic three-item batch. Heap-backed captures (see `CaseSync`) keep this safe even if a
    /// stranded follower later spins through it on the RED path.
    store->setRefPreCarveHookForTest([sync, store, ns]
    {
        std::unique_lock lk(sync->m);
        if (sync->entered)
            return;   /// only the first carve blocks; retries proceed straight through
        sync->entered = true;
        sync->cv.notify_all();
        /// Bounded (10s) so a staging bug bounds the wait instead of blocking the whole suite; the
        /// predicate is normally satisfied well before the deadline.
        sync->cv.wait_for(lk, std::chrono::seconds(10), [&] { return store->refQueuePendingForTest(ns) >= 3; });
    });
    store->setCarveHookForTest([sync, target_phase, target_ordinal](Phase ph)
    {
        if (ph != target_phase)
            return;
        if (sync->phase_hits[static_cast<int>(ph)].fetch_add(1) + 1 == target_ordinal)
            throw std::bad_alloc{};
    });

    Caller ca = launchDrop(store, ns, "a");
    {
        std::unique_lock lk(sync->m);
        sync->cv.wait_for(lk, std::chrono::seconds(10), [&] { return sync->entered; });
    }
    Caller cb = launchDrop(store, ns, "b");
    Caller cc = launchDrop(store, ns, "c");
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (store->refQueuePendingForTest(ns) < 3 && std::chrono::steady_clock::now() < deadline)
            std::this_thread::yield();
    }
    sync->cv.notify_all();   /// release the pre-carve hook now its (>=3 pending) predicate holds

    bool hung = false;
    std::vector<Caller *> callers = {&ca, &cb, &cc};
    for (Caller * c : callers)
    {
        /// Bounded (5s): a stranded item never completes, so this is where the hang surfaces.
        if (c->fut.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
        {
            hung = true;
            EXPECT_TRUE(false) << label << ": caller for ref '" << c->ref
                               << "' never returned within 5s — its item was stranded (removed from "
                                  "pending, never completed) and the waiter hung";
            /// Cannot join a permanently-hung thread. Detach it; its heap-backed captures (including a
            /// `store` copy) keep everything it touches alive until the process exits. Leave the hooks
            /// installed — clearing them here would race the detached thread's `std::function` read.
            c->t.detach();
        }
        else
        {
            c->t.join();
        }
    }
    if (hung)
        return true;

    /// GREEN: everything joined, so clearing the hooks now cannot race any live flush.
    store->setRefPreCarveHookForTest(nullptr);
    store->setCarveHookForTest(nullptr);
    EXPECT_EQ(store->refQueuePendingForTest(ns), 0u) << label << ": the append queue did not fully drain";
    /// The leader ("a") is the item whose plan threw; the guard fails it, so its drop must NOT commit.
    EXPECT_TRUE(store->resolveRef(ns, "a").has_value())
        << label << ": the failed leader's drop must not have committed";
    /// The untouched followers commit on retry.
    EXPECT_FALSE(store->resolveRef(ns, "b").has_value()) << label << ": survivor 'b' must have been dropped";
    EXPECT_FALSE(store->resolveRef(ns, "c").has_value()) << label << ": survivor 'c' must have been dropped";
    return false;
}

}

/// Test 7: a throw at any plan-phase point of the carve must leave the queue intact and hang no waiter.
/// RED on the pre-fix interleaved carve (a plan-point throw after the first pop strands followers whose
/// waiters then hang, tripping the 5s bounded wait); GREEN on the two-phase carve.
TEST(CASRefWriterCarve, CarveThrowLeavesQueueIntact)
{
    /// `PlanSeenRefs`/`PlanBatchGrow` fire once per scanned item; injecting on the third firing strands a
    /// popped follower under the old carve. `PlanReserveOwned` fires once, after the whole selection is
    /// (under the old carve) already popped, stranding every follower. `ASSERT_FALSE` stops at the first
    /// demonstrated hang so at most one case leaks a detached thread on the RED path.
    ASSERT_FALSE(runPlanPointCase(Phase::PlanSeenRefs, 3, "PlanSeenRefs"));
    ASSERT_FALSE(runPlanPointCase(Phase::PlanBatchGrow, 3, "PlanBatchGrow"));
    ASSERT_FALSE(runPlanPointCase(Phase::PlanReserveOwned, 1, "PlanReserveOwned"));
}

/// Test 8: an allocation failure at the per-item accumulation point must leave the failed item's effects
/// out of BOTH `working` (the in-memory committed state) and the durable transaction. RED on the pre-fix
/// loop (which moved `working` and appended `final_ops` before the throwing point, so the failed drop
/// committed while its caller was told it failed); GREEN on the reserve-before-publish loop.
TEST(CASRefWriterCarve, ValidationAllocFailureLeavesWorkingClean)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPool(backend);
    const DB::Cas::Layout & layout = store->layout();
    const RootNamespace ns{"srv1/carve_validate"};
    publishEmptyPart(store, ns, "x");
    publishEmptyPart(store, ns, "y");
    ASSERT_TRUE(store->resolveRef(ns, "x").has_value());
    ASSERT_TRUE(store->resolveRef(ns, "y").has_value());

    auto sync = std::make_shared<CaseSync>();
    store->setRefPreCarveHookForTest([sync, store, ns]
    {
        std::unique_lock lk(sync->m);
        if (sync->entered)
            return;
        sync->entered = true;
        sync->cv.notify_all();
        sync->cv.wait_for(lk, std::chrono::seconds(10), [&] { return store->refQueuePendingForTest(ns) >= 2; });
    });
    /// Fail the FIRST admitted item (the leader's own drop of "x") at its accumulation point.
    store->setCarveHookForTest([sync](Phase ph)
    {
        if (ph != Phase::ValidateFinalOps)
            return;
        if (sync->phase_hits[static_cast<int>(ph)].fetch_add(1) + 1 == 1)
            throw std::bad_alloc{};
    });

    Caller cx = launchDrop(store, ns, "x");
    {
        std::unique_lock lk(sync->m);
        sync->cv.wait_for(lk, std::chrono::seconds(10), [&] { return sync->entered; });
    }
    Caller cy = launchDrop(store, ns, "y");
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (store->refQueuePendingForTest(ns) < 2 && std::chrono::steady_clock::now() < deadline)
            std::this_thread::yield();
    }
    sync->cv.notify_all();

    ASSERT_EQ(cx.fut.wait_for(std::chrono::seconds(10)), std::future_status::ready) << "drop x must not hang";
    ASSERT_EQ(cy.fut.wait_for(std::chrono::seconds(10)), std::future_status::ready) << "drop y must not hang";
    const std::exception_ptr x_err = cx.fut.get();
    const std::exception_ptr y_err = cy.fut.get();
    cx.t.join();
    cy.t.join();
    store->setRefPreCarveHookForTest(nullptr);
    store->setCarveHookForTest(nullptr);

    /// The injected item's own caller was told the append failed.
    ASSERT_TRUE(x_err != nullptr) << "drop x's caller must observe the injected allocation failure";
    /// The co-batched survivor committed cleanly.
    EXPECT_TRUE(y_err == nullptr) << "the co-batched survivor drop y must commit";

    /// (1) `working`/committed state stays clean: x's drop, whose caller failed, must NOT have taken
    /// effect — x remains resolvable.
    EXPECT_TRUE(store->resolveRef(ns, "x").has_value())
        << "the failed item's drop leaked into the committed state — `working` was not kept clean";
    EXPECT_FALSE(store->resolveRef(ns, "y").has_value()) << "survivor drop y must be committed";

    /// (2) Decode the committed object: no committed ref-log transaction may carry x's removal op, and
    /// exactly the survivor's removal must be present.
    EXPECT_EQ(committedRemovalCountForRef(*backend, layout, ns, "x"), 0u)
        << "the failed item's removal op leaked into a durably-committed ref-log object";
    EXPECT_EQ(committedRemovalCountForRef(*backend, layout, ns, "y"), 1u)
        << "the survivor's removal op must be present in exactly one committed ref-log object";

    const auto newest = newestLogTxn(*backend, layout, ns);
    ASSERT_TRUE(newest.has_value());
    for (const RefOp & op : newest->ops)
        if (op.kind == RefOpKind::OwnerTransition && op.old_binding.has_value() && !op.new_binding.has_value())
            EXPECT_NE(op.old_binding->ref_name, String("x"))
                << "the newest committed transaction must not contain the failed item's removal";
}
