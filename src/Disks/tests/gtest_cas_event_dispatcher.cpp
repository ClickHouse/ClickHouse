#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasEventDispatcher.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefLedger.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEvent.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/tests/cas_test_helpers.h>

#include <atomic>
#include <chrono>
#include <future>
#include <latch>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

using namespace DB::Cas;
using DB::Cas::tests::idOf;
using DB::Cas::tests::u128Of;

namespace
{

/// A single-blob part: upload one blob, stage a one-entry manifest naming it, precommit + promote the
/// ref. Mirrors `publishOneBlobPart` in `gtest_cas_event_log.cpp` so a committed ref exists for
/// `resolveRef` to resolve and emit against.
void publishOneBlobPart(const PoolPtr & s, const String & ns, const String & ref, const String & payload)
{
    const RootNamespace nsr{ns};
    PartWriteInfo info;
    info.intended_ref = ns + "/" + ref;
    auto build = s->beginPartWrite(info);
    build->putBlob(idOf(payload), BlobSource::fromString(payload));
    ManifestEntry e;
    e.path = "data.bin";
    e.placement = EntryPlacement::Blob;
    e.ref = BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(u128Of(payload))};
    e.blob_size = payload.size();
    const ManifestId id = build->stageManifest({e});
    build->precommitAdd(nsr, ref, id);
    build->promote(nsr, ref, build->buildId(), id);
}

}

/// Concurrent emitters must be serialized: N threads emit M events each into a sink that appends to a
/// DELIBERATELY UNGUARDED vector. If the dispatcher did not serialize delivery the concurrent
/// `push_back`s would tear the vector (and TSan on that lane would flag the data race); serialized
/// delivery makes the unguarded append correct. The count/uniqueness assertions catch dropped or
/// duplicated events on any lane.
TEST(CASEventDispatcher, SerializesConcurrentEmitters)
{
    EventDispatcher disp;
    std::vector<CasEvent> seen;   /// unguarded on purpose -- the dispatcher is the only serialization
    disp.setSink([&](CasEvent e) { seen.push_back(std::move(e)); });

    constexpr int N = 8;     /// emitter threads
    constexpr int M = 250;   /// emits per thread
    std::latch start{N};     /// release all emitters together to maximize contention on the dispatcher
    std::vector<std::thread> threads;
    threads.reserve(N);
    for (int t = 0; t < N; ++t)
        threads.emplace_back([&, t]
        {
            start.arrive_and_wait();
            for (int m = 0; m < M; ++m)
            {
                CasEvent e;
                e.type = CasEventType::BlobPut;
                e.object_hash = std::to_string(t * M + m);
                disp.emit(std::move(e));
            }
        });
    for (auto & th : threads)
        th.join();

    ASSERT_EQ(seen.size(), static_cast<size_t>(N * M));
    std::set<String> ids;
    for (const auto & e : seen)
        ids.insert(e.object_hash);
    EXPECT_EQ(ids.size(), static_cast<size_t>(N * M)) << "every emitted event delivered exactly once";
}

/// A sink that emits again from inside its own delivery must not deadlock. The drain-loop design
/// never holds the dispatcher mutex across the sink call, so the reentrant `emit` acquires the mutex,
/// finds a drain already running, enqueues, and returns; the running loop delivers it after the
/// current sink returns. Delivery is synchronous on the emitting thread, so no timed wait is needed:
/// `emit` returns only after the whole queue (including the reentrant event) has drained.
TEST(CASEventDispatcher, ReentrantSinkDoesNotDeadlock)
{
    EventDispatcher disp;
    std::vector<CasEventType> delivered;
    std::atomic<bool> reentered_once{false};
    disp.setSink([&](CasEvent e)
    {
        delivered.push_back(e.type);
        if (e.type == CasEventType::BlobPut && !reentered_once.exchange(true))
        {
            CasEvent second;
            second.type = CasEventType::BlobDelete;
            disp.emit(std::move(second));
        }
    });

    CasEvent first;
    first.type = CasEventType::BlobPut;
    disp.emit(std::move(first));

    ASSERT_EQ(delivered.size(), 2u) << "both the original and the reentrant event must be delivered";
    EXPECT_EQ(delivered[0], CasEventType::BlobPut);
    EXPECT_EQ(delivered[1], CasEventType::BlobDelete)
        << "the reentrant event is drained AFTER the current sink returns, not recursively";
}

/// Test 17: a ledger emission must fire OUTSIDE the ledger lock. Install a sink that, on delivery of
/// a `RefResolve` event, re-enters a ledger read (`resolveRef`) that itself takes `state_mutex`; then
/// drive a real emitting `resolveRef` on a worker thread while a second thread emits upload-style
/// events concurrently. If `resolveRef` emitted while holding `state_mutex` (the pre-fix defect), the
/// worker would re-lock `state_mutex` on the same thread from inside the sink and self-deadlock. The
/// restructured emit (after the lock scope) lets the reentrant read take the lock freshly, and the
/// dispatcher serializes the concurrent upload emissions.
TEST(CASEventDispatcher, LedgerEmissionOutsideLocks)
{
    auto b = std::make_shared<InMemoryBackend>();
    std::vector<CasEvent> seen;   /// declared before the Pool so it outlives any late background emit
    std::mutex seen_mutex;
    auto s = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});

    const RootNamespace ns{"srv1/tbl"};
    const String ref = "all_0_0_0";
    publishOneBlobPart(s, ns.string(), ref, "the-resolvable-payload");

    std::atomic<bool> reentered{false};
    s->setEventSink([&](CasEvent e)
    {
        {
            std::lock_guard<std::mutex> g(seen_mutex);
            seen.push_back(e);
        }
        /// Re-enter a ledger read that takes `state_mutex`, exactly once (`Deferred` => this read
        /// itself emits nothing, so there is no unbounded emit recursion). Under the pre-fix code the
        /// outer `resolveRef` still holds `state_mutex` here, so this call self-deadlocks.
        if (e.type == CasEventType::RefResolve && !reentered.exchange(true))
            (void)s->resolveRef(ns, ref, false, ResolveAudit::Deferred);
    });

    std::promise<void> resolve_done;
    auto resolve_future = resolve_done.get_future();
    std::thread resolver([&]
    {
        (void)s->resolveRef(ns, ref);   /// ResolveAudit::Emit (default) -> emits RefResolve -> drives the sink
        resolve_done.set_value();
    });

    /// A second thread emits upload-task-style events concurrently with the resolve, so the dispatcher's
    /// serialization is exercised alongside the reentrancy path.
    std::thread uploader([&]
    {
        for (int i = 0; i < 32; ++i)
        {
            CasEvent up;
            up.type = CasEventType::BlobPut;
            up.object_hash = "up-" + std::to_string(i);
            up.reason = "concurrent upload-task emission";
            s->emitEvent(std::move(up));
        }
    });

    /// Bounded wait: the resolve is two in-memory map lookups plus queue drains -- microseconds of
    /// real work. 10 seconds is many orders of magnitude above that and only elapses if the
    /// emit-under-lock defect self-deadlocks the worker on `state_mutex`.
    const auto status = resolve_future.wait_for(std::chrono::seconds(10));
    ASSERT_EQ(status, std::future_status::ready)
        << "resolveRef with a re-entrant sink did not complete: emission is happening under state_mutex";
    resolver.join();
    uploader.join();

    EXPECT_TRUE(reentered.load()) << "the reentrant ledger read must have run";
    std::lock_guard<std::mutex> g(seen_mutex);
    size_t resolves = 0;
    size_t uploads = 0;
    for (const auto & e : seen)
    {
        if (e.type == CasEventType::RefResolve)
            ++resolves;
        else if (e.type == CasEventType::BlobPut)
            ++uploads;
    }
    EXPECT_GE(resolves, 1u) << "the driving resolve emitted its RefResolve";
    EXPECT_EQ(uploads, 32u) << "every concurrent upload emission was delivered exactly once";
}
