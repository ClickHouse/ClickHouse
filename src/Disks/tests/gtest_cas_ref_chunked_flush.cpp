#include <gtest/gtest.h>

#include "config.h"

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
#include <Common/ProfileEvents.h>

#include <IO/S3Common.h>

#include <Poco/Exception.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace DB::ErrorCodes
{
extern const int LIMIT_EXCEEDED;
extern const int CORRUPTED_DATA;
extern const int NETWORK_ERROR;
}

namespace ProfileEvents
{
extern const Event CASRefBatchFlushes;
extern const Event CASRefBatchedMutations;
extern const Event CASRefSnapshotPublishDispatched;
}

/// Task 8 (stage-1 §3 "Budget: counts only, chunked flush"): the counts-only admission caps --
/// `ref_txn_max_ops` (5000), the carve item cap `kMaxRefBatch` (1000), and the per-op size cap
/// `ref_op_max_bytes` (4096 bytes on normal-class ops) -- plus their failure-isolation contract: a
/// single item whose own op count, or whose one op's encoded size, exceeds its cap fails ALONE; a
/// neighbor co-batched into the same flush still commits. `ref_txn_max_ops` is checked exactly (the
/// `build_ops` result's size), and the per-op cap is checked by encoding exactly one op at a time --
/// no accumulation, matching the admission machinery this replaces. T9 (removal-class detection by
/// op inspection) and T10 (chunked flush across a whole-batch op-count overflow) extend this file;
/// this task adds only the per-item / per-op isolation tests and the canonical round-trip leg of
/// test 12 (the maximum legally-admissible normal-class transaction).
///
/// The suite name is prefixed `RefWriter` so it is covered by the `RefWriter*` unit-test gate filter.

using namespace DB::Cas;
using DB::Cas::tests::committedRow;
using DB::Cas::tests::minimalLiveSnapshot;
using DB::Cas::tests::writeRefSnapshotRaw;

namespace
{

PoolPtr openPool(const BackendPtr & backend)
{
    /// A fresh pool with no residue, mirroring the T7 carve suite's `openPool`.
    ///
    /// The FROZEN clock is load-bearing, not hygiene. `CasMountRuntime::refAppendFenceOk` gates every
    /// controlled attempt against `boot_ms_fn`, and with the compiled defaults (mount_lease_ttl_ms
    /// 30000, safety margin 7000) a pool opened on the REAL clock fences itself ~23s later — no
    /// background renewal advances that deadline in a unit-test pool. Every test in this suite is
    /// about chunking and op-caps, none about wall-clock lease behaviour, so any of them that runs
    /// long enough simply dies of an unrelated fence trip: `DropNamespaceOverOpCapSucceeds` (5200
    /// refs) takes 43-65s under a sanitizer and failed deterministically on all three sanitizer CI
    /// builds with `txn is UNCERTAIN (retry budget exhausted)` — the pre-attempt fence reject, not a
    /// real retry exhaustion. Same artifact, same fix as
    /// `CASPartWriteTxn.ManifestCapEncodedBytesJustUnderStagesSuccessfully` (2026-07-18): decouple the
    /// fence from execution speed. The waits in this file are `steady_clock` timeouts on futures and
    /// condvars, which are unaffected by this injection.
    DB::Cas::tests::seedPoolMetaForRestart(*backend);
    return Pool::open(backend, PoolConfig{
        .pool_prefix = "p", .server_root_id = "test", .boot_ms_fn = [] { return uint64_t{0}; }});
}

/// A legal blob-free part: stage an empty manifest, precommit, promote -- enough to leave one
/// committed ref (and a `Live` table) that a later co-batched item can join.
///
/// Stage B (Task 4-C): pin `ns` to the sentinel before the first real touch -- the ONE choke point
/// every test in this file uses to birth its namespace, before any `launchAppendOps`/`launchAppend`/
/// `launchDrop` call. Several tests separately compute an expected key via
/// `DB::Cas::tests::fixture::fixtureLife(ns)` for verification/fault injection; without this the real
/// production birth mints a random incarnation and those computed keys land nowhere real.
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

/// One queued append (or drop) driven on its own thread; the future becomes ready only when the call
/// RETURNS (normally or by throwing). Mirrors `gtest_cas_ref_carve.cpp`'s `Caller`/`launchDrop`.
struct Caller
{
    std::thread t;
    std::future<std::exception_ptr> fut;
};

Caller launchAppend(const PoolPtr & store, const RootNamespace & ns, MutationScope scope,
                     std::function<std::vector<RefOp>(const RefTableState &)> build_ops)
{
    auto prom = std::make_shared<std::promise<std::exception_ptr>>();
    std::future<std::exception_ptr> fut = prom->get_future();
    std::thread t([store, ns, scope, build_ops, prom]
    {
        std::exception_ptr err;
        try { store->appendRefOps(ns, scope, build_ops, RootMutationOrigin::Writer, RootMutationKind::Publish); }
        catch (...) { err = std::current_exception(); }
        prom->set_value(err);
    });
    return Caller{std::move(t), std::move(fut)};
}

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
    return Caller{std::move(t), std::move(fut)};
}

/// `n` filler ops for a `build_ops` result whose only purpose is to overflow the per-item op-count
/// cap. They are NOT inert-when-applied: a default-constructed op is a `NamespaceBirth`, which throws
/// `CORRUPTED_DATA` ("namespace_birth while already Live") if it were ever applied to the pre-published
/// namespace. The load-bearing safety property is that the count check fires BEFORE any of these ops is
/// applied or otherwise inspected.
std::vector<RefOp> fillerOps(size_t n)
{
    return std::vector<RefOp>(n, RefOp{});
}

/// A zero-padded ref name for index `i`, so `kTotalRefs` names sort in the same order as their index
/// (the snapshot fixture's committed rows must already be sorted by `ref_name`).
String paddedRefName(size_t i)
{
    String s = std::to_string(i);
    return "ref_" + String(6 - s.size(), '0') + s;
}

/// A single `SetPublishedAt` op whose `ref_name` is padded so its OWN encoded size (`encodedOpSize`)
/// is exactly `target_bytes`. Every added 'a' is one un-escaped byte in the JSON ref-name string, so
/// the size grows one-for-one; `checkCanonicalRefName` imposes no length limit, so this stays a
/// valid, merely over-long, canonical ref name.
RefOp paddedSetPublishedAtOp(size_t target_bytes)
{
    RefOp op;
    op.kind = RefOpKind::SetPublishedAt;
    op.ref_name = "r";
    op.expected_manifest_ref = ManifestRef{1, 1, 1};
    op.published_at_ms = 0;
    const size_t base = encodedOpSize(op);
    op.ref_name = "r" + String(target_bytes - base, 'a');
    return op;
}

/// Blocks the FIRST flush's leader in the pre-carve window until `expected_pending` items are queued,
/// forcing a deterministic multi-item batch (mirrors `gtest_cas_ref_carve.cpp`'s `CaseSync`/pre-carve
/// hook pattern). Only the first carve blocks; retries proceed straight through.
struct CaseSync
{
    std::mutex m;
    std::condition_variable cv;
    bool entered = false;
};

void armPreCarveBlock(const PoolPtr & store, const RootNamespace & ns, const std::shared_ptr<CaseSync> & sync, size_t expected_pending)
{
    store->setRefPreCarveHookForTest([sync, store, ns, expected_pending]
    {
        std::unique_lock lk(sync->m);
        if (sync->entered)
            return;
        sync->entered = true;
        sync->cv.notify_all();
        /// Bounded (10s) so a staging bug bounds the wait instead of blocking the whole suite.
        sync->cv.wait_for(lk, std::chrono::seconds(10), [&] { return store->refQueuePendingForTest(ns) >= expected_pending; });
    });
}

void waitEntered(const std::shared_ptr<CaseSync> & sync)
{
    std::unique_lock lk(sync->m);
    sync->cv.wait_for(lk, std::chrono::seconds(10), [&] { return sync->entered; });
}

void waitPendingAtLeast(const PoolPtr & store, const RootNamespace & ns, size_t n)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (store->refQueuePendingForTest(ns) < n && std::chrono::steady_clock::now() < deadline)
        std::this_thread::yield();
}

/// Asserts `err` is non-null and carries EXACTLY `expected_code` -- distinguishes the new counts-only
/// admission checks (`LIMIT_EXCEEDED`) from any other per-item validation failure.
void expectFailedWithCode(const std::exception_ptr & err, int expected_code, const char * what)
{
    ASSERT_TRUE(err != nullptr) << what << ": the caller must observe the admission-cap error";
    try
    {
        std::rethrow_exception(err);
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), expected_code) << what;
    }
}

}

/// Test 10 (spec §3 "Oversized item / oversized op fail alone"): an item whose OWN op count exceeds
/// `ref_txn_max_ops` fails alone -- its ops never enter the batch's transaction -- and a co-batched
/// neighbor still commits.
TEST(CASRefWriterChunkedFlush, OversizedItemFailsAlone)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/chunked_oversized_item"};
    publishEmptyPart(store, ns, "neighbor");
    ASSERT_TRUE(store->resolveRef(ns, "neighbor").has_value());

    auto sync = std::make_shared<CaseSync>();
    armPreCarveBlock(store, ns, sync, 2);

    Caller oversized = launchAppend(store, ns, MutationScope::ref("oversized"),
        [](const RefTableState &) -> std::vector<RefOp> { return fillerOps(ref_txn_max_ops + 1); });
    waitEntered(sync);
    Caller neighbor = launchDrop(store, ns, "neighbor");
    waitPendingAtLeast(store, ns, 2);
    sync->cv.notify_all();   /// release the pre-carve hook now its (>=2 pending) predicate holds

    ASSERT_EQ(oversized.fut.wait_for(std::chrono::seconds(10)), std::future_status::ready) << "oversized item must not hang";
    ASSERT_EQ(neighbor.fut.wait_for(std::chrono::seconds(10)), std::future_status::ready) << "neighbor must not hang";
    const std::exception_ptr oversized_err = oversized.fut.get();
    const std::exception_ptr neighbor_err = neighbor.fut.get();
    oversized.t.join();
    neighbor.t.join();
    store->setRefPreCarveHookForTest(nullptr);

    expectFailedWithCode(oversized_err, DB::ErrorCodes::LIMIT_EXCEEDED, "oversized item (op count)");
    EXPECT_TRUE(neighbor_err == nullptr) << "the co-batched neighbor must commit despite the oversized item";
    EXPECT_FALSE(store->resolveRef(ns, "neighbor").has_value()) << "neighbor's drop must have committed";
}

/// Test 10, second leg: one op whose OWN encoded size exceeds `ref_op_max_bytes` (a maximum-length
/// ref name -- `checkCanonicalRefName` imposes no length limit) fails only its item; a co-batched
/// neighbor still commits.
TEST(CASRefWriterChunkedFlush, OversizedOpFailsItsItemAlone)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/chunked_oversized_op"};
    publishEmptyPart(store, ns, "neighbor");
    ASSERT_TRUE(store->resolveRef(ns, "neighbor").has_value());

    const RefOp oversized_op = paddedSetPublishedAtOp(ref_op_max_bytes + 1);
    ASSERT_GT(encodedOpSize(oversized_op), ref_op_max_bytes);

    auto sync = std::make_shared<CaseSync>();
    armPreCarveBlock(store, ns, sync, 2);

    Caller oversized = launchAppend(store, ns, MutationScope::ref("oversized_op"),
        [oversized_op](const RefTableState &) -> std::vector<RefOp> { return {oversized_op}; });
    waitEntered(sync);
    Caller neighbor = launchDrop(store, ns, "neighbor");
    waitPendingAtLeast(store, ns, 2);
    sync->cv.notify_all();

    ASSERT_EQ(oversized.fut.wait_for(std::chrono::seconds(10)), std::future_status::ready) << "oversized op item must not hang";
    ASSERT_EQ(neighbor.fut.wait_for(std::chrono::seconds(10)), std::future_status::ready) << "neighbor must not hang";
    const std::exception_ptr oversized_err = oversized.fut.get();
    const std::exception_ptr neighbor_err = neighbor.fut.get();
    oversized.t.join();
    neighbor.t.join();
    store->setRefPreCarveHookForTest(nullptr);

    expectFailedWithCode(oversized_err, DB::ErrorCodes::LIMIT_EXCEEDED, "oversized op");
    EXPECT_TRUE(neighbor_err == nullptr) << "the co-batched neighbor must commit despite the oversized op";
    EXPECT_FALSE(store->resolveRef(ns, "neighbor").has_value()) << "neighbor's drop must have committed";
}

/// Test 12, canonical round-trip leg: the maximum legally-admissible normal-class transaction under
/// the new counts-only caps -- `ref_txn_max_ops` ops, each padded to exactly `ref_op_max_bytes` --
/// round-trips comfortably under the whole-transaction `ref_txn_max_bytes` decode cap (5000 * 4096 =
/// 20,480,000 bytes, with framing headroom to spare). Pure codec-level: proves the two counts-only
/// caps compose without ever approaching the byte cap the encode-side estimation machinery used to
/// police.
TEST(CASRefWriterChunkedFlush, CanonicalMaxTransactionRoundTrips)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};
    txn.ops.reserve(ref_txn_max_ops);
    for (size_t i = 0; i < ref_txn_max_ops; ++i)
    {
        RefOp op = paddedSetPublishedAtOp(ref_op_max_bytes);
        ASSERT_EQ(encodedOpSize(op), ref_op_max_bytes);
        txn.ops.push_back(std::move(op));
    }

    const String bytes = encodeRefLogTxn(txn);
    /// Every op contributes exactly `ref_op_max_bytes`; header/meta/trailer framing adds strictly
    /// more on top, and the whole thing still stays well under the 20 MiB decode cap.
    EXPECT_GT(bytes.size(), ref_txn_max_ops * ref_op_max_bytes);
    EXPECT_LT(bytes.size(), ref_txn_max_bytes);

    const RefLogTxn decoded = decodeRefLogTxn(bytes, txn.ns, txn.txn_id);
    EXPECT_EQ(decoded.ops.size(), ref_txn_max_ops);
    EXPECT_EQ(decoded, txn);
}

/// Test 11 (spec §3 "Removal-class detection, falsifiably"): `dropNamespace` over a table with
/// > `ref_txn_max_ops` committed refs builds ONE transaction whose ops (one `owner_transition`
/// removal per ref, plus a terminal `remove_namespace`) exceed the normal-class op-count cap --
/// and must still succeed, because removal-class is byte-budgeted (`ref_removal_max_bytes`, 64 MiB)
/// and has no op-count cap. Seeded via a raw snapshot (not `kTotalRefs` individual writer round-trips
/// through `publishEmptyPart`) so the fixture stays fast; the writer never touches these rows until
/// `dropNamespace` itself builds the one removal transaction.
TEST(CASRefWriterChunkedFlush, DropNamespaceOverOpCapSucceeds)
{
    auto backend = std::make_shared<DB::Cas::tests::CountingBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/dropns_over_cap"};
    constexpr size_t kTotalRefs = static_cast<size_t>(ref_txn_max_ops) + 200;

    /// Open the store FIRST (still untouched for `ns`) so the seeded snapshot can use THIS mount's own
    /// writer_epoch: namespace recovery is per-namespace and lazy (first touch), so writing the raw
    /// fixture directly to `backend` after open, but before `ns` is ever touched, is observed identically
    /// to writing it before open.
    auto store = openPool(backend);
    const uint64_t epoch = store->writerEpoch();
    /// Stage B (Task 4-C): pin `ns` to the sentinel now, before the raw snapshot below -- `listRefs`/
    /// `dropNamespace` further down are real production reads that trigger `resolveNamespaceLife`,
    /// which for an UNADMITTED namespace mints a fresh RANDOM incarnation rather than adopting the
    /// sentinel the raw fixture wrote at. Pinning first makes them adopt it instead.
    DB::Cas::tests::fixture::admitLive(*backend, layout, ns);

    /// Ids are PER-NAMESPACE and derived from the table's own `greatest_applied` (INV-1), so seeding
    /// `ns` at `{epoch, 1}` is all this fixture has to do: the `dropNamespace` below derives `{epoch, 2}`
    /// from the seeded snapshot, and no other namespace's traffic can move it.
    std::vector<RefCommittedRow> committed;
    committed.reserve(kTotalRefs);
    for (size_t i = 0; i < kTotalRefs; ++i)
        committed.push_back(committedRow(paddedRefName(i), ManifestRef{epoch, i + 1, 1}));
    ASSERT_GT(committed.size(), ref_txn_max_ops);

    /// Recovery's checkpoint anchor includes the same-id ordinary log. The synthetic snapshot stands
    /// for a long prior history, while this genesis record supplies the retained non-seal witness the
    /// real publisher would necessarily leave at the selected id.
    DB::Cas::tests::fixture::writeRefLogRaw(*backend, layout, RefLogTxn{
        .ns = ns.string(),
        .txn_id = RefTxnId{epoch, 1},
        .ops = {DB::Cas::tests::namespaceBirthOp()},
        .prev_epoch_seal = std::nullopt});
    writeRefSnapshotRaw(*backend, layout, minimalLiveSnapshot(ns.string(), RefTxnId{epoch, 1}, committed));
    DB::Cas::tests::writeRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = epoch,
        .committed_through = RefTxnId{epoch, 1},
        .checkpoint_snapshot_id = RefTxnId{epoch, 1},
        .last_epoch_seal = std::nullopt,
    });
    const NamespaceLifeId life = CasRefCatalog::lifeIfCataloged(*backend, layout, ns).value();
    backend->resetCounts();
    ASSERT_EQ(store->listRefs(ns).size(), kTotalRefs);
    EXPECT_EQ(backend->getCount(layout.refLogKey(life, RefTxnId{epoch, 1})), 1u);
    EXPECT_EQ(backend->getCount(layout.refSnapshotKey(life, RefTxnId{epoch, 1})), 1u);

    DropNamespaceStats stats;
    EXPECT_NO_THROW(stats = store->dropNamespace(ns));
    EXPECT_EQ(stats.committed_refs, kTotalRefs);
    EXPECT_EQ(CasRefCatalog::read(*backend, layout).catalog.entries.front().state, NsState::Removing);
}

/// Test 11, second leg: `WholeShard` scope ALONE is not the removal-class discriminator -- the
/// stale-precommit reclaim sweep is also `WholeShard`-scoped but is not removal-class
/// (`CasRefLedger.cpp` ~:1979). Only a SYNTHETIC item can pin this: the production stale-precommit
/// sweep self-limits its own chunk size to the op cap, so running it proves nothing (spec's own
/// warning). This item drives `MutationScope::wholeShard()` directly with ops that contain NO
/// `RemoveNamespace` op -- if classification were keyed on scope instead of op inspection, this would
/// be wrongly treated as removal-class and admitted; op-inspection correctly rejects it under the
/// ordinary normal-class op-count cap, exactly like `OversizedItemFailsAlone` above.
TEST(CASRefWriterChunkedFlush, SyntheticWholeShardNonRemovalRejected)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/synthetic_wholeshard_nonremoval"};

    Caller synthetic = launchAppend(store, ns, MutationScope::wholeShard(),
        [](const RefTableState &) -> std::vector<RefOp> { return fillerOps(ref_txn_max_ops + 1); });
    ASSERT_EQ(synthetic.fut.wait_for(std::chrono::seconds(10)), std::future_status::ready)
        << "synthetic WholeShard item must not hang";
    const std::exception_ptr err = synthetic.fut.get();
    synthetic.t.join();

    expectFailedWithCode(err, DB::ErrorCodes::LIMIT_EXCEEDED,
        "synthetic WholeShard-scoped item with non-removal ops over the op cap");
}

/// ===================================================================================
/// Task 10 (spec §3 "Chunked flush, where each chunk is a complete commit boundary"): when admitting
/// the next item's ops would exceed `ref_txn_max_ops`, the leader commits the accumulated chunk as a
/// COMPLETE ref-log transaction (real id, PUT, apply, tail, metrics, survivor completion + waiter
/// wakeups, snapshot scheduling), reseeds `working`/the trial-id high-water mark from the now-live
/// state, and continues into a fresh chunk -- so one tenure can emit several transactions, each a valid
/// persisted prefix. The failure-isolation and tenure-containment contracts are pinned below.
/// ===================================================================================

namespace
{

/// The `_log/`-PUT fault seam these tests are built on now lives in `cas_test_helpers.h`, next to
/// `CountingBackend` it derives from: `gtest_cas_ref_install_safety.cpp` needs the SAME seam (spec §A1
/// sites 2 and 3 both turn on what happens when a `_log/` PUT's response is lost), and two copies of a
/// fault backend would drift apart.
using DB::Cas::tests::ChunkFaultBackend;

PoolPtr openPoolWith(const BackendPtr & backend, PoolConfig cfg)
{
    DB::Cas::tests::seedPoolMetaForRestart(*backend);
    cfg.pool_prefix = "p";
    cfg.server_root_id = "test";
    /// Same frozen clock as `openPool` above, and for the same reason — see its comment. Defaulted
    /// rather than forced, so a future test that IS about lease timing can still supply its own.
    if (!cfg.boot_ms_fn)
        cfg.boot_ms_fn = [] { return uint64_t{0}; };
    return Pool::open(backend, cfg);
}

/// `num_pairs` add-then-remove precommit op pairs (2 * `num_pairs` ops total) for distinct refs
/// (`prefix` + zero-padded index) each naming a distinct valid manifest. Every pair adds a precommit
/// binding and immediately removes it, so the LIVE state (the `precommits` set, the committed COW map,
/// the owned-manifest index) stays ~empty throughout the whole transaction -- keeping the per-op
/// `admits` preview and the sanitizer-only body-counter assert O(1), so validating a maximal chunk of
/// thousands of ops stays O(ops), not O(ops^2). It is the OP COUNT (not the resident state) that drives
/// the chunk split under test; each op is tiny (well under `ref_op_max_bytes`), so the whole run is
/// admissible on a `Live` namespace. The durable transaction still carries every op verbatim, so a
/// chunk's ops can be compared against the exact expected vector.
std::vector<RefOp> addRemovePrecommitPairs(const String & prefix, size_t num_pairs, uint64_t manifest_epoch)
{
    std::vector<RefOp> ops;
    ops.reserve(num_pairs * 2);
    for (size_t i = 0; i < num_pairs; ++i)
    {
        const String ref = prefix + paddedRefName(i);
        const ManifestRef manifest{manifest_epoch, i + 1, 1};
        RefOp add;
        add.kind = RefOpKind::OwnerTransition;
        add.new_binding = RefOwnerBinding{RefOwnerKind::Precommit, ref, manifest};
        ops.push_back(std::move(add));
        RefOp remove;
        remove.kind = RefOpKind::OwnerTransition;
        remove.old_binding = RefOwnerBinding{RefOwnerKind::Precommit, ref, manifest};
        ops.push_back(std::move(remove));
    }
    return ops;
}

/// Every durable `_log/` transaction for `ns`, decoded, sorted ascending by transaction id. Undecodable
/// objects (e.g. the foreign bytes a `ForeignConflict` fault lands) are skipped so a corrupt object never
/// breaks the inventory. Reads the backend directly (no Pool cache).
std::vector<RefLogTxn> listLogTxns(DB::Cas::Backend & backend, const DB::Cas::Layout & layout, const RootNamespace & ns)
{
    std::vector<RefTxnId> ids;
    String cursor;
    for (;;)
    {
        const ListPage page = backend.list(layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)), cursor, 1000);
        for (const ListedKey & lk : page.keys)
        {
            const auto parsed = layout.parseRefObjectKey(lk.key);
            if (parsed && parsed->life_id == DB::Cas::tests::fixture::fixtureLife(ns).incarnation
                && parsed->kind == RefObjectKind::Log)
                ids.push_back(parsed->txn_id);
        }
        if (page.next_cursor.empty())
            break;
        cursor = page.next_cursor;
    }
    std::sort(ids.begin(), ids.end(), [](const RefTxnId & a, const RefTxnId & b) { return a < b; });
    std::vector<RefLogTxn> txns;
    for (const RefTxnId & id : ids)
    {
        const auto got = backend.get(layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), id));
        if (!got)
            continue;
        try
        {
            txns.push_back(decodeRefLogTxn(openObject(FormatId::RefLog, got->bytes), ns.string(), id));
        }
        catch (...) // NOLINT(bugprone-empty-catch): best-effort helper -- an undecodable txn is simply skipped, not asserted on
        {
        }
    }
    return txns;
}

/// One queued append driven on its own thread, capturing BOTH the committed transaction id (on success)
/// and the exception (on failure); `build_calls` (when non-null) counts `build_ops` invocations to pin
/// the at-most-once contract across chunk boundaries. The ops are precomputed and returned verbatim, so a
/// second invocation (a bug) is caught by the counter, not masked by a state-dependent rebuild.
struct AppendResult
{
    std::exception_ptr err;
    RefTxnId id{};
};

struct AppendCaller
{
    std::thread t;
    std::future<AppendResult> fut;
};

AppendCaller launchAppendOps(const PoolPtr & store, const RootNamespace & ns, MutationScope scope,
                             std::vector<RefOp> ops, std::shared_ptr<std::atomic<int>> build_calls)
{
    auto prom = std::make_shared<std::promise<AppendResult>>();
    std::future<AppendResult> fut = prom->get_future();
    auto build_ops = [captured_ops = std::move(ops), build_calls](const RefTableState &) -> std::vector<RefOp>
    {
        if (build_calls)
            build_calls->fetch_add(1);
        return captured_ops;
    };
    std::thread t([store, ns, scope, build_ops, prom]
    {
        AppendResult r;
        try { r.id = store->appendRefOps(ns, scope, build_ops, RootMutationOrigin::Writer, RootMutationKind::Publish); }
        catch (...) { r.err = std::current_exception(); }
        prom->set_value(r);
    });
    return AppendCaller{std::move(t), std::move(fut)};
}

}

/// Test 9 (happy path): a carve whose total ops exceed `ref_txn_max_ops` emits >= 2 ref-log transactions
/// in ONE leader tenure. Three items (2000 ops each = 6000 > 5000) split into chunk 1 = {item_a,item_b}
/// (4000 ops, one id) and chunk 2 = {item_c} (2000 ops, the next id). Per-chunk assertions: committed
/// ids (co-chunk survivors share one real id; the next chunk allocates the next), tail counters (one per
/// chunk), per-chunk metrics (`CASRefBatchFlushes` once per chunk, `CASRefBatchedMutations` counting
/// survivors per chunk), follower wakeups (both followers return their correct real id -> completed +
/// woken at their chunk's commit), `build_ops` at-most-once (invocation counters == 1), and folded state
/// == the sequential result (the two durable transactions carry exactly item_a++item_b, then item_c).
TEST(CASRefWriterChunkedFlush, ChunkedFlushCommitsPerChunk)
{
    auto backend = std::make_shared<InMemoryBackend>();
    /// Default thresholds: this handful of transactions never crosses the snapshot-publish threshold, so
    /// no background publish interleaves and the tail/metric deltas below are exact.
    auto store = openPool(backend);
    const DB::Cas::Layout & layout = store->layout();
    const RootNamespace ns{"srv1/chunked_commits_per_chunk"};
    publishEmptyPart(store, ns, "seed");
    ASSERT_TRUE(store->resolveRef(ns, "seed").has_value());

    /// 2000 ops per item (1000 add/remove pairs) -> 6000 > ref_txn_max_ops (5000): chunk 1 =
    /// {item_a,item_b} (4000), chunk 2 = {item_c} (2000).
    const std::vector<RefOp> ops1 = addRemovePrecommitPairs("aaa_", 1000, 900000001);
    const std::vector<RefOp> ops2 = addRemovePrecommitPairs("bbb_", 1000, 900000002);
    const std::vector<RefOp> ops3 = addRemovePrecommitPairs("ccc_", 1000, 900000003);
    auto c1 = std::make_shared<std::atomic<int>>(0);
    auto c2 = std::make_shared<std::atomic<int>>(0);
    auto c3 = std::make_shared<std::atomic<int>>(0);

    const size_t tail_before = store->tailSinceSnapshotCountForTest(ns);
    const uint64_t flushes_before = ProfileEvents::global_counters[ProfileEvents::CASRefBatchFlushes].load();
    const uint64_t mutations_before = ProfileEvents::global_counters[ProfileEvents::CASRefBatchedMutations].load();

    auto sync = std::make_shared<CaseSync>();
    armPreCarveBlock(store, ns, sync, 3);
    /// Serialise the enqueue order so the batch is exactly [item_a(leader), item_b, item_c].
    AppendCaller a = launchAppendOps(store, ns, MutationScope::ref("item_a"), ops1, c1);
    waitEntered(sync);
    AppendCaller b = launchAppendOps(store, ns, MutationScope::ref("item_b"), ops2, c2);
    waitPendingAtLeast(store, ns, 2);
    AppendCaller c = launchAppendOps(store, ns, MutationScope::ref("item_c"), ops3, c3);
    waitPendingAtLeast(store, ns, 3);
    sync->cv.notify_all();

    ASSERT_EQ(a.fut.wait_for(std::chrono::seconds(20)), std::future_status::ready) << "item_a must not hang";
    ASSERT_EQ(b.fut.wait_for(std::chrono::seconds(20)), std::future_status::ready) << "item_b must not hang";
    ASSERT_EQ(c.fut.wait_for(std::chrono::seconds(20)), std::future_status::ready) << "item_c must not hang";
    const AppendResult ra = a.fut.get();
    const AppendResult rb = b.fut.get();
    const AppendResult rc = c.fut.get();
    a.t.join();
    b.t.join();
    c.t.join();
    store->setRefPreCarveHookForTest(nullptr);

    ASSERT_TRUE(ra.err == nullptr) << "item_a must commit";
    ASSERT_TRUE(rb.err == nullptr) << "item_b must commit";
    ASSERT_TRUE(rc.err == nullptr) << "item_c must commit";

    /// `build_ops` ran exactly once per item -- including item_c, the overflowing item validated once in
    /// the fresh chunk it lands in.
    EXPECT_EQ(c1->load(), 1);
    EXPECT_EQ(c2->load(), 1);
    EXPECT_EQ(c3->load(), 1);

    /// Committed ids per chunk: item_a and item_b share chunk 1's real id (co-chunk survivors, both
    /// woken with it); item_c gets chunk 2's id, exactly one sequence step above chunk 1.
    EXPECT_EQ(ra.id, rb.id) << "co-chunk survivors must complete with the SAME real transaction id";
    EXPECT_EQ(rc.id.writer_epoch, ra.id.writer_epoch);
    EXPECT_EQ(rc.id.ref_sequence, ra.id.ref_sequence + 1) << "chunk 2 must allocate the id after chunk 1";

    /// >= 2 durable transactions in the tenure, and the split is exactly the sequential result: chunk 1
    /// carries item_a's then item_b's ops (survivor order), chunk 2 carries item_c's.
    const std::vector<RefLogTxn> logs = listLogTxns(*backend, layout, ns);
    std::optional<RefLogTxn> chunk1_txn;
    std::optional<RefLogTxn> chunk2_txn;
    for (const RefLogTxn & txn : logs)
    {
        if (txn.txn_id == ra.id)
            chunk1_txn = txn;
        if (txn.txn_id == rc.id)
            chunk2_txn = txn;
    }
    ASSERT_TRUE(chunk1_txn.has_value()) << "chunk 1 must be durable";
    ASSERT_TRUE(chunk2_txn.has_value()) << "chunk 2 must be durable (a second transaction in one tenure)";
    std::vector<RefOp> expect_chunk1 = ops1;
    expect_chunk1.insert(expect_chunk1.end(), ops2.begin(), ops2.end());
    EXPECT_EQ(chunk1_txn->ops, expect_chunk1);
    EXPECT_EQ(chunk2_txn->ops, ops3);

    /// Tail counters advanced once per committed chunk.
    EXPECT_EQ(store->tailSinceSnapshotCountForTest(ns), tail_before + 2);

    /// Per-chunk metrics: one batch-flush per chunk (2), survivors counted per chunk (2 + 1 = 3). The
    /// snapshot-scheduling trigger is the final step of the SAME committed arm that increments
    /// `CASRefBatchFlushes`, so == 2 also proves the scheduler was invoked per chunk;
    /// `SnapshotPublisherLatchedAcrossChunks` proves that trigger actually re-fires across chunks.
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASRefBatchFlushes].load() - flushes_before, 2u);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASRefBatchedMutations].load() - mutations_before, 3u);
}

namespace
{

/// Shared body for the three chunk-failure variants: two items (3000 ops each) -> chunk 1 = {item_a}
/// (the leader's own item), chunk 2 = {item_b}. `mode` faults ONLY chunk 2's `_log/` PUT (skip chunk 1).
/// In every variant chunk 1 commits and the leader's own call returns chunk 1's real id, while chunk 2's
/// caller fails. Returns the two callers' results plus chunk 1's id for the per-variant assertions.
struct ChunkFailureOutcome
{
    AppendResult leader;     /// item_a, chunk 1
    AppendResult follower;   /// item_b, chunk 2
    RefTxnId chunk1_id{};
    std::shared_ptr<ChunkFaultBackend> backend;
    PoolPtr store;
};

ChunkFailureOutcome runChunkFailureCase(const String & ns_suffix, ChunkFaultBackend::Mode mode)
{
    auto backend = std::make_shared<ChunkFaultBackend>();
    PoolConfig cfg;
    /// Single-attempt budget: one ambiguous PUT is a conclusive Unresolved (wedge) / DefiniteFailure,
    /// with no inter-attempt sleep to serve.
    CasRequestBudget budget;
    budget.max_attempts = 1;
    budget.attempt_timeout_ms = 100;
    budget.operation_deadline_ms = 5000;   /// strictly above attempt_timeout_ms: equality is a wall-clock race (validateCasRequestBudget)
    budget.lease_safety_margin_ms = 100;
    cfg.cas_request_budget = budget;
    auto store = openPoolWith(backend, cfg);
    const DB::Cas::Layout & layout = store->layout();
    const RootNamespace ns{String("srv1/") + ns_suffix};
    publishEmptyPart(store, ns, "seed");

    /// Fault ONLY chunk 2's `_log/` PUT: skip chunk 1's (the first match), fault the second. Armed AFTER
    /// the seed so only the flush's two log PUTs are counted.
    backend->fault_substr = layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_log/";
    backend->mode = mode;
    backend->fault_skip = 1;
    backend->fault_count = 1;

    auto sync = std::make_shared<CaseSync>();
    armPreCarveBlock(store, ns, sync, 2);
    /// 3000 ops per item (1500 add/remove pairs) -> 6000 > ref_txn_max_ops: chunk 1 = {item_a},
    /// chunk 2 = {item_b}.
    AppendCaller a = launchAppendOps(store, ns, MutationScope::ref("item_a"), addRemovePrecommitPairs("aaa_", 1500, 900000001), nullptr);
    waitEntered(sync);
    AppendCaller b = launchAppendOps(store, ns, MutationScope::ref("item_b"), addRemovePrecommitPairs("bbb_", 1500, 900000002), nullptr);
    waitPendingAtLeast(store, ns, 2);
    sync->cv.notify_all();

    EXPECT_EQ(a.fut.wait_for(std::chrono::seconds(20)), std::future_status::ready) << "leader must not hang";
    EXPECT_EQ(b.fut.wait_for(std::chrono::seconds(20)), std::future_status::ready) << "follower must not hang";
    ChunkFailureOutcome out;
    out.leader = a.fut.get();
    out.follower = b.fut.get();
    a.t.join();
    b.t.join();
    store->setRefPreCarveHookForTest(nullptr);
    out.chunk1_id = out.leader.id;
    out.backend = backend;
    out.store = store;
    return out;
}

}

/// Test 9 (chunk-failure variant a -- definite failure): chunk 2's PUT is conclusively rejected
/// (`CasWriteOutcome::DefiniteFailure`). Chunk 1's caller (the leader's own item) observes SUCCESS with
/// chunk 1's real id; chunk 2's caller fails; the lane does NOT wedge (a definite rejection is a safe
/// gap, not an uncertain PUT).
TEST(CASRefWriterChunkedFlush, ChunkFailureDefinite)
{
#if !USE_AWS_S3
    GTEST_SKIP() << "DefiniteFailure classification requires S3 error types (USE_AWS_S3 off)";
#endif
    ChunkFailureOutcome out = runChunkFailureCase("chunk_fail_definite", ChunkFaultBackend::Mode::Definite);
    ASSERT_TRUE(out.leader.err == nullptr) << "chunk-1 caller must observe success even though chunk 2 failed";
    ASSERT_TRUE(out.follower.err != nullptr) << "chunk-2 caller must observe the definite failure";
    EXPECT_FALSE(out.store->refLaneWedgedForTest(RootNamespace{"srv1/chunk_fail_definite"}))
        << "a definite failure is proven non-durable and must NOT wedge the lane";

    const auto logs = listLogTxns(*out.backend, out.store->layout(), RootNamespace{"srv1/chunk_fail_definite"});
    bool saw_chunk1 = false;
    for (const RefLogTxn & txn : logs)
        if (txn.txn_id == out.chunk1_id)
            saw_chunk1 = true;
    EXPECT_TRUE(saw_chunk1) << "chunk 1 must be durably committed";
}

/// Test 9 (chunk-failure variant b -- unresolved wedge): chunk 2's PUT is ambiguous and exhausts the
/// budget, wedging the lane. Chunk 1's caller observes SUCCESS; chunk 2's caller fails; the wedge holds
/// ONLY chunk 2's key (chunk 1 + 1), and chunk 1's object is durable while chunk 2's was never written.
TEST(CASRefWriterChunkedFlush, ChunkFailureWedge)
{
    const RootNamespace ns{"srv1/chunk_fail_wedge"};
    ChunkFailureOutcome out = runChunkFailureCase("chunk_fail_wedge", ChunkFaultBackend::Mode::Unresolved);
    ASSERT_TRUE(out.leader.err == nullptr) << "chunk-1 caller must observe success even though chunk 2 wedged";
    ASSERT_TRUE(out.follower.err != nullptr) << "chunk-2 caller must observe the append failure";

    EXPECT_TRUE(out.store->refLaneWedgedForTest(ns)) << "chunk 2's unresolved PUT must wedge the lane";
    RefTxnId chunk2_id = out.chunk1_id;
    ++chunk2_id.ref_sequence;
    EXPECT_EQ(out.store->wedgedKeyForTest(ns), out.store->layout().refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), chunk2_id))
        << "the wedge must contain ONLY chunk 2's key";

    const auto logs = listLogTxns(*out.backend, out.store->layout(), ns);
    bool saw_chunk1 = false;
    bool saw_chunk2 = false;
    for (const RefLogTxn & txn : logs)
    {
        if (txn.txn_id == out.chunk1_id)
            saw_chunk1 = true;
        if (txn.txn_id == chunk2_id)
            saw_chunk2 = true;
    }
    EXPECT_TRUE(saw_chunk1) << "chunk 1 must be durably committed";
    EXPECT_FALSE(saw_chunk2) << "chunk 2's wedged object was never durably written";
}

/// Test 9 (chunk-failure variant c -- a throw): chunk 2's PUT surfaces a proven conflict (CORRUPTED_DATA
/// thrown by the controller). Chunk 1's caller observes SUCCESS; chunk 2's caller fails with
/// CORRUPTED_DATA; the lane does NOT wedge (a conclusive rejection).
TEST(CASRefWriterChunkedFlush, ChunkFailureThrow)
{
    const RootNamespace ns{"srv1/chunk_fail_throw"};
    ChunkFailureOutcome out = runChunkFailureCase("chunk_fail_throw", ChunkFaultBackend::Mode::ForeignConflict);
    ASSERT_TRUE(out.leader.err == nullptr) << "chunk-1 caller must observe success even though chunk 2 threw";
    ASSERT_TRUE(out.follower.err != nullptr) << "chunk-2 caller must observe the thrown failure";
    expectFailedWithCode(out.follower.err, DB::ErrorCodes::CORRUPTED_DATA, "chunk-2 proven-conflict throw");
    EXPECT_FALSE(out.store->refLaneWedgedForTest(ns)) << "a proven conflict is conclusive and must NOT wedge";

    const auto logs = listLogTxns(*out.backend, out.store->layout(), ns);
    bool saw_chunk1 = false;
    for (const RefLogTxn & txn : logs)
        if (txn.txn_id == out.chunk1_id)
            saw_chunk1 = true;
    EXPECT_TRUE(saw_chunk1) << "chunk 1 must be durably committed";
}

/// Test 9 (containment variant 1): the leader's OWN item lands in chunk 1; a throw is injected at the
/// chunk boundary (simulating a reseed allocation failure) AFTER chunk 1 committed. Tenure containment
/// (spec §3): the leader's own `appendRefOps` returns chunk 1's real id -- NOT the later exception --
/// while the unattempted remainder (item_b) fails. This exercises the reworked outer catch, which no
/// longer rethrows unconditionally over a durable own item.
TEST(CASRefWriterChunkedFlush, LeaderOwnItemCommittedBeforeThrow)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPool(backend);
    const DB::Cas::Layout & layout = store->layout();
    const RootNamespace ns{"srv1/chunk_leader_own_committed"};
    publishEmptyPart(store, ns, "seed");

    auto c1 = std::make_shared<std::atomic<int>>(0);
    auto c2 = std::make_shared<std::atomic<int>>(0);

    /// Throw once at the first chunk boundary -- after chunk 1 (the leader's own item) is durable and
    /// before the reseed completes.
    auto boundary_hits = std::make_shared<std::atomic<int>>(0);
    store->setCarveHookForTest([boundary_hits](CasRefLedger::CarvePhaseForTest ph)
    {
        if (ph == CasRefLedger::CarvePhaseForTest::ChunkReseed && boundary_hits->fetch_add(1) == 0)
            throw std::bad_alloc{};
    });

    auto sync = std::make_shared<CaseSync>();
    armPreCarveBlock(store, ns, sync, 2);
    /// 3000 ops per item (1500 add/remove pairs) -> chunk 1 = {item_a}, boundary throw before chunk 2.
    AppendCaller a = launchAppendOps(store, ns, MutationScope::ref("item_a"), addRemovePrecommitPairs("aaa_", 1500, 900000001), c1);
    waitEntered(sync);
    AppendCaller b = launchAppendOps(store, ns, MutationScope::ref("item_b"), addRemovePrecommitPairs("bbb_", 1500, 900000002), c2);
    waitPendingAtLeast(store, ns, 2);
    sync->cv.notify_all();

    ASSERT_EQ(a.fut.wait_for(std::chrono::seconds(20)), std::future_status::ready) << "leader must not hang";
    ASSERT_EQ(b.fut.wait_for(std::chrono::seconds(20)), std::future_status::ready) << "follower must not hang";
    const AppendResult ra = a.fut.get();
    const AppendResult rb = b.fut.get();
    a.t.join();
    b.t.join();
    store->setCarveHookForTest(nullptr);
    store->setRefPreCarveHookForTest(nullptr);

    ASSERT_TRUE(ra.err == nullptr)
        << "the leader's own committed-chunk item must return success, not the later boundary throw";
    ASSERT_TRUE(rb.err != nullptr) << "the unattempted remainder must fail";

    const std::vector<RefLogTxn> logs = listLogTxns(*backend, layout, ns);
    std::optional<RefLogTxn> chunk1_txn;
    for (const RefLogTxn & txn : logs)
        if (txn.txn_id == ra.id)
            chunk1_txn = txn;
    ASSERT_TRUE(chunk1_txn.has_value()) << "chunk 1 must be durable";
    EXPECT_EQ(chunk1_txn->ops, addRemovePrecommitPairs("aaa_", 1500, 900000001));
    /// item_a's build_ops ran once (chunk 1); item_b's ran once (before the boundary throw preempted its
    /// validation) and is NOT re-invoked -- the at-most-once contract holds through the failed tenure.
    EXPECT_EQ(c1->load(), 1);
    EXPECT_EQ(c2->load(), 1);
}

/// Test 9 (containment variant 2 -- snapshot coalescing): a snapshot publisher dispatched by chunk 1 is
/// latched at its PUT AFTER capturing chunk 1's prefix; chunk 2 then commits and its publish trigger is
/// discarded by the single-in-flight gate. When the latched publisher settles, settlement must re-fire
/// the dropped trigger so a FOLLOW-UP publication covers chunk 2 -- otherwise chunk 2 would stay
/// unsnapshotted until an unrelated later mutation. The chunk boundary is gated until the publisher has
/// parked, so its captured candidate is provably chunk 1's prefix only.
TEST(CASRefWriterChunkedFlush, SnapshotPublisherLatchedAcrossChunks)
{
    auto backend = std::make_shared<ChunkFaultBackend>();
    PoolConfig cfg;
    cfg.snapshot_log_count_threshold = 0;   /// every committed chunk crosses the tail-count threshold
    auto store = openPoolWith(backend, cfg);
    const DB::Cas::Layout & layout = store->layout();
    const RootNamespace ns{"srv1/chunk_snapshot_coalesce"};
    publishEmptyPart(store, ns, "seed");
    store->waitForSnapshotPublishSettleForTest(ns);   /// drain the seed's publish chain -> tail == 0

    /// Latch the FIRST `_snap/` PUT (chunk 1's publisher) at its conditional PUT -- i.e. AFTER it has
    /// captured chunk 1's prefix under state_mutex.
    backend->armBlock(layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_snap/");
    /// Gate the leader at the chunk boundary until that publisher has parked on its PUT, so its captured
    /// candidate is EXACTLY chunk 1's prefix (not chunk 1 + chunk 2).
    store->setCarveHookForTest([backend](CasRefLedger::CarvePhaseForTest ph)
    {
        if (ph == CasRefLedger::CarvePhaseForTest::ChunkReseed)
            backend->awaitBlockEntered();
    });

    auto sync = std::make_shared<CaseSync>();
    armPreCarveBlock(store, ns, sync, 2);
    /// 3000 ops per item (1500 add/remove pairs) -> chunk 1 = {item_a}, chunk 2 = {item_b}.
    AppendCaller a = launchAppendOps(store, ns, MutationScope::ref("item_a"), addRemovePrecommitPairs("aaa_", 1500, 900000001), nullptr);
    waitEntered(sync);
    AppendCaller b = launchAppendOps(store, ns, MutationScope::ref("item_b"), addRemovePrecommitPairs("bbb_", 1500, 900000002), nullptr);
    waitPendingAtLeast(store, ns, 2);
    sync->cv.notify_all();

    ASSERT_EQ(a.fut.wait_for(std::chrono::seconds(20)), std::future_status::ready) << "leader must not hang";
    ASSERT_EQ(b.fut.wait_for(std::chrono::seconds(20)), std::future_status::ready) << "follower must not hang";
    const AppendResult ra = a.fut.get();
    const AppendResult rb = b.fut.get();
    a.t.join();
    b.t.join();
    ASSERT_TRUE(ra.err == nullptr) << "chunk 1 must commit";
    ASSERT_TRUE(rb.err == nullptr) << "chunk 2 must commit";
    RefTxnId chunk2_id = ra.id;
    ++chunk2_id.ref_sequence;
    EXPECT_EQ(rb.id, chunk2_id);

    /// Release the latched chunk-1 publisher. Its settlement must re-fire the chunk-2 trigger the
    /// single-flight gate dropped -> a follow-up publication covers chunk 2.
    backend->releaseBlock();
    store->waitForSnapshotPublishSettleForTest(ns);
    store->setCarveHookForTest(nullptr);
    store->setRefPreCarveHookForTest(nullptr);

    const std::optional<RefTxnId> newest = store->newestPublishedSnapshotIdForTest(ns);
    ASSERT_TRUE(newest.has_value()) << "at least one snapshot must have been published";
    EXPECT_FALSE(*newest < chunk2_id)
        << "settlement must re-fire the dropped chunk-2 trigger so a snapshot covers chunk 2 (no lost trigger)";
}
