#include <gtest/gtest.h>

#include "config.h"

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCkpt.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefProtocol.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>

#include <Poco/Exception.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace ProfileEvents
{
extern const Event CASRefSnapshotPublishDispatched;
extern const Event CASRefSnapshotPublishBackoff;
}

namespace DB::ErrorCodes
{
extern const int NETWORK_ERROR;
}

/// Task 6b remainder (Stage B, `{#t2}`): the publication-ordering coverage that Task 6b's rename left
/// undone. This suite PINS existing behavior of `CasRefLedger::tryPublishSnapshotAndAdvanceCheckpointOnce`
/// (the one retry unit), `admitSnapshotPublishUnderStateLock`, `advancePublishBackoff`/
/// `resetPublishBackoff`, and `dispatchSnapshotPublisher`/`settleSnapshotPublish`.
///
/// Normative ordering: (1) the immutable snapshot body becomes durable; (2) `_ckpt` advances; (3) the new
/// snapshot is adopted in this cache's memory. `NeedsRecovery` (this campaign's `Poisoned`) blocks
/// publication -- a durable transaction may be missing from the cached view -- and forces
/// `ensureRefTableRecovered` to re-walk the durable stream on the very next touch.
///
/// The suite name is prefixed `CAS` so it is covered by the `CAS*` unit-test gate filter.

using namespace DB::Cas;
using DB::Cas::tests::CountingBackend;
using DB::Cas::tests::OrderedFaultBackend;
using DB::Cas::tests::expectThrowsCode;
using DB::Cas::tests::namespaceBirthOp;
using DB::Cas::tests::publishCommittedOps;

namespace
{

PoolPtr openPool(const std::shared_ptr<OrderedFaultBackend> & backend, PoolConfig config = {})
{
    config.pool_prefix = "p";
    config.server_root_id = "test";
    DB::Cas::tests::seedPoolMetaForRestart(*backend);
    return Pool::open(backend, std::move(config));
}

/// The same one-transaction publish every other ref suite drives, so a namespace reaches `Live` through
/// the REAL append lane (which is also what creates its `_ckpt`).
RefTxnId publishRef(const PoolPtr & store, const RootNamespace & ns, const String & ref, uint64_t ordinal)
{
    return store->appendRefOps(ns, MutationScope::ref(ref),
        [&ref, ordinal](const RefTableState & state)
        {
            std::vector<RefOp> ops;
            if (state.getLifecycle() != RefLifecycle::Live)
                ops.push_back(namespaceBirthOp());
            for (const RefOp & op : publishCommittedOps(ref, ManifestRef{1, ordinal, 1}))
                ops.push_back(op);
            return ops;
        },
        RootMutationOrigin::Writer, RootMutationKind::Publish);
}

void forceAdoptablePublishWedge(
    const PoolPtr & store, const RootNamespace & ns, uint64_t ref_sequence, const String & ref, uint64_t ordinal)
{
    const RefTxnId txn_id{store->writerEpoch(), ref_sequence};
    RefLogTxn txn;
    txn.ns = ns.string();
    txn.txn_id = txn_id;
    txn.ops = publishCommittedOps(ref, ManifestRef{1, ordinal, 1});
    const String bytes = sealObject(FormatId::RefLog, encodeRefLogTxn(txn));
    const NamespaceLifeId life = *store->refTableLifeForTest(ns);
    store->forceWedgeForTest(
        ns, txn_id.writer_epoch, txn_id.ref_sequence, store->layout().refLogKey(life, txn_id), bytes);
}

}

/// ---------------------------------------------------------------------------------------------
/// 1. Snapshot body durable strictly before `_ckpt` advances
/// ---------------------------------------------------------------------------------------------

TEST(CASRefSnapshotPublishOrdering, SnapshotBodyIsDurableBeforeCheckpointAdvances)
{
    auto backend = std::make_shared<OrderedFaultBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/order_body_before_ckpt"};

    ASSERT_EQ(publishRef(store, ns, "ref_1", 1), (RefTxnId{store->writerEpoch(), 1}));
    const NamespaceLifeId life = *store->refTableLifeForTest(ns);
    const String snapshot_key = store->layout().refSnapshotKey(life, RefTxnId{store->writerEpoch(), 1});
    const String ckpt_key = store->layout().refCkptKey(life);

    /// The birth transaction above already CAS'd `_ckpt` itself (once for its own `life_epoch`, once for
    /// its committed frontier) -- ordinary append-commit traffic that has nothing to do with the snapshot
    /// publisher. The comparison below must therefore look only at what happens FROM this offset, or it
    /// would find the birth's ckpt writes (which precede the snapshot body by construction) and conclude
    /// nothing about the publisher's own ordering.
    const size_t offset = backend->journalSize();
    const uint64_t put_before = backend->putCount(snapshot_key);
    const uint64_t cas_before = backend->casPutCount(ckpt_key);

    ASSERT_TRUE(store->tryPublishSnapshotAndAdvanceCheckpointOnce(ns))
        << "a healthy Ready-lane table with an uncovered tail must publish";

    /// Positive control: this attempt touched each key exactly once (no retry, no redundant write) --
    /// which is what makes the index comparison below meaningful rather than an artifact of a busy log.
    EXPECT_EQ(backend->putCount(snapshot_key) - put_before, 1u);
    EXPECT_EQ(backend->casPutCount(ckpt_key) - cas_before, 1u);

    const auto body_index = backend->firstIndexFrom(OrderedFaultBackend::Op::Put, snapshot_key, offset);
    const auto ckpt_index = backend->firstIndexFrom(OrderedFaultBackend::Op::Cas, ckpt_key, offset);
    ASSERT_TRUE(body_index.has_value()) << "the snapshot body must have been PUT";
    ASSERT_TRUE(ckpt_index.has_value()) << "the checkpoint must have been CAS-advanced";
    EXPECT_LT(*body_index, *ckpt_index)
        << "INV-4's second `_ckpt` writer runs strictly after the immutable body is durable";
}


/// ---------------------------------------------------------------------------------------------
/// 2. Adoption happens last, and only once both durable effects landed
/// ---------------------------------------------------------------------------------------------

TEST(CASRefSnapshotPublishOrdering, AdoptionHappensLastAndOnlyAfterBothDurableEffects)
{
    auto backend = std::make_shared<OrderedFaultBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/order_adoption_after_both"};

    ASSERT_EQ(publishRef(store, ns, "ref_1", 1), (RefTxnId{store->writerEpoch(), 1}));
    const NamespaceLifeId life = *store->refTableLifeForTest(ns);
    const String snapshot_key = store->layout().refSnapshotKey(life, RefTxnId{store->writerEpoch(), 1});
    const String ckpt_key = store->layout().refCkptKey(life);

    /// Fail every one of the (attempt-bounded) 100 `_ckpt` CAS attempts `publishCkpt` will make: the
    /// body PUT still commits (dedup: an identical, already-durable body resolves as `Committed` without
    /// re-sending), but the checkpoint never advances within this call.
    backend->armCasConflict(ckpt_key, 100);
    EXPECT_FALSE(store->tryPublishSnapshotAndAdvanceCheckpointOnce(ns))
        << "a persistently conflicting checkpoint CAS must not be reported as a successful publish";

    EXPECT_EQ(backend->putCount(snapshot_key), 1u) << "the body is durable regardless of the ckpt outcome";
    EXPECT_FALSE(store->newestPublishedSnapshotIdForTest(ns).has_value())
        << "in-memory adoption must NOT happen while the checkpoint has not advanced";

    /// Disarm the fault and retry (the one retry unit): the retry issues its OWN `putIfAbsent` attempt at
    /// the same content-addressed key with the same bytes (so `putCount`, a call counter, becomes 2 --
    /// not a "no write happened" 1), but the backend resolves it as `Committed` against the already-durable
    /// object rather than sending a distinct object, and the checkpoint CAS now succeeds.
    ASSERT_TRUE(store->tryPublishSnapshotAndAdvanceCheckpointOnce(ns))
        << "the retry, with the fault cleared, must publish";
    EXPECT_EQ(backend->putCount(snapshot_key), 2u)
        << "the retry's body PUT is its own attempt, resolved via dedup against identical, "
           "already-durable bytes rather than writing a second object";
    EXPECT_EQ(store->newestPublishedSnapshotIdForTest(ns), std::make_optional(RefTxnId{store->writerEpoch(), 1}))
        << "adoption happens exactly once, after both effects are durable";
}

/// ---------------------------------------------------------------------------------------------
/// 3. `NeedsRecovery` ("Poisoned") lane: recovery precedes any snapshot publication
/// ---------------------------------------------------------------------------------------------

/// `Poisoned` is this task's plan's name for what the code spells `RefLaneState::NeedsRecovery` -- the
/// state the header documents as "a transaction is known durable but cannot be installed in this cache
/// ... a hard write and certification fence until replay completes". Recorded here as the vocabulary
/// correction for later tasks: there is no state literally named `Poisoned` anywhere in `CasRefLedger`.
/// This test is the plan's `PoisonedRefusesPublicationAndTriggersReRecovery`, renamed to state the actual
/// pinned behavior precisely (recovery precedes publication, rather than an outright refusal).
///
/// It is reached here the same way `gtest_cas_ref_writer.cpp`'s
/// `CASRefWriterAppendLane.CheckpointConflictAfterLogCommitRequiresRecoveryWithoutInstall` reaches it: a
/// mutation's ref-log body commits durably while its OWN checkpoint-frontier CAS (`commitRefChunk`'s
/// `commit_contribution`, not the snapshot publisher's) conflicts persistently.
///
/// The INVARIANT this pins (not a raw write count): a snapshot must never be published FROM AN
/// UNRECOVERED CACHE -- a durable transaction may be missing from the cached view, and advancing `_ckpt`
/// onto a snapshot built from that stale view is the data-loss shape `NeedsRecovery` exists to prevent.
/// `tryPublishSnapshotAndAdvanceCheckpointOnce` calls `ensureRefTableRecovered` unconditionally, and that
/// function re-walks the durable stream whenever the lane is `NeedsRecovery`, regardless of `recovered`.
/// Recovery's own `_ckpt` catch-up write is NOT a violation of this invariant -- it is the remedy: it is
/// how the cache stops being stale before anything is allowed to read it for a snapshot. So a request
/// against a poisoned lane recovers first and MAY legitimately go on to publish (this table had never
/// published a snapshot, so once recovered it has a real, uncovered candidate) -- "inert refusal with
/// zero writes" is NOT what production implements, and recover-then-proceed is the correct behavior, not
/// a deviation from it. What this test pins is: (a) no snapshot-publish effect (body PUT, publisher's own
/// checkpoint-advance CAS) can ever appear in the journal before recovery's reconciliation CAS; (b)
/// re-recovery is an observable state transition, never a silent skip; (c) if a snapshot IS published, it
/// reflects the RECOVERED frontier -- the durable transaction the stale cache was missing is actually
/// covered by it, not merely "some snapshot, from whichever view".
TEST(CASRefSnapshotPublishOrdering, NeedsRecoveryLaneRecoversBeforeAnySnapshotPublication)
{
    auto backend = std::make_shared<OrderedFaultBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/order_poisoned_refuses"};

    ASSERT_EQ(publishRef(store, ns, "ref_1", 1), (RefTxnId{store->writerEpoch(), 1}));
    const NamespaceLifeId life = *store->refTableLifeForTest(ns);
    const String ckpt_key = store->layout().refCkptKey(life);
    /// The durable transaction the stale cache will be missing: `dropRef`'s removal, sequence 2.
    const RefTxnId missing_durable_txn{store->writerEpoch(), 2};
    const String next_snapshot_key = store->layout().refSnapshotKey(life, missing_durable_txn);

    /// Drive the very next mutation's OWN checkpoint-frontier CAS into persistent conflict: the log PUT
    /// for `missing_durable_txn` commits durably, but its checkpoint never advances within this call, and
    /// the lane is left `NeedsRecovery` rather than installing an uncertain result -- so the cached view
    /// still reflects `ref_1` present, while the durable log already reflects it removed.
    backend->armCasConflict(ckpt_key, 100);
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropRef(ns, "ref_1"); });
    ASSERT_EQ(store->laneStateForTest(ns), RefLaneState::NeedsRecovery);

    const uint64_t recovery_installs_before = store->recoveryInstallCountForTest();
    backend->armCasConflict(ckpt_key, 0);   /// clear the fault so re-recovery's OWN catch-up CAN succeed
    const size_t offset = backend->journalSize();

    EXPECT_TRUE(store->tryPublishSnapshotAndAdvanceCheckpointOnce(ns))
        << "recovery reconciles the durable gap and this table has never published a snapshot, so the "
           "same call legitimately goes on to publish one -- see the invariant note above the test";

    /// Re-recovery WAS triggered as an observable state transition (not a silent skip): the lane left
    /// `NeedsRecovery`, and `recoveryInstallCountForTest` -- a counter of exact recovery-result
    /// publications -- advanced.
    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Ready)
        << "ensureRefTableRecovered must have re-walked the durable stream and cleared the fence";
    EXPECT_GT(store->recoveryInstallCountForTest(), recovery_installs_before)
        << "a re-recovery install must be observable, not indistinguishable from never having run";

    /// ORDER, not a global zero: recovery's OWN checkpoint catch-up CAS is the boundary marker. NO
    /// snapshot-publish effect (the new snapshot's body PUT, nor the publisher's own checkpoint-advance
    /// CAS) may appear at or before it.
    const auto ckpt_cas_indices = backend->indicesFrom(OrderedFaultBackend::Op::Cas, ckpt_key, offset);
    const auto snap_put_indices = backend->indicesFrom(OrderedFaultBackend::Op::Put, next_snapshot_key, offset);
    ASSERT_GE(ckpt_cas_indices.size(), 2u)
        << "expected one checkpoint CAS from recovery's catch-up and one from the snapshot publisher";
    const size_t recovery_catchup_index = ckpt_cas_indices.front();
    const size_t publisher_ckpt_index = ckpt_cas_indices.back();
    ASSERT_FALSE(snap_put_indices.empty()) << "the recovered, uncovered candidate must have been published";
    for (const size_t snap_put_index : snap_put_indices)
        EXPECT_GT(snap_put_index, recovery_catchup_index)
            << "no snapshot-publish body PUT may precede recovery's own checkpoint reconciliation";
    EXPECT_LT(snap_put_indices.front(), publisher_ckpt_index)
        << "the snapshot publisher's own checkpoint CAS still runs after ITS OWN body PUT (INV-4), even "
           "immediately following recovery";

    /// STRONGEST form: the published snapshot is not merely "some snapshot from whichever view" -- it
    /// covers EXACTLY the recovered, previously-missing-from-cache frontier. Its id names the durable
    /// removal transaction, and the recovered cache (which the snapshot was built from) no longer
    /// resolves the removed ref.
    EXPECT_EQ(store->newestPublishedSnapshotIdForTest(ns), std::make_optional(missing_durable_txn))
        << "the published snapshot's frontier IS the durable transaction the stale cache was missing";
    EXPECT_FALSE(store->resolveRef(ns, "ref_1").has_value())
        << "the recovered (and now snapshotted) cache reflects the durable removal the stale view lacked";
}

/// ---------------------------------------------------------------------------------------------
/// 4. Publish backoff: characterized against a controlled clock (`PoolConfig::boot_ms_fn`)
/// ---------------------------------------------------------------------------------------------

/// `admitSnapshotPublishUnderStateLock`, `advancePublishBackoff` and `resetPublishBackoff` are private
/// to `CasRefLedger`, so they can only be characterized through the public dispatch surface
/// (`appendRefOps`/`resolveRef` triggering `maybeScheduleSnapshotPublish`, and
/// `waitForSnapshotPublishSettleForTest`/`ProfileEvents::CASRefSnapshotPublishDispatched` as the
/// observables). `CASRequestControllerBackoff` is a DIFFERENT mechanism (the request controller's
/// per-attempt retry backoff); this characterizes ONLY the per-table snapshot-publish dispatch backoff.
///
/// A controlled clock (`PoolConfig::boot_ms_fn`) DOES exist for this seam (`gtest_cas_ref_writer.cpp`'s
/// `C4BackoffDefersThenRetriesAndPublishes` already relies on it) -- so unlike the plan's anticipated
/// fallback, this pins literal accept/refuse decisions against exact clock offsets rather than only
/// attempt counts.
TEST(CASRefSnapshotPublishOrdering, PublishBackoffDecisionsAreCharacterized)
{
    using ProfileEvents::global_counters;
    auto backend = std::make_shared<OrderedFaultBackend>();

    /// A single-attempt request budget, exactly as `gtest_cas_ref_writer.cpp`'s
    /// `C4BackoffDefersThenRetriesAndPublishes` uses: with `max_attempts = 1` a faulted PUT resolves to a
    /// definite, non-`Committed` outcome on its own attempt, with no internal retry loop and so no
    /// wall-clock wait.
    CasRequestBudget budget;
    budget.max_attempts = 1;
    budget.attempt_timeout_ms = 100;
    budget.operation_deadline_ms = 5000;
    budget.lease_safety_margin_ms = 100;

    uint64_t fake_now = 1'000'000;
    PoolConfig config;
    config.snapshot_log_count_threshold = 0;              /// any nonempty tail is over-threshold
    config.snapshot_log_bytes_threshold = 1ULL << 40;
    config.snapshot_publish_backoff_initial_ms = 1000;
    config.snapshot_publish_backoff_max_ms = 4000;
    config.mount_lease_ttl_ms = std::chrono::milliseconds(10'000'000);
    config.boot_ms_fn = [&fake_now] { return fake_now; };
    config.cas_request_budget = budget;
    auto store = openPool(backend, config);
    const RootNamespace ns{"srv1/order_backoff"};

    ASSERT_EQ(publishRef(store, ns, "ref_1", 1), (RefTxnId{store->writerEpoch(), 1}));
    store->waitForSnapshotPublishSettleForTest(ns);   /// drain the birth's own auto-dispatched publish
    /// The birth's own auto-dispatch already published a snapshot at this point (threshold 0); the
    /// baseline every "no new publish yet" check below compares against.
    const auto snapshot_after_birth = store->newestPublishedSnapshotIdForTest(ns);
    ASSERT_TRUE(snapshot_after_birth.has_value());

    /// Fault the snapshot BODY put (never the `_ckpt` CAS -- an append-commit's OWN checkpoint write
    /// shares that key, and faulting it would drive the append lane into `NeedsRecovery` instead of
    /// exercising the snapshot-publish backoff this test targets). Exactly 3 failures: the next 3
    /// automatic dispatch attempts fail (arming, then doubling, then re-doubling the backoff); the 4th
    /// finds the fault disarmed and succeeds.
    backend->armPutFailure("_snap/", 3);

    const auto dispatchCount = [&] { return global_counters[ProfileEvents::CASRefSnapshotPublishDispatched].load(); };

    /// Attempt 1: admitted immediately (no backoff armed yet). Fails -> backoff armed at the initial 1000ms.
    ASSERT_EQ(publishRef(store, ns, "ref_2", 2), (RefTxnId{store->writerEpoch(), 2}));
    store->waitForSnapshotPublishSettleForTest(ns);
    const uint64_t d1 = dispatchCount();
    EXPECT_EQ(store->newestPublishedSnapshotIdForTest(ns), snapshot_after_birth)
        << "the failed attempt must not have advanced the published snapshot";

    /// Still within the 1000ms window: a further trigger must NOT re-dispatch.
    store->resolveRef(ns, "ref_1");
    store->waitForSnapshotPublishSettleForTest(ns);
    EXPECT_EQ(dispatchCount(), d1) << "a read within the initial backoff window must not re-dispatch";

    /// Cross the 1000ms deadline: exactly one retry dispatches (and fails again, doubling to 2000ms).
    fake_now += 1000;
    store->resolveRef(ns, "ref_1");
    store->waitForSnapshotPublishSettleForTest(ns);
    EXPECT_EQ(dispatchCount(), d1 + 1) << "past the first deadline, exactly one retry dispatches";

    /// Short of the DOUBLED (2000ms) deadline: still refused.
    fake_now += 1000;
    store->resolveRef(ns, "ref_1");
    store->waitForSnapshotPublishSettleForTest(ns);
    EXPECT_EQ(dispatchCount(), d1 + 1)
        << "advancePublishBackoff doubled the interval to 2000ms; 1000ms elapsed is not enough";

    /// Cross the doubled deadline: one more retry dispatches (and fails again -- the third and last armed
    /// failure -- doubling to the 4000ms cap).
    fake_now += 1000;
    store->resolveRef(ns, "ref_1");
    store->waitForSnapshotPublishSettleForTest(ns);
    EXPECT_EQ(dispatchCount(), d1 + 2) << "past the doubled deadline, exactly one more retry dispatches";

    /// Pin the 4000ms cap FROM BELOW: without this probe, a regression that stopped doubling at
    /// 2000ms, or that read `initial` where it means `max`, would still pass -- the only check so far
    /// is AT the +4000 crossing below. 2000ms past the doubled deadline is still short of the capped
    /// 4000ms backoff, so no third retry may dispatch yet.
    fake_now += 2000;
    store->resolveRef(ns, "ref_1");
    store->waitForSnapshotPublishSettleForTest(ns);
    EXPECT_EQ(dispatchCount(), d1 + 2)
        << "2000ms past the doubled deadline is still short of the capped 4000ms backoff";

    /// Cross the (capped) 4000ms deadline: the retry's fault budget is exhausted, so this attempt
    /// succeeds, and `resetPublishBackoff` clears the cooldown -- proved by the NEXT trigger dispatching
    /// with no wait at all.
    fake_now += 2000;
    store->resolveRef(ns, "ref_1");
    store->waitForSnapshotPublishSettleForTest(ns);
    EXPECT_EQ(dispatchCount(), d1 + 3) << "past the second (capped) deadline, the retry dispatches and succeeds";
    EXPECT_NE(store->newestPublishedSnapshotIdForTest(ns), snapshot_after_birth)
        << "the fault budget is exhausted, so this attempt actually advances the published snapshot";

    ASSERT_EQ(publishRef(store, ns, "ref_3", 3), (RefTxnId{store->writerEpoch(), 3}));
    store->waitForSnapshotPublishSettleForTest(ns);
    EXPECT_EQ(dispatchCount(), d1 + 4)
        << "resetPublishBackoff must have cleared the cooldown: the very next over-threshold trigger, at "
           "the SAME clock reading as the successful publish, dispatches immediately with no wait";

    /// The assertion just above cannot tell a real reset from a no-op: the successful publish and this
    /// next trigger share one `fake_now`, so `now >= until` would still hold even with the stale
    /// (pre-reset) deadline in place. Arm one more failure and check that the schedule restarts from
    /// the INITIAL 1000ms interval rather than continuing from the 4000ms cap -- refused short of
    /// 1000ms, admitted at 1000ms -- which a no-op reset cannot produce (it would refuse both probes,
    /// since the stale deadline is still far in the future).
    backend->armPutFailure("_snap/", 1);
    ASSERT_EQ(publishRef(store, ns, "ref_4", 4), (RefTxnId{store->writerEpoch(), 4}));
    store->waitForSnapshotPublishSettleForTest(ns);
    const uint64_t d2 = dispatchCount();
    fake_now += 500;
    store->resolveRef(ns, "ref_1");
    store->waitForSnapshotPublishSettleForTest(ns);
    EXPECT_EQ(dispatchCount(), d2) << "short of 1000ms since the reset, no retry may dispatch yet";
    fake_now += 500;
    store->resolveRef(ns, "ref_1");
    store->waitForSnapshotPublishSettleForTest(ns);
    EXPECT_EQ(dispatchCount(), d2 + 1)
        << "resetPublishBackoff must have restarted the schedule at the INITIAL 1000ms interval, not "
           "left it continuing from the 4000ms cap";
}

TEST(CASRefSnapshotPublishOrdering, NotReadyRefusalBacksOffAndResetsAfterDurablePublish)
{
    using ProfileEvents::global_counters;
    auto backend = std::make_shared<OrderedFaultBackend>();

    CasRequestBudget budget;
    budget.max_attempts = 1;
    budget.attempt_timeout_ms = 100;
    budget.operation_deadline_ms = 5000;
    budget.lease_safety_margin_ms = 100;

    uint64_t fake_now = 2'000'000;
    PoolConfig config;
    config.snapshot_log_count_threshold = 0;
    config.snapshot_log_bytes_threshold = 1ULL << 40;
    config.snapshot_publish_backoff_initial_ms = 200;
    config.snapshot_publish_backoff_max_ms = 30'000;
    config.mount_lease_ttl_ms = std::chrono::milliseconds(10'000'000);
    config.boot_ms_fn = [&fake_now] { return fake_now; };
    config.cas_request_budget = budget;
    auto store = openPool(backend, config);
    const RootNamespace ns{"srv1/order_not_ready_backoff"};

    const auto dispatch_count = [&]
    {
        return global_counters[ProfileEvents::CASRefSnapshotPublishDispatched].load();
    };
    const auto backoff_count = [&]
    {
        return global_counters[ProfileEvents::CASRefSnapshotPublishBackoff].load();
    };

    ASSERT_EQ(publishRef(store, ns, "ref_1", 1), (RefTxnId{store->writerEpoch(), 1}));
    store->waitForSnapshotPublishSettleForTest(ns);
    ASSERT_EQ(
        store->newestPublishedSnapshotIdForTest(ns),
        std::make_optional(RefTxnId{store->writerEpoch(), 1}));

    /// Direct calls remain one attempt per invocation even while a cooldown is armed. This first
    /// refusal is also the non-hanging RED discriminator: without the production fix the backoff
    /// counter is unchanged, so the fatal assertion stops before settlement can redispatch forever.
    forceAdoptablePublishWedge(store, ns, 2, "ref_2", 2);
    ASSERT_EQ(store->laneStateForTest(ns), RefLaneState::Wedged);
    const uint64_t warmup_backoffs = backoff_count();
    EXPECT_FALSE(store->tryPublishSnapshotAndAdvanceCheckpointOnce(ns));
    ASSERT_EQ(backoff_count(), warmup_backoffs + 1)
        << "one admitted NotReady refusal must arm the initial snapshot-publish backoff";
    EXPECT_FALSE(store->tryPublishSnapshotAndAdvanceCheckpointOnce(ns));
    ASSERT_EQ(backoff_count(), warmup_backoffs + 2)
        << "a direct call is still one admitted attempt per invocation and doubles the cooldown";

    /// Resolve the exact wedge through the real append-lane adoption path. Its adopted txn and
    /// the caller's own txn raise the table above threshold, but the warm-up cooldown prevents an
    /// automatic publish while the fixture prepares one uncovered tail entry.
    ASSERT_EQ(publishRef(store, ns, "ref_3", 3), (RefTxnId{store->writerEpoch(), 3}));
    ASSERT_EQ(store->laneStateForTest(ns), RefLaneState::Ready);

    bool appended_during_capture = false;
    store->setSnapshotAfterCaptureHookForTest([&]
    {
        if (appended_during_capture)
            return;
        appended_during_capture = true;
        EXPECT_EQ(publishRef(store, ns, "ref_4", 4), (RefTxnId{store->writerEpoch(), 4}));
    });
    ASSERT_TRUE(store->tryPublishSnapshotAndAdvanceCheckpointOnce(ns));
    store->setSnapshotAfterCaptureHookForTest(nullptr);
    ASSERT_TRUE(appended_during_capture);
    ASSERT_EQ(
        store->newestPublishedSnapshotIdForTest(ns),
        std::make_optional(RefTxnId{store->writerEpoch(), 3}));

    /// One uncovered tail entry now exists with no cooldown. Make the lane non-Ready before the read
    /// trigger, so the first production dispatch is an admitted refusal rather than a body PUT.
    forceAdoptablePublishWedge(store, ns, 5, "ref_5", 5);
    ASSERT_EQ(store->laneStateForTest(ns), RefLaneState::Wedged);
    const uint64_t production_dispatches = dispatch_count();
    const uint64_t production_backoffs = backoff_count();

    store->resolveRef(ns, "ref_1");
    store->waitForSnapshotPublishSettleForTest(ns);
    ASSERT_EQ(dispatch_count(), production_dispatches + 1)
        << "the over-threshold table must dispatch one admitted refusal";
    ASSERT_EQ(backoff_count(), production_backoffs + 1)
        << "the admitted refusal must arm exactly one 200ms cooldown";

    /// Settlement re-evaluates immediately. The armed deadline must stop that handoff from becoming a
    /// second dispatch, and an ordinary trigger at the same BOOTTIME instant must also remain refused.
    store->resolveRef(ns, "ref_1");
    store->waitForSnapshotPublishSettleForTest(ns);
    EXPECT_EQ(dispatch_count(), production_dispatches + 1);
    EXPECT_EQ(backoff_count(), production_backoffs + 1);

    uint64_t admitted_retries = 0;
    uint64_t delay_ms = 200;
    const std::vector<uint64_t> next_delays{
        400, 800, 1600, 3200, 6400, 12'800, 25'600, 30'000, 30'000};
    for (const uint64_t next_delay_ms : next_delays)
    {
        fake_now += delay_ms - 1;
        store->resolveRef(ns, "ref_1");
        store->waitForSnapshotPublishSettleForTest(ns);
        EXPECT_EQ(dispatch_count(), production_dispatches + 1 + admitted_retries)
            << "no retry may dispatch one millisecond before the current deadline";
        EXPECT_EQ(backoff_count(), production_backoffs + 1 + admitted_retries);

        ++fake_now;
        store->resolveRef(ns, "ref_1");
        store->waitForSnapshotPublishSettleForTest(ns);
        ++admitted_retries;
        EXPECT_EQ(dispatch_count(), production_dispatches + 1 + admitted_retries)
            << "exactly one retry must dispatch at the BOOTTIME deadline";
        EXPECT_EQ(backoff_count(), production_backoffs + 1 + admitted_retries)
            << "each admitted NotReady retry advances the same bounded cooldown once";
        delay_ms = next_delay_ms;
    }

    /// The last two intervals are both 30 seconds: the retry at the first capped deadline must arm the
    /// same cap, rather than overflow, reset, or continue doubling.
    EXPECT_EQ(delay_ms, 30'000u);

    /// Adopt the outstanding wedge through production and publish durably. The hook commits one later
    /// txn after capture while the capped cooldown is still armed; a correct durable publication resets
    /// that cooldown, so an immediate same-clock read dispatches the leftover tail without waiting.
    ASSERT_EQ(publishRef(store, ns, "ref_6", 6), (RefTxnId{store->writerEpoch(), 6}));
    ASSERT_EQ(store->laneStateForTest(ns), RefLaneState::Ready);
    bool appended_after_reset_capture = false;
    store->setSnapshotAfterCaptureHookForTest([&]
    {
        if (appended_after_reset_capture)
            return;
        appended_after_reset_capture = true;
        EXPECT_EQ(publishRef(store, ns, "ref_7", 7), (RefTxnId{store->writerEpoch(), 7}));
    });
    ASSERT_TRUE(store->tryPublishSnapshotAndAdvanceCheckpointOnce(ns));
    store->setSnapshotAfterCaptureHookForTest(nullptr);
    ASSERT_TRUE(appended_after_reset_capture);
    ASSERT_EQ(
        store->newestPublishedSnapshotIdForTest(ns),
        std::make_optional(RefTxnId{store->writerEpoch(), 6}));

    const uint64_t dispatches_before_reset_probe = dispatch_count();
    store->resolveRef(ns, "ref_1");
    store->waitForSnapshotPublishSettleForTest(ns);
    EXPECT_EQ(dispatch_count(), dispatches_before_reset_probe + 1)
        << "durable publication must clear the capped cooldown for an immediate same-clock trigger";
    EXPECT_EQ(
        store->newestPublishedSnapshotIdForTest(ns),
        std::make_optional(RefTxnId{store->writerEpoch(), 7}));
}
