#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasServerRootFormats.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/tests/cas_test_helpers.h>

#include <chrono>
#include <limits>
#include <memory>
#include <vector>

namespace DB::ErrorCodes
{
extern const int NETWORK_ERROR;
}

using namespace DB::Cas;

namespace
{

PoolConfig singleAttemptConfig()
{
    PoolConfig config{
        .pool_prefix = "p",
        .server_root_id = "test",
        .background_watermark = false,
    };
    config.cas_request_budget.max_attempts = 1;
    config.cas_request_budget.attempt_timeout_ms = 100;
    config.cas_request_budget.operation_deadline_ms = 5000;
    config.cas_request_budget.lease_safety_margin_ms = 100;
    return config;
}

PoolPtr openSingleAttemptPool(const BackendPtr & backend)
{
    DB::Cas::tests::seedPoolMetaForRestart(*backend);
    return Pool::open(backend, singleAttemptConfig());
}

PoolPtr openFrozenSingleAttemptPool(const BackendPtr & backend)
{
    DB::Cas::tests::seedPoolMetaForRestart(*backend);
    PoolConfig config = singleAttemptConfig();
    config.boot_ms_fn = [] { return uint64_t{0}; };
    config.mount_renew_period = std::chrono::hours{1};
    return Pool::open(backend, config);
}

PartWriteTxnPtr stageEmptyManifest(
    const PoolPtr & store, const RootNamespace & ns, const String & ref_name, ManifestId & id)
{
    PartWriteInfo info;
    info.intended_namespace = ns;
    info.intended_ref = ns.string() + "/" + ref_name;
    auto build = store->beginPartWrite(std::move(info));
    id = build->stageManifest({});
    return build;
}

void publishEmptyRef(const PoolPtr & store, const RootNamespace & ns, const String & ref_name)
{
    ManifestId id;
    auto build = stageEmptyManifest(store, ns, ref_name, id);
    build->precommitAdd(ns, ref_name, id);
    build->promote(ns, ref_name, build->buildId(), id);
}

uint64_t leaveRejectedCleanupDuty(const PoolPtr & store, const RootNamespace & ns)
{
    ManifestId rejected_id;
    auto rejected = stageEmptyManifest(store, ns, "rejected", rejected_id);
    const uint64_t rejected_seq = rejected->buildSeq();

    store->setMountDeadline(100);
    DB::Cas::tests::expectThrowsCode(
        DB::ErrorCodes::NETWORK_ERROR,
        [&] { rejected->precommitAdd(ns, "rejected", rejected_id); });
    EXPECT_EQ(rejected->precommitState(), PartWriteTxn::PrecommitState::Uncertain);

    rejected.reset();
    EXPECT_EQ(store->minActive(), rejected_seq);
    store->setMountDeadline(30000);
    return rejected_seq;
}

}

/// Removing the deferred-cleanup transfer from `~PartWriteTxn` makes this test fail at the first
/// `minActive` assertion: the old unconditional destructor retirement advances the build floor while
/// the owner-grant outcome is still unknown. The later assertions pin the other half of the duty: the
/// next mutation resolves the durable wedge, removes the exact old precommit, and only then retires it.
TEST(CASWriterDuties, UncertainAdoptedGrantStaysActiveUntilTheNextMutationRemovesIt)
{
    auto backend = std::make_shared<DB::Cas::tests::ChunkFaultBackend>();
    auto store = openSingleAttemptPool(backend);
    const RootNamespace ns{"srv1/writer_duty_adopt"};
    DB::Cas::tests::casAdmitRecoverableEntry(*backend, store->layout(), ns, store->liveWriterEpoch());

    ManifestId abandoned_id;
    auto abandoned = stageEmptyManifest(store, ns, "abandoned", abandoned_id);
    const uint64_t abandoned_seq = abandoned->buildSeq();
    const String abandoned_manifest_key = store->layout().manifestKey(abandoned_id);

    backend->fault_substr = store->layout().namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_log/";
    backend->mode = DB::Cas::tests::ChunkFaultBackend::Mode::LandedThenLost;
    backend->fault_count = 1;
    DB::Cas::tests::expectThrowsCode(
        DB::ErrorCodes::NETWORK_ERROR,
        [&] { abandoned->precommitAdd(ns, "abandoned", abandoned_id); });
    ASSERT_TRUE(store->refLaneWedgedForTest(ns));
    ASSERT_EQ(abandoned->precommitState(), PartWriteTxn::PrecommitState::Uncertain);

    abandoned.reset();
    EXPECT_EQ(store->minActive(), abandoned_seq)
        << "an unresolved owner grant must keep its build active after the transaction object is gone";

    backend->mode = DB::Cas::tests::ChunkFaultBackend::Mode::None;
    ManifestId successor_id;
    auto successor = stageEmptyManifest(store, ns, "successor", successor_id);
    const uint64_t successor_seq = successor->buildSeq();
    successor->precommitAdd(ns, "successor", successor_id);

    EXPECT_FALSE(store->refLaneWedgedForTest(ns));
    EXPECT_EQ(store->minActive(), successor_seq)
        << "the abandoned build retires only after its exact cleanup duty settles";
    EXPECT_EQ(
        store->livePrecommitsForTest(ns),
        (std::set<std::pair<String, ManifestRef>>{{"successor", successor_id.ref}}));
    EXPECT_TRUE(backend->head(abandoned_manifest_key).exists)
        << "the removed precommit body remains GC-owned until its decrement is sealed";

    successor->abandon();
    EXPECT_TRUE(store->livePrecommitsForTest(ns).empty());
}

/// Removing the absent-owner arm makes the deferred duty either stay forever or try to remove an owner
/// that was never transmitted. A controller pre-attempt refusal proves the grant absent; the next
/// healthy mutation must drain that duty as a no-op and retire the old build before publishing itself.
TEST(CASWriterDuties, ProvenAbsentGrantDrainsAsNoOpBeforeTheNextMutation)
{
    auto backend = std::make_shared<DB::Cas::InMemoryBackend>();
    DB::Cas::tests::seedPoolMetaForRestart(*backend);
    PoolConfig config = singleAttemptConfig();
    config.boot_ms_fn = [] { return uint64_t{0}; };
    config.mount_renew_period = std::chrono::hours{1};
    auto store = Pool::open(backend, config);
    const RootNamespace ns{"srv1/writer_duty_reject"};

    ManifestId rejected_id;
    auto rejected = stageEmptyManifest(store, ns, "rejected", rejected_id);
    const uint64_t rejected_seq = rejected->buildSeq();

    store->setMountDeadline(100);
    DB::Cas::tests::expectThrowsCode(
        DB::ErrorCodes::NETWORK_ERROR,
        [&] { rejected->precommitAdd(ns, "rejected", rejected_id); });
    ASSERT_FALSE(store->refLaneWedgedForTest(ns));
    ASSERT_EQ(rejected->precommitState(), PartWriteTxn::PrecommitState::Uncertain);

    rejected.reset();
    EXPECT_EQ(store->minActive(), rejected_seq)
        << "the destructor cannot retire even an uncertain grant whose rejection has not been consumed";

    store->setMountDeadline(30000);
    ManifestId successor_id;
    auto successor = stageEmptyManifest(store, ns, "successor", successor_id);
    const uint64_t successor_seq = successor->buildSeq();
    successor->precommitAdd(ns, "successor", successor_id);

    EXPECT_EQ(store->minActive(), successor_seq);
    EXPECT_EQ(
        store->livePrecommitsForTest(ns),
        (std::set<std::pair<String, ManifestRef>>{{"successor", successor_id.ref}}));

    successor->abandon();
    EXPECT_TRUE(store->livePrecommitsForTest(ns).empty());
}

/// The model gate recorded both wedge-resolution witnesses (adopt and reject); the C++ suite drove
/// only the adopt arm above. This drives an uncertain grant into an ACTUAL wedged lane -- unlike
/// `ProvenAbsentGrantDrainsAsNoOpBeforeTheNextMutation`'s controller pre-attempt refusal, which never
/// wedges at all -- and resolves it as REJECT: `Mode::Unresolved` lands nothing, so the next attempt's
/// resolve-before-reissue GET proves the key absent. The duty must then drain as a no-op: no
/// `OwnerTransition` removal is owed for an absent precommit, the wedge clears, and `minActive` advances
/// past the rejected build exactly as the no-wedge reject arm does.
TEST(CASWriterDuties, WedgeResolvedAsRejectDrainsTheDutyAsNoOp)
{
    auto backend = std::make_shared<DB::Cas::tests::ChunkFaultBackend>();
    auto store = openSingleAttemptPool(backend);
    const RootNamespace ns{"srv1/writer_duty_wedge_reject"};
    DB::Cas::tests::casAdmitRecoverableEntry(*backend, store->layout(), ns, store->liveWriterEpoch());

    ManifestId rejected_id;
    auto rejected = stageEmptyManifest(store, ns, "rejected", rejected_id);
    const uint64_t rejected_seq = rejected->buildSeq();

    backend->fault_substr = store->layout().namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_log/";
    backend->mode = DB::Cas::tests::ChunkFaultBackend::Mode::Unresolved;
    backend->fault_count = 1;
    DB::Cas::tests::expectThrowsCode(
        DB::ErrorCodes::NETWORK_ERROR,
        [&] { rejected->precommitAdd(ns, "rejected", rejected_id); });
    ASSERT_TRUE(store->refLaneWedgedForTest(ns));
    ASSERT_EQ(rejected->precommitState(), PartWriteTxn::PrecommitState::Uncertain);

    rejected.reset();
    EXPECT_EQ(store->minActive(), rejected_seq)
        << "an unresolved owner grant must keep its build active after the transaction object is gone";

    backend->mode = DB::Cas::tests::ChunkFaultBackend::Mode::None;
    ManifestId successor_id;
    auto successor = stageEmptyManifest(store, ns, "successor", successor_id);
    const uint64_t successor_seq = successor->buildSeq();
    successor->precommitAdd(ns, "successor", successor_id);

    EXPECT_FALSE(store->refLaneWedgedForTest(ns));
    EXPECT_EQ(store->minActive(), successor_seq)
        << "the rejected build retires only after its exact cleanup duty settles as a no-op";
    EXPECT_EQ(
        store->livePrecommitsForTest(ns),
        (std::set<std::pair<String, ManifestRef>>{{"successor", successor_id.ref}}));

    successor->abandon();
    EXPECT_TRUE(store->livePrecommitsForTest(ns).empty());
}

/// Removing `mutateRefsAfterWriterCleanup` from the `dropRef` delegate leaves the rejected build at
/// `minActive` even though the ref removal succeeds. The observable floor proves the direct API
/// serviced the inherited cleanup duty before performing its own mutation.
TEST(CASWriterDuties, DropRefServicesPendingDutyBeforeRemovingTheRef)
{
    auto backend = std::make_shared<DB::Cas::InMemoryBackend>();
    auto store = openFrozenSingleAttemptPool(backend);
    const RootNamespace ns{"srv1/writer_duty_drop_ref"};
    publishEmptyRef(store, ns, "target");
    leaveRejectedCleanupDuty(store, ns);

    store->dropRef(ns, "target");

    EXPECT_FALSE(store->resolveRef(ns, "target").has_value());
    EXPECT_EQ(store->minActive(), store->peekNextBuildSeq());
}

/// Removing the shared drain seam from `updateRefPublishedAt` lets the timestamp mutation overtake a
/// pending writer duty. The update remains observable, while the independent watermark assertion
/// catches that bypass.
TEST(CASWriterDuties, UpdateRefPublishedAtServicesPendingDutyBeforeUpdatingTheRef)
{
    auto backend = std::make_shared<DB::Cas::InMemoryBackend>();
    auto store = openFrozenSingleAttemptPool(backend);
    const RootNamespace ns{"srv1/writer_duty_update_ref"};
    publishEmptyRef(store, ns, "target");
    leaveRejectedCleanupDuty(store, ns);

    store->updateRefPublishedAt(ns, "target", [](RefPublishedAtUpdate & update) { update.published_at_ms = 17; });

    const auto resolved = store->resolveRef(ns, "target");
    ASSERT_TRUE(resolved.has_value());
    EXPECT_EQ(resolved->published_at_ms, 17);
    EXPECT_EQ(store->minActive(), store->peekNextBuildSeq());
}

/// Each public namespace-removal overload has its own Pool delegate. Omitting the shared seam from
/// either one still removes the namespace but strands the rejected build at the active floor, so the
/// two independent cases protect both forwarding paths.
TEST(CASWriterDuties, DropNamespaceOverloadsServicePendingDutyBeforeRemoval)
{
    {
        auto backend = std::make_shared<DB::Cas::InMemoryBackend>();
        auto store = openFrozenSingleAttemptPool(backend);
        const RootNamespace ns{"srv1/writer_duty_drop_namespace"};
        publishEmptyRef(store, ns, "target");
        leaveRejectedCleanupDuty(store, ns);

        store->dropNamespace(ns);

        EXPECT_TRUE(store->listRefs(ns).empty());
        EXPECT_EQ(store->minActive(), store->peekNextBuildSeq());
    }

    {
        auto backend = std::make_shared<DB::Cas::InMemoryBackend>();
        auto store = openFrozenSingleAttemptPool(backend);
        const RootNamespace ns{"srv1/writer_duty_drop_namespace_life"};
        publishEmptyRef(store, ns, "target");
        const NamespaceLifeId life = store->namespaceLife(ns);
        leaveRejectedCleanupDuty(store, ns);

        store->dropNamespace(life);

        EXPECT_TRUE(store->listRefs(ns).empty());
        EXPECT_EQ(store->minActive(), store->peekNextBuildSeq());
    }
}

/// The explicit snapshot/checkpoint attempt is the audited sibling mutation: without the common seam
/// it may publish ledger state while leaving the older writer duty pinned. Its return value is allowed
/// to be false; advancing the active floor is the cleanup contract under test.
TEST(CASWriterDuties, SnapshotAttemptServicesPendingDutyBeforePublishingLedgerState)
{
    auto backend = std::make_shared<DB::Cas::InMemoryBackend>();
    auto store = openFrozenSingleAttemptPool(backend);
    const RootNamespace ns{"srv1/writer_duty_snapshot"};
    publishEmptyRef(store, ns, "target");
    leaveRejectedCleanupDuty(store, ns);

    static_cast<void>(store->tryPublishSnapshotAndAdvanceCheckpointOnce(ns));

    EXPECT_EQ(store->minActive(), store->peekNextBuildSeq());
}

/// Removing the pending-duty term from `Pool` teardown makes this test fail at the farewell
/// assertion: a clean marker would falsely certify that the durable precommit below has no remaining
/// writer work. The unclean handoff forces a fresh writer epoch; its arithmetic recovery seal then
/// makes the ordinary stale-precommit sweep the crash-remnant cleanup path.
TEST(CASWriterDuties, PendingDutySkipsCleanFarewellAndSuccessorSweepsTheCrashRemnant)
{
    auto backend = std::make_shared<DB::Cas::InMemoryBackend>();
    DB::Cas::tests::seedPoolMetaForRestart(*backend);
    const CasRequestBudget budget{
        .attempt_timeout_ms = 50,
        .operation_deadline_ms = 500,
        .max_attempts = 1,
        .lease_safety_margin_ms = 50,
    };
    const RootNamespace ns{"srv1/writer_duty_crash"};

    auto predecessor = Pool::open(backend, PoolConfig{
        .pool_prefix = "p",
        .server_id = UInt128(1),
        .server_root_id = "test",
        .background_watermark = false,
        .mount_lease_ttl_ms = std::chrono::milliseconds(500),
        .mount_renew_period = std::chrono::milliseconds(100),
        .cas_request_budget = budget,
    });
    DB::Cas::tests::casAdmitRecoverableEntry(
        *backend, predecessor->layout(), ns, predecessor->liveWriterEpoch());

    ManifestId abandoned_id;
    auto abandoned = stageEmptyManifest(predecessor, ns, "abandoned", abandoned_id);
    abandoned->precommitAdd(ns, "abandoned", abandoned_id);
    const uint64_t predecessor_epoch = predecessor->writerEpoch();
    const Layout layout = predecessor->layout();
    const String mount_key = layout.mountKey("test");

    abandoned.reset();
    predecessor.reset();

    const auto mount = backend->get(mount_key);
    ASSERT_TRUE(mount.has_value());
    EXPECT_NE(decodeMountLease(mount->bytes).min_active, std::numeric_limits<uint64_t>::max())
        << "a live writer-cleanup duty forbids the clean-release certificate";

    uint64_t fake_boot = 0;
    std::vector<uint64_t> waits;
    auto successor_store = Pool::open(backend, PoolConfig{
        .pool_prefix = "p",
        .server_id = UInt128(1),
        .server_root_id = "test",
        .background_watermark = false,
        .mount_lease_ttl_ms = std::chrono::milliseconds(500),
        .mount_renew_period = std::chrono::milliseconds(100),
        .cas_request_budget = budget,
        .boot_ms_fn = [&] { return fake_boot; },
        .wait_sleep_fn = [&](uint64_t ms) { fake_boot += ms; waits.push_back(ms); },
    });
    ASSERT_GT(successor_store->writerEpoch(), predecessor_epoch);
    ASSERT_FALSE(waits.empty()) << "the predecessor supplied no clean-death certificate";

    ManifestId successor_id;
    auto successor = stageEmptyManifest(successor_store, ns, "successor", successor_id);
    successor->precommitAdd(ns, "successor", successor_id);

    EXPECT_EQ(
        successor_store->livePrecommitsForTest(ns),
        (std::set<std::pair<String, ManifestRef>>{{"successor", successor_id.ref}}));
    const auto seal = successor_store->lastEpochSealForTest(ns);
    ASSERT_TRUE(seal.has_value());
    EXPECT_EQ(seal->writer_epoch, predecessor_epoch);

    successor->abandon();
}

/// The duty queue above only ever resolves the ref
/// table's precommit BINDING -- a rejected grant's manifest BODY is orphan from birth (no owner ever
/// named it, so the edge-before-observe `+1` a durable precommit would have folded never landed
/// either) and its reclaim is entirely the orphan sweep's job, gated on the one thing the duty queue
/// cannot give it: the build's own epoch durably closed. This drives that closure (the same crash
/// pattern as `PendingDutySkipsCleanFarewellAndSuccessorSweepsTheCrashRemnant`, but the predecessor's
/// build is REJECTED rather than adopted) and then runs real GC rounds until the body is gone.
TEST(CASWriterDuties, RejectedAttemptBodyIsEventuallyNominatedAndSwept)
{
    auto backend = std::make_shared<DB::Cas::tests::ChunkFaultBackend>();
    DB::Cas::tests::seedPoolMetaForRestart(*backend);
    const CasRequestBudget budget{
        .attempt_timeout_ms = 50,
        .operation_deadline_ms = 500,
        .max_attempts = 1,
        .lease_safety_margin_ms = 50,
    };
    /// Rooted under the POOL's OWN `server_root_id` ("test", unlike this file's other fixtures, which
    /// stay under "srv1" precisely because they never drive the orphan sweep): `prefixEligible`'s
    /// watermark floor is looked up by walking the NAMESPACE's own prefix segments for a live mount
    /// lease, so a namespace rooted under any other server-root would find no floor and retain forever
    /// regardless of epoch/coverage.
    const RootNamespace ns{"test/writer_duty_rejected_sweep"};

    auto predecessor = Pool::open(backend, PoolConfig{
        .pool_prefix = "p",
        .server_id = UInt128(1),
        .server_root_id = "test",
        .manifest_sweep_list_budget_keys = 100,
        .manifest_sweep_delete_budget_keys = 100,
        .gc_fold_max_defer_rounds = 0,
        .background_watermark = false,
        .mount_lease_ttl_ms = std::chrono::milliseconds(500),
        .mount_renew_period = std::chrono::milliseconds(100),
        .cas_request_budget = budget,
    });

    /// A real, fully-promoted ref through the ordinary production write path (no seeded catalog/ckpt)
    /// gives the namespace genuine epoch-1 content, so the successor's recovery below has something
    /// real to close -- unlike a pre-attempt-refused grant, which never touches the backend at all and
    /// so leaves the namespace's fold coverage exactly where it started.
    publishEmptyRef(predecessor, ns, "anchor");

    ManifestId rejected_id;
    auto rejected = stageEmptyManifest(predecessor, ns, "rejected", rejected_id);
    const String rejected_manifest_key = predecessor->layout().manifestKey(rejected_id);
    ASSERT_TRUE(backend->head(rejected_manifest_key).exists)
        << "stageManifest's body write is unconditional; only the owner grant is refused below";

    /// `Unresolved` lands nothing, so the wedge it leaves resolves as a conclusive REJECT once the
    /// successor's own recovery walks past it -- unlike the ADOPT-arm crash-remnant test, this
    /// manifest never becomes a live owner in any epoch. `anchor`'s real birth just above minted a
    /// genuine (random) incarnation, so the fault key is computed from the namespace's ACTUAL life,
    /// not the deterministic `fixtureLife` fallback a raw, never-touched fixture would use.
    backend->fault_substr = predecessor->layout().namespaceStreamPrefix(predecessor->namespaceLife(ns)) + "_log/";
    backend->mode = DB::Cas::tests::ChunkFaultBackend::Mode::Unresolved;
    backend->fault_count = 1;
    DB::Cas::tests::expectThrowsCode(
        DB::ErrorCodes::NETWORK_ERROR,
        [&] { rejected->precommitAdd(ns, "rejected", rejected_id); });
    ASSERT_TRUE(predecessor->refLaneWedgedForTest(ns));
    ASSERT_EQ(rejected->precommitState(), PartWriteTxn::PrecommitState::Uncertain);
    const uint64_t predecessor_epoch = predecessor->writerEpoch();

    rejected.reset();
    predecessor.reset();

    uint64_t fake_boot = 0;
    std::vector<uint64_t> waits;
    auto successor_store = Pool::open(backend, PoolConfig{
        .pool_prefix = "p",
        .server_id = UInt128(1),
        .server_root_id = "test",
        .manifest_sweep_list_budget_keys = 100,
        .manifest_sweep_delete_budget_keys = 100,
        .gc_fold_max_defer_rounds = 0,
        .background_watermark = false,
        .mount_lease_ttl_ms = std::chrono::milliseconds(500),
        .mount_renew_period = std::chrono::milliseconds(100),
        .cas_request_budget = budget,
        .boot_ms_fn = [&] { return fake_boot; },
        .wait_sleep_fn = [&](uint64_t ms) { fake_boot += ms; waits.push_back(ms); },
    });
    ASSERT_GT(successor_store->writerEpoch(), predecessor_epoch);
    ASSERT_FALSE(waits.empty()) << "the predecessor supplied no clean-death certificate";

    /// An ordinary successor mutation both drains the inherited duty as a no-op (the rejected grant
    /// was never durable) and forces the predecessor's dead epoch to close with an arithmetic seal --
    /// the fact rule (1) of the sweep's deletion premise reads.
    ManifestId successor_id;
    auto successor = stageEmptyManifest(successor_store, ns, "successor", successor_id);
    successor->precommitAdd(ns, "successor", successor_id);

    EXPECT_EQ(
        successor_store->livePrecommitsForTest(ns),
        (std::set<std::pair<String, ManifestRef>>{{"successor", successor_id.ref}}));
    const auto seal = successor_store->lastEpochSealForTest(ns);
    ASSERT_TRUE(seal.has_value());
    EXPECT_EQ(seal->writer_epoch, predecessor_epoch);

    Gc gc(successor_store, hexToU128("000000000000000000000000000000e1"));
    for (int round = 0; round < 16 && backend->head(rejected_manifest_key).exists; ++round)
        DB::Cas::tests::runRegularRoundReclaiming(gc);

    EXPECT_FALSE(backend->head(rejected_manifest_key).exists)
        << "the rejected attempt's orphan manifest must eventually be nominated and swept once its "
           "build epoch is durably closed";

    successor->abandon();
}

/// The settlement's own ordering is load-bearing: append the exact `OwnerTransition` removal (or
/// observe conclusive absence), only then retire the build seq, only then drop the duty -- a throw
/// between those steps must leave the duty owned by nobody but the queue. Faulting the SETTLEMENT's
/// append (not the original grant, which is a plain pre-attempt refusal here) proves the retry path
/// directly: the duty survives the throw and the mutation it was blocking aborts with it, then the
/// very next drain -- once the fault clears -- settles the duty and lets that mutation proceed.
TEST(CASWriterDuties, DutySurvivesSettlementFailureForRetry)
{
    auto backend = std::make_shared<DB::Cas::tests::ChunkFaultBackend>();
    auto store = openFrozenSingleAttemptPool(backend);
    const RootNamespace ns{"srv1/writer_duty_settlement_retry"};
    DB::Cas::tests::casAdmitRecoverableEntry(*backend, store->layout(), ns, store->liveWriterEpoch());
    publishEmptyRef(store, ns, "target");

    /// A plain, unfaulted precommit that is simply destroyed without promote/abandon: Durable, never
    /// settled, and (unlike a proven-absent grant) its duty's own settlement owes a REAL
    /// `OwnerTransition` removal -- exactly the append this test needs to fault.
    ManifestId durable_id;
    auto durable = stageEmptyManifest(store, ns, "durable", durable_id);
    const uint64_t durable_seq = durable->buildSeq();
    durable->precommitAdd(ns, "durable", durable_id);
    durable.reset();
    ASSERT_TRUE(store->writerCleanupDutiesPendingForTest());

    backend->fault_substr = store->layout().namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_log/";
    backend->mode = DB::Cas::tests::ChunkFaultBackend::Mode::Unresolved;
    backend->fault_count = 1;
    DB::Cas::tests::expectThrowsCode(
        DB::ErrorCodes::NETWORK_ERROR,
        [&] { store->dropRef(ns, "target"); });

    EXPECT_TRUE(store->writerCleanupDutiesPendingForTest())
        << "a settlement that throws must retain the duty for retry, never lose it";
    EXPECT_TRUE(store->resolveRef(ns, "target").has_value())
        << "the settlement's failure must abort the mutation it was blocking too, not just its own append";
    EXPECT_EQ(store->minActive(), durable_seq);

    backend->mode = DB::Cas::tests::ChunkFaultBackend::Mode::None;
    store->dropRef(ns, "target");

    EXPECT_FALSE(store->writerCleanupDutiesPendingForTest());
    EXPECT_FALSE(store->resolveRef(ns, "target").has_value());
    EXPECT_EQ(store->minActive(), store->peekNextBuildSeq())
        << "the retried drain settles the retained duty and lets the mutation proceed";
}
