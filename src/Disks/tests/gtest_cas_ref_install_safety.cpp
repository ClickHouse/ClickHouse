#include <gtest/gtest.h>

#include "config.h"

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefLedger.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefProtocol.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequestControl.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/MemoryTracker.h>
#include <Common/ProfileEvents.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <exception>
#include <functional>
#include <memory>
#include <string>

/// Task 3 (spec §A1, site 1): the region of `CasRefLedger::commitRefChunk` between "this chunk's
/// ref-log object is durable" and "the runtime records it".
///
/// Before the fix that region ran `applyRefLogTxn(rt->state, chunk_txn)`, which allocates (the COW
/// containers build an overlay) and can therefore throw `MEMORY_LIMIT_EXCEEDED`. A throw there left the
/// transaction durable but invisible to the writer -- and because a later transaction only needs
/// `greatest_applied < its own id` (contiguity is never checked), a snapshot published afterwards is
/// labelled with that LATER id, so recovery skips the stranded transaction permanently while GC, which
/// folds the ref logs themselves, still applies it. That divergence loses data (a stranded removal
/// leaves the writer holding a ref whose blobs GC deleted), which is why the install is now a
/// prepared-candidate swap: allocation-free, hence non-throwing, and enforced as such by
/// `DENY_ALLOCATIONS_IN_SCOPE`.
///
/// The suite name is prefixed `Cas` so it is covered by the `Cas*` unit-test gate filter.

namespace DB::ErrorCodes
{
extern const int NETWORK_ERROR;
extern const int CORRUPTED_DATA;
extern const int LOGICAL_ERROR;
extern const int MEMORY_LIMIT_EXCEEDED;
}

namespace ProfileEvents
{
extern const Event CASRefNeedsRecovery;
}

using namespace DB::Cas;

namespace
{

PoolPtr openPool(const BackendPtr & backend)
{
    /// A fresh pool with no residue, mirroring `gtest_cas_ref_chunked_flush.cpp`'s `openPool`.
    DB::Cas::tests::seedPoolMetaForRestart(*backend);
    return Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
}

/// As `openPool`, but with a SINGLE-attempt request budget, which is what makes one ambiguous `PUT`
/// conclusive: with retries allowed the controller's resolve-before-reissue would either re-`PUT` (the
/// object never landed) or prove the object durable (it did) and report `Committed`, and neither of the
/// wedge arms under test would ever be reached. Same budget shape as
/// `gtest_cas_ref_chunked_flush.cpp`'s `runChunkFailureCase`, including the short timeouts so there is
/// no inter-attempt sleep to serve.
PoolPtr openPoolSingleAttempt(const BackendPtr & backend)
{
    DB::Cas::tests::seedPoolMetaForRestart(*backend);
    PoolConfig cfg{.pool_prefix = "p", .server_root_id = "test"};
    CasRequestBudget budget;
    /// ONE attempt is the whole mechanism these tests need: it is what turns an injected lost
    /// acknowledgement into `Unresolved` instead of a transparent retry, and it does so independently of
    /// how fast the machine is.
    ///
    /// The operation deadline must therefore NOT sit at `attempt_timeout_ms`, which is where it used to.
    /// The controller's pre-send gate (`putIfAbsentControlled`: `now + attempt_timeout > deadline`
    /// returns `Unresolved` WITHOUT sending) is then a zero-width race that passes only if no
    /// millisecond tick elapses between the deadline capture and the gate. Under parallel-build load it
    /// loses: the gate fires first, nothing is sent, the injected fault is never reached, and the flush
    /// fails CLEAN -- so the product correctly does NOT wedge the lane and the wedge expectations flip.
    /// `UncertainPrecommitKeepsItsCleanupOwnerAndItsBody` was observed failing exactly that way (Task 9,
    /// `refLaneWedgedForTest` false at the wedge assertion), and every test on this fixture carries the
    /// same razor. Same root cause and same fix as `8f9e63c7a19` for the sweep-interruption test.
    ///
    /// A WIDE deadline keeps the request always actually sent, so the injected fault decides the outcome
    /// rather than the scheduler. Tests that want the pre-send REFUSAL instead use
    /// `openPoolFenceControlled`, where a frozen clock makes that refusal deterministic rather than raced.
    budget.max_attempts = 1;
    budget.attempt_timeout_ms = 100;
    budget.operation_deadline_ms = 5000;
    budget.lease_safety_margin_ms = 100;
    cfg.cas_request_budget = budget;
    return Pool::open(backend, cfg);
}

/// The mount-fence deadlines the pre-attempt tests drive, in the FROZEN boot clock of
/// `openPoolFenceControlled` (which is pinned at 0, so these are also the remaining lease budgets).
///
/// `CasMountRuntime` has TWO fence predicates and they are deliberately not the same:
///   `mayMutate`         -- `now < deadline`; the top-of-flush gate in `flushRefBatch`.
///   `refAppendFenceOk`  -- additionally `attempt_timeout_ms + lease_safety_margin_ms < deadline - now`,
///                          i.e. "there is room for one whole controlled attempt"; the `fence_ok`
///                          `commitRefChunk` hands to `putIfAbsentControlled`.
/// With `openPoolFenceControlled`'s budget below that margin is 100 + 100 = 200 ms, so a 100 ms
/// remaining lease sits BETWEEN them: the flush is admitted and then its very first pre-attempt gate
/// refuses. That is
/// exactly the production shape this task is about (a lease too short to start a write, not a lost
/// one), and it needs no fault injection at all -- which is the point: nothing is sent.
constexpr uint64_t FENCE_DEADLINE_HEALTHY_MS = 30000;
constexpr uint64_t FENCE_DEADLINE_REFUSES_ATTEMPT_MS = 100;

/// A legal blob-free part: stage an empty manifest, precommit, promote -- enough to drive real
/// ref-log transactions through the append lane.
void publishEmptyPart(const PoolPtr & s, const RootNamespace & ns, const String & ref)
{
    PartWriteInfo info;
    info.intended_namespace = ns;
    info.intended_ref = ns.string() + "/" + ref;
    auto build = s->beginPartWrite(info);
    const ManifestId id = build->stageManifest({});
    build->precommitAdd(ns, ref, id);
    build->promote(ns, ref, build->buildId(), id);
}

/// As `openPoolSingleAttempt`, but with the mount fence under the TEST's control instead of the wall
/// clock's:
///   - the boot clock is FROZEN at 0, so `setMountDeadline` alone decides both fence predicates and no
///     elapsed real time can flip one of them mid-test (the same load-bearing injection, for the same
///     reason, as `gtest_cas_ref_chunked_flush.cpp`'s `openPool`);
///   - lease renewal is parked an hour out, so the keeper's background renew cannot re-arm the deadline
///     underneath a test that just shortened it. Ten seconds (the default) would be enough in practice
///     and flaky in principle; this removes the race rather than betting on it.
PoolPtr openPoolFenceControlled(const BackendPtr & backend)
{
    DB::Cas::tests::seedPoolMetaForRestart(*backend);
    PoolConfig cfg{.pool_prefix = "p", .server_root_id = "test"};
    cfg.boot_ms_fn = [] { return uint64_t{0}; };
    cfg.mount_renew_period = std::chrono::milliseconds{3600000};
    CasRequestBudget budget;
    budget.max_attempts = 1;
    budget.attempt_timeout_ms = 100;
    budget.operation_deadline_ms = 5000;   /// strictly above attempt_timeout_ms: equality is a wall-clock race (validateCasRequestBudget)
    budget.lease_safety_margin_ms = 100;
    cfg.cas_request_budget = budget;
    return Pool::open(backend, cfg);
}

/// Runs `f`, requires it to throw the ref lane's retry-later condition, and returns the message so a
/// caller can assert WHICH condition it was. The message is the only place the `CasUnresolvedReason`
/// surfaces -- there is no accessor for it, by design (it is a diagnostic, not state) -- so this is how
/// a test proves the reason actually reached the decision site instead of defaulting.
String retryLaterMessageOf(const std::function<void()> & f)
{
    try
    {
        f();
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::NETWORK_ERROR) << e.message();
        return e.message();
    }
    ADD_FAILURE() << "expected the CAS retry-later condition, but nothing was thrown";
    return {};
}

/// Installs a ONE-SHOT throwing probe into the post-durable install regions (spec §A2): the next region
/// entered throws, every later one runs normally -- which is what lets a terminality test drive a
/// successful flush after the recovery transition.
///
/// The exception is built HERE, outside the region, and the probe only rethrows it: constructing a
/// `DB::Exception` inside the region would allocate and trip `DENY_ALLOCATIONS_IN_SCOPE`, so the test
/// would be exercising the guard instead of the recovery transition. `MEMORY_LIMIT_EXCEEDED` (what a
/// real tracked allocation failure raises) is used deliberately instead of `LOGICAL_ERROR`, which
/// aborts at construction in debug/sanitizer builds.
///
/// With §A1 landed this seam is the only way to reach `NeedsRecovery` from an install region:
/// install regions are allocation-free and therefore cannot throw on their own.
void armOneShotInstallFailure(const PoolPtr & store)
{
    auto planned = std::make_exception_ptr(DB::Exception(DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED,
        "simulated allocation failure inside the post-durable install region"));
    auto fired = std::make_shared<std::atomic<bool>>(false);
    store->setInstallRegionProbeForTest([planned, fired]
    {
        if (fired->exchange(true))
            return;
        /// The throw itself allocates its exception object through `malloc`, which the memory tracker
        /// does not see, so it would not trip the guard anyway -- re-allowing allocations for the
        /// duration of the throw makes that a stated property of the test rather than a bet on a libc++
        /// implementation detail.
        ALLOW_ALLOCATIONS_IN_SCOPE;
        std::rethrow_exception(planned);
    });
}

}

/// The post-durable install seam exists, is reached by an ordinary commit, and every transaction that
/// reaches it is RECORDED: the tail counter advances exactly once per install, and the ref resolves.
/// The equality is the point -- it is the invariant the old code could break, since there the install
/// was an allocating apply that could throw between the durable `PUT` and the counter bump.
TEST(CASRefInstallSafety, PostDurableInstallIsAllocationFree)
{
    auto backend = std::make_shared<DB::Cas::tests::CountingBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/install_safety_seam"};

    /// Fired on the calling thread by the flush leader, which is this thread; `atomic` regardless, so
    /// the assertions below cannot be read as depending on that.
    std::atomic<size_t> installs{0};
    store->setCarveHookForTest([&installs](CasRefLedger::CarvePhaseForTest phase)
    {
        if (phase == CasRefLedger::CarvePhaseForTest::PostDurableInstall)
            installs.fetch_add(1);
    });

    publishEmptyPart(store, ns, "part_a");
    store->setCarveHookForTest(nullptr);

    const size_t seen = installs.load();
    EXPECT_GT(seen, 0u) << "the post-durable install seam must be reached by an ordinary commit";
    /// No snapshot publish can interfere: the thresholds are 256 logs / 1 MiB and this part is a
    /// handful of tiny transactions, so the tail counter still holds every one of them.
    EXPECT_EQ(store->tailSinceSnapshotCountForTest(ns), seen)
        << "every durable transaction that entered the install region must be recorded in the tail";
    EXPECT_TRUE(store->resolveRef(ns, "part_a", /*allow_stale=*/false).has_value());
}

/// Task 4 (spec §A1, site 3). An `Unresolved` PUT must ALWAYS leave the lane wedged with the exact
/// {id, key, bytes} of the in-doubt object -- the wedge is the only record that can ever resolve it, and
/// on this path the object may already be durable. Building the wedge AFTER the PUT copies two `String`s
/// and could therefore fail on allocation, recording NEITHER the transaction nor the wedge: strictly
/// worse than a wedge, because the next append then mints a fresh id and proceeds against a state that
/// is missing a landed transaction. It is now preconstructed before the PUT and installed by a
/// non-throwing move.
///
/// `Mode::Unresolved` deliberately lands NOTHING, so this test also pins the other half of the wedge
/// contract: an ambiguous outcome wedges even when the object turns out never to have existed. The tail
/// counter must NOT advance -- an unproven transaction is not a recorded one.
TEST(CASRefInstallSafety, UnresolvedAlwaysRecordsTheWedge)
{
    auto backend = std::make_shared<DB::Cas::tests::ChunkFaultBackend>();
    auto store = openPoolSingleAttempt(backend);
    const RootNamespace ns{"srv1/unresolved_wedge"};
    /// Stage B (Task 4-C): pin `ns` to the Stage-A sentinel BEFORE its first real touch, so
    /// the fault injected below (computed from that same sentinel) lands on the key production
    /// actually writes to.
    DB::Cas::tests::casAdmitRecoverableEntry(*backend, store->layout(), ns, store->liveWriterEpoch());

    /// Scoped to THIS namespace's ref log, so nothing else the part publish writes (the manifest, the
    /// pool's own metadata) can consume the single fault.
    backend->fault_substr = store->layout().namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_log/";
    backend->mode = DB::Cas::tests::ChunkFaultBackend::Mode::Unresolved;
    backend->fault_count = 1;

    const String message = retryLaterMessageOf([&] { publishEmptyPart(store, ns, "part_a"); });

    EXPECT_TRUE(store->refLaneWedgedForTest(ns)) << "an Unresolved PUT must always leave a wedge";
    const String wedged_key = store->wedgedKeyForTest(ns);
    EXPECT_FALSE(wedged_key.empty()) << "the wedge must retain the in-doubt object's key";
    EXPECT_TRUE(wedged_key.starts_with(store->layout().namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_log/"))
        << "the wedged key must be this namespace's ref-log object, not some other key: " << wedged_key;
    EXPECT_EQ(store->tailSinceSnapshotCountForTest(ns), 0u)
        << "an UNPROVEN transaction must not be recorded as applied";

    /// Task 18. The contrast half of `PreAttemptRefusalDoesNotWedgeTheLane` below, asserted on the ONE
    /// artifact that carries the distinction: an attempt WAS sent here (the fault is thrown by the
    /// backend's `putIfAbsent`, so the request reached it), the single-attempt budget is then spent, and
    /// the lane wedges. The message must say so -- and must NOT say "no attempt was sent", which is the
    /// only shape allowed to skip the wedge.
    EXPECT_NE(message.find("attempt budget was exhausted"), String::npos)
        << "the reason must reach the wedge message rather than defaulting: " << message;
    EXPECT_EQ(message.find("no attempt was sent"), String::npos)
        << "an ambiguous PUT is not a pre-attempt refusal: " << message;
}

/// Task 18 (finding #37 defect 3, behavioural half). A `NoAttemptSent` `Unresolved` must NOT wedge.
///
/// The wedge exists because an ambiguous PUT MAY HAVE LANDED, so the durable log may or may not contain
/// the transaction and only an exact-key GET can settle it. That reasoning needs an attempt to have been
/// SENT. Here both pre-attempt gates reject on the FIRST iteration -- the remaining lease has no room
/// for one controlled attempt -- so nothing reaches the backend, the key is provably unwritten, and a
/// wedge would protect against nothing while costing the table every ref append (inserts included)
/// until a remount: an exact-key GET of a key that was never written reports `Unresolved` forever, so
/// such a wedge can never clear itself.
///
/// No fault injection anywhere in this test, deliberately: the ZERO ref-log I/O assertion below is the
/// direct proof that nothing was sent, and it would be meaningless if a fault backend were swallowing
/// the request.
TEST(CASRefInstallSafety, PreAttemptRefusalDoesNotWedgeTheLane)
{
    auto backend = std::make_shared<DB::Cas::tests::CountingBackend>();
    auto store = openPoolFenceControlled(backend);
    const RootNamespace ns{"srv1/pre_attempt_refusal"};

    publishEmptyPart(store, ns, "part_a");
    const size_t tail_after_seed = store->tailSinceSnapshotCountForTest(ns);
    const String log_prefix = store->layout().namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_log/";
    const uint64_t log_io_after_seed = backend->ioCountForKeysContaining(log_prefix);

    /// Shorten the lease to the window where the flush is admitted but no attempt may start.
    store->setMountDeadline(FENCE_DEADLINE_REFUSES_ATTEMPT_MS);
    /// Half of "the PRE-ATTEMPT gate is what refuses" is asserted here (the flush is admitted, so this
    /// is not the top-of-flush `mayMutate` gate); the other half is asserted below, by the message
    /// naming `NoAttemptSent` and by the ref-log I/O count not moving. `refAppendFenceOk` itself is
    /// private to `Pool`, and is not worth widening for a test that can prove the same thing from the
    /// outside.
    ASSERT_TRUE(store->mayMutate()) << "the flush must still be ADMITTED, or this exercises the "
                                       "top-of-flush gate instead of the pre-attempt one";

    const String message = retryLaterMessageOf([&] { store->dropRef(ns, "part_a"); });

    EXPECT_NE(message.find("no attempt was sent"), String::npos)
        << "the caller must be told WHY, and this is the reason the no-wedge decision rests on: " << message;
    EXPECT_FALSE(store->refLaneWedgedForTest(ns))
        << "nothing was sent, so nothing can be durable: there is no ambiguity for a wedge to resolve";
    EXPECT_TRUE(store->wedgedKeyForTest(ns).empty());
    EXPECT_EQ(backend->ioCountForKeysContaining(log_prefix), log_io_after_seed)
        << "the refusal must be PRE-attempt: not one ref-log object may have been touched";
    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Ready)
        << "no apply is owed for a transaction that was never sent -- leaving the marker pending would "
           "claim this table may be missing a durable transaction for the rest of its life";
    EXPECT_EQ(store->tailSinceSnapshotCountForTest(ns), tail_after_seed)
        << "nothing was committed, so nothing may be recorded";
    EXPECT_TRUE(store->resolveRef(ns, "part_a", /*allow_stale=*/false).has_value())
        << "the refused drop must not have taken effect";

    /// The availability half of the claim: the lane is usable the moment the lease is healthy again --
    /// no remount, no wedge resolution, nothing to clear. Before this task the same sequence left a
    /// wedge over a key that was never written, and this append would have failed forever.
    store->setMountDeadline(FENCE_DEADLINE_HEALTHY_MS);
    store->dropRef(ns, "part_a");
    EXPECT_FALSE(store->resolveRef(ns, "part_a", /*allow_stale=*/false).has_value())
        << "the retry on the same lane must commit";
    EXPECT_FALSE(store->refLaneWedgedForTest(ns));
    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Ready);
}

/// Task 18, the same pair on the WEDGE-RESOLUTION path -- the negative half.
///
/// One flush does both things: it resolves an outstanding wedge over a genuinely durable object (the
/// resolving GET proves it, so the transaction is installed and the lane unwedged), and then commits
/// its own new chunk, which the pre-attempt gate refuses. The lane must come out CLEAN.
///
/// This is the worst pre-fix shape and the reason the case is worth its own test: the flush had just
/// converted a resolvable wedge into a recorded transaction, and the old code immediately re-wedged the
/// lane over an id whose object was never written -- turning a wedge that WOULD have cleared into one
/// that never can.
TEST(CASRefInstallSafety, PreAttemptRefusalAfterAWedgeResolutionLeavesTheLaneClean)
{
    auto backend = std::make_shared<DB::Cas::tests::ChunkFaultBackend>();
    auto store = openPoolFenceControlled(backend);
    const RootNamespace ns{"srv1/pre_attempt_after_unwedge"};
    /// Stage B (Task 4-C): pin `ns` to the Stage-A sentinel BEFORE its first real touch, so
    /// the fault injected below (computed from that same sentinel) lands on the key production
    /// actually writes to.
    DB::Cas::tests::casAdmitRecoverableEntry(*backend, store->layout(), ns, store->liveWriterEpoch());

    publishEmptyPart(store, ns, "x");
    publishEmptyPart(store, ns, "y");
    const size_t tail_after_seed = store->tailSinceSnapshotCountForTest(ns);

    /// Wedge over an object that IS durable: the write lands, its acknowledgement is lost, and the
    /// controller's own verifying read is lost too (the only mode that reaches the resolution install).
    backend->fault_substr = store->layout().namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_log/";
    backend->mode = DB::Cas::tests::ChunkFaultBackend::Mode::LandedThenLost;
    backend->fault_count = 1;
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropRef(ns, "x"); });
    ASSERT_TRUE(store->refLaneWedgedForTest(ns));
    ASSERT_EQ(store->tailSinceSnapshotCountForTest(ns), tail_after_seed);

    backend->mode = DB::Cas::tests::ChunkFaultBackend::Mode::None;
    /// The wedge resolution is itself a conditional CREATE under the every-attempt rule, so it is
    /// fence-gated like any other write: shortening the lease BEFORE the flush would refuse the
    /// resolution too, and there would be no "after a wedge resolution" left to test. Shorten it
    /// BETWEEN the two instead -- the pre-carve hook fires exactly there, after the wedge block and
    /// before the batch is carved.
    store->setRefPreCarveHookForTest([&] { store->setMountDeadline(FENCE_DEADLINE_REFUSES_ATTEMPT_MS); });
    const String message = retryLaterMessageOf([&] { store->dropRef(ns, "y"); });
    store->setRefPreCarveHookForTest(nullptr);

    EXPECT_NE(message.find("no attempt was sent"), String::npos) << message;
    EXPECT_FALSE(store->refLaneWedgedForTest(ns))
        << "the wedge that existed was RESOLVED, and the chunk that followed it was never sent -- the "
           "lane must be left clean, not re-wedged over an id that can never resolve";
    EXPECT_EQ(store->tailSinceSnapshotCountForTest(ns), tail_after_seed + 1)
        << "the resolved wedge must still have been installed exactly once";
    EXPECT_FALSE(store->resolveRef(ns, "x", /*allow_stale=*/false).has_value())
        << "the wedged drop was proven durable, so its removal must be visible";
    EXPECT_TRUE(store->resolveRef(ns, "y", /*allow_stale=*/false).has_value())
        << "the refused chunk must not have taken effect";
    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Ready);

    store->setMountDeadline(FENCE_DEADLINE_HEALTHY_MS);
    store->dropRef(ns, "y");
    EXPECT_FALSE(store->resolveRef(ns, "y", /*allow_stale=*/false).has_value());
    EXPECT_FALSE(store->refLaneWedgedForTest(ns));
}

/// Task 18, the same pair on the wedge-resolution path -- the POSITIVE half, so the test above cannot
/// pass by the fix having weakened the wedge generally. Same flush shape (resolve a durable wedge, then
/// commit a new chunk), except the new chunk's PUT is genuinely ambiguous: an attempt WAS sent, so the
/// lane must wedge again, now over the NEW transaction.
TEST(CASRefInstallSafety, AmbiguousChunkAfterAWedgeResolutionRewedgesTheLane)
{
    auto backend = std::make_shared<DB::Cas::tests::ChunkFaultBackend>();
    auto store = openPoolFenceControlled(backend);
    const RootNamespace ns{"srv1/ambiguous_after_unwedge"};
    /// Stage B (Task 4-C): pin `ns` to the Stage-A sentinel BEFORE its first real touch, so
    /// the fault injected below (computed from that same sentinel) lands on the key production
    /// actually writes to.
    DB::Cas::tests::casAdmitRecoverableEntry(*backend, store->layout(), ns, store->liveWriterEpoch());

    publishEmptyPart(store, ns, "x");
    publishEmptyPart(store, ns, "y");
    const size_t tail_after_seed = store->tailSinceSnapshotCountForTest(ns);

    backend->fault_substr = store->layout().namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_log/";
    backend->mode = DB::Cas::tests::ChunkFaultBackend::Mode::LandedThenLost;
    backend->fault_count = 1;
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropRef(ns, "x"); });
    ASSERT_TRUE(store->refLaneWedgedForTest(ns));
    const String first_wedged_key = store->wedgedKeyForTest(ns);
    ASSERT_FALSE(first_wedged_key.empty());

    /// The resolution is a conditional CREATE at the wedged key now, and that key already holds our
    /// own landed object, so it conflicts and the follow-up read adopts it (`LandedThenLost`'s one-shot
    /// lost read was consumed inside the previous attempt, so this read succeeds). `fault_skip` lets
    /// that create through and puts the fault on this flush's OWN chunk PUT, which is the subject.
    backend->mode = DB::Cas::tests::ChunkFaultBackend::Mode::Unresolved;
    backend->fault_skip = 1;
    backend->fault_count = 1;
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropRef(ns, "y"); });

    EXPECT_TRUE(store->refLaneWedgedForTest(ns))
        << "an attempt was sent for the new chunk, so its object may be durable: the lane must wedge";
    EXPECT_NE(store->wedgedKeyForTest(ns), first_wedged_key)
        << "the new wedge must describe the NEW transaction, not the resolved one";
    EXPECT_EQ(store->tailSinceSnapshotCountForTest(ns), tail_after_seed + 1)
        << "only the resolved wedge is recorded; the in-doubt chunk is not";
    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Wedged)
        << "a wedged lane may hold a durable transaction the runtime has not recorded";
}

/// Task 18's regression guard, asserted on the mapping itself rather than through six pieces of fault
/// choreography. `unresolvedProvesNothingWasSent` is the whole decision: the ledger wedges unless it
/// answers true, so this table IS the protocol.
///
/// What protects a future contributor who adds a `CasUnresolvedReason` member and forgets this file:
/// the predicate is a switch with NO `default`, so the addition is a `-Wswitch` build error (a forced
/// decision, not a silent one), and its trailing `return false` makes the runtime answer "wedge" even
/// if that diagnostic is ever suppressed. Both directions fail closed; neither can widen the allow-list
/// by omission. The `static_assert`s make the mapping a compile-time fact, and the `EXPECT`s repeat it
/// so a break names the offending value in the test report.
TEST(CASRefInstallSafety, OnlyNoAttemptSentMaySkipTheWedge)
{
    static_assert(unresolvedProvesNothingWasSent(CasUnresolvedReason::NoAttemptSent));
    static_assert(!unresolvedProvesNothingWasSent(CasUnresolvedReason::NotUnresolved));
    static_assert(!unresolvedProvesNothingWasSent(CasUnresolvedReason::FenceLostMidWay));
    static_assert(!unresolvedProvesNothingWasSent(CasUnresolvedReason::DeadlineMidWay));
    static_assert(!unresolvedProvesNothingWasSent(CasUnresolvedReason::FenceLostPostWrite));
    static_assert(!unresolvedProvesNothingWasSent(CasUnresolvedReason::AttemptsExhausted));

    EXPECT_TRUE(unresolvedProvesNothingWasSent(CasUnresolvedReason::NoAttemptSent))
        << "the pre-attempt gates rejected before the first request: the key is provably unwritten";
    /// `NotUnresolved` is reachable at the decision site if any path ever returns `Unresolved` without
    /// recording a reason, so it is listed here as a real case, not as enum hygiene.
    EXPECT_FALSE(unresolvedProvesNothingWasSent(CasUnresolvedReason::NotUnresolved))
        << "an unrecorded reason proves nothing and must keep wedging";
    EXPECT_FALSE(unresolvedProvesNothingWasSent(CasUnresolvedReason::FenceLostMidWay))
        << "an attempt was already sent: its object may be durable";
    EXPECT_FALSE(unresolvedProvesNothingWasSent(CasUnresolvedReason::DeadlineMidWay))
        << "an attempt was already sent: its object may be durable";
    EXPECT_FALSE(unresolvedProvesNothingWasSent(CasUnresolvedReason::FenceLostPostWrite))
        << "the attempt COMMITTED and only the fence was lost afterwards -- the most durable case of all";
    EXPECT_FALSE(unresolvedProvesNothingWasSent(CasUnresolvedReason::AttemptsExhausted))
        << "every attempt is a candidate for having landed";
}

/// Task 5 (spec §A1, site 2). Resolving a wedge is a post-durable install too: the resolving GET PROVES
/// the object landed, so the transaction MUST be recorded -- and recording it must be inseparable from
/// clearing the wedge. It was not: the apply and the `materializeCommitted` fold sat between them
/// WITHOUT the ordinary commit arm's swallow, so a fold failure left the transaction applied and the
/// wedge still set, and the next resolution re-applied the same transaction and DOUBLE-bumped the tail
/// counters. The candidate is now built before the GET and installed by a `noexcept` swap that clears
/// the wedge in the same allocation-free region, with the fold outside it and swallowing.
///
/// Drives the real thing end to end (no seam beyond the `LandedThenLost` backend mode): a drop whose
/// object landed but whose acknowledgement -- and whose immediate verification read -- were both lost,
/// then a second append whose flush resolves it. The tail counter is the "exactly once" witness: it is
/// bumped once per install, so a re-applied transaction shows up as one extra.
TEST(CASRefInstallSafety, WedgeResolutionInstallsExactlyOnce)
{
    auto backend = std::make_shared<DB::Cas::tests::ChunkFaultBackend>();
    auto store = openPoolSingleAttempt(backend);
    const RootNamespace ns{"srv1/wedge_resolution"};
    /// Stage B (Task 4-C): pin `ns` to the Stage-A sentinel BEFORE its first real touch, so
    /// the fault injected below (computed from that same sentinel) lands on the key production
    /// actually writes to.
    DB::Cas::tests::casAdmitRecoverableEntry(*backend, store->layout(), ns, store->liveWriterEpoch());

    publishEmptyPart(store, ns, "x");
    publishEmptyPart(store, ns, "y");
    /// No snapshot publish can interfere and reset these: the thresholds are 256 logs / 1 MiB and this
    /// whole test is a handful of tiny transactions, so every delta below is exact.
    const size_t tail_after_seed = store->tailSinceSnapshotCountForTest(ns);

    /// Drop "x" through a PUT that LANDS and then loses its response, plus the one-shot lost read that
    /// keeps the controller's own resolve-before-reissue from settling it inside the same attempt. One
    /// attempt, so the lane wedges over an object that is genuinely durable -- the only way in.
    backend->fault_substr = store->layout().namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_log/";
    backend->mode = DB::Cas::tests::ChunkFaultBackend::Mode::LandedThenLost;
    backend->fault_count = 1;
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropRef(ns, "x"); });

    ASSERT_TRUE(store->refLaneWedgedForTest(ns)) << "the lost-response drop must wedge the lane";
    ASSERT_FALSE(store->wedgedKeyForTest(ns).empty());
    ASSERT_EQ(store->tailSinceSnapshotCountForTest(ns), tail_after_seed)
        << "the wedged transaction is durable but not yet PROVEN, so it must not be recorded yet";

    /// A second append into the same table: its flush resolves the wedge first (+1 install) and then
    /// commits its own transaction (+1). Nothing else can add a transaction in between.
    backend->mode = DB::Cas::tests::ChunkFaultBackend::Mode::None;
    store->dropRef(ns, "y");

    EXPECT_FALSE(store->refLaneWedgedForTest(ns)) << "a wedge proven durable must be cleared";
    EXPECT_EQ(store->tailSinceSnapshotCountForTest(ns), tail_after_seed + 2)
        << "the resolved transaction must be recorded EXACTLY once: +1 for it and +1 for the append that "
           "resolved it (a double-apply would show as +3)";
    /// Both drops took effect -- the wedged one via the resolution install, which is what proves that
    /// install happened at all rather than the wedge merely being discarded.
    EXPECT_FALSE(store->resolveRef(ns, "x", /*allow_stale=*/false).has_value())
        << "the wedged drop was proven durable, so its removal must be visible in the cached state";
    EXPECT_FALSE(store->resolveRef(ns, "y", /*allow_stale=*/false).has_value());
}

/// Negative control, part 1 of 2: does `DENY_ALLOCATIONS_IN_SCOPE` actually fire on an allocation in
/// THIS binary? Gated on `MEMORY_TRACKER_DEBUG_CHECKS`, because that is the macro the guard itself is
/// gated on (`MemoryTracker.h`: defined only under `!NDEBUG`, i.e. plain debug builds; everywhere else
/// `DENY_ALLOCATIONS_IN_SCOPE` compiles to `static_assert(true)` and there is nothing to observe).
/// An earlier version dispatched on `DEBUG_OR_SANITIZER_BUILD` instead — but sanitizer builds define
/// NDEBUG, so the guard is a no-op there and the death test "failed to die" on all three sanitizer CI
/// lanes. Note the implication chain: `MEMORY_TRACKER_DEBUG_CHECKS` ⇒ `!NDEBUG` ⇒
/// `DEBUG_OR_SANITIZER_BUILD`, so whenever the guard exists its `LOGICAL_ERROR` aborts at Exception
/// construction (`Exception.cpp`) — death is the only observable outcome, and a throw-only variant is
/// dead code.
#if defined(MEMORY_TRACKER_DEBUG_CHECKS)
TEST(CASRefInstallSafetyDeathTest, DenyGuardStopsAnAllocation)
{
    EXPECT_DEATH(
        {
            DENY_ALLOCATIONS_IN_SCOPE;
            volatile auto * p = new char[64];
            (void)p;
        },
        "");
}
#endif

/// Negative control, part 2 of 2: the region the guard protects is actually ENTERED, and the guard is
/// armed at that exact point. A probe that only reads flags proves both without allocating, so unlike
/// part 1 this assertion is immune to how a build type renders a `LOGICAL_ERROR`. Together the two
/// parts give what a single death test was meant to give, and a failure now names WHICH half broke:
/// "the guard does not fire" versus "the install region is never reached / not armed".
TEST(CASRefInstallSafety, InstallRegionProbeIsInvokedAndTheGuardIsArmed)
{
    auto backend = std::make_shared<DB::Cas::tests::CountingBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/install_probe_diag"};

    bool probe_ran = false;
    [[maybe_unused]] bool guard_armed_when_probe_ran = false;
    store->setInstallRegionProbeForTest([&]
    {
        probe_ran = true;
#if defined(MEMORY_TRACKER_DEBUG_CHECKS)
        guard_armed_when_probe_ran = memory_tracker_always_throw_logical_error_on_allocation;
#endif
    });

    publishEmptyPart(store, ns, "part_a");
    store->setInstallRegionProbeForTest(nullptr);

    EXPECT_TRUE(probe_ran) << "the install-region probe was never invoked";
#if defined(MEMORY_TRACKER_DEBUG_CHECKS)
    EXPECT_TRUE(guard_armed_when_probe_ran) << "the probe ran but DENY_ALLOCATIONS_IN_SCOPE was not armed";
#endif
}

/// ===================================================================================
/// The append lane state machine.
/// ===================================================================================

/// The exact attempt is visible as `Writing` after durability and before installation.
TEST(CASRefInstallSafety, WritingOwnsTheAttemptUntilInstall)
{
    auto backend = std::make_shared<DB::Cas::tests::CountingBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/apply_state_commit"};

    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Closed)
        << "a resident-only observer must not materialize a runtime for an untouched name";

    /// Fired on the calling thread by the flush leader, which is this thread; `atomic` regardless, as in
    /// `PostDurableInstallIsAllocationFree` above, so no assertion here reads as depending on that.
    std::atomic<size_t> observations{0};
    std::atomic<size_t> pending_observations{0};
    store->setCarveHookForTest([&](CasRefLedger::CarvePhaseForTest phase)
    {
        if (phase != CasRefLedger::CarvePhaseForTest::PostDurableInstall)
            return;
        observations.fetch_add(1);
        if (store->laneStateForTest(ns) == RefLaneState::Writing)
            pending_observations.fetch_add(1);
    });

    publishEmptyPart(store, ns, "part_a");
    store->setCarveHookForTest(nullptr);

    EXPECT_GT(observations.load(), 0u) << "the post-durable seam must be reached by an ordinary commit";
    EXPECT_EQ(pending_observations.load(), observations.load())
        << "every durable-but-not-yet-installed transaction remains Writing";
    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Ready)
        << "a completed install owes no apply: the marker must be back to Clean";
}

/// Ambiguity transfers the same exact attempt from `Writing` to `Wedged`.
TEST(CASRefInstallSafety, UnresolvedTransfersWritingToWedged)
{
    auto backend = std::make_shared<DB::Cas::tests::ChunkFaultBackend>();
    auto store = openPoolSingleAttempt(backend);
    const RootNamespace ns{"srv1/apply_state_wedge"};
    /// Stage B (Task 4-C): pin `ns` to the Stage-A sentinel BEFORE its first real touch, so
    /// the fault injected below (computed from that same sentinel) lands on the key production
    /// actually writes to.
    DB::Cas::tests::casAdmitRecoverableEntry(*backend, store->layout(), ns, store->liveWriterEpoch());

    backend->fault_substr = store->layout().namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_log/";
    backend->mode = DB::Cas::tests::ChunkFaultBackend::Mode::Unresolved;
    backend->fault_count = 1;

    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { publishEmptyPart(store, ns, "part_a"); });

    ASSERT_TRUE(store->refLaneWedgedForTest(ns));
    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Wedged)
        << "a wedged lane may hold a durable transaction the runtime has not recorded";
}

/// Durable resolution installs the attempt and returns the lane to `Ready`.
TEST(CASRefInstallSafety, WedgeResolutionReturnsReady)
{
    auto backend = std::make_shared<DB::Cas::tests::ChunkFaultBackend>();
    auto store = openPoolSingleAttempt(backend);
    const RootNamespace ns{"srv1/apply_state_unwedge"};
    /// Stage B (Task 4-C): pin `ns` to the Stage-A sentinel BEFORE its first real touch, so
    /// the fault injected below (computed from that same sentinel) lands on the key production
    /// actually writes to.
    DB::Cas::tests::casAdmitRecoverableEntry(*backend, store->layout(), ns, store->liveWriterEpoch());

    publishEmptyPart(store, ns, "x");
    publishEmptyPart(store, ns, "y");

    /// The one mode that wedges over a GENUINELY durable object (see `ChunkFaultBackend`): the write
    /// lands, its acknowledgement is lost, and the controller's own verifying read is lost too.
    backend->fault_substr = store->layout().namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_log/";
    backend->mode = DB::Cas::tests::ChunkFaultBackend::Mode::LandedThenLost;
    backend->fault_count = 1;
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropRef(ns, "x"); });
    ASSERT_TRUE(store->refLaneWedgedForTest(ns));
    ASSERT_EQ(store->laneStateForTest(ns), RefLaneState::Wedged);

    backend->mode = DB::Cas::tests::ChunkFaultBackend::Mode::None;
    store->dropRef(ns, "y");

    EXPECT_FALSE(store->refLaneWedgedForTest(ns));
    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Ready)
        << "the wedged transaction was proven durable AND installed, so nothing is owed any more";
}

/// A foreign occupant is a terminal `Faulted` verdict.
TEST(CASRefInstallSafety, ConclusiveForeignConflictFaultsTheLane)
{
    auto backend = std::make_shared<DB::Cas::tests::ChunkFaultBackend>();
    auto store = openPoolSingleAttempt(backend);
    const RootNamespace ns{"srv1/apply_state_conflict"};
    /// Stage B (Task 4-C): pin `ns` to the Stage-A sentinel BEFORE its first real touch, so
    /// the fault injected below (computed from that same sentinel) lands on the key production
    /// actually writes to.
    DB::Cas::tests::casAdmitRecoverableEntry(*backend, store->layout(), ns, store->liveWriterEpoch());

    backend->fault_substr = store->layout().namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_log/";
    backend->mode = DB::Cas::tests::ChunkFaultBackend::Mode::ForeignConflict;
    backend->fault_count = 1;

    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { publishEmptyPart(store, ns, "part_a"); });

    EXPECT_FALSE(store->refLaneWedgedForTest(ns)) << "a proven conflict is conclusive and must not wedge";
    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Faulted)
        << "a foreign occupant is a terminal protocol verdict, not a retryable attempt";
}

/// `DefiniteFailure` proves nothing became durable and returns the lane to `Ready`. Needs S3 error
/// classification: that is the only exception family
/// `classifyConditionalWriteResult` will ever call definite (everything else is fail-safe Unresolved).
TEST(CASRefInstallSafety, DefiniteFailureReturnsReady)
{
#if !USE_AWS_S3
    GTEST_SKIP() << "DefiniteFailure classification requires S3 error types (USE_AWS_S3 off)";
#else
    auto backend = std::make_shared<DB::Cas::tests::ChunkFaultBackend>();
    auto store = openPoolSingleAttempt(backend);
    const RootNamespace ns{"srv1/apply_state_definite"};
    /// Stage B (Task 4-C): pin `ns` to the Stage-A sentinel BEFORE its first real touch, so
    /// the fault injected below (computed from that same sentinel) lands on the key production
    /// actually writes to.
    DB::Cas::tests::casAdmitRecoverableEntry(*backend, store->layout(), ns, store->liveWriterEpoch());

    backend->fault_substr = store->layout().namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_log/";
    backend->mode = DB::Cas::tests::ChunkFaultBackend::Mode::Definite;
    backend->fault_count = 1;

    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { publishEmptyPart(store, ns, "part_a"); });

    EXPECT_FALSE(store->refLaneWedgedForTest(ns)) << "a definite failure is proven non-durable and must not wedge";
    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Ready)
        << "a definitively rejected PUT is proven non-durable, so no apply is owed";
#endif
}

/// A foreign occupant is the conclusive negative wedge resolution:
/// the wedged (write-once) key prove our body never landed there. `resolveByExactGet` never reports a
/// plain "absent" verdict -- absent or unreadable is `Unresolved`, since another attempt may still be
/// legal -- so this arm is the whole of "a resolution that proves the key is not ours".
///
/// Runs in every build: the arm reports `CORRUPTED_DATA` (storage-controlled input must never be able
/// to abort the server), where it used to raise the process-aborting `LOGICAL_ERROR` and this test had
/// to be release-only with a death-test twin standing in. The marker is cleared BEFORE the anomaly
/// reaction, which is what this test pins; the fence/audit half is
/// `CASAnomalyPolicy.ForeignBytesAtWedgeKeyTripFenceAndRemount`'s.
TEST(CASRefInstallSafety, WedgeResolutionProvenForeignFaultsTheLane)
{
    auto backend = std::make_shared<DB::Cas::tests::ChunkFaultBackend>();
    auto store = openPoolSingleAttempt(backend);
    const RootNamespace ns{"srv1/apply_state_foreign_wedge"};
    /// Stage B (Task 4-C): pin `ns` to the Stage-A sentinel BEFORE its first real touch, so
    /// the fault injected below (computed from that same sentinel) lands on the key production
    /// actually writes to.
    DB::Cas::tests::casAdmitRecoverableEntry(*backend, store->layout(), ns, store->liveWriterEpoch());

    publishEmptyPart(store, ns, "x");
    publishEmptyPart(store, ns, "y");

    backend->fault_substr = store->layout().namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_log/";
    backend->mode = DB::Cas::tests::ChunkFaultBackend::Mode::Unresolved;
    backend->fault_count = 1;
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropRef(ns, "x"); });
    ASSERT_EQ(store->laneStateForTest(ns), RefLaneState::Wedged);

    /// Out of band, a foreign writer lands DIFFERENT bytes at the exact wedged key. The fault mode is
    /// off first so this write is not itself intercepted.
    backend->mode = DB::Cas::tests::ChunkFaultBackend::Mode::None;
    const String wedged_key = store->wedgedKeyForTest(ns);
    ASSERT_FALSE(wedged_key.empty());
    ASSERT_EQ(backend->putIfAbsent(wedged_key, "a-different-object").outcome, PutOutcome::Done);

    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { store->dropRef(ns, "y"); });

    EXPECT_FALSE(store->refLaneWedgedForTest(ns));
    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Faulted)
        << "foreign interference is a terminal verdict, not an unresolved attempt";
}

/// `Writing -> NeedsRecovery` at the ordinary candidate install. Unreachable
/// in production with §A1 landed -- the region allocates nothing -- so the probe seam simulates the
/// post-durable failure. The transaction is durable at that point, but the runtime has not installed it:
/// exactly the condition that `NeedsRecovery` names.
TEST(CASRefInstallSafety, PostDurableInstallFailureRequiresRecovery)
{
    auto backend = std::make_shared<DB::Cas::tests::CountingBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/apply_state_poison"};

    publishEmptyPart(store, ns, "x");
    ASSERT_EQ(store->laneStateForTest(ns), RefLaneState::Ready);

    const uint64_t needs_recovery_before = ProfileEvents::global_counters[ProfileEvents::CASRefNeedsRecovery].load();
    armOneShotInstallFailure(store);
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED, [&] { store->dropRef(ns, "x"); });
    store->setInstallRegionProbeForTest(nullptr);

    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::NeedsRecovery)
        << "an install that failed AFTER its object was durable must be visible, not silent";
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASRefNeedsRecovery].load() - needs_recovery_before, 1u)
        << "the transition to NeedsRecovery must be exported exactly once";
}

/// `Wedged -> NeedsRecovery` at the wedge-resolution install. Same class of failure
/// one region over: the resolving GET already PROVED the object durable, so an install that does not
/// complete there leaves the same missing transaction -- and the wedge survives, because the swap that
/// would have cleared it is in the same region that threw.
TEST(CASRefInstallSafety, WedgeResolutionInstallFailureRequiresRecovery)
{
    auto backend = std::make_shared<DB::Cas::tests::ChunkFaultBackend>();
    auto store = openPoolSingleAttempt(backend);
    const RootNamespace ns{"srv1/apply_state_poison_unwedge"};
    /// Stage B (Task 4-C): pin `ns` to the Stage-A sentinel BEFORE its first real touch, so
    /// the fault injected below (computed from that same sentinel) lands on the key production
    /// actually writes to.
    DB::Cas::tests::casAdmitRecoverableEntry(*backend, store->layout(), ns, store->liveWriterEpoch());

    publishEmptyPart(store, ns, "x");
    publishEmptyPart(store, ns, "y");

    backend->fault_substr = store->layout().namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_log/";
    backend->mode = DB::Cas::tests::ChunkFaultBackend::Mode::LandedThenLost;
    backend->fault_count = 1;
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropRef(ns, "x"); });
    ASSERT_TRUE(store->refLaneWedgedForTest(ns));

    backend->mode = DB::Cas::tests::ChunkFaultBackend::Mode::None;
    armOneShotInstallFailure(store);
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED, [&] { store->dropRef(ns, "y"); });
    store->setInstallRegionProbeForTest(nullptr);

    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::NeedsRecovery);
    EXPECT_FALSE(store->refLaneWedgedForTest(ns))
        << "known durability transfers ownership from the attempt to recovery";
}

/// A later append may proceed only after the top-of-flush recovery has replayed the known-durable
/// transaction and returned the lane to `Ready`.
TEST(CASRefInstallSafety, NeedsRecoveryReplaysBeforeALaterFlush)
{
    auto backend = std::make_shared<DB::Cas::tests::CountingBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/apply_state_poison_terminal"};

    publishEmptyPart(store, ns, "x");
    publishEmptyPart(store, ns, "y");

    const uint64_t needs_recovery_before = ProfileEvents::global_counters[ProfileEvents::CASRefNeedsRecovery].load();
    armOneShotInstallFailure(store);
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED, [&] { store->dropRef(ns, "x"); });
    store->setInstallRegionProbeForTest(nullptr);
    ASSERT_EQ(store->laneStateForTest(ns), RefLaneState::NeedsRecovery);

    /// A perfectly ordinary, fully successful append afterwards.
    store->dropRef(ns, "y");
    EXPECT_FALSE(store->resolveRef(ns, "y", /*allow_stale=*/false).has_value())
        << "the later flush must really have committed AND installed -- otherwise the assertion below "
           "would pass for the wrong reason";

    /// A flush's own success is not evidence that the stranded transaction was installed. What returns
    /// the lane to `Ready` is the re-derivation that `ensureRefTableRecovered` performs at the top of
    /// that same flush: the walk reads the stranded transaction from the durable log, then installs the
    /// recovered state.
    ///
    /// Both halves are asserted, because only together do they mean the repair happened rather than the
    /// state being relabeled: `x` really is gone (the stranded drop is applied at last), and the lane is
    /// `Ready`.
    EXPECT_FALSE(store->resolveRef(ns, "x", /*allow_stale=*/false).has_value())
        << "the stranded drop of 'x' is durable, so the re-derivation must install it before returning "
           "the lane to Ready";
    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Ready);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASRefNeedsRecovery].load() - needs_recovery_before, 1u)
        << "the event counts transitions, so the successful flush must not have added another";
}

/// Part B review, BLOCKER 2: an UNCERTAIN `precommitAdd` must keep its cleanup owner and its body.
///
/// `PartWriteTxn::precommitAdd` used to record `precommit_*` and set its `precommitted` flag only AFTER
/// `appendRefOps` returned. But an `Unresolved` append MAY HAVE LANDED -- the `Unresolved` arm of
/// `commitRefChunk` says exactly that, and wedges the lane for precisely that reason -- so on that path
/// `abandon` ran against an object that believed it had never precommitted. It therefore queued NO
/// removal, and `cleanupStagedManifestDebrisBestEffort`, deciding from the same unset state, DELETED the
/// manifest body. When the wedge later resolved as committed, the table gained a live precommit with no
/// cleanup owner and no body -- which clamps GC's fold barrier (a live precommit whose body is missing)
/// forever.
///
/// The fix is the same discipline the wedge itself uses: record the intent BEFORE the ambiguous
/// operation. Both assertions below fail against the old code -- the body is gone, and the precommit is
/// still live once the wedge resolves.
TEST(CASRefInstallSafety, UncertainPrecommitKeepsItsCleanupOwnerAndItsBody)
{
    auto backend = std::make_shared<DB::Cas::tests::ChunkFaultBackend>();
    auto store = openPoolSingleAttempt(backend);
    const RootNamespace ns{"srv1/uncertain_precommit"};
    /// Stage B (Task 4-C): pin `ns` to the Stage-A sentinel BEFORE its first real touch, so
    /// the fault injected below (computed from that same sentinel) lands on the key production
    /// actually writes to.
    DB::Cas::tests::casAdmitRecoverableEntry(*backend, store->layout(), ns, store->liveWriterEpoch());

    PartWriteInfo info;
    info.intended_namespace = ns;
    info.intended_ref = ns.string() + "/part_a";
    auto build = store->beginPartWrite(info);
    const ManifestId id = build->stageManifest({});
    const String manifest_key = store->layout().manifestKey(id);
    ASSERT_TRUE(backend->head(manifest_key).exists) << "the staged body must exist before the precommit";

    /// Scoped to THIS namespace's ref log so the manifest body's own PUT cannot consume the fault. The
    /// object LANDS and only its acknowledgement is lost, which with the single-attempt budget wedges
    /// the lane over a genuinely durable precommit -- the exact shape the old code mishandled.
    backend->fault_substr = store->layout().namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_log/";
    backend->mode = DB::Cas::tests::ChunkFaultBackend::Mode::LandedThenLost;
    backend->fault_count = 1;
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { build->precommitAdd(ns, "part_a", id); });
    ASSERT_TRUE(store->refLaneWedgedForTest(ns)) << "the lost-response precommit must wedge the lane";
    EXPECT_EQ(build->precommitState(), PartWriteTxn::PrecommitState::Uncertain)
        << "an append that may have landed is neither 'never precommitted' nor 'durably precommitted'";

    /// The cleanup owner survives the uncertainty: this `abandon` resolves the wedge (proving the
    /// precommit durable) and appends the exact removal in the same flush.
    build->abandon();

    EXPECT_TRUE(backend->head(manifest_key).exists)
        << "abandon writer-deleted the body of a precommit that may be live -- GC's fold barrier would "
           "clamp on it forever";
    EXPECT_TRUE(store->livePrecommitsForTest(ns).empty())
        << "the uncertain precommit landed, so abandon owed its exact removal";
    EXPECT_FALSE(store->refLaneWedgedForTest(ns)) << "the abandon's own flush must have resolved the wedge";
}

/// The other side of the same state, and the reason it is a STATE and not just an extra bool: an
/// `Uncertain` precommit that in fact never landed must not make `abandon` fail forever.
///
/// `RefTableState::applyOwnerTransition` rejects a removal whose `old_binding` names an absent
/// precommit, so the removal is NOT unconditionally idempotent (the review's "it is idempotent" is only
/// true with the presence check this test pins). Here the append is refused BEFORE any request is sent,
/// which is provably-nothing-durable, and yet the transaction has already recorded the intent -- so the
/// removal it owes must resolve to a no-op rather than to `CORRUPTED_DATA`.
TEST(CASRefInstallSafety, UncertainPrecommitThatNeverLandedStillAbandonsCleanly)
{
    auto backend = std::make_shared<DB::Cas::tests::CountingBackend>();
    auto store = openPoolFenceControlled(backend);
    const RootNamespace ns{"srv1/uncertain_precommit_absent"};

    PartWriteInfo info;
    info.intended_namespace = ns;
    info.intended_ref = ns.string() + "/part_a";
    auto build = store->beginPartWrite(info);
    const ManifestId id = build->stageManifest({});

    /// A lease with room for the flush but not for one whole controlled attempt: the pre-attempt gate
    /// refuses, nothing is sent, and no wedge forms (`PreAttemptRefusalDoesNotWedgeTheLane`).
    store->setMountDeadline(FENCE_DEADLINE_REFUSES_ATTEMPT_MS);
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { build->precommitAdd(ns, "part_a", id); });
    ASSERT_FALSE(store->refLaneWedgedForTest(ns));
    EXPECT_EQ(build->precommitState(), PartWriteTxn::PrecommitState::Uncertain);

    store->setMountDeadline(FENCE_DEADLINE_HEALTHY_MS);
    build->abandon();   /// must not throw: there is no binding to remove, and that is not an anomaly here
    EXPECT_EQ(build->precommitState(), PartWriteTxn::PrecommitState::Settled);
    EXPECT_TRUE(store->livePrecommitsForTest(ns).empty());
}
