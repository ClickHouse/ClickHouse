#include <gtest/gtest.h>

#include "config.h"

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefLedger.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefProtocol.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequestControl.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPoolMetaFormat.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/ProfileEvents.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace ProfileEvents
{
extern const Event CASMountReleaseSkippedForeignOccupant;
extern const Event CASMountExclusivityViolation;
}

/// Stage A task 3 (INV-1): ref-log transaction ids are PER-NAMESPACE and CONTIGUOUS.
///
/// The id an append persists is not drawn from a counter at all -- it is DERIVED from the table's own
/// durable state: `{live_epoch, greatest_applied.ref_sequence + 1}` within one epoch, `{live_epoch, 1}`
/// at an epoch change. Two consequences this suite pins, both of which the pool-wide counter this
/// replaced made impossible:
///
///   1. namespaces are independent -- a busy table cannot push another table's ids up, so `(namespace,
///      epoch)` ids are dense `1..T` and a reader can tell "this stream is complete" from the ids alone;
///   2. an attempt that provably sent nothing consumes nothing -- the next caller re-derives the SAME
///      id, so a refusal leaves no hole behind it.
///
/// The read side enforces exactly what the allocator produces: `RefTableState::applyTxnInPlace` rejects
/// a non-successor id as `CORRUPTED_DATA`, so a hole can never become durable even if some future
/// writer path forgot the rule.
///
/// The suite name is prefixed `Cas` so it is covered by the `Cas*` unit-test gate filter.

namespace DB::ErrorCodes
{
extern const int CORRUPTED_DATA;
extern const int MEMORY_LIMIT_EXCEEDED;
extern const int UNKNOWN_FORMAT_VERSION;
}

using namespace DB::Cas;
using DB::Cas::tests::namespaceBirthOp;
using DB::Cas::tests::publishCommittedOps;

namespace
{

PoolPtr openPool(const BackendPtr & backend)
{
    DB::Cas::tests::seedPoolMetaForRestart(*backend);
    return Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
}

/// The fence-controlled pool of `gtest_cas_ref_install_safety.cpp`, for the pre-attempt refusal: the
/// boot clock is frozen so `setMountDeadline` alone decides both fence predicates, renewal is parked an
/// hour out so nothing re-arms the deadline underneath the test, and the single-attempt budget makes
/// `attempt_timeout_ms + lease_safety_margin_ms` (200 ms) the window between "the flush is admitted"
/// and "an attempt may start".
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

constexpr uint64_t FENCE_DEADLINE_HEALTHY_MS = 30000;
constexpr uint64_t FENCE_DEADLINE_REFUSES_ATTEMPT_MS = 100;

/// A bare `Pool::open` with no `_pool_meta` seeded: the path an operator's pool RECREATION takes, and
/// the only one that runs the bootstrap residual + quiesce gates (`seedPoolMetaForRestart` mints the
/// metadata directly and would bypass them).
PoolPtr openPoolWithoutSeeding(const BackendPtr & backend, const String & srid)
{
    return Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = srid});
}

/// Deletes every object whose key contains `substr` ("" = the whole prefix), as an operator clearing
/// the prefix would. Returns how many were removed.
size_t eraseKeysContaining(Backend & backend, const String & substr)
{
    size_t removed = 0;
    String cursor;
    std::vector<String> keys;
    while (true)
    {
        const ListPage page = backend.list("", cursor, 1000);
        for (const ListedKey & listed : page.keys)
            if (substr.empty() || listed.key.find(substr) != String::npos)
                keys.push_back(listed.key);
        if (page.next_cursor.empty())
            break;
        cursor = page.next_cursor;
    }
    for (const String & key : keys)
    {
        const HeadResult h = backend.head(key);
        if (h.exists && backend.deleteExact(key, h.token).kind == DeleteOutcome::Kind::Deleted)
            ++removed;
    }
    return removed;
}

String messageOfThrow(const std::function<void()> & fn)
{
    try
    {
        fn();
    }
    catch (const DB::Exception & e)
    {
        return e.message();
    }
    return {};
}

/// One ordinary publish transaction, driven straight through the append lane so the committed id is
/// observable: `namespace_birth` while the table is not yet `Live`, then the precommit+promote pair for
/// `ref`. Returns the id the append persisted under.
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

}

/// INV-1, first half: each namespace has its OWN stream. Two tables are written strictly alternately,
/// so a pool-wide counter would hand them 1,3,5 and 2,4 -- every id unique across the pool and dense
/// nowhere. Per-namespace derivation gives each table 1,2,3.. of its own.
TEST(CASRefContiguousAlloc, TwoNamespacesAllocateIndependently)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPool(backend);
    const uint64_t epoch = store->writerEpoch();
    const RootNamespace ns_a{"srv1/contig_ns_a"};
    const RootNamespace ns_b{"srv1/contig_ns_b"};

    const RefTxnId a1 = publishRef(store, ns_a, "ref_1", 1);
    const RefTxnId b1 = publishRef(store, ns_b, "ref_1", 1);
    const RefTxnId a2 = publishRef(store, ns_a, "ref_2", 2);
    const RefTxnId b2 = publishRef(store, ns_b, "ref_2", 2);
    const RefTxnId a3 = publishRef(store, ns_a, "ref_3", 3);

    EXPECT_EQ(a1, (RefTxnId{epoch, 1}));
    EXPECT_EQ(a2, (RefTxnId{epoch, 2}));
    EXPECT_EQ(a3, (RefTxnId{epoch, 3}))
        << "ns_a's third transaction must be its own third id -- the two ns_b transactions interleaved "
           "between them belong to a different stream and must not push it up";
    EXPECT_EQ(b1, (RefTxnId{epoch, 1}));
    EXPECT_EQ(b2, (RefTxnId{epoch, 2}));
}

/// INV-1, second half (the free half of the every-attempt rule): a refusal that PROVES nothing was sent
/// consumes no id. The pre-attempt gate refuses while the flush is still admitted -- no fault injection,
/// nothing reaches the backend -- and the very next append on that table commits under the SAME id the
/// refused one would have used. Under the pool-wide counter that id was burned as a "safe gap", which is
/// precisely what makes a durable stream unreadable as a contiguous chain.
TEST(CASRefContiguousAlloc, PreAttemptRefusalConsumesNoId)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolFenceControlled(backend);
    const uint64_t epoch = store->writerEpoch();
    const RootNamespace ns{"srv1/contig_no_gap"};

    ASSERT_EQ(publishRef(store, ns, "ref_1", 1), (RefTxnId{epoch, 1}));

    store->setMountDeadline(FENCE_DEADLINE_REFUSES_ATTEMPT_MS);
    ASSERT_TRUE(store->mayMutate()) << "the flush must still be ADMITTED, or this exercises the "
                                       "top-of-flush gate instead of the pre-attempt one";
    const String refusal = messageOfThrow([&] { publishRef(store, ns, "ref_2", 2); });
    ASSERT_NE(refusal, String()) << "the pre-attempt gate must refuse this append";
    /// Pin WHICH refusal this is. The id-reuse below is only meaningful for a refusal that proves
    /// nothing was sent; a different failure (an ambiguous PUT, say) would be free to have landed, and
    /// re-deriving its id would then be a collision rather than the no-gap property under test.
    EXPECT_NE(refusal.find("was refused BEFORE any request was sent"), String::npos)
        << "this test is about the provably-sent-nothing refusal specifically: " << refusal;
    EXPECT_NE(refusal.find("the txn id is not consumed"), String::npos) << refusal;
    ASSERT_FALSE(store->refLaneWedgedForTest(ns)) << "a refusal that sent nothing must not wedge";

    store->setMountDeadline(FENCE_DEADLINE_HEALTHY_MS);
    EXPECT_EQ(publishRef(store, ns, "ref_2", 2), (RefTxnId{epoch, 2}))
        << "the refused attempt sent nothing, so the next caller must re-derive the SAME id -- a refusal "
           "must never leave a hole in the durable stream";
}

/// The epoch component is the second half of the id, and the sequence is dense WITHIN an epoch: a new
/// mount incarnation restarts its table's sequence at 1 rather than continuing the dead incarnation's
/// numbering. `{E1, 2}` -> `{E2, 1}` is therefore not a gap, and the apply-side check must admit it.
TEST(CASRefContiguousAlloc, EpochChangeRestartsTheSequenceAtOne)
{
    auto backend = std::make_shared<InMemoryBackend>();
    const RootNamespace ns{"srv1/contig_epoch_reset"};

    uint64_t e1 = 0;
    {
        auto predecessor = openPool(backend);
        e1 = predecessor->writerEpoch();
        ASSERT_EQ(publishRef(predecessor, ns, "ref_1", 1), (RefTxnId{e1, 1}));
        ASSERT_EQ(publishRef(predecessor, ns, "ref_2", 2), (RefTxnId{e1, 2}));
    }   /// predecessor destroyed: its mount lease is released

    auto successor = openPool(backend);
    const uint64_t e2 = successor->writerEpoch();
    ASSERT_GT(e2, e1);
    EXPECT_EQ(publishRef(successor, ns, "ref_3", 3), (RefTxnId{e2, 1}))
        << "a fresh incarnation starts this table's sequence over at 1";
    EXPECT_EQ(publishRef(successor, ns, "ref_4", 4), (RefTxnId{e2, 2}));
}

/// The read side is what makes INV-1 an invariant rather than a convention: a transaction whose id is
/// not the successor of `greatest_applied` is CORRUPTED_DATA, naming both ids. Before this task the
/// state machine checked strict increase only, so a stream with a hole applied cleanly and no reader
/// could tell a complete chain from a truncated one.
TEST(CASRefContiguousAlloc, NonSuccessorIdIsRejectedOnApply)
{
    const String ns = "srv1/contig_density";
    constexpr uint64_t kEpoch = 7;

    RefTableState state = replay(DB::Cas::tests::minimalLiveSnapshot(ns, RefTxnId{kEpoch, 1}), {});
    ASSERT_EQ(state.getGreatestApplied(), (RefTxnId{kEpoch, 1}));

    /// Strictly greater, but skips {7,2}: admitted before this task, rejected now.
    try
    {
        applyRefLogTxn(state, RefLogTxn{ns, RefTxnId{kEpoch, 3}, publishCommittedOps("r", ManifestRef{1, 1, 1}), std::nullopt});
        FAIL() << "a non-successor id must be rejected";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::CORRUPTED_DATA);
        EXPECT_NE(e.message().find("7-3"), String::npos) << "the offending id must be named: " << e.message();
        EXPECT_NE(e.message().find("7-1"), String::npos) << "the greatest applied id must be named: " << e.message();
    }
    EXPECT_EQ(state.getGreatestApplied(), (RefTxnId{kEpoch, 1})) << "the rejected apply must change nothing";

    /// A new epoch must ALSO start at 1: continuing the previous epoch's numbering is a hole in the new
    /// epoch's stream, which reads exactly like a lost first transaction.
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&]
    {
        applyRefLogTxn(state, RefLogTxn{ns, RefTxnId{kEpoch + 1, 2}, publishCommittedOps("r", ManifestRef{1, 1, 1}), std::nullopt});
    });

    /// The two shapes the allocator can produce are the two the checker admits -- and `nextRefTxnId` is
    /// the single rule both sides use, so they cannot drift apart.
    EXPECT_EQ(nextRefTxnId(state.getGreatestApplied(), kEpoch), (RefTxnId{kEpoch, 2}));
    EXPECT_NO_THROW(applyRefLogTxn(state, RefLogTxn{ns, nextRefTxnId(state.getGreatestApplied(), kEpoch),
        publishCommittedOps("r", ManifestRef{1, 1, 1}), std::nullopt}));
    ASSERT_EQ(state.getGreatestApplied(), (RefTxnId{kEpoch, 2}));

    EXPECT_EQ(nextRefTxnId(state.getGreatestApplied(), kEpoch + 1), (RefTxnId{kEpoch + 1, 1}));
    /// The id is admissible, but a Live table crossing into a new epoch also owes INV-2's chain link --
    /// the seal that closed the epoch below, at the slot one past its last durable id.
    EXPECT_NO_THROW(applyRefLogTxn(state, RefLogTxn{ns, nextRefTxnId(state.getGreatestApplied(), kEpoch + 1),
        publishCommittedOps("r2", ManifestRef{1, 2, 1}), RefTxnId{kEpoch, 3}}));
    EXPECT_EQ(state.getGreatestApplied(), (RefTxnId{kEpoch + 1, 1}));
}

/// The format floor. A pool written before contiguous ref streams holds ref logs whose ids this build
/// would read as a corrupt (holed) chain, so opening it must fail closed at the pool metadata, naming
/// recreation as the migration -- CAS is pre-release and has no in-place migration path.
TEST(CASRefContiguousAlloc, OldPoolFormatIsRefusedNamingRecreation)
{
    PoolMeta pm;
    pm.pool_id = UInt128{1, 2};
    pm.blob_header_len = 256;
    pm.min_reader_generation = G_BUILD;
    pm.algos_used = {static_cast<uint8_t>(BlobHashAlgo::CityHash128)};

    const String current = encodePoolMeta(pm);
    EXPECT_NO_THROW(decodePoolMeta(current));

    /// Rewrite the header-line generation to the last pre-contiguous one, exactly as an older build
    /// would have stamped it.
    const String from = "\"v\":" + std::to_string(G_BUILD);
    const String to = "\"v\":" + std::to_string(kContiguousRefStreamsGeneration - 1);
    const size_t at = current.find(from);
    ASSERT_NE(at, String::npos);
    String old_format = current;
    old_format.replace(at, from.size(), to);

    try
    {
        decodePoolMeta(old_format);
        FAIL() << "a pre-contiguous pool must not open";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::UNKNOWN_FORMAT_VERSION);
        EXPECT_NE(e.message().find(fmt::format("CAS pool format {} predates generation-9 exact _ckpt committed_through recovery frontier",
                                               kContiguousRefStreamsGeneration - 1)), String::npos)
            << "the message must name the migration: " << e.message();
    }
}

/// Generation 6 is a recreate-only physical-layout cut. A generation-5 pool has contiguous,
/// incarnation-qualified streams but still repeats the logical namespace in every key; accepting it
/// would silently run the generation-6 parsers over a different grammar.
TEST(CASRefContiguousAlloc, GenerationFiveNamespaceBearingPoolIsRefusedNamingRecreation)
{
    PoolMeta pm;
    pm.pool_id = UInt128{1, 2};
    pm.blob_header_len = 256;
    pm.min_reader_generation = G_BUILD;
    pm.algos_used = {static_cast<uint8_t>(BlobHashAlgo::CityHash128)};

    const String current = encodePoolMeta(pm);
    EXPECT_NO_THROW(decodePoolMeta(current));

    /// Rewrite the header to the immediately preceding generation, which used
    /// `cas/refs/<namespace>/<incarnation>/...`.
    const String from = "\"v\":" + std::to_string(G_BUILD);
    const String to = "\"v\":" + std::to_string(kNamespaceLifeKeyedGeneration);
    const size_t at = current.find(from);
    ASSERT_NE(at, String::npos);
    String old_format = current;
    old_format.replace(at, from.size(), to);
    ASSERT_EQ(kNamespaceLifeKeyedGeneration + 1, kOpaqueNamespaceLifeLayoutGeneration)
        << "this test pins the immediately preceding namespace-bearing generation";

    try
    {
        decodePoolMeta(old_format);
        FAIL() << "a generation-5 namespace-bearing pool must not open";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::UNKNOWN_FORMAT_VERSION);
        EXPECT_NE(e.message().find(fmt::format("CAS pool format {} predates generation-9 exact _ckpt committed_through recovery frontier",
                                               kNamespaceLifeKeyedGeneration)), String::npos)
            << "the message must name the migration: " << e.message();
    }
}

/// Mutation caught: leaving the pool floor at generation 6 would admit a seal whose independent
/// name-keyed coverage and cleanup collections this build no longer has. Generation 7 is a
/// recreate-only grammar cut, so the immediately preceding generation must fail at pool open.
TEST(CASRefContiguousAlloc, GenerationSixSplitFoldSealPoolIsRefusedNamingRecreation)
{
    PoolMeta pm;
    pm.pool_id = UInt128{1, 2};
    pm.blob_header_len = 256;
    pm.min_reader_generation = G_BUILD;
    pm.algos_used = {static_cast<uint8_t>(BlobHashAlgo::CityHash128)};

    const String current = encodePoolMeta(pm);
    const String from = "\"v\":" + std::to_string(G_BUILD);
    const String to = "\"v\":6";
    const size_t at = current.find(from);
    ASSERT_NE(at, String::npos);
    String old_format = current;
    old_format.replace(at, from.size(), to);

    try
    {
        decodePoolMeta(old_format);
        FAIL() << "a generation-6 split ref-life fold seal pool must not open";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::UNKNOWN_FORMAT_VERSION);
        EXPECT_NE(e.message().find("CAS pool format 6 predates generation-9 exact _ckpt committed_through recovery frontier"), String::npos)
            << "the message must name the recreate-only grammar cut: " << e.message();
    }
}

TEST(CASPoolMeta, GcShardsIsPersistedAndOverridesMismatchedReopenConfig)
{
    InMemoryBackend backend;
    const Layout layout("p");
    const PoolMeta created = PoolMeta::createOrValidate(
        backend, layout, /*blob_header_len=*/256, /*gc_shards=*/4,
        BlobHashAlgo::CityHash128, /*allow_new=*/false, /*allow_mint=*/true);
    EXPECT_EQ(created.gc_shards, 4u);

    const PoolMeta reopened = PoolMeta::createOrValidate(
        backend, layout, /*blob_header_len=*/256, /*gc_shards=*/1,
        BlobHashAlgo::CityHash128, /*allow_new=*/false, /*allow_mint=*/false);
    EXPECT_EQ(reopened.gc_shards, 4u);
    EXPECT_EQ(decodePoolMeta(backend.get(layout.poolMetaKey())->bytes).gc_shards, 4u);
}

/// The one path where "an attempt that provably sent nothing consumes nothing" does not hold, and the
/// A known-durable install failure must replay before the next id is derived. Replay installs the
/// stranded transaction, so the next append derives its real contiguous successor.
TEST(CASRefContiguousAlloc, NeedsRecoveryReplaysBeforeAllocatingTheNextId)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPool(backend);
    const uint64_t epoch = store->writerEpoch();
    const RootNamespace ns{"srv1/contig_durable_floor"};

    ASSERT_EQ(publishRef(store, ns, "ref_1", 1), (RefTxnId{epoch, 1}));

    /// One-shot throw inside the post-durable install region: txn {epoch, 2} commits durably and is
    /// never installed. The exception is built OUTSIDE the region (building it inside would trip
    /// `DENY_ALLOCATIONS_IN_SCOPE` and test the guard instead of the recovery transition).
    auto planned = std::make_exception_ptr(DB::Exception(DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED,
        "simulated allocation failure inside the post-durable install region"));
    auto fired = std::make_shared<std::atomic<bool>>(false);
    store->setInstallRegionProbeForTest([planned, fired]
    {
        if (fired->exchange(true))
            return;
        ALLOW_ALLOCATIONS_IN_SCOPE;
        std::rethrow_exception(planned);
    });
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED,
        [&] { publishRef(store, ns, "ref_2", 2); });
    store->setInstallRegionProbeForTest(nullptr);
    ASSERT_EQ(store->laneStateForTest(ns), RefLaneState::NeedsRecovery);

    /// The next append first recovers `{epoch, 2}`, then lands at `{epoch, 3}`.
    EXPECT_EQ(publishRef(store, ns, "ref_3", 3), (RefTxnId{epoch, 3}))
        << "the stranded transaction is durable, so the next id must be its successor, not itself";
    EXPECT_TRUE(store->resolveRef(ns, "ref_3", /*allow_stale=*/false).has_value())
        << "the append may proceed only after recovery has repaired the cached state";

    EXPECT_TRUE(store->resolveRef(ns, "ref_2", /*allow_stale=*/false).has_value())
        << "the stranded transaction is back in this cache, which is what repairs the divergence";
    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Ready);

    /// The durable stream itself is dense: `1`, `2`, `3` all exist as objects. `ns` was born through
    /// the REAL append lane (Stage B Task 4-C), so its objects sit at a real catalog-minted incarnation,
    /// not the Stage-A sentinel -- resolve it the same way production discovery does.
    const NamespaceLifeId life = CasRefCatalog::lifeIfCataloged(*backend, store->layout(), ns).value();
    for (uint64_t seq = 1; seq <= 3; ++seq)
        EXPECT_TRUE(backend->head(store->layout().refLogKey(life, RefTxnId{epoch, seq})).exists)
            << "log object " << epoch << "-" << seq << " must exist: the durable stream has no hole";
}

/// Snapshot publication also recovers a `NeedsRecovery` lane before it captures state.
TEST(CASRefContiguousAlloc, NeedsRecoveryReplaysBeforeSnapshotPublication)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPool(backend);
    const uint64_t epoch = store->writerEpoch();
    const RootNamespace ns{"srv1/contig_poison_publish"};

    ASSERT_EQ(publishRef(store, ns, "ref_1", 1), (RefTxnId{epoch, 1}));
    ASSERT_TRUE(store->tryPublishSnapshotAndAdvanceCheckpointOnce(ns)) << "a healthy table must publish, or the refusal "
                                                     "asserted below would prove nothing";
    const auto published_before = store->newestPublishedSnapshotIdForTest(ns);
    ASSERT_TRUE(published_before.has_value());

    auto planned = std::make_exception_ptr(DB::Exception(DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED,
        "simulated allocation failure inside the post-durable install region"));
    auto fired = std::make_shared<std::atomic<bool>>(false);
    store->setInstallRegionProbeForTest([planned, fired]
    {
        if (fired->exchange(true))
            return;
        ALLOW_ALLOCATIONS_IN_SCOPE;
        std::rethrow_exception(planned);
    });
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED,
        [&] { publishRef(store, ns, "ref_2", 2); });
    store->setInstallRegionProbeForTest(nullptr);
    ASSERT_EQ(store->laneStateForTest(ns), RefLaneState::NeedsRecovery);

    /// The append entry point replays before admitting this transaction.
    EXPECT_EQ(publishRef(store, ns, "ref_3", 3), (RefTxnId{epoch, 3}));

    /// Publication is safe because recovery installed the stranded transaction first.
    EXPECT_TRUE(store->tryPublishSnapshotAndAdvanceCheckpointOnce(ns));
    EXPECT_TRUE(store->resolveRef(ns, "ref_2", /*allow_stale=*/false).has_value())
        << "the stranded transaction is durable and was re-derived -- publishing is safe precisely "
           "because there is nothing left to omit";
    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Ready);
    EXPECT_NE(store->newestPublishedSnapshotIdForTest(ns), published_before);
}

/// Recreation quiesce, refusal leg. Refusing to OPEN an old-format pool fences nothing: the server that
/// mounted it before the operator acted is still running, still holds its mount lease, and still has
/// queued writes. If "recreate the pool" is followed literally -- clear the prefix, start fresh -- that
/// writer's next flush lands its old-format transactions inside the NEW pool. So a recreation over a
/// prefix whose mount slots are not terminal must fail closed, and must say why, BEFORE the operator
/// clears anything.
TEST(CASRefContiguousAlloc, RecreationRefusedWhileAMountSlotIsStillHeld)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto holder = openPool(backend);
    const RootNamespace ns{"srv1/contig_quiesce"};
    ASSERT_EQ(publishRef(holder, ns, "ref_1", 1), (RefTxnId{holder->writerEpoch(), 1}));

    /// The operator removes the pool identity, intending to recreate -- but the holder is still up.
    ASSERT_EQ(eraseKeysContaining(*backend, "_pool_meta"), 1u);

    const String message = messageOfThrow([&] { openPoolWithoutSeeding(backend, "test2"); });
    EXPECT_NE(message.find("mount lease(s) under this prefix are still held"), String::npos)
        << "the refusal must name the held lease, not merely the residual data: " << message;
    EXPECT_NE(message.find("do NOT clear the prefix first"), String::npos)
        << "the remedy ordering is the whole point of this gate: " << message;
    EXPECT_NE(message.find("server root 'test'"), String::npos)
        << "the holder must be identified so the operator knows what to stop: " << message;

    /// And the holder is untouched by the refused recreation: its own stream continues contiguously.
    EXPECT_EQ(publishRef(holder, ns, "ref_2", 2), (RefTxnId{holder->writerEpoch(), 2}));
}

/// Recreation quiesce, acceptance leg. Once the holder is gone its slot carries the graceful-farewell
/// marker -- one of the two clock-free certificates of death the mount protocol already recognises --
/// so the quiesce gate stops firing and the ordinary bootstrap rules take over: clear the prefix, and
/// the recreation mints a fresh pool.
TEST(CASRefContiguousAlloc, RecreationProceedsOnceTheHolderIsTerminal)
{
    auto backend = std::make_shared<InMemoryBackend>();
    const RootNamespace ns{"srv1/contig_quiesce_ok"};
    {
        auto holder = openPool(backend);
        publishRef(holder, ns, "ref_1", 1);
    }   /// destroyed: the keeper stamps the farewell, making the slot terminal
    ASSERT_EQ(eraseKeysContaining(*backend, "_pool_meta"), 1u);

    /// The prefix still holds this pool's data, so the bootstrap still refuses -- but on the ORDINARY
    /// residual rule, not the quiesce gate. That difference is the whole assertion: nothing is being
    /// held any more.
    const String residual = messageOfThrow([&] { openPoolWithoutSeeding(backend, "test2"); });
    EXPECT_EQ(residual.find("still held"), String::npos)
        << "a terminal slot must not block recreation: " << residual;
    EXPECT_NE(residual.find("refusing to bootstrap over residual data"), String::npos) << residual;

    /// The operator now clears the prefix -- in the order the refusal prescribed -- and the recreation
    /// mints a fresh pool that starts its own ref stream at 1.
    ASSERT_GT(eraseKeysContaining(*backend, ""), 0u);
    auto recreated = openPoolWithoutSeeding(backend, "test");
    EXPECT_EQ(publishRef(recreated, ns, "ref_1", 1), (RefTxnId{recreated->writerEpoch(), 1}));
}

/// The other half of the rule: if the prefix IS cleared while a writer survives (the mistake the
/// refusal above exists to prevent, or a writer that was already mid-flight), the recreated pool's
/// ordinary mount claim is what stops it. The survivor's next lease renewal finds a slot it can no
/// longer hold, its local fence latches shut, and every later write is refused -- so a straggler can
/// never append into the new pool.
///
/// The recreating mount here is a DIFFERENT server (its own `server_id`), which is what makes the
/// survivor's renewal conclusive. Clearing the prefix also resets the durable writer-epoch counter, so
/// a recreation by the SAME server uuid can be handed the very same `(uuid, epoch)` the survivor still
/// holds -- and the two are then indistinguishable to the lease protocol, which reads the survivor's
/// renewal as its own keeper adopting a refreshed body. That is precisely why the refusal above is the
/// primary defence and this fence is only the backstop: quiescing the holder BEFORE the prefix is
/// cleared is what keeps the ambiguous case from arising at all.
TEST(CASRefContiguousAlloc, SurvivingWriterIsFencedByTheRecreatedPoolsMount)
{
    auto backend = std::make_shared<InMemoryBackend>();
    /// The survivor renews on its own thread, as a real mount does: the renewal loop is what latches the
    /// write fence when a renewal fails, so a hand-driven `renewWatermarkOnce` would reproduce only the
    /// failure and not the fencing it causes.
    PoolConfig survivor_cfg{.pool_prefix = "p", .server_root_id = "test"};
    survivor_cfg.background_watermark = true;
    survivor_cfg.mount_renew_period = std::chrono::milliseconds{50};
    DB::Cas::tests::seedPoolMetaForRestart(*backend);
    auto survivor = Pool::open(backend, survivor_cfg);
    const RootNamespace ns{"srv1/contig_survivor"};
    ASSERT_EQ(publishRef(survivor, ns, "ref_1", 1), (RefTxnId{survivor->writerEpoch(), 1}));

    /// The prefix is cleared and the pool recreated underneath the still-running survivor.
    ASSERT_GT(eraseKeysContaining(*backend, ""), 0u);
    PoolConfig recreated_cfg{.pool_prefix = "p", .server_root_id = "test"};
    recreated_cfg.server_id = UInt128{7, 7};
    auto recreated = Pool::open(backend, recreated_cfg);
    ASSERT_TRUE(recreated->mayMutate());

    /// The survivor's next renewal finds a slot held by a foreign server and fails closed, and the loop
    /// latches the local write fence. Bounded wait: a real hang fails the test instead of stalling it.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (survivor->mayMutate() && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_FALSE(survivor->mayMutate())
        << "a survivor whose slot was reclaimed must be fenced closed by its own failing renewal, not "
           "left writing into the new pool";
    EXPECT_NE(messageOfThrow([&] { publishRef(survivor, ns, "ref_2", 2); }), String())
        << "the survivor's queued write must be refused";

    /// The recreated pool is unaffected and owns the stream from 1.
    EXPECT_EQ(publishRef(recreated, ns, "ref_1", 1), (RefTxnId{recreated->writerEpoch(), 1}));

    /// The survivor's TEARDOWN is the other half, and it is asserted here rather than left to the
    /// destructor at scope exit, because which arm of the release path it takes is exactly what a
    /// regressed discriminator would get wrong — silently. A deposed writer meeting its successor in the
    /// slot is the EXPECTED end of a failover (arm A: skip the farewell, leave the successor's slot
    /// untouched); a writer that still believed it owned the mount meeting a stranger is
    /// single-writer exclusivity BROKEN (arm B, must-always-be-zero). If `deposition_observed` ever stops
    /// being set, every ordinary failover in production starts reporting itself as a broken guarantee —
    /// and without the +0 assertion below, not one test would notice.
    const String survivor_mount_key = recreated->layout().mountKey("test");
    const auto successor_slot_before = backend->get(survivor_mount_key);
    ASSERT_TRUE(successor_slot_before.has_value());
    const uint64_t skipped_before
        = ProfileEvents::global_counters[ProfileEvents::CASMountReleaseSkippedForeignOccupant].load();
    const uint64_t violations_before
        = ProfileEvents::global_counters[ProfileEvents::CASMountExclusivityViolation].load();

    survivor.reset();

    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASMountReleaseSkippedForeignOccupant].load(),
              skipped_before + 1)
        << "a deposed writer's release must take the skip-the-farewell arm";
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASMountExclusivityViolation].load(),
              violations_before)
        << "and must NOT report an exclusivity violation: this is a failover, not a broken guarantee";
    const auto successor_slot_after = backend->get(survivor_mount_key);
    ASSERT_TRUE(successor_slot_after.has_value());
    EXPECT_EQ(successor_slot_after->bytes, successor_slot_before->bytes)
        << "the deposed writer must not stamp its farewell over the successor's lease";
    EXPECT_TRUE(recreated->mayMutate()) << "and must not disturb the live successor";
}
