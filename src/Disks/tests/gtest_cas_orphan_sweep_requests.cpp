#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasServerRootFormats.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasOrphanManifestSweep.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/ThreadPool.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>

#include <limits>

namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

namespace ProfileEvents
{
    extern const Event CASGCReadAheadWasted;
    extern const Event CASGCReadAheadHit;
}

/// The orphan-manifest sweep's request shape per page. The floor a namespace's builds are judged
/// against is one mount body per server root, so a page reads it once per namespace, not once per
/// listed build; the tests below count the mount-key reads and pin the pure eligibility predicate.

using namespace DB::Cas;
using namespace DB::Cas::tests;

namespace
{

/// A three-segment namespace, the shape a real table gets (`<root>/store/<3hex>/<uuid>@cas@`), so the
/// floor lookup has three `/`-prefixes to try and two of them miss.
const RootNamespace kNs{"test/store/465/aa@cas@"};

ManifestRef build(uint64_t seq)
{
    return ManifestRef{.writer_epoch = 1, .build_sequence = seq, .manifest_ordinal = 1};
}

struct PageFixture
{
    std::shared_ptr<CountingBackend> backend = std::make_shared<CountingBackend>();
    PoolPtr store;
    uint64_t manifests = 0;

    explicit PageFixture(uint64_t manifests_, uint64_t min_active_build_sequence)
        : manifests(manifests_)
    {
        PoolConfig config;
        config.pool_prefix = "p";
        config.server_root_id = "test";
        config.manifest_sweep_list_budget_keys = 1000;
        config.manifest_sweep_delete_budget_keys = 100;
        config.gc_fold_max_defer_rounds = 0;
        store = Pool::open(backend, config);
        const Layout & layout = store->layout();
        casAdmitEntry(*backend, layout, kNs);
        /// One committed birth log at {1,1} and a checkpoint naming it, so the namespace has a protection
        /// view and every eligible key reaches the premise (which retains it for lack of fold coverage).
        /// The live ref's own manifest occupies build_sequence == manifests (it is itself one of the
        /// `manifests` listed objects, always active); the loop below fills the debris below it.
        publishAt(*backend, layout, kNs, RefTxnId{1, 1}, "live", /*build_sequence=*/manifests,
                  DB::UInt128(0x7001), /*birth=*/true);
        writeRecoverableCkptForRawFixture(*backend, layout, kNs, RefCkpt{
            .life_epoch = 1,
            .committed_through = RefTxnId{1, 1},
            .checkpoint_snapshot_id = std::nullopt,
            .last_epoch_seal = std::nullopt,
        });
        for (uint64_t seq = 1; seq < manifests; ++seq)
            writeManifestRaw(*backend, layout, kNs, build(seq), {blobEntryFor("a", DB::UInt128(0x100 + seq))});
        setWatermarkMinActive(*backend, layout, "test", /*writer_epoch=*/1, min_active_build_sequence);
        backend->resetCounts();
    }

    ManifestSweepResult page()
    {
        return planManifestCursorPage(*store, "", /*list_budget=*/1000, /*nomination_budget=*/100,
                                      /*catalog_recovery_authoritative=*/true, nullptr);
    }
};

}

TEST(CASOrphanSweepRequests, FloorIsReadOncePerNamespacePerPage)
{
    PageFixture f(/*manifests=*/50, /*min_active=*/25);
    const Layout & layout = f.store->layout();
    const ManifestSweepResult result = f.page();

    EXPECT_EQ(result.listed, 50u);
    EXPECT_EQ(result.floor_lookups, 1u);
    EXPECT_EQ(result.floor_reads, 3u);
    EXPECT_EQ(f.backend->getCount(layout.mountKey("test/store/465")), 1u);
    EXPECT_EQ(f.backend->getCount(layout.mountKey("test/store")), 1u);
    EXPECT_EQ(f.backend->getCount(layout.mountKey("test")), 1u);
    /// Builds 1..24 are retired under the floor and reach the premise, which retains them for lack of
    /// coverage; builds 25..50 are active and never get that far.
    EXPECT_EQ(result.retained_no_coverage, 24u);
    EXPECT_TRUE(result.nominations.empty());
}

TEST(CASOrphanSweepRequests, AbsentFloorRetainsEverythingWithOneLookup)
{
    PageFixture f(/*manifests=*/10, /*min_active=*/100);
    const Layout & layout = f.store->layout();
    {
        OperationForTest op(*f.backend);
        const auto h = (*op).head(layout.mountKey("test"), Retry::once());
        ASSERT_TRUE(h.has_value());
        ASSERT_EQ((*op).remove(layout.mountKey("test"), h->etag, Retry::once()), Removal::Removed);
    }
    f.backend->resetCounts();
    const ManifestSweepResult result = f.page();

    EXPECT_EQ(result.floor_lookups, 1u);
    EXPECT_EQ(result.floor_reads, 3u);
    EXPECT_EQ(result.listed, 10u);
    EXPECT_EQ(result.skipped, 10u);
    EXPECT_EQ(result.retained_no_coverage, 0u) << "an absent floor admits nothing, so no key reaches the premise";
}

TEST(CASOrphanSweepRequests, RetainedKeysCostNoBodyRead)
{
    PageFixture f(/*manifests=*/50, /*min_active=*/25);
    const Layout & layout = f.store->layout();
    const ManifestSweepResult result = f.page();
    EXPECT_EQ(result.retained_no_coverage, 24u);
    for (uint64_t seq = 1; seq <= 50; ++seq)
        EXPECT_EQ(f.backend->getCount(layout.manifestKey(ManifestId{kNs, build(seq)})), 0u) << seq;
}

TEST(CASOrphanSweepRequests, PrefixEligibleUnderIsTheFourComparisons)
{
    MountLease floor;
    floor.writer_epoch = 3;
    floor.min_active_build_sequence = 10;
    EXPECT_TRUE(prefixEligibleUnder(floor, BuildPrefix{.writer_epoch = 2, .build_sequence = 999}));
    EXPECT_FALSE(prefixEligibleUnder(floor, BuildPrefix{.writer_epoch = 4, .build_sequence = 1}));
    EXPECT_TRUE(prefixEligibleUnder(floor, BuildPrefix{.writer_epoch = 3, .build_sequence = 9}));
    EXPECT_FALSE(prefixEligibleUnder(floor, BuildPrefix{.writer_epoch = 3, .build_sequence = 10}));
    floor.min_active_build_sequence = std::numeric_limits<uint64_t>::max();
    EXPECT_TRUE(prefixEligibleUnder(floor, BuildPrefix{.writer_epoch = 3, .build_sequence = 10}));
    EXPECT_FALSE(prefixEligibleUnder(std::nullopt, BuildPrefix{.writer_epoch = 1, .build_sequence = 1}));
}

namespace
{

/// Deletes the mount key the first time it is read, so the page decides with a floor whose object is
/// gone by the time it decides. Retirement is permanent, so the decisions must be the ones the floor
/// admitted when read, and no active build may be nominated.
class MountVanishesBackend final : public CountingBackend
{
public:
    using CountingBackend::read;
    std::optional<Raw> read(const String & key, TransportAccess & access) override
    {
        auto got = CountingBackend::read(key, access);
        if (got && key == mount_key && !fired)
        {
            fired = true;
            static_cast<void>(InMemoryBackend::remove(key, got->value, access));
        }
        return got;
    }
    String mount_key;
    bool fired = false;
};

}

TEST(CASOrphanSweepRequests, MountVanishingMidPageKeepsTheDecisionsOfTheFloorAsRead)
{
    auto backend = std::make_shared<MountVanishesBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test",
                                                .manifest_sweep_list_budget_keys = 1000,
                                                .manifest_sweep_delete_budget_keys = 100,
                                                .gc_fold_max_defer_rounds = 0});
    const Layout & layout = store->layout();
    casAdmitEntry(*backend, layout, kNs);
    publishAt(*backend, layout, kNs, RefTxnId{1, 1}, "live", /*build_sequence=*/51, DB::UInt128(0x7001), /*birth=*/true);
    writeRecoverableCkptForRawFixture(*backend, layout, kNs, RefCkpt{
        .life_epoch = 1, .committed_through = RefTxnId{1, 1},
        .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt});
    for (uint64_t seq = 1; seq <= 50; ++seq)
        writeManifestRaw(*backend, layout, kNs, build(seq), {blobEntryFor("a", DB::UInt128(0x100 + seq))});
    setWatermarkMinActive(*backend, layout, "test", 1, /*min_active=*/25);
    backend->mount_key = layout.mountKey("test");

    const ManifestSweepResult result = planManifestCursorPage(*store, "", 1000, 100, true, nullptr);
    EXPECT_TRUE(backend->fired);
    EXPECT_EQ(result.floor_lookups, 1u);
    EXPECT_EQ(result.retained_no_coverage, 24u) << "the 24 retired builds were decided under the floor as read";
    EXPECT_TRUE(result.nominations.empty());
    OperationForTest op(*backend);
    EXPECT_FALSE((*op).head(layout.mountKey("test"), Retry::once()).has_value());
}

namespace
{

ThreadPool makeReadPool(size_t threads)
{
    return ThreadPool{CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive,
                      CurrentMetrics::LocalThreadScheduled, threads, threads, /*queue_size*/ 0};
}

/// Every GET the page issued, by key, in whichever thread it ran.
std::map<String, uint64_t> getsOf(CountingBackend & backend)
{
    std::map<String, uint64_t> gets;
    for (const String & key : backend.touchedKeys())
        if (const uint64_t n = backend.getCount(key); n != 0)
            gets[key] = n;
    return gets;
}

struct PageOutcome
{
    uint64_t listed, skipped, deleted, undecodable, retained_no_coverage, retained_hold,
        retained_unconsumed_seal, retained_tail_removal, retained_work_budget, floor_lookups, floor_reads;
    bool wrapped;
    String next_cursor;
    bool operator==(const PageOutcome &) const = default;
};

PageOutcome outcomeOf(const ManifestSweepResult & r)
{
    return {r.listed, r.skipped, r.deleted, r.undecodable, r.retained_no_coverage, r.retained_hold,
            r.retained_unconsumed_seal, r.retained_tail_removal, r.retained_work_budget,
            r.floor_lookups, r.floor_reads, r.wrapped, r.next_cursor};
}

bool sameNomination(const ManifestSweepResult::Nomination & a, const ManifestSweepResult::Nomination & b)
{
    if (!(a.id == b.id) || a.key != b.key || a.token.dialect != b.token.dialect || a.token.value != b.token.value)
        return false;
    if (a.source_retirements.size() != b.source_retirements.size())
        return false;
    for (size_t i = 0; i < a.source_retirements.size(); ++i)
        if (!(a.source_retirements[i].ref == b.source_retirements[i].ref)
            || !(a.source_retirements[i].source_id == b.source_retirements[i].source_id))
            return false;
    return true;
}

/// Both runs decide candidates from the SAME listed order and append nominations in that same order,
/// whichever reader fetched their bytes, so an index-wise comparison is exact -- no sort needed.
bool sameNominations(const std::vector<ManifestSweepResult::Nomination> & a,
                     const std::vector<ManifestSweepResult::Nomination> & b)
{
    if (a.size() != b.size())
        return false;
    for (size_t i = 0; i < a.size(); ++i)
        if (!sameNomination(a[i], b[i]))
            return false;
    return true;
}

/// A fixture whose sweep has REAL candidates and a committed-tail walk long enough to hint ahead,
/// entirely WITHOUT crossing an epoch: this life is born directly at epoch 2 (`{2,1}`, no
/// `prev_epoch_seal` needed -- genesis, not a chain link), and `kDebrisManifests` unowned raw
/// manifests sit at prefix epoch 1 -- a legacy build-prefix number, never part of this life's own ref
/// stream, but eligible under the floor (an old epoch is always eligible) and covered by the folded
/// cursor sitting in epoch 2 (rule 1) all the same, so they become genuine nominations. `kEpochTwoLogs`
/// ordinary committed grants after the birth give the committed-tail walk (and the recovery walk, which
/// starts at this same genesis) a range comfortably longer than one read-ahead window, all inside the
/// ONE epoch neither walk ever leaves -- unlike the epoch-crossing fixture below, whose hints legitimately
/// overshoot a seal and so cannot be expected to read the identical key set at every concurrency, this
/// fixture's GET set is invariant to concurrency, which is what the comparison after it needs.
constexpr uint64_t kDebrisManifests = 6;
constexpr uint64_t kEpochTwoLogs = 80;

struct CandidateFixture
{
    std::shared_ptr<CountingBackend> backend = std::make_shared<CountingBackend>();
    PoolPtr store;

    CandidateFixture()
    {
        PoolConfig config;
        config.pool_prefix = "p";
        config.server_root_id = "test";
        config.manifest_sweep_list_budget_keys = 1000;
        config.manifest_sweep_delete_budget_keys = 100;
        config.gc_fold_max_defer_rounds = 0;
        store = Pool::open(backend, config);
        const Layout & layout = store->layout();
        casAdmitEntry(*backend, layout, kNs);

        for (uint64_t seq = 1; seq <= kDebrisManifests; ++seq)
            writeManifestRaw(*backend, layout, kNs, build(seq), {blobEntryFor("a", DB::UInt128(0x100 + seq))});
        publishAt(*backend, layout, kNs, RefTxnId{2, 1}, "live", /*build_sequence=*/2000,
                  DB::UInt128(0x7001), /*birth=*/true);
        for (uint64_t seq = 2; seq <= kEpochTwoLogs; ++seq)
            publishAt(*backend, layout, kNs, RefTxnId{2, seq}, "epoch2-" + std::to_string(seq),
                      2000 + seq, DB::UInt128(0x9000 + seq), /*birth=*/false);
        writeRecoverableCkptForRawFixture(*backend, layout, kNs, RefCkpt{
            .life_epoch = 2, .committed_through = RefTxnId{2, kEpochTwoLogs},
            .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt});
        seedFoldCursorForTest(*backend, layout, kNs, RefTxnId{2, 1});
        setWatermarkMinActive(*backend, layout, "test", /*writer_epoch=*/2, /*min_active=*/1);
        backend->resetCounts();
    }
};

}

/// The read-ahead reader must issue the same GETs against the same keys as the inline reader, decide
/// the same way, and nominate the exact same candidates; only when the bytes arrive moves. The fixture
/// gives the page real candidates and a committed-tail walk spanning more than one window, so the
/// comparison actually exercises the read-ahead instead of vacuously agreeing over nothing.
TEST(CASOrphanSweepRequests, PageIsIdenticalInlineAndWithReadAhead)
{
    CandidateFixture inline_f;
    const ManifestSweepResult inline_r = planManifestCursorPage(*inline_f.store, "", 1000, 100, true, nullptr);
    const auto inline_gets = getsOf(*inline_f.backend);
    ASSERT_EQ(inline_r.nominations.size(), kDebrisManifests)
        << "the fixture must actually produce candidates, or this test proves nothing";

    CandidateFixture ahead_f;
    ThreadPool pool = makeReadPool(4);
    const uint64_t hits_before = ProfileEvents::global_counters[ProfileEvents::CASGCReadAheadHit].load();
    const ManifestSweepResult ahead_r = planManifestCursorPage(
        *ahead_f.store, "", 1000, 100, true, nullptr, &pool, /*read_concurrency=*/16);
    const uint64_t hits = ProfileEvents::global_counters[ProfileEvents::CASGCReadAheadHit].load() - hits_before;
    const auto ahead_gets = getsOf(*ahead_f.backend);

    EXPECT_GT(hits, 0u) << "the fixture's committed-tail walk and candidates must actually hit the "
                            "read-ahead, or this oracle could never catch a hinting regression";
    EXPECT_EQ(outcomeOf(inline_r), outcomeOf(ahead_r));
    EXPECT_TRUE(sameNominations(inline_r.nominations, ahead_r.nominations));
    EXPECT_EQ(inline_gets, ahead_gets);
}

/// A committed tail that spans two epochs: the walk hints ids past the seal in the old epoch, which
/// do not exist, discards them at the crossing (at most one window), and hints the new epoch's ids.
TEST(CASOrphanSweepRequests, EpochCrossingDiscardsAtMostOneWindowAndTheNewEpochIsHinted)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test",
                                                .manifest_sweep_list_budget_keys = 1000,
                                                .manifest_sweep_delete_budget_keys = 100,
                                                .gc_fold_max_defer_rounds = 0});
    const Layout & layout = store->layout();
    casAdmitEntry(*backend, layout, kNs);
    /// Epoch 1: birth at {1,1}, six ordinary logs {1,2..7}, seal at {1,8}. Epoch 2: {2,1..40}.
    publishAt(*backend, layout, kNs, RefTxnId{1, 1}, "t1", /*build_sequence=*/100, DB::UInt128(0x7001), /*birth=*/true);
    for (uint64_t seq = 2; seq <= 7; ++seq)
        publishAt(*backend, layout, kNs, RefTxnId{1, seq}, "t" + std::to_string(seq), 100 + seq, DB::UInt128(0x7000 + seq), /*birth=*/false);
    writeSealAt(*backend, layout, kNs, RefTxnId{1, 8});
    publishAt(*backend, layout, kNs, RefTxnId{2, 1}, "u1", 200, DB::UInt128(0x8001), /*birth=*/false, /*prev_epoch_seal=*/RefTxnId{1, 8});
    for (uint64_t seq = 2; seq <= 40; ++seq)
        publishAt(*backend, layout, kNs, RefTxnId{2, seq}, "u" + std::to_string(seq), 200 + seq, DB::UInt128(0x8000 + seq), /*birth=*/false);
    writeRecoverableCkptForRawFixture(*backend, layout, kNs, RefCkpt{
        .life_epoch = 1, .committed_through = RefTxnId{2, 40},
        .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = RefTxnId{1, 8}});
    writeManifestRaw(*backend, layout, kNs, build(1), {blobEntryFor("a", DB::UInt128(0x100))});
    setWatermarkMinActive(*backend, layout, "test", 2, /*min_active=*/1000);
    backend->resetCounts();

    const uint64_t wasted_before = ProfileEvents::global_counters[ProfileEvents::CASGCReadAheadWasted].load();
    const uint64_t hits_before = ProfileEvents::global_counters[ProfileEvents::CASGCReadAheadHit].load();
    ThreadPool pool = makeReadPool(4);
    const ManifestSweepResult result = planManifestCursorPage(*store, "", 1000, 100, true, nullptr, &pool, 16);
    const uint64_t wasted = ProfileEvents::global_counters[ProfileEvents::CASGCReadAheadWasted].load() - wasted_before;
    const uint64_t hits = ProfileEvents::global_counters[ProfileEvents::CASGCReadAheadHit].load() - hits_before;

    /// `publishAt` writes a manifest as a side effect of every call above, so the page's LIST sees
    /// every one of those (47) plus the one raw manifest -- the fixture is about the hint/discard
    /// mechanics of the ref-log walks a namespace's first candidate triggers, not about the listing.
    EXPECT_EQ(result.listed, 48u);
    /// The SAME reader crosses the epoch twice on this fixture: once inside the recovery walk
    /// `activeManifestKeys` runs via `recoverRefTableDetailedFromAuthority`, and again in its own
    /// committed-tail walk over the same {1,1}..{2,40} range (today's pre-existing double walk, not
    /// something this change introduces) -- so at most two windows are discarded, not one.
    EXPECT_LE(wasted, 128u) << "at most two windows at concurrency 16: one per walk crossing the epoch";
    EXPECT_GE(hits, 30u) << "the new epoch's logs were hinted and taken";
    /// Both walks read epoch 2's logs once each, so every key is read twice -- today's behaviour with
    /// or without read-ahead, not something the hint/discard rule changes.
    for (uint64_t seq = 1; seq <= 40; ++seq)
        EXPECT_EQ(backend->getCount(layout.refLogKey(NamespaceLifeId::fromCatalogEntry(kNs, catalogLifeIdForTest(*backend, layout, kNs)), RefTxnId{2, seq})), 2u) << seq;
}
