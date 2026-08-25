#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedSettings.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Poco/AutoPtr.h>
#include <Poco/Util/XMLConfiguration.h>

#include <algorithm>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <vector>

/// THE RETIREMENT SWEEP's executable half.
///
/// Two mechanisms of the pre-v9 ref protocol lost their premise when the ref stream became a
/// contiguous, arithmetically-walkable chain, and this file is where each retirement is proved rather
/// than asserted in a comment:
///
///   1. PROBE A's ABORT. The detector compared the round's two enumerations of `cas/ns/stream/` and, on any
///      disagreement, aborted ref folding for the whole round -- because the fold ITERATED the listing,
///      so a hole in it meant a record was about to be skipped forever. The intake reads by exact key
///      now, so a hole folds through; a detector that still aborted would be halting a round that is
///      provably doing the right thing. It was demoted to a sampled store-quality detector and then
///      deleted outright: the round enumerates `cas/ns/stream/` exactly once, on every round.
///   2. THE MATERIALIZATION GRACE (`T_mat`). A post-reclaim sleep, long enough for a straggler
///      conditional `PUT` from a dying epoch to land or exhaust its retries BEFORE the successor
///      trusted its recovery LISTINGS. Recovery does not trust listings; it closes every dead epoch
///      with an in-band `EpochSeal` written as a conditional create, and the straggler's own create
///      loses to it. The wait is deleted outright, setting and all -- the feature never shipped, so
///      there is no config to protect and no parsed-but-inert period to serve.
///
/// The retirement rationale (premise / verdict / replacement / evidence, one row per retired item)
/// is captured in `docs/en/antalya/cas/architecture/design-history.md`.

namespace DB::ErrorCodes
{
    extern const int NETWORK_ERROR;
}

using namespace DB::Cas;
using DB::Cas::tests::idOf;
using DB::Cas::tests::u128Of;

namespace
{

/// A backend that drops ONE chosen key from ONE chosen `list` call while exact `get`/`head` of that key
/// keep working: the minimal realisation of "the store returned an incomplete answer". WHICH call is
/// load-bearing here, because the round enumerates the ref prefix exactly once -- so `nth = 0` is
/// always the round's own enumeration, the one the fold regroups.
///
/// `nth` counts only those `list` calls that WOULD have returned the key, so unrelated prefix
/// enumerations cannot shift it; arm it AFTER every seeding write, since the writer's own namespace
/// listings would otherwise consume a qualifying call.
class HoleyListBackend : public InMemoryBackend
{
public:
    void omitFromNthListCall(const String & key, size_t nth)
    {
        std::lock_guard lock(m);
        omitted = key;
        target_call = nth;
        seen_calls = 0;
        served = false;
    }

    /// Whether the hole was actually served. Asserted by every test that plants one, so a mistyped key
    /// or a miscounted `nth` cannot let a test pass vacuously.
    bool holeServed() const
    {
        std::lock_guard lock(m);
        return served;
    }

    ListPage list(const String & prefix, const String & cursor, size_t limit) override
    {
        ListPage page = InMemoryBackend::list(prefix, cursor, limit);
        std::lock_guard lock(m);
        if (omitted.empty())
            return page;
        auto it = std::find_if(page.keys.begin(), page.keys.end(),
                               [&](const ListedKey & k) { return k.key == omitted; });
        if (it == page.keys.end())
            return page;              /// not a qualifying call -- do not count it
        if (seen_calls++ != target_call)
            return page;
        page.keys.erase(it);
        served = true;
        omitted.clear();              /// one hole only
        return page;
    }

private:
    mutable std::mutex m;
    String omitted;
    size_t target_call = 0;
    size_t seen_calls = 0;
    bool served = false;
};

/// Counts full enumerations of the ref prefix -- one increment per `list` call whose prefix is EXACTLY
/// `cas/ns/stream/`, which is how "the round lists this prefix once" becomes an assertion instead of a
/// claim. `janitor_prefix_lists` counts the bounded `cas/ns/` janitor page separately, by the same exact
/// match: the two prefixes are distinct strings (`cas/ns/stream/` vs `cas/ns/`), so a hot scan and the
/// janitor's own bounded page can never be conflated by this counter.
class RefPrefixListCountingBackend : public InMemoryBackend
{
public:
    String refs_prefix;
    String janitor_prefix;
    std::atomic<size_t> ref_prefix_lists{0};
    std::atomic<size_t> janitor_prefix_lists{0};

    ListPage list(const String & prefix, const String & cursor, size_t limit) override
    {
        if (!refs_prefix.empty() && prefix == refs_prefix)
            ++ref_prefix_lists;
        if (!janitor_prefix.empty() && prefix == janitor_prefix)
            ++janitor_prefix_lists;
        return InMemoryBackend::list(prefix, cursor, limit);
    }
};

/// Forces the FIRST `putIfAbsent` whose key contains `fault_key_substr` to throw an ambiguous
/// (Unresolved-classified) exception, `fault_count` times -- the minimal fault injection needed to drive
/// a ref-log append into the `Unresolved`/wedge outcome, with `max_attempts = 1` in the budget so the
/// single failed attempt exhausts the retry budget immediately. (Same shape as `gtest_cas_pool.cpp`'s
/// file-local backend of the same name; both are three lines of `throw` over `InMemoryBackend`, and
/// hoisting a shared one would couple two suites' fault models for no gain.)
class UnresolvedPutBackend final : public InMemoryBackend
{
public:
    using Backend::putIfAbsent;

    String fault_key_substr;
    int fault_count = 0;

    PutResult putIfAbsent(const String & key, const String & bytes, const ObjectMeta & meta) override
    {
        if (fault_count > 0 && !fault_key_substr.empty() && key.find(fault_key_substr) != String::npos)
        {
            --fault_count;
            throw Poco::TimeoutException("UnresolvedPutBackend: simulated ambiguous result (response lost)");
        }
        return InMemoryBackend::putIfAbsent(key, bytes, meta);
    }
};

/// GC's fence-out applied directly to the mount lease: preserve the body, set `gc_fenced`, bump `seq`
/// (token-guarded). A subsequent `tryRemountOnce` then reclaims a fresh incarnation.
void fenceOutMount(Backend & backend, const String & mount_key)
{
    const auto got = backend.get(mount_key);
    ASSERT_TRUE(got.has_value());
    MountLease m = decodeMountLease(got->bytes);
    m.gc_fenced = true;
    m.seq += 1;
    ASSERT_EQ(backend.putOverwrite(mount_key, encodeMountLease(m), got->token).outcome, PutOutcome::Done);
}

/// Publish one part `ref` with a single content blob whose payload is `payload`.
ManifestId publishOneBlobPart(const PoolPtr & s, const RootNamespace & ns, const String & ref,
                              const String & payload)
{
    PartWriteInfo info;
    info.intended_ref = ns.string() + "/" + ref;
    auto build = s->beginPartWrite(info);

    ManifestEntry e;
    e.path = "data.bin";
    e.placement = EntryPlacement::Blob;
    e.ref = BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(u128Of(payload))};
    e.blob_size = payload.size();

    const ManifestId id = build->stageManifest({e});
    build->precommitAdd(ns, ref, id);
    build->putBlob(idOf(payload), BlobSource::fromString(payload));
    build->promote(ns, ref, build->buildId(), id);
    return id;
}

/// Every ref-log key of `ns` currently listed, in key order.
std::set<String> listRefLogKeys(Backend & b, const Layout & l, const RootNamespace & ns)
{
    std::set<String> out;
    String cursor;
    while (true)
    {
        const ListPage page = b.list(l.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)), cursor, 1000);
        for (const ListedKey & k : page.keys)
            if (const auto parsed = l.parseRefObjectKey(k.key); parsed && parsed->kind == RefObjectKind::Log)
                out.insert(k.key);
        if (page.next_cursor.empty())
            break;
        cursor = page.next_cursor;
    }
    return out;
}

/// The greatest ref-log id of `ns` in the current listing.
RefTxnId greatestLoggedId(Backend & b, const Layout & l, const RootNamespace & ns)
{
    RefTxnId best{};
    for (const String & key : listRefLogKeys(b, l, ns))
        if (const auto parsed = l.parseRefObjectKey(key); parsed && best < parsed->txn_id)
            best = parsed->txn_id;
    return best;
}

Poco::AutoPtr<Poco::Util::XMLConfiguration> makeDiskConfig(const std::string & inner)
{
    std::istringstream iss("<clickhouse><disk>" + inner + "</disk></clickhouse>");
    return new Poco::Util::XMLConfiguration(iss);
}

}

/// The blob the hidden removal releases must actually be reclaimed. This is the retention half of the
/// skipped-transaction class, and it is the outcome the abort used to buy at the price of a lost round:
/// under arithmetic intake the very round that was served the hole folds the removal, so the blob dies
/// on the normal schedule rather than waiting for a listing to become honest again.
TEST(CASRetirementSweep, AHiddenRemovalStillReclaimsItsBlob)
{
    auto backend = std::make_shared<HoleyListBackend>();
    auto store = Pool::open(backend, PoolConfig{
        .pool_prefix = "p", .server_root_id = "test",
        .gc_fold_max_defer_rounds = 0,
    });
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv/tbl"};
    const String payload = "reclaimed-payload";
    /// Stage B (Task 4-C): pin to the sentinel before the first real touch -- `listRefLogKeys` below
    /// lists at that exact prefix. The raw `Live` row also needs the same empty checkpoint authority
    /// as a completed production birth before `publishOneBlobPart` invokes recovery.
    DB::Cas::tests::fixture::admitLive(*backend, layout, ns);
    DB::Cas::tests::writeRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = std::nullopt,
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    publishOneBlobPart(store, ns, "part_a", payload);
    Gc gc(store, hexToU128("00000000000000000000000000000012"));
    /// Reclaiming rounds: Stage A's destructive gate refuses a universe it cannot enumerate, and this
    /// test's subject IS the reclamation (see `runRegularRoundReclaiming`).
    ASSERT_TRUE(DB::Cas::tests::runRegularRoundReclaiming(gc).acquired_lease);
    store->renewWatermarkOnce();
    const String blob_key = layout.blobKey(BlobRef{BlobHashAlgo::CityHash128,
                                                   BlobDigest::fromU128(u128Of(payload))});
    ASSERT_TRUE(backend->head(blob_key).exists);

    const std::set<String> before_drop = listRefLogKeys(*backend, layout, ns);
    store->dropRef(ns, "part_a");
    String removal_key;
    for (const String & k : listRefLogKeys(*backend, layout, ns))
        if (!before_drop.contains(k))
            removal_key = k;
    ASSERT_FALSE(removal_key.empty());
    store->renewWatermarkOnce();

    backend->omitFromNthListCall(removal_key, /*nth=*/0);

    /// condemn -> graduate -> exact-token delete needs several rounds; the first of them is the one
    /// served the hole.
    for (int i = 0; i < 12; ++i)
    {
        ASSERT_TRUE(DB::Cas::tests::runRegularRoundReclaiming(gc).acquired_lease);
        store->renewWatermarkOnce();
    }
    ASSERT_TRUE(backend->holeServed()) << "the sabotage never fired";

    EXPECT_FALSE(backend->head(blob_key).exists)
        << "the removal was hidden from one enumeration and never folded -- the retention half of the "
           "skipped-transaction class, which arithmetic intake is supposed to close";
}

/// THE RETIREMENT's whole point, made observable: a folding round enumerates `cas/ns/stream/` exactly
/// ONCE, on EVERY round of a multi-round run -- not just the first, since a regression that quietly
/// reintroduced a second enumeration only on a LATER round would pass a one-round check for the wrong
/// reason. The bounded `cas/ns/` janitor page is a separate exact-string prefix and runs every round
/// too; it must never be counted as, or mistaken for, a hot scan of the ref prefix. 32 rounds exercises
/// the deleted detector's own cadence (every 16th folding round) twice over, so a regression that only
/// reintroduces the second enumeration on that cadence cannot hide inside a shorter run.
TEST(CASRetirementSweep, TheRoundEnumeratesTheRefPrefixExactlyOnce)
{
    auto backend = std::make_shared<RefPrefixListCountingBackend>();
    auto store = Pool::open(backend, PoolConfig{
        .pool_prefix = "p", .server_root_id = "test",
        .gc_fold_max_defer_rounds = 0,
    });
    backend->refs_prefix = store->layout().casRefsPrefix();
    backend->janitor_prefix = store->layout().namespaceRootPrefix();
    const RootNamespace ns{"srv/tbl"};
    publishOneBlobPart(store, ns, "part_a", "counted-payload");
    store->renewWatermarkOnce();

    Gc gc(store, hexToU128("00000000000000000000000000000013"));
    for (int round = 0; round < 32; ++round)
    {
        backend->ref_prefix_lists.store(0);
        backend->janitor_prefix_lists.store(0);
        const RoundReport report = gc.runRegularRound();
        ASSERT_TRUE(report.acquired_lease);
        ASSERT_FALSE(report.deferred);
        EXPECT_EQ(backend->ref_prefix_lists.load(), 1u)
            << "round " << round << " enumerated cas/ns/stream/ a number of times other than once";
        EXPECT_GT(backend->janitor_prefix_lists.load(), 0u)
            << "round " << round << " never took the bounded cas/ns/ janitor page";
        store->renewWatermarkOnce();
    }
}


/// ==================== item 2: the materialization grace, retired ====================

/// THE MECHANISM THAT REPLACED THE WAIT, tested directly. A ref lane is left holding an UNDECIDED
/// conditional `PUT` when the fence trips -- the exact state `T_mat` was introduced to wait out. The
/// remount proceeds with no wait at all, the next recovery closes the dead epoch with an in-band
/// `EpochSeal` at the slot the straggler would have taken, and the straggler's own conditional create
/// then LOSES to it.
///
/// The assertion is the conflict itself, not the absence of damage: "nothing bad happened" would also
/// be true of a run where the straggler simply never arrived.
TEST(CASRetirementSweep, AStragglerFromTheDyingEpochLosesItsCreateToTheRecoverySeal)
{
    CasRequestBudget budget;
    budget.max_attempts = 1;
    budget.attempt_timeout_ms = 100;
    budget.operation_deadline_ms = 5000;
    budget.lease_safety_margin_ms = 100;

    auto backend = std::make_shared<UnresolvedPutBackend>();
    uint64_t fake_boot = 1'000'000;
    std::vector<uint64_t> waits;
    auto store = Pool::open(backend, PoolConfig{
        .pool_prefix = "p", .server_root_id = "test",
        .mount_lease_ttl_ms = std::chrono::milliseconds(30000),
        .cas_request_budget = budget,
        .boot_ms_fn = [&] { return fake_boot; },
        .wait_sleep_fn = [&](uint64_t ms) { fake_boot += ms; waits.push_back(ms); },
    });
    ASSERT_TRUE(store);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv/straggler"};
    /// Pin to the transition life before the first real touch, and give that raw `Live` row the exact
    /// empty checkpoint authority that production birth would have published before recovery.
    DB::Cas::tests::fixture::admitLive(*backend, layout, ns);
    DB::Cas::tests::writeRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = std::nullopt,
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    publishOneBlobPart(store, ns, "x", "straggler-payload");
    ASSERT_EQ(store->liveWriterEpoch(), 1u);

    /// Drive the next ref-log append into the Unresolved/wedge outcome: the single attempt the budget
    /// allows fails ambiguously, so this process can never learn whether its conditional PUT landed.
    /// That undecidability is the whole reason the resolution is a conditional CREATE and not a GET.
    backend->fault_key_substr = layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_log/";
    backend->fault_count = 1;
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropRef(ns, "x"); });
    ASSERT_TRUE(store->refLaneWedgedForTest(ns));

    /// The id the straggler would occupy: one past the greatest record that is actually durable in the
    /// dying epoch. That is also, by construction, where the recovery seal goes.
    const RefTxnId greatest = greatestLoggedId(*backend, layout, ns);
    ASSERT_EQ(greatest.writer_epoch, 1u);
    const RefTxnId straggler_slot{greatest.writer_epoch, greatest.ref_sequence + 1};
    ASSERT_FALSE(backend->head(layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), straggler_slot)).exists)
        << "the slot must be empty before recovery -- otherwise this test proves nothing about who won";

    /// Fence and remount. No wait: this is the case that used to cost 30 seconds.
    fake_boot += 30001;
    fenceOutMount(*backend, layout.mountKey("test"));
    ASSERT_TRUE(store->tryRemountOnce());
    ASSERT_EQ(store->liveWriterEpoch(), 2u);
    EXPECT_TRUE(waits.empty())
        << "the remount blocked on an operator-configured wait; the grace is supposed to be gone";

    /// Touch the namespace so it re-recovers under the new epoch: the walk closes epoch 1 in band. The
    /// ref itself is still THERE -- the removal's PUT was the undecided one and (in this fixture) never
    /// landed, which is precisely the state that leaves a straggler outstanding.
    backend->fault_key_substr.clear();
    EXPECT_EQ(store->listRefs(ns).size(), 1u);
    ASSERT_TRUE(backend->head(layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), straggler_slot)).exists)
        << "recovery did not seal the dead epoch at the slot a straggler would take -- without that "
           "seal there is nothing for the straggler's create to lose to";

    /// THE STRAGGLER ARRIVES. Its conditional create is refused, whenever it happens to land.
    const PutResult put = backend->putIfAbsent(layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), straggler_slot), "ghost-body");
    EXPECT_EQ(put.outcome, PutOutcome::PreconditionFailed)
        << "the dying epoch's straggler overwrote (or joined) a slot the successor had already sealed";
}

/// THE FAIL-CLOSE THAT REPLACES THE DELETED SETTING. `materialization_grace_ms` is gone from the
/// settings table outright -- no parsed-but-inert period, no deprecation log -- so a config that still
/// asks for the wait is refused at disk open by the generic unknown-key path, loudly, instead of being
/// silently ignored by a server that no longer honours it. The feature never shipped, so there is no
/// deployed config this can break.
TEST(CASRetirementSweep, AConfigStillAskingForTheMaterializationGraceIsRejected)
{
    auto cfg = makeDiskConfig(
        "<cas_server_root_id>srv1</cas_server_root_id><cas_materialization_grace_ms>30000</cas_materialization_grace_ms>");
    DB::ContentAddressedSettings s;
    EXPECT_THROW(
        s.loadFromConfig(*cfg, "disk", "/scratch", "/scratch", [](const std::string & v) { return v; }),
        DB::Exception)
        << "a retired setting must fail the disk open, not be quietly accepted and ignored";
}
