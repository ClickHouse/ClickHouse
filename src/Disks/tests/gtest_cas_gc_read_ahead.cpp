#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcReadAhead.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRetry.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasGcStateFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCatalog.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/tests/cas_test_helpers.h>

#include <Common/CurrentMetrics.h>
#include <Common/ThreadPool.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <map>
#include <stdexcept>
#include <thread>

namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

using namespace DB::Cas;
using DB::Cas::tests::CountingBackend;
using DB::Cas::tests::idOf;
using DB::Cas::tests::openRequestsForTest;
using DB::Cas::tests::u128Of;

namespace
{

/// ============================ the class, on its own ============================

struct ReadAheadRig
{
    std::shared_ptr<CountingBackend> backend = std::make_shared<CountingBackend>();
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    ThreadPool pool{CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive,
                    CurrentMetrics::LocalThreadScheduled,
                    /*max_threads*/ 4, /*max_free_threads*/ 4, /*queue_size*/ 0};

    void put(const String & key, const String & bytes)
    {
        ASSERT_TRUE(std::holds_alternative<Committed>(op.create(key, bytes, Retry::once()))) << key;
    }
};

}

TEST(CASGCReadAhead, HitReturnsTheHintedBytesWithOneRequest)
{
    ReadAheadRig rig;
    rig.put("k1", "one");
    GcReadAhead reads(rig.op, rig.requests, rig.pool, 4);
    rig.backend->resetCounts();

    reads.hintRead("k1");
    EXPECT_EQ(reads.pending(), 1u);
    const auto got = reads.takeRead("k1");
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->bytes, "one");
    EXPECT_EQ(reads.pending(), 0u);
    EXPECT_EQ(rig.backend->getCount("k1"), 1u);
}

TEST(CASGCReadAhead, MissReadsInlineOnTheCallersOperation)
{
    ReadAheadRig rig;
    rig.put("k2", "two");
    GcReadAhead reads(rig.op, rig.requests, rig.pool, 4);
    rig.backend->resetCounts();

    const auto got = reads.takeRead("k2");
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->bytes, "two");
    EXPECT_EQ(rig.backend->getCount("k2"), 1u);
}

TEST(CASGCReadAhead, AbsentKeyIsNulloptHintedOrNot)
{
    ReadAheadRig rig;
    GcReadAhead reads(rig.op, rig.requests, rig.pool, 4);

    reads.hintRead("absent-hinted");
    EXPECT_FALSE(reads.takeRead("absent-hinted").has_value());
    EXPECT_FALSE(reads.takeRead("absent-inline").has_value());
}

TEST(CASGCReadAhead, DuplicateHintIsOneRequest)
{
    ReadAheadRig rig;
    rig.put("k1", "one");
    GcReadAhead reads(rig.op, rig.requests, rig.pool, 4);
    rig.backend->resetCounts();

    reads.hintRead("k1");
    reads.hintRead("k1");
    EXPECT_EQ(reads.pending(), 1u);
    ASSERT_TRUE(reads.takeRead("k1").has_value());
    EXPECT_EQ(rig.backend->getCount("k1"), 1u);
}

TEST(CASGCReadAhead, WorkerExceptionRethrowsAtTheTakeSiteAndDoesNotPoisonTheKey)
{
    ReadAheadRig rig;
    rig.put("k3", "three");
    /// A non-Poco exception is a deterministic local failure to the engine, so it is thrown on the
    /// first attempt rather than reissued.
    rig.backend->failNextReadWith("k3", std::make_exception_ptr(std::runtime_error("injected read fault")));
    GcReadAhead reads(rig.op, rig.requests, rig.pool, 4);

    reads.hintRead("k3");
    EXPECT_THROW(reads.takeRead("k3"), std::runtime_error);
    EXPECT_EQ(reads.pending(), 0u);

    const auto again = reads.takeRead("k3");   /// the fault was consumed; an inline read now answers
    ASSERT_TRUE(again.has_value());
    EXPECT_EQ(again->bytes, "three");
}

TEST(CASGCReadAhead, ConcurrencyOneNeverHints)
{
    ReadAheadRig rig;
    rig.put("k1", "one");
    GcReadAhead reads(rig.op, rig.requests, rig.pool, 1);
    rig.backend->resetCounts();

    EXPECT_EQ(reads.window(), 0u);
    reads.hintRead("k1");
    reads.hintHead("k1");
    EXPECT_EQ(reads.pending(), 0u);
    ASSERT_TRUE(reads.takeRead("k1").has_value());
    ASSERT_TRUE(reads.takeHead("k1").has_value());
    EXPECT_EQ(rig.backend->getCount("k1"), 1u);
    EXPECT_EQ(rig.backend->headCount("k1"), 1u);
}

TEST(CASGCReadAhead, DestructorWaitsForOutstandingRequests)
{
    ReadAheadRig rig;
    rig.put("k1", "one");
    rig.backend->resetCounts();
    {
        GcReadAhead reads(rig.op, rig.requests, rig.pool, 4);
        reads.hintRead("k1");
        reads.hintHead("k1");
    }
    EXPECT_EQ(rig.backend->getCount("k1"), 1u);
    EXPECT_EQ(rig.backend->headCount("k1"), 1u);
}

TEST(CASGCReadAhead, HeadHitCarriesSizeAndAbsentIsNullopt)
{
    ReadAheadRig rig;
    rig.put("k1", "one");
    GcReadAhead reads(rig.op, rig.requests, rig.pool, 4);
    rig.backend->resetCounts();

    reads.hintHead("k1");
    const auto meta = reads.takeHead("k1");
    ASSERT_TRUE(meta.has_value());
    EXPECT_EQ(meta->size, 3u);
    EXPECT_FALSE(reads.takeHead("absent").has_value());
    EXPECT_EQ(rig.backend->headCount("k1"), 1u);
}

TEST(CASGCReadAhead, WindowIsFourTimesConcurrency)
{
    ReadAheadRig rig;
    GcReadAhead reads(rig.op, rig.requests, rig.pool, 8);
    EXPECT_EQ(reads.window(), 32u);
}

/// ============================ the fold, at 1 against 8 ============================

namespace
{

const UInt128 kGc = u128Of("gc-read-ahead");

ManifestId publishPart(const PoolPtr & s, const String & ns, const String & ref, const String & payload)
{
    const RootNamespace nsr{ns};
    PartWriteInfo info;
    info.intended_ref = ns + "/" + ref;
    auto build = s->beginPartWrite(info);

    ManifestEntry e;
    e.path = "data.bin";
    e.placement = EntryPlacement::Blob;
    e.ref = BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(u128Of(payload))};
    e.blob_size = payload.size();

    const ManifestId id = build->stageManifest({e});
    build->precommitAdd(nsr, ref, id);
    build->putBlob(idOf(payload), BlobSource::fromString(payload));
    build->promote(nsr, ref, build->buildId(), id);
    return id;
}

/// Three namespaces. `wide` carries a ref-log backlog longer than any window this file uses, so the
/// same-epoch lookahead is genuinely exercised; `quiet` publishes once and never drops, so its
/// frontier is proved by the checkpoint ceiling with no read at all; `gone` is emptied entirely, so
/// its blobs reach in-degree zero and the reduce phase HEADs them. Every blob is unique to its part.
void populate(const PoolPtr & store)
{
    for (int i = 0; i < 30; ++i)
        publishPart(store, "srv1/wide", fmt::format("part_{}", i), fmt::format("wide-payload-{}", i));
    for (int i = 0; i < 15; ++i)
        store->dropRef(RootNamespace{"srv1/wide"}, fmt::format("part_{}", i));

    publishPart(store, "srv1/quiet", "only", "quiet-payload");

    publishPart(store, "srv1/gone", "a", "gone-payload-a");
    publishPart(store, "srv1/gone", "b", "gone-payload-b");
    store->dropRef(RootNamespace{"srv1/gone"}, "a");
    store->dropRef(RootNamespace{"srv1/gone"}, "b");

    store->renewWatermarkOnce();
}

/// TWO POOLS ARE NOT BYTE-COMPARABLE UNTIL THEIR IDENTITIES ARE MAPPED. A namespace's catalog
/// incarnation is minted from the process RNG at creation, and it appears BOTH inside every one of that
/// namespace's object keys and inside the fold seal's `life` rows -- so two independently created pools
/// running the identical workload produce identical decisions under different names, and the seal's
/// `ref_life` rows come out in a different order because they are keyed by that random id.
///
/// Neither fact has anything to do with the read-ahead, and hiding them by weakening the comparison
/// would hide the read-ahead's own defects too. So the identities are MAPPED instead of dropped: each
/// run reports its own incarnation-hex -> namespace-name table, every 32-hex id in a key or a seal is
/// rewritten to the namespace it names, and the `ref_life` rows are sorted once their names are stable.
/// What survives the rewrite is everything the fold decided; what it removes is only the naming.
using IdNames = std::map<String, String>;

String normalizeIds(const String & text, const IdNames & names)
{
    String out = text;
    for (const auto & [hex, name] : names)
    {
        size_t at = 0;
        while ((at = out.find(hex, at)) != String::npos)
        {
            out.replace(at, hex.size(), name);
            at += name.size();
        }
    }
    return out;
}

/// The seal with its ids named and its `ref_life` rows sorted; every other row keeps its position.
String canonicalSeal(const String & seal, const IdNames & names)
{
    const String named = normalizeIds(seal, names);
    std::vector<String> out;
    std::vector<String> lives;
    size_t pos = 0;
    while (pos <= named.size())
    {
        const size_t nl = named.find('\n', pos);
        const String line = named.substr(pos, nl == String::npos ? String::npos : nl - pos);
        if (line.find("\"kind\":\"ref_life\"") != String::npos)
        {
            lives.push_back(line);
        }
        else
        {
            if (!lives.empty())
            {
                std::sort(lives.begin(), lives.end());
                out.insert(out.end(), lives.begin(), lives.end());
                lives.clear();
            }
            out.push_back(line);
        }
        if (nl == String::npos)
            break;
        pos = nl + 1;
    }
    std::sort(lives.begin(), lives.end());
    out.insert(out.end(), lives.begin(), lives.end());

    String joined;
    for (const String & line : out)
    {
        joined += line;
        joined += '\n';
    }
    return joined;
}

struct FoldRun
{
    std::vector<String> seals;                        /// the fold seal's bytes after each round
    std::vector<std::map<String, UInt64>> intake;     /// `fold_ref_intake` metrics, per round
    std::vector<std::map<String, UInt64>> reduce;     /// `fold_reduce` metrics, per round
    std::map<String, uint64_t> gets;                  /// key -> GETs over the whole run
    std::map<String, uint64_t> heads;                 /// key -> HEADs over the whole run
    std::vector<size_t> condemned;
    std::vector<uint64_t> deleted;
    IdNames id_names;                                 /// incarnation hex -> namespace, for the comparison
};

void runFolds(uint64_t concurrency, size_t rounds, FoldRun & out)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = Pool::open(backend,
        PoolConfig{.pool_prefix = "p", .server_root_id = "test",
                   .gc_fold_max_defer_rounds = 0, .gc_read_concurrency = concurrency});
    populate(store);

    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const Layout & layout = store->layout();

    /// Read the id table BEFORE the rounds: a namespace the fold reclaims loses its catalog row, and its
    /// keys still have to be nameable when the two runs are compared.
    for (const CatalogEntry & entry : CasRefCatalog::read(op, layout).catalog.entries)
        out.id_names.emplace(u128ToHex(entry.incarnation), "<" + entry.ns.string() + ">");

    Gc gc(store, kGc);
    gc.setPhaseSink([&](const GcPhaseRecord & rec)
    {
        if (rec.phase == "fold_ref_intake")
            out.intake.push_back(rec.metrics);
        else if (rec.phase == "fold_reduce")
            out.reduce.push_back(rec.metrics);
    });

    /// Only the rounds' own I/O is compared; the identical population above is not part of the claim.
    backend->resetCounts();

    for (size_t round = 0; round < rounds; ++round)
    {
        const RoundReport report = gc.runRegularRound();
        ASSERT_TRUE(report.acquired_lease) << "round " << round;
        out.condemned.push_back(report.condemned);
        out.deleted.push_back(report.deleted);
        store->renewWatermarkOnce();

        const GcState st = decodeGcState(op.read(layout.gcStateKey(), Retry::once())->bytes);
        const auto seal = op.read(layout.foldSealKey(st.snap_generation, st.snap_attempt), Retry::once());
        out.seals.push_back(seal ? canonicalSeal(seal->bytes, out.id_names) : String{});
    }

    for (const String & key : backend->touchedKeys())
    {
        const String named = normalizeIds(key, out.id_names);
        if (const uint64_t n = backend->getCount(key); n != 0)
            out.gets[named] += n;
        if (const uint64_t n = backend->headCount(key); n != 0)
            out.heads[named] += n;
    }
}

}

TEST(CASGCReadAhead, FoldIsIdenticalAtConcurrencyOneAndEight)
{
    constexpr size_t kRounds = 6;
    FoldRun one;
    FoldRun eight;
    ASSERT_NO_FATAL_FAILURE(runFolds(1, kRounds, one));
    ASSERT_NO_FATAL_FAILURE(runFolds(8, kRounds, eight));

    ASSERT_EQ(one.seals.size(), kRounds);
    ASSERT_EQ(eight.seals.size(), kRounds);
    for (size_t i = 0; i < kRounds; ++i)
        EXPECT_EQ(one.seals[i], eight.seals[i]) << "fold seal differs at round " << i;

    EXPECT_EQ(one.intake, eight.intake);
    EXPECT_EQ(one.reduce, eight.reduce);
    EXPECT_EQ(one.condemned, eight.condemned);
    EXPECT_EQ(one.deleted, eight.deleted);

    /// THE REQUEST-SET CLAIM. Every namespace here is healthy, so nothing is hinted that the walk
    /// does not go on to read: the hints stop at the checkpoint ceiling the walk stops at, a quiet
    /// namespace is not hinted at all, and every decoded log's manifest edges are all folded. So the
    /// read-ahead must issue the SAME GETs against the SAME keys, not merely produce the same answer.
    EXPECT_EQ(one.gets, eight.gets);

    ASSERT_FALSE(one.intake.empty());
    EXPECT_GT(one.intake[0].at("logs_applied"), 32u)
        << "the wide namespace must carry more logs than the window, or the lookahead is untested";
}

TEST(CASGCReadAhead, ReduceCondemnsTheSameBlobsWithTheSameHeadsAtConcurrencyOneAndEight)
{
    /// `populate` gives every part its own blob and drops whole parts, so a dropped blob loses its only
    /// edge and no surviving blob has a removal: the hinted set equals the set `head_blob` takes, and the
    /// per-key HEAD counts must match exactly rather than merely producing the same verdict.
    constexpr size_t kRounds = 6;
    FoldRun one;
    FoldRun eight;
    ASSERT_NO_FATAL_FAILURE(runFolds(1, kRounds, one));
    ASSERT_NO_FATAL_FAILURE(runFolds(8, kRounds, eight));

    EXPECT_EQ(one.condemned, eight.condemned);
    EXPECT_EQ(one.heads, eight.heads);

    uint64_t condemned_total = 0;
    for (const size_t n : one.condemned)
        condemned_total += n;
    EXPECT_GT(condemned_total, 0u) << "the scenario must condemn, or the reduce read-ahead is untested";
}

namespace
{

/// Throws once on the first read issued from a thread other than the one that armed it: exactly a
/// read-ahead worker's request, never the round thread's own.
class WorkerReadFaultBackend : public CountingBackend
{
public:
    void armAgainstOtherThreads()
    {
        owner = std::this_thread::get_id();
        armed.store(true);
    }

    bool fired() const { return !armed.load(); }

    std::optional<DB::Cas::Backend::Raw> read(const String & key, DB::Cas::TransportAccess & access) override
    {
        if (armed.load() && std::this_thread::get_id() != owner)
        {
            armed.store(false);
            throw std::runtime_error("injected worker read fault");
        }
        return CountingBackend::read(key, access);
    }

private:
    std::atomic<bool> armed{false};
    std::thread::id owner;
};

}

namespace
{

/// Proves OVERLAP, which no equality test can: it releases a read only once `k_overlap` reads are
/// inside the backend at the same time. If the fold's reads were still strictly one after another the
/// count could never reach two, so the round would block until the bounded wait expires and the flag
/// below would stay false. The wait is bounded and the last arrival wakes everyone, so nothing here can
/// hang the suite: a fold with no overlap finishes late, it does not finish never.
class OverlapWitnessBackend : public CountingBackend
{
public:
    explicit OverlapWitnessBackend(size_t k_overlap_) : k_overlap(k_overlap_) {}

    /// ARMED ONLY FOR THE ROUND. Holding reads is fatal to a WRITER: its checkpoint publication is a
    /// CAS with a bounded retry budget, and a latch on every read exhausts it long before the round
    /// under test ever starts.
    void arm() { armed.store(true); }

    bool sawOverlap() const { return saw_overlap.load(); }

    std::optional<DB::Cas::Backend::Raw> read(const String & key, DB::Cas::TransportAccess & access) override
    {
        if (!armed.load())
            return CountingBackend::read(key, access);
        {
            std::unique_lock lock(mutex);
            ++in_flight;
            peak = std::max(peak, in_flight);
            if (in_flight >= k_overlap)
            {
                saw_overlap.store(true);
                gate.notify_all();
            }
            else
            {
                gate.wait_for(lock, std::chrono::milliseconds(250),
                              [&] { return in_flight >= k_overlap || saw_overlap.load(); });
            }
        }
        /// The count stays raised ACROSS the read, so what it measures is requests genuinely in the
        /// backend together. Releasing it before the read would leave a window of a few instructions
        /// that two threads would have to hit simultaneously to be seen -- which is a measurement of
        /// luck, not of overlap.
        std::optional<DB::Cas::Backend::Raw> raw = CountingBackend::read(key, access);
        {
            std::lock_guard lock(mutex);
            --in_flight;
        }
        return raw;
    }

    size_t peakInFlight() const
    {
        std::lock_guard lock(mutex);
        return peak;
    }

private:
    const size_t k_overlap;
    std::atomic<bool> armed{false};
    mutable std::mutex mutex;
    std::condition_variable gate;
    size_t in_flight = 0;
    size_t peak = 0;
    std::atomic<bool> saw_overlap{false};
};

}

TEST(CASGCReadAhead, TheFoldsReadsActuallyOverlap)
{
    auto backend = std::make_shared<OverlapWitnessBackend>(/*k_overlap*/ 2);
    auto store = Pool::open(backend,
        PoolConfig{.pool_prefix = "p", .server_root_id = "test",
                   .gc_fold_max_defer_rounds = 0, .gc_read_concurrency = 8});
    populate(store);

    Gc gc(store, kGc);
    backend->arm();
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);

    EXPECT_TRUE(backend->sawOverlap())
        << "no two of the fold's reads were ever in the backend at the same time; peak in flight was "
        << backend->peakInFlight();
    EXPECT_GT(backend->peakInFlight(), 1u);
}

TEST(CASGCReadAhead, WorkerReadFaultFailsTheRoundAndTheNextRoundRecovers)
{
    auto backend = std::make_shared<WorkerReadFaultBackend>();
    auto store = Pool::open(backend,
        PoolConfig{.pool_prefix = "p", .server_root_id = "test",
                   .gc_fold_max_defer_rounds = 0, .gc_read_concurrency = 8});
    populate(store);

    Gc gc(store, kGc);
    backend->armAgainstOtherThreads();
    EXPECT_ANY_THROW(gc.runRegularRound());
    EXPECT_TRUE(backend->fired()) << "no read-ahead worker ever issued a request";

    store->renewWatermarkOnce();
    const RoundReport recovered = gc.runRegularRound();
    EXPECT_TRUE(recovered.acquired_lease);
}
