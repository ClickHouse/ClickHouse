#include <gtest/gtest.h>
#include <IO/ReadBufferFromString.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasBlobUploadPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasBlobEnvelopeFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasBlobMeta.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <base/scope_guard.h>
#include <Poco/Exception.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <latch>
#include <map>
#include <mutex>
#include <optional>
#include <span>
#include <thread>
#include <tuple>
#include <vector>

using namespace DB::Cas;
using DB::Cas::tests::idOf;
using DB::Cas::tests::u128Of;
using DB::Cas::tests::blobEntryFor;
using DB::Cas::tests::writeMetaClean;
using DB::Cas::tests::condemnMeta;
using DB::Cas::tests::loadMetaForTest;
using DB::Cas::tests::expectThrowsCode;
using DB::Cas::tests::runRoundsUntilAbsent;
using DB::Cas::tests::blobAbsent;
using DB::Cas::tests::CountingBackend;

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int INCORRECT_DATA;
extern const int NOT_IMPLEMENTED;
}

namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

namespace
{

/// A local upload pool of a chosen size. Task 5 takes the pool as a parameter (rather than reaching
/// for the server-wide `Cas::blobUploadPool()`) precisely so a test can run the SAME fan-out through a
/// size-1 pool (the serial reference) and a size-N pool (the fanned-out world) in ONE process -- the
/// server-wide pool is once-only per binary and cannot be re-sized. The calling thread only submits
/// and joins (it never occupies a pool slot), so size 1 is a valid fully-serial configuration.
std::unique_ptr<ThreadPool> makePool(size_t size)
{
    return std::make_unique<ThreadPool>(
        CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, size);
}

/// Open a Pool over any InMemoryBackend-derived backend (the plain one, or the CountingBackend that
/// records per-key GET counts).
PoolPtr openPool(const std::shared_ptr<InMemoryBackend> & b)
{
    return Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
}

/// Stage a one-blob seed manifest and precommit it, so every adopt branch of `uploadBlobDetached`
/// passes its EDGE-BEFORE-OBSERVE fail-closed gate (which only checks the `precommitted` flag). One
/// precommit covers an arbitrary number of subsequently-uploaded blobs, mirroring
/// `precommitBuildFor`/`MergeAppliesAllDeps` in the detached suite.
PartWriteTxnPtr precommitBuildFor(const PoolPtr & s, const RootNamespace & ns, const String & ref)
{
    PartWriteInfo info;
    info.intended_ref = ns.string() + "/" + ref;
    PartWriteTxnPtr build = s->beginPartWrite(std::move(info));
    const String seed = "seed-manifest-" + ns.string() + "/" + ref;
    const ManifestId id = build->stageManifest({blobEntryFor("col.bin", u128Of(seed), seed.size())});
    build->precommitAdd(ns, ref, id);
    return build;
}

/// Seed a present, well-formed blob body whose LOGICAL bytes are exactly `payload`.
void seedPresentBody(InMemoryBackend & b, const Layout & layout, const PoolMeta & pm, const String & payload)
{
    EnvelopeHeader h;
    h.kind = ObjectKind::Blob;
    h.incarnation_tag = DB::UInt128(0xABCD);
    h.build_id = DB::UInt128(0x1111);
    const String head = encodeEnvelopeHeader(h, static_cast<uint32_t>(pm.blob_header_len));
    b.putIfAbsent(layout.blobKey(idOf(payload)), head + payload);
}

/// The logical payload stored at a blob key (object body minus the fixed blob header), or empty when absent.
String logicalPayloadAt(InMemoryBackend & b, const String & key, uint64_t header_len)
{
    const auto got = b.get(key);
    if (!got || got->bytes.size() < header_len)
        return {};
    return got->bytes.substr(header_len);
}

/// The blob's meta state, or nullopt when the meta object is absent.
std::optional<MetaState> metaStateAt(InMemoryBackend & b, const Layout & layout, const String & payload)
{
    const auto lm = loadMetaForTest(b, layout, u128Of(payload));
    return lm ? std::optional<MetaState>(lm->meta.state) : std::nullopt;
}

/// A local streaming source for `payload`, exactly as `ContentAddressedTransaction::uploadPendingBlobs`
/// builds for a Local-staging pending blob.
BlobUploadRequest localRequest(const String & payload)
{
    return BlobUploadRequest{idOf(payload), BlobSource::fromString(payload), payload.size()};
}

/// An S3-staging source: the bytes already live at `staging_key` and the upload is a server-side copy.
BlobUploadRequest s3Request(const String & payload, const String & staging_key)
{
    BlobSource src;
    src.size = payload.size();
    src.server_side_copy_from = staging_key;
    src.open = [payload]() -> std::unique_ptr<DB::ReadBuffer>
    {
        return std::make_unique<DB::ReadBufferFromOwnString>(payload);
    };
    return BlobUploadRequest{idOf(payload), std::move(src), payload.size()};
}

/// The stable dependency state is independent of backend incarnation tokens: all successful upload
/// branches establish `Materialized`, regardless of serial or parallel token-mint ordering.
using StableDep = std::tuple<ObjectKind, uint64_t, BlobDependencyProof>;
std::map<BlobRef, StableDep> stableDeps(const PartWriteTxn & build)
{
    std::map<BlobRef, StableDep> out;
    for (const auto & [ref, dep] : build.depsSnapshotForTest())
        out.emplace(ref, StableDep{dep.kind, dep.size, dep.proof});
    return out;
}

/// The backend end state for a set of blob refs: (logical payload, meta state) per ref. Deterministic
/// (content is the payload; meta settles to Clean), so it is compared byte-for-byte across worlds.
using BackendState = std::map<BlobRef, std::pair<String, std::optional<MetaState>>>;
BackendState backendState(InMemoryBackend & b, const PoolPtr & s, const std::vector<String> & payloads)
{
    BackendState out;
    for (const auto & p : payloads)
        out.emplace(idOf(p),
            std::make_pair(logicalPayloadAt(b, s->layout().blobKey(idOf(p)), s->poolMeta().blob_header_len),
                           metaStateAt(b, s->layout(), p)));
    return out;
}

/// A one-shot event with a BOUNDED wait. Not a sleep-sequencer: the wait blocks only until the event
/// fires; the bound exists solely so a design regression surfaces as a fast test failure instead of an
/// infinite hang.
struct BoundedEvent
{
    std::mutex m;
    std::condition_variable cv;
    bool fired = false;
    void fire()
    {
        {
            std::lock_guard l(m);
            fired = true;
        }
        cv.notify_all();
    }
    bool wait(std::chrono::milliseconds bound)
    {
        std::unique_lock l(m);
        return cv.wait_for(l, bound, [&] { return fired; });
    }
};

/// Records the peak number of tasks simultaneously "inside" the rendezvous. A task calls `enter(want)`
/// from the fan-out's in-task seam; it blocks (BOUNDED) until `want` tasks are inside together, OR every
/// dispatched task has entered (so a final straggler is never stranded when the pool cannot form another
/// pair), OR the bound elapses. A pool that CANNOT muster `want` concurrent tasks (size 1, where THIS
/// task occupies the single worker) times out on the first waiter, marks the run serial, and every later
/// task skips the wait -- so a too-small pool fails FAST and the whole run stays bounded, never
/// deadlocked. `total` (the dispatched task count) is set before dispatch.
struct ConcurrencyProbe
{
    std::mutex m;
    std::condition_variable cv;
    int current = 0;
    int peak = 0;
    int entered = 0;
    int total = 0;
    bool timed_out = false;
    void enter(int want, std::chrono::milliseconds bound)
    {
        std::unique_lock l(m);
        ++current;
        ++entered;
        peak = std::max(peak, current);
        cv.notify_all();
        const bool ok = cv.wait_for(l, bound,
            [&] { return current >= want || entered == total || timed_out; });
        if (!ok)
            timed_out = true;   /// the pool cannot reach `want`; later tasks skip the wait
        cv.notify_all();
        --current;
    }
};

/// A deterministic native-copy rejection used to prove that the logical source's publication state
/// survives every request copy made by the fan-out. The first call must propagate; a later request
/// copied from the same source may only stream a newly tagged envelope, never retry verbatim copy.
class RejectFirstStagedCopyBackend final : public InMemoryBackend
{
public:
    void publishBlob(const BlobPublishRequest & request) override
    {
        if (std::holds_alternative<VerbatimStagedBlobPublication>(request.publication))
        {
            ++copy_publications;
            if (reject_copy)
            {
                reject_copy = false;
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "test rejects the first staged copy");
            }
        }
        else
        {
            ++streaming_publications;
        }
        InMemoryBackend::publishBlob(request);
    }

    bool reject_copy = true;
    size_t copy_publications = 0;
    size_t streaming_publications = 0;
};

}

TEST(CASUploadFanout, CopiedAndMovedRequestsSharePublicationAttemptedState)
{
    auto backend = std::make_shared<RejectFirstStagedCopyBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/shared-publication-state"};
    auto build = precommitBuildFor(store, ns, "part");
    const String payload = "shared-publication-attempted-payload";
    const BlobRef ref = idOf(payload);
    const String staging_key = "p/staging/mount1/shared-attempt.tmp";

    EnvelopeHeader header;
    header.kind = ObjectKind::Blob;
    header.incarnation_tag = DB::UInt128(0xC0FFEE);
    const String staging_bytes
        = encodeEnvelopeHeader(header, static_cast<uint32_t>(store->poolMeta().blob_header_len)) + payload;
    backend->putIfAbsent(staging_key, staging_bytes);

    BlobSource source;
    source.size = payload.size();
    source.server_side_copy_from = staging_key;
    source.open = [payload]() -> std::unique_ptr<DB::ReadBuffer>
    {
        return std::make_unique<DB::ReadBufferFromOwnString>(payload);
    };

    BlobUploadRequest original{ref, source, payload.size()};
    BlobUploadRequest first_copy = original;
    BlobUploadRequest fanout_copy = original;

    expectThrowsCode(DB::ErrorCodes::NOT_IMPLEMENTED, [&]
    {
        build->uploadBlobDetached(first_copy);
    });

    std::vector<BlobUploadRequest> requests;
    requests.emplace_back(std::move(fanout_copy));
    auto pool = makePool(1);
    fanOutBlobUploads(*build, requests, *pool);

    EXPECT_EQ(backend->copy_publications, 1u)
        << "only the source's first publication may attempt verbatim staged copy";
    EXPECT_EQ(backend->streaming_publications, 1u)
        << "the request copied and moved through fan-out must retain the consumed first-attempt state";
    EXPECT_EQ(build->dependencyProof(ref), BlobDependencyProof::Materialized);
    const auto stored = backend->get(store->layout().blobKey(ref));
    ASSERT_TRUE(stored.has_value());
    EXPECT_EQ(stored->bytes.substr(store->poolMeta().blob_header_len), payload);
}

/// Test 1 (spec §1 "serial-vs-parallel equivalence for successful runs"): a multi-blob part that
/// exercises every branch of `uploadBlobDetached` produces IDENTICAL recorded deps and IDENTICAL backend
/// end state whether the fan-out runs serially (pool size 1) or in parallel (pool size 4). It covers
/// present-clean observation, metadata backfill, fresh local publication, staging copy, and local and
/// staged condemned-body republication.
namespace
{

/// Arrange the six-branch world and return the payloads it uploads. Every branch
/// is seeded on a DISTINCT ref so the one-task-per-unique-ref fan-out runs six independent tasks.
struct WorldA
{
    std::shared_ptr<InMemoryBackend> b;
    PoolPtr s;
    PartWriteTxnPtr build;
    std::vector<BlobUploadRequest> requests;
    std::vector<String> payloads;
};

const char * const kObserved = "fanoutA-observed-clean";
const char * const kAdopt = "fanoutA-head-miss-adopt";
const char * const kFresh = "fanoutA-fresh-local";
const char * const kStaging = "fanoutA-s3-staging";
const char * const kResLocal = "fanoutA-condemned-local";
const char * const kResS3 = "fanoutA-condemned-s3";

WorldA arrangeWorldA()
{
    WorldA w;
    w.b = std::make_shared<InMemoryBackend>();
    w.s = openPool(w.b);
    const RootNamespace ns{"srv1/nsFanoutA"};
    w.build = precommitBuildFor(w.s, ns, "part");

    /// Present body with `Clean` metadata: safe observation avoids publication.
    seedPresentBody(*w.b, w.s->layout(), w.s->poolMeta(), kObserved);
    writeMetaClean(*w.b, w.s->layout(), u128Of(kObserved), std::string(kObserved).size());

    /// HEAD-miss then 412-path live adopt with meta backfill: present body, NO meta, not cached.
    seedPresentBody(*w.b, w.s->layout(), w.s->poolMeta(), kAdopt);

    /// fresh local streaming: nothing present.

    /// S3-native staging promotion: bytes live in a staging object, blob key absent.
    {
        EnvelopeHeader h;
        h.kind = ObjectKind::Blob;
        h.incarnation_tag = DB::UInt128(0xC0FFEE);
        const String staging = encodeEnvelopeHeader(h, static_cast<uint32_t>(w.s->poolMeta().blob_header_len)) + kStaging;
        w.b->putIfAbsent("p/staging/mount1/A-staging.tmp", staging);
    }

    /// condemned-local resurrection: present body + condemned meta, local source.
    seedPresentBody(*w.b, w.s->layout(), w.s->poolMeta(), kResLocal);
    writeMetaClean(*w.b, w.s->layout(), u128Of(kResLocal), std::string(kResLocal).size());
    condemnMeta(*w.b, w.s->layout(), u128Of(kResLocal), /*condemn_round=*/7);

    /// condemned-S3 resurrection: present body (= a verbatim promote of the staging object) + condemned
    /// meta, S3 staging source.
    {
        EnvelopeHeader h;
        h.kind = ObjectKind::Blob;
        h.incarnation_tag = DB::UInt128(0xC0FFEE);
        const String staging = encodeEnvelopeHeader(h, static_cast<uint32_t>(w.s->poolMeta().blob_header_len)) + kResS3;
        w.b->putIfAbsent("p/staging/mount1/A-republish.tmp", staging);
        w.b->putIfAbsent(w.s->layout().blobKey(idOf(kResS3)), staging);
        writeMetaClean(*w.b, w.s->layout(), u128Of(kResS3), std::string(kResS3).size());
        condemnMeta(*w.b, w.s->layout(), u128Of(kResS3), /*condemn_round=*/9);
    }

    w.requests = {
        localRequest(kObserved),
        localRequest(kAdopt),
        localRequest(kFresh),
        s3Request(kStaging, "p/staging/mount1/A-staging.tmp"),
        localRequest(kResLocal),
        s3Request(kResS3, "p/staging/mount1/A-republish.tmp"),
    };
    w.payloads = {kObserved, kAdopt, kFresh, kStaging, kResLocal, kResS3};
    return w;
}

}

TEST(CASUploadFanout, DependencyProofEquivalentAcrossFanoutBranches)
{
    /// Serial reference: pool size 1.
    WorldA serial = arrangeWorldA();
    auto serial_pool = makePool(1);
    fanOutBlobUploads(*serial.build, serial.requests, *serial_pool);
    const auto serial_deps = stableDeps(*serial.build);
    const auto serial_backend = backendState(*serial.b, serial.s, serial.payloads);

    /// Fanned-out: pool size 4, same inputs, freshly arranged world.
    WorldA fanned = arrangeWorldA();
    auto fanned_pool = makePool(4);
    fanOutBlobUploads(*fanned.build, fanned.requests, *fanned_pool);
    const auto fanned_deps = stableDeps(*fanned.build);
    const auto fanned_backend = backendState(*fanned.b, fanned.s, fanned.payloads);

    EXPECT_EQ(serial_deps.size(), 6u) << "one dep per unique ref";
    EXPECT_EQ(serial_deps, fanned_deps) << "recorded deps must match across serial and fanned runs";
    EXPECT_EQ(serial_backend, fanned_backend) << "backend end state must match across serial and fanned runs";

    /// Every successful upload branch records materialized evidence only after the fan-out joins.
    for (const auto & [ref, dep] : serial_deps)
    {
        EXPECT_EQ(std::get<0>(dep), ObjectKind::Blob);
        EXPECT_EQ(std::get<2>(dep), BlobDependencyProof::Materialized);
    }
    for (const auto & p : serial.payloads)
        EXPECT_EQ(metaStateAt(*fanned.b, fanned.s->layout(), p), std::optional<MetaState>(MetaState::Clean));

}

/// Test 1, GET-observability (routed from T3 review (a)): the republication invariant is that a condemned
/// object is NEVER GET (revival is a fresh re-upload from the writer's own source). With a
/// CountingBackend, assert ZERO get/getStream against the condemned blob keys through the whole fan-out.
TEST(CASUploadFanout, CondemnedBranchesNeverGet)
{
    auto counting = std::make_shared<CountingBackend>();
    auto s = openPool(counting);
    const RootNamespace ns{"srv1/nsNoGet"};
    auto build = precommitBuildFor(s, ns, "part");

    const String local_payload = "noget-condemned-local";
    const String s3_payload = "noget-condemned-s3";
    const String s3_staging = "p/staging/mount1/noget-republish.tmp";

    seedPresentBody(*counting, s->layout(), s->poolMeta(), local_payload);
    writeMetaClean(*counting, s->layout(), u128Of(local_payload), local_payload.size());
    condemnMeta(*counting, s->layout(), u128Of(local_payload), /*condemn_round=*/3);

    {
        EnvelopeHeader h;
        h.kind = ObjectKind::Blob;
        h.incarnation_tag = DB::UInt128(0xC0FFEE);
        const String staging = encodeEnvelopeHeader(h, static_cast<uint32_t>(s->poolMeta().blob_header_len)) + s3_payload;
        counting->putIfAbsent(s3_staging, staging);
        counting->putIfAbsent(s->layout().blobKey(idOf(s3_payload)), staging);
        writeMetaClean(*counting, s->layout(), u128Of(s3_payload), s3_payload.size());
        condemnMeta(*counting, s->layout(), u128Of(s3_payload), /*condemn_round=*/5);
    }

    counting->resetCounts();   /// count only the fan-out's own backend traffic

    std::vector<BlobUploadRequest> reqs{localRequest(local_payload), s3Request(s3_payload, s3_staging)};
    auto pool = makePool(2);
    fanOutBlobUploads(*build, reqs, *pool);

    /// INV-1: revival is a fresh re-upload from the writer's own source; the condemned BODY object is
    /// never read. (The fan-out DOES read the two condemned-META objects -- the meta point-read is how it
    /// LEARNS an incarnation is condemned -- so the invariant is per-body-key, not a global GET count.)
    const String local_key = s->layout().blobKey(idOf(local_payload));
    const String s3_key = s->layout().blobKey(idOf(s3_payload));
    EXPECT_EQ(counting->getCount(local_key), 0u) << "INV-1: the condemned local body is never read";
    EXPECT_EQ(counting->getStreamCount(local_key), 0u) << "INV-1: the condemned local body is never streamed";
    EXPECT_EQ(counting->getCount(s3_key), 0u) << "INV-1: the condemned S3 body is never read";
    EXPECT_EQ(counting->getStreamCount(s3_key), 0u) << "INV-1: the condemned S3 body is never streamed";

    /// Both resurrections still completed to Clean.
    EXPECT_EQ(metaStateAt(*counting, s->layout(), local_payload), std::optional<MetaState>(MetaState::Clean));
    EXPECT_EQ(metaStateAt(*counting, s->layout(), s3_payload), std::optional<MetaState>(MetaState::Clean));
}

/// Test 2: duplicate refs (staged-hardlink copies push a duplicate PendingBlob record) collapse to ONE
/// task, and the merged build records exactly one dep for the ref.
TEST(CASUploadFanout, DuplicateRefsLaunchOneTask)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/nsDup"};
    auto build = precommitBuildFor(s, ns, "part");

    const String payload = "dup-fresh-payload";
    std::vector<BlobUploadRequest> reqs{localRequest(payload), localRequest(payload)};   /// same ref twice

    std::atomic<int> dispatched{0};
    std::map<BlobRef, int> per_ref;
    std::mutex per_ref_m;
    BlobUploadFanoutHooksForTest hooks;
    hooks.on_dispatch = [&](const BlobRef & ref)
    {
        ++dispatched;
        std::lock_guard l(per_ref_m);
        ++per_ref[ref];
    };

    auto pool = makePool(4);
    fanOutBlobUploads(*build, reqs, *pool, &hooks);

    EXPECT_EQ(dispatched.load(), 1) << "two pending-blob records for one ref launch exactly one task";
    EXPECT_EQ(per_ref[idOf(payload)], 1);
    EXPECT_EQ(build->dependencyProof(idOf(payload)), BlobDependencyProof::Materialized)
        << "the one task's dep was merged";
    EXPECT_EQ(build->depsSnapshotForTest().size(), 1u) << "exactly one dep for the unique ref";
}

/// Test 2, conflicting-size backstop: two records for the SAME ref with different declared sizes are a
/// staging bug -- rejected with LOGICAL_ERROR before any task runs. LOGICAL_ERROR aborts under
/// debug/sanitizer builds, so the abort is proven positively there (DeathTest) and the exception +
/// build-untouched postcondition in a release build.
#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(CASUploadFanout, ConflictingDuplicateSizesRejected)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/nsDupConflict"};
    auto build = precommitBuildFor(s, ns, "part");

    const String payload = "dup-conflict-payload";
    BlobUploadRequest a = localRequest(payload);
    BlobUploadRequest c = localRequest(payload);
    c.declared_size = a.declared_size + 1;   /// same ref, conflicting declared size
    c.source.size = c.declared_size;         /// keep declared == source so only the group conflict trips

    const auto before = build->depsSnapshotForTest();
    auto pool = makePool(4);
    expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR, [&]
    {
        fanOutBlobUploads(*build, std::vector<BlobUploadRequest>{a, c}, *pool);
    });
    EXPECT_EQ(build->depsSnapshotForTest(), before) << "a rejected fan-out merges nothing";
}
#endif

#if defined(DEBUG_OR_SANITIZER_BUILD)
TEST(CASUploadFanoutDeathTest, ConflictingDuplicateSizesAbort)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/nsDupConflict"};
    auto build = precommitBuildFor(s, ns, "part");

    const String payload = "dup-conflict-payload";
    BlobUploadRequest a = localRequest(payload);
    BlobUploadRequest c = localRequest(payload);
    c.declared_size = a.declared_size + 1;
    c.source.size = c.declared_size;

    auto pool = makePool(4);
    EXPECT_DEATH({ fanOutBlobUploads(*build, std::vector<BlobUploadRequest>{a, c}, *pool); }, "");
}
#endif

/// Test 2, declared_size == source.size fail-close (routed from T3 review (b)): a request whose grouping
/// key (declared_size) disagrees with its streaming authority (source.size) is a wiring bug -- rejected
/// with LOGICAL_ERROR before dispatch (DeathTest split for debug/sanitizer builds).
#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(CASUploadFanout, DeclaredSizeMustMatchSourceSize)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/nsDeclared"};
    auto build = precommitBuildFor(s, ns, "part");

    BlobUploadRequest r = localRequest("declared-mismatch-payload");
    r.declared_size = r.source.size + 7;   /// diverge the grouping key from the streaming authority

    auto pool = makePool(2);
    expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR, [&]
    {
        fanOutBlobUploads(*build, std::vector<BlobUploadRequest>{r}, *pool);
    });
}
#endif

#if defined(DEBUG_OR_SANITIZER_BUILD)
TEST(CASUploadFanoutDeathTest, DeclaredSizeMismatchAborts)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/nsDeclared"};
    auto build = precommitBuildFor(s, ns, "part");

    BlobUploadRequest r = localRequest("declared-mismatch-payload");
    r.declared_size = r.source.size + 7;

    auto pool = makePool(2);
    EXPECT_DEATH({ fanOutBlobUploads(*build, std::vector<BlobUploadRequest>{r}, *pool); }, "");
}
#endif

/// The condemned-LOCAL displacement, end to end on the new unconditional streaming shape: the
/// resurrected body is [fresh_header][payload], its token differs from the condemned one, and the
/// meta flips back to Clean -- which is exactly what a later attempt reads to adopt instead of
/// re-writing.
TEST(CASUploadFanout, CondemnedLocalResurrectStreamsAndFlipsMetaClean)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/nsResLocalStream"};
    auto build = precommitBuildFor(s, ns, "part");

    const String payload = "condemned-local-streamed-payload";
    seedPresentBody(*b, s->layout(), s->poolMeta(), payload);
    writeMetaClean(*b, s->layout(), u128Of(payload), payload.size());
    condemnMeta(*b, s->layout(), u128Of(payload), /*condemn_round=*/13);

    const String blob_key = s->layout().blobKey(idOf(payload));
    const Token condemned_token = b->head(blob_key).token;

    std::vector<BlobUploadRequest> reqs{localRequest(payload)};
    auto pool = makePool(2);
    fanOutBlobUploads(*build, reqs, *pool, nullptr);

    /// A fresh incarnation displaced the condemned one; INV-NO-RETURN: the queued exact-token delete
    /// of the condemned incarnation must miss the resurrection.
    const HeadResult after = b->head(blob_key);
    ASSERT_TRUE(after.exists);
    EXPECT_NE(after.token, condemned_token);
    EXPECT_EQ(b->deleteExact(blob_key, condemned_token).kind, DeleteOutcome::Kind::TokenMismatch);
    EXPECT_TRUE(b->head(blob_key).exists);

    /// The payload survived verbatim under the fresh header.
    const auto got = b->get(blob_key);
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->bytes.substr(s->poolMeta().blob_header_len), payload);

    /// The meta flipped back to Clean -- the signal a later attempt adopts on.
    const auto lm = loadMetaForTest(*b, s->layout(), u128Of(payload));
    ASSERT_TRUE(lm.has_value());
    EXPECT_EQ(lm->meta.state, MetaState::Clean);
}

/// `open` is the per-publication unit of re-readability. A present `Condemned` observation selects
/// exactly one unconditional stream; the mandatory `HEAD` itself never opens the source.
TEST(CASUploadFanout, CondemnedLocalPublicationOpensSourceOnce)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/nsResLocalOpens"};
    auto build = precommitBuildFor(s, ns, "part");

    const String payload = "condemned-local-open-count-payload";
    seedPresentBody(*b, s->layout(), s->poolMeta(), payload);
    writeMetaClean(*b, s->layout(), u128Of(payload), payload.size());
    condemnMeta(*b, s->layout(), u128Of(payload), /*condemn_round=*/17);

    int opens = 0;
    BlobSource source;
    source.size = payload.size();
    source.open = [&opens, payload]() -> std::unique_ptr<DB::ReadBuffer>
    {
        ++opens;
        return std::make_unique<DB::ReadBufferFromOwnString>(payload);
    };

    std::vector<BlobUploadRequest> reqs{BlobUploadRequest{idOf(payload), std::move(source), payload.size()}};
    auto pool = makePool(2);
    fanOutBlobUploads(*build, reqs, *pool, nullptr);

    EXPECT_EQ(opens, 1) << "the mandatory `HEAD` selects one unconditional streaming publication";
    EXPECT_EQ(build->dependencyProof(idOf(payload)), BlobDependencyProof::Materialized);
}

/// Test 2, condemned-S3 duplicate pair resurrects content-correctly: two duplicate S3-staging records
/// for one condemned ref collapse to ONE republication task; the fresh incarnation displaces the condemned
/// one (token changes, meta returns to Clean) and the content is the staging object's payload.
TEST(CASUploadFanout, DuplicateCondemnedS3ResurrectsCorrectly)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/nsDupResS3"};
    auto build = precommitBuildFor(s, ns, "part");

    const String payload = "dup-condemned-s3-payload";
    const String staging_key = "p/staging/mount1/dup-republish.tmp";
    EnvelopeHeader h;
    h.kind = ObjectKind::Blob;
    h.incarnation_tag = DB::UInt128(0xC0FFEE);
    const String staging_bytes = encodeEnvelopeHeader(h, static_cast<uint32_t>(s->poolMeta().blob_header_len)) + payload;
    b->putIfAbsent(staging_key, staging_bytes);
    b->putIfAbsent(s->layout().blobKey(idOf(payload)), staging_bytes);
    writeMetaClean(*b, s->layout(), u128Of(payload), payload.size());
    condemnMeta(*b, s->layout(), u128Of(payload), /*condemn_round=*/11);

    const Token condemned_token = b->head(s->layout().blobKey(idOf(payload))).token;

    std::atomic<int> dispatched{0};
    BlobUploadFanoutHooksForTest hooks;
    hooks.on_dispatch = [&](const BlobRef &) { ++dispatched; };

    std::vector<BlobUploadRequest> reqs{s3Request(payload, staging_key), s3Request(payload, staging_key)};
    auto pool = makePool(4);
    fanOutBlobUploads(*build, reqs, *pool, &hooks);

    EXPECT_EQ(dispatched.load(), 1) << "duplicate condemned records collapse to one republication task";
    EXPECT_EQ(build->dependencyProof(idOf(payload)), BlobDependencyProof::Materialized);
    const Token after_token = b->head(s->layout().blobKey(idOf(payload))).token;
    EXPECT_NE(after_token.value, condemned_token.value) << "a fresh incarnation displaced the condemned one";
    EXPECT_EQ(metaStateAt(*b, s->layout(), payload), std::optional<MetaState>(MetaState::Clean));
    EXPECT_EQ(logicalPayloadAt(*b, s->layout().blobKey(idOf(payload)), s->poolMeta().blob_header_len), payload);
}

/// Test 3: one task fails (a poisoned source), one sibling succeeds. Merge-nothing means the build stays
/// at its pre-fan-out state; the abandoned precommit turns the successful sibling's uploaded body into
/// ORDINARY GC-reclaimable debris (NOT a new orphan class) -- a GC round reclaims it.
TEST(CASUploadFanout, PendingFanoutFailureCreatesNoDependency)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/nsMergeNothing"};

    const String good = "merge-nothing-good-sibling";
    const String poisoned = "merge-nothing-poisoned";

    PartWriteInfo info;
    info.intended_ref = ns.string() + "/part";
    auto build = s->beginPartWrite(info);
    /// The precommit names BOTH blobs (the durable manifest edge the real writer establishes before any
    /// upload), so the successful sibling's body is edge-protected until the precommit is abandoned.
    const ManifestId id = build->stageManifest({blobEntryFor("data.bin", u128Of(good), good.size()),
                                                blobEntryFor("data.cmrk3", u128Of(poisoned), poisoned.size())});
    build->precommitAdd(ns, "part", id);
    EXPECT_EQ(build->dependencyProof(idOf(good)), std::nullopt);
    EXPECT_EQ(build->dependencyProof(idOf(poisoned)), std::nullopt);

    /// Poison the failing sibling via the in-task seam: throw a plain (non-LOGICAL, non-ABORTED)
    /// exception so it is neither retried nor an abort under sanitizer builds.
    BlobUploadFanoutHooksForTest hooks;
    hooks.in_task = [&](const BlobRef & ref)
    {
        if (ref == idOf(poisoned))
            throw DB::Exception(DB::ErrorCodes::INCORRECT_DATA, "poisoned upload source (test)");
    };

    std::vector<BlobUploadRequest> reqs{localRequest(good), localRequest(poisoned)};
    auto pool = makePool(4);
    expectThrowsCode(DB::ErrorCodes::INCORRECT_DATA, [&]
    {
        fanOutBlobUploads(*build, reqs, *pool, &hooks);
    });

    /// Merge-nothing: the build recorded NO dep, even though the good sibling's body was uploaded.
    EXPECT_EQ(build->dependencyProof(idOf(good)), std::nullopt);
    EXPECT_EQ(build->dependencyProof(idOf(poisoned)), std::nullopt);
    EXPECT_EQ(build->depsSnapshotForTest().size(), 0u);

    /// Abandon the precommit (the existing failure path), then GC reclaims the orphaned sibling body.
    build->abandon();
    s->renewWatermarkOnce();
    Gc gc(s, DB::Cas::hexToU128("00000000000000000000000000000001"));
    EXPECT_TRUE(runRoundsUntilAbsent(s, gc, *b, s->layout(), u128Of(good)))
        << "the successful sibling's body is ordinary GC-reclaimable debris after abandon";
    EXPECT_TRUE(blobAbsent(*b, s->layout(), u128Of(poisoned))) << "the poisoned sibling never uploaded a body";
}

/// Two distinct refs publish concurrently and establish materialized dependencies.
TEST(CASUploadFanout, ConcurrentPublicationsEstablishProof)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/nsPublicationRace"};
    auto build = precommitBuildFor(s, ns, "part");

    const String pa = "publication-race-a";
    const String pb = "publication-race-b";

    /// A latch of 2 crossed with a pool of 2 cannot hang: both tasks are guaranteed to run concurrently
    /// (the calling thread never occupies a pool slot), so both reach the latch and release together.
    std::latch both_in{2};
    BlobUploadFanoutHooksForTest hooks;
    hooks.in_task = [&](const BlobRef &) { both_in.arrive_and_wait(); };

    std::vector<BlobUploadRequest> reqs{localRequest(pa), localRequest(pb)};
    auto pool = makePool(2);
    fanOutBlobUploads(*build, reqs, *pool, &hooks);

    EXPECT_EQ(build->dependencyProof(idOf(pa)), BlobDependencyProof::Materialized);
    EXPECT_EQ(build->dependencyProof(idOf(pb)), BlobDependencyProof::Materialized);
}

/// Test 5: pool saturation is bounded. Eight blobs run through a pool of 2 (peak concurrency 2 is
/// observed) and a pool of 1 (peak concurrency 1 -- the single worker cannot self-wait for a second
/// concurrent task, so it FAILS FAST via the bounded wait). Both configurations complete every upload:
/// pool size 1 correctly degenerates to serial without deadlock.
TEST(CASUploadFanout, PoolSaturationBounded)
{
    constexpr int kBlobs = 8;
    /// Bounds are DECOUPLED by pool size. Pool 2 will reach the 2-task rendezvous in microseconds under
    /// any realistic load, so its bound is generous (10s) purely as a hang guard -- it is essentially
    /// never waited out (and `entered == total` releases any final straggler). Pool 1 CANNOT form a pair
    /// (its single worker is occupied by the waiting task while the caller thread only joins), so its
    /// first waiter must time out; 500ms is far above the microseconds a real pair needs, yet keeps the
    /// serial run fast.
    auto runEight = [](size_t pool_size, ConcurrencyProbe & probe, std::chrono::milliseconds bound)
    {
        auto b = std::make_shared<InMemoryBackend>();
        auto s = openPool(b);
        const RootNamespace ns{"srv1/nsSaturate"};
        auto build = precommitBuildFor(s, ns, "part");

        std::vector<BlobUploadRequest> reqs;
        std::vector<String> payloads;
        for (int i = 0; i < kBlobs; ++i)
        {
            payloads.push_back("saturate-payload-" + std::to_string(i));
            reqs.push_back(localRequest(payloads.back()));
        }
        probe.total = kBlobs;

        BlobUploadFanoutHooksForTest hooks;
        hooks.in_task = [&, bound](const BlobRef &) { probe.enter(2, bound); };

        auto pool = makePool(pool_size);
        fanOutBlobUploads(*build, reqs, *pool, &hooks);

        for (const auto & p : payloads)
            EXPECT_EQ(build->dependencyProof(idOf(p)), BlobDependencyProof::Materialized)
                << "every blob uploaded (pool_size=" << pool_size << ")";
    };

    ConcurrencyProbe probe2;
    runEight(2, probe2, std::chrono::seconds(10));
    EXPECT_EQ(probe2.peak, 2) << "pool of 2 runs two blob uploads concurrently";
    EXPECT_FALSE(probe2.timed_out) << "pool of 2 forms a pair without hitting the bound";

    ConcurrencyProbe probe1;
    runEight(1, probe1, std::chrono::milliseconds(500));
    EXPECT_EQ(probe1.peak, 1) << "pool of 1 degenerates to serial (never occupies the caller thread's slot)";
    EXPECT_TRUE(probe1.timed_out) << "the single worker fails fast on the bounded wait instead of hanging";
}

/// Test 6a: even when one task fails immediately, the join drains EVERY task before the failure surfaces.
/// A failing task counts down an event and throws; a sibling waits for that event, then uploads. The
/// fan-out rethrows only after the join, so the sibling's body is present in the backend by the time the
/// caller observes the failure.
TEST(CASUploadFanout, DrainPrecedesUnwind)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/nsDrain"};
    auto build = precommitBuildFor(s, ns, "part");

    const String failing = "drain-failing";
    const String slow = "drain-slow-sibling";

    BoundedEvent failing_threw;
    BlobUploadFanoutHooksForTest hooks;
    hooks.in_task = [&](const BlobRef & ref)
    {
        if (ref == idOf(failing))
        {
            failing_threw.fire();
            throw DB::Exception(DB::ErrorCodes::INCORRECT_DATA, "drain-test failing task");
        }
        else
        {
            /// 5-second bound: the failing task fires the event in microseconds; the bound only guards
            /// against a hang if the failing task never runs (a design regression).
            (void)failing_threw.wait(std::chrono::seconds(5));
        }
    };

    std::vector<BlobUploadRequest> reqs{localRequest(failing), localRequest(slow)};
    auto pool = makePool(2);
    expectThrowsCode(DB::ErrorCodes::INCORRECT_DATA, [&]
    {
        fanOutBlobUploads(*build, reqs, *pool, &hooks);
    });

    EXPECT_TRUE(b->head(s->layout().blobKey(idOf(slow))).exists)
        << "the sibling's upload was drained by the join before the failure surfaced";
    EXPECT_EQ(build->dependencyProof(idOf(slow)), std::nullopt)
        << "merge-nothing: the drained sibling's dep is not merged";
}

/// Test 6b: a throw injected DURING the dispatch loop (before all tasks are enqueued) still drains the
/// tasks already scheduled -- the fan-out drains every already-scheduled task on the unwinding path
/// before the captured storage is destroyed (the B90 lesson). The first task's body is present after the
/// dispatch throw is caught.
///
/// The throw is GATED on the first task actually entering its body: the runner marks tasks that are
/// still SCHEDULED as CANCELLED during unwind and skips waiting for a cancelled task, so a throw fired
/// before the first task's body ran could cancel it and leave its body ABSENT -- a real flakiness the
/// gate removes. Once the first task's `in_task` hook has fired, that task is past SCHEDULED (RUNNING),
/// so it can no longer be cancelled and the drain deterministically waits for its upload.
TEST(CASUploadFanout, DispatchThrowStillDrains)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/nsDispatchThrow"};
    auto build = precommitBuildFor(s, ns, "part");

    const String first = "dispatch-throw-first";
    const String second = "dispatch-throw-second";
    /// Dispatch runs in ascending-ref order, so the SMALLER-ref payload is the one enqueued before the
    /// second dispatch throws.
    const String enqueued = (idOf(first) < idOf(second)) ? first : second;

    BoundedEvent first_task_running;
    std::atomic<int> dispatch_calls{0};
    BlobUploadFanoutHooksForTest hooks;
    hooks.in_task = [&](const BlobRef & ref)
    {
        if (ref == idOf(enqueued))
            first_task_running.fire();
    };
    hooks.on_dispatch = [&](const BlobRef &)
    {
        if (++dispatch_calls == 2)
        {
            /// Wait until the first task's body has entered before throwing, so it is RUNNING (not
            /// SCHEDULED) and the unwind cannot cancel it. Pool size 2 guarantees the first task gets a
            /// worker while this (dispatch) thread waits; 10s is a pure hang guard, never a sequencer.
            EXPECT_TRUE(first_task_running.wait(std::chrono::seconds(10)))
                << "the first dispatched task must reach its body before the dispatch throw";
            throw DB::Exception(DB::ErrorCodes::INCORRECT_DATA, "dispatch-loop throw (test)");
        }
    };

    std::vector<BlobUploadRequest> reqs{localRequest(first), localRequest(second)};
    auto pool = makePool(2);
    expectThrowsCode(DB::ErrorCodes::INCORRECT_DATA, [&]
    {
        fanOutBlobUploads(*build, reqs, *pool, &hooks);
    });

    EXPECT_EQ(dispatch_calls.load(), 2) << "the throw fired on the second dispatch";
    /// The already-RUNNING first task was drained before the stack unwound, so its body is present
    /// although nothing was merged.
    EXPECT_TRUE(b->head(s->layout().blobKey(idOf(enqueued))).exists)
        << "the already-dispatched task was drained before the stack unwound";
    EXPECT_EQ(build->depsSnapshotForTest().size(), 0u) << "merge-nothing on a dispatch throw";
}




/// Test 6c (codex stage-1 review, Critical): a throw at the TRACKING-PUBLICATION seam still drains every
/// already-scheduled task before the captured `results` storage is destroyed. In the broken form a task
/// could be scheduled-but-untracked at the throw and run later against freed `results` (a
/// heap-use-after-free); the fix schedules-and-tracks in ONE no-throw step (pre-reserved handle vector)
/// and joins via a scope-exit drain guard, so a seam throw finds every scheduled task already tracked and
/// drains it. The throw is gated on the first task RUNNING (same reason as `DispatchThrowStillDrains`) so
/// its drain is deterministic; under ASan this run is UAF-clean -- the regression signature of a
/// scheduled-but-untracked task is a heap-use-after-free on `results` here.
TEST(CASUploadFanout, TrackingSeamThrowStillDrains)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/nsTrackSeam"};
    auto build = precommitBuildFor(s, ns, "part");

    const String first = "track-seam-first";
    const String second = "track-seam-second";
    /// Dispatch is ascending-ref, so the smaller ref is enqueued first.
    const String smaller = (idOf(first) < idOf(second)) ? first : second;

    BoundedEvent first_task_running;
    std::atomic<int> enqueue_calls{0};
    BlobUploadFanoutHooksForTest hooks;
    hooks.in_task = [&](const BlobRef & ref)
    {
        if (ref == idOf(smaller))
            first_task_running.fire();
    };
    hooks.after_enqueue = [&](const BlobRef &)
    {
        /// Throw at the tracking seam of the SECOND enqueue -- by then both tasks are scheduled and (in
        /// the fixed code) tracked, so the drain guard must join both. Gate on the first task RUNNING so
        /// the drain cannot race a still-SCHEDULED cancellation; 10s is a pure hang guard.
        if (++enqueue_calls == 2)
        {
            EXPECT_TRUE(first_task_running.wait(std::chrono::seconds(10)))
                << "the first task must be RUNNING before the tracking-seam throw";
            throw DB::Exception(DB::ErrorCodes::INCORRECT_DATA, "tracking-seam throw (test)");
        }
    };

    std::vector<BlobUploadRequest> reqs{localRequest(first), localRequest(second)};
    auto pool = makePool(2);
    expectThrowsCode(DB::ErrorCodes::INCORRECT_DATA, [&]
    {
        fanOutBlobUploads(*build, reqs, *pool, &hooks);
    });

    /// The already-scheduled first task was drained before `results` was destroyed, so its body is
    /// present; nothing was merged (merge-nothing on any fan-out throw).
    EXPECT_TRUE(b->head(s->layout().blobKey(idOf(smaller))).exists)
        << "an already-scheduled task was not drained before the stack unwound";
    EXPECT_EQ(build->depsSnapshotForTest().size(), 0u) << "merge-nothing on a tracking-seam throw";
}
