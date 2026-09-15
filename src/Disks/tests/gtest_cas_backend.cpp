#include <gtest/gtest.h>

#include "config.h"

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInstrumentedBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasObjectStorageBackend.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/ProfileEvents.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteSettings.h>
#include <base/defines.h>
#include <base/scope_guard.h>

#include <chrono>
#include <atomic>
#include <future>
#include <tuple>
#include <type_traits>

#if USE_AWS_S3
#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Disks/WriteMode.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/S3Common.h>
#include <chrono>
#include <filesystem>
#include <map>
#include <mutex>
#endif

using namespace DB::Cas;

using DB::Cas::tests::expectBytes;
using DB::Cas::tests::openRequestsForTest;
using DB::Cas::tests::OperationForTest;

namespace DB::ErrorCodes
{
extern const int CAS_DELETE_MARKER;
extern const int CORRUPTED_DATA;
extern const int NOT_IMPLEMENTED;
extern const int LOGICAL_ERROR;
}

namespace
{

BlobPublishRequest streamingPublication(
    String destination_key, String fresh_envelope, String payload, uint64_t payload_size)
{
    return BlobPublishRequest{
        .destination_key = std::move(destination_key),
        .publication = StreamingBlobPublication{
            .payload_size = payload_size,
            .fresh_envelope = std::move(fresh_envelope),
            .open_payload = [stored_payload = std::move(payload)]
            {
                return std::make_unique<DB::ReadBufferFromOwnString>(stored_payload);
            }}};
}

struct CountingSourceState
{
    size_t bytes_exposed = 0;
};

class OneByteAtATimeReadBuffer final : public DB::ReadBuffer
{
public:
    OneByteAtATimeReadBuffer(size_t total_bytes_, std::shared_ptr<CountingSourceState> state_)
        : DB::ReadBuffer(nullptr, 0)
        , total_bytes(total_bytes_)
        , state(std::move(state_))
    {
    }

private:
    bool nextImpl() override
    {
        if (state->bytes_exposed == total_bytes)
            return false;

        ++state->bytes_exposed;
        working_buffer = Buffer(&byte, &byte + 1);
        return true;
    }

    const size_t total_bytes;
    const std::shared_ptr<CountingSourceState> state;
    char byte = 'x';
};

BlobPublishRequest countedLongPublication(
    String destination_key,
    String fresh_envelope,
    uint64_t payload_size,
    size_t source_size,
    const std::shared_ptr<CountingSourceState> & state)
{
    return BlobPublishRequest{
        .destination_key = std::move(destination_key),
        .publication = StreamingBlobPublication{
            .payload_size = payload_size,
            .fresh_envelope = std::move(fresh_envelope),
            .open_payload = [source_size, state]
            {
                return std::make_unique<OneByteAtATimeReadBuffer>(source_size, state);
            }}};
}

class PublishCountingInMemoryBackend final : public InMemoryBackend
{
public:
    void publish(const BlobPublishRequest & request, TransportAccess & access) override
    {
        ++publish_calls;
        InMemoryBackend::publish(request, access);
    }

    size_t publish_calls = 0;
};

}

/// `NullBackend` and its two tests (`PublishBlobReturnsNoIncarnationToken`,
/// `NullBackendShapeAndDefaults`) are deleted here: their entire subject was the shape and defaults of
/// the legacy Token-typed forwarders (get/head/putIfAbsent/putOverwrite/casPut/deleteExact/list), which
/// no longer exist -- `Backend` now declares only the primitives, all pure virtual, with no default
/// bodies to pin. The primitive surface's own shape is exercised by every concrete-backend test below
/// (`CASInMemory`, `CASObjectStorageBackend`) through `CasRequests`/`CasOperation`, and the request
/// engine's own default behaviour (a `create` finding the key occupied, a `replace` losing its
/// precondition, a `remove` of an absent key) is pinned in `gtest_cas_requests.cpp`.

// =====================================================================
// Task 3: CasInMemoryBackend — enforcing token semantics
// =====================================================================

TEST(CASInMemory, PutIfAbsentAndGet)
{
    InMemoryBackend b;
    CasRequests requests = openRequestsForTest(b);
    CasOperation op = requests.admit();

    const WriteResult put = op.create("k", "v1", Retry::once());
    ASSERT_TRUE(std::holds_alternative<Committed>(put));
    const Etag t1 = std::get<Committed>(put).etag;

    const WriteResult clobber = op.create("k", "clobber", Retry::once());
    EXPECT_TRUE(std::holds_alternative<Conflict>(clobber));

    auto g = op.read("k", Retry::once());
    ASSERT_TRUE(g.has_value());
    EXPECT_EQ(g->bytes, "v1");
    EXPECT_EQ(g->etag, t1);
    EXPECT_FALSE(op.read("absent", Retry::once()).has_value());
}

TEST(CASInMemory, OverwriteIsTokenExactAndMintsFreshToken)
{
    InMemoryBackend b;
    CasRequests requests = openRequestsForTest(b);
    CasOperation op = requests.admit();

    const Etag t1 = std::get<Committed>(op.create("k", "v1", Retry::once())).etag;
    /// A stale precondition for the SAME key: an Etag is bound to the key it was minted for, so a
    /// cross-key Etag is a caller bug (LOGICAL_ERROR), not "the wrong token" any more -- a stale
    /// same-key incarnation is the real-world shape a precondition mismatch has to cover instead.
    const Etag t2 = std::get<Committed>(op.replace("k", "v1.5", t1, Retry::once())).etag;
    EXPECT_TRUE(std::holds_alternative<Conflict>(op.replace("k", "v2", t1, Retry::once())));
    expectBytes(b, "k", "v1.5");                              // untouched on mismatch

    const WriteResult overwrite = op.replace("k", "v2", t2, Retry::once());
    ASSERT_TRUE(std::holds_alternative<Committed>(overwrite));
    EXPECT_NE(std::get<Committed>(overwrite).etag, t2);   // tokens never repeat
    expectBytes(b, "k", "v2");
}

TEST(CASInMemory, CasPutCreateAndSwap)
{
    InMemoryBackend b;
    CasRequests requests = openRequestsForTest(b);
    CasOperation op = requests.admit();

    const WriteResult create = op.create("m", "s1", Retry::once());
    ASSERT_TRUE(std::holds_alternative<Committed>(create));                     // create-if-absent
    const Etag t1 = std::get<Committed>(create).etag;
    EXPECT_TRUE(std::holds_alternative<Conflict>(op.create("m", "s1x", Retry::once())));   // exists now

    /// A stale, same-key precondition -- see OverwriteIsTokenExactAndMintsFreshToken for why a
    /// cross-key Etag can no longer stand in for "the wrong token".
    const Etag t2 = std::get<Committed>(op.replace("m", "s1.5", t1, Retry::once())).etag;
    EXPECT_TRUE(std::holds_alternative<Conflict>(op.replace("m", "s2", t1, Retry::once())));
    EXPECT_EQ(op.read("m", Retry::once())->bytes, "s1.5");
    EXPECT_TRUE(std::holds_alternative<Committed>(op.replace("m", "s2", t2, Retry::once())));
    EXPECT_EQ(op.read("m", Retry::once())->bytes, "s2");
}

TEST(CASInMemory, DeleteExactEnforced)
{
    InMemoryBackend b;
    CasRequests requests = openRequestsForTest(b);
    CasOperation op = requests.admit();

    const Etag t0 = std::get<Committed>(op.create("k", "v1", Retry::once())).etag;
    const Etag t1 = std::get<Committed>(op.replace("k", "v1b", t0, Retry::once())).etag;
    /// t0 is now stale for this SAME key -- see OverwriteIsTokenExactAndMintsFreshToken for why a
    /// cross-key Etag can no longer stand in for "the wrong token".
    EXPECT_EQ(op.remove("k", t0, Retry::once()), Removal::Mismatch);
    EXPECT_TRUE(op.read("k", Retry::once()).has_value());     // SURVIVES wrong-token delete
    EXPECT_EQ(op.remove("k", t1, Retry::once()), Removal::Removed);
    EXPECT_FALSE(op.read("k", Retry::once()).has_value());
    EXPECT_EQ(op.remove("k", t1, Retry::once()), Removal::Gone);
}

TEST(CASInMemory, GetAndHeadAndList)
{
    InMemoryBackend b;
    CasRequests requests = openRequestsForTest(b);
    CasOperation op = requests.admit();

    op.create("p/a", "0123456789", Retry::once());
    op.create("p/b", "xy", Retry::once());
    op.create("q/c", "z", Retry::once());
    EXPECT_EQ(op.read("p/a", Retry::once())->bytes, "0123456789");
    auto h = op.head("p/a", Retry::once());
    ASSERT_TRUE(h.has_value());
    EXPECT_EQ(h->size, 10u);
    auto page = op.list("p/", "", 10, Retry::once());
    ASSERT_EQ(page.keys.size(), 2u);                          // sorted, prefix-scoped
    EXPECT_EQ(page.keys[0].key, "p/a");
    EXPECT_EQ(page.keys[1].key, "p/b");
    EXPECT_TRUE(page.next_cursor.empty());
    auto page1 = op.list("p/", "", 1, Retry::once());         // pagination
    EXPECT_EQ(page1.keys.size(), 1u);
    EXPECT_EQ(page1.keys[0].key, "p/a");
    EXPECT_EQ(page1.next_cursor, "p/a");
    EXPECT_FALSE(page1.next_cursor.empty());
    auto page2 = op.list("p/", page1.next_cursor, 1, Retry::once());
    EXPECT_EQ(page2.keys[0].key, "p/b");
}

TEST(CASInMemory, PublishBlobStreamingWritesFreshEnvelopeAndExactPayload)
{
    InMemoryBackend backend;
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const auto request = streamingPublication("blob", "fresh-envelope", "payload", 7);

    op.publish(request, Retry::once());

    const auto result = op.read("blob", Retry::once());
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->bytes, "fresh-envelopepayload");
}

TEST(CASInMemory, PublishBlobRejectsShortAndLongStreamingSourcesWithoutVisibility)
{
    InMemoryBackend backend;
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    ASSERT_TRUE(std::holds_alternative<Committed>(op.create("short", "old-short", Retry::once())));
    ASSERT_TRUE(std::holds_alternative<Committed>(op.create("long", "old-long", Retry::once())));

    for (const auto & [key, payload, declared_size] : std::vector<std::tuple<String, String, uint64_t>>{
             {"short", "abc", 4},
             {"long", "abcd", 3}})
    {
        const auto request = streamingPublication(key, "fresh", payload, declared_size);
        try
        {
            op.publish(request, Retry::once());
            FAIL() << "expected a source-size mismatch for " << key;
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), DB::ErrorCodes::CORRUPTED_DATA);
        }
    }

    EXPECT_EQ(op.read("short", Retry::once())->bytes, "old-short");
    EXPECT_EQ(op.read("long", Retry::once())->bytes, "old-long");
}

TEST(CASInMemory, PublishBlobLongSourceReadsOnlyDeclaredPayloadAndOneProbeByte)
{
    InMemoryBackend backend;
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    ASSERT_TRUE(std::holds_alternative<Committed>(op.create("long", "old-complete-body", Retry::once())));
    auto state = std::make_shared<CountingSourceState>();

    try
    {
        op.publish(countedLongPublication("long", "fresh", 3, 1024, state), Retry::once());
        FAIL() << "expected a long-source mismatch";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::CORRUPTED_DATA);
    }

    EXPECT_EQ(state->bytes_exposed, 4u);
    const auto still_present = op.read("long", Retry::once());
    ASSERT_TRUE(still_present.has_value());
    EXPECT_EQ(still_present->bytes, "old-complete-body");
}

TEST(CASInMemory, PublishBlobKeepsThePreviousIncarnationVisibleUntilTheCompleteBodyIsReady)
{
    using namespace std::chrono_literals;

    InMemoryBackend backend;
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    ASSERT_TRUE(std::holds_alternative<Committed>(op.create("blob", "old-complete-body", Retry::once())));

    std::promise<void> source_opened;
    std::promise<void> release_source;
    const std::shared_future<void> release = release_source.get_future().share();
    /// Set when `open_payload`'s own internal wait below times out instead of observing the release.
    /// The timeout alone silently lets the publisher proceed either way -- this flag is what lets the
    /// test body downstream (after calling `releaseOnce`) assert that the release actually reached the
    /// publisher, so a regression that leaves it unreleased for the full bound FAILS instead of quietly
    /// passing because the timeout eventually let it through anyway.
    std::atomic<bool> release_wait_expired{false};
    const BlobPublishRequest request{
        .destination_key = "blob",
        .publication = StreamingBlobPublication{
            .payload_size = 7,
            .fresh_envelope = "fresh-envelope",
            .open_payload = [&source_opened, release, &release_wait_expired]
            {
                source_opened.set_value();
                /// Bounded, not `.wait()`: even if the release guards below somehow never fire, this
                /// lambda -- and therefore `publish()`, and therefore the `std::async` task wrapping it
                /// -- must still return within a bounded time, so `publication`'s blocking destructor (a
                /// `std::async` future's destructor blocks until its task finishes) can never hang the
                /// whole process. This is the ONLY wait `open_payload` performs, so it also bounds
                /// `publish()`'s total time to this 20s plus whatever negligible in-memory work follows.
                if (release.wait_for(20s) != std::future_status::ready)
                    release_wait_expired = true;
                return std::make_unique<DB::ReadBufferFromOwnString>(String("payload"));
            }}};

    /// `CasOperation` carries mutable per-call state and is single-threaded by design: the publish and
    /// the concurrent read below each admit their OWN operation from the shared `requests` rather than
    /// racing on `op`.
    CasOperation publish_op = requests.admit();
    auto publication = std::async(std::launch::async, [&] { publish_op.publish(request, Retry::once()); });

    /// `publication` and (below) `observation` are both futures returned by `std::async`, so EACH one's
    /// destructor blocks until its own task finishes. A failing ASSERT_*/EXPECT_* can unwind this
    /// function while the publisher is still parked on `release`; this releases it promptly on that
    /// exit rather than relying solely on the 20s bound above. Idempotent (the ordinary release near the
    /// end sets `released` first) and declared right after `publication` so it protects every exit from
    /// here on -- including the one right below, before `observation` exists.
    bool released = false;
    const auto releaseOnce = [&]
    {
        if (!released)
        {
            released = true;
            release_source.set_value();
        }
    };
    SCOPE_EXIT({ releaseOnce(); });

    ASSERT_EQ(source_opened.get_future().wait_for(20s), std::future_status::ready)
        << "publish() never reached open_payload";

    CasOperation read_op = requests.admit();
    auto observation = std::async(std::launch::async, [&] { return read_op.read("blob", Retry::once()); });
    /// A SECOND copy of the same guard, declared AFTER `observation` so it tears down BEFORE
    /// `observation`'s own blocking destructor on any unwind past this point. Without it, a genuine
    /// visibility-lock regression (exactly what this test exists to catch) would leave `read_op.read`
    /// blocked on the same lock `publish_op.publish` holds while parked on `release`, and the FIRST
    /// guard above -- which, being declared earlier, tears down only AFTER `observation`'s destructor --
    /// would then release the publisher too late to ever unblock that read.
    SCOPE_EXIT({ releaseOnce(); });

    const auto observation_status = observation.wait_for(20s);
    EXPECT_EQ(observation_status, std::future_status::ready)
        << "publication must not hold the visibility lock while draining its source";
    if (observation_status == std::future_status::ready)
    {
        const auto visible = observation.get();
        ASSERT_TRUE(visible.has_value());
        EXPECT_EQ(visible->bytes, "old-complete-body");
    }

    releaseOnce();
    /// `open_payload` above is the ONLY wait reachable from `publish_op.publish`, and it is itself
    /// bounded to 20s: a stuck publisher therefore costs at most that 20s bound (plus negligible
    /// in-memory work) before `publish()` returns and `publication`'s `std::async` destructor can
    /// complete -- never an unbounded hang. `EXPECT_FALSE` below turns a timeout that silently released
    /// the publisher into a visible test failure instead of a pass for the wrong reason.
    ASSERT_EQ(publication.wait_for(20s), std::future_status::ready) << "publish() never completed after release";
    EXPECT_FALSE(release_wait_expired.load())
        << "open_payload's internal wait timed out instead of observing the release";
    EXPECT_NO_THROW(publication.get());
    const auto after = op.read("blob", Retry::once());
    ASSERT_TRUE(after.has_value());
    EXPECT_EQ(after->bytes, "fresh-envelopepayload");
}

TEST(CASInMemory, PublishBlobCopiesStagedObjectBytesVerbatim)
{
    InMemoryBackend backend;
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    ASSERT_TRUE(std::holds_alternative<Committed>(op.create("stage", "staged-envelopepayload", Retry::once())));
    ASSERT_TRUE(std::holds_alternative<Committed>(op.create("blob", "old-body", Retry::once())));

    op.publish(BlobPublishRequest{
        .destination_key = "blob",
        .publication = VerbatimStagedBlobPublication{
            .object_key = "stage",
            .object_size = 22}}, Retry::once());

    const auto after = op.read("blob", Retry::once());
    ASSERT_TRUE(after.has_value());
    EXPECT_EQ(after->bytes, "staged-envelopepayload");
}

// =====================================================================
// Task 4: CasInMemoryBackend — fault injection and probe-test modes
// =====================================================================

TEST(CASInMemoryFaults, HeldDeleteLandsLater)
{
    InMemoryBackend b;
    CasRequests requests = openRequestsForTest(b);
    CasOperation op = requests.admit();
    const Etag t1 = std::get<Committed>(op.create("k", "v1", Retry::once())).etag;
    b.setHoldDeletes(true);
    EXPECT_EQ(op.remove("k", t1, Retry::once()), Removal::Removed);   // message "sent", not landed
    EXPECT_TRUE(op.read("k", Retry::once()).has_value());             // ... but nothing landed yet
    ASSERT_EQ(b.pendingDeletes(), 1u);
    // the object is recreated before the zombie lands:
    op.replace("k", "v1'", t1, Retry::once());
    auto landed = b.landPendingDelete(0);             // the zombie lands NOW
    EXPECT_EQ(landed, DB::Cas::Backend::RawRemoval::Mismatch);   // 412 — INV-NO-RETURN in miniature
    expectBytes(b, "k", "v1'");
}

TEST(CASInMemoryFaults, InjectedCasConflictFiresOnce)
{
    InMemoryBackend b;
    CasRequests requests = openRequestsForTest(b);
    CasOperation op = requests.admit();
    const Etag t1 = std::get<Committed>(op.create("m", "s1", Retry::once())).etag;
    b.refuseNextWrite("m");
    EXPECT_TRUE(std::holds_alternative<Conflict>(op.replace("m", "s2", t1, Retry::once())));   // injected
    EXPECT_EQ(op.read("m", Retry::once())->bytes, "s1");
    EXPECT_TRUE(std::holds_alternative<Committed>(op.replace("m", "s2", t1, Retry::once())));  // next attempt is real
}

TEST(CASInMemoryFaults, NonEnforcingModeMimicsBadBackend)
{
    InMemoryBackend b;
    CasRequests requests = openRequestsForTest(b);
    CasOperation op = requests.admit();
    b.setEnforceTokens(false);                        // MinIO-OSS-shaped backend
    const Etag t0 = std::get<Committed>(op.create("k", "v1", Retry::once())).etag;
    ASSERT_TRUE(std::holds_alternative<Committed>(op.replace("k", "v2", t0, Retry::once())));   // mints a later incarnation
    EXPECT_EQ(op.remove("k", t0, Retry::once()), Removal::Removed);   // stale-but-same-key precondition silently deletes anyway — the dangerous behavior
    EXPECT_FALSE(op.read("k", Retry::once()).has_value());
}

TEST(CASInMemoryFaults, VersioningMarkerMode)
{
    InMemoryBackend b;
    b.setSimulateDeleteMarkers(true);
    CasRequests requests = openRequestsForTest(b);
    CasOperation op = requests.admit();
    const Etag t1 = std::get<Committed>(op.create("k", "v1", Retry::once())).etag;
    /// A removal that only archives (never reclaims) is not an ordinary Removed: the engine reports it
    /// as CAS_DELETE_MARKER so the capability probe can reject a versioned pool.
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CAS_DELETE_MARKER, [&] { op.remove("k", t1, Retry::once()); });
}

// =====================================================================
// stream seam (forward-only reads of write-once objects)
// =====================================================================

/// The legacy getStream's byte-range window is retired along with it: the primitive `stream` takes no
/// Range, and every consumer (RunFileReader) already bounds its own consumption client-side rather than
/// relying on a server-side window. What survives here is presence: a present key opens a readable
/// stream, an absent one opens none.
TEST(CASBackendStream, StreamsWholeBodyOrNullWhenAbsent)
{
    auto backend = std::make_shared<InMemoryBackend>();
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    op.create("k", "0123456789", Retry::once());

    auto got = op.stream("k", Retry::once());
    ASSERT_TRUE(got != nullptr);
    String out;
    DB::readStringUntilEOF(out, *got);
    EXPECT_EQ(out, "0123456789");

    EXPECT_EQ(op.stream("absent", Retry::once()), nullptr);
}

// =====================================================================
// B168 P0: InstrumentedBackend per-namespace/op ProfileEvents
// =====================================================================

namespace ProfileEvents
{
extern const Event CASBlobPut;
extern const Event CASBlobPutDeduplicated;
extern const Event CASBlobHead;
extern const Event CASBlobHeadMiss;
extern const Event CASGCPut;
}

TEST(CASInstrumentedBackend, ClassifierAndPerNamespaceOpEvents)
{
    /// Namespace classification by substring.
    EXPECT_EQ(classifyCasNs("pool/blobs/ab/abcdef"), CasNs::Blob);
    EXPECT_EQ(classifyCasNs("pool/gc/registry"), CasNs::Gc);   /// gc/ prefix covers GC state (state, retired sets, etc.)
    EXPECT_EQ(classifyCasNs("pool/roots/default/_files/x"), CasNs::Root);
    EXPECT_EQ(classifyCasNs("pool/gc/state"), CasNs::Gc);
    /// D3: the old per-server-control key shapes (`_watermark`, `_precommits/<n>`) have no producer
    /// anymore -- control state now lives under `/gc/server-roots/...` (classifies as Gc). A key of
    /// this legacy shape, if it ever showed up, would fall through to the generic /roots/ rule.
    EXPECT_EQ(classifyCasNs("pool/roots/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/_watermark"), CasNs::Root);
    EXPECT_EQ(classifyCasNs("pool/roots/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/_precommits/3"), CasNs::Root);
    EXPECT_EQ(classifyCasNs("pool/_pool_meta"), CasNs::Other);
    /// Final opaque-life layout: both immutable streams and point/path-addressed state remain Root
    /// instrumentation, while part manifests remain Manifest. None may fall into Other (the
    /// 2026-07-03 operator-stand CREATE storm misread as CASOtherHeadMiss=102 because of this).
    EXPECT_EQ(classifyCasNs("pool/cas/ns/stream/00000000000000000000000000000017/_log/1-1.zst"), CasNs::Root);
    EXPECT_EQ(classifyCasNs("pool/cas/ns/state/00000000000000000000000000000017/_ckpt.zst"), CasNs::Root);
    EXPECT_EQ(classifyCasNs("pool/cas/ns/state/00000000000000000000000000000017/_files/format_version.txt"), CasNs::Root);
    EXPECT_EQ(classifyCasNs("pool/cas/manifests/0/srv/store/d18/uuid@cas@/24/1/000001.proto"), CasNs::Manifest);

    auto inner = std::make_shared<InMemoryBackend>();
    auto instrumented = std::make_shared<InstrumentedBackend>(inner);
    CasRequests requests = openRequestsForTest(instrumented);
    CasOperation op = requests.admit();

    using ProfileEvents::global_counters;
    const auto blob_put_before   = global_counters[ProfileEvents::CASBlobPut].load();
    const auto blob_dedup_before = global_counters[ProfileEvents::CASBlobPutDeduplicated].load();
    const auto blob_head_before  = global_counters[ProfileEvents::CASBlobHead].load();
    const auto blob_miss_before  = global_counters[ProfileEvents::CASBlobHeadMiss].load();
    const auto gc_put_before     = global_counters[ProfileEvents::CASGCPut].load();

    const String blob_key = "pool/blobs/ab/abcdef0123456789";

    /// First create of a blob ⇒ Put.
    EXPECT_TRUE(std::holds_alternative<Committed>(op.create(blob_key, "payload", Retry::once())));
    /// Second create of the same key ⇒ PutDeduplicated (content already exists).
    EXPECT_TRUE(std::holds_alternative<Conflict>(op.create(blob_key, "payload", Retry::once())));
    /// head of an absent blob key ⇒ HeadMiss (the 404 signal).
    EXPECT_FALSE(op.head("pool/blobs/zz/absent", Retry::once()).has_value());
    /// head of the present blob key ⇒ Head.
    EXPECT_TRUE(op.head(blob_key, Retry::once()).has_value());
    /// create on a gc key ⇒ Gc Put.
    EXPECT_TRUE(std::holds_alternative<Committed>(op.create("pool/gc/state", "g1", Retry::once())));
    /// Under coverage builds ProfileEvents propagate into a thread-local subtree that does not reach
    /// `global_counters`; deltas read 0 there only (see gtest_unique_key_index_cache).
#if !WITH_COVERAGE
    EXPECT_EQ(global_counters[ProfileEvents::CASBlobPut].load()      - blob_put_before,   1u);
    EXPECT_EQ(global_counters[ProfileEvents::CASBlobPutDeduplicated].load() - blob_dedup_before, 1u);
    EXPECT_EQ(global_counters[ProfileEvents::CASBlobHead].load()     - blob_head_before,  1u);
    EXPECT_EQ(global_counters[ProfileEvents::CASBlobHeadMiss].load() - blob_miss_before,  1u);
    EXPECT_EQ(global_counters[ProfileEvents::CASGCPut].load()        - gc_put_before,     1u);
#else
    (void)blob_put_before; (void)blob_dedup_before; (void)blob_head_before;
    (void)blob_miss_before; (void)gc_put_before;
#endif
}

TEST(CASInstrumentedBackend, PublishBlobDelegatesOnceAndRecordsOnePhysicalBlobWrite)
{
    auto inner = std::make_shared<PublishCountingInMemoryBackend>();
    auto backend = std::make_shared<InstrumentedBackend>(inner);
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();

    using ProfileEvents::global_counters;
    const auto blob_put_before = global_counters[ProfileEvents::CASBlobPut].load();

    const auto request = streamingPublication("pool/blobs/ab/published", "fresh", "payload", 7);
    op.publish(request, Retry::once());

    EXPECT_EQ(inner->publish_calls, 1u);
    CasRequests inner_requests = openRequestsForTest(inner);
    CasOperation inner_op = inner_requests.admit();
    const auto published = inner_op.read("pool/blobs/ab/published", Retry::once());
    ASSERT_TRUE(published.has_value());
    EXPECT_EQ(published->bytes, "freshpayload");
#if !WITH_COVERAGE
    EXPECT_EQ(global_counters[ProfileEvents::CASBlobPut].load() - blob_put_before, 1u);
#else
    (void)blob_put_before;
#endif
}

// =====================================================================
// M-C2 Task 2: typed S3 precondition signal
// =====================================================================

/// The per-dialect grammar in isolation, independent of any backend fixture.
TEST(CASBackendGrammar, GenerationDialectAcceptsOnlyCanonicalPositiveDecimal)
{
    using DB::Cas::ObjectStorageBackend;
    using DB::Cas::Dialect;
    EXPECT_TRUE(ObjectStorageBackend::isValidTokenValue(Dialect::Generation, "123"));
    EXPECT_FALSE(ObjectStorageBackend::isValidTokenValue(Dialect::Generation, "0"));
    EXPECT_FALSE(ObjectStorageBackend::isValidTokenValue(Dialect::Generation, "00123"));
    EXPECT_FALSE(ObjectStorageBackend::isValidTokenValue(Dialect::Generation, "\"123\""));
    EXPECT_FALSE(ObjectStorageBackend::isValidTokenValue(Dialect::Generation, "12a"));
    EXPECT_TRUE(ObjectStorageBackend::isValidTokenValue(Dialect::ETag, "\"abc\""));
    EXPECT_FALSE(ObjectStorageBackend::isValidTokenValue(Dialect::ETag, " * "));
    EXPECT_FALSE(ObjectStorageBackend::isValidTokenValue(Dialect::ETag, "a,b"));
    EXPECT_TRUE(ObjectStorageBackend::isValidTokenValue(Dialect::Emulated, "7"));
    EXPECT_FALSE(ObjectStorageBackend::isValidTokenValue(Dialect::Emulated, ""));
}

/// §1 (opt round-B): the fold/point GETs read tiny bodies but a default `ReadBufferFromS3` preallocates
/// ~1 MiB. `casSizedReadSettings` shrinks the buffer to the known body size + slack, capped at the
/// caller's default — never larger than before, regardless of the reported size.
TEST(CASSizedReadSettings, CapsToKnownSizePlusSlackButNeverAboveBase)
{
    DB::ReadSettings base;
    base.remote_fs_settings.buffer_size = 1ULL << 20;   /// 1 MiB default
    base.local_fs_settings.buffer_size = 1ULL << 20;

    /// A ~3.7 KB fold body: buffer shrinks to size + slack, far below the 1 MiB default.
    const auto small = DB::Cas::casSizedReadSettings(base, 3700);
    EXPECT_EQ(small.remote_fs_settings.buffer_size, 3700 + DB::Cas::CAS_FOLD_READ_SLACK_BYTES);
    EXPECT_EQ(small.local_fs_settings.buffer_size, 3700 + DB::Cas::CAS_FOLD_READ_SLACK_BYTES);

    /// A body larger than the default is capped AT the default (never grown).
    const auto big = DB::Cas::casSizedReadSettings(base, 8ULL << 20);
    EXPECT_EQ(big.remote_fs_settings.buffer_size, 1ULL << 20);

    /// Unknown size (0) = leave the base untouched (the metadata-fetch fallback path).
    const auto unknown = DB::Cas::casSizedReadSettings(base, 0);
    EXPECT_EQ(unknown.remote_fs_settings.buffer_size, 1ULL << 20);
}

/// The CountingBackend recorders the streaming-memory gates consume: per-key and total stream counts.
/// A window is no longer part of the shape -- a materialized read is always whole, so `stream` no
/// longer carries one either, and it is not what the gates measure.
TEST(CASCountingBackendShape, RecordsStreamOpensPerKeyAndInTotal)
{
    auto backend = std::make_shared<DB::Cas::tests::CountingBackend>();
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    op.create("k", String(1000, 'x'), Retry::once());

    op.stream("k", Retry::once());
    op.stream("k", Retry::once());
    op.stream("absent", Retry::once());
    EXPECT_EQ(backend->getStreamCount("k"), 2u);
    EXPECT_EQ(backend->getStreamTotal(), 3u);

    backend->resetCounts();
    EXPECT_EQ(backend->getStreamCount("k"), 0u);
    EXPECT_EQ(backend->getStreamTotal(), 0u);
}

/// Armed chunking makes this backend serve a stream the way a network-backed store does, in bounded
/// windows, instead of handing over the materialized object in one piece. The bytes a consumer reads
/// are the same either way; what changes is that a consumer which assumed one contiguous window can no
/// longer get one.
TEST(CASCountingBackendShape, AnArmedChunkBoundsTheWindowAStreamHandsOut)
{
    const String body(10'000, 'x');
    auto backend = std::make_shared<DB::Cas::tests::CountingBackend>();
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    ASSERT_TRUE(std::holds_alternative<Committed>(op.create("run", body, Retry::once())));

    /// Unarmed: the whole object arrives as one window, which is what this backend's materialization
    /// makes of any stream and exactly what the bound exists to remove.
    {
        auto opened = op.stream("run", Retry::once());
        ASSERT_TRUE(opened != nullptr);
        String drained;
        readStringUntilEOF(drained, *opened);
        EXPECT_EQ(drained, body);
        EXPECT_EQ(backend->largestStreamChunk("run"), 0u) << "nothing records a window while chunking is off";
    }

    backend->setStreamChunkForTest(4096);
    {
        auto opened = op.stream("run", Retry::once());
        ASSERT_TRUE(opened != nullptr);
        String drained;
        readStringUntilEOF(drained, *opened);
        EXPECT_EQ(drained, body) << "chunking changes the window, never the bytes";
        EXPECT_EQ(backend->largestStreamChunk("run"), 4096u);
        EXPECT_LT(backend->largestStreamChunk("run"), body.size())
            << "the consumer never held the object entire";
    }

    /// The mode outlives a counter reset, and the recorded window does not.
    backend->resetCounts();
    EXPECT_EQ(backend->largestStreamChunk("run"), 0u);
    auto reopened = op.stream("run", Retry::once());
    ASSERT_TRUE(reopened != nullptr);
    String again;
    readStringUntilEOF(again, *reopened);
    EXPECT_EQ(backend->largestStreamChunk("run"), 4096u);
}

/// What makes every request-profile gate in this tree trustworthy: a counter names a PHYSICAL request.
/// The transport primitives are the only surface left that can issue one, so this now pins that a
/// create/head/read/replace/remove issued through `CasOperation` counts exactly once each.
TEST(CASCountingBackendShape, OneRequestIsCountedOnceWhicheverSurfaceIssuedIt)
{
    auto backend = std::make_shared<DB::Cas::tests::CountingBackend>();
    DB::Cas::CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    DB::Cas::CasOperation op = requests.admit();

    /// `Retry::once()` on every verb below, not `standard()`: this test pins ONE physical request per
    /// call, and a healthy backend never distinguishes the two policies by outcome -- only `once()`
    /// forbids a reissue by construction, so a regression that made the engine reissue speculatively
    /// would still fail here instead of passing on a backend too healthy to ever need the second attempt.
    EXPECT_TRUE(std::holds_alternative<Committed>(op.create("k", "v", Retry::once())));
    EXPECT_TRUE(std::holds_alternative<Committed>(op.create("k2", "v", Retry::once())));
    EXPECT_EQ(backend->putCount("k"), 1u);
    EXPECT_EQ(backend->putCount("k2"), 1u);
    EXPECT_EQ(backend->writeTotal(), 2u);
    EXPECT_EQ(backend->putOverwriteTotal(), 0u) << "neither write carried a precondition";

    const std::optional<Meta> k_meta = op.head("k", Retry::once());
    ASSERT_TRUE(k_meta);
    EXPECT_EQ(backend->headCount("k"), 1u);

    expectBytes(*backend, "k", "v");
    EXPECT_TRUE(op.read("k", Retry::once()));
    EXPECT_EQ(backend->getCount("k"), 2u);

    EXPECT_TRUE(std::holds_alternative<Committed>(op.replace("k", "w", k_meta->etag, Retry::once())));
    EXPECT_EQ(backend->putOverwriteCount("k"), 1u) << "a write with a precondition is the replace shape";
    EXPECT_EQ(backend->writeCount("k"), 2u);

    const std::optional<Meta> k2_meta = op.head("k2", Retry::once());
    ASSERT_TRUE(k2_meta);
    EXPECT_EQ(op.remove("k2", k2_meta->etag, Retry::once()), Removal::Removed);
    const std::optional<Meta> k_meta_after = op.head("k", Retry::once());
    ASSERT_TRUE(k_meta_after);
    EXPECT_EQ(op.remove("k", k_meta_after->etag, Retry::once()), Removal::Removed);
    EXPECT_EQ(backend->deleteCount("k"), 1u);
    EXPECT_EQ(backend->deleteCount("k2"), 1u);
    EXPECT_EQ(backend->deleteTotal(), 2u);
}

#if USE_AWS_S3

namespace
{

class PublicationRecordingWriteBuffer final : public DB::WriteBufferFromFileBase
{
public:
    PublicationRecordingWriteBuffer(size_t & cancel_calls_, size_t & finalize_calls_, size_t & bytes_at_cancel_)
        : DB::WriteBufferFromFileBase(DB::DBMS_DEFAULT_BUFFER_SIZE, nullptr, 0)
        , cancel_calls(cancel_calls_)
        , finalize_calls(finalize_calls_)
        , bytes_at_cancel(bytes_at_cancel_)
    {
    }

    void sync() override
    {
        next();
    }

    std::string getFileName() const override
    {
        return "publication-recording-write-buffer";
    }

private:
    void nextImpl() override
    {
    }

    void finalizeImpl() override
    {
        next();
        ++finalize_calls;
    }

    void cancelImpl() noexcept override
    {
        bytes_at_cancel = count();
        ++cancel_calls;
    }

    size_t & cancel_calls;
    size_t & finalize_calls;
    size_t & bytes_at_cancel;
};

struct PublicationWriteBarrier
{
    std::promise<void> opened;
    std::promise<void> release;
    std::shared_future<void> release_future = release.get_future().share();
    /// Set when the wait on `release_future` below times out instead of observing the release. The
    /// timeout alone silently lets the write proceed either way -- this flag is what lets the test body
    /// assert (after calling the release itself) that it actually reached the write, so a regression
    /// that leaves it unreleased for the full bound FAILS instead of quietly passing because the
    /// timeout eventually let it through anyway.
    std::atomic<bool> release_wait_expired{false};
};

class PublicationRecordingLocalObjectStorage final : public DB::LocalObjectStorage
{
public:
    using DB::LocalObjectStorage::LocalObjectStorage;

    std::unique_ptr<DB::WriteBufferFromFileBase> writeObject(
        const DB::StoredObject & object,
        DB::WriteMode mode,
        std::optional<DB::ObjectAttributes> attributes,
        size_t buf_size,
        const DB::WriteSettings & write_settings) override
    {
        ++write_calls;
        last_opened_key = object.remote_path;
        last_write_mode = mode;
        last_write_settings = write_settings;
        if (record_cancellation_only)
            return std::make_unique<PublicationRecordingWriteBuffer>(cancel_calls, finalize_calls, bytes_at_cancel);

        auto out = DB::LocalObjectStorage::writeObject(object, mode, attributes, buf_size, write_settings);
        if (throw_after_open)
            throw std::runtime_error("injected write failure after opening local object");
        if (write_barrier)
        {
            write_barrier->opened.set_value();
            /// Bounded, not `.wait()`: even if a caller's release guard somehow never fires, this call
            /// -- and therefore the `std::async` task wrapping the publish that reaches it -- must still
            /// return within a bounded time, so that future's blocking destructor (a `std::async`
            /// future's destructor blocks until its task finishes) can never hang the whole process.
            /// This is the ONLY wait this write performs, so it also bounds the whole call's total time
            /// to this 20s plus whatever negligible local-filesystem work follows.
            if (write_barrier->release_future.wait_for(std::chrono::seconds(20)) != std::future_status::ready)
                write_barrier->release_wait_expired = true;
        }
        return out;
    }

    void copyObject(
        const DB::StoredObject & object_from,
        const DB::StoredObject & object_to,
        const DB::ReadSettings & read_settings,
        const DB::WriteSettings & write_settings,
        std::optional<DB::ObjectAttributes> object_to_attributes) override
    {
        ++copy_calls;
        last_copy_settings = write_settings;
        DB::LocalObjectStorage::copyObject(
            object_from, object_to, read_settings, write_settings, object_to_attributes);
    }

    bool supportsCopyMode(DB::ObjectStorageCopyMode copy_mode) const override
    {
        return copy_mode == DB::ObjectStorageCopyMode::Default
            || (copy_mode == DB::ObjectStorageCopyMode::NativeOnly && native_copy_supported);
    }

    std::optional<DB::ObjectMetadata> tryGetObjectMetadataWithNativeToken(
        const std::string & path, bool with_tags) const override
    {
        ++native_metadata_calls;
        return DB::LocalObjectStorage::tryGetObjectMetadataWithNativeToken(path, with_tags);
    }

    std::optional<DB::ObjectMetadata> tryGetObjectMetadata(const std::string & path, bool with_tags) const override
    {
        ++metadata_calls;
        return DB::LocalObjectStorage::tryGetObjectMetadata(path, with_tags);
    }

    void resetRecording()
    {
        write_calls = 0;
        copy_calls = 0;
        metadata_calls = 0;
        native_metadata_calls = 0;
        cancel_calls = 0;
        finalize_calls = 0;
        bytes_at_cancel = 0;
        last_write_mode.reset();
        last_write_settings.reset();
        last_copy_settings.reset();
    }

    bool native_copy_supported = true;
    bool record_cancellation_only = false;
    bool throw_after_open = false;
    std::shared_ptr<PublicationWriteBarrier> write_barrier;
    size_t write_calls = 0;
    size_t copy_calls = 0;
    mutable size_t metadata_calls = 0;
    mutable size_t native_metadata_calls = 0;
    size_t cancel_calls = 0;
    size_t finalize_calls = 0;
    size_t bytes_at_cancel = 0;
    String last_opened_key;
    std::optional<DB::WriteMode> last_write_mode;
    std::optional<DB::WriteSettings> last_write_settings;
    std::optional<DB::WriteSettings> last_copy_settings;
};

std::shared_ptr<PublicationRecordingLocalObjectStorage> makePublicationRecordingStorage()
{
    static std::atomic<uint64_t> counter{0};
    const auto unique = std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1));
    const auto root = (std::filesystem::temp_directory_path() / ("cas_publish_blob_unit_" + unique)).string();

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    std::filesystem::create_directories(root, ec);

    DB::LocalObjectStorageSettings settings("test", root, /*read_only_=*/false);
    return std::make_shared<PublicationRecordingLocalObjectStorage>(std::move(settings));
}

String readStorageObject(const DB::ObjectStoragePtr & storage, const String & key)
{
    auto in = storage->readObject(DB::StoredObject(key), DB::ReadSettings{});
    String bytes;
    DB::readStringUntilEOF(bytes, *in);
    return bytes;
}

}

TEST(CASObjectStorageBackend, PublishBlobStreamingUsesOrdinaryDefaultWriteTransport)
{
    auto storage = makePublicationRecordingStorage();
    auto backend = std::make_shared<ObjectStorageBackend>(storage, ObjectStorageBackend::Mode::Native);
    backend->setNativeTokenTypeForTest(Dialect::Generation);
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const String destination = DB::Cas::tests::nativeKeyUnder(storage, "publish/streaming");

    const auto request = streamingPublication(destination, "fresh-envelope", "payload", 7);
    op.publish(request, Retry::once());

    ASSERT_EQ(storage->write_calls, 1u);
    ASSERT_TRUE(storage->last_write_mode.has_value());
    EXPECT_EQ(*storage->last_write_mode, DB::WriteMode::Rewrite);
    ASSERT_TRUE(storage->last_write_settings.has_value());
    EXPECT_EQ(storage->last_write_settings->object_storage_request_mode, DB::ObjectStorageRequestMode::Default);
    EXPECT_EQ(storage->last_write_settings->object_storage_retry_profile, DB::ObjectStorageRetryProfile::Default);
    EXPECT_EQ(storage->last_write_settings->s3_max_unexpected_write_error_retries_override, 0u);
    EXPECT_FALSE(storage->last_write_settings->s3_force_single_part_upload);
    EXPECT_TRUE(storage->last_write_settings->object_storage_write_if_none_match.empty());
    EXPECT_TRUE(storage->last_write_settings->object_storage_write_if_match.empty());
    EXPECT_EQ(storage->native_metadata_calls, 0u)
        << "tokenless publication must not issue a response-token HEAD";
    EXPECT_EQ(readStorageObject(storage, destination), "fresh-envelopepayload");
}

TEST(CASObjectStorageBackend, PublishBlobEmulatedKeepsDestinationCompleteUntilAtomicReplacement)
{
    using namespace std::chrono_literals;

    auto storage = makePublicationRecordingStorage();
    auto backend = std::make_shared<ObjectStorageBackend>(storage, ObjectStorageBackend::Mode::EmulatedSingleProcess);
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const String key = "publish/emulated-atomic";
    const String physical_key = DB::Cas::tests::nativeKeyUnder(storage, key);

    {
        auto out = storage->writeObject(
            DB::StoredObject(physical_key), DB::WriteMode::Rewrite, {}, DB::DBMS_DEFAULT_BUFFER_SIZE, DB::WriteSettings{});
        DB::writeString(String("old-complete-body"), *out);
        out->finalize();
    }
    storage->resetRecording();

    auto barrier = std::make_shared<PublicationWriteBarrier>();
    storage->write_barrier = barrier;
    auto opened = barrier->opened.get_future();
    auto publication = std::async(std::launch::async, [&]
    {
        op.publish(streamingPublication(key, "fresh-envelope", "payload", 7), Retry::once());
    });

    /// Same hazard as `PublishBlobKeepsThePreviousIncarnationVisibleUntilTheCompleteBodyIsReady`: an
    /// exception unwinding out of this function (a failing ASSERT_*, or `readStorageObject` throwing)
    /// while the write is still parked on `barrier->release_future.wait()` would deadlock `publication`'s
    /// blocking `std::async` destructor against `barrier`'s own (later) teardown. Declared after
    /// `publication` so it tears down FIRST, this guard releases the write unconditionally.
    bool released = false;
    SCOPE_EXIT({
        if (!released)
        {
            released = true;
            barrier->release.set_value();
        }
    });

    const auto opened_status = opened.wait_for(20s);
    EXPECT_EQ(opened_status, std::future_status::ready);
    if (opened_status == std::future_status::ready)
        EXPECT_EQ(readStorageObject(storage, physical_key), "old-complete-body");

    released = true;
    barrier->release.set_value();
    /// The write inside `writeObject` is the ONLY wait reachable from `op.publish`, and it is itself
    /// bounded to 20s: a stuck write therefore costs at most that 20s bound (plus negligible
    /// local-filesystem work) before `publish()` returns and `publication`'s `std::async` destructor
    /// can complete -- never an unbounded hang. `EXPECT_FALSE` below turns a timeout that silently
    /// released the write into a visible test failure instead of a pass for the wrong reason.
    ASSERT_EQ(publication.wait_for(20s), std::future_status::ready) << "publish() never completed after release";
    EXPECT_FALSE(barrier->release_wait_expired.load())
        << "the write's internal wait timed out instead of observing the release";
    EXPECT_NO_THROW(publication.get());
    EXPECT_EQ(storage->metadata_calls, 0u);
    EXPECT_EQ(readStorageObject(storage, physical_key), "fresh-envelopepayload");
}

TEST(CASObjectStorageBackend, PublishBlobEmulatedWriteFailurePreservesDestinationAndCleansTemporary)
{
    auto storage = makePublicationRecordingStorage();
    auto backend = std::make_shared<ObjectStorageBackend>(storage, ObjectStorageBackend::Mode::EmulatedSingleProcess);
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const String key = "publish/emulated-failure";
    const String physical_key = DB::Cas::tests::nativeKeyUnder(storage, key);

    {
        auto out = storage->writeObject(
            DB::StoredObject(physical_key), DB::WriteMode::Rewrite, {}, DB::DBMS_DEFAULT_BUFFER_SIZE, DB::WriteSettings{});
        DB::writeString(String("old-complete-body"), *out);
        out->finalize();
    }
    const Etag old_token = op.head(key, Retry::once())->etag;

    storage->throw_after_open = true;
    EXPECT_THROW(
        op.publish(streamingPublication(key, "fresh-envelope", "payload", 7), Retry::once()),
        std::runtime_error);
    storage->throw_after_open = false;

    EXPECT_NE(storage->last_opened_key, physical_key);
    EXPECT_FALSE(storage->exists(DB::StoredObject(storage->last_opened_key)));
    EXPECT_EQ(readStorageObject(storage, physical_key), "old-complete-body");
    EXPECT_EQ(op.head(key, Retry::once())->etag, old_token);
}

TEST(CASObjectStorageBackend, PublishBlobCancelsShortAndLongStreamingSourcesBeforeVisibility)
{
    auto storage = makePublicationRecordingStorage();
    auto backend = std::make_shared<ObjectStorageBackend>(storage, ObjectStorageBackend::Mode::Native);
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const String destination = DB::Cas::tests::nativeKeyUnder(storage, "publish/mismatch");

    {
        auto out = storage->writeObject(
            DB::StoredObject(destination), DB::WriteMode::Rewrite, {}, DB::DBMS_DEFAULT_BUFFER_SIZE, DB::WriteSettings{});
        DB::writeString(String("old-complete-body"), *out);
        out->finalize();
    }
    storage->resetRecording();
    storage->record_cancellation_only = true;

    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&]
    {
        op.publish(streamingPublication(destination, "fresh", "abc", 4), Retry::once());
    });
    EXPECT_EQ(storage->cancel_calls, 1u);
    EXPECT_EQ(storage->finalize_calls, 0u);
    EXPECT_EQ(storage->bytes_at_cancel, 8u);
    EXPECT_EQ(readStorageObject(storage, destination), "old-complete-body");

    auto state = std::make_shared<CountingSourceState>();
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&]
    {
        op.publish(countedLongPublication(destination, "fresh", 3, 1024, state), Retry::once());
    });
    EXPECT_EQ(state->bytes_exposed, 4u);
    EXPECT_EQ(storage->cancel_calls, 2u);
    EXPECT_EQ(storage->finalize_calls, 0u);
    EXPECT_EQ(storage->bytes_at_cancel, 8u);
    EXPECT_EQ(readStorageObject(storage, destination), "old-complete-body");
}

TEST(CASObjectStorageBackend, PublishBlobEmulatedLongSourceReadsOnlyDeclaredPayloadAndOneProbeByte)
{
    auto storage = makePublicationRecordingStorage();
    auto backend = std::make_shared<ObjectStorageBackend>(storage, ObjectStorageBackend::Mode::EmulatedSingleProcess);
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const String key = "publish/emulated-long";
    const String physical_key = DB::Cas::tests::nativeKeyUnder(storage, key);

    {
        auto out = storage->writeObject(
            DB::StoredObject(physical_key), DB::WriteMode::Rewrite, {}, DB::DBMS_DEFAULT_BUFFER_SIZE, DB::WriteSettings{});
        DB::writeString(String("old-complete-body"), *out);
        out->finalize();
    }

    auto state = std::make_shared<CountingSourceState>();
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&]
    {
        op.publish(countedLongPublication(key, "fresh", 3, 1024, state), Retry::once());
    });

    EXPECT_EQ(state->bytes_exposed, 4u);
    EXPECT_EQ(readStorageObject(storage, physical_key), "old-complete-body");
}

TEST(CASObjectStorageBackend, PublishBlobCopiesStagedBytesWithNativeOnlyDefaultRequestMode)
{
    auto storage = makePublicationRecordingStorage();
    auto backend = std::make_shared<ObjectStorageBackend>(storage, ObjectStorageBackend::Mode::Native);
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const String staging = DB::Cas::tests::nativeKeyUnder(storage, "publish/staging");
    const String destination = DB::Cas::tests::nativeKeyUnder(storage, "publish/copied");

    {
        auto out = storage->writeObject(
            DB::StoredObject(staging), DB::WriteMode::Rewrite, {}, DB::DBMS_DEFAULT_BUFFER_SIZE, DB::WriteSettings{});
        DB::writeString(String("staged-envelopepayload"), *out);
        out->finalize();
    }
    storage->resetRecording();

    op.publish(BlobPublishRequest{
        .destination_key = destination,
        .publication = VerbatimStagedBlobPublication{
            .object_key = staging,
            .object_size = 22}}, Retry::once());

    ASSERT_EQ(storage->copy_calls, 1u);
    ASSERT_TRUE(storage->last_copy_settings.has_value());
    EXPECT_EQ(storage->last_copy_settings->object_storage_copy_mode, DB::ObjectStorageCopyMode::NativeOnly);
    EXPECT_EQ(storage->last_copy_settings->object_storage_request_mode, DB::ObjectStorageRequestMode::Default);
    EXPECT_EQ(storage->last_copy_settings->object_storage_retry_profile, DB::ObjectStorageRetryProfile::Default);
    EXPECT_TRUE(storage->last_copy_settings->object_storage_write_if_none_match.empty());
    EXPECT_TRUE(storage->last_copy_settings->object_storage_write_if_match.empty());
    EXPECT_EQ(readStorageObject(storage, destination), "staged-envelopepayload");
}

TEST(CASObjectStorageBackend, PublishBlobRefusesVerbatimCopyWithoutNativeTransport)
{
    auto storage = makePublicationRecordingStorage();
    auto backend = std::make_shared<ObjectStorageBackend>(storage, ObjectStorageBackend::Mode::Native);
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const String staging = DB::Cas::tests::nativeKeyUnder(storage, "publish/unsupported-staging");
    const String destination = DB::Cas::tests::nativeKeyUnder(storage, "publish/unsupported-copy");

    {
        auto out = storage->writeObject(
            DB::StoredObject(staging), DB::WriteMode::Rewrite, {}, DB::DBMS_DEFAULT_BUFFER_SIZE, DB::WriteSettings{});
        DB::writeString(String("complete-staged-object"), *out);
        out->finalize();
    }
    storage->resetRecording();
    storage->native_copy_supported = false;

    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::NOT_IMPLEMENTED, [&]
    {
        op.publish(BlobPublishRequest{
            .destination_key = destination,
            .publication = VerbatimStagedBlobPublication{
                .object_key = staging,
                .object_size = 22}}, Retry::once());
    });

    EXPECT_EQ(storage->copy_calls, 0u);
    EXPECT_FALSE(storage->exists(DB::StoredObject(destination)));
}

/// Every Native conditional write selects the SingleAttempt object-storage retry profile (RFC
/// cas-s3-timeout-retry-control §disable-transparent-conditional-write-retries): the CAS request
/// engine, not the object-storage client, owns retry/backoff for a conditional PUT, so a client-level
/// retry loop reissuing the identical request underneath it would double the reissue and could land a
/// write the engine itself had already given up on. Two seams prove the property without a live/fake
/// S3 endpoint: `IObjectStorage::supportsRetryProfile` is the fail-closed capability check
/// `checkConditionalWriteSingleAttemptSupport` relies on at mount time, and
/// `s3_max_unexpected_write_error_retries_override` is the SECOND retry-affecting layer above the S3
/// client -- `WriteBufferFromS3`'s own makeSinglepartUpload/completeMultipartUpload loop, bounded
/// independently of the client-level override.
TEST(CASObjectStorageBackend, ConditionalWriteSelectsSingleAttemptAndLocalStorageDoesNotSupportIt)
{
    auto backend = std::make_shared<ObjectStorageBackend>(
        tests::makeLocalObjectStorageForTest(), ObjectStorageBackend::Mode::Native);
    const auto ws = backend->conditionalWriteSettingsForTest();
    EXPECT_EQ(ws.object_storage_retry_profile, DB::ObjectStorageRetryProfile::SingleAttempt);
    EXPECT_EQ(ws.s3_max_unexpected_write_error_retries_override, 1u);
    EXPECT_FALSE(tests::makeLocalObjectStorageForTest()->supportsRetryProfile(DB::ObjectStorageRetryProfile::SingleAttempt));
}

/// The Native conditional-PUT path discriminates a lost precondition by the canonical S3 error code
/// string ("PreconditionFailed", "NoSuchKey", ...) that `S3Exception` carries from the response XML
/// `<Code>` — a 412 is UNMODELED for the AWS SDK (the enum value is UNKNOWN), so the name is the only
/// machine-readable signal.
TEST(CASS3Signal, S3ExceptionCarriesCanonicalErrorName)
{
    DB::S3Exception e("412 from backend", Aws::S3::S3Errors::UNKNOWN, "PreconditionFailed");
    EXPECT_EQ(e.getExceptionName(), "PreconditionFailed");
    DB::S3Exception bare("no name attached", Aws::S3::S3Errors::UNKNOWN);
    EXPECT_TRUE(bare.getExceptionName().empty());
}

namespace
{

/// WriteBuffer stub whose finalize throws a configured S3Exception — drives the classifier directly.
class ThrowOnFinalizeBuffer final : public DB::WriteBuffer
{
public:
    ThrowOnFinalizeBuffer() : DB::WriteBuffer(nullptr, 0) {}

    explicit ThrowOnFinalizeBuffer(DB::S3Exception e) : DB::WriteBuffer(nullptr, 0), to_throw(std::move(e)) {}

private:
    void nextImpl() override {}

    void finalizeImpl() override
    {
        if (to_throw)
            throw *to_throw; /// NOLINT(cert-err09-cpp,cert-err60-cpp,cert-err61-cpp,misc-throw-by-value-catch-by-reference) -- the mock stores the configured exception to throw later, so it cannot be an anonymous temporary
    }

    std::optional<DB::S3Exception> to_throw;
};

}

/// detail::finalizeConditionalWrite maps a lost precondition to an OUTCOME by exact-matching the
/// canonical S3 error name (plus the modeled NO_SUCH_KEY enum, which WriteBufferFromS3 surfaces
/// nameless on retry exhaustion) and rethrows anything else.
TEST(CASS3Signal, FinalizeClassifierMapsPreconditionLossExactly)
{
    using DB::Cas::detail::finalizeConditionalWrite;

    auto classify = [](DB::S3Exception e)
    {
        ThrowOnFinalizeBuffer buf(std::move(e));
        return finalizeConditionalWrite(buf);
    };

    EXPECT_EQ(classify(DB::S3Exception("412", Aws::S3::S3Errors::UNKNOWN, "PreconditionFailed")),
              DB::Cas::detail::ConditionalWriteOutcome::PreconditionLost);
    EXPECT_EQ(classify(DB::S3Exception("404 gone under If-Match", Aws::S3::S3Errors::UNKNOWN, "NoSuchKey")),
              DB::Cas::detail::ConditionalWriteOutcome::PreconditionLost);
    EXPECT_EQ(classify(DB::S3Exception("retries exhausted, no name attached", Aws::S3::S3Errors::NO_SUCH_KEY)),
              DB::Cas::detail::ConditionalWriteOutcome::PreconditionLost);

    ThrowOnFinalizeBuffer unrelated(DB::S3Exception("503", Aws::S3::S3Errors::UNKNOWN, "SlowDown"));
    EXPECT_THROW(finalizeConditionalWrite(unrelated), DB::S3Exception);

    ThrowOnFinalizeBuffer clean;
    EXPECT_EQ(finalizeConditionalWrite(clean), DB::Cas::detail::ConditionalWriteOutcome::Applied);
}

namespace
{

/// A `LocalObjectStorage` whose `readObject` throws `S3Exception(NO_SUCH_KEY)` for a configured
/// physical key, while `tryGetObjectMetadata` still reports that key as PRESENT.
/// This simulates the HEAD→GET race window: the HEAD succeeds, then the object is deleted before
/// the GET arrives.
class NativeReadThrowsNoSuchKeyObjectStorage final : public DB::LocalObjectStorage
{
public:
    using DB::LocalObjectStorage::LocalObjectStorage;

    void setThrowOnRead(const std::string & path)
    {
        throw_on_read_path = path;
    }

    std::unique_ptr<DB::ReadBufferFromFileBase> readObject(
        const DB::StoredObject & object,
        const DB::ReadSettings & read_settings,
        std::optional<size_t> read_hint,
        bool use_external_buffer,
        bool restrict_seek) const override
    {
        if (object.remote_path == throw_on_read_path)
            throw DB::S3Exception(
                "NoSuchKey: The specified key does not exist.",
                Aws::S3::S3Errors::NO_SUCH_KEY);

        return DB::LocalObjectStorage::readObject(object, read_settings, read_hint, use_external_buffer, restrict_seek);
    }

private:
    std::string throw_on_read_path;
};

struct ThrowOnReadFixture
{
    DB::ObjectStoragePtr storage;
    /// Anchored under `storage`'s own root, because `Mode::Native` hands the key to the object storage
    /// verbatim and this one is a real filesystem.
    std::string key;
};

ThrowOnReadFixture makeThrowOnReadStorageForTest(const std::string & key_suffix)
{
    static std::atomic<uint64_t> counter{0};
    const auto unique = std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1));
    const auto root = (std::filesystem::temp_directory_path() / ("cas_midget_unit_" + unique)).string();

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    std::filesystem::create_directories(root, ec);

    DB::LocalObjectStorageSettings settings("test", root, /*read_only_=*/false);
    auto storage = std::make_shared<NativeReadThrowsNoSuchKeyObjectStorage>(std::move(settings));
    const std::string key = DB::Cas::tests::nativeKeyUnder(storage, key_suffix);

    /// Write the object so tryGetObjectMetadata reports it present (HEAD succeeds).
    {
        auto buf = storage->writeObject(DB::StoredObject(key), DB::WriteMode::Rewrite, std::nullopt);
        buf->write("content", 7);
        buf->finalize();
    }

    /// Now configure: future readObject calls for this key will throw NO_SUCH_KEY.
    storage->setThrowOnRead(key);
    return {std::move(storage), key};
}

}

/// `ObjectStorageBackend::get` in `Native` mode: when `tryGetObjectMetadata` (`nativeHead`) reports the
/// key PRESENT but `readObject` throws `S3Exception(NO_SUCH_KEY)` — simulating a deletion in the
/// HEAD→GET window — `get` MUST return `std::nullopt` rather than letting the raw exception escape.
TEST(CASObjectStorageBackend, NativeModeGetReturnsNulloptOnMidGetNoSuchKey)
{
    /// The Native mode backend uses the key verbatim as the physical path (no emu_root prefix), so the
    /// logical key IS the physical one the fixture wrote and armed.
    const auto fixture = makeThrowOnReadStorageForTest("pool/blobs/ab/abcdef0123456789abcdef0123456789");

    auto backend = std::make_shared<ObjectStorageBackend>(fixture.storage, ObjectStorageBackend::Mode::Native);
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();

    /// `head` answers present for a key the fixture failed to place, so without this the nullopt below
    /// would be satisfied vacuously — the mid-read race would go untested and the case would still pass.
    ASSERT_TRUE(op.head(fixture.key, Retry::once()).has_value());

    /// HEAD reports the key present; readObject then throws NO_SUCH_KEY.
    /// Contract: read must return std::nullopt, not propagate the S3Exception.
    const auto result = op.read(fixture.key, Retry::once());
    EXPECT_FALSE(result.has_value());
}

/// codex-review-triage §3.18, finding 19c: the `EmulatedSingleProcess` adapter used to mint tokens
/// from a plain in-process counter (`emu_seq`), NOT actually seeded from the underlying object's etag
/// despite the class comment's claim. After a process restart (modeled here as a fresh
/// `ObjectStorageBackend` instance over the SAME storage) the counter restarts at 0 and can re-mint a
/// value that TEXTUALLY collides with a token persisted before the restart (e.g. a GC condemned-delete
/// token queued for replay), even though the two values name completely different incarnations of the
/// key. `deleteExact` must never let a stale, pre-restart token match a freshly recreated object.
#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(CASObjectStorageBackend, EmuTokenSurvivesProcessRestartAcrossRecreate)
{
    auto storage = tests::makeLocalObjectStorageForTest();

    auto backend1 = std::make_shared<ObjectStorageBackend>(storage, ObjectStorageBackend::Mode::EmulatedSingleProcess);
    CasRequests requests1 = openRequestsForTest(backend1);
    CasOperation op1 = requests1.admit();
    /// A throwaway prior mutation on a DIFFERENT key: with the old counter this advances backend1's
    /// process-wide op counter to 1, so "k/restart"'s own mint below lands on 2 — chosen so it collides
    /// with backend2's post-restart recreate mint further down (also its SECOND op; see there).
    ASSERT_TRUE(std::holds_alternative<Committed>(op1.create("k/other", "junk", Retry::once())));
    const WriteResult restart_create = op1.create("k/restart", "v1", Retry::once());
    ASSERT_TRUE(std::holds_alternative<Committed>(restart_create));
    const Etag stale_token = std::get<Committed>(restart_create).etag;

    /// Simulate a process restart: a brand-new `ObjectStorageBackend` instance (fresh emu state) over
    /// the SAME underlying storage — exactly what happens when the CAS process restarts.
    auto backend2 = std::make_shared<ObjectStorageBackend>(storage, ObjectStorageBackend::Mode::EmulatedSingleProcess);
    CasRequests requests2 = openRequestsForTest(backend2);
    CasOperation op2 = requests2.admit();

    /// Delete and recreate the key through the NEW instance — a fresh incarnation with a fresh mtime.
    /// This is backend2's first-ever op (op 1) then a delete (no mint) then the recreate (op 2) — the
    /// same op-index as `stale_token` above under the old counter, so the two textually collide there.
    const auto current = op2.head("k/restart", Retry::once());
    ASSERT_TRUE(current.has_value());
    ASSERT_EQ(op2.remove("k/restart", current->etag, Retry::once()), Removal::Removed);
    ASSERT_TRUE(std::holds_alternative<Committed>(op2.create("k/restart", "v2-after-restart", Retry::once())));

    /// The pre-restart incarnation must NEVER be usable as a precondition against the post-restart
    /// backend instance, however coincidentally a process-local counter would have re-minted the
    /// identical textual value: an `Etag` carries the identity of the backend that observed it, and the
    /// engine refuses one minted elsewhere before it ever reaches the store (LOGICAL_ERROR), which is a
    /// STRONGER guarantee than the old bare-value comparison this test used to pin. The underlying
    /// same-instance mtime-quantum disambiguation this fixture was ALSO probing is covered directly by
    /// `EmuTokenDisambiguatesSameEtagRewrite`, within one backend instance where the engine's own
    /// cross-backend check cannot pre-empt it. A LOGICAL_ERROR aborts under
    /// DEBUG_OR_SANITIZER_BUILD before it can ever be thrown and caught here; the debug/sanitizer arm
    /// of this split (below) pins the same refusal via EXPECT_DEATH instead.
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR, [&]
    {
        op2.remove("k/restart", stale_token, Retry::once());
    });

    /// The live (post-restart) incarnation must be untouched by the rejected stale delete.
    EXPECT_TRUE(op2.head("k/restart", Retry::once()).has_value());
}
#endif

#if defined(DEBUG_OR_SANITIZER_BUILD)
TEST(CASObjectStorageBackendDeathTest, EmuTokenSurvivesProcessRestartAcrossRecreateAborts)
{
    auto storage = tests::makeLocalObjectStorageForTest();

    auto backend1 = std::make_shared<ObjectStorageBackend>(storage, ObjectStorageBackend::Mode::EmulatedSingleProcess);
    CasRequests requests1 = openRequestsForTest(backend1);
    CasOperation op1 = requests1.admit();
    ASSERT_TRUE(std::holds_alternative<Committed>(op1.create("k/other", "junk", Retry::once())));
    const WriteResult restart_create = op1.create("k/restart", "v1", Retry::once());
    ASSERT_TRUE(std::holds_alternative<Committed>(restart_create));
    const Etag stale_token = std::get<Committed>(restart_create).etag;

    auto backend2 = std::make_shared<ObjectStorageBackend>(storage, ObjectStorageBackend::Mode::EmulatedSingleProcess);
    CasRequests requests2 = openRequestsForTest(backend2);
    CasOperation op2 = requests2.admit();

    const auto current = op2.head("k/restart", Retry::once());
    ASSERT_TRUE(current.has_value());
    ASSERT_EQ(op2.remove("k/restart", current->etag, Retry::once()), Removal::Removed);
    ASSERT_TRUE(std::holds_alternative<Committed>(op2.create("k/restart", "v2-after-restart", Retry::once())));

    /// See EmuTokenSurvivesProcessRestartAcrossRecreate above for the property under test; a
    /// LOGICAL_ERROR aborts the process under DEBUG_OR_SANITIZER_BUILD, so this arm pins the refusal
    /// via EXPECT_DEATH instead of an exception.
    EXPECT_DEATH(
        { op2.remove("k/restart", stale_token, Retry::once()); },
        "cannot be the precondition for");
}
#endif

/// `list`'s `EmulatedSingleProcess` branch must surface the SAME incarnation value `head` would for the
/// same key. An earlier defect minted the listed value under the wrong dialect regardless of `mode`,
/// so a list-derived value could never satisfy an emulated `remove`/`replace` precondition: a
/// fail-safe leak (never a wrong delete), but every consumer of listed values (GC namespace cleanup,
/// `deletePrefixWholesale`, orphan sweep, decommission drain) always saw a mismatch against a LOCAL pool.
TEST(CASObjectStorageBackend, EmulatedListTokenMatchesHeadToken)
{
    auto backend = std::make_shared<ObjectStorageBackend>(
        tests::makeLocalObjectStorageForTest(), ObjectStorageBackend::Mode::EmulatedSingleProcess);
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();

    ASSERT_TRUE(std::holds_alternative<Committed>(op.create("k/listed", "body", Retry::once())));

    const auto head = op.head("k/listed", Retry::once());
    ASSERT_TRUE(head.has_value());
    ASSERT_EQ(head->etag.dialect(), Dialect::Emulated);

    const ListPage page = op.list("k/", "", /*limit=*/10, Retry::once());
    ASSERT_EQ(page.keys.size(), 1u);
    ASSERT_TRUE(page.keys.front().etag.has_value());
    EXPECT_EQ(*page.keys.front().etag, head->etag);
}

namespace
{

/// A `LocalObjectStorage` whose reported etag never changes -- simulating a filesystem/clock whose
/// mtime resolution is too coarse to separate two writes issued back-to-back (the "same mtime
/// quantum" hazard flagged for the etag-seeded emu token: two DIFFERENT incarnations must still mint
/// DIFFERENT tokens even when the storage's own etag does not advance between them).
class FixedEtagLocalObjectStorage final : public DB::LocalObjectStorage
{
public:
    using DB::LocalObjectStorage::LocalObjectStorage;

    std::optional<DB::ObjectMetadata> tryGetObjectMetadata(const std::string & path, bool with_tags) const override
    {
        auto metadata = DB::LocalObjectStorage::tryGetObjectMetadata(path, with_tags);
        if (metadata)
            metadata->etag = "same-quantum";
        return metadata;
    }
};

DB::ObjectStoragePtr makeFixedEtagStorageForTest()
{
    static std::atomic<uint64_t> counter{0};
    const auto unique = std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1));
    const auto root = (std::filesystem::temp_directory_path() / ("cas_fixed_etag_unit_" + unique)).string();

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    std::filesystem::create_directories(root, ec);

    DB::LocalObjectStorageSettings settings("test", root, /*read_only_=*/false);
    return std::make_shared<FixedEtagLocalObjectStorage>(std::move(settings));
}

}

/// The mtime-resolution guard (codex-review-triage §3.18, 19c step 4): two writes to the same key
/// whose underlying etag does not advance between them (stubbed here to model a coarse clock) must
/// still mint DISTINCT emulated tokens, and a stale token from the first incarnation must not match
/// the second.
TEST(CASObjectStorageBackend, EmuTokenDisambiguatesSameEtagRewrite)
{
    auto backend = std::make_shared<ObjectStorageBackend>(makeFixedEtagStorageForTest(), ObjectStorageBackend::Mode::EmulatedSingleProcess);
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();

    const WriteResult put1 = op.create("k/tick", "v1", Retry::once());
    ASSERT_TRUE(std::holds_alternative<Committed>(put1));
    const Etag inc1 = std::get<Committed>(put1).etag;
    const WriteResult put2 = op.replace("k/tick", "v2", inc1, Retry::once());
    ASSERT_TRUE(std::holds_alternative<Committed>(put2));
    const Etag inc2 = std::get<Committed>(put2).etag;

    EXPECT_NE(inc1, inc2);
    EXPECT_EQ(inc1.dialect(), Dialect::Emulated);
    EXPECT_EQ(inc2.dialect(), Dialect::Emulated);

    /// A stale delete using the FIRST incarnation must not match the live (second) one.
    EXPECT_EQ(op.remove("k/tick", inc1, Retry::once()), Removal::Mismatch);
    EXPECT_TRUE(op.head("k/tick", Retry::once()).has_value());
}

TEST(CASObjectStorageBackend, PublishBlobEmulatedDisambiguatesSameEtagFromStaleDelete)
{
    auto backend = std::make_shared<ObjectStorageBackend>(makeFixedEtagStorageForTest(), ObjectStorageBackend::Mode::EmulatedSingleProcess);
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const String key = "k/publish-tick";

    ASSERT_TRUE(std::holds_alternative<Committed>(op.create(key, "old-complete-body", Retry::once())));
    const auto stale = op.head(key, Retry::once());
    ASSERT_TRUE(stale.has_value());
    const Etag stale_token = stale->etag;

    op.publish(streamingPublication(key, "fresh-envelope", "payload", 7), Retry::once());

    const auto published = op.head(key, Retry::once());
    ASSERT_TRUE(published.has_value());
    EXPECT_NE(published->etag, stale_token);
    EXPECT_EQ(published->etag.dialect(), Dialect::Emulated);
    EXPECT_EQ(op.remove(key, stale_token, Retry::once()), Removal::Mismatch);

    const auto live = op.read(key, Retry::once());
    ASSERT_TRUE(live.has_value());
    EXPECT_EQ(live->bytes, "fresh-envelopepayload");
    EXPECT_EQ(live->etag, published->etag);
}

namespace
{

/// A `LocalObjectStorage` that always reports a caller-supplied, fixed NUMERIC etag string — lets a
/// test pin `emuMintToken`'s etag input to a precise, controlled nanosecond value (an old timestamp
/// vs. one close to "now") regardless of the real filesystem clock. Used to test the
/// `emu_token_state` erase-on-delete bound (codex-review-triage §3.18, Important #1): the entry
/// must be erased only when the deleted incarnation's own etag is comfortably in the past.
class FixedNumericEtagLocalObjectStorage final : public DB::LocalObjectStorage
{
public:
    FixedNumericEtagLocalObjectStorage(DB::LocalObjectStorageSettings settings, String etag_)
        : DB::LocalObjectStorage(std::move(settings)), etag(std::move(etag_))
    {
    }

    std::optional<DB::ObjectMetadata> tryGetObjectMetadata(const std::string & path, bool with_tags) const override
    {
        auto metadata = DB::LocalObjectStorage::tryGetObjectMetadata(path, with_tags);
        if (metadata)
            metadata->etag = etag;
        return metadata;
    }

private:
    String etag;
};

DB::ObjectStoragePtr makeFixedNumericEtagStorageForTest(const String & etag)
{
    static std::atomic<uint64_t> counter{0};
    const auto unique = std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1));
    const auto root = (std::filesystem::temp_directory_path() / ("cas_fixed_numeric_etag_unit_" + unique)).string();

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    std::filesystem::create_directories(root, ec);

    DB::LocalObjectStorageSettings settings("test", root, /*read_only_=*/false);
    return std::make_shared<FixedNumericEtagLocalObjectStorage>(std::move(settings), etag);
}

class ClockEtagLocalObjectStorage final : public DB::LocalObjectStorage
{
public:
    ClockEtagLocalObjectStorage(DB::LocalObjectStorageSettings settings, std::shared_ptr<std::atomic<uint64_t>> now_ns_)
        : DB::LocalObjectStorage(std::move(settings)), now_ns(std::move(now_ns_))
    {
    }

    std::optional<DB::ObjectMetadata> tryGetObjectMetadata(const std::string & path, bool with_tags) const override
    {
        auto metadata = DB::LocalObjectStorage::tryGetObjectMetadata(path, with_tags);
        if (metadata)
            metadata->etag = std::to_string(now_ns->load());
        return metadata;
    }

private:
    std::shared_ptr<std::atomic<uint64_t>> now_ns;
};

DB::ObjectStoragePtr makeClockEtagStorageForTest(const std::shared_ptr<std::atomic<uint64_t>> & now_ns)
{
    static std::atomic<uint64_t> counter{0};
    const auto unique = std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1));
    const auto root = (std::filesystem::temp_directory_path() / ("cas_clock_etag_unit_" + unique)).string();

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    std::filesystem::create_directories(root, ec);

    DB::LocalObjectStorageSettings settings("test", root, /*read_only_=*/false);
    return std::make_shared<ClockEtagLocalObjectStorage>(std::move(settings), now_ns);
}

}

/// codex-review-triage §3.18, Important #1: `emu_token_state` must be BOUNDED, not grow for the
/// lifetime of the backend instance. `deleteExact` erases a key's entry only when its last-minted
/// etag is comfortably (>= 2s) in the past — recent enough to still collide with an immediate
/// same-process recreate must be RETAINED (the mtime-quantum guard stays intact).
TEST(CASObjectStorageBackend, DeleteExactErasesEmuTokenStateOnlyWhenEtagIsComfortablyOld)
{
    /// An etag far in the past (nanoseconds since epoch, ~2001): delete must erase the entry, so an
    /// immediate recreate reporting the SAME fixed etag is treated as a brand-new incarnation (bare
    /// etag, no disambiguator) rather than a same-quantum tie with the just-consumed delete token.
    {
        const String old_etag = "1000000000000000000";
        auto backend = std::make_shared<ObjectStorageBackend>(makeFixedNumericEtagStorageForTest(old_etag), ObjectStorageBackend::Mode::EmulatedSingleProcess);
        CasRequests requests = openRequestsForTest(backend);
        CasOperation op = requests.admit();

        const WriteResult put1 = op.create("k/old", "v1", Retry::once());
        ASSERT_TRUE(std::holds_alternative<Committed>(put1));
        ASSERT_EQ(PersistedEtag::capture(std::get<Committed>(put1).etag).value, old_etag);
        ASSERT_EQ(op.remove("k/old", std::get<Committed>(put1).etag, Retry::once()), Removal::Removed);

        const WriteResult put2 = op.create("k/old", "v2", Retry::once());
        ASSERT_TRUE(std::holds_alternative<Committed>(put2));
        EXPECT_EQ(PersistedEtag::capture(std::get<Committed>(put2).etag).value, old_etag)
            << "entry should have been erased on delete (etag comfortably old), "
               "so the recreate mints the bare etag, not a disambiguated one";
    }

    /// An etag within the safety margin of "now": delete must RETAIN the entry, so the same
    /// immediate-recreate scenario still gets disambiguated -- the guard this bound must not break.
    {
        const auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        const String recent_etag = std::to_string(now_ns);
        auto backend = std::make_shared<ObjectStorageBackend>(makeFixedNumericEtagStorageForTest(recent_etag), ObjectStorageBackend::Mode::EmulatedSingleProcess);
        CasRequests requests = openRequestsForTest(backend);
        CasOperation op = requests.admit();

        const WriteResult put1 = op.create("k/fresh", "v1", Retry::once());
        ASSERT_TRUE(std::holds_alternative<Committed>(put1));
        ASSERT_EQ(PersistedEtag::capture(std::get<Committed>(put1).etag).value, recent_etag);
        ASSERT_EQ(op.remove("k/fresh", std::get<Committed>(put1).etag, Retry::once()), Removal::Removed);

        const WriteResult put2 = op.create("k/fresh", "v2", Retry::once());
        ASSERT_TRUE(std::holds_alternative<Committed>(put2));
        EXPECT_EQ(PersistedEtag::capture(std::get<Committed>(put2).etag).value, recent_etag + "#1")
            << "entry should have been RETAINED on delete (etag recent), "
               "so the recreate is disambiguated against it";
    }
}

TEST(CASObjectStorageBackend, EmuTokenStateEventuallyPrunesDistinctShortLivedKeys)
{
    constexpr uint64_t start_ns = 1'700'000'000'000'000'000ULL;
    constexpr uint64_t step_ns = 100'000'000ULL;
    constexpr size_t key_count = 128;
    constexpr size_t expected_recent_key_bound = 24;

    auto now_ns = std::make_shared<std::atomic<uint64_t>>(start_ns);
    auto backend = std::make_shared<ObjectStorageBackend>(makeClockEtagStorageForTest(now_ns), ObjectStorageBackend::Mode::EmulatedSingleProcess);
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();

    for (size_t i = 0; i < key_count; ++i)
    {
        const uint64_t current_ns = start_ns + i * step_ns;
        now_ns->store(current_ns);
        backend->setEmuNowNsForTest(current_ns);

        const String key = "k/short-lived-" + std::to_string(i);
        const WriteResult put = op.create(key, "body", Retry::once());
        ASSERT_TRUE(std::holds_alternative<Committed>(put));
        ASSERT_EQ(op.remove(key, std::get<Committed>(put).etag, Retry::once()), Removal::Removed);
    }

    const uint64_t sweep_ns = start_ns + key_count * step_ns + 2'000'000'000ULL;
    now_ns->store(sweep_ns);
    backend->setEmuNowNsForTest(sweep_ns);
    const WriteResult trigger = op.create("k/sweep-trigger", "body", Retry::once());
    ASSERT_TRUE(std::holds_alternative<Committed>(trigger));
    ASSERT_EQ(op.remove("k/sweep-trigger", std::get<Committed>(trigger).etag, Retry::once()), Removal::Removed);

    EXPECT_LE(backend->emuTokenStateSizeForTest(), expected_recent_key_bound)
        << "token state should track only the bounded recent-key window, not all " << key_count << " deleted keys";
}

TEST(CASObjectStorageBackend, EnsureBackendMatchesBudgetAcceptsAMatchingHandoff)
{
    CasRequestBudget budget;
    budget.attempt_timeout_ms = 4000;
    budget.connect_timeout_cap_ms = 900;
    auto backend = std::make_shared<ObjectStorageBackend>(
        tests::makeLocalObjectStorageForTest(), ObjectStorageBackend::Mode::EmulatedSingleProcess,
        /*single_attempt_control_plane_=*/false, budget.attempt_timeout_ms, *budget.connect_timeout_cap_ms);
    EXPECT_NO_THROW(ensureBackendMatchesBudget(*backend, budget));
}

TEST(CASObjectStorageBackend, EnsureBackendMatchesBudgetRejectsAMismatchedConnectCap)
{
    CasRequestBudget budget;
    budget.attempt_timeout_ms = 4000;
    budget.connect_timeout_cap_ms = 900;
    /// The backend is built with a DIFFERENT connect cap than the budget it will be paired with --
    /// exactly the handoff mistake the production check at `openPoolView` guards against.
    auto backend = std::make_shared<ObjectStorageBackend>(
        tests::makeLocalObjectStorageForTest(), ObjectStorageBackend::Mode::EmulatedSingleProcess,
        /*single_attempt_control_plane_=*/false, budget.attempt_timeout_ms, /*connect_timeout_cap_ms_=*/1500);
    /// The mismatch is a programmer error (LOGICAL_ERROR): under DEBUG_OR_SANITIZER_BUILD it aborts the
    /// process before it can be caught, so that arm pins the refusal via EXPECT_DEATH.
#if defined(DEBUG_OR_SANITIZER_BUILD)
    EXPECT_DEATH({ ensureBackendMatchesBudget(*backend, budget); }, "does not match the pool's request budget");
#else
    EXPECT_THROW(ensureBackendMatchesBudget(*backend, budget), DB::Exception);
#endif
}

TEST(CASObjectStorageBackend, EnsureBackendMatchesBudgetRejectsAMismatchedAttemptTimeout)
{
    CasRequestBudget budget;
    budget.attempt_timeout_ms = 4000;
    budget.connect_timeout_cap_ms = 900;
    auto backend = std::make_shared<ObjectStorageBackend>(
        tests::makeLocalObjectStorageForTest(), ObjectStorageBackend::Mode::EmulatedSingleProcess,
        /*single_attempt_control_plane_=*/false, /*attempt_timeout_ms_=*/6000, *budget.connect_timeout_cap_ms);
    /// Same split as the connect-cap twin above: a LOGICAL_ERROR aborts under DEBUG_OR_SANITIZER_BUILD.
#if defined(DEBUG_OR_SANITIZER_BUILD)
    EXPECT_DEATH({ ensureBackendMatchesBudget(*backend, budget); }, "does not match the pool's request budget");
#else
    EXPECT_THROW(ensureBackendMatchesBudget(*backend, budget), DB::Exception);
#endif
}

/// `NativeRejectsWrongDialectTokenBeforeTouchingTheWire` is deleted here: it built a `Token{value,
/// Dialect::Emulated}` holding a NATIVE backend's live wire value under the WRONG dialect tag, to prove
/// the mismatch was caught locally rather than forwarded to the wire. `Etag` no longer admits that
/// construction -- it is minted ONLY by `CasRequests::mint`/`tryMint`, always from `backend->dialect()`,
/// so a caller can never hold an `Etag` tagged with a dialect other than the backend that observed it.
/// The property this test pinned ("a value observed under one dialect can never be mistaken for another
/// backend's incarnation") is now enforced by the type itself rather than by a runtime comparison; see
/// `CasRequests::valueFor`'s backend-identity check (also exercised, from the other side, by
/// `EmuTokenSurvivesProcessRestartAcrossRecreate` above).
///
/// `CASBackendGrammar.RejectsEmptyStarAndListTokensOnEveryMutation` and its
/// `CASBackendGrammarDeathTest` sibling are deleted for the same reason: they built literal
/// `Token{"", ...}` / `Token{"*", ...}` / `Token{"\"a\", \"b\"", ...}` values to drive `putOverwrite`/
/// `casPut`/`deleteExact` into the primitive's `LOGICAL_ERROR` grammar guard. `Etag::mint`/`tryMint`
/// refuse to construct an `Etag` from a malformed value in the first place (`CORRUPTED_DATA`), so no
/// caller reaching the primitives through `CasOperation` can ever hold one -- the grammar guard inside
/// `ObjectStorageBackend::write`/`removeUnder` is unreachable from the public engine surface and stays
/// as defense in depth only. The empty/`*`/quoted-list token cases the deleted tests drove are covered
/// at the `Etag::mint`/`tryMint` boundary by `CASIncarnation.GrammarRefusesTheNineWays`
/// (`gtest_cas_requests.cpp`); the grammar predicate itself remains directly pinned by
/// `CASBackendGrammar.GenerationDialectAcceptsOnlyCanonicalPositiveDecimal` above.

#endif
