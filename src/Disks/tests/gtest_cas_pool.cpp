#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPoolMetaFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPartManifestFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasFsck.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <Poco/Exception.h>
#include <Poco/StreamChannel.h>
#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <future>
#include <latch>
#include <limits>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <vector>

namespace DB::ErrorCodes
{
extern const int ABORTED;
extern const int BAD_ARGUMENTS;
extern const int CORRUPTED_DATA;
extern const int NOT_IMPLEMENTED;
extern const int UNKNOWN_FORMAT_VERSION;
extern const int FILE_DOESNT_EXIST;
extern const int UNKNOWN_EXCEPTION;
extern const int NETWORK_ERROR;
}

namespace ProfileEvents
{
extern const Event CASRefRecoveryEpochSealed;
extern const Event CASMountExclusivityViolation;
extern const Event CASMountLeaseLost;
extern const Event CASMountReleaseSkippedForeignOccupant;
extern const Event CASRemountAttempts;
extern const Event CASRemountSucceeded;
extern const Event CASRemountFailed;
}

using namespace DB::Cas;
using DB::Cas::tests::blobEntryFor;
using DB::Cas::tests::expectThrowsCode;
using DB::Cas::tests::idOf;
using DB::Cas::tests::SharedWaitLog;
using DB::Cas::tests::u128Of;

namespace
{
/// Counts mutating backend calls so a test can assert an open path is write-free.
class WriteCountingBackend final : public DB::Cas::Backend
{
public:
    explicit WriteCountingBackend(std::shared_ptr<DB::Cas::Backend> inner_) : inner(std::move(inner_)) {}
    size_t writes = 0;

    bool supportsListTokens() const override { return inner->supportsListTokens(); }

    /// Every write reaches the store through these primitives, so `writes` sees it whichever verb
    /// (`create`/`replace`/`remove`/`publish`) issued it.
    std::optional<Raw> read(const String & key, TransportAccess & access) override { return inner->read(key, access); }
    std::optional<RawMeta> head(const String & key, TransportAccess & access) override { return inner->head(key, access); }
    RawListPage list(const String & prefix, const String & cursor, size_t limit, TransportAccess & access) override { return inner->list(prefix, cursor, limit, access); }
    RawRemoval remove(const String & key, const String & expected_value, TransportAccess & access) override
    {
        ++writes;
        return inner->remove(key, expected_value, access);
    }
    void removeManyWriteOnce(const std::vector<WriteOnceKey> & keys, TransportAccess & access) override
    {
        ++writes;
        inner->removeManyWriteOnce(keys, access);
    }
    std::expected<String, RawConflict> write(const String & key, const String & bytes,
                                             const std::optional<String> & expected_value, TransportAccess & access) override
    {
        ++writes;
        return inner->write(key, bytes, expected_value, access);
    }
    std::unique_ptr<DB::ReadBuffer> stream(const String & key, TransportAccess & access) override { return inner->stream(key, access); }
    void publish(const BlobPublishRequest & request, TransportAccess & access) override
    {
        ++writes;
        inner->publish(request, access);
    }
    Dialect dialect() const override { return inner->dialect(); }
private:
    std::shared_ptr<DB::Cas::Backend> inner;
};

/// A one-shot `create`, asserting it committed (mirrors the retired `backend.putIfAbsent(key, bytes)`).
void createObj(Backend & backend, const String & key, const String & bytes)
{
    DB::Cas::tests::OperationForTest op(backend);
    ASSERT_TRUE(std::holds_alternative<Committed>((*op).create(key, bytes, Retry::once())));
}

/// An exact read (mirrors the retired `backend.get(key)`).
std::optional<Object> readObj(Backend & backend, const String & key)
{
    DB::Cas::tests::OperationForTest op(backend);
    return (*op).read(key, Retry::standard());
}

/// A HEAD (mirrors the retired `backend.head(key)`).
std::optional<Meta> headObj(Backend & backend, const String & key)
{
    DB::Cas::tests::OperationForTest op(backend);
    return (*op).head(key, Retry::standard());
}

/// Publish one part `ref` through the REAL PartWriteTxn write path: stage a manifest holding a single content
/// blob whose payload is `payload`, precommit-add into the owning shard, then promote precommit ->
/// committed. Returns the published ManifestId. This is the canonical write-side fixture for the
/// read-path tests (the same shape as `publishPart` in gtest_cas_gc_log.cpp). The manifest entry path
/// is `data.bin` unless `entry_path` overrides it.
ManifestId publishPart(
    const PoolPtr & s, const String & ns, const String & ref, const String & payload,
    const String & entry_path = "data.bin")
{
    const RootNamespace nsr{ns};
    PartWriteInfo info;
    info.intended_ref = ns + "/" + ref;
    auto build = s->beginPartWrite(info);

    ManifestEntry e;
    e.path = entry_path;
    e.placement = EntryPlacement::Blob;
    e.ref = DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(u128Of(payload))};

    e.blob_size = payload.size();

    const ManifestId id = build->stageManifest({e});
    build->precommitAdd(nsr, ref, id);
    build->putBlob(idOf(payload), BlobSource::fromString(payload));
    build->promote(nsr, ref, build->buildId(), id);
    return id;
}

/// A ManifestRef carrying a unique instance id derived from `tag` (all fields explicit so the
/// missing-designated-field-initializer warning never fires). The writer/build fields are stable test
/// constants — the read path keys identity by the full ref, so any consistent choice works here.
ManifestRef manifestRefFor(const String & tag)
{
    uint32_t ordinal = 1;
    for (char c : tag)
        ordinal = ordinal * 131 + static_cast<unsigned char>(c);
    ordinal = ordinal % 999999 + 1;
    return ManifestRef{
        .writer_epoch = 1,
        .build_sequence = 1,
        .manifest_ordinal = ordinal};
}

/// Publish a part holding the given manifest entries verbatim through the real PartWriteTxn. Used by read-path
/// lookup/list tests that want a precise multi-entry manifest. Each Blob entry's body MUST be present at
/// promote: the promote gate revalidates EVERY blob leaf with a HEAD and fails closed on an absent body.
/// So write a blob body for each Blob entry (addressed by its hash) and record it as W-EVIDENCE before
/// staging. Inline entries need no body. Returns the published ManifestId.
ManifestId publishPartWithEntries(
    const PoolPtr & s, const String & ns, const String & ref, std::vector<ManifestEntry> entries)
{
    const RootNamespace nsr{ns};
    PartWriteInfo info;
    info.intended_ref = ns + "/" + ref;
    auto build = s->beginPartWrite(info);
    for (const auto & e : entries)
        if (e.placement == EntryPlacement::Blob)
        {
            /// Materialize the blob body so the promote-time HEAD revalidation succeeds, then record the
            /// tokenless W-EVIDENCE dep (the gate re-observes the current token at promote).
            DB::Cas::tests::writeBlobBody(*s->poolBackendPtr(), s->layout(), e.ref.digest.toU128());
            build->adoptEvidence(e);
        }
    const ManifestId id = build->stageManifest(std::move(entries));
    build->precommitAdd(nsr, ref, id);
    build->promote(nsr, ref, build->buildId(), id);
    return id;
}
}

TEST(CASPool, ReadOnlyOpenSkipsProbe)
{
    auto shared = std::make_shared<DB::Cas::InMemoryBackend>();

    DB::Cas::PoolConfig cfg;
    cfg.pool_prefix = "pool";
    cfg.server_id = DB::UInt128(1);
    cfg.server_root_id = "test";
    /// Writable open: creates _pool_meta and runs the probe (which writes+cleans up).
    DB::Cas::Pool::open(std::make_shared<WriteCountingBackend>(shared), cfg);

    /// Read-only re-open over the SAME data must perform ZERO writes (no probe, meta already present).
    auto counter = std::make_shared<WriteCountingBackend>(shared);
    DB::Cas::PoolConfig ro = cfg;
    ro.read_only = true;
    auto store = DB::Cas::Pool::open(counter, ro);
    EXPECT_EQ(counter->writes, 0u);
    ASSERT_NE(store, nullptr);
}

namespace
{
/// Records whether any MUTATING op touched a `_probe/` key, so a test can assert an open ran (or
/// skipped) the capability probe. Mirrors WriteCountingBackend above but keys on the probe subtree.
class ProbeWatchingBackend final : public DB::Cas::Backend
{
public:
    explicit ProbeWatchingBackend(std::shared_ptr<DB::Cas::Backend> inner_) : inner(std::move(inner_)) {}
    bool probe_touched = false;

    bool supportsListTokens() const override { return inner->supportsListTokens(); }

    /// Every mutation reaches the store through these primitives, so a probe-key touch is noted
    /// whichever verb (`create`/`replace`/`remove`/`publish`) issued it.
    std::optional<Raw> read(const String & key, TransportAccess & access) override { return inner->read(key, access); }
    std::optional<RawMeta> head(const String & key, TransportAccess & access) override { return inner->head(key, access); }
    RawListPage list(const String & prefix, const String & cursor, size_t limit, TransportAccess & access) override { return inner->list(prefix, cursor, limit, access); }
    RawRemoval remove(const String & key, const String & expected_value, TransportAccess & access) override
    {
        note(key);
        return inner->remove(key, expected_value, access);
    }
    void removeManyWriteOnce(const std::vector<WriteOnceKey> & keys, TransportAccess & access) override
    {
        for (const WriteOnceKey & key : keys)
            note(key.str());
        inner->removeManyWriteOnce(keys, access);
    }
    std::expected<String, RawConflict> write(const String & key, const String & bytes,
                                             const std::optional<String> & expected_value, TransportAccess & access) override
    {
        note(key);
        return inner->write(key, bytes, expected_value, access);
    }
    std::unique_ptr<DB::ReadBuffer> stream(const String & key, TransportAccess & access) override { return inner->stream(key, access); }
    void publish(const BlobPublishRequest & request, TransportAccess & access) override
    {
        note(request.destination_key);
        inner->publish(request, access);
    }
    Dialect dialect() const override { return inner->dialect(); }
private:
    void note(const String & k) { if (k.find("/_probe/") != String::npos) probe_touched = true; }
    std::shared_ptr<DB::Cas::Backend> inner;
};
}

TEST(CASPool, SkipAccessCheckOpenSkipsProbeButStaysWritable)
{
    auto shared = std::make_shared<DB::Cas::InMemoryBackend>();

    DB::Cas::PoolConfig cfg;
    cfg.pool_prefix = "pool";
    cfg.server_id = DB::UInt128(1);
    cfg.server_root_id = "srv-1";

    /// Baseline: a normal writable open runs the capability probe (PUT+delete of `_probe/` keys).
    {
        auto watch = std::make_shared<ProbeWatchingBackend>(shared);
        auto s = DB::Cas::Pool::open(watch, cfg);
        ASSERT_NE(s, nullptr);
        EXPECT_TRUE(watch->probe_touched) << "the probe must run by default";
    }

    /// skip_access_check open ("start now, fix later"): NO probe I/O, yet still a WRITABLE mount
    /// (owner/epoch/mount/watermark bootstrap writes still happen — unlike a read_only open, which is
    /// a total no-op). Distinct root over the same (now-created) pool.
    {
        auto watch = std::make_shared<ProbeWatchingBackend>(shared);
        DB::Cas::PoolConfig sac = cfg;
        sac.server_id = DB::UInt128(2);
        sac.server_root_id = "srv-2";
        sac.skip_access_check = true;
        auto s = DB::Cas::Pool::open(watch, sac);
        ASSERT_NE(s, nullptr);
        EXPECT_FALSE(watch->probe_touched) << "skip_access_check must perform no probe I/O";

        /// Prove the mount is genuinely WRITABLE, not merely non-null — a read_only open would also
        /// satisfy the two assertions above. Publish a part through the real PartWriteTxn write path
        /// (beginPartWrite/putBlob/stageManifest/precommitAdd/promote) and read it back.
        publishPart(s, "srv-2/tbl", "part_1", "payload-x");
        const auto r = s->resolveRef(DB::Cas::RootNamespace{"srv-2/tbl"}, "part_1");
        ASSERT_TRUE(r.has_value()) << "skip_access_check open must accept real writes, not just open";
    }
}

namespace
{
/// Delegates every storage operation to `inner` and leaves the mount-time capability gates at their
/// permissive defaults, so a subclass can make exactly ONE gate throw and a test can attribute a
/// refused mount to that gate alone.
class ForwardingBackend : public DB::Cas::Backend
{
public:
    explicit ForwardingBackend(std::shared_ptr<DB::Cas::Backend> inner_) : inner(std::move(inner_)) {}

    bool supportsListTokens() const override { return inner->supportsListTokens(); }

    /// The transport primitives forward to `inner`. Declared because `Backend` declares them pure.
    std::optional<Raw> read(const String & key, TransportAccess & access) override { return inner->read(key, access); }
    std::optional<RawMeta> head(const String & key, TransportAccess & access) override { return inner->head(key, access); }
    RawListPage list(const String & prefix, const String & cursor, size_t limit, TransportAccess & access) override { return inner->list(prefix, cursor, limit, access); }
    RawRemoval remove(const String & key, const String & expected_value, TransportAccess & access) override { return inner->remove(key, expected_value, access); }
    void removeManyWriteOnce(const std::vector<WriteOnceKey> & keys, TransportAccess & access) override { inner->removeManyWriteOnce(keys, access); }
    std::expected<String, RawConflict> write(const String & key, const String & bytes,
                                             const std::optional<String> & expected_value, TransportAccess & access) override
    {
        return inner->write(key, bytes, expected_value, access);
    }
    std::unique_ptr<DB::ReadBuffer> stream(const String & key, TransportAccess & access) override { return inner->stream(key, access); }
    void publish(const BlobPublishRequest & request, TransportAccess & access) override { inner->publish(request, access); }
    Dialect dialect() const override { return inner->dialect(); }

private:
    std::shared_ptr<DB::Cas::Backend> inner;
};

/// A backend whose checkConditionalWriteSingleAttemptSupport ALWAYS throws — a stand-in for a
/// Native-mode backend with no working single-attempt client (see
/// ObjectStorageBackend::checkConditionalWriteSingleAttemptSupport). Pins that skip_access_check does
/// NOT bypass this gate: the regression this guards is reverting Pool::open's skip_access_check
/// branch back to the naive "wrap the whole probe" shape, which would silently skip this check too.
class ThrowingSingleAttemptBackend final : public ForwardingBackend
{
public:
    using ForwardingBackend::ForwardingBackend;

    void checkConditionalWriteSingleAttemptSupport() override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "test: no single-attempt client");
    }
};

/// A backend whose store-level preconditions refuse the pool outright — a stand-in for a versioning or
/// dialect combination `ObjectStorageBackend::checkPoolPreconditions` rejects.
class ThrowingPoolPreconditionsBackend final : public ForwardingBackend
{
public:
    using ForwardingBackend::ForwardingBackend;

    void checkPoolPreconditions() override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "test: pool preconditions refused");
    }
};

/// A backend that forbids skipping the access-check battery — a stand-in for the writable
/// generation-dialect (GCS) backend (see ObjectStorageBackend::checkSkipAccessCheckSupport).
class ThrowingSkipAccessCheckBackend final : public ForwardingBackend
{
public:
    using ForwardingBackend::ForwardingBackend;

    void checkSkipAccessCheckSupport() override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "test: this backend forbids skip_access_check");
    }
};

DB::Cas::PoolConfig writablePoolConfigForTest()
{
    DB::Cas::PoolConfig cfg;
    cfg.pool_prefix = "pool";
    cfg.server_id = DB::UInt128(1);
    cfg.server_root_id = "test";
    return cfg;
}
}

TEST(CASPool, SkipAccessCheckStillEnforcesSingleAttemptGate)
{
    auto backend = std::make_shared<ThrowingSingleAttemptBackend>(std::make_shared<DB::Cas::InMemoryBackend>());

    DB::Cas::PoolConfig cfg = writablePoolConfigForTest();
    cfg.skip_access_check = true;

    /// skip_access_check must NOT bypass checkConditionalWriteSingleAttemptSupport (RFC
    /// cas-s3-timeout-retry-control): a writable open still refuses to mount on a backend that cannot
    /// prove single-attempt conditional-write support, exactly as it does without skip_access_check.
    EXPECT_THROW(DB::Cas::Pool::open(backend, cfg), DB::Exception);
}

/// A backend that forbids skipping the battery refuses the writable mount outright. Asserting the
/// gate's own message, not merely that open threw: Pool::open has many other refusals, and a mount
/// that failed for one of those would satisfy a bare EXPECT_THROW.
TEST(CASPool, SkipAccessCheckRefusedByBackendFailsTheWritableMount)
{
    auto backend = std::make_shared<ThrowingSkipAccessCheckBackend>(std::make_shared<DB::Cas::InMemoryBackend>());

    DB::Cas::PoolConfig cfg = writablePoolConfigForTest();
    cfg.skip_access_check = true;

    try
    {
        DB::Cas::Pool::open(backend, cfg);
        FAIL() << "expected the skip_access_check gate to refuse the mount";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::NOT_IMPLEMENTED);
        EXPECT_NE(e.message().find("forbids skip_access_check"), std::string::npos) << "actual message: " << e.message();
    }
}

/// The discriminator for the test above: the SAME backend opens fine without the flag, so that
/// refusal came from the new gate rather than from anything else in the open path. It also pins the
/// gate's scope — it is consulted only where skip_access_check is honoured, so a mount that runs the
/// battery is unaffected.
TEST(CASPool, BackendForbiddingSkipAccessCheckStillOpensWhenTheBatteryRuns)
{
    auto backend = std::make_shared<ThrowingSkipAccessCheckBackend>(std::make_shared<DB::Cas::InMemoryBackend>());

    DB::Cas::PoolConfig cfg = writablePoolConfigForTest();
    cfg.background_watermark = false;
    ASSERT_FALSE(cfg.skip_access_check);

    auto store = DB::Cas::Pool::open(backend, cfg);
    ASSERT_NE(store, nullptr);
}

/// The ORDINARY writable mount -- the one that runs the battery -- must still be refused by the two
/// store-level gates. They used to be the capability probe's own first two steps; they are the caller's
/// now, and nothing else in the open path would notice if the caller stopped asking. The write counter is
/// what makes each of these a fence rather than a bare `EXPECT_THROW`: `Pool::open` refuses for many
/// reasons, but only a refusal BEFORE the battery leaves the store unwritten.
TEST(CASPool, WritableOpenRunsThePoolPreconditionGateBeforeTheBattery)
{
    auto counting = std::make_shared<WriteCountingBackend>(std::make_shared<DB::Cas::InMemoryBackend>());
    auto backend = std::make_shared<ThrowingPoolPreconditionsBackend>(counting);

    DB::Cas::PoolConfig cfg = writablePoolConfigForTest();
    ASSERT_FALSE(cfg.skip_access_check) << "this test is about the branch that RUNS the battery";

    try
    {
        DB::Cas::Pool::open(backend, cfg);
        FAIL() << "expected the pool-precondition gate to refuse the mount";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::NOT_IMPLEMENTED);
        EXPECT_NE(e.message().find("pool preconditions refused"), std::string::npos)
            << "actual message: " << e.message();
    }
    EXPECT_EQ(counting->writes, 0u) << "the gate must refuse before the battery writes anything";
}

TEST(CASPool, WritableOpenRunsTheSingleAttemptGateBeforeTheBattery)
{
    auto counting = std::make_shared<WriteCountingBackend>(std::make_shared<DB::Cas::InMemoryBackend>());
    auto backend = std::make_shared<ThrowingSingleAttemptBackend>(counting);

    DB::Cas::PoolConfig cfg = writablePoolConfigForTest();
    ASSERT_FALSE(cfg.skip_access_check) << "this test is about the branch that RUNS the battery";

    try
    {
        DB::Cas::Pool::open(backend, cfg);
        FAIL() << "expected the single-attempt gate to refuse the mount";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::NOT_IMPLEMENTED);
        EXPECT_NE(e.message().find("no single-attempt client"), std::string::npos)
            << "actual message: " << e.message();
    }
    EXPECT_EQ(counting->writes, 0u) << "the gate must refuse before the battery writes anything";
}

/// The positive control for the two above: with no gate refusing, the same open DOES write. Without it
/// `writes == 0` would be satisfied by an open that refused for any earlier reason, and both fences would
/// pass while the gates were gone.
TEST(CASPool, WritableOpenWithoutAGateRefusalDoesReachTheBattery)
{
    auto counting = std::make_shared<WriteCountingBackend>(std::make_shared<DB::Cas::InMemoryBackend>());

    DB::Cas::PoolConfig cfg = writablePoolConfigForTest();
    cfg.background_watermark = false;
    ASSERT_FALSE(cfg.skip_access_check);

    auto store = DB::Cas::Pool::open(counting, cfg);
    ASSERT_NE(store, nullptr);
    EXPECT_GT(counting->writes, 0u);
}

TEST(CASPool, MinActiveTracksInFlightBuilds)
{
    auto backend = std::make_shared<DB::Cas::InMemoryBackend>();
    DB::Cas::PoolConfig cfg;
    cfg.pool_prefix = "pool";
    cfg.server_id = DB::UInt128(1);
    cfg.server_root_id = "test";
    cfg.background_watermark = false;
    auto store = DB::Cas::Pool::open(backend, cfg);

    ASSERT_EQ(store->minActive(), store->peekNextBuildSeq());   /// no builds: floor == next seq
    auto b1 = store->beginPartWrite({});                            /// seq 1
    auto b2 = store->beginPartWrite({});                            /// seq 2
    ASSERT_EQ(store->minActive(), 1u);
    b1->abandon();                                              /// finishes seq 1
    ASSERT_EQ(store->minActive(), 2u);                          /// floor advances
    b2->abandon();
    ASSERT_EQ(store->minActive(), store->peekNextBuildSeq());   /// empty again
}

/// A throwing audit sink must NOT break a storage operation. The single reentrancy-safe event
/// dispatcher (stage-1 §1, Task 2) CONTAINS sink exceptions ("never throws through"), so an arbitrary
/// observer/sink callback failing during `beginPartWrite` is swallowed and construction succeeds --
/// consistent with `CASPartWriteTxn.AbandonSwallowsThrowingEventSink` and
/// `PromoteSwallowsPostDurableEventSinkFailure`, which already establish that an audit-sink failure
/// never aborts the operation. Before Task 2 the sink was invoked directly and its exception
/// propagated out of construction (audit-log backpressure breaking a write); the dispatcher removes
/// that. The build_seq lifecycle is still exercised: the in-flight build holds the `minActive` GC
/// floor and is retired on `abandon`.
TEST(CASPool, BeginPartWriteSwallowsThrowingEventSink)
{
    auto backend = std::make_shared<DB::Cas::InMemoryBackend>();
    DB::Cas::PoolConfig cfg;
    cfg.pool_prefix = "pool";
    cfg.server_id = DB::UInt128(1);
    cfg.server_root_id = "test";
    cfg.background_watermark = false;
    auto store = DB::Cas::Pool::open(backend, cfg);

    const uint64_t next_seq = store->peekNextBuildSeq();
    /// UNKNOWN_EXCEPTION (not LOGICAL_ERROR): this simulates an arbitrary observer/sink callback
    /// failing, not a CAS invariant violation -- LOGICAL_ERROR would abort the whole process under
    /// debug/sanitizer builds instead of behaving like a catchable exception.
    store->setEventSink([](const CasEvent & e)
    {
        if (e.type == CasEventType::BuildStart)
            throw DB::Exception(DB::ErrorCodes::UNKNOWN_EXCEPTION, "injected audit sink failure");
    });

    PartWriteTxnPtr build;
    ASSERT_NO_THROW({ build = store->beginPartWrite({}); })
        << "a throwing audit sink must be contained by the dispatcher, not fail construction";
    store->setEventSink(nullptr);

    EXPECT_EQ(build->buildSeq(), next_seq);
    EXPECT_EQ(store->peekNextBuildSeq(), next_seq + 1);
    EXPECT_EQ(store->minActive(), build->buildSeq());              /// the in-flight build holds the floor
    build->abandon();
    EXPECT_EQ(store->minActive(), store->peekNextBuildSeq());      /// retired on abandon
}

TEST(CASPool, BuildSeqIsStrictlyMonotone)
{
    auto backend = std::make_shared<DB::Cas::InMemoryBackend>();
    DB::Cas::PoolConfig cfg;
    cfg.pool_prefix = "pool";
    cfg.server_id = DB::UInt128(1);
    cfg.server_root_id = "test";
    cfg.background_watermark = false;
    auto store = DB::Cas::Pool::open(backend, cfg);
    auto a = store->beginPartWrite({});
    auto sa = a->buildSeq();
    a->abandon();
    auto b = store->beginPartWrite({});
    ASSERT_GT(b->buildSeq(), sa);                               /// never reused, never lower
}

TEST(CASPoolMeta, CreateThenReopen)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout layout("p");
    PoolMeta created = PoolMeta::createOrValidate(*DB::Cas::tests::OperationForTest(b), layout, /*blob_header_len*/ 256,
        BlobHashAlgo::CityHash128, /*allow_new*/ false, /*allow_mint*/ true);
    EXPECT_NE(created.pool_id, UInt128{});
    PoolMeta reopened = PoolMeta::createOrValidate(*DB::Cas::tests::OperationForTest(b), layout, /*blob_header_len*/ 512);
    EXPECT_EQ(reopened.pool_id, created.pool_id);     /// pool is authoritative — config ignored on reopen
    EXPECT_EQ(reopened.blob_header_len, 256u);
}

TEST(CASPoolMeta, FailClosed)
{
    Layout layout("p");
    /// Garbage bytes are not a valid cas_pool_meta text object => CORRUPTED_DATA at the header line
    /// (createOrValidate path). The future-version fail-closed (v > G_BUILD => UNKNOWN_FORMAT_VERSION)
    /// is exercised at the codec level by the battery's per-row v+1 gate.
    auto b2 = std::make_shared<InMemoryBackend>();
    createObj(*b2, layout.poolMetaKey(), "garbage");
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { PoolMeta::createOrValidate(*DB::Cas::tests::OperationForTest(b2), layout, 256); });
}

TEST(CASPoolMeta, RoundTripAndReadability)
{
    PoolMeta pm;
    pm.pool_id = hexToU128("0123456789abcdeffedcba9876543210");
    pm.blob_header_len = 256;
    pm.algos_used = {static_cast<uint8_t>(BlobHashAlgo::CityHash128)};

    const String encoded = encodePoolMeta(pm);
    /// v3 text form: a header line + one JSON body object, human-readable (jq/less friendly). No binary
    /// magic; the object starts with '{' and names its type so a reader can identify it by eye.
    ASSERT_GE(encoded.size(), 8u);
    EXPECT_EQ(encoded.front(), '{');
    EXPECT_NE(encoded.find(String("cas_pool_meta")), String::npos);
    EXPECT_EQ(encoded.find(String("CAPM")), String::npos);

    PoolMeta decoded = decodePoolMeta(encoded);
    EXPECT_EQ(decoded.pool_id, pm.pool_id);
    EXPECT_EQ(decoded.blob_header_len, pm.blob_header_len);
}

TEST(CASPoolMeta, RejectsBadConstantsAtCreation)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout layout("p");

    /// not 8-aligned (above the floor, so it is the alignment rule that rejects it)
    expectThrowsCode(DB::ErrorCodes::BAD_ARGUMENTS,
        [&] { PoolMeta::createOrValidate(*DB::Cas::tests::OperationForTest(b), layout, 250); });
    /// below the v3 envelope floor (240) but 8-aligned: rejected by the floor, not the alignment rule.
    /// Without the raised floor this pool would pass creation and LOGICAL_ERROR on the first blob write.
    expectThrowsCode(DB::ErrorCodes::BAD_ARGUMENTS,
        [&] { PoolMeta::createOrValidate(*DB::Cas::tests::OperationForTest(b), layout, 128); });
    /// well below the floor
    expectThrowsCode(DB::ErrorCodes::BAD_ARGUMENTS,
        [&] { PoolMeta::createOrValidate(*DB::Cas::tests::OperationForTest(b), layout, 64); });
    /// above the 16 KiB ceiling
    expectThrowsCode(DB::ErrorCodes::BAD_ARGUMENTS,
        [&] { PoolMeta::createOrValidate(*DB::Cas::tests::OperationForTest(b), layout, 17 * 1024); });

    /// A creation that fails config validation must not have written anything.
    EXPECT_FALSE(readObj(*b, layout.poolMetaKey()).has_value());
}

TEST(CASPoolMeta, RejectsBadConstantsOnDecode)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout layout("p");
    /// Encode a PoolMeta with blob_header_len=100 (not 8-aligned); decode must reject it as CORRUPTED_DATA.
    PoolMeta bad_pm;
    bad_pm.pool_id = hexToU128("00000000000000000000000000000001");
    bad_pm.blob_header_len = 100;   /// violates 8-alignment invariant
    bad_pm.algos_used = {static_cast<uint8_t>(BlobHashAlgo::CityHash128)};
    createObj(*b, layout.poolMetaKey(), encodePoolMeta(bad_pm));
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { PoolMeta::createOrValidate(*DB::Cas::tests::OperationForTest(b), layout, 256); });
}

TEST(CASPoolMeta, DecodeGarbageFails)
{
    /// Any non-CAPM framing byte sequence => CORRUPTED_DATA.
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [] { decodePoolMeta(String("garbage")); });
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [] { decodePoolMeta(String("")); });
}

TEST(CASPoolMeta, ConcurrentCreateRace)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout layout("p");

    /// A racing creator already wrote a valid foreign pool_id. createOrValidate must NOT overwrite it:
    /// it re-reads (after losing the create-if-absent CAS, or seeing it present) and returns the
    /// foreign pool_id, validated like a reopen.
    const UInt128 foreign = hexToU128("0123456789abcdeffedcba9876543210");
    PoolMeta foreign_pm;
    foreign_pm.pool_id = foreign;
    foreign_pm.blob_header_len = 256;
    foreign_pm.algos_used = {static_cast<uint8_t>(BlobHashAlgo::CityHash128)};
    createObj(*b, layout.poolMetaKey(), encodePoolMeta(foreign_pm));

    PoolMeta result = PoolMeta::createOrValidate(*DB::Cas::tests::OperationForTest(b), layout, /*blob_header_len*/ 512);
    EXPECT_EQ(result.pool_id, foreign);
    EXPECT_EQ(result.blob_header_len, 256u);     /// the foreign pool's constants win
}

TEST(CASPoolMeta, CasConflictReReadsWinner)
{
    /// The subtlest branch: the initial GET sees ABSENT, so createOrValidate proceeds to the
    /// create-if-absent write — and loses, because a racing creator committed in between. The loser
    /// must then re-read and return the WINNER's pool identity, not LOGICAL_ERROR. A single-threaded
    /// `refuseNextWrite` alone cannot exercise this: it returns Conflict without leaving the object
    /// readable, so the re-read would fire the LOGICAL_ERROR guard. We model the real interleaving
    /// with a backend whose write primitive commits the winner's object and THEN reports Conflict --
    /// exactly what the loser observes.
    class RacingBackend : public InMemoryBackend
    {
    public:
        String winner_bytes;
        /// The fault sits on the WRITE PRIMITIVE: the create-if-absent this models is issued there.
        std::expected<String, RawConflict> write(const String & key, const String & bytes,
            const std::optional<String> & expected_value, TransportAccess & access) override
        {
            if (!winner_committed && !expected_value)
            {
                winner_committed = true;
                /// The winner lands first; our create-if-absent now necessarily conflicts.
                (void)InMemoryBackend::write(key, winner_bytes, std::nullopt, access);
                return std::unexpected(RawConflict{});
            }
            return InMemoryBackend::write(key, bytes, expected_value, access);
        }
    private:
        bool winner_committed = false;
    };

    const UInt128 winner = hexToU128("0123456789abcdeffedcba9876543210");
    PoolMeta winner_pm;
    winner_pm.pool_id = winner;
    winner_pm.blob_header_len = 256;
    winner_pm.algos_used = {static_cast<uint8_t>(BlobHashAlgo::CityHash128)};

    auto b = std::make_shared<RacingBackend>();
    b->winner_bytes = encodePoolMeta(winner_pm);
    Layout layout("p");

    /// Our config (512) is what we WOULD have minted, but we lose the race and inherit the winner.
    PoolMeta result = PoolMeta::createOrValidate(*DB::Cas::tests::OperationForTest(b), layout, /*blob_header_len*/ 512,
        BlobHashAlgo::CityHash128, /*allow_new*/ false, /*allow_mint*/ true);
    EXPECT_EQ(result.pool_id, winner);
    EXPECT_EQ(result.blob_header_len, 256u);
}

TEST(CASPool, OpenFailsClosedOnNonEnforcingBackend)
{
    auto b = std::make_shared<InMemoryBackend>();
    b->setEnforceTokens(false);
    expectThrowsCode(DB::ErrorCodes::NOT_IMPLEMENTED,
        [&] { Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"}); });   /// the probe error contract
}

TEST(CASPool, OpenCreatesPoolMetaAndReopens)
{
    auto b = std::make_shared<InMemoryBackend>();
    /// Two CONCURRENT opens over the same POOL: a shared pool is the multi-server model, so each
    /// mounts a DISTINCT server_root_id (and a distinct server_id) — same-root same-uuid co-mounting
    /// is correctly fail-closed by the mount-safety protocol. This test only asserts that pool-meta is
    /// pool-authoritative and shared across opens.
    auto s1 = Pool::open(b, PoolConfig{
        .pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "srv-1"});
    auto s2 = Pool::open(b, PoolConfig{
        .pool_prefix = "p", .server_id = UInt128(2), .server_root_id = "srv-2"});
    EXPECT_EQ(s1->poolMeta().pool_id, s2->poolMeta().pool_id);      /// pool authoritative
}

TEST(CASPool, OpenWithExplicitConstantsCreatesThem)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test", .blob_header_len = 512});
    EXPECT_EQ(s->poolMeta().blob_header_len, 512u);                 /// config applies at creation
}

TEST(CASPool, VerbatimFilesLifecycle)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    RootNamespace ns{"srv1/tbl"};
    s->putNamespaceFile(DB::Cas::tests::fixture::fixtureLife(ns), "format_version.txt", "1\n");
    s->putNamespaceFile(DB::Cas::tests::fixture::fixtureLife(ns), "uuid.txt", "abc");
    EXPECT_EQ(s->getNamespaceFile(DB::Cas::tests::fixture::fixtureLife(ns), "format_version.txt"), String("1\n"));
    EXPECT_FALSE(s->getNamespaceFile(DB::Cas::tests::fixture::fixtureLife(ns), "absent").has_value());
    auto names = s->listNamespaceFiles(DB::Cas::tests::fixture::fixtureLife(ns));
    EXPECT_EQ(names, (std::vector<String>{"format_version.txt", "uuid.txt"}));
    s->putNamespaceFile(DB::Cas::tests::fixture::fixtureLife(ns), "uuid.txt", "def");                     /// overwrite allowed (head + putOverwrite)
    EXPECT_EQ(s->getNamespaceFile(DB::Cas::tests::fixture::fixtureLife(ns), "uuid.txt"), String("def"));
}

TEST(CASPool, ListNamespaceFilesEmpty)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    RootNamespace ns{"srv1/tbl"};
    EXPECT_TRUE(s->listNamespaceFiles(DB::Cas::tests::fixture::fixtureLife(ns)).empty());
}

/// ---------- read side (spec §6): resolveRef / readManifest / findEntry / entryRange / listRefs ----------

/// Phase 1c read path: a published ref resolves to a ManifestId; readManifest returns the immutable
/// body; locate yields a ranged blob read; an Inline entry has no location. Replaces the old
/// resolveRef().tree_id / readTree round trip (the tree model is gone — a part is a single ManifestId).
TEST(CASPool, ResolveReturnsManifestId)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    const RootNamespace ns{"srv1/tbl"};

    /// blob "hello world" + an inline file, published through the real PartWriteTxn write path.
    const String payload = "hello world";
    PartWriteInfo info;
    info.intended_ref = ns.string() + "/part_1";
    auto build = s->beginPartWrite(info);

    ManifestEntry blob_entry;
    blob_entry.path = "data.bin";
    blob_entry.placement = EntryPlacement::Blob;
    blob_entry.ref = DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(u128Of(payload))};

    blob_entry.blob_size = payload.size();
    ManifestEntry inline_entry;
    inline_entry.path = "small.txt";
    inline_entry.placement = EntryPlacement::Inline;
    inline_entry.inline_bytes = "tiny\n";

    const ManifestId id = build->stageManifest({blob_entry, inline_entry});
    build->precommitAdd(ns, "part_1", id);
    build->putBlob(idOf(payload), BlobSource::fromString(payload));
    build->promote(ns, "part_1", build->buildId(), id);

    auto r = s->resolveRef(ns, "part_1");
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->manifest_id, id);                  /// resolve yields the published ManifestId

    auto manifest = s->readManifest(r->manifest_id);
    ASSERT_EQ(manifest.entries.size(), 2u);

    /// "data.bin" sorts before "small.txt" (canonical path order).
    const auto * data = findEntry(manifest.entries, "data.bin");
    ASSERT_TRUE(data != nullptr);
    auto loc = s->locate(*data);
    EXPECT_EQ(loc.offset, s->poolMeta().blob_header_len);
    EXPECT_EQ(loc.length, payload.size());

    auto bytes = readObj(*b, loc.key);
    ASSERT_TRUE(bytes.has_value());
    /// The located window holds exactly the payload: the envelope header is outside it.
    EXPECT_EQ(bytes->bytes.substr(static_cast<size_t>(loc.offset), static_cast<size_t>(loc.length)), payload);

    const auto * small = findEntry(manifest.entries, "small.txt");
    ASSERT_TRUE(small != nullptr);
    EXPECT_THROW(s->locate(*small), DB::Exception);  /// Inline has no location
}

/// readManifest fail-closes on a body whose self-described `ref`/`root_namespace_id` does NOT match the
/// resolved ManifestId — the ref is addressing the wrong object / a cross-namespace dangle. We stage a
/// body raw (writeManifestRaw, the on-storage write fixture) at a ManifestId, then resolve through a
/// committed binding that names a DIFFERENT ManifestRef pointing at the SAME object key — so the head
/// succeeds, the body decodes, but refMatchesBody fails => CORRUPTED_DATA.
TEST(CASPool, ReadManifestValidatesBodyAndFailsClosed)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    const RootNamespace ns{"srv1/tbl"};
    Layout layout("p");

    /// (1) ref/namespace mismatch: the BODY self-describes namespace `srv1/other`, but it is addressed
    /// as a manifest of `srv1/tbl` => manifestNamespaceMatches fails => CORRUPTED_DATA. We craft an id
    /// whose key lives under `srv1/tbl` but whose body carries the foreign namespace.
    {
        const ManifestRef ref = manifestRefFor("mismatch-ns");
        const ManifestId addressed{.root_namespace = ns, .ref = ref};
        /// Encode a body that claims a DIFFERENT namespace than `addressed.root_namespace`.
        PartManifest body;
        body.ref = ref;                                     /// ref matches
        body.root_namespace_id = RootNamespace{"srv1/other"};  /// namespace does NOT
        body.entries = {blobEntryFor("f", u128Of("x"), 1)};
        body.payload_digest = computePayloadDigest(body);
        createObj(*b, layout.manifestKey(addressed), encodePartManifest(body));

        expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { s->readManifest(addressed); });
    }

    /// (2) ref mismatch: the body self-describes a DIFFERENT ManifestRef than the id addressing it =>
    /// refMatchesBody fails => CORRUPTED_DATA.
    {
        const ManifestRef addressed_ref = manifestRefFor("addressed-ref");
        const ManifestRef body_ref = manifestRefFor("body-ref-other");
        const ManifestId addressed{.root_namespace = ns, .ref = addressed_ref};
        PartManifest body;
        body.ref = body_ref;                                /// ref does NOT match `addressed`
        body.root_namespace_id = ns;                        /// namespace matches
        body.entries = {blobEntryFor("f", u128Of("y"), 1)};
        body.payload_digest = computePayloadDigest(body);
        createObj(*b, layout.manifestKey(addressed), encodePartManifest(body));

        expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { s->readManifest(addressed); });
    }

    /// (3) a committed ref naming a manifest with NO body present => readManifest throws
    /// FILE_DOESNT_EXIST (INV-NO-DANGLE surfaced on the read path). resolveRef itself SUCCEEDS — refs
    /// are pure manifest state. A raw ref-log fixture (not the real PartWriteTxn path, which validates the
    /// body exists at promote) is the only way to construct this state.
    {
        const ManifestRef missing_ref = manifestRefFor("never-staged");
        DB::Cas::tests::fixture::writeRefLogRaw(*b, layout, RefLogTxn{ns.string(), RefTxnId{1, 1},
            {DB::Cas::tests::namespaceBirthOp(), DB::Cas::tests::publishCommittedOps("part_dangle", missing_ref)[0],
             DB::Cas::tests::publishCommittedOps("part_dangle", missing_ref)[1]}, std::nullopt});
        DB::Cas::tests::writeRecoverableCkptForRawFixture(*b, layout, ns, RefCkpt{
            .life_epoch = 1,
            .committed_through = RefTxnId{1, 1},
            .checkpoint_snapshot_id = std::nullopt,
            .last_epoch_seal = std::nullopt,
        });

        auto r = s->resolveRef(ns, "part_dangle");
        ASSERT_TRUE(r.has_value());
        expectThrowsCode(DB::ErrorCodes::FILE_DOESNT_EXIST, [&] { s->readManifest(r->manifest_id); });
    }
}

/// findEntry and entryRange over a decoded part manifest's canonical-path-ordered entries.
TEST(CASPool, LookupAndListOverManifestEntries)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    const RootNamespace ns{"srv1/tbl"};

    /// A multi-file/multi-directory part: top-level + a projection subdir.
    std::vector<ManifestEntry> entries;
    entries.push_back(blobEntryFor("columns.txt", u128Of("cols"), 4));
    entries.push_back(blobEntryFor("data.bin", u128Of("data"), 8));
    entries.push_back(blobEntryFor("p.proj/data.bin", u128Of("proj-data"), 6));
    entries.push_back(blobEntryFor("p.proj/columns.txt", u128Of("proj-cols"), 5));
    const ManifestId id = publishPartWithEntries(s, ns.string(), "all_1_1_0", entries);

    auto r = s->resolveRef(ns, "all_1_1_0");
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->manifest_id, id);
    auto manifest = s->readManifest(r->manifest_id);
    ASSERT_EQ(manifest.entries.size(), 4u);

    /// findEntry: exact-path hit + miss.
    const auto * hit = findEntry(manifest.entries, "data.bin");
    ASSERT_TRUE(hit != nullptr);
    EXPECT_EQ(hit->ref.digest.toU128(), u128Of("data"));
    EXPECT_TRUE(findEntry(manifest.entries, "no_such_file") == nullptr);

    /// entryRange under "p.proj/" yields exactly the two projection files, in canonical order.
    auto [proj_first, proj_last] = entryRange(manifest.entries, "p.proj/");
    std::vector<ManifestEntry> proj(proj_first, proj_last);
    ASSERT_EQ(proj.size(), 2u);
    EXPECT_EQ(proj[0].path, "p.proj/columns.txt");
    EXPECT_EQ(proj[1].path, "p.proj/data.bin");

    /// The empty prefix lists everything (all four), still in canonical order.
    auto [all_first, all_last] = entryRange(manifest.entries, "");
    std::vector<ManifestEntry> all(all_first, all_last);
    ASSERT_EQ(all.size(), 4u);
    EXPECT_EQ(all[0].path, "columns.txt");
    EXPECT_EQ(all[3].path, "p.proj/data.bin");
}

/// The manifest decode cache is keyed by ManifestId alone: an id is minted once and its body is
/// written once, so one id names one content forever. Resolve+read the same ref twice: the second
/// readManifest is served from the cache with NO request at all. A fresh publish under a DIFFERENT
/// ref name mints a NEW ManifestId, so the cache misses and the body is fetched once.
TEST(CASPool, ManifestCacheIsKeyedById)
{
    auto b = std::make_shared<DB::Cas::tests::CountingBackend>();
    auto s = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    const RootNamespace ns{"srv1/tbl"};
    Layout layout("p");

    const ManifestId id1 = publishPart(s, ns.string(), "part_1", "payload-1");
    const String key1 = layout.manifestKey(id1);
    b->resetCounts();

    /// First read: a body GET populates the id1 cache entry.
    {
        auto r = s->resolveRef(ns, "part_1");
        ASSERT_TRUE(r.has_value());
        auto m = s->readManifest(r->manifest_id);
        ASSERT_EQ(m.entries.size(), 1u);
    }
    const uint64_t gets_after_first = b->getCount(key1);
    ASSERT_GE(gets_after_first, 1u);               /// the first read DID fetch the body

    /// Second read of the SAME id: the id-keyed cache must serve it — NO additional body GET.
    {
        auto r = s->resolveRef(ns, "part_1");
        ASSERT_TRUE(r.has_value());
        EXPECT_EQ(r->manifest_id, id1);
        auto m = s->readManifest(r->manifest_id);
        ASSERT_EQ(m.entries.size(), 1u);
    }
    EXPECT_EQ(b->getCount(key1), gets_after_first)
        << "second readManifest re-GET the body for the same ManifestId — cache miss";
    EXPECT_EQ(b->headCount(key1), 0u) << "keyed by id alone: no HEAD on a miss or a hit";

    /// A fresh publish under a DIFFERENT ref name mints a NEW ManifestId: the cache (keyed by id) misses.
    /// (Promoting a different manifest over the SAME committed ref is a distinct promote-over-committed
    /// leak that `PartWriteTxn::promote` now forbids — see the CASPromoteRepublish tests.)
    const ManifestId id2 = publishPart(s, ns.string(), "part_2", "payload-2");
    EXPECT_FALSE(id2 == id1);                       /// a new publish never reuses a ManifestId
    const String key2 = layout.manifestKey(id2);

    auto r2 = s->resolveRef(ns, "part_2");
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(r2->manifest_id, id2);               /// resolve now sees the new manifest
    auto m2 = s->readManifest(r2->manifest_id);
    ASSERT_EQ(m2.entries.size(), 1u);
    EXPECT_GE(b->getCount(key2), 1u)               /// the new id's body WAS fetched (cache miss)
        << "fresh publish (new ManifestId) should miss the id-keyed manifest cache";
    EXPECT_EQ(b->headCount(key2), 0u);
}

/// Phase 5 (part-folder cache spec): manifest_cache is now a byte-weighted CacheBase LRU instead of a
/// count-only bound, since decoded manifests carry inline bytes and can each be megabytes.
TEST(CASPool, ManifestDecodeCacheIsByteBounded)
{
    auto backend = std::make_shared<DB::Cas::tests::CountingBackend>();
    const DB::Cas::Layout layout("p");
    DB::Cas::tests::seedPoolMetaForRestart(*backend);
    const DB::Cas::RootNamespace ns{"srv/t1"};

    /// 8 manifests x ~1 MiB of inline bytes; a 2 MiB decode-cache bound must hold while every
    /// read stays correct (evicted decodes just re-GET + re-decode).
    std::vector<DB::Cas::ManifestId> ids;
    std::vector<DB::Cas::RefOp> birth_ops{DB::Cas::tests::namespaceBirthOp()};
    for (int i = 0; i < 8; ++i)
    {
        const DB::Cas::ManifestRef ref{.writer_epoch = 1, .build_sequence = static_cast<uint64_t>(i + 1),
                                       .manifest_ordinal = 1};
        DB::Cas::ManifestEntry e;
        e.path = "big.txt";
        e.placement = DB::Cas::EntryPlacement::Inline;
        e.ref = DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(DB::UInt128(i + 1))};

        e.inline_bytes = String(1 << 20, static_cast<char>('a' + i));
        e.blob_size = e.inline_bytes.size();
        ids.push_back(DB::Cas::tests::writeManifestRaw(*backend, layout, ns, ref, {e}));

        const String ref_name = "part_" + std::to_string(i);
        std::vector<DB::Cas::RefOp> ops = i == 0 ? birth_ops : std::vector<DB::Cas::RefOp>{};
        const auto committed_ops = DB::Cas::tests::publishCommittedOps(ref_name, ref);
        ops.insert(ops.end(), committed_ops.begin(), committed_ops.end());
        DB::Cas::tests::fixture::writeRefLogRaw(*backend, layout, RefLogTxn{ns.string(), RefTxnId{1, static_cast<uint64_t>(i + 1)}, ops, std::nullopt});
    }
    DB::Cas::tests::writeRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 8},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    DB::Cas::PoolConfig config{.pool_prefix = "p", .server_root_id = "test"};
    config.manifest_decode_cache_bytes = 2ULL << 20;
    auto store = DB::Cas::Pool::open(backend, std::move(config));

    uint64_t total_gets = 0;
    for (int round = 0; round < 2; ++round)
        for (int i = 0; i < 8; ++i)
        {
            auto resolved = store->resolveRef(ns, "part_" + std::to_string(i));
            ASSERT_TRUE(resolved.has_value());
            auto m = store->readManifestShared(resolved->manifest_id);
            ASSERT_EQ(m->entries.size(), 1u);
            EXPECT_EQ(m->entries[0].inline_bytes[0], static_cast<char>('a' + i));   /// always correct
        }
    for (const auto & id : ids)
        total_gets += backend->getCount(layout.manifestKey(id));

    /// The bound forces re-GETs (16 reads over a 2 MiB window of ~1 MiB decodes cannot all hit),
    /// proving eviction actually happens...
    EXPECT_GT(total_gets, 8u);
    /// ...and the cache reports an in-bound retained size.
    EXPECT_LE(store->manifestDecodeCacheBytesForTest(), 2ULL << 20);
}

TEST(CASPool, ResolveDecodeCacheInvalidatesOnWrite)
{
    /// B113: resolveRef uses a token-validated shard-manifest decode cache. A write to the shard
    /// mints a new token, so a subsequent resolve must observe the change (cache must NOT serve a
    /// stale decoded manifest). Without token invalidation this would still see the dropped ref.
    auto b = std::make_shared<InMemoryBackend>();
    auto s = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    RootNamespace ns{"srv1/tbl"};

    publishPart(s, ns.string(), "part_1", "payload-1");

    /// First resolve decodes + caches; second is a cache hit — both must see part_1.
    ASSERT_TRUE(s->resolveRef(ns, "part_1").has_value());
    ASSERT_TRUE(s->resolveRef(ns, "part_1").has_value());

    /// Write through the Pool (mutateShard => new shard token), removing part_1.
    s->dropRef(ns, "part_1");

    /// The cache must invalidate on the token change: resolve now reflects the drop.
    EXPECT_FALSE(s->resolveRef(ns, "part_1").has_value());
    EXPECT_TRUE(s->listRefs(ns).empty());
}

TEST(CASPool, ResolveAbsentRefAndAbsentNamespace)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    RootNamespace ns{"srv1/tbl"};

    /// A freshly-opened pool has no shard manifests: an absent shard is an empty manifest, so resolve
    /// yields nullopt and listRefs is empty (NOT an error).
    EXPECT_FALSE(s->resolveRef(ns, "anything").has_value());
    EXPECT_TRUE(s->listRefs(ns).empty());
}

TEST(CASPool, ListRefsMergesAllShards)
{
    /// Task 10: refs are no longer sharded (the snapshot+log protocol caches one coherent table state
    /// per namespace, not one manifest per shard) -- this now proves listRefs returns every committed
    /// ref of a table built from a single multi-owner transaction, the closest surviving analogue of
    /// the old "merges refs spread across shards" contract.
    auto b = std::make_shared<InMemoryBackend>();
    auto s = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    Layout layout("p");
    RootNamespace ns{"srv1/tbl"};

    std::vector<RefOp> ops{DB::Cas::tests::namespaceBirthOp()};
    for (char c = 'a'; c <= 'h'; ++c)
    {
        const String ref(1, c);
        const auto committed_ops = DB::Cas::tests::publishCommittedOps(ref, manifestRefFor("manifest-" + ref));
        ops.insert(ops.end(), committed_ops.begin(), committed_ops.end());
    }
    DB::Cas::tests::fixture::writeRefLogRaw(*b, layout, RefLogTxn{ns.string(), RefTxnId{1, 1}, ops, std::nullopt});
    DB::Cas::tests::writeRecoverableCkptForRawFixture(*b, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 1},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    auto refs = s->listRefs(ns);
    ASSERT_EQ(refs.size(), 8u);
    for (char c = 'a'; c <= 'h'; ++c)
    {
        const String ref(1, c);
        ASSERT_TRUE(refs.count(ref));
        EXPECT_EQ(refs.at(ref).manifest_id.ref, manifestRefFor("manifest-" + ref));
        EXPECT_EQ(refs.at(ref).manifest_id.root_namespace.string(), ns.string());
    }
}

/// An empty namespace recovers from its exact `_ckpt` authority and exact successor GET. It performs
/// ZERO LISTs and ZERO HEADs: recovery no longer enumerates the stream, and it never probes a shard
/// fan-out. Measure deltas around `listRefs`; `Pool::open` and fixture admission have their own metadata
/// traffic.
TEST(CASPool, ListRefsEmptyNamespaceCostsZeroListsAndHeads)
{
    auto b = std::make_shared<DB::Cas::tests::CountingBackend>();
    auto s = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    RootNamespace ns{"srv1/tbl"};
    /// EMPTY, but EXISTING and recoverable. A namespace the catalog does not name is answered from the
    /// catalog and never reaches recovery; that separate shape is measured by the case below.
    DB::Cas::tests::casAdmitRecoverableEntry(*b, Layout("p"), ns);

    const uint64_t heads_before = b->headTotal();
    const uint64_t lists_before = b->listTotal();

    auto refs = s->listRefs(ns);

    EXPECT_TRUE(refs.empty());
    EXPECT_EQ(b->headTotal() - heads_before, 0u)
        << "empty-namespace listRefs must not HEAD any shard";
    EXPECT_EQ(b->listTotal() - lists_before, 0u)
        << "checkpoint-grounded recovery reads exact keys and must not LIST the ref stream";
}

/// The other shape: a namespace that was never born. A read must not be what brings one into existence,
/// so the answer comes from the catalog alone -- no recovery, and therefore not even the one LIST the
/// case above pins.
TEST(CASPool, ListRefsOnANeverBornNamespaceCostsNoListAndNoHead)
{
    auto b = std::make_shared<DB::Cas::tests::CountingBackend>();
    auto s = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    RootNamespace ns{"srv1/tbl"};

    const uint64_t heads_before = b->headTotal();
    const uint64_t lists_before = b->listTotal();
    const uint64_t gets_before = b->getTotal();

    auto refs = s->listRefs(ns);

    EXPECT_TRUE(refs.empty());
    EXPECT_EQ(b->listTotal() - lists_before, 0u)
        << "a never-born namespace has no ref stream to LIST";
    EXPECT_EQ(b->headTotal() - heads_before, 0u);
    /// Positive control: the zeros above are the answer coming from the catalog, not from a call that
    /// did nothing at all.
    EXPECT_GT(b->getTotal() - gets_before, 0u)
        << "the answer must come from a catalog read";
}

/// listRefs must return every committed ref of a table, correctly, regardless of how many refs the
/// table holds (Task 10: there is no more shard fan-out to discover -- see the comment inside).
TEST(CASPool, ListRefsReturnsSameContentAsBefore)
{
    /// Task 10: there is no more per-shard HEAD fan-out to bound (a warm listRefs costs ZERO requests;
    /// a cold empty one costs zero LISTs and HEADs, already covered by
    /// `ListRefsEmptyNamespaceCostsZeroListsAndHeads`) -- this now just proves the returned content is
    /// correct for a multi-ref table built from a single raw ref-log fixture.
    auto b = std::make_shared<DB::Cas::tests::CountingBackend>();
    auto s = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    Layout layout("p");
    RootNamespace ns{"srv1/tbl"};

    std::vector<RefOp> ops{DB::Cas::tests::namespaceBirthOp()};
    for (const String & ref : {String("a"), String("m"), String("z")})
    {
        const auto committed_ops = DB::Cas::tests::publishCommittedOps(ref, manifestRefFor("manifest-" + ref));
        ops.insert(ops.end(), committed_ops.begin(), committed_ops.end());
    }
    DB::Cas::tests::fixture::writeRefLogRaw(*b, layout, RefLogTxn{ns.string(), RefTxnId{1, 1}, ops, std::nullopt});
    DB::Cas::tests::writeRecoverableCkptForRawFixture(*b, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 1},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    auto refs = s->listRefs(ns);

    ASSERT_EQ(refs.size(), 3u);
    for (const String & ref : {String("a"), String("m"), String("z")})
    {
        ASSERT_TRUE(refs.count(ref));
        EXPECT_EQ(refs.at(ref).manifest_id.ref, manifestRefFor("manifest-" + ref));
        EXPECT_EQ(refs.at(ref).manifest_id.root_namespace.string(), ns.string());
    }
}

/// A stray key under the namespace's ref-object prefix that does not parse as one of Task 10's
/// `_log`/`_snap` kinds (a foreign/corrupt object) must not break listRefs — it is skipped
/// defensively, listRefs still returns the legit refs and never throws.
TEST(CASPool, ListRefsSkipsForeignKeys)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    Layout layout("p");
    RootNamespace ns{"srv1/tbl"};

    const String ref = "legit";
    const ManifestRef mref = manifestRefFor("manifest-" + ref);
    DB::Cas::tests::fixture::writeRefLogRaw(*b, layout, RefLogTxn{ns.string(), RefTxnId{1, 1},
        {DB::Cas::tests::namespaceBirthOp(), DB::Cas::tests::publishCommittedOps(ref, mref)[0],
         DB::Cas::tests::publishCommittedOps(ref, mref)[1]}, std::nullopt});
    DB::Cas::tests::writeRecoverableCkptForRawFixture(*b, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 1},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    /// A stray key directly under the namespace's ref-object prefix that is not `_log`/
    /// `_snap` shaped (also covers the legacy shard-number layout GC/dropNamespace still write).
    createObj(*b, layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "garbage", "not-a-ref-object");

    std::map<String, Resolved> refs;
    EXPECT_NO_THROW(refs = s->listRefs(ns));
    ASSERT_EQ(refs.size(), 1u);
    ASSERT_TRUE(refs.count(ref));
    EXPECT_EQ(refs.at(ref).manifest_id.ref, mref);
}

/// readManifest fails CLOSED on a corrupt or kind-mismatched manifest body addressed by a live id.
TEST(CASPool, ReadManifestFailsClosed)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    Layout layout("p");
    const RootNamespace ns{"srv1/tbl"};

    /// (1) Garbage bytes at the manifest key => decodePartManifest throws CORRUPTED_DATA.
    {
        const ManifestRef ref = manifestRefFor("garbage-body");
        const ManifestId id{.root_namespace = ns, .ref = ref};
        createObj(*b, layout.manifestKey(id), "not a valid manifest body");
        expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { s->readManifest(id); });
    }

    /// (2) A ref naming a manifest id with NO object present => readManifest throws FILE_DOESNT_EXIST
    /// (INV-NO-DANGLE), carrying the manifest key.
    {
        const ManifestRef ref = manifestRefFor("absent-body");
        const ManifestId id{.root_namespace = ns, .ref = ref};
        expectThrowsCode(DB::ErrorCodes::FILE_DOESNT_EXIST, [&] { s->readManifest(id); });
    }
}

/// ---------- ref lifecycle: dropRef / updateRefPublishedAt / dropNamespace ----------

TEST(CASPool, DropRefAppendsJournalAtomically)
{
    /// Task 10: the OLD shared-journal record assertions are gone (there is no shared mutable journal
    /// object anymore — dropRef appends its OWN immutable ref-log transaction); the surviving
    /// behavioral contract is: the drop is atomic (visible to resolveRef only once durable), and
    /// dropping a missing ref is fail-closed, never a silent no-op.
    auto b = std::make_shared<InMemoryBackend>();
    auto s = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    RootNamespace ns{"srv1/tbl"};

    publishPart(s, ns.string(), "part_1", "payload-1");
    ASSERT_TRUE(s->resolveRef(ns, "part_1").has_value());

    s->dropRef(ns, "part_1");
    EXPECT_FALSE(s->resolveRef(ns, "part_1").has_value());
    EXPECT_TRUE(s->listRefs(ns).empty());

    /// Dropping a missing ref is fail-closed, never a silent no-op.
    expectThrowsCode(DB::ErrorCodes::FILE_DOESNT_EXIST, [&] { s->dropRef(ns, "no_such_ref"); });
}

/// Task 10 renamed this from "...WithoutJournal": updateRefPublishedAt now DOES append an immutable
/// `set_published_at` ref-log transaction (spec §Update Payload) -- the old journal-free in-place field
/// mutation had no equivalent once persistence is an append-only log; every change, even timestamp-only,
/// must be a logged operation to be part of the ordered history. All-tree-part-files Task 9: the
/// carrier's mutable-file map is gone -- `published_at_ms` is the only field left to mutate. The
/// surviving contract is the user-visible one: a `published_at_ms` update is observable through
/// resolveRef and the manifest edge cannot change on this path -- the `RefPublishedAtUpdate` carrier
/// deliberately has no `manifest_ref` field, so a reachability change is structurally impossible here
/// (it goes through publish/drop/repoint instead).
TEST(CASPool, UpdateRefPublishedAtUpdatesPublishedAtMs)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    RootNamespace ns{"srv1/tbl"};

    const ManifestId id = publishPart(s, ns.string(), "part_1", "payload-1");
    const ManifestRef manifest_ref = id.ref;

    s->updateRefPublishedAt(ns, "part_1", [](RefPublishedAtUpdate & r) { r.published_at_ms = 1; });
    s->updateRefPublishedAt(ns, "part_1", [](RefPublishedAtUpdate & r) { r.published_at_ms = 7; });

    auto after = s->resolveRef(ns, "part_1");
    ASSERT_TRUE(after.has_value());
    EXPECT_EQ(after->published_at_ms, 7u);
    EXPECT_EQ(after->manifest_id.ref, manifest_ref);
}

/// Task 11: dropNamespace removes every owner through the ref-log `remove_namespace` transaction and
/// performs NO physical deletion at all -- verbatim files survive until GC's perpetual janitor
/// reclaims the dead life. So after the drop every ref resolves away and
/// `listRefs` is empty, but the verbatim files remain readable.
TEST(CASPool, DropNamespaceRemovesEveryOwnerButLeavesFilesForGc)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    RootNamespace ns{"srv1/tbl"};

    const std::vector<String> ref_names{"alpha", "bravo", "charlie"};
    for (const String & name : ref_names)
        publishPart(s, ns.string(), name, "payload-" + name);
    for (const String & name : ref_names)
        ASSERT_TRUE(s->resolveRef(ns, name).has_value());

    s->putNamespaceFile(DB::Cas::tests::fixture::fixtureLife(ns), "format_version.txt", "1\n");
    s->putNamespaceFile(DB::Cas::tests::fixture::fixtureLife(ns), "uuid.txt", "abc");

    s->dropNamespace(ns);

    for (const String & name : ref_names)
        EXPECT_FALSE(s->resolveRef(ns, name).has_value());
    EXPECT_TRUE(s->listRefs(ns).empty());

    /// The writer performs NO physical deletion; verbatim files survive until the perpetual janitor
    /// reclaims the dead life.
    EXPECT_TRUE(s->getNamespaceFile(DB::Cas::tests::fixture::fixtureLife(ns), "format_version.txt").has_value());
    EXPECT_TRUE(s->getNamespaceFile(DB::Cas::tests::fixture::fixtureLife(ns), "uuid.txt").has_value());

    /// Repeated drop is idempotent: no throw, no second transaction (nothing left to observe changing).
    EXPECT_NO_THROW(s->dropNamespace(ns));

    /// Ordinary mutations on a cataloged `Removing` life are rejected with typed retry-later until
    /// the terminal fold and catalog-only drain complete.
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { s->dropRef(ns, "alpha"); });
}

TEST(CASPool, ListNamespacesFromCatalog)
{
    /// `listNamespaces` projects logical names from the authoritative catalog. Physical life keys
    /// contain no namespace spelling and therefore cannot participate in this enumeration.
    auto b = std::make_shared<InMemoryBackend>();
    auto s = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});

    EXPECT_TRUE(s->listNamespaces("").namespaces.empty());   /// fresh pool: empty catalog

    /// The real publication path admits each namespace before writing its stream.
    DB::Cas::tests::publishCommittedTransition(*b, s->layout(), RootNamespace{"srv1/tbl"},
        "ref1", std::nullopt, DB::Cas::ManifestRef{.writer_epoch = 1, .build_sequence = 1, .manifest_ordinal = 1});
    DB::Cas::tests::publishCommittedTransition(*b, s->layout(), RootNamespace{"srv1/shadow/bk1/tbl"},
        "ref1", std::nullopt, DB::Cas::ManifestRef{.writer_epoch = 1, .build_sequence = 1, .manifest_ordinal = 1});
    DB::Cas::tests::publishCommittedTransition(*b, s->layout(), RootNamespace{"srv1/shadow/bk2/tbl"},
        "ref1", std::nullopt, DB::Cas::ManifestRef{.writer_epoch = 1, .build_sequence = 1, .manifest_ordinal = 1});

    const auto all = s->listNamespaces("").namespaces;
    EXPECT_EQ(all.size(), 3u);
    const auto shadows = s->listNamespaces("srv1/shadow/").namespaces;
    ASSERT_EQ(shadows.size(), 2u);
    /// listNamespaces returns results from an unordered_set; sort for deterministic comparison.
    auto sorted_shadows = shadows;
    std::sort(sorted_shadows.begin(), sorted_shadows.end());
    EXPECT_EQ(sorted_shadows[0], "srv1/shadow/bk1/tbl");
    EXPECT_EQ(sorted_shadows[1], "srv1/shadow/bk2/tbl");
    EXPECT_TRUE(s->listNamespaces("nope/").namespaces.empty());
}

/// Physical namespace files carry only an opaque life id and cannot mint a logical catalog row.
TEST(CASPool, ListNamespacesDoesNotMintLogicalNamesFromFileKeys)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    const RootNamespace ns{"test/tbl@cas@"};

    s->putNamespaceFile(DB::Cas::tests::fixture::fixtureLife(ns), "format_version.txt", "1\n");
    /// A second life of the SAME name, written by exact key because no helper mints two lives yet.
    const NamespaceLifeId other = NamespaceLifeId::fromCatalogEntry(ns, DB::UInt128(0x5eed));
    createObj(*b, s->layout().namespaceFileKey(other, "format_version.txt"), "1\n");

    const NamespaceListing listing = s->listNamespaces("");
    EXPECT_TRUE(listing.skipped.empty());
    EXPECT_TRUE(listing.namespaces.empty());
}

/// Catalog discovery neither adopts nor reports malformed physical debris. Diagnostic ownership-tree
/// scans, not ordinary logical enumeration, classify those keys.
TEST(CASPool, ListNamespacesDoesNotTreatPhysicalDebrisAsCatalogAuthority)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    const RootNamespace ns{"test/tbl@cas@"};

    /// One well-formed key per family, so the namespace is attributable either way.
    DB::Cas::tests::publishCommittedTransition(*b, s->layout(), ns,
        "ref1", std::nullopt, DB::Cas::ManifestRef{.writer_epoch = 1, .build_sequence = 1, .manifest_ordinal = 1});
    s->putNamespaceFile(DB::Cas::tests::fixture::fixtureLife(ns), "format_version.txt", "1\n");

    /// Hand-built un-incarnated keys: no helper can mint either shape any more.
    const String lifeless_ref = s->layout().casRefsPrefix() + ns.string() + "/_log/"
        + renderRefTxnId(RefTxnId{1, 1}) + ".zst";
    const String lifeless_file = s->layout().rootsPrefix() + ns.string() + "/_files/format_version.txt";
    createObj(*b, lifeless_ref, "garbage");
    createObj(*b, lifeless_file, "garbage");

    NamespaceListing listing;
    ASSERT_NO_THROW(listing = s->listNamespaces(""))
        << "one un-attributable key must not abort the enumeration for every consumer of it";

    /// The healthy namespace is still listed -- attribution is per key, so a namespace disappears only
    /// when every key that would name it is unattributable.
    ASSERT_EQ(listing.namespaces.size(), 1u);
    EXPECT_EQ(listing.namespaces[0], ns.string());

    EXPECT_TRUE(listing.skipped.empty());
    EXPECT_TRUE(headObj(*b, lifeless_ref).has_value());
    EXPECT_TRUE(headObj(*b, lifeless_file).has_value());
}

TEST(CASPool, ListMirroredChildren)
{
    using namespace DB::Cas;
    auto b = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    /// Seed two catalog-authoritative shadow archives; physical files alone carry no logical path.
    DB::Cas::tests::fixture::admitLive(*b, store->layout(), RootNamespace{"srv1/shadow/bk1/store/3f2/3f2a-uuid@cas@"});
    DB::Cas::tests::fixture::admitLive(*b, store->layout(), RootNamespace{"srv1/shadow/bk2/store/3f2/3f2a-uuid@cas@"});
    auto children = store->listMirroredChildren("srv1/shadow/");
    std::sort(children.begin(), children.end());
    ASSERT_EQ(children.size(), 2u);
    EXPECT_EQ(children[0], "bk1");
    EXPECT_EQ(children[1], "bk2");
}

namespace
{

/// Delegating backend that fences the mount slot IN PLACE the first time a `get` returns a present
/// body for the armed key — reproducing the S13 window: the GC's token-guarded fence-out lands
/// between the renewer adopt's GET and its CAS. The caller's subsequent token-guarded `putOverwrite`
/// then fails `PreconditionFailed`, the adopt re-reads, sees `gc_fenced`, and throws
/// `MountFencedException` — which `Pool::open`'s fence-recovery loop must turn into a fresh-epoch
/// retry rather than a permanent wedge (P3.1 vector C).
class FenceInAdoptWindowBackend final : public DB::Cas::Backend
{
public:
    explicit FenceInAdoptWindowBackend(std::shared_ptr<DB::Cas::Backend> inner_) : inner(std::move(inner_)) {}
    String fence_key;   /// empty = fault disarmed; set to the mount key to arm the one-shot fence

    bool supportsListTokens() const override { return inner->supportsListTokens(); }

    /// The fault sits on the READ PRIMITIVE: the renewer's adopt reads the mount slot through it.
    std::optional<Raw> read(const String & key, TransportAccess & access) override
    {
        auto got = inner->read(key, access);
        if (!fence_key.empty() && key == fence_key && got.has_value())
        {
            /// One-shot: fence the slot in place exactly as `computeHeartbeatFloor` does (preserve the
            /// body, gc_fenced = true, seq + 1, guarded against the incarnation we just read), then
            /// disarm so the retry can adopt cleanly.
            DB::Cas::MountLease fenced = DB::Cas::decodeMountLease(got->bytes);
            fenced.gc_fenced = true;
            fenced.seq += 1;
            (void)inner->write(key, DB::Cas::encodeMountLease(fenced), got->value, access);
            fence_key.clear();
        }
        return got;
    }
    std::optional<RawMeta> head(const String & key, TransportAccess & access) override { return inner->head(key, access); }
    RawListPage list(const String & prefix, const String & cursor, size_t limit, TransportAccess & access) override { return inner->list(prefix, cursor, limit, access); }
    RawRemoval remove(const String & key, const String & expected_value, TransportAccess & access) override { return inner->remove(key, expected_value, access); }
    void removeManyWriteOnce(const std::vector<WriteOnceKey> & keys, TransportAccess & access) override { inner->removeManyWriteOnce(keys, access); }
    std::expected<String, RawConflict> write(const String & key, const String & bytes,
                                             const std::optional<String> & expected_value, TransportAccess & access) override
    {
        return inner->write(key, bytes, expected_value, access);
    }
    std::unique_ptr<DB::ReadBuffer> stream(const String & key, TransportAccess & access) override { return inner->stream(key, access); }
    void publish(const BlobPublishRequest & request, TransportAccess & access) override { inner->publish(request, access); }
    Dialect dialect() const override { return inner->dialect(); }

private:
    std::shared_ptr<DB::Cas::Backend> inner;
};

}

TEST(CASPoolMountFence, OpenRecoversFromFenceInAdoptWindowWithFreshEpoch)
{
    auto inner = std::make_shared<InMemoryBackend>();
    auto fencing = std::make_shared<FenceInAdoptWindowBackend>(inner);
    /// Arm the one-shot fence on the mount slot. Pool::open first claims the mount (fresh mint), then
    /// the renewer adopts it — the adopt's GET trips the fence, its CAS fails, and open must recover.
    const DB::Cas::Layout layout("p");
    fencing->fence_key = layout.mountKey("test");

    /// The retry that recovers from the fence reclaims a same-uuid, different-epoch, `gc_fenced` body
    /// -> `MountPriorState::Fenced` (a fenced prior is reclaimed on the first attempt, with no
    /// observation polling -- see `CASMountOpenWaits.FencedPriorReclaimsWithoutAnyWait`). The injected
    /// `boot_ms_fn`/`wait_sleep_fn` below keep this test off the real clock regardless.
    /// Held in a shared atomic, not a plain local: `wait_sleep_fn` below mutates it, and the Pool can
    /// outlive this stack frame (a background publish holds `shared_from_this()`), so a by-reference
    /// capture of a local would dangle.
    auto fake_boot = std::make_shared<std::atomic<uint64_t>>(0);
    DB::Cas::PoolPtr store;
    ASSERT_NO_THROW(
        store = DB::Cas::Pool::open(fencing,
            DB::Cas::PoolConfig{.pool_prefix = "p", .server_root_id = "test",
                .boot_ms_fn = [fake_boot]
                {
                    return fake_boot->load();
                },
                .wait_sleep_fn = [fake_boot](uint64_t ms)
                {
                    *fake_boot += ms;
                }}))
        << "open must recover from a fence in the adopt window, not wedge (exit-49 S13 bug)";
    ASSERT_TRUE(store);

    /// The final live lease is unfenced and at a HIGHER writer_epoch than the first attempt (a fence
    /// costs an epoch): the first claim took epoch 1, got fenced, the retry took epoch 2 and mounted.
    const auto got = readObj(*inner, layout.mountKey("test"));
    ASSERT_TRUE(got.has_value());
    const MountLease final_lease = decodeMountLease(got->bytes);
    EXPECT_FALSE(final_lease.gc_fenced);
    EXPECT_GT(final_lease.writer_epoch, 1u) << "recovery must draw a fresh writer_epoch";
    EXPECT_TRUE(fencing->fence_key.empty()) << "the one-shot fence must have fired";
}

/// Task 12: the write-fence deadline is a CLOCK_BOOTTIME instant (boottime includes VM-suspend time,
/// so a resumed sleeper sees its fence expired — unlike CLOCK_MONOTONIC, which freezes across suspend).
/// A CLOCK_MONOTONIC freeze cannot be simulated in a unit test, so we exercise the injected-fn seam: a
/// fake boot clock that we advance past the ttl must flip mayMutate to false and make a gated mutate
/// fail closed with ABORTED.
TEST(CASPool, WriteFenceUsesInjectedBootClock)
{
    auto backend = std::make_shared<InMemoryBackend>();
    /// Held in a shared atomic, not a plain local: this test mutates the clock below, and the Pool can
    /// outlive this stack frame (a background publish holds `shared_from_this()`), so a by-reference
    /// capture of a local would dangle.
    auto fake_boot = std::make_shared<std::atomic<uint64_t>>(1'000'000);   /// arbitrary boottime origin (ms)
    auto store = DB::Cas::Pool::open(backend, DB::Cas::PoolConfig{
        .pool_prefix = "p",
        .server_root_id = "test",
        .mount_lease_ttl_ms = std::chrono::milliseconds(30000),
        .boot_ms_fn = [fake_boot]
        {
            return fake_boot->load();
        },
    });

    /// Freshly armed at open (deadline = fake_boot + ttl): well within the ttl, mutations are allowed.
    EXPECT_TRUE(store->mayMutate());

    /// Advance the boot clock just short of the deadline — still armed.
    *fake_boot += 29999;
    EXPECT_TRUE(store->mayMutate());

    /// Cross the deadline (ttl elapsed with no renew — a resumed sleeper's view). The fence must expire.
    /// (The "a gated mutate then fails closed with ABORTED" leg used `mutateShardForTest` -- the held
    /// Phase-E shard lane -- and moves there; here we pin the boot-clock fence flip itself.)
    *fake_boot += 2;   /// now fake_boot = origin + 30001 > origin + 30000
    EXPECT_FALSE(store->mayMutate());
}

/// ==== self-remount after GC fence-out (liveness counterpart of the fence-out safety rule) ====

namespace
{

/// GC's fence-out, applied directly: preserve the body, set gc_fenced, bump seq (token-guarded).
void fenceOutMount(DB::Cas::Backend & backend, const String & mount_key)
{
    DB::Cas::tests::OperationForTest op(backend);
    const auto got = (*op).read(mount_key, Retry::standard());
    ASSERT_TRUE(got.has_value());
    MountLease m = decodeMountLease(got->bytes);
    m.gc_fenced = true;
    m.seq += 1;
    ASSERT_TRUE(std::holds_alternative<Committed>(
        (*op).replace(mount_key, encodeMountLease(m), got->etag, Retry::standard())));
}

}

TEST(CASPoolRemount, FenceOutThenSelfRemountRestoresWrites)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);
    const String mount_key = store->layout().mountKey("test");
    const uint64_t epoch_before = decodeMountLease(readObj(*backend, mount_key)->bytes).writer_epoch;
    EXPECT_EQ(store->liveWriterEpoch(), epoch_before);

    fenceOutMount(*backend, mount_key);

    /// The renewer's next renewal fails closed (foreign touch — never re-mint).
    EXPECT_THROW(store->renewWatermarkOnce(), DB::Exception);

    /// Self-remount claims a FRESH incarnation: epoch bumped, gc_fenced cleared, writes restored.
    ASSERT_TRUE(store->tryRemountOnce());
    const MountLease after = decodeMountLease(readObj(*backend, mount_key)->bytes);
    EXPECT_EQ(after.writer_epoch, epoch_before + 1);
    EXPECT_FALSE(after.gc_fenced);
    EXPECT_EQ(store->liveWriterEpoch(), epoch_before + 1);

    /// The renewal path works again (the new renewer owns the slot). (The follow-on "...and so does a
    /// ref-shard mutation" check used `mutateShardForTest` -- the held Phase-E shard lane -- and moves
    /// to Phase E's own tests; the self-remount liveness assertion above is the point of this test.)
    EXPECT_NO_THROW(store->renewWatermarkOnce());
}

TEST(CASPoolRemount, OldEpochBuildFailsClosedAfterRemount)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);
    auto build = store->beginPartWrite({});

    fenceOutMount(*backend, store->layout().mountKey("test"));
    ASSERT_TRUE(store->tryRemountOnce());

    /// The build was minted under the superseded incarnation — every further step fails closed.
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR,
        [&] { build->putBlob(DB::Cas::tests::idOf("x"), DB::Cas::BlobSource::fromString("x")); });

    /// A FRESH build under the live incarnation works once its publication edge is durable.
    const RootNamespace ns{"srv/remount"};
    PartWriteInfo info;
    info.intended_ref = ns.string() + "/fresh";
    auto fresh = store->beginPartWrite(info);
    const ManifestId id = fresh->stageManifest({blobEntryFor("data.bin", DB::Cas::tests::u128Of("y"))});
    fresh->precommitAdd(ns, "fresh", id);
    EXPECT_NO_THROW(fresh->putBlob(DB::Cas::tests::idOf("y"), DB::Cas::BlobSource::fromString("y")));
    fresh->abandon();
}

TEST(CASPoolRemount, ForeignOwnerIsNeverTakenOver)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);
    const String mount_key = store->layout().mountKey("test");

    /// A genuinely foreign uuid holds the mount (live or not — foreign is terminal for the claim).
    DB::Cas::tests::OperationForTest overwrite_op(*backend);
    const auto got = (*overwrite_op).read(mount_key, Retry::standard());
    MountLease foreign = decodeMountLease(got->bytes);
    foreign.server_uuid = foreign.server_uuid + DB::UInt128(1);
    foreign.seq += 1;
    ASSERT_TRUE(std::holds_alternative<Committed>(
        (*overwrite_op).replace(mount_key, encodeMountLease(foreign), got->etag, Retry::standard())));

    EXPECT_FALSE(store->tryRemountOnce());
    /// The foreign body is untouched (no takeover, ever).
    EXPECT_EQ(decodeMountLease(readObj(*backend, mount_key)->bytes).server_uuid, foreign.server_uuid);

    /// Move the parent fixture to the production-recognized fenced terminal state before explicitly
    /// destroying its superseded renewer. The unfenced foreign-release guard is covered separately below.
    fenceOutMount(*backend, mount_key);
    store.reset();

    /// A foreign owner is never taken over — at remount OR at release. This was an `EXPECT_DEATH`
    /// pinning a `LOGICAL_ERROR` abort on the release half; the abort fired from `~Pool` and defeated
    /// `finishTeardown`'s own catch by aborting at exception construction. The runtime never observed a
    /// deposition (the slot was overwritten out of band), so the release takes the
    /// exclusivity-violation arm: refuse, leave the foreign occupant untouched, and SURVIVE teardown.
    auto foreign_backend = std::make_shared<InMemoryBackend>();
    auto invalid_store = DB::Cas::tests::openPoolForTest(foreign_backend);
    const String foreign_mount_key = invalid_store->layout().mountKey("test");
    DB::Cas::tests::OperationForTest foreign_overwrite_op(*foreign_backend);
    const auto foreign_got = (*foreign_overwrite_op).read(foreign_mount_key, Retry::standard());
    ASSERT_TRUE(foreign_got.has_value());
    MountLease foreign_lease = decodeMountLease(foreign_got->bytes);
    foreign_lease.server_uuid = foreign_lease.server_uuid + DB::UInt128(1);
    foreign_lease.seq += 1;
    ASSERT_TRUE(std::holds_alternative<Committed>((*foreign_overwrite_op).replace(
        foreign_mount_key, encodeMountLease(foreign_lease), foreign_got->etag, Retry::standard())));
    const auto occupant_before = readObj(*foreign_backend, foreign_mount_key);
    ASSERT_TRUE(occupant_before.has_value());

    EXPECT_FALSE(invalid_store->tryRemountOnce()) << "a foreign owner is never taken over at remount";

    const uint64_t violations_before
        = ProfileEvents::global_counters[ProfileEvents::CASMountExclusivityViolation].load();
    invalid_store.reset();   /// must not abort, must not terminate

    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASMountExclusivityViolation].load(),
              violations_before + 1)
        << "the release must report the broken single-writer guarantee rather than dying on it";
    const auto occupant_after = readObj(*foreign_backend, foreign_mount_key);
    ASSERT_TRUE(occupant_after.has_value()) << "nor is it taken over at release";
    EXPECT_EQ(occupant_after->bytes, occupant_before->bytes)
        << "the slot must be left byte-for-byte as the foreign owner wrote it";
}

TEST(CASPoolRemount, ShutdownGuardRefusesToArmRemount)
{
    auto backend = std::make_shared<InMemoryBackend>();
    /// `background_watermark = true` so `scheduleRemount` can latch a recovery generation for the
    /// persistent worker in production mode (the same gate both runtime workers check).
    auto store = DB::Cas::Pool::open(backend,
        DB::Cas::PoolConfig{.pool_prefix = "p", .server_root_id = "test", .background_watermark = true});

    /// Teardown has begun: `Pool` latches this before joining either persistent worker.
    store->beginShutdownForTest();

    /// A lease-renewal failure firing during teardown re-enters `scheduleRemount`. With the guard it
    /// must refuse to latch another generation after the workers are stopping.
    EXPECT_FALSE(store->scheduleRemountForTest())
        << "scheduleRemount must not latch recovery work once teardown has begun";
}

namespace
{
/// A sequenced fake boot clock: the first N `bootMsNow()` calls return the values queued via
/// `.queue`, in order; every call after the queue drains returns `.steady`. `CasMountRuntime::bootMsNow`
/// re-invokes `PoolConfig::boot_ms_fn` on EVERY call, with zero memoization -- so a plain call-counter
/// deterministically distinguishes an early (anchor) reading from a later (response-time) one, with no
/// real sleep and no threads.
struct SequencedBootClock
{
    std::vector<uint64_t> queue;
    size_t next = 0;
    uint64_t steady = 0;

    uint64_t operator()()
    {
        if (next < queue.size())
            return queue[next++];
        return steady;
    }
};
}

/// Phase B addendum 2 (task 5b review, reviewer's probe): the self-remount arm must anchor at the
/// claim attempt's pre-I/O instant (`remount_anchor_boot_ms`, captured right after `installRenewer`
/// and right before `renewerStart()` in `Pool::tryRemountOnce`), never at a later reading taken after
/// `renewerStart`/`quiesceRefTablesForRemount` have already run.
///
/// The two `bootMsNow()` calls of interest, in the ORDER each code version issues them:
///   - FIXED code: call #1 = the new anchor (`remount_anchor_boot_ms`, before `renewerStart`);
///     call #2 = `MountLeaseRenewer::prepareRenew`'s own internal boot read inside `renewerStart`'s
///     `doStart` (feeds only the renewer's OWN internal `confirmed_deadline_ms` -- unrelated to the
///     Pool-level arm -- so its value is irrelevant to the arm post-fix).
///   - PRE-FIX code (no anchor line): call #1 = that SAME `prepareRenew` read (now the first boot
///     call of the attempt, since nothing reads the clock before `renewerStart`); call #2 = the
///     arm-site's own `mount_runtime.bootMsNow()`, read AFTER `renewerStart` returns -- the stale,
///     response-time reading this whole fix exists to stop using.
/// A sequenced clock returning 10000 then 11000 (a later response-time reading that remains inside
/// the normal renewal window) therefore arms the FIXED code from 10000 and the PRE-FIX code from
/// 11000, regardless of which call site reads which value -- letting a single deterministic probe
/// (`mayMutate()` at boot == 10000+ttl) tell
/// them apart with no sleep and no thread. (TDD evidence for both branches is recorded in the task-5
/// report, not re-asserted here: this test body only encodes the FIXED expectation.)
TEST(CASPoolRemount, RemountArmAnchorsAtClaimAttemptNotResponseTime)
{
    /// Heap-owned, not a plain stack local: the Pool can outlive this stack frame (a background
    /// publish holds `shared_from_this()`), so a by-reference capture of a local would dangle.
    auto clock = std::make_shared<SequencedBootClock>();
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = DB::Cas::Pool::open(backend, DB::Cas::PoolConfig{
        .pool_prefix = "p", .server_root_id = "test",
        .mount_lease_ttl_ms = std::chrono::milliseconds(30'000),
        .boot_ms_fn = [clock]
        {
            return (*clock)();
        },
    });
    ASSERT_TRUE(store);

    /// Trip the fence exactly as every other remount test in this file does.
    fenceOutMount(*backend, store->layout().mountKey("test"));

    /// Arm the sequence for the upcoming remount attempt: the initial `open` above already drained
    /// an unrelated number of `bootMsNow()` calls (all served from `.steady = 0` -- irrelevant, since
    /// nothing probes the resulting arm before this point). Reset the counter so the FIRST call from
    /// here on is the remount attempt's own call #1.
    clock->queue = {10000, 11000};
    clock->next = 0;

    ASSERT_TRUE(store->tryRemountOnce());

    /// Probe at boot == anchor + ttl (10000 + 30000 = 40000): the fixed code armed from the anchor
    /// (10000), so the fence has JUST expired here -- `mayMutate` must be false. (The pre-fix code
    /// would still read `mayMutate` as true here, armed from 11000 + 30000 -- see the TDD run in the
    /// report.)
    clock->steady = 40000;
    EXPECT_FALSE(store->mayMutate())
        << "the remount arm must anchor at the claim attempt's pre-I/O instant, not a later "
           "response-time reading taken after renewerStart/quiesceRefTablesForRemount";
}

/// ==== self-remount vs. a live successor carrying the same uuid under the unsafe-reclaim knob ====
///
/// `cas_unsafe_remount_no_delay` is consulted at exactly one site: the writable `Pool::open` claim.
/// `Pool::tryRemountOnce` (self-remount after a fence loss) does NOT consult it -- an incarnation
/// superseded by a duplicate-uuid process must still OBSERVE the slot's write-token before it may
/// reclaim, or two processes sharing a uuid (a copied uuid file, a stalled predecessor restarted under
/// the knob) would alternate authority indefinitely.

TEST(CASMountRemount, SupersededIncarnationDoesNotReclaimALiveSuccessor)
{
    auto backend = std::make_shared<InMemoryBackend>();
    /// Held in shared atomics, not plain locals: `wait_sleep_fn`/`setWaitSleepForTest` below mutate
    /// them, and each Pool can outlive this stack frame (a background publish holds
    /// `shared_from_this()`), so a by-reference or by-raw-pointer capture of a local would dangle.
    auto boot_a = std::make_shared<std::atomic<uint64_t>>(0);
    auto boot_b = std::make_shared<std::atomic<uint64_t>>(0);
    /// Mirrors `UncleanOpenPaysOnlyTheObservationWindow`'s tiny budget: the 1s lease TTL below is far
    /// under the default `cas_request_budget`, so it must be scaled down to fit the required-timeout
    /// inequality (attempt_timeout + safety_margin < lease TTL).
    const CasRequestBudget tiny_budget{
        .attempt_timeout_ms = 50, .lease_safety_margin_ms = 50, .connect_timeout_cap_ms = std::nullopt};
    auto config_for = [&](const std::shared_ptr<std::atomic<uint64_t>> & boot, bool unsafe)
    {
        return PoolConfig{
            .pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "test",
            .mount_lease_ttl_ms = std::chrono::milliseconds(1000),
            .mount_renew_period = std::chrono::milliseconds(200),
            .unsafe_remount_no_delay = unsafe,
            .cas_request_budget = tiny_budget,
            .boot_ms_fn = [boot]
            {
                return boot->load();
            },
            .wait_sleep_fn = [boot](uint64_t ms)
            {
                *boot += ms;
            },
        };
    };

    PoolPtr pool_a = Pool::open(backend, config_for(boot_a, /*unsafe=*/false));
    ASSERT_TRUE(pool_a);
    /// B carries the SAME (server_root_id, server_id) as A -- a copied uuid file -- and opens over A's
    /// still-live slot under the operator's unsafe knob, reclaiming it at once (no observation).
    PoolPtr pool_b = Pool::open(backend, config_for(boot_b, /*unsafe=*/true));
    ASSERT_TRUE(pool_b);
    EXPECT_NE(pool_a->liveWriterEpoch(), pool_b->liveWriterEpoch())
        << "the unsafe reclaim must have minted B a fresh epoch over A's slot";

    /// A's next renewal meets the token guard: same uuid, a newer epoch now sits on the slot. Pin the
    /// terminal classification directly (the "superseded" branch of `throwRenewConflict`, the one
    /// that maps to `MountRenewOutcome::Terminal`) rather than accepting any exception -- no accessor
    /// exposes the renewer's outcome/state today, so the error code and the classification's own
    /// wording are what distinguish this from every other terminal reason (foreign owner, GC fence,
    /// vanished slot, an unresolved write).
    try
    {
        pool_a->renewWatermarkOnce();
        FAIL() << "A's renewal must be refused once B's reclaim superseded its epoch";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::ABORTED);
        EXPECT_NE(e.message().find("superseded by a newer incarnation"), std::string::npos)
            << "actual message: " << e.message();
    }
    EXPECT_FALSE(pool_a->mayMutate()) << "the superseded classification must trip A's local write fence closed";

    /// A's self-remount now observes the slot's write-token. Drive B's renewal from INSIDE every one
    /// of A's observation polls, so the token never stabilizes across the whole bounded observation --
    /// the knob is not consulted by `tryRemountOnce` (only by `Pool::open`), so nothing else could let
    /// A reclaim a slot a live successor keeps renewing. This cannot deadlock: A and B are distinct
    /// `Pool` objects, so B's `renewWatermarkOnce` takes none of A's locks (each `Pool` owns its own
    /// `remount_mutex`), and the wait fires between `claimMountAwaitingExpiry`'s polls -- with no
    /// backend request of A's own in flight -- so B's call is the only one touching the shared
    /// in-memory backend at that instant.
    /// Heap-owned, not a plain local: same lifetime rule as `boot_a`/`boot_b` above.
    auto polls = std::make_shared<std::atomic<size_t>>(0);
    pool_a->setWaitSleepForTest([boot_a, boot_b, polls, pool_b](uint64_t ms)
    {
        *boot_a += ms;
        ++(*polls);
        *boot_b += ms;
        EXPECT_NO_THROW(pool_b->renewWatermarkOnce());
    });
    EXPECT_FALSE(pool_a->tryRemountOnce())
        << "a superseded incarnation must never reclaim a live successor's slot";
    /// Bounded, not merely nonzero: B renews on every poll, so the observed token changes every
    /// iteration and the FIRST (non-restart) observation start plus `kMaxObservationRestarts` further
    /// restarts is exactly the number of polls before `claimMountAwaitingExpiry` gives up -- one
    /// `sleep_ms_fn` call per iteration that does not itself exceed the bound, and none on the
    /// terminal iteration that does. A widened or removed restart bound would make this hang instead
    /// of failing, so pin the exact count rather than only asserting it ran.
    EXPECT_EQ(polls->load(), DB::Cas::kMaxObservationRestarts + 1)
        << "the observation must give up after exactly kMaxObservationRestarts restarts, not wait "
           "indefinitely for a live twin to go quiet";

    const MountLease final_lease = decodeMountLease(readObj(*backend, pool_a->layout().mountKey("test"))->bytes);
    EXPECT_EQ(final_lease.writer_epoch, pool_b->liveWriterEpoch())
        << "the mount slot must still belong to B's incarnation -- A never reclaimed it";
}

/// Cutoff-only fencing: with no renewals and no competing incarnation at all, crossing the armed
/// deadline on the local BOOTTIME clock alone must fence a mount closed -- the mechanism
/// `SupersededIncarnationDoesNotReclaimALiveSuccessor` above relies on is not special-cased to a
/// renewal conflict; the plain boot-clock cutoff fences unconditionally.
TEST(CASMountRemount, CutoffFencesWithoutRenewals)
{
    auto backend = std::make_shared<InMemoryBackend>();
    /// Held in a shared atomic, not a plain local: this test mutates the clock below, and the Pool can
    /// outlive this stack frame (a background publish holds `shared_from_this()`), so a by-reference
    /// capture of a local would dangle.
    auto boot = std::make_shared<std::atomic<uint64_t>>(0);
    const CasRequestBudget tiny_budget{
        .attempt_timeout_ms = 50, .lease_safety_margin_ms = 50, .connect_timeout_cap_ms = std::nullopt};
    PoolPtr store = Pool::open(backend, PoolConfig{
        .pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "test",
        .mount_lease_ttl_ms = std::chrono::milliseconds(1000),
        .mount_renew_period = std::chrono::milliseconds(200),
        .cas_request_budget = tiny_budget,
        .boot_ms_fn = [boot]
        {
            return boot->load();
        },
        .wait_sleep_fn = [boot](uint64_t ms)
        {
            *boot += ms;
        },
    });
    ASSERT_TRUE(store);
    EXPECT_TRUE(store->mayMutate()) << "freshly armed at open, well within the ttl";

    /// No renewals at all -- advance the boot clock past the armed deadline (open's claim anchor plus
    /// the lease ttl) on this incarnation's own clock alone.
    *boot += 1001;
    EXPECT_FALSE(store->mayMutate())
        << "crossing the armed deadline must fence closed on the boot clock alone, with no renewal "
           "conflict needed to trip it";
}

/// ==== rev.6 Task 5: clean-release drain gates the farewell marker ====

namespace
{
/// Makes every write whose key contains `fault_key_substr` throw an ambiguous exception -- the minimal
/// subset of `RefWriterTestBackend`'s fault injection (gtest_cas_ref_writer.cpp) this file's shutdown
/// and remount tests need to drive a ref-log append into the wedge outcome. It stays armed: one
/// ambiguous attempt is not a wedge, because the engine resolves it by reading and reissues -- the lane
/// wedges only once a bound refuses with an attempt already sent, so the tests injecting it also give
/// the pool a clock they can advance.
class UnresolvedPutBackend final : public DB::Cas::tests::CountingBackend
{
public:
    String fault_key_substr;
    int fault_count = 0;

    /// The fault sits on the WRITE PRIMITIVE: the ref-log append it models is issued there. Nothing
    /// reaches the store, so the engine's resolve read proves the key absent and every reissue is
    /// ambiguous again -- which is what leaves the lane wedged once a bound refuses.
    std::expected<String, RawConflict> write(const String & key, const String & bytes,
        const std::optional<String> & expected_value, DB::Cas::TransportAccess & access) override
    {
        /// Only the create: a ref-log append is a create-if-absent, so a conditional write on the same
        /// key must not consume the fault.
        if (!expected_value && fault_count > 0 && !fault_key_substr.empty()
            && key.find(fault_key_substr) != String::npos)
        {
            --fault_count;
            throw Poco::TimeoutException("UnresolvedPutBackend: simulated ambiguous result (response lost)");
        }
        return DB::Cas::tests::CountingBackend::write(key, bytes, expected_value, access);
    }
};

class RuntimeRenewBackend final : public DB::Cas::tests::CountingBackend
{
public:
    enum class Fault : uint8_t
    {
        None,
        ThrowBefore,
        LandThenThrow,
        BlockThenDelegate,
        BlockThenThrow,
    };

    Fault fault = Fault::None;
    DB::Cas::tests::ManualBarrier * barrier = nullptr;
    std::function<void()> after_commit;
    /// Runs just before an armed fault throws. The engine draws its inter-attempt backoff randomly and
    /// admits the reissue against that drawn duration, so a test that needs the ambiguity to be refused
    /// rather than reissued has to move the injected clock here -- from inside the attempt, which is the
    /// only point between admission and the resolve read a test can reach.
    std::function<void()> before_throw;

    /// The fault sits on the WRITE PRIMITIVE, and only on a CONDITIONAL one: a lease renewal is a
    /// replace, so a create on the same key must not consume the one-shot fault.
    std::expected<String, RawConflict> write(const String & key, const String & bytes,
        const std::optional<String> & expected_value, DB::Cas::TransportAccess & access) override
    {
        if (!expected_value)
            return DB::Cas::tests::CountingBackend::write(key, bytes, expected_value, access);
        const Fault current = std::exchange(fault, Fault::None);
        if (current == Fault::BlockThenDelegate || current == Fault::BlockThenThrow)
        {
            if (!barrier)
                throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "runtime renewal barrier is absent");
            barrier->arriveAndWait();
        }
        if (current == Fault::ThrowBefore || current == Fault::BlockThenThrow)
        {
            if (before_throw)
                before_throw();
            throw Poco::TimeoutException("injected runtime renewal ambiguity before result");
        }

        auto result = DB::Cas::tests::CountingBackend::write(key, bytes, expected_value, access);
        if (after_commit)
            after_commit();
        if (current == Fault::LandThenThrow)
            throw Poco::TimeoutException("injected runtime renewal response loss after commit");
        return result;
    }
};

CasRequestBudget runtimeRenewBudget();

/// A directly-constructed `CasMountRuntime` plus the two request planes it needs. `Pool` builds those
/// from its own members; a test has no `Pool`, so the mount plane's fence reaches the runtime through
/// this holder -- the closures run only once the runtime is issuing requests, well after construction.
class RuntimeUnderTest
{
public:
    template <typename BackendT, typename... Args>
    RuntimeUnderTest(const std::shared_ptr<BackendT> & backend, Args &&... args)
        : mount(backend, DB::Cas::Fence{
              [this] { return runtime.fenceGeneration(); },
              [this](uint64_t g, uint64_t needed) { return runtime.admit(g, needed); },
              [this](uint64_t g) { runtime.checkFenceOrThrow(g); }})
        , farewell(backend, DB::Cas::Fence::open())
        , runtime(backend, mount, farewell, std::forward<Args>(args)...)
    {
        /// What the request engine reserves per attempt is the BACKEND's attempt timeout, not the
        /// budget field alone; every construction of this holder pairs the two via `runtimeRenewBudget`,
        /// the sole budget it is ever built with in this file.
        backend->setAttemptTimeoutMs(runtimeRenewBudget().attempt_timeout_ms);
        /// The runtime arms its lease deadline on ITS boot clock, and the engine measures that deadline
        /// against the clock it reads. Production runs both on `CLOCK_BOOTTIME`, so they agree; a test
        /// that injects one MUST inject the other, or `Retry::untilLeaseSafe` compares a synthetic
        /// deadline against real boottime, finds it long past, and refuses every request unsent.
        mount.setNowFnForTest([this] { return runtime.bootMsNow(); });
        farewell.setNowFnForTest([this] { return runtime.bootMsNow(); });
    }

    /// The workers are joined HERE, not only by the tests that assert on teardown: `CasMountRuntime`
    /// aborts the process when it is destroyed with a worker still joinable, so an exception on any
    /// path out of a test body -- a barrier that timed out, an assertion that threw -- would take the
    /// whole binary down and hide every test after it.
    ~RuntimeUnderTest()
    {
        try
        {
            runtime.stopBackgroundWorkers();
        }
        catch (...)   // NOLINT(bugprone-empty-catch)
        {
        }
    }

    CasMountRuntime & operator*() { return runtime; }

private:
    DB::Cas::CasRequests mount;
    DB::Cas::CasRequests farewell;
    CasMountRuntime runtime;
};

enum class ForeignConflictSinkBehavior : uint8_t
{
    ReenterSameRuntime,
    Throw,
};

void verifyForeignConflictSinkIsNonInterfering(ForeignConflictSinkBehavior behavior)
{
    auto backend = std::make_shared<RuntimeRenewBackend>();
    const Layout layout(
        behavior == ForeignConflictSinkBehavior::ReenterSameRuntime
            ? "runtime-reentrant-foreign-conflict"
            : "runtime-throwing-foreign-conflict");
    const String server_root_id = "test";
    const String key = layout.mountKey(server_root_id);
    uint64_t wall_ms = 1000;
    uint64_t boot_ms = 100;
    const UInt128 uuid{1};
    ASSERT_EQ(claimMount(*DB::Cas::tests::OperationForTest(backend), layout, server_root_id, uuid, 1, wall_ms, 1000).kind, MountClaimResult::Claimed);

    std::vector<CasEvent> events;
    bool reentered = false;
    std::optional<PoolLifecycle> reentrant_lifecycle;
    std::optional<bool> reentrant_may_mutate;
    CasMountRuntime * runtime_ptr = nullptr;
    CasEventSink sink = [&](CasEvent event)
    {
        const bool foreign_conflict
            = event.type == CasEventType::MountConflict && event.outcome == "foreign_writer";
        events.push_back(event);
        if (!foreign_conflict)
            return;
        if (behavior == ForeignConflictSinkBehavior::ReenterSameRuntime)
        {
            if (!std::exchange(reentered, true))
            {
                reentrant_lifecycle = runtime_ptr->lifecycle();
                reentrant_may_mutate = runtime_ptr->mayMutate();
                throw std::runtime_error("injected reentrant mount diagnostic sink failure");
            }
        }
        else
        {
            throw std::runtime_error("injected mount diagnostic sink failure");
        }
    };
    RuntimeUnderTest runtime_holder(
        backend,
        layout,
        MountConfig{
            .mount_lease_ttl_ms = std::chrono::milliseconds(1000),
            .boot_ms_fn = [&] { return boot_ms; },
        },
        server_root_id,
        sink,
        runtimeRenewBudget(),
        [] { return false; });
    CasMountRuntime & runtime = *runtime_holder;
    runtime_ptr = &runtime;
    runtime.installRenewer(uuid, 1, [&] { return wall_ms; });
    const uint64_t anchor = runtime.startRenewer();
    runtime.armMountFence(uuid, 1, anchor + 1000);

    DB::Cas::tests::OperationForTest successor_op(*backend);
    auto ours = (*successor_op).read(key, Retry::standard());
    ASSERT_TRUE(ours.has_value());
    MountLease successor = decodeMountLease(ours->bytes);
    successor.server_uuid = UInt128{2};
    successor.writer_epoch = 9;
    successor.seq += 1;
    ASSERT_TRUE(std::holds_alternative<Committed>(
        (*successor_op).replace(key, encodeMountLease(successor), ours->etag, Retry::standard())));
    const uint64_t skipped_before
        = ProfileEvents::global_counters[ProfileEvents::CASMountReleaseSkippedForeignOccupant].load();
    const uint64_t violations_before
        = ProfileEvents::global_counters[ProfileEvents::CASMountExclusivityViolation].load();

    int failure_code = 0;
    String failure_message;
    try
    {
        runtime.renewWatermarkOnce();
        ADD_FAILURE() << "authoritative foreign successor must terminalize renewal";
    }
    catch (const DB::Exception & e)
    {
        failure_code = e.code();
        failure_message = e.message();
    }

    EXPECT_EQ(reentered, behavior == ForeignConflictSinkBehavior::ReenterSameRuntime);
    if (behavior == ForeignConflictSinkBehavior::ReenterSameRuntime)
    {
        ASSERT_TRUE(reentrant_lifecycle.has_value());
        EXPECT_EQ(*reentrant_lifecycle, PoolLifecycle::Live);
        ASSERT_TRUE(reentrant_may_mutate.has_value());
        EXPECT_TRUE(*reentrant_may_mutate);
    }
    else
    {
        EXPECT_FALSE(reentrant_lifecycle.has_value());
        EXPECT_FALSE(reentrant_may_mutate.has_value());
    }
    EXPECT_EQ(failure_code, DB::ErrorCodes::ABORTED) << failure_message;
    EXPECT_NE(failure_message.find("held by a foreign server"), String::npos) << failure_message;
    EXPECT_FALSE(runtime.mayMutate());
    EXPECT_EQ(runtime.lifecycle(), PoolLifecycle::TransientNotLive);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASMountReleaseSkippedForeignOccupant].load(),
              skipped_before + 1);
    const auto failed = std::find_if(events.begin(), events.end(), [](const CasEvent & event)
    {
        return event.type == CasEventType::WatermarkRenew && event.outcome == "failed";
    });
    EXPECT_NE(failed, events.end());
    if (failed != events.end())
        EXPECT_EQ(failed->detail.at("classification"), "conflict");

    const auto successor_before_teardown = readObj(*backend, key);
    ASSERT_TRUE(successor_before_teardown.has_value());
    const uint64_t heads_before_teardown = backend->headCount(key);
    const uint64_t gets_before_teardown = backend->getCount(key);
    const uint64_t writes_before_teardown = backend->putOverwriteCount(key);
    const uint64_t skipped_before_teardown
        = ProfileEvents::global_counters[ProfileEvents::CASMountReleaseSkippedForeignOccupant].load();
    runtime.finishTeardown(true);
    EXPECT_EQ(backend->headCount(key), heads_before_teardown);
    EXPECT_EQ(backend->getCount(key), gets_before_teardown);
    EXPECT_EQ(backend->putOverwriteCount(key), writes_before_teardown);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASMountReleaseSkippedForeignOccupant].load(),
              skipped_before_teardown);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASMountExclusivityViolation].load(), violations_before);
    const auto successor_after_teardown = readObj(*backend, key);
    ASSERT_TRUE(successor_after_teardown.has_value());
    EXPECT_EQ(successor_after_teardown->bytes, successor_before_teardown->bytes);
}

TEST(CASPoolRemount, SameRuntimeReentrantForeignConflictSinkCannotReplaceTerminalOutcome)
{
    verifyForeignConflictSinkIsNonInterfering(ForeignConflictSinkBehavior::ReenterSameRuntime);
}

TEST(CASPoolRemount, ThrowingForeignConflictSinkCannotReplaceTerminalOutcome)
{
    verifyForeignConflictSinkIsNonInterfering(ForeignConflictSinkBehavior::Throw);
}

class RemountStepBackend final : public DB::Cas::tests::CountingBackend
{
public:
    void failNextRead(String key)
    {
        failed_key = std::move(key);
    }

    /// The fault sits on the READ PRIMITIVE: the lifecycle gate reads `_pool_meta` through
    /// `probeSentinelRaw`, which speaks the primitives. A legacy caller reaches it anyway, through the
    /// forwarder, so arming it here covers both surfaces rather than only one.
    std::optional<Raw> read(const String & key, DB::Cas::TransportAccess & access) override
    {
        if (!failed_key.empty() && key == failed_key)
        {
            failed_key.clear();
            throw DB::Exception(DB::ErrorCodes::NETWORK_ERROR, "injected remount probe failure");
        }
        return DB::Cas::tests::CountingBackend::read(key, access);
    }

private:
    String failed_key;
};

class ScopedRemountLogCapture
{
public:
    ScopedRemountLogCapture()
        : logger(getLogger("CasPool"))
        , channel(new Poco::StreamChannel(stream))
        , old_channel(logger->getChannel(), /*shared=*/true)
        , old_level(logger->getLevel())
    {
        logger->setChannel(channel.get());
        logger->setLevel("information");
    }

    ~ScopedRemountLogCapture()
    {
        logger->setChannel(old_channel);
        logger->setLevel(old_level);
    }

    String captured() const { return stream.str(); }

private:
    LoggerPtr logger;
    std::ostringstream stream; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::AutoPtr<Poco::StreamChannel> channel;
    /// A real reference (shared=true), so the parked previous channel cannot die while ours is installed.
    Poco::AutoPtr<Poco::Channel> old_channel;
    int old_level;
};

class ScopedParkedRenewalLogCapture
{
public:
    ScopedParkedRenewalLogCapture()
        : logger(getLogger("CasMountLeaseRenewer"))
        , channel(new Poco::StreamChannel(stream))
        , old_channel(logger->getChannel(), /*shared=*/true)
        , old_level(logger->getLevel())
    {
        logger->setChannel(channel.get());
        logger->setLevel("information");
    }

    ~ScopedParkedRenewalLogCapture()
    {
        logger->setChannel(old_channel);
        logger->setLevel(old_level);
    }

    String captured() const { return stream.str(); }

private:
    LoggerPtr logger;
    std::ostringstream stream; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::AutoPtr<Poco::StreamChannel> channel;
    /// A real reference (shared=true), so the parked previous channel cannot die while ours is installed.
    Poco::AutoPtr<Poco::Channel> old_channel;
    int old_level;
};

size_t countRemountFinalLogs(const String & output)
{
    constexpr std::string_view needle = "CAS whole-chain remount attempt";
    size_t count = 0;
    for (size_t pos = 0; (pos = output.find(needle, pos)) != String::npos; pos += needle.size())
        ++count;
    return count;
}

class WorkerExitLatch
{
public:
    void recordExit()
    {
        std::lock_guard lock(mutex);
        ++exits;
        cv.notify_all();
    }

    bool waitForAtLeast(uint64_t expected)
    {
        std::unique_lock lock(mutex);
        return cv.wait_for(lock, std::chrono::seconds(20), [&] { return exits >= expected; });
    }

    uint64_t count() const
    {
        std::lock_guard lock(mutex);
        return exits;
    }

private:
    mutable std::mutex mutex;
    std::condition_variable cv;
    uint64_t exits = 0;
};

/// The budget every runtime-renewal test uses. `attempt_timeout_ms`/`lease_safety_margin_ms` bound the
/// mount lease's own admission arithmetic; a renewal write's own attempt count and backoff are the
/// request engine's fence-derived `Retry::standard()` policy now, not a budget knob -- every caller of
/// this helper used to pass `max_attempts=1` and no other value, so that parameter carried nothing.
CasRequestBudget runtimeRenewBudget()
{
    return CasRequestBudget{
        .attempt_timeout_ms = 10,
        .lease_safety_margin_ms = 20,
        /// The default cap (1000 ms) would make the envelope (10 + 2*1000 = 2010) blow every tiny TTL
        /// this budget is used against; no connect notion is exercised by these tests.
        .connect_timeout_cap_ms = std::nullopt,
    };
}
}

TEST(CASPoolShutdown, CleanStopDrainsAndWritesFarewell)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = DB::Cas::Pool::open(backend, DB::Cas::PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    publishPart(store, "srv/clean_stop", "x", "payload");

    const String mount_key = store->layout().mountKey("test");
    store.reset();   /// drives ~Pool(): with no in-flight ref-log PUT, the drain must succeed.

    const auto got = readObj(*backend, mount_key);
    ASSERT_TRUE(got.has_value());
    const MountLease lease = decodeMountLease(got->bytes);
    EXPECT_EQ(lease.min_active_build_sequence, std::numeric_limits<uint64_t>::max())
        << "a clean drain (no in-flight ref-log PUT) must write the farewell marker";
}

TEST(CASPoolShutdown, UnresolvedWedgeSkipsFarewell)
{
    CasRequestBudget budget;
    budget.attempt_timeout_ms = 100;
    budget.lease_safety_margin_ms = 100;

    auto backend = std::make_shared<UnresolvedPutBackend>();
    /// What the request engine reserves per attempt is the BACKEND's attempt timeout, not the budget
    /// field alone; pair the two so the mount lease's admission arithmetic sees what the budget claims.
    backend->setAttemptTimeoutMs(budget.attempt_timeout_ms);
    /// Held in a shared atomic, not a plain local: `wait_sleep_fn` and the retry-sleep hook below
    /// mutate it, and the Pool can outlive this stack frame (a background publish holds
    /// `shared_from_this()`), so a by-reference capture of a local would dangle.
    auto fake_boot = std::make_shared<std::atomic<uint64_t>>(1'000'000);
    auto store = DB::Cas::Pool::open(backend, DB::Cas::PoolConfig{
        .pool_prefix = "p", .server_root_id = "test", .cas_request_budget = budget,
        .boot_ms_fn = [fake_boot]
        {
            return fake_boot->load();
        },
        .wait_sleep_fn = [fake_boot](uint64_t ms)
        {
            *fake_boot += ms;
        }});
    /// The engine's own inter-attempt sleep advances the same clock its deadlines are read from, so the
    /// retry bound is reached in test time rather than in ninety real seconds.
    store->setCasRetrySleepForTest([fake_boot](uint64_t ms)
    {
        *fake_boot += ms;
    });
    /// By value: `layout` is used after `store.reset()` below, a reference would dangle.
    const Layout layout = store->layout();
    const RootNamespace ns{"srv/wedge_shutdown"};
    /// Stage B (Task 4-C): pin `ns` to the Stage-A sentinel BEFORE its first real touch, so the fault
    /// injected below (computed from that same sentinel) lands on the key production actually writes
    /// to -- otherwise the real append mints an unrelated random incarnation and the fault misses.
    DB::Cas::tests::casAdmitRecoverableEntry(*backend, layout, ns, store->liveWriterEpoch());
    publishPart(store, ns.string(), "x", "payload");

    /// Force the ref-log append the drop below performs into the wedge outcome (as in the wedge tests
    /// in gtest_cas_ref_writer.cpp): every attempt is ambiguous, so the lane is still unresolved when
    /// the retry bound refuses.
    backend->fault_key_substr = layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_log/";
    backend->fault_count = std::numeric_limits<int>::max();
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropRef(ns, "x"); });
    ASSERT_TRUE(store->refLaneWedgedForTest(ns));

    const String mount_key = store->layout().mountKey("test");
    store.reset();   /// drives ~Pool(): the still-wedged lane must skip the farewell marker.

    const auto got = readObj(*backend, mount_key);
    ASSERT_TRUE(got.has_value());
    const MountLease lease = decodeMountLease(got->bytes);
    EXPECT_NE(lease.min_active_build_sequence, std::numeric_limits<uint64_t>::max())
        << "an unresolved ref-log PUT must skip the clean-release farewell marker";
    EXPECT_FALSE(lease.gc_fenced);

    /// A successor claimMount on this body must return LiveDoubleStart (unclean path): no certificate of
    /// death (not fenced, not the clean farewell marker, no proven-dead observation) justifies a
    /// same-uuid, different-epoch reclaim.
    const MountClaimResult claim = claimMount(*DB::Cas::tests::OperationForTest(backend), layout, "test", lease.server_uuid,
        lease.writer_epoch + 1, /*now_ms=*/1, /*ttl_ms=*/30000);
    EXPECT_EQ(claim.kind, MountClaimResult::LiveDoubleStart);
}

/// ==== What a writable mount open may block on ====
///
/// Exactly one thing: the token-stability observation window, and only when the predecessor's death
/// has to be OBSERVED rather than certified. The post-reclaim materialization grace (`T_mat`) that
/// used to run beside it is retired -- it existed so a straggler conditional `PUT` from the dying
/// epoch would settle before the successor trusted its recovery LISTINGS, and recovery does not trust
/// listings any more (it walks arithmetically and fences the straggler with an in-band `EpochSeal`).
/// These three tests pin the surviving shape from all three directions: observed-dead, certified-dead,
/// and cleanly departed.

TEST(CASMountOpenWaits, UncleanOpenPaysOnlyTheObservationWindow)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l{"p"};
    DB::Cas::tests::seedPoolMetaForRestart(*b);
    /// Predecessor: claim epoch 7, no farewell (simulate crash: just drop the renewer) -- a bare
    /// `claimMount` plants the lease directly, with no clean-farewell `min_active_build_sequence` marker and no
    /// `gc_fenced`, so the successor below has no certificate of death until it observes one itself.
    ASSERT_EQ(claimMount(*DB::Cas::tests::OperationForTest(b), l, "test", UInt128(1), /*epoch*/ 7, /*now_ms*/ 1000, /*ttl_ms*/ 500).kind,
              MountClaimResult::Claimed);
    /// A real predecessor at epoch 7 durably minted it first (`allocateWriterEpoch` always runs
    /// before the mount claim); seed that durable epoch object here too, or the successor's own
    /// `allocateWriterEpoch` trips the Phase C guard (epoch absent, mount present -> fail closed).
    createObj(*b, l.epochKey("test"), encodeServerEpoch(ServerEpoch{.next_writer_epoch = 8}));

    /// A 500ms lease TTL is far below the default `cas_request_budget` (RFC
    /// cas-s3-timeout-retry-control §required-timeout-model requires attempt_timeout + safety_margin <
    /// lease TTL), so scale the budget down to fit -- mirrors `CasMountStartup::StaleSelfMountReclaimedAfterWait`.
    const CasRequestBudget tiny_budget{
        .attempt_timeout_ms = 50, .lease_safety_margin_ms = 50, .connect_timeout_cap_ms = std::nullopt};

    /// Held in shared, heap-owned state, not plain locals: the hooks below mutate them, and the Pool
    /// can outlive this stack frame (a background publish holds `shared_from_this()`), so a
    /// by-reference capture of a local would dangle.
    auto fake_boot = std::make_shared<std::atomic<uint64_t>>(0);
    auto waits = std::make_shared<SharedWaitLog>();
    PoolPtr store;
    ASSERT_NO_THROW(
        store = Pool::open(b, PoolConfig{
            .pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "test",
            .mount_lease_ttl_ms = std::chrono::milliseconds(500),
            .mount_renew_period = std::chrono::milliseconds(100),
            .cas_request_budget = tiny_budget,
            .boot_ms_fn = [fake_boot]
            {
                return fake_boot->load();
            },
            .wait_sleep_fn = [fake_boot, waits](uint64_t ms)
            {
                *fake_boot += ms;
                waits->push(ms);
            },
        }));
    ASSERT_TRUE(store);

    /// The token-stability observation window is paid in full, pinned to the exact configured
    /// formula (`mountObservationThresholdMs`): threshold_ms = ttl_ms + ttl_ms/20 + poll_interval_ms
    /// = 500 + 25 + 50 = 575 ms, where poll_interval_ms = max(1, mount_renew_period/2) = 50 ms. The
    /// loop only re-checks the threshold between polls, so the observed wait rounds UP to the next
    /// whole poll: ceil(575 / 50) * 50 = 600 ms, i.e. exactly 12 polls of 50 ms each -- because this
    /// predecessor's death was never certified, only observed.
    const std::vector<uint64_t> observed_waits = waits->snapshot();
    uint64_t total = 0;
    for (uint64_t w : observed_waits)
        total += w;
    EXPECT_EQ(total, 600u) << "the observation window must be paid in full, poll-rounded to the "
                              "configured threshold -- neither less (a shortened wait) nor more "
                              "(a reintroduced grace period)";
    /// And every one of those polls is exactly one poll interval -- no wait beyond the observation
    /// poll (the straggler it used to wait out is fenced by the recovery seal instead).
    for (uint64_t w : observed_waits)
        EXPECT_EQ(w, 50u)
            << "an unclean reclaim must not block on any wait beyond the observation poll -- the "
               "straggler it used to wait out is fenced by the recovery seal instead";
}

TEST(CASMountOpenWaits, UnsafeNoDelayOpensWithoutTheObservationWindow)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l{"p"};
    DB::Cas::tests::seedPoolMetaForRestart(*b);
    /// Same predecessor shape as UncleanOpenPaysOnlyTheObservationWindow above: a bare `claimMount`
    /// plants the lease directly, with no clean-farewell marker and no `gc_fenced`, so this slot has no
    /// certificate of death -- only `cas_unsafe_remount_no_delay` below will let the successor skip
    /// observing it.
    ASSERT_EQ(claimMount(*DB::Cas::tests::OperationForTest(b), l, "test", UInt128(1), 7, 1000, 500).kind, MountClaimResult::Claimed);
    /// A real predecessor at epoch 7 durably minted this first; seed it here too, or the successor's
    /// own `allocateWriterEpoch` trips the Phase C guard (epoch absent, mount present -> fail closed).
    createObj(*b, l.epochKey("test"), encodeServerEpoch(ServerEpoch{.next_writer_epoch = 8}));
    /// Held in shared, heap-owned state, not plain locals: the hooks below mutate them, and the Pool
    /// can outlive this stack frame (a background publish holds `shared_from_this()`), so a
    /// by-reference capture of a local would dangle.
    auto events = std::make_shared<DB::Cas::tests::SharedEventLog>();
    auto fake_boot = std::make_shared<std::atomic<uint64_t>>(0);
    auto waits = std::make_shared<SharedWaitLog>();
    PoolPtr store;
    /// Same server_id (uuid) as the seeded predecessor and a different epoch -- exactly the shape
    /// `unsafe_remount_no_delay` is for. Unlike the neighbour test, no wait is expected: the bare
    /// `claimMount` reclaims at once under the operator's authorization.
    ASSERT_NO_THROW(store = Pool::open(b, PoolConfig{
        .pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "test",
        .event_sink = [events](CasEvent e)
        {
            events->push(std::move(e));
        },
        .mount_lease_ttl_ms = std::chrono::milliseconds(500), .mount_renew_period = std::chrono::milliseconds(100),
        .unsafe_remount_no_delay = true,
        .cas_request_budget = CasRequestBudget{.attempt_timeout_ms = 50, .lease_safety_margin_ms = 50, .connect_timeout_cap_ms = std::nullopt},
        .boot_ms_fn = [fake_boot]
        {
            return fake_boot->load();
        },
        .wait_sleep_fn = [fake_boot, waits](uint64_t ms)
        {
            *fake_boot += ms;
            waits->push(ms);
        },
    }));
    ASSERT_TRUE(store);
    EXPECT_TRUE(waits->snapshot().empty()) << "no observation window under the unsafe setting";
    /// `Pool` has no test accessor for the adopted `MountPriorState`, so the `UncleanUnsafe`
    /// classification is asserted through the mount audit event instead: `claimMount`'s unsafe-reclaim
    /// branch (`CasServerRoot.cpp`) emits exactly one `MountClaim`/"reclaim" event whose reason names
    /// the setting, and `CASMountClaim.UnsafeAuthorizationIsTokenExact` already pins the classification
    /// itself at the `claimMount` level.
    const std::vector<CasEvent> observed_events = events->snapshot();
    const auto reclaim_event = std::ranges::find_if(observed_events,
        [](const CasEvent & e) { return e.reason.find("cas_unsafe_remount_no_delay") != String::npos; });
    ASSERT_NE(reclaim_event, observed_events.end());
    EXPECT_EQ(reclaim_event->type, CasEventType::MountClaim);
    EXPECT_EQ(reclaim_event->outcome, "reclaim");
    EXPECT_EQ(decodeMountLease((*DB::Cas::tests::OperationForTest(b)).read(l.mountKey("test"), Retry::standard())->bytes).writer_epoch, 8u);
}

TEST(CASMountOpenWaits, CleanOpenSkipsAllWaits)
{
    auto b = std::make_shared<InMemoryBackend>();
    /// Predecessor released cleanly (drain + farewell from Task 5): open, then reset() drives ~Pool(),
    /// which -- with nothing in flight -- writes the farewell marker (min_active_build_sequence == UINT64_MAX).
    auto predecessor = Pool::open(b, PoolConfig{
        .pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "test"});
    predecessor.reset();

    /// Heap-owned, not a plain local: the hook below mutates it, and the Pool can outlive this stack
    /// frame (a background publish holds `shared_from_this()`), so a by-reference capture would dangle.
    auto waits = std::make_shared<SharedWaitLog>();
    PoolPtr successor;
    ASSERT_NO_THROW(
        successor = Pool::open(b, PoolConfig{
            .pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "test",
            .wait_sleep_fn = [waits](uint64_t ms)
            {
                waits->push(ms);
            },
        }));
    ASSERT_TRUE(successor);

    EXPECT_TRUE(waits->snapshot().empty())
        << "a clean farewell (Task 5) needs no observation window";
}

namespace
{
/// Reports the SHIPPED PRODUCTION default envelope (`CasRequestBudget{}`'s own defaults --
/// `attempt_timeout_ms=5000`, `connect_timeout_cap_ms=1000` -> `attemptEnvelopeMs()=7000`), so the
/// teardown below pays the SAME two-envelope reservation (14000 ms) production pays, not the
/// near-zero envelope a bare `InMemoryBackend` reports by default.
struct DefaultBudgetEnvelopeBackend : InMemoryBackend
{
    uint64_t attemptTimeoutMs() const override { return 5000; }
    uint64_t attemptEnvelopeMs() const override { return 7000; }
};
}

/// `CleanOpenSkipsAllWaits` above proves a clean farewell skips the observation window, but its bare
/// `InMemoryBackend` reports a zero attempt envelope, so its teardown never exercises the farewell's
/// own policy window against a write's real cost. Pin the shipped default budget specifically: a
/// window that cannot admit the write's `2 * attemptEnvelopeMs()` reservation refuses the farewell
/// before its first attempt, and the successor below then pays a full incarnation-stability
/// observation instead of reclaiming instantly.
TEST(CASMountOpenWaits, CleanTeardownUnderDefaultBudgetLeavesAFarewell)
{
    auto b = std::make_shared<DefaultBudgetEnvelopeBackend>();
    auto predecessor = Pool::open(b, PoolConfig{
        .pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "test"});
    predecessor.reset();   /// drives ~Pool(): with nothing in flight, this is the graceful-shutdown farewell.

    const Layout layout{"p"};
    const auto got = readObj(*b, layout.mountKey("test"));
    ASSERT_TRUE(got.has_value());
    const MountLease lease = decodeMountLease(got->bytes);
    EXPECT_EQ(lease.min_active_build_sequence, std::numeric_limits<uint64_t>::max())
        << "the farewell's policy window must admit the write's own two-envelope reservation at the "
           "shipped default budget (2 * 7000 ms) -- otherwise a clean teardown never hands the mount "
           "slot back";

    /// Heap-owned, not a plain local: the hook below mutates it, and the Pool can outlive this stack
    /// frame (a background publish holds `shared_from_this()`), so a by-reference capture would dangle.
    auto waits = std::make_shared<SharedWaitLog>();
    PoolPtr successor;
    ASSERT_NO_THROW(
        successor = Pool::open(b, PoolConfig{
            .pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "test",
            .wait_sleep_fn = [waits](uint64_t ms)
            {
                waits->push(ms);
            },
        }));
    ASSERT_TRUE(successor);
    EXPECT_TRUE(waits->snapshot().empty())
        << "a clean farewell needs no observation window on reopen, even at the shipped default budget";
}

TEST(CASMountOpenWaits, FencedPriorReclaimsWithoutAnyWait)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l{"p"};
    DB::Cas::tests::seedPoolMetaForRestart(*b);
    ASSERT_EQ(claimMount(*DB::Cas::tests::OperationForTest(b), l, "test", UInt128(1), /*epoch*/ 7, /*now_ms*/ 1000, /*ttl_ms*/ 500).kind,
              MountClaimResult::Claimed);
    /// A real predecessor at epoch 7 durably minted it first (`allocateWriterEpoch` always runs
    /// before the mount claim); seed that durable epoch object here too, or the successor's own
    /// `allocateWriterEpoch` trips the Phase C guard (epoch absent, mount present -> fail closed).
    createObj(*b, l.epochKey("test"), encodeServerEpoch(ServerEpoch{.next_writer_epoch = 8}));
    /// Predecessor lease carries gc_fenced=true: fence it directly, exactly as `computeHeartbeatFloor`'s
    /// fence-out does (preserve the body, gc_fenced = true, seq + 1, token-guarded).
    fenceOutMount(*b, l.mountKey("test"));

    /// See UncleanOpenPaysOnlyTheObservationWindow above: a 500ms TTL needs a scaled-down budget too.
    const CasRequestBudget tiny_budget{
        .attempt_timeout_ms = 50, .lease_safety_margin_ms = 50, .connect_timeout_cap_ms = std::nullopt};

    /// Heap-owned, not a plain local: the hook below mutates it, and the Pool can outlive this stack
    /// frame (a background publish holds `shared_from_this()`), so a by-reference capture would dangle.
    auto waits = std::make_shared<SharedWaitLog>();
    PoolPtr store;
    ASSERT_NO_THROW(
        store = Pool::open(b, PoolConfig{
            .pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "test",
            .mount_lease_ttl_ms = std::chrono::milliseconds(500),
            .cas_request_budget = tiny_budget,
            .wait_sleep_fn = [waits](uint64_t ms)
            {
                waits->push(ms);
            },
        }));
    ASSERT_TRUE(store);

    /// A GC-fenced prior is a terminal, already-threshold-gated certificate of death -- reclaimed on the
    /// FIRST attempt, with no observation polling. It is also an UNCLEAN prior, which used to mean it
    /// paid the materialization grace; nothing is owed now, so this open blocks on nothing at all.
    EXPECT_TRUE(waits->snapshot().empty())
        << "a certified-dead predecessor needs neither the observation window nor any grace period";
}

/// The open-time publication horizon must reserve TWO attempt envelopes (connect cap included), not
/// two bare attempt timeouts -- a slow connect could otherwise overrun the reservation the horizon
/// check was guarding. `background_watermark` defaults false (not set below), so `CasPool.cpp`'s
/// `renewal_window_ms` ternary takes its no-period branch: `2 * attemptEnvelopeMs()`. The check is also
/// STRICT (refuses equality), matching `CasMountRuntime::admit`.
TEST(CASMountOpenWaits, PublicationHorizonUsesTheEnvelope)
{
    /// Opens with a boot clock costing `per_call_ms` per read (models a faster or slower claim) and
    /// returns how many times the mount key was written. attempt 100, cap 100: envelope =
    /// 100 + 2*100 = 300, so 2*envelope = 600; the old code reserved 2*attempt = 200. Empirically the
    /// claim path's own anchor read and the horizon check's own `now_boot_ms` read are five reads apart,
    /// so `remaining = safe_deadline(TTL 1000 - margin 50 = 950) - now = 950 - 5 * per_call_ms`.
    const auto mountWriteCount = [](uint64_t per_call_ms) -> uint64_t
    {
        auto b = std::make_shared<DB::Cas::tests::CountingBackend>();
        Layout l{"p"};
        DB::Cas::tests::seedPoolMetaForRestart(*b);
        /// Held in a shared atomic, not a plain local: the hooks below mutate it, and the Pool can
        /// outlive this lambda's own stack frame (a background publish holds `shared_from_this()`), so
        /// a by-reference capture of a local would dangle.
        auto fake_boot = std::make_shared<std::atomic<uint64_t>>(0);
        PoolPtr store;
        store = Pool::open(b, PoolConfig{
            .pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "test",
            .mount_lease_ttl_ms = std::chrono::milliseconds(1000),
            .cas_request_budget = CasRequestBudget{.attempt_timeout_ms = 100, .lease_safety_margin_ms = 50, .connect_timeout_cap_ms = 100},
            .boot_ms_fn = [fake_boot, per_call_ms]
            {
                return fake_boot->fetch_add(per_call_ms);
            },
            .wait_sleep_fn = [fake_boot](uint64_t ms)
            {
                *fake_boot += ms;
            },
        });
        if (!store)
            return 0;
        return b->putOverwriteCount(l.mountKey("test")) + b->putCount(l.mountKey("test"));
    };

    /// remaining = 945 (per_call_ms=1): both 2*attempt(200) and 2*envelope(600) fit -- two writes (the
    /// claim's own reclaim, then the renewer's adopt) and no re-anchor.
    EXPECT_EQ(mountWriteCount(1), 2u) << "a horizon that fits both windows must not re-anchor";
    /// remaining = 450 (per_call_ms=100): 2*attempt(200) fits, 2*envelope(600) does not -- the
    /// re-anchor costs one extra write. This is the discriminator: reverting the reservation to
    /// 2*attempt would make this case behave like the one above (two writes).
    EXPECT_EQ(mountWriteCount(100), 3u) << "the old 2*attempt window fit here; only the envelope window must redo";
    /// remaining = 600 (per_call_ms=70) exactly equals 2*envelope: STRICT ("<", not "<=") refuses
    /// equality too, so this must also redo -- reverting the strict comparison to "<=" would make this
    /// case behave like the fits-both case (two writes).
    EXPECT_EQ(mountWriteCount(70), 3u) << "an exact boundary (renewal_window_ms == remaining) must be refused, not accepted";
}

/// Same reservation change as `PublicationHorizonUsesTheEnvelope` above, exercised through the remount
/// path's own `renewer_redo` step (`CasPool.cpp` ~1503). Modelled directly on
/// `CASPoolRemount.TheRenewerRedoRenewsOnTheOpenPlane` above: the step's admission refuses a driver that
/// was never parked by a persistent renewal worker, so `background_watermark` must be true and the
/// remount must be driven through `scheduleRemountForTest` (which parks the worker before running it),
/// never through a bare `tryRemountOnce` with no workers -- the direct-driven attempt deadlocks in
/// exactly the way that test's own comment describes.
TEST(CASPoolRemount, RemountRenewerRedoUsesTheEnvelope)
{
    /// One successful self-remount whose quiescence costs `quiesce_ms`; returns the conditional
    /// mount-slot writes it issued, counted while the remount worker is still held inside the event
    /// sink that reported the result (so the renewal worker it un-parks cannot add one).
    const auto remountConditionalMountWrites = [](uint64_t quiesce_ms) -> uint64_t
    {
        auto backend = std::make_shared<DB::Cas::tests::CountingBackend>();
        /// Held in a shared atomic, not a plain local: the hooks below mutate it, and the Pool can
        /// outlive this lambda's own stack frame (a background publish holds `shared_from_this()`), so
        /// a by-reference capture of a local would dangle.
        auto fake_boot = std::make_shared<std::atomic<uint64_t>>(1'000'000);
        /// Heap-owned, not a plain local: declaration order relative to `store` only protects against an
        /// ordinary same-thread unwind, not a detached background completion that holds an extra
        /// `shared_from_this()` and can still be running on another thread after this call returns.
        auto committed = std::make_shared<DB::Cas::tests::ManualBarrier>();
        auto store = Pool::open(backend, PoolConfig{
            .pool_prefix = "remount-renewer-redo-envelope",
            .server_root_id = "test",
            .background_watermark = true,
            .event_sink = [committed](const CasEvent & event)
            {
                if (event.type == CasEventType::MountRemount && event.outcome == "ok")
                    committed->arriveAndWait();
            },
            .mount_lease_ttl_ms = std::chrono::milliseconds(1000),
            .mount_renew_period = std::chrono::milliseconds(100),
            .cas_request_budget = CasRequestBudget{.attempt_timeout_ms = 100, .lease_safety_margin_ms = 50, .connect_timeout_cap_ms = 100},
            .boot_ms_fn = [fake_boot]
            {
                return fake_boot->load();
            },
            .wait_sleep_fn = [fake_boot](uint64_t ms)
            {
                *fake_boot += ms;
            },
            .remount_quiesce_hook_for_test = [fake_boot, quiesce_ms]
            {
                *fake_boot += quiesce_ms;
            },
        });
        const String mount_key = store->layout().mountKey("test");

        fenceOutMount(*backend, mount_key);
        const uint64_t before = backend->putOverwriteCount(mount_key);
        EXPECT_TRUE(store->scheduleRemountForTest())
            << "the remount must be latched with quiesce_ms=" << quiesce_ms;
        committed->waitUntilArrived();
        const uint64_t writes = backend->putOverwriteCount(mount_key) - before;
        committed->release();
        return writes;
    };

    /// attempt 100, cap 100: envelope = 100 + 2*100 = 300, so period(100) + 2*envelope(600) = 700. A
    /// 450 ms quiescence leaves remaining = TTL(1000) - margin(50) - 450 = 500: the old
    /// period + 2*attempt (300) window fit that, but the new period + 2*envelope (700) window does not
    /// -- so only the envelope-based check must redo (validation: 100 + 600 + 50 = 750 < 1000).
    const uint64_t control = remountConditionalMountWrites(0);
    EXPECT_GT(remountConditionalMountWrites(450), control)
        << "a quiescence that fits the old attempt-only window but not the envelope window must still cost the redo";
    /// A 250 ms quiescence leaves remaining = 950 - 250 = 700, exactly equal to
    /// period + 2*envelope (700): STRICT ("<", not "<=") refuses equality too, so this must also
    /// redo -- reverting the strict comparison to "<=" would make this case behave like the control.
    EXPECT_GT(remountConditionalMountWrites(250), control)
        << "an exact boundary (renewal_window_ms == remaining) must be refused, not accepted";
}

namespace
{
/// Stalls the CLAIM ITSELF past the lease TTL, and counts what the open writes afterwards.
///
/// The mount key is written twice before the write fence arms: once by `claimMount`'s reclaim, then
/// once by the renewer's adopt -- and the fence's anchor is taken BETWEEN them. So advancing the
/// injected boot clock on the SECOND write models exactly the thing the Phase B redo exists for: the
/// claim's own I/O outliving the lease it is about to arm a fence under. (This used to be modelled by
/// a materialization grace long enough to consume the TTL; that wait is retired, and the guard it
/// motivated is not -- a stalled socket can still outlive a validated request budget.)
class StalledMountClaimBackend final : public DB::Cas::InMemoryBackend
{
public:
    String mount_key;
    std::function<void()> on_second_mount_write;
    std::atomic<int> mount_writes{0};
    std::atomic<int> mount_writes_after_stall{0};

    /// The hook sits on the WRITE PRIMITIVE: both the reclaim and the renewer's adopt reach the mount
    /// slot through it. It counts only CONDITIONAL overwrites, which is what every production mount-slot
    /// write is -- the unconditional create that seeds the predecessor lease reaches this same virtual
    /// too, and counting it would shift the stall onto the reclaim instead of the adopt.
    std::expected<String, RawConflict> write(const String & key, const String & bytes,
        const std::optional<String> & expected_value, DB::Cas::TransportAccess & access) override
    {
        if (key == mount_key && expected_value)
        {
            const int n = ++mount_writes;
            if (n == 2 && on_second_mount_write)
                on_second_mount_write();
            else if (n > 2)
                ++mount_writes_after_stall;
        }
        return InMemoryBackend::write(key, bytes, expected_value, access);
    }
};
}

/// Phase B startup-arm (spec rev.4, codex round-3 finding 2): a claim path that consumed the lease TTL
/// must force ONE fresh conditional lease write before arming — the fence must never arm from an anchor
/// that has already expired (a successor could have legally reclaimed meanwhile).
TEST(CASPool, StartupArmRedoesLeaseWriteWhenTheClaimConsumesTtl)
{
    auto backend = std::make_shared<StalledMountClaimBackend>();
    DB::Cas::Layout layout("pool");
    DB::Cas::tests::seedPoolMetaForRestart(*backend, "pool");
    const String srid = "s";
    const DB::UInt128 uuid(0x42);
    backend->mount_key = layout.mountKey(srid);

    /// Seed a FENCED, expired predecessor body under a DIFFERENT epoch (7, matching
    /// `FencedPriorPaysOnlyTmat`'s convention). The durable epoch object seeded a few lines below
    /// carries `next_writer_epoch = 8`, so THIS pool's own first-allocated `writer_epoch` is 8 --
    /// non-colliding with the seeded epoch-7 prior by construction. With no collision the first
    /// (and only) claim attempt reclaims directly with MountPriorState::Fenced, with no silent
    /// FencedSelf fence-recovery detour to account for -- so the mount key is written exactly twice
    /// before the arm, which is what the stall hook counts on.
    {
        DB::Cas::MountLease prior;
        prior.server_uuid = uuid;
        prior.writer_epoch = 7;
        prior.seq = 7;
        prior.expires_at_ms = 1;      /// long expired
        prior.gc_fenced = true;
        prior.write_attempt_id = DB::UInt128{7};
        createObj(*backend, layout.mountKey(srid), DB::Cas::encodeMountLease(prior));
    }
    /// A real predecessor at epoch 7 durably minted it first (`allocateWriterEpoch` always runs
    /// before the mount claim); seed that durable epoch object here too, or `Pool::open`'s own
    /// `allocateWriterEpoch` trips the Phase C guard (epoch absent, mount present -> fail closed).
    createObj(*backend, layout.epochKey(srid), DB::Cas::encodeServerEpoch(DB::Cas::ServerEpoch{.next_writer_epoch = 8}));
    /// Held in a shared atomic, not a plain local: `on_second_mount_write` below mutates it, and the
    /// Pool can outlive this stack frame (a background publish holds `shared_from_this()`), so a
    /// by-reference capture of a local would dangle.
    auto fake_boot_ms = std::make_shared<std::atomic<uint64_t>>(10'000);
    DB::Cas::PoolConfig cfg;
    cfg.pool_prefix = "pool";
    cfg.server_id = uuid;
    cfg.server_root_id = srid;
    cfg.background_watermark = true;
    cfg.mount_lease_ttl_ms = std::chrono::milliseconds(30'000);
    cfg.boot_ms_fn = [fake_boot_ms]
    {
        return fake_boot_ms->load();
    };
    /// The renewer's adopt write stalls for 15 s of boot clock. That consumes the publication horizon
    /// (one 10 s cadence plus one 5 s attempt) while leaving one physical attempt admissible inside
    /// the old lease's safety window, so the synchronous redo can safely re-anchor.
    backend->on_second_mount_write = [fake_boot_ms]
    {
        *fake_boot_ms += 15'000;
    };

    auto store = DB::Cas::Pool::open(backend, cfg);
    ASSERT_NE(store, nullptr);

    ASSERT_EQ(backend->mount_writes.load(), 3)
        << "the fixture assumes exactly two mount writes before the redo (the reclaim and the renewer's "
           "adopt, with the fence anchor between them); a different sequence would make the stall land "
           "somewhere else and this test would stop testing the redo";
    EXPECT_EQ(backend->mount_writes_after_stall.load(), 1)
        << "a TTL-consuming claim must be followed by exactly ONE fresh conditional lease write "
           "(the re-anchoring redo) before the write fence arms";
}

/// ==== What a self-remount may block on ====
///
/// Nothing an operator configures. The remount used to consult `refLanesSettledForRemount` and pay the
/// materialization grace whenever a ref lane still held an undecided `PUT`; both are retired, because
/// the undecided `PUT` is settled by the protocol rather than waited out — recovery closes the dead
/// epoch with an in-band `EpochSeal` written as a conditional create, and the straggler's own create
/// loses to it. `gtest_cas_retirement_sweep.cpp` proves that conflict directly; these two pin that the
/// wait is gone from both the drained and the still-wedged path.

TEST(CASRemountWaits, DrainedRemountPaysNoWait)
{
    auto backend = std::make_shared<InMemoryBackend>();
    /// Held in shared, heap-owned state, not plain locals: the hooks below mutate them, and the Pool
    /// can outlive this stack frame (a background publish holds `shared_from_this()`), so a
    /// by-reference capture of a local would dangle.
    auto fake_boot = std::make_shared<std::atomic<uint64_t>>(1'000'000);
    auto waits = std::make_shared<SharedWaitLog>();
    auto store = Pool::open(backend, PoolConfig{
        .pool_prefix = "p", .server_root_id = "test",
        .mount_lease_ttl_ms = std::chrono::milliseconds(30000),
        .boot_ms_fn = [fake_boot]
        {
            return fake_boot->load();
        },
        .wait_sleep_fn = [fake_boot, waits](uint64_t ms)
        {
            *fake_boot += ms;
            waits->push(ms);
        },
    });
    ASSERT_TRUE(store);
    EXPECT_TRUE(waits->snapshot().empty()) << "a fresh mount (no predecessor) pays no wait at open";
    store->setCasRetrySleepForTest([fake_boot](uint64_t ms)
    {
        *fake_boot += ms;
    });

    /// Trip the fence: advance the local boot clock past the deadline (as in `WriteFenceUsesInjectedBootClock`
    /// above) and mark the durable lease `gc_fenced` (the certificate `claimMountAwaitingExpiry` reclaims
    /// on its FIRST attempt, no observation polling -- avoids a real sleep in this test).
    *fake_boot += 30001;
    fenceOutMount(*backend, store->layout().mountKey("test"));

    /// No in-flight ref-log PUT at all -- the easy direction.
    ASSERT_TRUE(store->tryRemountOnce());

    EXPECT_TRUE(waits->snapshot().empty())
        << "a drained self-remount must pay no wait";
}

TEST(CASRemountWaits, UnresolvedWedgeRemountPaysNoWaitEither)
{
    CasRequestBudget budget;
    budget.attempt_timeout_ms = 100;
    budget.lease_safety_margin_ms = 100;

    auto backend = std::make_shared<UnresolvedPutBackend>();
    /// What the request engine reserves per attempt is the BACKEND's attempt timeout, not the budget
    /// field alone; pair the two so the mount lease's admission arithmetic sees what the budget claims.
    backend->setAttemptTimeoutMs(budget.attempt_timeout_ms);
    /// Held in shared, heap-owned state, not plain locals: the hooks below mutate them, and the Pool
    /// can outlive this stack frame (a background publish holds `shared_from_this()`), so a
    /// by-reference capture of a local would dangle.
    auto fake_boot = std::make_shared<std::atomic<uint64_t>>(1'000'000);
    auto waits = std::make_shared<SharedWaitLog>();
    auto store = Pool::open(backend, PoolConfig{
        .pool_prefix = "p", .server_root_id = "test",
        .mount_lease_ttl_ms = std::chrono::milliseconds(30000),
        .cas_request_budget = budget,
        .boot_ms_fn = [fake_boot]
        {
            return fake_boot->load();
        },
        .wait_sleep_fn = [fake_boot, waits](uint64_t ms)
        {
            *fake_boot += ms;
            waits->push(ms);
        },
    });
    ASSERT_TRUE(store);
    EXPECT_TRUE(waits->snapshot().empty()) << "a fresh mount (no predecessor) pays no wait at open";
    /// `dropRef` below drives the fault through `ensureRefTableRecovered`'s own recovery-retry loop,
    /// which sleeps via `recovery_retry_sleep_fn` (a REAL 200ms-slice sleep by default) while measuring
    /// elapsed time against `boot_ms_now_fn` -- the frozen `fake_boot` this fixture already injects.
    /// Without also virtualizing the sleep, that elapsed check never advances and the loop spins for
    /// real until the harness times the test out.
    store->setCasRetrySleepForTest([fake_boot](uint64_t ms)
    {
        *fake_boot += ms;
    });

    const Layout & layout = store->layout();
    const RootNamespace ns{"srv/remount_wedge"};
    /// Stage B (Task 4-C): see `CASPoolShutdown.UnresolvedWedgeSkipsFarewell`'s identical comment.
    DB::Cas::tests::casAdmitRecoverableEntry(*backend, layout, ns, store->liveWriterEpoch());
    publishPart(store, ns.string(), "x", "payload");

    /// Force the ref-log append `dropRef` below performs into the Unresolved/wedge outcome (as in
    /// `CASPoolShutdown.UnresolvedWedgeSkipsFarewell`): the single attempt the budget allows fails
    /// ambiguously.
    backend->fault_key_substr = layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_log/";
    backend->fault_count = std::numeric_limits<int>::max();
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropRef(ns, "x"); });
    ASSERT_TRUE(store->refLaneWedgedForTest(ns));

    /// Trip the fence exactly as in `DrainedRemountSkipsGrace` above.
    *fake_boot += 30001;
    fenceOutMount(*backend, store->layout().mountKey("test"));

    /// THE HARD DIRECTION, and the one the retired wait existed for: a ref lane that still holds an
    /// UNDECIDED conditional PUT when the fence trips. It used to buy a 30 s grace. It buys nothing now
    /// -- the remount proceeds straight through, and the undecided PUT is decided by the seal the next
    /// recovery writes into its slot.
    ASSERT_TRUE(store->tryRemountOnce());

    EXPECT_TRUE(waits->snapshot().empty())
        << "an unresolved ref-lane wedge must not make the remount block: the straggler it describes is "
           "fenced by the recovery seal, not waited out";
}

/// Sealing is decided by ARITHMETIC -- `epoch < live_epoch` -- and by nothing else. This test used to
/// pin the opposite ("a table recovered under a later CLEAN boundary must not seal"), which was the
/// right rule while a seal was a synthetic SNAPSHOT published only to close an unclean handover: such a
/// seal after a clean shutdown was pure parasitic cost, so it was gated on the per-epoch unclean flag.
///
/// INV-2's seal is not that object. It is the chain link that makes a MISSING epoch detectable across a
/// transition, and a chain that skips every epoch whose mount happened to shut down cleanly is not a
/// chain -- the next sequence-1 transaction would have no `prev_epoch_seal` to name, and no reader could
/// tell "epoch 2 was empty" from "epoch 2's records are gone". So a late-touched table now closes EVERY
/// dead epoch below the live one, however its predecessors died, and this test pins that plus the two
/// things that must still be true: the seals land IN-BAND (at log keys, at the slot a straggler would
/// have taken) and no synthetic seal SNAPSHOT is written anywhere.
TEST(CASRemountWaits, ALateTouchedTableClosesEveryDeadEpochInBandHoweverItsPredecessorsDied)
{
    CasRequestBudget budget;
    budget.attempt_timeout_ms = 100;
    budget.lease_safety_margin_ms = 100;

    auto backend = std::make_shared<UnresolvedPutBackend>();
    /// What the request engine reserves per attempt is the BACKEND's attempt timeout, not the budget
    /// field alone; pair the two so the mount lease's admission arithmetic sees what the budget claims.
    backend->setAttemptTimeoutMs(budget.attempt_timeout_ms);
    /// Held in a shared atomic, not a plain local: the hooks below mutate it, and the Pool can outlive
    /// this stack frame (a background publish holds `shared_from_this()`), so a by-reference capture
    /// of a local would dangle.
    auto fake_boot = std::make_shared<std::atomic<uint64_t>>(1'000'000);
    auto store = Pool::open(backend, PoolConfig{
        .pool_prefix = "p", .server_root_id = "test",
        .mount_lease_ttl_ms = std::chrono::milliseconds(30000),
        .cas_request_budget = budget,
        .boot_ms_fn = [fake_boot]
        {
            return fake_boot->load();
        },
        .wait_sleep_fn = [fake_boot](uint64_t ms)
        {
            *fake_boot += ms;
        },
    });
    ASSERT_TRUE(store);
    store->setCasRetrySleepForTest([fake_boot](uint64_t ms)
    {
        *fake_boot += ms;
    });

    const Layout & layout = store->layout();
    const RootNamespace ns1{"srv/table_a"};
    const RootNamespace ns2{"srv/table_b"};
    /// Stage B (Task 4-C): `ns1` is pinned because the fault below targets its key by exact sentinel
    /// match. `ns2` must ALSO be pinned: the epoch-close assertions further down read its ref-log keys
    /// directly at `DB::Cas::tests::fixture::fixtureLife(ns2)`.
    DB::Cas::tests::casAdmitRecoverableEntry(*backend, layout, ns1, store->liveWriterEpoch());
    DB::Cas::tests::casAdmitRecoverableEntry(*backend, layout, ns2, store->liveWriterEpoch());
    publishPart(store, ns1.string(), "x", "payload-a");
    /// ns2's epoch-1 data: never touched again by this incarnation until the final check below, well
    /// after both remounts -- the "table recovered for the first time, late" the fix must not over-seal.
    /// Distinct content from ns1's part: identical payloads collide on the same blob and race
    /// `PartWriteTxn::ensureBlobPresent`'s mandatory observation, unrelated to what this test is about.
    publishPart(store, ns2.string(), "y", "payload-b");

    /// Force ns1's ref-log append into the Unresolved/wedge outcome (mirrors
    /// `UnresolvedWedgeRemountPaysNoWaitEither` above).
    backend->fault_key_substr = layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns1)) + "_log/";
    backend->fault_count = std::numeric_limits<int>::max();
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropRef(ns1, "x"); });
    ASSERT_TRUE(store->refLaneWedgedForTest(ns1));

    /// Self-remount #1: UNCLEAN (the wedge above). Epoch 1 -> 2.
    *fake_boot += 30001;
    fenceOutMount(*backend, store->layout().mountKey("test"));
    ASSERT_TRUE(store->tryRemountOnce());
    ASSERT_EQ(store->liveWriterEpoch(), 2u);

    /// Self-remount #2: CLEAN (no wedge left behind -- `quiesceRefTablesForRemount` already cleared the
    /// cache). Epoch 2 -> 3.
    *fake_boot += 30001;
    fenceOutMount(*backend, store->layout().mountKey("test"));
    ASSERT_TRUE(store->tryRemountOnce());
    ASSERT_EQ(store->liveWriterEpoch(), 3u);

    using ProfileEvents::global_counters;
    const auto sealed_before = global_counters[ProfileEvents::CASRefRecoveryEpochSealed].load();

    /// ns2's FIRST recovery under this incarnation happens now, at epoch 3 -- strictly after both
    /// remounts. Its only data is at epoch 1, so epochs 1 and 2 are both dead for it.
    EXPECT_EQ(store->listRefs(ns2).size(), 1u);

    EXPECT_EQ(global_counters[ProfileEvents::CASRefRecoveryEpochSealed].load(), sealed_before + 2)
        << "both dead epochs must be closed -- the chain link is what a later reader needs to tell an "
           "EMPTY epoch from a LOST one, and that is independent of how each mount ended";
    EXPECT_TRUE(readObj(*backend, layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(ns2), RefTxnId{1, 2})).has_value())
        << "epoch 1 closes at the slot right after its last durable id, in-band";
    EXPECT_TRUE(readObj(*backend, layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(ns2), RefTxnId{2, 1})).has_value())
        << "empty epoch 2 closes at its own sequence 1, chained to the epoch-1 seal";
    const RefTxnId retired_sentinel_id{2, std::numeric_limits<uint64_t>::max()};
    EXPECT_FALSE(readObj(*backend, layout.refSnapshotKey(DB::Cas::tests::fixture::fixtureLife(ns2), retired_sentinel_id)).has_value())
        << "and NO synthetic seal snapshot is written: that shape is retired";
}

TEST(CASPool, ReadManifestSharedReturnsSharedDecodeWithoutCopy)
{
    auto backend = std::make_shared<DB::Cas::tests::CountingBackend>();
    const DB::Cas::Layout layout("p");
    DB::Cas::tests::seedPoolMetaForRestart(*backend);
    const DB::Cas::RootNamespace ns{"srv/t1"};
    const DB::Cas::ManifestRef ref{.writer_epoch = 1, .build_sequence = 1, .manifest_ordinal = 1};
    const auto id = DB::Cas::tests::writeManifestRaw(*backend, layout, ns, ref,
        {DB::Cas::tests::blobEntryFor("data.bin", DB::UInt128(7))});
    DB::Cas::tests::fixture::writeRefLogRaw(*backend, layout, RefLogTxn{ns.string(), RefTxnId{1, 1},
        {DB::Cas::tests::namespaceBirthOp(), DB::Cas::tests::publishCommittedOps("part_1", ref)[0],
         DB::Cas::tests::publishCommittedOps("part_1", ref)[1]}, std::nullopt});
    DB::Cas::tests::writeRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 1},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    auto store = DB::Cas::Pool::open(backend,
        DB::Cas::PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    const auto resolved = store->resolveRef(ns, "part_1");
    ASSERT_TRUE(resolved.has_value());

    const String manifest_key = layout.manifestKey(id);
    backend->resetCounts();

    auto m1 = store->readManifestShared(resolved->manifest_id);
    auto m2 = store->readManifestShared(resolved->manifest_id);
    EXPECT_EQ(m1.get(), m2.get());                          /// the SAME shared decode, no copy
    EXPECT_EQ(backend->getCount(manifest_key), 1u);         /// one body GET
    EXPECT_EQ(backend->headCount(manifest_key), 0u);        /// keyed by id: no HEAD on a miss or a hit
    ASSERT_EQ(m1->entries.size(), 1u);
    EXPECT_EQ(m1->entries[0].path, "data.bin");
}

/// A miss whose object is absent is the one dangling-reference case the reader still detects
/// itself: exactly one GET, no HEAD, one `ReadMissing` event, FILE_DOESNT_EXIST.
TEST(CASPool, ReadManifestAbsentBodyEmitsReadMissingWithOneGetAndNoHead)
{
    auto b = std::make_shared<DB::Cas::tests::CountingBackend>();
    auto s = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    Layout layout("p");
    const RootNamespace ns{"srv1/tbl"};
    const ManifestId id{.root_namespace = ns, .ref = manifestRefFor("absent-body-event")};
    const String key = layout.manifestKey(id);

    /// Heap-owned, not a plain local: `setEventSink(nullptr)` below only stops FUTURE sink installs from
    /// using this closure -- it does not guarantee an already-in-flight background call is not still
    /// executing the old one -- and the Pool can outlive this stack frame regardless (a background
    /// publish holds `shared_from_this()`).
    auto events = std::make_shared<DB::Cas::tests::SharedEventLog>();
    s->setEventSink([events](CasEvent e)
    {
        events->push(std::move(e));
    });
    b->resetCounts();
    expectThrowsCode(DB::ErrorCodes::FILE_DOESNT_EXIST, [&] { s->readManifest(id); });
    s->setEventSink(nullptr);

    EXPECT_EQ(b->getCount(key), 1u);
    EXPECT_EQ(b->headCount(key), 0u);
    size_t read_missing = 0;
    for (const auto & e : events->snapshot())
    {
        if (e.type != CasEventType::ReadMissing)
            continue;
        ++read_missing;
        EXPECT_EQ(e.object_kind, CasEventObjectKind::Manifest);
        EXPECT_EQ(e.detail.at("code"), "FILE_DOESNT_EXIST");
        EXPECT_EQ(e.detail.at("site"), "readManifest");
    }
    EXPECT_EQ(read_missing, 1u);
}

/// A reader holding a decode for a manifest the collector has since removed sees a snapshot-consistent
/// manifest: the second read is the same shared decode with no request, `locate` is pure, and the
/// missing blob is observed only when its key is read. Nothing here is a fallback: the absence is
/// surfaced by the blob read, never masked by the cache.
TEST(CASPool, StaleSnapshotServesCachedManifestAndBlobAbsenceSurfacesOnRead)
{
    auto b = std::make_shared<DB::Cas::tests::CountingBackend>();
    auto s = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    Layout layout("p");
    const RootNamespace ns{"srv1/tbl"};

    const ManifestId id = publishPart(s, ns.string(), "part_1", "payload-1");
    auto r = s->resolveRef(ns, "part_1");
    ASSERT_TRUE(r.has_value());
    auto m1 = s->readManifestShared(r->manifest_id);
    const String manifest_key = layout.manifestKey(id);
    const String blob_key = layout.blobKey(idOf("payload-1"));

    /// What GC does after the owner is removed and the decrement is adopted: exact-token deletes of
    /// the body and of the now-unreferenced blob.
    {
        DB::Cas::tests::OperationForTest op(*b);
        const auto h = (*op).head(manifest_key, Retry::standard());
        ASSERT_TRUE(h.has_value());
        (*op).remove(manifest_key, h->etag, Retry::once());
    }
    {
        DB::Cas::tests::OperationForTest op(*b);
        const auto h = (*op).head(blob_key, Retry::standard());
        ASSERT_TRUE(h.has_value());
        (*op).remove(blob_key, h->etag, Retry::once());
    }
    b->resetCounts();

    auto m2 = s->readManifestShared(r->manifest_id);
    EXPECT_EQ(m1.get(), m2.get());
    EXPECT_EQ(b->getCount(manifest_key), 0u);
    EXPECT_EQ(b->headCount(manifest_key), 0u);

    ASSERT_EQ(m2->entries.size(), 1u);
    const BlobLocation location = s->locate(m2->entries[0]);
    EXPECT_EQ(location.key, blob_key);
    EXPECT_EQ(b->getCount(blob_key), 0u);          /// locate is pure: no I/O until the read
    EXPECT_FALSE(readObj(*b, location.key).has_value());   /// the read observes the absence
}

/// The scoped contract for mutation evidence, executable. A carry-forward from a committed source
/// (what createHardLink, republishRef, repointRef and the relink receiver do) adopts each entry as a
/// tokenless TrustedManifest dependency and promote issues no probe for it: the live source edge is
/// what keeps the blob alive, and under protocol-compliant GC the state "cached source decode, blob
/// gone" cannot be constructed. Out-of-band deletion of BOTH the cached source body and the blob is
/// outside that contract; the carry-forward then commits a ref to an absent blob and fsck's
/// reachable-but-absent scan is the detector. This test pins that documented outcome so a later
/// change that silently alters it is noticed. It is not a defect report.
TEST(CASPool, CachedSourceDecodeLetsAdoptionCommitAnAbsentBlobThatFsckReports)
{
    auto b = std::make_shared<DB::Cas::tests::CountingBackend>();
    auto s = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    Layout layout("p");
    const RootNamespace ns{"srv1/tbl"};

    const ManifestId src_id = publishPart(s, ns.string(), "part_src", "payload-src");
    auto src = s->resolveRef(ns, "part_src");
    ASSERT_TRUE(src.has_value());
    const auto src_manifest = s->readManifestShared(src->manifest_id);   /// warms the decode cache
    const String src_manifest_key = layout.manifestKey(src_id);
    const String blob_key = layout.blobKey(idOf("payload-src"));

    /// Out of band: both objects gone, the committed source ref untouched.
    for (const String & key : {src_manifest_key, blob_key})
    {
        DB::Cas::tests::OperationForTest op(*b);
        const auto h = (*op).head(key, Retry::standard());
        ASSERT_TRUE(h.has_value());
        (*op).remove(key, h->etag, Retry::once());
    }
    b->resetCounts();

    /// The carry-forward reaches its source through the reader, the way every production caller does,
    /// and the cache answers: the same decode as before, with no request on the body just deleted.
    /// Adopting from the `shared_ptr` held across the deletion would prove nothing about the cache --
    /// were the cache to stop retaining, this re-read would fetch and throw, and the rest of this
    /// scenario would be unreachable in production for the same reason.
    const auto cached_manifest = s->readManifestShared(src->manifest_id);
    ASSERT_EQ(cached_manifest.get(), src_manifest.get());
    EXPECT_EQ(b->getCount(src_manifest_key), 0u);
    EXPECT_EQ(b->headCount(src_manifest_key), 0u);

    /// The carry-forward, in the order prepareEntries runs it for a committed source: adopt, stage,
    /// precommit, promote. No blob body is written.
    PartWriteInfo info;
    info.intended_ref = ns.string() + "/part_dst";
    info.intended_namespace = ns;
    auto build = s->beginPartWrite(info);
    ASSERT_EQ(src_manifest->entries.size(), 1u);
    build->adoptEvidence(cached_manifest->entries[0]);
    const ManifestId dst_id = build->stageManifest({cached_manifest->entries[0]});
    build->precommitAdd(ns, "part_dst", dst_id);
    EXPECT_NO_THROW(build->promote(ns, "part_dst", build->buildId(), dst_id));
    EXPECT_EQ(b->headCount(blob_key), 0u);   /// a TrustedManifest leaf is not probed, by design
    EXPECT_EQ(b->getCount(blob_key), 0u);

    /// The documented outcome: a committed ref names an absent blob, and fsck reports it.
    ASSERT_TRUE(s->resolveRef(ns, "part_dst").has_value());
    const FsckReport rep = runFsck(*s, /*detail=*/true);
    EXPECT_GE(rep.dangling, 1u);
    bool blob_reported = false;
    for (const FsckObject & o : rep.objects)
        if (o.key == blob_key && o.cls == FsckClass::Dangling)
            blob_reported = true;
    EXPECT_TRUE(blob_reported) << "fsck must report the adopted-but-absent blob " << blob_key;
}

#if defined(DEBUG_OR_SANITIZER_BUILD)
#define EXPECT_RUNTIME_STATE_REJECTION(statement) EXPECT_DEATH({ statement; }, "CAS mount runtime")
#else
#define EXPECT_RUNTIME_STATE_REJECTION(statement) EXPECT_THROW(statement, DB::Exception)
#endif

TEST(CASPoolRemount, DirectRenewCannotRaceWorkerStartOrRenewerReplacement)
{
    auto backend = std::make_shared<RuntimeRenewBackend>();
    const Layout layout("runtime-direct");
    uint64_t wall_ms = 1000;
    uint64_t boot_ms = 100;
    const UInt128 uuid{1};
    ASSERT_EQ(claimMount(*DB::Cas::tests::OperationForTest(backend), layout, "test", uuid, 1, wall_ms, 1000).kind, MountClaimResult::Claimed);
    CasEventSink sink;
    RuntimeUnderTest runtime_holder(
        backend, layout, MountConfig{.mount_lease_ttl_ms = std::chrono::milliseconds(1000),
                                     .boot_ms_fn = [&] { return boot_ms; }},
        "test", sink, runtimeRenewBudget(), [] { return false; });
    CasMountRuntime & runtime = *runtime_holder;
    runtime.installRenewer(uuid, 1, [&] { return wall_ms; });
    const uint64_t anchor = runtime.startRenewer();
    runtime.armMountFence(uuid, 1, anchor + 1000);

    DB::Cas::tests::ManualBarrier barrier;
    backend->barrier = &barrier;
    backend->fault = RuntimeRenewBackend::Fault::BlockThenDelegate;
    auto direct = std::async(std::launch::async, [&] { runtime.renewWatermarkOnce(); });
    barrier.waitUntilArrived();
    EXPECT_RUNTIME_STATE_REJECTION(runtime.startBackgroundWorkers(std::chrono::milliseconds(10)));
    EXPECT_RUNTIME_STATE_REJECTION(runtime.installRenewer(uuid, 2, [&] { return wall_ms; }));
    EXPECT_RUNTIME_STATE_REJECTION(runtime.renewerReset());
    barrier.release();
    EXPECT_NO_THROW(direct.get());
    runtime.finishTeardown(true);
}

TEST(CASPoolRemount, DueWorkerAdmissionIsReservedBeforeParkRequest)
{
    auto backend = std::make_shared<RuntimeRenewBackend>();
    const Layout layout("runtime-admission-park");
    uint64_t wall_ms = 1000;
    uint64_t boot_ms = 100;
    const UInt128 uuid{1};
    ASSERT_EQ(claimMount(*DB::Cas::tests::OperationForTest(backend), layout, "test", uuid, 1, wall_ms, 1000).kind, MountClaimResult::Claimed);
    DB::Cas::tests::ManualBarrier admitted;
    DB::Cas::tests::ManualBarrier remount;
    CasEventSink sink;
    RuntimeUnderTest runtime_holder(
        backend, layout,
        MountConfig{
            .mount_lease_ttl_ms = std::chrono::milliseconds(1000),
            .background_watermark = true,
            .boot_ms_fn = [&] { return boot_ms; },
            .renewal_admitted_hook_for_test = [&] { admitted.arriveAndWait(); }},
        "test", sink, runtimeRenewBudget(), [&]
        {
            remount.arriveAndWait();
            return false;
        });
    CasMountRuntime & runtime = *runtime_holder;
    runtime.installRenewer(uuid, 1, [&] { return wall_ms; });
    const uint64_t anchor = runtime.startRenewer();
    runtime.armMountFence(uuid, 1, anchor + 1000);
    runtime.startBackgroundWorkers(std::chrono::milliseconds(0));
    admitted.waitUntilArrived();
    runtime.tripMountLost();
    runtime.scheduleRemount();
    EXPECT_EQ(runtime.renewalDriverStateForTest(), RenewalDriverState::ParkRequested);
    admitted.release();
    remount.waitUntilArrived();
    EXPECT_EQ(runtime.renewalDriverStateForTest(), RenewalDriverState::Parked);
    remount.release();
    runtime.stopBackgroundWorkers();
    runtime.finishTeardown(false);
}

TEST(CASPoolRemount, DueWorkerAdmissionIsReservedBeforeStop)
{
    auto backend = std::make_shared<RuntimeRenewBackend>();
    const Layout layout("runtime-admission-stop");
    uint64_t wall_ms = 1000;
    uint64_t boot_ms = 100;
    const UInt128 uuid{1};
    ASSERT_EQ(claimMount(*DB::Cas::tests::OperationForTest(backend), layout, "test", uuid, 1, wall_ms, 1000).kind, MountClaimResult::Claimed);
    DB::Cas::tests::ManualBarrier admitted;
    CasEventSink sink;
    RuntimeUnderTest runtime_holder(
        backend, layout,
        MountConfig{
            .mount_lease_ttl_ms = std::chrono::milliseconds(1000),
            .background_watermark = true,
            .boot_ms_fn = [&] { return boot_ms; },
            .renewal_admitted_hook_for_test = [&] { admitted.arriveAndWait(); }},
        "test", sink, runtimeRenewBudget(), [] { return false; });
    CasMountRuntime & runtime = *runtime_holder;
    runtime.installRenewer(uuid, 1, [&] { return wall_ms; });
    const uint64_t anchor = runtime.startRenewer();
    runtime.armMountFence(uuid, 1, anchor + 1000);
    runtime.startBackgroundWorkers(std::chrono::milliseconds(0));
    admitted.waitUntilArrived();
    auto stop = std::async(std::launch::async, [&] { runtime.stopBackgroundWorkers(); });
    runtime.waitForRenewalDriverStateForTest(RenewalDriverState::Stopping);
    admitted.release();
    EXPECT_NO_THROW(stop.get());
    EXPECT_EQ(runtime.renewalDriverStateForTest(), RenewalDriverState::Dormant);
    runtime.finishTeardown(false);
}

TEST(CASPoolRemount, DirectRenewIsRefusedForBackgroundConfiguredRuntimeAfterStop)
{
    auto backend = std::make_shared<RuntimeRenewBackend>();
    const Layout layout("runtime-direct-after-stop");
    uint64_t wall_ms = 1000;
    uint64_t boot_ms = 100;
    const UInt128 uuid{1};
    ASSERT_EQ(claimMount(*DB::Cas::tests::OperationForTest(backend), layout, "test", uuid, 1, wall_ms, 1000).kind, MountClaimResult::Claimed);
    CasEventSink sink;
    RuntimeUnderTest runtime_holder(
        backend, layout,
        MountConfig{.mount_lease_ttl_ms = std::chrono::milliseconds(1000), .background_watermark = true,
                    .boot_ms_fn = [&] { return boot_ms; }},
        "test", sink, runtimeRenewBudget(), [] { return false; });
    CasMountRuntime & runtime = *runtime_holder;
    runtime.installRenewer(uuid, 1, [&] { return wall_ms; });
    const uint64_t anchor = runtime.startRenewer();
    runtime.armMountFence(uuid, 1, anchor + 1000);
    runtime.startBackgroundWorkers(std::chrono::hours(1));
    runtime.stopBackgroundWorkers();
    EXPECT_RUNTIME_STATE_REJECTION(runtime.renewWatermarkOnce());
    runtime.finishTeardown(true);
}

#undef EXPECT_RUNTIME_STATE_REJECTION

TEST(CASPoolRemount, RemountWaitsForRenewalParkedBeforeReplacement)
{
    auto backend = std::make_shared<RuntimeRenewBackend>();
    const Layout layout("runtime-park");
    uint64_t wall_ms = 1000;
    uint64_t boot_ms = 10'000;
    const UInt128 uuid{1};
    ASSERT_EQ(claimMount(*DB::Cas::tests::OperationForTest(backend), layout, "test", uuid, 1, wall_ms, 1000).kind, MountClaimResult::Claimed);
    DB::Cas::tests::ManualBarrier renewal_barrier;
    DB::Cas::tests::ManualBarrier remount_barrier;
    std::atomic<uint64_t> remount_calls{0};
    CasEventSink sink;
    RuntimeUnderTest runtime_holder(
        backend, layout,
        MountConfig{.mount_lease_ttl_ms = std::chrono::milliseconds(1000), .background_watermark = true,
                    .boot_ms_fn = [&] { return boot_ms; }},
        "test", sink, runtimeRenewBudget(), [&]
        {
            ++remount_calls;
            remount_barrier.arriveAndWait();
            return false;
        });
    CasMountRuntime & runtime = *runtime_holder;
    runtime.installRenewer(uuid, 1, [&] { return wall_ms; });
    const uint64_t anchor = runtime.startRenewer();
    runtime.armMountFence(uuid, 1, anchor + 1000);
    backend->barrier = &renewal_barrier;
    backend->fault = RuntimeRenewBackend::Fault::BlockThenDelegate;
    runtime.startBackgroundWorkers(std::chrono::milliseconds(0));
    renewal_barrier.waitUntilArrived();
    runtime.tripMountLost();
    runtime.scheduleRemount();
    runtime.waitForRenewalDriverStateForTest(RenewalDriverState::ParkRequested);
    EXPECT_EQ(remount_calls.load(), 0u) << "replacement callback must wait until renewal has parked";
    renewal_barrier.release();
    remount_barrier.waitUntilArrived();
    EXPECT_EQ(runtime.renewalDriverStateForTest(), RenewalDriverState::Parked);
    remount_barrier.release();
    runtime.stopBackgroundWorkers();
    runtime.finishTeardown(false);
}

TEST(CASPoolRemount, TeardownJoinsBothWorkersBeforeRelease)
{
    auto backend = std::make_shared<RuntimeRenewBackend>();
    const Layout layout("runtime-join");
    uint64_t wall_ms = 1000;
    uint64_t boot_ms = 100;
    const UInt128 uuid{1};
    ASSERT_EQ(claimMount(*DB::Cas::tests::OperationForTest(backend), layout, "test", uuid, 1, wall_ms, 1000).kind, MountClaimResult::Claimed);
    std::atomic<uint64_t> worker_exits{0};
    RuntimeWorkerFactory factory = [&](std::function<void()> worker_body)
    {
        return ThreadFromGlobalPool([&, body = std::move(worker_body)]
        {
            body();
            ++worker_exits;
        });
    };
    CasEventSink sink;
    RuntimeUnderTest runtime_holder(
        backend, layout,
        MountConfig{.mount_lease_ttl_ms = std::chrono::milliseconds(1000), .background_watermark = true,
                    .boot_ms_fn = [&] { return boot_ms; }, .worker_factory = factory},
        "test", sink, runtimeRenewBudget(), [] { return false; });
    CasMountRuntime & runtime = *runtime_holder;
    runtime.installRenewer(uuid, 1, [&] { return wall_ms; });
    const uint64_t anchor = runtime.startRenewer();
    runtime.armMountFence(uuid, 1, anchor + 1000);
    runtime.startBackgroundWorkers(std::chrono::hours(1));
    runtime.stopBackgroundWorkers();
    EXPECT_EQ(worker_exits.load(), 2u);
    runtime.finishTeardown(true);
    EXPECT_EQ(decodeMountLease(readObj(*backend, layout.mountKey("test"))->bytes).min_active_build_sequence,
              std::numeric_limits<uint64_t>::max());
}

TEST(CASPoolRemount, NaturalTerminalTransitionMakesBothPersistentWorkersSelfExit)
{
    for (PoolLifecycle terminal : {PoolLifecycle::IdentityLost, PoolLifecycle::VanishedReplaced})
    {
        auto backend = std::make_shared<RuntimeRenewBackend>();
        const Layout layout(terminal == PoolLifecycle::IdentityLost
            ? "runtime-natural-identity-lost"
            : "runtime-natural-vanished");
        uint64_t wall_ms = 1000;
        uint64_t boot_ms = 100;
        const UInt128 uuid{1};
        ASSERT_EQ(claimMount(*DB::Cas::tests::OperationForTest(backend), layout, "test", uuid, 1, wall_ms, 1000).kind, MountClaimResult::Claimed);
        WorkerExitLatch exits;
        DB::Cas::tests::ManualBarrier transitioned;
        RuntimeWorkerFactory factory = [&](std::function<void()> worker_body)
        {
            return ThreadFromGlobalPool([&, body = std::move(worker_body)]
            {
                body();
                exits.recordExit();
            });
        };
        CasMountRuntime * runtime_ptr = nullptr;
        CasEventSink sink;
        RuntimeUnderTest runtime_holder(
            backend, layout,
            MountConfig{
                .mount_lease_ttl_ms = std::chrono::milliseconds(1000),
                .background_watermark = true,
                .boot_ms_fn = [&] { return boot_ms; },
                .worker_factory = factory},
            "test", sink, runtimeRenewBudget(), [&]
            {
                if (terminal == PoolLifecycle::IdentityLost)
                    runtime_ptr->enterIdentityLost();
                else
                    runtime_ptr->enterVanished(PoolLifecycle::VanishedReplaced, "injected natural replacement");
                transitioned.arriveAndWait();
                return false;
            });
        CasMountRuntime & runtime = *runtime_holder;
        runtime_ptr = &runtime;
        runtime.installRenewer(uuid, 1, [&] { return wall_ms; });
        const uint64_t anchor = runtime.startRenewer();
        runtime.armMountFence(uuid, 1, anchor + 1000);
        runtime.startBackgroundWorkers(std::chrono::hours(1));
        runtime.tripMountLost();
        runtime.scheduleRemount();
        transitioned.waitUntilArrived();
        transitioned.release();
        const bool both_exited_without_stop = exits.waitForAtLeast(2);
        runtime.stopBackgroundWorkers();
        EXPECT_TRUE(both_exited_without_stop);
        EXPECT_EQ(exits.count(), 2u);
        runtime.finishTeardown(false);
    }
}

TEST(CASPoolRemount, ParkedRenewalCannotMissNaturalTerminalPublication)
{
    for (PoolLifecycle terminal : {PoolLifecycle::IdentityLost, PoolLifecycle::VanishedReplaced})
    {
        auto backend = std::make_shared<RuntimeRenewBackend>();
        const Layout layout(terminal == PoolLifecycle::IdentityLost
            ? "runtime-parked-terminal-identity-lost"
            : "runtime-parked-terminal-vanished");
        uint64_t wall_ms = 1000;
        uint64_t boot_ms = 100;
        const UInt128 uuid{1};
        ASSERT_EQ(claimMount(*DB::Cas::tests::OperationForTest(backend), layout, "test", uuid, 1, wall_ms, 1000).kind, MountClaimResult::Claimed);
        WorkerExitLatch exits;
        std::latch renewal_before_driver_lock{1};
        std::latch release_renewal{1};
        std::once_flag pause_renewal_once;
        std::latch parked_predicate_sampled_false{1};
        std::latch release_parked_predicate{1};
        std::latch terminal_pre_lock_reached{1};
        std::latch terminal_post_lock_reached{1};
        std::once_flag release_once;
        std::atomic<bool> renewal_holds_driver_mutex{false};
        std::atomic<bool> terminal_reached_post_lock_while_renewal_held{false};
        const auto release_parked = [&]
        {
            std::call_once(release_once, [&] { release_parked_predicate.count_down(); });
        };
        RuntimeWorkerFactory factory = [&](std::function<void()> worker_body)
        {
            return ThreadFromGlobalPool([&, body = std::move(worker_body)]
            {
                body();
                exits.recordExit();
            });
        };
        CasMountRuntime * runtime_ptr = nullptr;
        CasEventSink sink;
        RuntimeUnderTest runtime_holder(
            backend, layout,
            MountConfig{
                .mount_lease_ttl_ms = std::chrono::milliseconds(1000),
                .background_watermark = true,
                .boot_ms_fn = [&] { return boot_ms; },
                .worker_factory = factory,
                .remount_parked_hook_for_test = [&]
                {
                    release_renewal.count_down();
                },
                .renewal_before_driver_lock_hook_for_test = [&]
                {
                    std::call_once(pause_renewal_once, [&]
                    {
                        renewal_before_driver_lock.count_down();
                        release_renewal.wait();
                    });
                },
                .renewal_parked_predicate_false_hook_for_test = [&]
                {
                    renewal_holds_driver_mutex.store(true, std::memory_order_release);
                    parked_predicate_sampled_false.count_down();
                    release_parked_predicate.wait();
                    renewal_holds_driver_mutex.store(false, std::memory_order_release);
                },
                .terminal_publication_waiting_for_driver_lock_hook_for_test = [&]
                {
                    terminal_pre_lock_reached.count_down();
                },
                .terminal_publication_driver_lock_contended_hook_for_test = [&]
                {
                    release_parked();
                },
                .terminal_publication_driver_lock_acquired_hook_for_test = [&]
                {
                    if (renewal_holds_driver_mutex.load(std::memory_order_acquire))
                        terminal_reached_post_lock_while_renewal_held.store(true, std::memory_order_release);
                    release_parked();
                    terminal_post_lock_reached.count_down();
                }},
            "test", sink, runtimeRenewBudget(), [&]
            {
                parked_predicate_sampled_false.wait();
                if (terminal == PoolLifecycle::IdentityLost)
                    runtime_ptr->enterIdentityLost();
                else
                    runtime_ptr->enterVanished(PoolLifecycle::VanishedReplaced, "injected parked-wait replacement");
                /// Before the fix, terminal publication does not wait for `driver_mutex`, so it reaches
                /// this release only after its notification has raced ahead of the renewal worker's wait.
                /// After the fix, the pre-lock hook above releases the waiter before publication blocks.
                release_parked();
                return false;
            });
        CasMountRuntime & runtime = *runtime_holder;
        runtime_ptr = &runtime;
        runtime.installRenewer(uuid, 1, [&] { return wall_ms; });
        const uint64_t anchor = runtime.startRenewer();
        runtime.armMountFence(uuid, 1, anchor + 1000);
        runtime.startBackgroundWorkers(std::chrono::hours(1));
        renewal_before_driver_lock.wait();
        runtime.tripMountLost();
        runtime.scheduleRemount();
        terminal_pre_lock_reached.wait();
        terminal_post_lock_reached.wait();
        const bool violated_serialization
            = terminal_reached_post_lock_while_renewal_held.load(std::memory_order_acquire);
        const bool both_exited_without_stop = violated_serialization ? false : exits.waitForAtLeast(2);
        runtime.stopBackgroundWorkers();
        EXPECT_FALSE(violated_serialization);
        EXPECT_TRUE(both_exited_without_stop);
        EXPECT_EQ(exits.count(), 2u);
        runtime.finishTeardown(false);
    }
}

TEST(CASPoolRemount, VanishedReasonPreparationFailureLeavesTerminalTransitionRetryable)
{
    auto backend = std::make_shared<RuntimeRenewBackend>();
    const Layout layout("runtime-vanished-reason-preparation");
    uint64_t wall_ms = 1000;
    uint64_t boot_ms = 100;
    const UInt128 uuid{1};
    ASSERT_EQ(claimMount(*DB::Cas::tests::OperationForTest(backend), layout, "test", uuid, 1, wall_ms, 1000).kind, MountClaimResult::Claimed);
    WorkerExitLatch exits;
    RuntimeWorkerFactory factory = [&](std::function<void()> worker_body)
    {
        return ThreadFromGlobalPool([&, body = std::move(worker_body)]
        {
            body();
            exits.recordExit();
        });
    };
    std::atomic<uint64_t> preparation_calls{0};
    CasEventSink sink;
    RuntimeUnderTest runtime_holder(
        backend, layout,
        MountConfig{
            .mount_lease_ttl_ms = std::chrono::milliseconds(1000),
            .background_watermark = true,
            .boot_ms_fn = [&] { return boot_ms; },
            .worker_factory = factory,
            .vanished_reason_prepare_hook_for_test = [&]
            {
                if (preparation_calls.fetch_add(1) == 0)
                    throw DB::Exception(DB::ErrorCodes::NETWORK_ERROR, "injected vanished-reason preparation failure");
            }},
        "test", sink, runtimeRenewBudget(), [] { return false; });
    CasMountRuntime & runtime = *runtime_holder;
    runtime.installRenewer(uuid, 1, [&] { return wall_ms; });
    const uint64_t anchor = runtime.startRenewer();
    runtime.armMountFence(uuid, 1, anchor + 1000);
    runtime.startBackgroundWorkers(std::chrono::hours(1));
    runtime.tripMountLost();

    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&]
    {
        runtime.enterVanished(PoolLifecycle::VanishedReplaced, "must-not-publish");
    });
    EXPECT_FALSE(runtime.vanishedIntentPublished());
    EXPECT_EQ(runtime.lifecycle(), PoolLifecycle::TransientNotLive);
    EXPECT_TRUE(runtime.vanishedReason().empty());

    runtime.enterVanished(PoolLifecycle::VanishedReplaced, "retry-completed");
    EXPECT_EQ(runtime.lifecycle(), PoolLifecycle::VanishedReplaced);
    EXPECT_EQ(runtime.vanishedReason(), "retry-completed");
    runtime.enterVanished(PoolLifecycle::VanishedForgotten, "must-remain-ignored");
    EXPECT_EQ(runtime.lifecycle(), PoolLifecycle::VanishedReplaced);
    EXPECT_EQ(runtime.vanishedReason(), "retry-completed");
    const bool both_exited_without_stop = exits.waitForAtLeast(2);
    runtime.stopBackgroundWorkers();
    EXPECT_TRUE(both_exited_without_stop);
    EXPECT_EQ(exits.count(), 2u);
    EXPECT_EQ(preparation_calls.load(), 2u);
    runtime.finishTeardown(false);
}

TEST(CASPoolRemount, WorkerConstructionRollbackFailsOpenClosed)
{
    for (uint64_t throw_on : {1u, 2u})
    {
        auto backend = std::make_shared<RuntimeRenewBackend>();
        const Layout layout("runtime-worker-failure-" + std::to_string(throw_on));
        uint64_t wall_ms = 1000;
        uint64_t boot_ms = 100;
        uint64_t factory_calls = 0;
        RuntimeWorkerFactory factory = [&](std::function<void()> fn)
        {
            if (++factory_calls == throw_on)
                throw DB::Exception(DB::ErrorCodes::NETWORK_ERROR, "injected runtime worker construction failure");
            return ThreadFromGlobalPool(std::move(fn));
        };
        const UInt128 uuid{1};
        ASSERT_EQ(claimMount(*DB::Cas::tests::OperationForTest(backend), layout, "test", uuid, 1, wall_ms, 1000).kind, MountClaimResult::Claimed);
        CasEventSink sink;
        RuntimeUnderTest runtime_holder(
            backend, layout,
            MountConfig{.mount_lease_ttl_ms = std::chrono::milliseconds(1000), .background_watermark = true,
                        .boot_ms_fn = [&] { return boot_ms; }, .worker_factory = factory},
            "test", sink, runtimeRenewBudget(), [] { return false; });
        CasMountRuntime & runtime = *runtime_holder;
        runtime.installRenewer(uuid, 1, [&] { return wall_ms; });
        const uint64_t anchor = runtime.startRenewer();
        runtime.armMountFence(uuid, 1, anchor + 1000);
        EXPECT_THROW(runtime.startBackgroundWorkers(std::chrono::milliseconds(10)), DB::Exception);
        EXPECT_FALSE(runtime.mayMutate());
        EXPECT_FALSE(runtime.workersRunningForTest());
        runtime.finishTeardown(false);
    }
}

TEST(CASPoolRemount, ExternalLossDuringRenewalUsesOneRecoveryGeneration)
{
    auto backend = std::make_shared<RuntimeRenewBackend>();
    const Layout layout("runtime-external-loss");
    uint64_t wall_ms = 1000;
    uint64_t boot_ms = 100'000;
    const UInt128 uuid{1};
    ASSERT_EQ(claimMount(*DB::Cas::tests::OperationForTest(backend), layout, "test", uuid, 1, wall_ms, 1000).kind, MountClaimResult::Claimed);
    DB::Cas::tests::ManualBarrier renewal_barrier;
    DB::Cas::tests::ManualBarrier remount_barrier;
    std::atomic<uint64_t> remount_calls{0};
    std::atomic<uint64_t> fresh_epochs{0};
    CasEventSink sink;
    /// The remount callback reaches the runtime it is installed on, so it goes through a pointer the
    /// line after construction fills in -- the callback runs only once the workers are started.
    CasMountRuntime * runtime_ptr = nullptr;
    RuntimeUnderTest runtime_holder(
        backend, layout,
        MountConfig{.mount_lease_ttl_ms = std::chrono::milliseconds(1000), .background_watermark = true,
                    .boot_ms_fn = [&] { return boot_ms; }},
        "test", sink, runtimeRenewBudget(), [&]
        {
            ++remount_calls;
            ++fresh_epochs;
            fenceOutMount(*backend, layout.mountKey("test"));
            const MountClaimResult fresh = claimMount(*DB::Cas::tests::OperationForTest(backend), layout, "test", uuid, 2, wall_ms, 1000);
            EXPECT_EQ(fresh.kind, MountClaimResult::Claimed);
            if (fresh.kind != MountClaimResult::Claimed)
                return false;
            runtime_ptr->installRenewer(uuid, 2, [&] { return wall_ms; });
            const uint64_t fresh_anchor = runtime_ptr->startRenewer();
            runtime_ptr->setProcessEpoch(2, std::memory_order_release);
            runtime_ptr->setLiveWriterEpoch(2);
            runtime_ptr->armMountFence(uuid, 2, fresh_anchor + 1000);
            runtime_ptr->noteRemounted();
            remount_barrier.arriveAndWait();
            return true;
        });
    CasMountRuntime & runtime = *runtime_holder;
    runtime_ptr = &runtime;
    runtime.installRenewer(uuid, 1, [&] { return wall_ms; });
    const uint64_t anchor = runtime.startRenewer();
    runtime.armMountFence(uuid, 1, anchor + 1000);
    backend->barrier = &renewal_barrier;
    backend->fault = RuntimeRenewBackend::Fault::BlockThenDelegate;
    const auto lost_before = ProfileEvents::global_counters[ProfileEvents::CASMountLeaseLost].load();
    runtime.startBackgroundWorkers(std::chrono::milliseconds(0));
    renewal_barrier.waitUntilArrived();
    runtime.tripMountLost();
    runtime.scheduleRemount();
    renewal_barrier.release();
    remount_barrier.waitUntilArrived();
    EXPECT_EQ(remount_calls.load(), 1u);
    EXPECT_EQ(fresh_epochs.load(), 1u);
    EXPECT_EQ(runtime.remountRequestedGenerationForTest(), 1u);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASMountLeaseLost].load(), lost_before + 1);
    remount_barrier.release();
    runtime.stopBackgroundWorkers();
    runtime.finishTeardown(false);
}

TEST(CASPoolRemount, TerminalDepositionDoesNotTouchRenewerAfterReplacement)
{
    auto backend = std::make_shared<RuntimeRenewBackend>();
    const Layout layout("runtime-terminal-replacement");
    uint64_t wall_ms = 1000;
    uint64_t boot_ms = 100;
    const UInt128 uuid{1};
    ASSERT_EQ(claimMount(*DB::Cas::tests::OperationForTest(backend), layout, "test", uuid, 1, wall_ms, 1000).kind, MountClaimResult::Claimed);
    DB::Cas::tests::ManualBarrier terminal_deposited;
    DB::Cas::tests::ManualBarrier remount;
    std::atomic<bool> replaced{false};
    CasMountRuntime * runtime_ptr = nullptr;
    CasEventSink sink;
    RuntimeUnderTest runtime_holder(
        backend, layout,
        MountConfig{
            .mount_lease_ttl_ms = std::chrono::milliseconds(1000),
            .background_watermark = true,
            .boot_ms_fn = [&] { return boot_ms; },
            .renewal_terminal_deposited_hook_for_test = [&]
            {
                runtime_ptr->renewerReset();
                runtime_ptr->installRenewer(uuid, 2, [&] { return wall_ms; });
                runtime_ptr->renewerReset();
                replaced.store(true, std::memory_order_release);
                terminal_deposited.arriveAndWait();
            }},
        "test", sink, runtimeRenewBudget(), [&]
        {
            remount.arriveAndWait();
            return false;
        });
    CasMountRuntime & runtime = *runtime_holder;
    runtime_ptr = &runtime;
    runtime.installRenewer(uuid, 1, [&] { return wall_ms; });
    const uint64_t anchor = runtime.startRenewer();
    runtime.armMountFence(uuid, 1, anchor + 1000);
    backend->fault = RuntimeRenewBackend::Fault::ThrowBefore;
    /// Expire the lease from inside the attempt. The fault alone no longer ends a renewal: the engine
    /// settles the ambiguity by reading and then reissues, and the reissue commits. With the clock past
    /// the deadline the renewal was admitted under, neither the settling read nor the reissue is
    /// admitted, so the renewal ends terminal -- which is what this test deposits.
    backend->before_throw = [&, deadline = anchor + 1000] { boot_ms = deadline; };
    runtime.startBackgroundWorkers(std::chrono::milliseconds(0));
    terminal_deposited.waitUntilArrived();
    EXPECT_TRUE(replaced.load(std::memory_order_acquire));
    terminal_deposited.release();
    remount.waitUntilArrived();
    remount.release();
    runtime.stopBackgroundWorkers();
    runtime.finishTeardown(false);
}

TEST(CASPoolRemount, ConcurrentRemountRequestIsProcessedAfterActiveGeneration)
{
    auto backend = std::make_shared<RuntimeRenewBackend>();
    const Layout layout("runtime-generations");
    uint64_t wall_ms = 1000;
    uint64_t boot_ms = 100;
    const UInt128 uuid{1};
    ASSERT_EQ(claimMount(*DB::Cas::tests::OperationForTest(backend), layout, "test", uuid, 1, wall_ms, 1000).kind, MountClaimResult::Claimed);
    DB::Cas::tests::ManualBarrier first;
    DB::Cas::tests::ManualBarrier second;
    std::atomic<uint64_t> calls{0};
    CasEventSink sink;
    RuntimeUnderTest runtime_holder(
        backend, layout,
        MountConfig{.mount_lease_ttl_ms = std::chrono::milliseconds(1000), .background_watermark = true,
                    .boot_ms_fn = [&] { return boot_ms; }},
        "test", sink, runtimeRenewBudget(), [&]
        {
            const uint64_t call = ++calls;
            (call == 1 ? first : second).arriveAndWait();
            return true;
        });
    CasMountRuntime & runtime = *runtime_holder;
    runtime.installRenewer(uuid, 1, [&] { return wall_ms; });
    const uint64_t anchor = runtime.startRenewer();
    runtime.armMountFence(uuid, 1, anchor + 1000);
    runtime.startBackgroundWorkers(std::chrono::hours(1));
    runtime.tripMountLost();
    runtime.scheduleRemount();
    first.waitUntilArrived();
    runtime.scheduleRemount();
    first.release();
    second.waitUntilArrived();
    EXPECT_EQ(calls.load(), 2u);
    EXPECT_EQ(runtime.remountRequestedGenerationForTest(), 2u);
    second.release();
    runtime.stopBackgroundWorkers();
    runtime.finishTeardown(false);
}

TEST(CASPoolRemount, ImmediatePostRemountRenewalFailureIsNotDropped)
{
    auto backend = std::make_shared<RuntimeRenewBackend>();
    const Layout layout("runtime-catchup");
    uint64_t wall_ms = 1000;
    uint64_t boot_ms = 100;
    const UInt128 uuid{1};
    ASSERT_EQ(claimMount(*DB::Cas::tests::OperationForTest(backend), layout, "test", uuid, 1, wall_ms, 10'000).kind, MountClaimResult::Claimed);
    DB::Cas::tests::ManualBarrier first;
    DB::Cas::tests::ManualBarrier second;
    std::atomic<uint64_t> calls{0};
    CasEventSink sink;
    /// The remount callback reaches the runtime it is installed on, so it goes through a pointer the
    /// line after construction fills in -- the callback runs only once the workers are started.
    CasMountRuntime * runtime_ptr = nullptr;
    RuntimeUnderTest runtime_holder(
        backend, layout,
        MountConfig{.mount_lease_ttl_ms = std::chrono::milliseconds(10'000), .background_watermark = true,
                    .boot_ms_fn = [&] { return boot_ms; }},
        "test", sink, runtimeRenewBudget(), [&]
        {
            const uint64_t call = ++calls;
            if (call == 1)
            {
                fenceOutMount(*backend, layout.mountKey("test"));
                const MountClaimResult fresh = claimMount(*DB::Cas::tests::OperationForTest(backend), layout, "test", uuid, 2, wall_ms, 10'000);
                EXPECT_EQ(fresh.kind, MountClaimResult::Claimed);
                if (fresh.kind != MountClaimResult::Claimed)
                    return false;
                runtime_ptr->installRenewer(uuid, 2, [&] { return wall_ms; });
                const uint64_t fresh_anchor = runtime_ptr->startRenewer();
                runtime_ptr->armMountFence(uuid, 2, fresh_anchor + 10'000);
                runtime_ptr->noteRemounted();
                boot_ms = 2'000;
                backend->fault = RuntimeRenewBackend::Fault::ThrowBefore;
                /// Expire the fresh lease from inside the attempt, so the ambiguity can be neither
                /// settled by a read nor reissued: otherwise the engine reissues and the renewal
                /// commits, and there is no dropped failure to catch up on.
                backend->before_throw = [&, deadline = fresh_anchor + 10'000] { boot_ms = deadline; };
                first.arriveAndWait();
                return true;
            }
            second.arriveAndWait();
            return false;
        });
    CasMountRuntime & runtime = *runtime_holder;
    runtime_ptr = &runtime;
    runtime.installRenewer(uuid, 1, [&] { return wall_ms; });
    const uint64_t anchor = runtime.startRenewer();
    runtime.armMountFence(uuid, 1, anchor + 10'000);
    runtime.startBackgroundWorkers(std::chrono::milliseconds(1000));
    runtime.tripMountLost();
    runtime.scheduleRemount();
    first.waitUntilArrived();
    first.release();
    second.waitUntilArrived();
    EXPECT_EQ(calls.load(), 2u);
    EXPECT_EQ(runtime.remountRequestedGenerationForTest(), 2u);
    second.release();
    runtime.stopBackgroundWorkers();
    runtime.finishTeardown(false);
}

TEST(CASPoolRemount, StaleRemountAnchorPerformsParkedRedo)
{
    auto backend = std::make_shared<RuntimeRenewBackend>();
    /// Held in a shared atomic, not a plain local: `remount_quiesce_hook_for_test` below mutates it,
    /// and the Pool can outlive this stack frame (a background publish holds `shared_from_this()`), so
    /// a by-reference capture of a local would dangle.
    auto fake_boot = std::make_shared<std::atomic<uint64_t>>(100);
    /// Heap-owned, not a plain local: declaration order relative to the Pool below only protects
    /// against an ordinary same-thread unwind, not a detached background completion that holds an
    /// extra `shared_from_this()` and can still be running on another thread after this frame returns.
    auto committed = std::make_shared<DB::Cas::tests::ManualBarrier>();
    PoolConfig config{
        .pool_prefix = "stale-remount-anchor",
        .server_root_id = "test",
        .background_watermark = true,
        .event_sink = [committed](const CasEvent & event)
        {
            if (event.type == CasEventType::MountRemount && event.outcome == "ok")
                committed->arriveAndWait();
        },
        .mount_lease_ttl_ms = std::chrono::milliseconds(1000),
        .mount_renew_period = std::chrono::milliseconds(100),
        .cas_request_budget = runtimeRenewBudget(),
        .boot_ms_fn = [fake_boot]
        {
            return fake_boot->load();
        },
        .remount_quiesce_hook_for_test = [fake_boot]
        {
            *fake_boot += 900;
        },
    };
    auto store = Pool::open(backend, config);
    const String key = store->layout().mountKey("test");
    fenceOutMount(*backend, key);
    const uint64_t writes_before = backend->putOverwriteCount(key);
    ASSERT_TRUE(store->scheduleRemountForTest());
    committed->waitUntilArrived();
    EXPECT_GE(backend->putOverwriteCount(key), writes_before + 3)
        << "claim, renewer start, and the stale-anchor parked redo must all write";
    committed->release();
}

TEST(CASPoolRemount, ParkedRedoRecoveryObservabilityPrecedesRemountResult)
{
    auto backend = std::make_shared<RuntimeRenewBackend>();
    /// Held in a shared atomic, not a plain local: `remount_quiesce_hook_for_test` below mutates it,
    /// and the Pool can outlive this stack frame (a background publish holds `shared_from_this()`), so
    /// a by-reference capture of a local would dangle.
    auto fake_boot = std::make_shared<std::atomic<uint64_t>>(100);
    /// Heap-owned, not plain locals: `event_sink` below mutates them, and the Pool can outlive this
    /// stack frame (a background publish holds `shared_from_this()`), so a by-reference capture of a
    /// local -- including a non-copyable `std::promise` -- would dangle.
    auto result_observed = std::make_shared<std::promise<void>>();
    std::future<void> result_future = result_observed->get_future();
    auto result_published = std::make_shared<std::atomic<bool>>(false);
    auto events = std::make_shared<DB::Cas::tests::SharedEventLog>();
    PoolConfig config{
        .pool_prefix = "parked-redo-recovered-observability",
        .server_root_id = "test",
        .background_watermark = true,
        .event_sink = [result_observed, result_published, events](CasEvent event)
        {
            const bool final_remount = event.type == CasEventType::MountRemount && event.outcome == "ok";
            events->push(std::move(event));
            if (final_remount && !result_published->exchange(true))
                result_observed->set_value();
        },
        .mount_lease_ttl_ms = std::chrono::milliseconds(1000),
        /// 500 with a 700 ms quiescence, so the redo's window (period + attempt timeout = 510) does not
        /// fit the 280 ms of safe lease left -- and the reissue the ambiguity needs still does, whatever
        /// the engine's jittered backoff draws from its first-reissue range of at most 200 ms.
        .mount_renew_period = std::chrono::milliseconds(500),
        .cas_request_budget = runtimeRenewBudget(),
        .boot_ms_fn = [fake_boot]
        {
            return fake_boot->load();
        },
        /// `backend` is captured BY VALUE (a copy of the shared_ptr, not the stack slot holding it):
        /// the Pool can outlive this frame, so a by-reference capture of the local `shared_ptr` itself
        /// would dangle even though the pointee it owns is heap-allocated.
        .remount_quiesce_hook_for_test = [fake_boot, backend]
        {
            *fake_boot += 700;
            backend->fault = RuntimeRenewBackend::Fault::ThrowBefore;
        },
    };
    auto store = Pool::open(backend, config);
    std::weak_ptr<Pool> store_lifetime = store;
    ScopedParkedRenewalLogCapture renewal_logs;
    fenceOutMount(*backend, store->layout().mountKey("test"));
    ASSERT_TRUE(store->scheduleRemountForTest());
    ASSERT_EQ(result_future.wait_for(std::chrono::seconds(20)), std::future_status::ready);

    const std::vector<CasEvent> observed = events->snapshot();
    const auto recovered = std::find_if(observed.begin(), observed.end(), [](const CasEvent & event)
    {
        return event.type == CasEventType::WatermarkRenew && event.outcome == "recovered";
    });
    const auto remounted = std::find_if(observed.begin(), observed.end(), [](const CasEvent & event)
    {
        return event.type == CasEventType::MountRemount && event.outcome == "ok";
    });
    ASSERT_NE(recovered, observed.end());
    ASSERT_NE(remounted, observed.end());
    EXPECT_LT(std::distance(observed.begin(), recovered), std::distance(observed.begin(), remounted));
    EXPECT_EQ(recovered->detail.at("remount_attempt_no"), remounted->detail.at("attempt_no"));
    EXPECT_EQ(recovered->detail.at("classification"), "committed_after_retry");
    /// The physical retry itself: the ambiguous attempt and the reissue that committed.
    EXPECT_EQ(recovered->detail.at("attempts_sent"), "2");
    EXPECT_NE(renewal_logs.captured().find("CAS mount renewal 'test' recovered"), String::npos);

    /// `~Pool` stops and joins both persistent runtime workers. Make that quiescence boundary part of
    /// the test, before any event/log capture state referenced by those workers can leave scope.
    store.reset();
    EXPECT_TRUE(store_lifetime.expired());
}

TEST(CASPoolRemount, ParkedRedoFailureObservabilityPrecedesRemountResult)
{
    auto backend = std::make_shared<RuntimeRenewBackend>();
    /// Held in a shared atomic, not a plain local: the hooks below mutate it, and the Pool can outlive
    /// this stack frame (a background publish holds `shared_from_this()`), so a by-reference capture
    /// of a local would dangle.
    auto fake_boot = std::make_shared<std::atomic<uint64_t>>(100);
    /// Heap-owned, not plain locals: `event_sink` below mutates them, and the Pool can outlive this
    /// stack frame (a background publish holds `shared_from_this()`), so a by-reference capture of a
    /// local -- including a non-copyable `std::promise` -- would dangle.
    auto result_observed = std::make_shared<std::promise<void>>();
    std::future<void> result_future = result_observed->get_future();
    auto result_published = std::make_shared<std::atomic<bool>>(false);
    auto events = std::make_shared<DB::Cas::tests::SharedEventLog>();
    PoolConfig config{
        .pool_prefix = "parked-redo-failed-observability",
        .server_root_id = "test",
        .background_watermark = true,
        .event_sink = [result_observed, result_published, events](CasEvent event)
        {
            const bool final_remount = event.type == CasEventType::MountRemount && event.outcome == "failed";
            events->push(std::move(event));
            if (final_remount && !result_published->exchange(true))
                result_observed->set_value();
        },
        .mount_lease_ttl_ms = std::chrono::milliseconds(1000),
        .mount_renew_period = std::chrono::milliseconds(100),
        .cas_request_budget = runtimeRenewBudget(),
        .boot_ms_fn = [fake_boot]
        {
            return fake_boot->load();
        },
        /// `backend` is captured BY VALUE (a copy of the shared_ptr): the Pool can outlive this frame,
        /// so a by-reference capture of the local `shared_ptr` itself would dangle.
        .remount_quiesce_hook_for_test = [fake_boot, backend]
        {
            *fake_boot += 900;
            backend->fault = RuntimeRenewBackend::Fault::ThrowBefore;
            /// The attempt is admitted 80 ms before its lease-safe bound; spending 90 inside it puts the
            /// resolve read past that bound, so the ambiguity is refused instead of reissued.
            backend->before_throw = [fake_boot]
            {
                *fake_boot += 90;
            };
        },
    };
    auto store = Pool::open(backend, config);
    std::weak_ptr<Pool> store_lifetime = store;
    ScopedParkedRenewalLogCapture renewal_logs;
    fenceOutMount(*backend, store->layout().mountKey("test"));
    ASSERT_TRUE(store->scheduleRemountForTest());
    ASSERT_EQ(result_future.wait_for(std::chrono::seconds(20)), std::future_status::ready);

    const std::vector<CasEvent> observed = events->snapshot();
    const auto failed_renew = std::find_if(observed.begin(), observed.end(), [](const CasEvent & event)
    {
        return event.type == CasEventType::WatermarkRenew && event.outcome == "failed";
    });
    const auto failed_remount = std::find_if(observed.begin(), observed.end(), [](const CasEvent & event)
    {
        return event.type == CasEventType::MountRemount && event.outcome == "failed";
    });
    ASSERT_NE(failed_renew, observed.end());
    ASSERT_NE(failed_remount, observed.end());
    EXPECT_LT(std::distance(observed.begin(), failed_renew), std::distance(observed.begin(), failed_remount));
    EXPECT_EQ(failed_renew->detail.at("remount_attempt_no"), failed_remount->detail.at("attempt_no"));
    EXPECT_EQ(failed_renew->detail.at("attempts_sent"), "1");
    EXPECT_EQ(failed_renew->detail.at("classification"), "external_lease_deadline");
    EXPECT_NE(renewal_logs.captured().find("CAS mount renewal 'test' fenced"), String::npos);

    /// A ready final-result future proves publication order; destruction additionally proves the
    /// background renewal/remount threads are joined before the fixture's captured state is destroyed.
    store.reset();
    EXPECT_TRUE(store_lifetime.expired());
}

TEST(CASPoolRemount, ThrowingEventSinkAfterCommitLeavesRuntimeLive)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend, PoolConfig{
        .pool_prefix = "throwing-remount-event", .server_root_id = "test", .background_watermark = true});
    /// Heap-owned, not a plain local declared after `store`: if `waitUntilArrived` below throws on its
    /// own internal timeout, unwinding would destroy a stack-local barrier before `store`'s destructor
    /// joins the remount worker, and that worker can still be inside `arriveAndWait` on the dangling
    /// reference. A `shared_ptr` capture keeps the barrier alive for as long as the worker needs it,
    /// independent of declaration order.
    auto committed = std::make_shared<DB::Cas::tests::ManualBarrier>();
    store->setEventSink([committed](const CasEvent & event)
    {
        if (event.type == CasEventType::MountRemount && event.outcome == "ok")
        {
            committed->arriveAndWait();
            throw DB::Exception(DB::ErrorCodes::NETWORK_ERROR, "injected remount event sink failure");
        }
    });
    fenceOutMount(*backend, store->layout().mountKey("test"));
    ASSERT_TRUE(store->scheduleRemountForTest());
    committed->waitUntilArrived();
    EXPECT_EQ(store->lifecycle(), PoolLifecycle::Live);
    EXPECT_TRUE(store->mayMutate());
    committed->release();
    EXPECT_NO_THROW(store.reset());
}

TEST(CASPoolShutdown, PreSendCancellationAllowsFarewellButAmbiguityDoesNot)
{
    const auto run = [](bool ambiguous)
    {
        auto backend = std::make_shared<RuntimeRenewBackend>();
        const Layout layout(ambiguous ? "shutdown-ambiguous" : "shutdown-presend");
        uint64_t wall_ms = 1000;
        uint64_t boot_ms = 100;
        const UInt128 uuid{1};
        const MountClaimResult claim = claimMount(*DB::Cas::tests::OperationForTest(backend), layout, "test", uuid, 1, wall_ms, 1000);
        EXPECT_EQ(claim.kind, MountClaimResult::Claimed);
        if (claim.kind != MountClaimResult::Claimed)
            return uint64_t{0};
        DB::Cas::tests::ManualBarrier barrier;
        CasEventSink sink;
        RuntimeUnderTest runtime_holder(
            backend, layout,
            MountConfig{.mount_lease_ttl_ms = std::chrono::milliseconds(1000), .background_watermark = true,
                        .boot_ms_fn = [&] { return boot_ms; }},
            "test", sink, runtimeRenewBudget(), [] { return false; });
        CasMountRuntime & runtime = *runtime_holder;
        runtime.installRenewer(uuid, 1, [&] { return wall_ms; });
        const uint64_t anchor = runtime.startRenewer();
        runtime.armMountFence(uuid, 1, anchor + 1000);
        if (ambiguous)
        {
            backend->barrier = &barrier;
            backend->fault = RuntimeRenewBackend::Fault::BlockThenThrow;
            runtime.startBackgroundWorkers(std::chrono::milliseconds(0));
            barrier.waitUntilArrived();
            auto stop = std::async(std::launch::async, [&] { runtime.stopBackgroundWorkers(); });
            barrier.release();
            stop.get();
        }
        else
        {
            runtime.startBackgroundWorkers(std::chrono::hours(1));
            runtime.stopBackgroundWorkers();
        }
        runtime.finishTeardown(true);
        return decodeMountLease(readObj(*backend, layout.mountKey("test"))->bytes).min_active_build_sequence;
    };

    EXPECT_EQ(run(false), std::numeric_limits<uint64_t>::max());
    EXPECT_NE(run(true), std::numeric_limits<uint64_t>::max());
}

TEST(CASPool, DirectAndStartupTerminalFailuresRethrowTypedExceptions)
{
    enum class Refusal : uint8_t { PreAttemptDeadline, RefusedAfterSend };
    const auto run = [](bool startup, Refusal refusal)
    {
        auto backend = std::make_shared<RuntimeRenewBackend>();
        const Layout layout(startup ? "typed-startup" : "typed-direct");
        uint64_t wall_ms = 1000;
        uint64_t boot_ms = 100;
        std::atomic<bool> renewal_live{true};
        const UInt128 uuid{1};
        ASSERT_EQ(claimMount(*DB::Cas::tests::OperationForTest(backend), layout, "test", uuid, 1, wall_ms, 1000).kind, MountClaimResult::Claimed);
        CasEventSink sink;
        RuntimeUnderTest runtime_holder(
            backend, layout,
            MountConfig{
                .mount_lease_ttl_ms = std::chrono::milliseconds(1000),
                .boot_ms_fn = [&] { return boot_ms; },
                .renewal_live_for_test = [&] { return renewal_live.load(std::memory_order_acquire); }},
            "test", sink, runtimeRenewBudget(), [] { return false; });
        CasMountRuntime & runtime = *runtime_holder;
        runtime.installRenewer(uuid, 1, [&] { return wall_ms; });
        const uint64_t anchor = runtime.startRenewer();
        runtime.armMountFence(uuid, 1, anchor + 1000);
        if (refusal == Refusal::PreAttemptDeadline)
            /// Past the point where the lease has more room left than the safety margin (deadline
            /// `anchor + 1000` == 1100, margin 20), so admission refuses before anything is sent.
            boot_ms = 1090;
        else
            backend->after_commit = [&] { renewal_live.store(false, std::memory_order_release); };
        try
        {
            if (startup)
                (void)runtime.renewRenewerForStartupOnce();
            else
                runtime.renewWatermarkOnce();
            ADD_FAILURE() << "terminal renewal did not propagate";
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), DB::ErrorCodes::NETWORK_ERROR) << e.message();
        }
        runtime.finishTeardown(false);
    };
    for (bool startup : {true, false})
        for (Refusal refusal : {Refusal::PreAttemptDeadline, Refusal::RefusedAfterSend})
            run(startup, refusal);
}

TEST(CASPool, BackgroundCadenceMustFitLeaseBeforeWritablePublication)
{
    auto backend = std::make_shared<DB::Cas::tests::CountingBackend>();
    PoolConfig config{
        .pool_prefix = "invalid-renew-cadence",
        .server_root_id = "test",
        .background_watermark = true,
        .mount_lease_ttl_ms = std::chrono::milliseconds(100),
        .mount_renew_period = std::chrono::milliseconds(80),
        .cas_request_budget = runtimeRenewBudget(),
    };
    EXPECT_THROW((void)Pool::open(backend, config), DB::Exception);
    /// One assertion over every write shape: the counters now sit on the write primitive, which both
    /// the create- and the replace-shaped verbs reach.
    EXPECT_EQ(backend->writeTotal(), 0u);
}

TEST(CASPool, DecommissionCadenceValidationPrecedesAuthorityWrites)
{
    auto backend = std::make_shared<DB::Cas::tests::CountingBackend>();
    {
        auto victim = Pool::open(backend, PoolConfig{.pool_prefix = "invalid-decommission-cadence", .server_root_id = "victim"});
    }
    backend->resetCounts();
    PoolConfig config{
        .pool_prefix = "invalid-decommission-cadence",
        .server_root_id = "admin",
        .mount_lease_ttl_ms = std::chrono::milliseconds(100),
        .mount_renew_period = std::chrono::milliseconds(80),
        .cas_request_budget = runtimeRenewBudget(),
    };
    expectThrowsCode(DB::ErrorCodes::BAD_ARGUMENTS, [&]
    {
        (void)Pool::openForDecommission(backend, config, "victim");
    });
    /// One assertion over every write shape: the counters now sit on the write primitive, which both
    /// the create- and the replace-shaped verbs reach.
    EXPECT_EQ(backend->writeTotal(), 0u);
}

TEST(CASPool, DisabledBackgroundDoesNotReserveRenewalCadence)
{
    auto backend = std::make_shared<RuntimeRenewBackend>();
    /// Captured by value: `fake_boot` is never mutated in this test, and the Pool can outlive this
    /// stack frame (a background publish holds `shared_from_this()`), so a by-reference capture would
    /// dangle.
    const uint64_t fake_boot = 100;
    PoolConfig config{
        .pool_prefix = "disabled-renew-cadence",
        .server_root_id = "test",
        .background_watermark = false,
        .mount_lease_ttl_ms = std::chrono::milliseconds(100),
        .mount_renew_period = std::chrono::hours(24),
        .cas_request_budget = runtimeRenewBudget(),
        .boot_ms_fn = [] { return fake_boot; },
    };
    auto store = Pool::open(backend, config);
    const String key = store->layout().mountKey("test");
    EXPECT_EQ(backend->putOverwriteCount(key), 1u)
        << "a disabled worker cadence must not force a synchronous startup redo";
}

TEST(CASPool, DeterministicWorkerFailureFencesWithoutWaitingForCadence)
{
    auto backend = std::make_shared<RuntimeRenewBackend>();
    const Layout layout("worker-failure");
    uint64_t wall_ms = 1000;
    uint64_t boot_ms = 100;
    const UInt128 uuid{1};
    ASSERT_EQ(claimMount(*DB::Cas::tests::OperationForTest(backend), layout, "test", uuid, 1, wall_ms, 1000).kind, MountClaimResult::Claimed);
    DB::Cas::tests::ManualBarrier remount_entered;
    CasEventSink sink;
    RuntimeUnderTest runtime_holder(
        backend, layout,
        MountConfig{.mount_lease_ttl_ms = std::chrono::milliseconds(1000), .background_watermark = true,
                    .boot_ms_fn = [&] { return boot_ms; }},
        "test", sink, runtimeRenewBudget(), [&]
        {
            remount_entered.arriveAndWait();
            return false;
        });
    CasMountRuntime & runtime = *runtime_holder;
    runtime.installRenewer(uuid, 1, [&] { return wall_ms; });
    const uint64_t anchor = runtime.startRenewer();
    runtime.armMountFence(uuid, 1, anchor + 1000);
    backend->fault = RuntimeRenewBackend::Fault::ThrowBefore;
    /// Expire the lease from inside the attempt, so the ambiguity can be neither settled by a read nor
    /// reissued: without that the engine reissues and the renewal commits, and this worker never fences.
    backend->before_throw = [&, deadline = anchor + 1000] { boot_ms = deadline; };
    runtime.startBackgroundWorkers(std::chrono::milliseconds(0));
    remount_entered.waitUntilArrived();
    EXPECT_FALSE(runtime.mayMutate());
    EXPECT_EQ(runtime.lifecycle(), PoolLifecycle::TransientNotLive);
    remount_entered.release();
    runtime.stopBackgroundWorkers();
    runtime.finishTeardown(false);
}

TEST(CASPool, RenewWatermarkOnceRefreshesFenceAndDepositsOneFailure)
{
    auto backend = std::make_shared<RuntimeRenewBackend>();
    /// Held in a shared atomic, not a plain local: this test mutates it directly below, and the Pool
    /// can outlive this stack frame (a background publish holds `shared_from_this()`), so a
    /// by-reference capture of a local would dangle.
    auto fake_boot = std::make_shared<std::atomic<uint64_t>>(100);
    PoolConfig config{
        .pool_prefix = "direct-renew",
        .server_root_id = "test",
        .mount_lease_ttl_ms = std::chrono::milliseconds(1000),
        .cas_request_budget = runtimeRenewBudget(),
        .boot_ms_fn = [fake_boot]
        {
            return fake_boot->load();
        },
    };
    auto store = Pool::open(backend, config);
    fake_boot->store(500);
    EXPECT_NO_THROW(store->renewWatermarkOnce());
    fake_boot->store(1200);
    EXPECT_TRUE(store->mayMutate()) << "direct success must refresh the local fence from attempt start";

    backend->fault = RuntimeRenewBackend::Fault::ThrowBefore;
    /// The renewal that succeeded at 500 anchored the lease for its 1000 ms TTL, so it expires at 1500.
    /// Expire it from inside the attempt: the fault alone no longer ends a renewal, because the engine
    /// settles the ambiguity by reading and reissues, and the reissue commits.
    backend->before_throw = [fake_boot]
    {
        fake_boot->store(1500);
    };
    const uint64_t schedules_before = store->scheduleRemountCallCountForTest();
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->renewWatermarkOnce(); });
    EXPECT_FALSE(store->mayMutate());
    EXPECT_EQ(store->scheduleRemountCallCountForTest(), schedules_before + 1);
}

TEST(CASPoolRemount, WholeChainResultsAreNumberedAndStepLabelled)
{
    auto backend = std::make_shared<RemountStepBackend>();
    /// Heap-owned, not a plain local: the Pool can outlive this stack frame (a background publish holds
    /// `shared_from_this()`), so a by-reference capture of a local would dangle.
    auto events = std::make_shared<DB::Cas::tests::SharedEventLog>();
    auto store = Pool::open(backend, PoolConfig{
        .pool_prefix = "remount-observability",
        .server_root_id = "test",
    });
    store->setEventSink([events](CasEvent event)
    {
        events->push(std::move(event));
    });
    ScopedRemountLogCapture logs;

    store->tripMountLost();
    backend->failNextRead(store->layout().poolMetaKey());
    const uint64_t attempts_before = ProfileEvents::global_counters[ProfileEvents::CASRemountAttempts].load();
    const uint64_t succeeded_before = ProfileEvents::global_counters[ProfileEvents::CASRemountSucceeded].load();
    const uint64_t failed_before = ProfileEvents::global_counters[ProfileEvents::CASRemountFailed].load();
    EXPECT_FALSE(store->tryRemountOnce());

    fenceOutMount(*backend, store->layout().mountKey("test"));
    EXPECT_TRUE(store->tryRemountOnce());

    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASRemountAttempts].load(), attempts_before + 2);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASRemountSucceeded].load(), succeeded_before + 1);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASRemountFailed].load(), failed_before + 1);

    const std::vector<CasEvent> observed_events = events->snapshot();
    std::vector<CasEvent> remounts;
    std::copy_if(observed_events.begin(), observed_events.end(), std::back_inserter(remounts), [](const CasEvent & event)
    {
        return event.type == CasEventType::MountRemount;
    });
    ASSERT_EQ(remounts.size(), 2u);
    EXPECT_EQ(remounts[0].outcome, "failed");
    EXPECT_EQ(remounts[0].detail.at("step"), "pool_identity_probe");
    EXPECT_EQ(remounts[1].outcome, "ok");
    EXPECT_EQ(remounts[1].detail.at("step"), "publish_live");
    const uint64_t first_attempt = std::stoull(remounts[0].detail.at("attempt_no"));
    const uint64_t second_attempt = std::stoull(remounts[1].detail.at("attempt_no"));
    EXPECT_EQ(second_attempt, first_attempt + 1);
    EXPECT_EQ(countRemountFinalLogs(logs.captured()), 2u) << logs.captured();
}

/// The remount's `renewer_redo` step re-anchors the lease BEFORE `armMountFence`, so it runs with the
/// fence still latched lost. Admitted on the mount plane it could only ever give up, and every remount
/// that reached the step would fail -- so it renews on the renewer's open plane instead.
///
/// Driven the way production reaches the step, which is the only way it CAN be reached: the persistent
/// renewal worker runs, `scheduleRemount` parks it, and the redo is the parked driver's one call. A
/// remount driven directly with no workers leaves that driver dormant, and the step's admission refuses
/// a dormant driver rather than renewing.
///
/// The step is reached only when quiescence has eaten most of the new lease: with a fresh anchor the
/// renewal window fits and the step is skipped entirely. So the quiesce hook advances the injected boot
/// clock to just inside the safety margin, and the paired run with no quiesce cost is the control that
/// proves the step was reached rather than skipped.
TEST(CASPoolRemount, TheRenewerRedoRenewsOnTheOpenPlane)
{
    /// One successful self-remount whose quiescence costs `quiesce_ms`; returns the conditional
    /// mount-slot writes it issued. Counted while the remount worker is still held inside the event
    /// sink that reported the result, so the renewal worker it un-parks cannot add one.
    const auto remountConditionalMountWrites = [](uint64_t quiesce_ms) -> uint64_t
    {
        auto backend = std::make_shared<DB::Cas::tests::CountingBackend>();
        /// Held in a shared atomic, not a plain local: the hooks below mutate it, and the Pool can
        /// outlive this lambda's own stack frame (a background publish holds `shared_from_this()`), so
        /// a by-reference capture of a local would dangle.
        auto fake_boot = std::make_shared<std::atomic<uint64_t>>(1'000'000);
        /// Heap-owned, not a plain local: declaration order relative to `store` only protects against an
        /// ordinary same-thread unwind, not a detached background completion that holds an extra
        /// `shared_from_this()` and can still be running on another thread after this call returns.
        auto committed = std::make_shared<DB::Cas::tests::ManualBarrier>();
        auto store = Pool::open(backend, PoolConfig{
            .pool_prefix = "remount-renewer-redo",
            .server_root_id = "test",
            .background_watermark = true,
            .event_sink = [committed](const CasEvent & event)
            {
                if (event.type == CasEventType::MountRemount && event.outcome == "ok")
                    committed->arriveAndWait();
            },
            .boot_ms_fn = [fake_boot]
            {
                return fake_boot->load();
            },
            .wait_sleep_fn = [fake_boot](uint64_t ms)
            {
                *fake_boot += ms;
            },
            .remount_quiesce_hook_for_test = [fake_boot, quiesce_ms]
            {
                *fake_boot += quiesce_ms;
            },
        });
        const String mount_key = store->layout().mountKey("test");

        fenceOutMount(*backend, mount_key);
        const uint64_t before = backend->putOverwriteCount(mount_key);
        EXPECT_TRUE(store->scheduleRemountForTest())
            << "the remount must be latched with quiesce_ms=" << quiesce_ms;
        committed->waitUntilArrived();
        const uint64_t writes = backend->putOverwriteCount(mount_key) - before;
        committed->release();
        return writes;
    };

    /// 27 s of a 30 s lease, against a 2 s safety margin and a window of one renewal period plus one
    /// attempt (15 s, since the renewal worker runs here): the window no longer fits.
    EXPECT_GT(remountConditionalMountWrites(27'000), remountConditionalMountWrites(0))
        << "a quiescence that consumed the lease must cost one extra lease write -- the redo";
}

TEST(CASPoolRemount, LeaseLossHasOneOperationalOwner)
{
    auto backend = std::make_shared<RemountStepBackend>();
    auto store = Pool::open(backend, PoolConfig{
        .pool_prefix = "lease-loss-owner",
        .server_root_id = "test",
    });
    const uint64_t lost_before = ProfileEvents::global_counters[ProfileEvents::CASMountLeaseLost].load();

    store->tripMountLost();
    store->tripMountLost();
    backend->failNextRead(store->layout().poolMetaKey());
    EXPECT_FALSE(store->tryRemountOnce());
    store->beginShutdownForTest();
    store->tripMountLost();

    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASMountLeaseLost].load(), lost_before + 1);
}

TEST(CASPoolRemount, LiveForgetDoesNotCountOperationalLeaseLoss)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend, PoolConfig{
        .pool_prefix = "forget-is-not-lease-loss",
        .server_root_id = "test",
    });
    ASSERT_EQ(store->lifecycle(), PoolLifecycle::Live);
    const uint64_t lost_before = ProfileEvents::global_counters[ProfileEvents::CASMountLeaseLost].load();

    store->forgetDisk([] {}, "deliberate test decommission");

    EXPECT_EQ(store->lifecycle(), PoolLifecycle::VanishedForgotten);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASMountLeaseLost].load(), lost_before)
        << "a deliberate terminal decommission is not an operational recovery generation";
}

/// Coverage gap (Task 13a): restores the get/exists/remove roundtrip for the mount access-check probe
/// object. The old `CASPool.MountpointObjectRoundTrip` was dropped in the refactor; the wiring test only
/// exercises `putMountpointObject` + `existsFile`, leaving `getMountpointObject`'s value round-trip and
/// `removeMountpointObject` unasserted even though both `Pool` methods remain live.
TEST(CASPool, MountpointObjectRoundTrip)
{
    auto b = std::make_shared<DB::Cas::InMemoryBackend>();
    auto store = DB::Cas::Pool::open(b, DB::Cas::PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    const String key = "srv1/clickhouse_access_check_abc";
    EXPECT_FALSE(store->getMountpointObject(key).has_value());
    EXPECT_FALSE(store->mountpointObjectExists(key));
    store->putMountpointObject(key, "probe-bytes");
    EXPECT_TRUE(store->mountpointObjectExists(key));
    auto got = store->getMountpointObject(key);
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(*got, "probe-bytes");
    store->removeMountpointObject(key);
    EXPECT_FALSE(store->getMountpointObject(key).has_value());
    EXPECT_FALSE(store->mountpointObjectExists(key));
}

namespace ProfileEvents
{
    extern const Event CASHotKeyReadStarts;
    extern const Event CASRequestResolveRead;
    extern const Event CASRequestConflictPause;
}

TEST(CASPool, ConcurrentNamespaceCreationsNeverRaceEachOtherOnTheCatalog)
{
    auto backend = std::make_shared<DB::Cas::tests::CountingBackend>();
    auto pool = DB::Cas::tests::openPoolForTest(backend);
    const DB::Cas::Layout layout("p");
    const String key = layout.refCatalogKey();
    constexpr int N = 6;

    /// Drain whatever the pool's own bootstrap touched on the catalog key before measuring.
    (void)pool->namespaceLife(DB::Cas::RootNamespace{"warmup"});

    const uint64_t writes_before = backend->writeCount(key);
    const auto reads_before = ProfileEvents::global_counters[ProfileEvents::CASHotKeyReadStarts].load();
    const auto resolves_before = ProfileEvents::global_counters[ProfileEvents::CASRequestResolveRead].load();
    std::vector<std::thread> threads;
    for (int i = 0; i < N; ++i)
        threads.emplace_back([&, i] { (void)pool->namespaceLife(DB::Cas::RootNamespace{"ns" + std::to_string(i)}); });
    for (auto & t : threads)
        t.join();

    EXPECT_EQ(backend->writeCount(key) - writes_before, 2u * N) << "two catalog steps per creation, each one write";
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASRequestResolveRead].load() - resolves_before, 0u)
        << "no refused precondition, so no resolve read";
    EXPECT_LE(ProfileEvents::global_counters[ProfileEvents::CASHotKeyReadStarts].load() - reads_before, 1u)
        << "at most one lane read; every later hold started from the cache";

    /// Another server writes the catalog between two of this pool's mutations: one extra read and one
    /// retry write, then the cache is current again. Raw `getCount` cannot isolate that cost: a
    /// `namespaceLife` call on a fresh namespace also issues the ledger's own snapshot reads
    /// (`CasRefCatalog::read`), which are outside the lane by design and fire the same number of times
    /// whether or not an external write happened. The lane's own signals are what the external write
    /// actually moves.
    {
        auto external_requests = DB::Cas::tests::openRequestsForTest(backend);
        auto external = external_requests.admit();
        DB::Cas::CasRefCatalog::casAdmitEntry(external, layout, 1,
            DB::Cas::CatalogEntry{.ns = DB::Cas::RootNamespace{"zz"}, .state = DB::Cas::NsState::Live, .incarnation = UInt128{99}});
    }
    const uint64_t writes_mid = backend->writeCount(key);
    const auto resolves_mid = ProfileEvents::global_counters[ProfileEvents::CASRequestResolveRead].load();
    const auto lane_reads_mid = ProfileEvents::global_counters[ProfileEvents::CASHotKeyReadStarts].load();
    (void)pool->namespaceLife(DB::Cas::RootNamespace{"after"});
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASRequestResolveRead].load() - resolves_mid, 1u)
        << "one resolve read for the external write";
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASHotKeyReadStarts].load() - lane_reads_mid, 0u)
        << "the next hold starts from what the resolve read saw";
    EXPECT_EQ(backend->writeCount(key) - writes_mid, 3u) << "one refused, two landed";
}
