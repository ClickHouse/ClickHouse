#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPoolMetaFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPartManifestFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>
#include <Poco/Exception.h>
#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <future>
#include <limits>
#include <optional>
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
}

using namespace DB::Cas;
using DB::Cas::tests::blobEntryFor;
using DB::Cas::tests::expectThrowsCode;
using DB::Cas::tests::idOf;
using DB::Cas::tests::u128Of;

namespace
{
/// Counts mutating backend calls so a test can assert an open path is write-free.
class WriteCountingBackend final : public DB::Cas::Backend
{
public:
    explicit WriteCountingBackend(std::shared_ptr<DB::Cas::Backend> inner_) : inner(std::move(inner_)) {}
    size_t writes = 0;

    std::optional<DB::Cas::GetResult> get(const String & k, DB::Cas::Range r) override { return inner->get(k, r); }
    std::optional<DB::Cas::GetStreamResult> getStream(const String & k, DB::Cas::Range r) override { return inner->getStream(k, r); }
    DB::Cas::HeadResult head(const String & k) override { return inner->head(k); }
    DB::Cas::ListPage list(const String & p, const String & c, size_t l) override { return inner->list(p, c, l); }
    DB::Cas::PutResult putIfAbsent(const String & k, const String & b, const DB::Cas::ObjectMeta & meta) override { ++writes; return inner->putIfAbsent(k, b, meta); }
    DB::Cas::WriteSinkPtr putIfAbsentStream(const String & k, const DB::Cas::ObjectMeta & meta) override { ++writes; return inner->putIfAbsentStream(k, meta); }
    DB::Cas::PutResult putOverwrite(const String & k, const String & b, const DB::Cas::Token & e, const DB::Cas::ObjectMeta & meta) override { ++writes; return inner->putOverwrite(k, b, e, meta); }
    DB::Cas::CasResult casPut(const String & k, const String & b, const std::optional<DB::Cas::Token> & e, const DB::Cas::ObjectMeta & meta) override { ++writes; return inner->casPut(k, b, e, meta); }
    DB::Cas::DeleteOutcome deleteExact(const String & k, const DB::Cas::Token & t) override { ++writes; return inner->deleteExact(k, t); }
    bool supportsListTokens() const override { return inner->supportsListTokens(); }
private:
    std::shared_ptr<DB::Cas::Backend> inner;
};

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
    build->putBlob(idOf(payload), BlobSource::fromString(payload));

    ManifestEntry e;
    e.path = entry_path;
    e.placement = EntryPlacement::Blob;
    e.ref = DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(u128Of(payload))};

    e.blob_size = payload.size();

    const ManifestId id = build->stageManifest({e});
    build->precommitAdd(nsr, ref, id);
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
            DB::Cas::tests::writeBlobBody(s->backend(), s->layout(), e.ref.digest.toU128());
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

    std::optional<DB::Cas::GetResult> get(const String & k, DB::Cas::Range r) override { return inner->get(k, r); }
    std::optional<DB::Cas::GetStreamResult> getStream(const String & k, DB::Cas::Range r) override { return inner->getStream(k, r); }
    DB::Cas::HeadResult head(const String & k) override { return inner->head(k); }
    DB::Cas::ListPage list(const String & p, const String & c, size_t l) override { return inner->list(p, c, l); }
    DB::Cas::PutResult putIfAbsent(const String & k, const String & b, const DB::Cas::ObjectMeta & m) override { note(k); return inner->putIfAbsent(k, b, m); }
    DB::Cas::WriteSinkPtr putIfAbsentStream(const String & k, const DB::Cas::ObjectMeta & m) override { note(k); return inner->putIfAbsentStream(k, m); }
    DB::Cas::PutResult putOverwrite(const String & k, const String & b, const DB::Cas::Token & e, const DB::Cas::ObjectMeta & m) override { note(k); return inner->putOverwrite(k, b, e, m); }
    DB::Cas::CasResult casPut(const String & k, const String & b, const std::optional<DB::Cas::Token> & e, const DB::Cas::ObjectMeta & m) override { note(k); return inner->casPut(k, b, e, m); }
    DB::Cas::DeleteOutcome deleteExact(const String & k, const DB::Cas::Token & t) override { note(k); return inner->deleteExact(k, t); }
    bool supportsListTokens() const override { return inner->supportsListTokens(); }
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
/// A backend whose checkConditionalWriteSingleAttemptSupport ALWAYS throws — a stand-in for a
/// Native-mode backend with no working single-attempt client (see
/// ObjectStorageBackend::checkConditionalWriteSingleAttemptSupport). Pins that skip_access_check does
/// NOT bypass this gate: the regression this guards is reverting Pool::open's skip_access_check
/// branch back to the naive "wrap the whole probe" shape, which would silently skip this check too.
class ThrowingSingleAttemptBackend final : public DB::Cas::Backend
{
public:
    explicit ThrowingSingleAttemptBackend(std::shared_ptr<DB::Cas::Backend> inner_) : inner(std::move(inner_)) {}

    std::optional<DB::Cas::GetResult> get(const String & k, DB::Cas::Range r) override { return inner->get(k, r); }
    std::optional<DB::Cas::GetStreamResult> getStream(const String & k, DB::Cas::Range r) override { return inner->getStream(k, r); }
    DB::Cas::HeadResult head(const String & k) override { return inner->head(k); }
    DB::Cas::ListPage list(const String & p, const String & c, size_t l) override { return inner->list(p, c, l); }
    DB::Cas::PutResult putIfAbsent(const String & k, const String & b, const DB::Cas::ObjectMeta & m) override { return inner->putIfAbsent(k, b, m); }
    DB::Cas::WriteSinkPtr putIfAbsentStream(const String & k, const DB::Cas::ObjectMeta & m) override { return inner->putIfAbsentStream(k, m); }
    DB::Cas::PutResult putOverwrite(const String & k, const String & b, const DB::Cas::Token & e, const DB::Cas::ObjectMeta & m) override { return inner->putOverwrite(k, b, e, m); }
    DB::Cas::CasResult casPut(const String & k, const String & b, const std::optional<DB::Cas::Token> & e, const DB::Cas::ObjectMeta & m) override { return inner->casPut(k, b, e, m); }
    DB::Cas::DeleteOutcome deleteExact(const String & k, const DB::Cas::Token & t) override { return inner->deleteExact(k, t); }
    bool supportsListTokens() const override { return inner->supportsListTokens(); }
    void checkConditionalWriteSingleAttemptSupport() override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "test: no single-attempt client");
    }
private:
    std::shared_ptr<DB::Cas::Backend> inner;
};
}

TEST(CASPool, SkipAccessCheckStillEnforcesSingleAttemptGate)
{
    auto backend = std::make_shared<ThrowingSingleAttemptBackend>(std::make_shared<DB::Cas::InMemoryBackend>());

    DB::Cas::PoolConfig cfg;
    cfg.pool_prefix = "pool";
    cfg.server_id = DB::UInt128(1);
    cfg.server_root_id = "test";
    cfg.skip_access_check = true;

    /// skip_access_check must NOT bypass checkConditionalWriteSingleAttemptSupport (RFC
    /// cas-s3-timeout-retry-control): a writable open still refuses to mount on a backend that cannot
    /// prove single-attempt conditional-write support, exactly as it does without skip_access_check.
    EXPECT_THROW(DB::Cas::Pool::open(backend, cfg), DB::Exception);
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
    PoolMeta created = PoolMeta::createOrValidate(*b, layout, /*blob_header_len*/ 256,
        BlobHashAlgo::CityHash128, /*allow_new*/ false, /*allow_mint*/ true);
    EXPECT_NE(created.pool_id, UInt128{});
    PoolMeta reopened = PoolMeta::createOrValidate(*b, layout, /*blob_header_len*/ 512);
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
    b2->putIfAbsent(layout.poolMetaKey(), "garbage");
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { PoolMeta::createOrValidate(*b2, layout, 256); });
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
        [&] { PoolMeta::createOrValidate(*b, layout, 250); });
    /// below the v3 envelope floor (240) but 8-aligned: rejected by the floor, not the alignment rule.
    /// Without the raised floor this pool would pass creation and LOGICAL_ERROR on the first blob write.
    expectThrowsCode(DB::ErrorCodes::BAD_ARGUMENTS,
        [&] { PoolMeta::createOrValidate(*b, layout, 128); });
    /// well below the floor
    expectThrowsCode(DB::ErrorCodes::BAD_ARGUMENTS,
        [&] { PoolMeta::createOrValidate(*b, layout, 64); });
    /// above the 16 KiB ceiling
    expectThrowsCode(DB::ErrorCodes::BAD_ARGUMENTS,
        [&] { PoolMeta::createOrValidate(*b, layout, 17 * 1024); });

    /// A creation that fails config validation must not have written anything.
    EXPECT_FALSE(b->get(layout.poolMetaKey()).has_value());
}

TEST(CASPoolMeta, RejectsBadConstantsOnDecode)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout layout("p");
    /// Encode a PoolMeta with blob_header_len=100 (not 8-aligned); decode must reject it as CORRUPTED_DATA.
    PoolMeta bad_pm;
    bad_pm.pool_id = hexToU128("00000000000000000000000000000001");
    bad_pm.blob_header_len = 100;   /// violates 8-alignment invariant
    b->putIfAbsent(layout.poolMetaKey(), encodePoolMeta(bad_pm));
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { PoolMeta::createOrValidate(*b, layout, 256); });
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
    b->putIfAbsent(layout.poolMetaKey(), encodePoolMeta(foreign_pm));

    PoolMeta result = PoolMeta::createOrValidate(*b, layout, /*blob_header_len*/ 512);
    EXPECT_EQ(result.pool_id, foreign);
    EXPECT_EQ(result.blob_header_len, 256u);     /// the foreign pool's constants win
}

TEST(CASPoolMeta, CasConflictReReadsWinner)
{
    /// The subtlest branch: the initial GET sees ABSENT, so createOrValidate proceeds to the
    /// create-if-absent casPut — and loses, because a racing creator committed in between. The loser
    /// must then re-read and return the WINNER's pool identity, not LOGICAL_ERROR. A single-threaded
    /// `failNextCasPut` alone cannot exercise this: it returns Conflict without leaving the object
    /// readable, so the re-read would fire the LOGICAL_ERROR guard. We model the real interleaving
    /// with a backend whose casPut commits the winner's object (via the public putIfAbsent) and THEN
    /// reports Conflict — exactly what the loser observes.
    class RacingBackend : public InMemoryBackend
    {
    public:
        String winner_bytes;
        CasResult casPut(const String & key, const String & bytes,
            const std::optional<Token> & expected, const ObjectMeta & meta) override
        {
            if (!winner_committed)
            {
                winner_committed = true;
                /// The winner lands first; our create-if-absent now necessarily conflicts.
                putIfAbsent(key, winner_bytes);
                return {CasOutcome::Conflict, {}};
            }
            return InMemoryBackend::casPut(key, bytes, expected, meta);
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
    PoolMeta result = PoolMeta::createOrValidate(*b, layout, /*blob_header_len*/ 512,
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
    build->putBlob(idOf(payload), BlobSource::fromString(payload));

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

    auto bytes = b->get(loc.key, Range{loc.offset, loc.length});
    ASSERT_TRUE(bytes.has_value());
    EXPECT_EQ(bytes->bytes, payload);               /// ranged read, no header touch

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
        b->putIfAbsent(layout.manifestKey(addressed), encodePartManifest(body));

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
        b->putIfAbsent(layout.manifestKey(addressed), encodePartManifest(body));

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

/// The Phase 1c manifest decode cache is keyed by (ManifestId, Token). Resolve+read the same ref twice:
/// the second readManifest must be served from the cache (no second GET of the body). A fresh publish
/// under a DIFFERENT ref name mints a NEW ManifestId (and a new shard token), so the cache misses and
/// the body is fetched again. A CountingBackend asserts the body GET count.
TEST(CASPool, ManifestCacheIsKeyedByIdAndToken)
{
    auto b = std::make_shared<DB::Cas::tests::CountingBackend>();
    auto s = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    const RootNamespace ns{"srv1/tbl"};
    Layout layout("p");

    const ManifestId id1 = publishPart(s, ns.string(), "part_1", "payload-1");
    const String key1 = layout.manifestKey(id1);

    /// First read: a body GET populates the (id1, token) cache entry.
    {
        auto r = s->resolveRef(ns, "part_1");
        ASSERT_TRUE(r.has_value());
        auto m = s->readManifest(r->manifest_id);
        ASSERT_EQ(m.entries.size(), 1u);
    }
    const uint64_t gets_after_first = b->getCount(key1);
    ASSERT_GE(gets_after_first, 1u);               /// the first read DID fetch the body

    /// Second read of the SAME id: the (id, token) cache must serve it — NO additional body GET.
    {
        auto r = s->resolveRef(ns, "part_1");
        ASSERT_TRUE(r.has_value());
        EXPECT_EQ(r->manifest_id, id1);
        auto m = s->readManifest(r->manifest_id);
        ASSERT_EQ(m.entries.size(), 1u);
    }
    EXPECT_EQ(b->getCount(key1), gets_after_first)
        << "second readManifest re-GET the body for the same (ManifestId, Token) — cache miss";

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
    b->putIfAbsent(layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "garbage", "not-a-ref-object");

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
        b->putIfAbsent(layout.manifestKey(id), "not a valid manifest body");
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
    DB::Cas::tests::publishCommittedTransition(*b, s->layout(), RootNamespace{"shadow/bk1/tbl"},
        "ref1", std::nullopt, DB::Cas::ManifestRef{.writer_epoch = 1, .build_sequence = 1, .manifest_ordinal = 1});
    DB::Cas::tests::publishCommittedTransition(*b, s->layout(), RootNamespace{"shadow/bk2/tbl"},
        "ref1", std::nullopt, DB::Cas::ManifestRef{.writer_epoch = 1, .build_sequence = 1, .manifest_ordinal = 1});

    const auto all = s->listNamespaces("").namespaces;
    EXPECT_EQ(all.size(), 3u);
    const auto shadows = s->listNamespaces("shadow/").namespaces;
    ASSERT_EQ(shadows.size(), 2u);
    /// listNamespaces returns results from an unordered_set; sort for deterministic comparison.
    auto sorted_shadows = shadows;
    std::sort(sorted_shadows.begin(), sorted_shadows.end());
    EXPECT_EQ(sorted_shadows[0], "shadow/bk1/tbl");
    EXPECT_EQ(sorted_shadows[1], "shadow/bk2/tbl");
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
    ASSERT_EQ(b->putIfAbsent(s->layout().namespaceFileKey(other, "format_version.txt"), "1\n").outcome,
              PutOutcome::Done);

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
    ASSERT_EQ(b->putIfAbsent(lifeless_ref, "garbage").outcome, PutOutcome::Done);
    ASSERT_EQ(b->putIfAbsent(lifeless_file, "garbage").outcome, PutOutcome::Done);

    NamespaceListing listing;
    ASSERT_NO_THROW(listing = s->listNamespaces(""))
        << "one un-attributable key must not abort the enumeration for every consumer of it";

    /// The healthy namespace is still listed -- attribution is per key, so a namespace disappears only
    /// when every key that would name it is unattributable.
    ASSERT_EQ(listing.namespaces.size(), 1u);
    EXPECT_EQ(listing.namespaces[0], ns.string());

    EXPECT_TRUE(listing.skipped.empty());
    EXPECT_TRUE(b->head(lifeless_ref).exists);
    EXPECT_TRUE(b->head(lifeless_file).exists);
}

TEST(CASPool, ListMirroredChildren)
{
    using namespace DB::Cas;
    auto b = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    /// Seed two catalog-authoritative shadow archives; physical files alone carry no logical path.
    DB::Cas::tests::fixture::admitLive(*b, store->layout(), RootNamespace{"shadow/bk1/store/3f2/3f2a-uuid@cas@"});
    DB::Cas::tests::fixture::admitLive(*b, store->layout(), RootNamespace{"shadow/bk2/store/3f2/3f2a-uuid@cas@"});
    auto children = store->listMirroredChildren("shadow/");
    std::sort(children.begin(), children.end());
    ASSERT_EQ(children.size(), 2u);
    EXPECT_EQ(children[0], "bk1");
    EXPECT_EQ(children[1], "bk2");
}

namespace
{

/// Delegating backend that fences the mount slot IN PLACE the first time a `get` returns a present
/// body for the armed key — reproducing the S13 window: the GC's token-guarded fence-out lands
/// between the keeper adopt's GET and its CAS. The caller's subsequent token-guarded `putOverwrite`
/// then fails `PreconditionFailed`, the adopt re-reads, sees `gc_fenced`, and throws
/// `MountFencedException` — which `Pool::open`'s fence-recovery loop must turn into a fresh-epoch
/// retry rather than a permanent wedge (P3.1 vector C).
class FenceInAdoptWindowBackend final : public DB::Cas::Backend
{
public:
    explicit FenceInAdoptWindowBackend(std::shared_ptr<DB::Cas::Backend> inner_) : inner(std::move(inner_)) {}
    String fence_key;   /// empty = fault disarmed; set to the mount key to arm the one-shot fence

    std::optional<DB::Cas::GetResult> get(const String & k, DB::Cas::Range r) override
    {
        auto got = inner->get(k, r);
        if (!fence_key.empty() && k == fence_key && got.has_value())
        {
            /// One-shot: fence the slot in place exactly as `computeHeartbeatFloor` does (preserve the
            /// body, gc_fenced = true, seq + 1, token-guarded against the value we just read), then
            /// disarm so the retry can adopt cleanly.
            DB::Cas::MountLease fenced = DB::Cas::decodeMountLease(got->bytes);
            fenced.gc_fenced = true;
            fenced.seq += 1;
            inner->putOverwrite(k, DB::Cas::encodeMountLease(fenced), got->token);
            fence_key.clear();
        }
        return got;
    }
    std::optional<DB::Cas::GetStreamResult> getStream(const String & k, DB::Cas::Range r) override { return inner->getStream(k, r); }
    DB::Cas::HeadResult head(const String & k) override { return inner->head(k); }
    DB::Cas::ListPage list(const String & p, const String & c, size_t l) override { return inner->list(p, c, l); }
    DB::Cas::PutResult putIfAbsent(const String & k, const String & b, const DB::Cas::ObjectMeta & m) override { return inner->putIfAbsent(k, b, m); }
    DB::Cas::WriteSinkPtr putIfAbsentStream(const String & k, const DB::Cas::ObjectMeta & m) override { return inner->putIfAbsentStream(k, m); }
    DB::Cas::PutResult putOverwrite(const String & k, const String & b, const DB::Cas::Token & e, const DB::Cas::ObjectMeta & m) override { return inner->putOverwrite(k, b, e, m); }
    DB::Cas::CasResult casPut(const String & k, const String & b, const std::optional<DB::Cas::Token> & e, const DB::Cas::ObjectMeta & m) override { return inner->casPut(k, b, e, m); }
    DB::Cas::DeleteOutcome deleteExact(const String & k, const DB::Cas::Token & t) override { return inner->deleteExact(k, t); }
    bool supportsListTokens() const override { return inner->supportsListTokens(); }

private:
    std::shared_ptr<DB::Cas::Backend> inner;
};

}

TEST(CASPoolMountFence, OpenRecoversFromFenceInAdoptWindowWithFreshEpoch)
{
    auto inner = std::make_shared<InMemoryBackend>();
    auto fencing = std::make_shared<FenceInAdoptWindowBackend>(inner);
    /// Arm the one-shot fence on the mount slot. Pool::open first claims the mount (fresh mint), then
    /// the keeper adopts it — the adopt's GET trips the fence, its CAS fails, and open must recover.
    const DB::Cas::Layout layout("p");
    fencing->fence_key = layout.mountKey("test");

    /// The retry that recovers from the fence reclaims a same-uuid, different-epoch, `gc_fenced` body
    /// -> `MountPriorState::Fenced` (a fenced prior is reclaimed on the first attempt, with no
    /// observation polling -- see `CASMountOpenWaits.FencedPriorReclaimsWithoutAnyWait`). The injected
    /// `boot_ms_fn`/`wait_sleep_fn` below keep this test off the real clock regardless.
    uint64_t fake_boot = 0;
    DB::Cas::PoolPtr store;
    ASSERT_NO_THROW(
        store = DB::Cas::Pool::open(fencing,
            DB::Cas::PoolConfig{.pool_prefix = "p", .server_root_id = "test",
                .boot_ms_fn = [&fake_boot] { return fake_boot; },
                .wait_sleep_fn = [&fake_boot](uint64_t ms) { fake_boot += ms; }}))
        << "open must recover from a fence in the adopt window, not wedge (exit-49 S13 bug)";
    ASSERT_TRUE(store);

    /// The final live lease is unfenced and at a HIGHER writer_epoch than the first attempt (a fence
    /// costs an epoch): the first claim took epoch 1, got fenced, the retry took epoch 2 and mounted.
    const auto got = inner->get(layout.mountKey("test"));
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
    uint64_t fake_boot = 1'000'000;   /// arbitrary boottime origin (ms)
    auto store = DB::Cas::Pool::open(backend, DB::Cas::PoolConfig{
        .pool_prefix = "p",
        .server_root_id = "test",
        .mount_lease_ttl_ms = std::chrono::milliseconds(30000),
        .boot_ms_fn = [&] { return fake_boot; },
    });

    /// Freshly armed at open (deadline = fake_boot + ttl): well within the ttl, mutations are allowed.
    EXPECT_TRUE(store->mayMutate());

    /// Advance the boot clock just short of the deadline — still armed.
    fake_boot += 29999;
    EXPECT_TRUE(store->mayMutate());

    /// Cross the deadline (ttl elapsed with no renew — a resumed sleeper's view). The fence must expire.
    /// (The "a gated mutate then fails closed with ABORTED" leg used `mutateShardForTest` -- the held
    /// Phase-E shard lane -- and moves there; here we pin the boot-clock fence flip itself.)
    fake_boot += 2;   /// now fake_boot = origin + 30001 > origin + 30000
    EXPECT_FALSE(store->mayMutate());
}

/// ==== self-remount after GC fence-out (liveness counterpart of the fence-out safety rule) ====

namespace
{

/// GC's fence-out, applied directly: preserve the body, set gc_fenced, bump seq (token-guarded).
void fenceOutMount(DB::Cas::Backend & backend, const String & mount_key)
{
    const auto got = backend.get(mount_key);
    ASSERT_TRUE(got.has_value());
    MountLease m = decodeMountLease(got->bytes);
    m.gc_fenced = true;
    m.seq += 1;
    ASSERT_EQ(backend.putOverwrite(mount_key, encodeMountLease(m), got->token).outcome,
              DB::Cas::PutOutcome::Done);
}

}

TEST(CASPoolRemount, FenceOutThenSelfRemountRestoresWrites)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);
    const String mount_key = store->layout().mountKey("test");
    const uint64_t epoch_before = decodeMountLease(backend->get(mount_key)->bytes).writer_epoch;
    EXPECT_EQ(store->liveWriterEpoch(), epoch_before);

    fenceOutMount(*backend, mount_key);

    /// The keeper's next renewal fails closed (foreign touch — never re-mint).
    EXPECT_THROW(store->renewWatermarkOnce(), DB::Exception);

    /// Self-remount claims a FRESH incarnation: epoch bumped, gc_fenced cleared, writes restored.
    ASSERT_TRUE(store->tryRemountOnce());
    const MountLease after = decodeMountLease(backend->get(mount_key)->bytes);
    EXPECT_EQ(after.writer_epoch, epoch_before + 1);
    EXPECT_FALSE(after.gc_fenced);
    EXPECT_EQ(store->liveWriterEpoch(), epoch_before + 1);

    /// The renewal path works again (the new keeper owns the slot). (The follow-on "...and so does a
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

    /// A FRESH build under the live incarnation works.
    auto fresh = store->beginPartWrite({});
    EXPECT_NO_THROW(fresh->putBlob(DB::Cas::tests::idOf("y"), DB::Cas::BlobSource::fromString("y")));
}

TEST(CASPoolRemount, ForeignOwnerIsNeverTakenOver)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);
    const String mount_key = store->layout().mountKey("test");

    /// A genuinely foreign uuid holds the mount (live or not — foreign is terminal for the claim).
    const auto got = backend->get(mount_key);
    MountLease foreign = decodeMountLease(got->bytes);
    foreign.server_uuid = foreign.server_uuid + DB::UInt128(1);
    foreign.seq += 1;
    ASSERT_EQ(backend->putOverwrite(mount_key, encodeMountLease(foreign), got->token).outcome,
              DB::Cas::PutOutcome::Done);

    EXPECT_FALSE(store->tryRemountOnce());
    /// The foreign body is untouched (no takeover, ever).
    EXPECT_EQ(decodeMountLease(backend->get(mount_key)->bytes).server_uuid, foreign.server_uuid);

    /// Move the parent fixture to the production-recognized fenced terminal state before explicitly
    /// destroying its superseded keeper. The unfenced foreign-release guard is covered separately below.
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
    const auto foreign_got = foreign_backend->get(foreign_mount_key);
    ASSERT_TRUE(foreign_got.has_value());
    MountLease foreign_lease = decodeMountLease(foreign_got->bytes);
    foreign_lease.server_uuid = foreign_lease.server_uuid + DB::UInt128(1);
    foreign_lease.seq += 1;
    ASSERT_EQ(
        foreign_backend->putOverwrite(foreign_mount_key, encodeMountLease(foreign_lease), foreign_got->token).outcome,
        DB::Cas::PutOutcome::Done);
    const auto occupant_before = foreign_backend->get(foreign_mount_key);
    ASSERT_TRUE(occupant_before.has_value());

    EXPECT_FALSE(invalid_store->tryRemountOnce()) << "a foreign owner is never taken over at remount";

    const uint64_t violations_before
        = ProfileEvents::global_counters[ProfileEvents::CASMountExclusivityViolation].load();
    invalid_store.reset();   /// must not abort, must not terminate

    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASMountExclusivityViolation].load(),
              violations_before + 1)
        << "the release must report the broken single-writer guarantee rather than dying on it";
    const auto occupant_after = foreign_backend->get(foreign_mount_key);
    ASSERT_TRUE(occupant_after.has_value()) << "nor is it taken over at release";
    EXPECT_EQ(occupant_after->bytes, occupant_before->bytes)
        << "the slot must be left byte-for-byte as the foreign owner wrote it";
}

TEST(CASPoolRemount, ShutdownGuardRefusesToArmRemount)
{
    auto backend = std::make_shared<InMemoryBackend>();
    /// background_watermark = true so scheduleRemount actually arms a recovery thread in production mode
    /// (the same gate every background thread checks).
    auto store = DB::Cas::Pool::open(backend,
        DB::Cas::PoolConfig{.pool_prefix = "p", .server_root_id = "test", .background_watermark = true});

    /// Teardown has begun: ~Pool() latches this at its very top, BEFORE its only remount-thread join.
    store->beginShutdownForTest();

    /// A lease-renewal failure firing DURING teardown re-enters scheduleRemount (the keeper's on_lost
    /// callback). With the guard it must refuse to spawn; without it, it arms remount_thread AFTER
    /// ~Pool()'s join — the leftover joinable ThreadFromGlobalPool handle then abort()s the process at
    /// member destruction (std::terminate). Reading joinable() immediately after the synchronous call is
    /// race-free: the armed thread never touches the handle.
    EXPECT_FALSE(store->scheduleRemountForTest())
        << "scheduleRemount must not arm a recovery thread once teardown has begun";
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
/// claim attempt's pre-I/O instant (`remount_anchor_boot_ms`, captured right after `installKeeper`
/// and right before `keeperStart()` in `Pool::tryRemountOnce`), never at a later reading taken after
/// `keeperStart`/`quiesceRefTablesForRemount` have already run.
///
/// The two `bootMsNow()` calls of interest, in the ORDER each code version issues them:
///   - FIXED code: call #1 = the new anchor (`remount_anchor_boot_ms`, before `keeperStart`);
///     call #2 = `MountLeaseKeeper::prepareRenew`'s own internal boot read inside `keeperStart`'s
///     `doStart` (feeds only the keeper's OWN internal `confirmed_deadline_ms` -- unrelated to the
///     Pool-level arm -- so its value is irrelevant to the arm post-fix).
///   - PRE-FIX code (no anchor line): call #1 = that SAME `prepareRenew` read (now the first boot
///     call of the attempt, since nothing reads the clock before `keeperStart`); call #2 = the
///     arm-site's own `mount_runtime.bootMsNow()`, read AFTER `keeperStart` returns -- the stale,
///     response-time reading this whole fix exists to stop using.
/// A sequenced clock returning 10000 then 999999 (an inflated, much-later reading) therefore arms
/// the FIXED code from 10000 and the PRE-FIX code from 999999, regardless of which call site reads
/// which value -- letting a single deterministic probe (`mayMutate()` at boot == 10000+ttl) tell
/// them apart with no sleep and no thread. (TDD evidence for both branches is recorded in the task-5
/// report, not re-asserted here: this test body only encodes the FIXED expectation.)
TEST(CASPoolRemount, RemountArmAnchorsAtClaimAttemptNotResponseTime)
{
    SequencedBootClock clock;
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = DB::Cas::Pool::open(backend, DB::Cas::PoolConfig{
        .pool_prefix = "p", .server_root_id = "test",
        .mount_lease_ttl_ms = std::chrono::milliseconds(30'000),
        .boot_ms_fn = [&] { return clock(); },
    });
    ASSERT_TRUE(store);

    /// Trip the fence exactly as every other remount test in this file does.
    fenceOutMount(*backend, store->layout().mountKey("test"));

    /// Arm the sequence for the upcoming remount attempt: the initial `open` above already drained
    /// an unrelated number of `bootMsNow()` calls (all served from `.steady = 0` -- irrelevant, since
    /// nothing probes the resulting arm before this point). Reset the counter so the FIRST call from
    /// here on is the remount attempt's own call #1.
    clock.queue = {10000, 999999};
    clock.next = 0;

    ASSERT_TRUE(store->tryRemountOnce());

    /// Probe at boot == anchor + ttl (10000 + 30000 = 40000): the fixed code armed from the anchor
    /// (10000), so the fence has JUST expired here -- `mayMutate` must be false. (The pre-fix code
    /// would still read `mayMutate` as true here, armed from 999999 + 30000 -- see the TDD run in the
    /// report.)
    clock.steady = 40000;
    EXPECT_FALSE(store->mayMutate())
        << "the remount arm must anchor at the claim attempt's pre-I/O instant, not a later "
           "response-time reading taken after keeperStart/quiesceRefTablesForRemount";
}

/// ==== rev.6 Task 5: clean-release drain gates the farewell marker ====

namespace
{
/// Forces the FIRST `putIfAbsent` whose key contains `fault_key_substr` to throw an ambiguous
/// (Unresolved-classified) exception, `fault_count` times -- the minimal one-shot subset of
/// `RefWriterTestBackend`'s fault injection (gtest_cas_ref_writer.cpp) this file's shutdown test needs
/// to drive a ref-log append into the `Unresolved`/wedge outcome, with `max_attempts = 1` in the budget
/// so the single failed attempt exhausts the retry budget immediately.
class UnresolvedPutBackend final : public DB::Cas::tests::CountingBackend
{
public:
    String fault_key_substr;
    int fault_count = 0;

    DB::Cas::PutResult putIfAbsent(const String & key, const String & bytes, const DB::Cas::ObjectMeta & meta) override
    {
        if (fault_count > 0 && !fault_key_substr.empty() && key.find(fault_key_substr) != String::npos)
        {
            --fault_count;
            throw Poco::TimeoutException("UnresolvedPutBackend: simulated ambiguous result (response lost)");
        }
        return DB::Cas::tests::CountingBackend::putIfAbsent(key, bytes, meta);
    }
};
}

TEST(CASPoolShutdown, CleanStopDrainsAndWritesFarewell)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = DB::Cas::Pool::open(backend, DB::Cas::PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    publishPart(store, "srv/clean_stop", "x", "payload");

    const String mount_key = store->layout().mountKey("test");
    store.reset();   /// drives ~Pool(): with no in-flight ref-log PUT, the drain must succeed.

    const auto got = backend->get(mount_key);
    ASSERT_TRUE(got.has_value());
    const MountLease lease = decodeMountLease(got->bytes);
    EXPECT_EQ(lease.min_active, std::numeric_limits<uint64_t>::max())
        << "a clean drain (no in-flight ref-log PUT) must write the farewell marker";
}

TEST(CASPoolShutdown, UnresolvedWedgeSkipsFarewell)
{
    CasRequestBudget budget;
    budget.max_attempts = 1;
    budget.attempt_timeout_ms = 100;
    budget.operation_deadline_ms = 5000;   /// strictly above attempt_timeout_ms: equality is a wall-clock race (validateCasRequestBudget)
    budget.lease_safety_margin_ms = 100;

    auto backend = std::make_shared<UnresolvedPutBackend>();
    auto store = DB::Cas::Pool::open(backend, DB::Cas::PoolConfig{
        .pool_prefix = "p", .server_root_id = "test", .cas_request_budget = budget});
    /// By value: `layout` is used after `store.reset()` below, a reference would dangle.
    const Layout layout = store->layout();
    const RootNamespace ns{"srv/wedge_shutdown"};
    /// Stage B (Task 4-C): pin `ns` to the Stage-A sentinel BEFORE its first real touch, so the fault
    /// injected below (computed from that same sentinel) lands on the key production actually writes
    /// to -- otherwise the real append mints an unrelated random incarnation and the fault misses.
    DB::Cas::tests::casAdmitRecoverableEntry(*backend, layout, ns, store->liveWriterEpoch());
    publishPart(store, ns.string(), "x", "payload");

    /// Force the ref-log append the drop below performs into the Unresolved/wedge outcome (as in the
    /// wedge tests in gtest_cas_ref_writer.cpp): the single attempt the budget allows fails ambiguously.
    backend->fault_key_substr = layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_log/";
    backend->fault_count = 1;
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropRef(ns, "x"); });
    ASSERT_TRUE(store->refLaneWedgedForTest(ns));

    const String mount_key = store->layout().mountKey("test");
    store.reset();   /// drives ~Pool(): the still-wedged lane must skip the farewell marker.

    const auto got = backend->get(mount_key);
    ASSERT_TRUE(got.has_value());
    const MountLease lease = decodeMountLease(got->bytes);
    EXPECT_NE(lease.min_active, std::numeric_limits<uint64_t>::max())
        << "an unresolved ref-log PUT must skip the clean-release farewell marker";
    EXPECT_FALSE(lease.gc_fenced);

    /// A successor claimMount on this body must return LiveDoubleStart (unclean path): no certificate of
    /// death (not fenced, not the clean farewell marker, no proven-dead observation) justifies a
    /// same-uuid, different-epoch reclaim.
    const MountClaimResult claim = claimMount(*backend, layout, "test", lease.server_uuid,
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
    /// Predecessor: claim epoch 7, no farewell (simulate crash: just drop the keeper) -- a bare
    /// `claimMount` plants the lease directly, with no clean-farewell `min_active` marker and no
    /// `gc_fenced`, so the successor below has no certificate of death until it observes one itself.
    ASSERT_EQ(claimMount(*b, l, "test", UInt128(1), /*epoch*/ 7, /*now_ms*/ 1000, /*ttl_ms*/ 500).kind,
              MountClaimResult::Claimed);
    /// A real predecessor at epoch 7 durably minted it first (`allocateWriterEpoch` always runs
    /// before the mount claim); seed that durable epoch object here too, or the successor's own
    /// `allocateWriterEpoch` trips the Phase C guard (epoch absent, mount present -> fail closed).
    b->putIfAbsent(l.epochKey("test"), encodeServerEpoch(ServerEpoch{.next_writer_epoch = 8}));

    /// A 500ms lease TTL is far below the default `cas_request_budget` (RFC
    /// cas-s3-timeout-retry-control §required-timeout-model requires attempt_timeout + safety_margin <
    /// lease TTL), so scale the budget down to fit -- mirrors `CasMountStartup::StaleSelfMountReclaimedAfterWait`.
    const CasRequestBudget tiny_budget{
        .attempt_timeout_ms = 50, .operation_deadline_ms = 500, .max_attempts = 1, .lease_safety_margin_ms = 50};

    uint64_t fake_boot = 0;
    std::vector<uint64_t> waits;
    PoolPtr store;
    ASSERT_NO_THROW(
        store = Pool::open(b, PoolConfig{
            .pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "test",
            .mount_lease_ttl_ms = std::chrono::milliseconds(500),
            .mount_renew_period = std::chrono::milliseconds(100),
            .cas_request_budget = tiny_budget,
            .boot_ms_fn = [&] { return fake_boot; },
            .wait_sleep_fn = [&](uint64_t ms) { fake_boot += ms; waits.push_back(ms); },
        }));
    ASSERT_TRUE(store);

    /// The token-stability observation window (>= the 500ms ttl) is paid, because this predecessor's
    /// death was never certified -- only observed.
    uint64_t total = 0;
    for (uint64_t w : waits)
        total += w;
    EXPECT_GE(total, 500u) << "the observation window must have been paid";
    /// And NOTHING is paid on top of it. Every recorded wait is a poll of that window, bounded by the
    /// lease TTL; a wait longer than the whole window can only be a reintroduced grace period.
    for (uint64_t w : waits)
        EXPECT_LE(w, 500u)
            << "an unclean reclaim must not block on any wait beyond the observation poll -- the "
               "straggler it used to wait out is fenced by the recovery seal instead";
}

TEST(CASMountOpenWaits, CleanOpenSkipsAllWaits)
{
    auto b = std::make_shared<InMemoryBackend>();
    /// Predecessor released cleanly (drain + farewell from Task 5): open, then reset() drives ~Pool(),
    /// which -- with nothing in flight -- writes the farewell marker (min_active == UINT64_MAX).
    auto predecessor = Pool::open(b, PoolConfig{
        .pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "test"});
    predecessor.reset();

    std::vector<uint64_t> waits;
    PoolPtr successor;
    ASSERT_NO_THROW(
        successor = Pool::open(b, PoolConfig{
            .pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "test",
            .wait_sleep_fn = [&](uint64_t ms) { waits.push_back(ms); },
        }));
    ASSERT_TRUE(successor);

    EXPECT_TRUE(waits.empty())
        << "a clean farewell (Task 5) needs no observation window";
}

TEST(CASMountOpenWaits, FencedPriorReclaimsWithoutAnyWait)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l{"p"};
    DB::Cas::tests::seedPoolMetaForRestart(*b);
    ASSERT_EQ(claimMount(*b, l, "test", UInt128(1), /*epoch*/ 7, /*now_ms*/ 1000, /*ttl_ms*/ 500).kind,
              MountClaimResult::Claimed);
    /// A real predecessor at epoch 7 durably minted it first (`allocateWriterEpoch` always runs
    /// before the mount claim); seed that durable epoch object here too, or the successor's own
    /// `allocateWriterEpoch` trips the Phase C guard (epoch absent, mount present -> fail closed).
    b->putIfAbsent(l.epochKey("test"), encodeServerEpoch(ServerEpoch{.next_writer_epoch = 8}));
    /// Predecessor lease carries gc_fenced=true: fence it directly, exactly as `computeHeartbeatFloor`'s
    /// fence-out does (preserve the body, gc_fenced = true, seq + 1, token-guarded).
    fenceOutMount(*b, l.mountKey("test"));

    /// See UncleanOpenPaysOnlyTheObservationWindow above: a 500ms TTL needs a scaled-down budget too.
    const CasRequestBudget tiny_budget{
        .attempt_timeout_ms = 50, .operation_deadline_ms = 500, .max_attempts = 1, .lease_safety_margin_ms = 50};

    std::vector<uint64_t> waits;
    PoolPtr store;
    ASSERT_NO_THROW(
        store = Pool::open(b, PoolConfig{
            .pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "test",
            .mount_lease_ttl_ms = std::chrono::milliseconds(500),
            .cas_request_budget = tiny_budget,
            .wait_sleep_fn = [&](uint64_t ms) { waits.push_back(ms); },
        }));
    ASSERT_TRUE(store);

    /// A GC-fenced prior is a terminal, already-threshold-gated certificate of death -- reclaimed on the
    /// FIRST attempt, with no observation polling. It is also an UNCLEAN prior, which used to mean it
    /// paid the materialization grace; nothing is owed now, so this open blocks on nothing at all.
    EXPECT_TRUE(waits.empty())
        << "a certified-dead predecessor needs neither the observation window nor any grace period";
}

namespace
{
/// Stalls the CLAIM ITSELF past the lease TTL, and counts what the open writes afterwards.
///
/// The mount key is written twice before the write fence arms: once by `claimMount`'s reclaim, then
/// once by the keeper's adopt -- and the fence's anchor is taken BETWEEN them. So advancing the
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

    DB::Cas::PutResult putOverwrite(const String & k, const String & b, const DB::Cas::Token & e,
                                    const DB::Cas::ObjectMeta & m) override
    {
        if (k == mount_key)
        {
            const int n = ++mount_writes;
            if (n == 2 && on_second_mount_write)
                on_second_mount_write();
            else if (n > 2)
                ++mount_writes_after_stall;
        }
        return InMemoryBackend::putOverwrite(k, b, e, m);
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
        backend->putIfAbsent(layout.mountKey(srid), DB::Cas::encodeMountLease(prior));
    }
    /// A real predecessor at epoch 7 durably minted it first (`allocateWriterEpoch` always runs
    /// before the mount claim); seed that durable epoch object here too, or `Pool::open`'s own
    /// `allocateWriterEpoch` trips the Phase C guard (epoch absent, mount present -> fail closed).
    backend->putIfAbsent(layout.epochKey(srid), DB::Cas::encodeServerEpoch(DB::Cas::ServerEpoch{.next_writer_epoch = 8}));
    uint64_t fake_boot_ms = 10'000;
    DB::Cas::PoolConfig cfg;
    cfg.pool_prefix = "pool";
    cfg.server_id = uuid;
    cfg.server_root_id = srid;
    cfg.mount_lease_ttl_ms = std::chrono::milliseconds(30'000);
    cfg.boot_ms_fn = [&] { return fake_boot_ms; };
    /// The keeper's adopt write stalls for 40 s of boot clock -- past the 30 s TTL the anchor a few
    /// microseconds earlier was taken against.
    backend->on_second_mount_write = [&] { fake_boot_ms += 40'000; };

    auto store = DB::Cas::Pool::open(backend, cfg);
    ASSERT_NE(store, nullptr);

    ASSERT_EQ(backend->mount_writes.load(), 3)
        << "the fixture assumes exactly two mount writes before the redo (the reclaim and the keeper's "
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
    uint64_t fake_boot = 1'000'000;
    std::vector<uint64_t> waits;
    auto store = Pool::open(backend, PoolConfig{
        .pool_prefix = "p", .server_root_id = "test",
        .mount_lease_ttl_ms = std::chrono::milliseconds(30000),
        .boot_ms_fn = [&] { return fake_boot; },
        .wait_sleep_fn = [&](uint64_t ms) { fake_boot += ms; waits.push_back(ms); },
    });
    ASSERT_TRUE(store);
    EXPECT_TRUE(waits.empty()) << "a fresh mount (no predecessor) pays no wait at open";

    /// Trip the fence: advance the local boot clock past the deadline (as in `WriteFenceUsesInjectedBootClock`
    /// above) and mark the durable lease `gc_fenced` (the certificate `claimMountAwaitingExpiry` reclaims
    /// on its FIRST attempt, no observation polling -- avoids a real sleep in this test).
    fake_boot += 30001;
    fenceOutMount(*backend, store->layout().mountKey("test"));

    /// No in-flight ref-log PUT at all -- the easy direction.
    ASSERT_TRUE(store->tryRemountOnce());

    EXPECT_TRUE(waits.empty())
        << "a drained self-remount must pay no wait";
}

TEST(CASRemountWaits, UnresolvedWedgeRemountPaysNoWaitEither)
{
    CasRequestBudget budget;
    budget.max_attempts = 1;
    budget.attempt_timeout_ms = 100;
    budget.operation_deadline_ms = 5000;   /// strictly above attempt_timeout_ms: equality is a wall-clock race (validateCasRequestBudget)
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
    EXPECT_TRUE(waits.empty()) << "a fresh mount (no predecessor) pays no wait at open";

    const Layout & layout = store->layout();
    const RootNamespace ns{"srv/remount_wedge"};
    /// Stage B (Task 4-C): see `CASPoolShutdown.UnresolvedWedgeSkipsFarewell`'s identical comment.
    DB::Cas::tests::casAdmitRecoverableEntry(*backend, layout, ns, store->liveWriterEpoch());
    publishPart(store, ns.string(), "x", "payload");

    /// Force the ref-log append `dropRef` below performs into the Unresolved/wedge outcome (as in
    /// `CASPoolShutdown.UnresolvedWedgeSkipsFarewell`): the single attempt the budget allows fails
    /// ambiguously.
    backend->fault_key_substr = layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_log/";
    backend->fault_count = 1;
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropRef(ns, "x"); });
    ASSERT_TRUE(store->refLaneWedgedForTest(ns));

    /// Trip the fence exactly as in `DrainedRemountSkipsGrace` above.
    fake_boot += 30001;
    fenceOutMount(*backend, store->layout().mountKey("test"));

    /// THE HARD DIRECTION, and the one the retired wait existed for: a ref lane that still holds an
    /// UNDECIDED conditional PUT when the fence trips. It used to buy a 30 s grace. It buys nothing now
    /// -- the remount proceeds straight through, and the undecided PUT is decided by the seal the next
    /// recovery writes into its slot.
    ASSERT_TRUE(store->tryRemountOnce());

    EXPECT_TRUE(waits.empty())
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
    budget.max_attempts = 1;
    budget.attempt_timeout_ms = 100;
    budget.operation_deadline_ms = 5000;   /// strictly above attempt_timeout_ms: equality is a wall-clock race (validateCasRequestBudget)
    budget.lease_safety_margin_ms = 100;

    auto backend = std::make_shared<UnresolvedPutBackend>();
    uint64_t fake_boot = 1'000'000;
    auto store = Pool::open(backend, PoolConfig{
        .pool_prefix = "p", .server_root_id = "test",
        .mount_lease_ttl_ms = std::chrono::milliseconds(30000),
        .cas_request_budget = budget,
        .boot_ms_fn = [&] { return fake_boot; },
        .wait_sleep_fn = [&](uint64_t ms) { fake_boot += ms; },
    });
    ASSERT_TRUE(store);

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
    /// `PartWriteTxn::observeAndAdmit`'s newborn-debris watermark, unrelated to what this test is about.
    publishPart(store, ns2.string(), "y", "payload-b");

    /// Force ns1's ref-log append into the Unresolved/wedge outcome (mirrors
    /// `UnresolvedWedgeRemountPaysNoWaitEither` above).
    backend->fault_key_substr = layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns1)) + "_log/";
    backend->fault_count = 1;
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropRef(ns1, "x"); });
    ASSERT_TRUE(store->refLaneWedgedForTest(ns1));

    /// Self-remount #1: UNCLEAN (the wedge above). Epoch 1 -> 2.
    fake_boot += 30001;
    fenceOutMount(*backend, store->layout().mountKey("test"));
    ASSERT_TRUE(store->tryRemountOnce());
    ASSERT_EQ(store->liveWriterEpoch(), 2u);

    /// Self-remount #2: CLEAN (no wedge left behind -- `quiesceRefTablesForRemount` already cleared the
    /// cache). Epoch 2 -> 3.
    fake_boot += 30001;
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
    EXPECT_TRUE(backend->get(layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(ns2), RefTxnId{1, 2})).has_value())
        << "epoch 1 closes at the slot right after its last durable id, in-band";
    EXPECT_TRUE(backend->get(layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(ns2), RefTxnId{2, 1})).has_value())
        << "empty epoch 2 closes at its own sequence 1, chained to the epoch-1 seal";
    const RefTxnId retired_sentinel_id{2, std::numeric_limits<uint64_t>::max()};
    EXPECT_FALSE(backend->get(layout.refSnapshotKey(DB::Cas::tests::fixture::fixtureLife(ns2), retired_sentinel_id)).has_value())
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
    EXPECT_EQ(backend->headCount(manifest_key), 2u);        /// mandatory HEAD per call (unchanged)
    ASSERT_EQ(m1->entries.size(), 1u);
    EXPECT_EQ(m1->entries[0].path, "data.bin");
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
