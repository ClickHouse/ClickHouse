#include <gtest/gtest.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasBlobInDegree.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEvent.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasGcStateFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasInspect.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Common/ProfileEvents.h>
#include <Poco/Exception.h>
#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

namespace ProfileEvents
{
extern const Event CASGCRetiredCondemned;
extern const Event CASGCRetireReplaced;
extern const Event CASMountRenewalAttempts;
extern const Event CASMountRenewalRetries;
extern const Event CASMountRenewalResolved;
extern const Event CASMountRenewalRecovered;
extern const Event CASMountRenewalDeadlineExceeded;
}

using namespace DB::Cas;
using DB::Cas::tests::idOf;
using DB::Cas::tests::u128Of;
using DB::Cas::tests::currentRetiredSet;

namespace
{

PoolPtr openPool(std::shared_ptr<InMemoryBackend> & b)
{
    b = std::make_shared<InMemoryBackend>();
    return Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
}

class RenewalCounterBackend final : public InMemoryBackend
{
public:
    enum class Fault : uint8_t
    {
        None,
        ThrowBefore,
        LandThenThrow,
    };

    using InMemoryBackend::putOverwrite;

    Fault fault = Fault::None;

    PutResult putOverwrite(
        const String & key,
        const String & bytes,
        const Token & expected,
        const ObjectMeta & meta) override
    {
        const Fault current = std::exchange(fault, Fault::None);
        if (current == Fault::ThrowBefore)
            throw Poco::TimeoutException("injected renewal timeout before commit");

        PutResult result = InMemoryBackend::putOverwrite(key, bytes, expected, meta);
        if (current == Fault::LandThenThrow)
            throw Poco::TimeoutException("injected renewal response loss after commit");
        return result;
    }
};

CasRequestBudget renewalCounterBudget(uint32_t max_attempts = 2)
{
    return CasRequestBudget{
        .attempt_timeout_ms = 10,
        .operation_deadline_ms = 500,
        .max_attempts = max_attempts,
        .lease_safety_margin_ms = 20,
        .retry_initial_backoff_ms = 0,
        .retry_max_backoff_ms = 0,
    };
}

struct RenewalCounterSnapshot
{
    uint64_t attempts;
    uint64_t retries;
    uint64_t resolved;
    uint64_t recovered;
    uint64_t deadline_exceeded;
};

RenewalCounterSnapshot renewalCounters()
{
    using ProfileEvents::global_counters;
    return {
        .attempts = global_counters[ProfileEvents::CASMountRenewalAttempts].load(),
        .retries = global_counters[ProfileEvents::CASMountRenewalRetries].load(),
        .resolved = global_counters[ProfileEvents::CASMountRenewalResolved].load(),
        .recovered = global_counters[ProfileEvents::CASMountRenewalRecovered].load(),
        .deadline_exceeded = global_counters[ProfileEvents::CASMountRenewalDeadlineExceeded].load(),
    };
}

void expectRenewalCounterDelta(
    const RenewalCounterSnapshot & before,
    const RenewalCounterSnapshot & after,
    uint64_t attempts,
    uint64_t retries,
    uint64_t resolved,
    uint64_t recovered,
    uint64_t deadline_exceeded)
{
    EXPECT_EQ(after.attempts - before.attempts, attempts);
    EXPECT_EQ(after.retries - before.retries, retries);
    EXPECT_EQ(after.resolved - before.resolved, resolved);
    EXPECT_EQ(after.recovered - before.recovered, recovered);
    EXPECT_EQ(after.deadline_exceeded - before.deadline_exceeded, deadline_exceeded);
}

/// Publish ONE ref naming a single-blob part through the real writer sequence (mirrors
/// `publishOneBlobPart` in `gtest_cas_gc_leak.cpp`, duplicated here because that helper has internal
/// linkage in its own translation unit).
ManifestId publishOneBlobPart(
    const PoolPtr & s, const RootNamespace & ns, const String & ref, const String & payload)
{
    PartWriteInfo info;
    info.intended_ref = ns.string() + "/" + ref;
    auto build = s->beginPartWrite(info);
    DB::Cas::ManifestEntry e;
    e.path = "data.bin";
    e.placement = EntryPlacement::Blob;
    e.ref = DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(u128Of(payload))};

    e.blob_size = payload.size();
    /// Wiring order (EDGE-BEFORE-OBSERVE): stageManifest -> precommitAdd -> putBlob -> promote.
    const ManifestId id = build->stageManifest({e});
    build->precommitAdd(ns, ref, id);
    build->putBlob(idOf(payload), BlobSource::fromString(payload));
    build->promote(ns, ref, build->buildId(), id);
    return id;
}

TEST(CASObservability, RenewalCountersHaveExactPhysicalAndLogicalDeltas)
{
    const auto run = [](RenewalCounterBackend::Fault fault, uint64_t attempts, uint64_t retries, uint64_t resolved, uint64_t recovered)
    {
        auto backend = std::make_shared<RenewalCounterBackend>();
        uint64_t boot_ms = 100;
        auto store = Pool::open(backend, PoolConfig{
            .pool_prefix = "renewal-counter-" + std::to_string(attempts) + "-" + std::to_string(resolved),
            .server_root_id = "test",
            .mount_lease_ttl_ms = std::chrono::milliseconds(1000),
            .cas_request_budget = renewalCounterBudget(),
            .boot_ms_fn = [&] { return boot_ms; },
        });
        backend->fault = fault;
        const RenewalCounterSnapshot before = renewalCounters();
        EXPECT_NO_THROW(store->renewWatermarkOnce());
        const RenewalCounterSnapshot after = renewalCounters();
        expectRenewalCounterDelta(before, after, attempts, retries, resolved, recovered, 0);
    };

    run(RenewalCounterBackend::Fault::None, /*attempts=*/1, /*retries=*/0, /*resolved=*/0, /*recovered=*/0);
    run(RenewalCounterBackend::Fault::ThrowBefore, /*attempts=*/2, /*retries=*/1, /*resolved=*/0, /*recovered=*/1);
    run(RenewalCounterBackend::Fault::LandThenThrow, /*attempts=*/1, /*retries=*/0, /*resolved=*/1, /*recovered=*/1);
}

TEST(CASObservability, ExternalLeaseDeadlineCountsOnceWithoutReconstructingAttempts)
{
    auto backend = std::make_shared<RenewalCounterBackend>();
    uint64_t boot_ms = 100;
    auto store = Pool::open(backend, PoolConfig{
        .pool_prefix = "renewal-deadline-counter",
        .server_root_id = "test",
        .mount_lease_ttl_ms = std::chrono::milliseconds(1000),
        .cas_request_budget = renewalCounterBudget(),
        .boot_ms_fn = [&] { return boot_ms; },
    });

    /// The confirmed external safety deadline is 1080. At 1071 a ten-millisecond physical attempt
    /// no longer fits, so the logical renewal ends without reconstructing a sent attempt.
    boot_ms = 1071;
    const RenewalCounterSnapshot before = renewalCounters();
    EXPECT_THROW(store->renewWatermarkOnce(), DB::Exception);
    const RenewalCounterSnapshot after = renewalCounters();
    expectRenewalCounterDelta(
        before, after, /*attempts=*/0, /*retries=*/0, /*resolved=*/0, /*recovered=*/0,
        /*deadline_exceeded=*/1);
}

}

/// B170/Task 1 (Part A audit events): `PartWriteTxn::stageManifest` writes a part-manifest body but never
/// emitted an audit row for it — the log could not answer "when was this manifest written." Verifies
/// the emitted `ManifestPut` event (exactly once per successful stage).
TEST(CASObservability, StageManifestEmitsManifestPut)
{
    std::shared_ptr<InMemoryBackend> b;
    std::vector<CasEvent> seen;   /// declared BEFORE the Pool so it outlives the background syncer's emits (ASan 2026-07-09)
    auto s = openPool(b);
    s->setEventSink([&](const CasEvent & e){ seen.push_back(e); });

    const RootNamespace ns{"srv/tbl@cas@"};
    auto build = s->beginPartWrite(PartWriteInfo{.intended_ref = ns.string() + "/all_0_0_0", .intended_namespace = ns});
    ManifestEntry e;
    e.path = "f";
    e.placement = EntryPlacement::Inline;
    e.inline_bytes = "AAA";
    const ManifestId id = build->stageManifest({e});
    s->setEventSink(nullptr);

    EXPECT_EQ(std::count_if(seen.begin(), seen.end(),
        [](const CasEvent & x){ return x.type == CasEventType::ManifestPut; }), 1);

    const auto it = std::find_if(seen.begin(), seen.end(),
        [](const CasEvent & x){ return x.type == CasEventType::ManifestPut; });
    ASSERT_NE(it, seen.end());
    EXPECT_EQ(it->object_kind, CasEventObjectKind::Manifest);
    EXPECT_EQ(it->object_hash, manifestRefDebugString(id.ref));
    EXPECT_FALSE(it->token.empty());
}

/// `PartWriteTxn::abandon` removes a live precommit's owner binding (the correctness-bearing step) but never
/// audited the removal — the log could not distinguish "never precommitted" from "precommitted then
/// abandoned." Verifies the emitted `PrecommitRemoved` event (exactly once, only when a precommit was
/// actually live).
TEST(CASObservability, AbandonEmitsPrecommitRemoved)
{
    std::shared_ptr<InMemoryBackend> b;
    std::vector<CasEvent> seen;   /// declared BEFORE the Pool so it outlives the background syncer's emits (ASan 2026-07-09)
    auto s = openPool(b);

    const RootNamespace ns{"srv/tbl@cas@"};
    auto build = s->beginPartWrite(PartWriteInfo{.intended_ref = ns.string() + "/all_0_0_0", .intended_namespace = ns});
    ManifestEntry e;
    e.path = "f";
    e.placement = EntryPlacement::Inline;
    e.inline_bytes = "AAA";
    const ManifestId id = build->stageManifest({e});
    build->precommitAdd(ns, "all_0_0_0", id);

    s->setEventSink([&](const CasEvent & x){ seen.push_back(x); });
    build->abandon();
    s->setEventSink(nullptr);

    EXPECT_EQ(std::count_if(seen.begin(), seen.end(),
        [](const CasEvent & x){ return x.type == CasEventType::PrecommitRemoved; }), 1);

    const auto it = std::find_if(seen.begin(), seen.end(),
        [](const CasEvent & x){ return x.type == CasEventType::PrecommitRemoved; });
    ASSERT_NE(it, seen.end());
    EXPECT_EQ(it->namespace_, ns.string());
    EXPECT_EQ(it->ref_name, "all_0_0_0");
    EXPECT_EQ(it->object_kind, CasEventObjectKind::Root);
    EXPECT_EQ(it->object_hash, manifestRefDebugString(id.ref));
}

/// A build that never precommitted has nothing to remove: `abandon` must not fabricate a
/// `PrecommitRemoved` row for a binding that was never live.
TEST(CASObservability, AbandonWithoutPrecommitEmitsNoPrecommitRemoved)
{
    std::shared_ptr<InMemoryBackend> b;
    std::vector<CasEvent> seen;   /// declared BEFORE the Pool so it outlives the background syncer's emits (ASan 2026-07-09)
    auto s = openPool(b);

    const RootNamespace ns{"srv/tbl@cas@"};
    auto build = s->beginPartWrite(PartWriteInfo{.intended_ref = ns.string() + "/all_0_0_0", .intended_namespace = ns});
    ManifestEntry e;
    e.path = "f";
    e.placement = EntryPlacement::Inline;
    e.inline_bytes = "AAA";
    build->stageManifest({e});   /// staged, never precommitted

    s->setEventSink([&](const CasEvent & x){ seen.push_back(x); });
    build->abandon();
    s->setEventSink(nullptr);

    EXPECT_EQ(std::count_if(seen.begin(), seen.end(),
        [](const CasEvent & x){ return x.type == CasEventType::PrecommitRemoved; }), 0);
}

/// Task 2 (Part A audit fix, 2026-07-08): the republication-supersede branch inside `closeBlob`
/// (`CasBlobInDegree.cpp`) used to peek the current token via `head_blob` — the FRESH-CONDEMN
/// observation hook — which double-emitted `blob_retire` alongside `blob_retire_replaced` and
/// double-counted `CASGCRetiredCondemned` for what is really one physical condemnation (republication
/// replaced a stale retired entry with the current token). Drives the same condemn-A / republish-B /
/// drop-B sequence as `CASGCLeak.ResurrectReplacedIncarnationReclaimed`, then isolates the ONE round
/// that folds B's create+drop and supersedes A's stale retired entry: that round must emit exactly one
/// `blob_retire_replaced` (carrying the STALE token A in `detail["superseded_token"]`), ZERO
/// `blob_retire` for this hash, one `CASGCRetireReplaced` increment, and NO `CASGCRetiredCondemned`
/// double-count.
TEST(CASObservability, ResurrectSupersedeEmitsOnlyRetireReplacedWithOldToken)
{
    std::shared_ptr<InMemoryBackend> b;
    std::vector<CasEvent> seen;   /// declared BEFORE the Pool so it outlives the background syncer's emits (ASan 2026-07-09)
    auto s = openPool(b);
    const RootNamespace ns{"test/tbl"};
    const String P = "republish-payload-audit";

    /// 1. Publish ref r1 -> token A referenced; drop it; ONE GC round condemns A (retired, not deleted).
    publishOneBlobPart(s, ns, "r1", P);
    const HeadResult hA = b->head(s->layout().blobKey(idOf(P)));
    ASSERT_TRUE(hA.exists);
    s->dropRef(ns, "r1");
    s->renewWatermarkOnce();

    Gc gc(s, hexToU128("000000000000000000000000000000ab"));
    {
        const RoundReport rep = gc.runRegularRound();
        ASSERT_TRUE(rep.acquired_lease);
    }
    {
        const auto lm = DB::Cas::tests::loadMetaForTest(*b, s->layout(), u128Of(P));
        ASSERT_TRUE(lm.has_value() && lm->meta.state == MetaState::Condemned)
            << "precondition: token A must be condemned before republication";
    }

    /// 2. RESURRECT: r2 dedup-hits P while A is condemned -> mints a fresh incarnation B; drop it too.
    publishOneBlobPart(s, ns, "r2", P);
    const HeadResult hB = b->head(s->layout().blobKey(idOf(P)));
    ASSERT_TRUE(hB.exists);
    ASSERT_NE(hB.token.value, hA.token.value) << "republication must mint a new incarnation token B";
    s->dropRef(ns, "r2");
    s->renewWatermarkOnce();

    /// 3. The NEXT round folds r2's create+drop in one pass and must SUPERSEDE A's stale retired entry
    /// with a fresh condemn of B (peek, not the fresh-condemn `head_blob` hook). Capture events + the
    /// counters for exactly THIS round.
    using ProfileEvents::global_counters;
    const auto condemned_before = global_counters[ProfileEvents::CASGCRetiredCondemned].load();
    const auto replaced_before  = global_counters[ProfileEvents::CASGCRetireReplaced].load();

    s->setEventSink([&](const CasEvent & e){ seen.push_back(e); });
    const RoundReport rep = gc.runRegularRound();
    s->setEventSink(nullptr);
    ASSERT_TRUE(rep.acquired_lease);

    const auto condemned_after = global_counters[ProfileEvents::CASGCRetiredCondemned].load();
    const auto replaced_after  = global_counters[ProfileEvents::CASGCRetireReplaced].load();

    /// Phase 3 (mixed-algo pools): event `object_hash` renders are `blobIdOf(ref)` ("<algoName>:<hex>"),
    /// never a bare hex.
    const String hash_hex = DB::Cas::blobIdOf(DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(u128Of(P))});
    const auto is_this_blob = [&](const CasEvent & e){ return e.object_hash == hash_hex; };

    EXPECT_EQ(std::count_if(seen.begin(), seen.end(),
        [&](const CasEvent & e){ return is_this_blob(e) && e.type == CasEventType::BlobRetire; }), 0)
        << "supersede must not also emit blob_retire (that is the fresh-condemn hook's event)";

    std::vector<CasEvent> replaced_events;
    std::copy_if(seen.begin(), seen.end(), std::back_inserter(replaced_events),
        [&](const CasEvent & e){ return is_this_blob(e) && e.type == CasEventType::BlobRetireReplaced; });
    ASSERT_EQ(replaced_events.size(), 1u) << "exactly one blob_retire_replaced for the supersede";
    EXPECT_EQ(replaced_events[0].token, hB.token.value) << "the event's own token is the fresh CURRENT token B";
    ASSERT_TRUE(replaced_events[0].detail.count("superseded_token"));
    EXPECT_FALSE(replaced_events[0].detail.at("superseded_token").empty());
    EXPECT_EQ(replaced_events[0].detail.at("superseded_token"), hA.token.value)
        << "superseded_token must name the stale token (A) that republication replaced";

    EXPECT_EQ(replaced_after - replaced_before, 1u) << "CASGCRetireReplaced increments exactly once";
    EXPECT_EQ(condemned_after - condemned_before, 0u)
        << "supersede peek must not fresh-condemn -- CASGCRetiredCondemned must not double-count";

    /// Size-unit regression guard (audit fix, 2026-07-08): `peek_head` used to return the RAW
    /// `backend.head(...)` size (physical, header-included), while the fresh-condemn hook `head_blob`
    /// strips the pool's fixed blob header via `retiredLogicalSize` before the size lands in
    /// `RetiredEntry.size`. That mismatch meant supersede-minted entries and fresh-condemn entries carried
    /// two different unit conventions in the SAME persisted `RetiredSet`. The superseded entry (now naming
    /// the fresh token B) must carry the LOGICAL size -- i.e. the payload length, with the pool's blob
    /// header already stripped -- exactly like a fresh condemn of the same blob would.
    const std::vector<RetiredEntry> retired = currentRetiredSet(*b, s->layout(), /*shard*/0);
    const auto it = std::find_if(retired.begin(), retired.end(),
        [&](const RetiredEntry & e){ return e.kind == ObjectKind::Blob && e.ref == DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(u128Of(P))}; });
    ASSERT_NE(it, retired.end()) << "the superseded entry must be present in the current retired set";
    EXPECT_EQ(it->token.value, hB.token.value) << "the persisted entry names the fresh CURRENT token B";
    EXPECT_EQ(it->size, P.size())
        << "supersede must persist the LOGICAL size (payload length, header stripped), matching what "
           "a fresh condemn of the same blob would carry -- not the raw physical (header-included) size";
}

/// Task 3 (Part B, `clickhouse-disks cas-inspect`): `caInspectToJson` is a FREE function (no
/// disk/backend involved) that decodes any CA bucket object at `key` and renders it as JSON, purely
/// by matching `key` against `Layout`'s prefixes/key-shapes and calling the matching `decode*`.
/// These tests drive it directly against real encoder output (one per recognized key shape) plus the
/// unknown-key fail-closed path — the same function the CLI command (`CommandCaInspect.cpp`) calls.

/// The legacy mutable ref-shard object is gone (snapshot+log ref model); inspect now decodes the two
/// immutable ref objects. A `_snap/<id>.proto` renders as a ref-table snapshot...
TEST(CASObservability, CaInspectDecodesRefSnapshotToJson)
{
    using DB::Cas::tests::committedRow;
    using DB::Cas::tests::minimalLiveSnapshot;
    Layout layout("p");
    const RootNamespace ns{"srv/tbl@cas@"};
    const RefTxnId snap_id{1, 7};
    const RefTableSnapshot snap = minimalLiveSnapshot(ns.string(), snap_id,
        {committedRow("all_0_0_0", ManifestRef{.writer_epoch = 1, .build_sequence = 2, .manifest_ordinal = 1})});
    const String key = layout.refSnapshotKey(DB::Cas::tests::fixture::fixtureLife(ns), snap_id);
    const String json = caInspectToJson(
        layout, key, encodeRefTableSnapshot(snap), DB::Cas::tests::fixture::fixtureLife(ns));
    EXPECT_NE(json.find(R"("object":"ref_snapshot")"), String::npos) << json;
    EXPECT_NE(json.find(R"("namespace":"srv/tbl@cas@")"), String::npos) << json;
    EXPECT_NE(json.find(R"("snapshot_id":{"writer_epoch":1,"ref_sequence":7})"), String::npos) << json;
    EXPECT_NE(json.find(R"("ref_name":"all_0_0_0")"), String::npos) << json;
    EXPECT_NE(json.find(R"("precommits":[])"), String::npos) << json;
    EXPECT_EQ(json.find("\"lifecycle\""), String::npos)
        << "generation-8 snapshot inspection must not recreate lifecycle state retired from the snapshot DTO";
}

/// ...and a `_log/<txn-id>` renders as a ref-transaction log.
TEST(CASObservability, CaInspectDecodesRefLogToJson)
{
    Layout layout("p");
    const RootNamespace ns{"srv/tbl@cas@"};
    const RefTxnId txn_id{1, 8};
    RefLogTxn txn;
    txn.ns = ns.string();
    txn.txn_id = txn_id;
    RefOp add;
    add.kind = RefOpKind::OwnerTransition;
    add.new_binding = RefOwnerBinding{RefOwnerKind::Precommit, "all_0_0_0",
        ManifestRef{.writer_epoch = 1, .build_sequence = 2, .manifest_ordinal = 1}};
    txn.ops = {add};
    const String key = layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), txn_id);
    const String json = caInspectToJson(
        layout, key, encodeRefLogTxn(txn), DB::Cas::tests::fixture::fixtureLife(ns));
    EXPECT_NE(json.find("ref_log"), String::npos);
    EXPECT_NE(json.find("OwnerTransition"), String::npos);
    EXPECT_NE(json.find("all_0_0_0"), String::npos);
}

TEST(CASObservability, CaInspectDecodesPartManifestToJson)
{
    Layout layout("p");
    const RootNamespace ns{"srv/tbl@cas@"};

    PartManifest m;
    m.ref = ManifestRef{.writer_epoch = 1, .build_sequence = 2, .manifest_ordinal = 3};
    m.root_namespace_id = ns;
    ManifestEntry e;
    e.path = "data.bin";
    e.placement = EntryPlacement::Inline;
    e.inline_bytes = "hello";
    m.entries = {e};
    m.payload_digest = computePayloadDigest(m);

    const ManifestId id{.root_namespace = ns, .ref = m.ref};
    const String key = layout.manifestKey(id);
    const String json = caInspectToJson(layout, key, encodePartManifest(m));
    EXPECT_NE(json.find("\"root_namespace_id\""), String::npos);
    EXPECT_NE(json.find("data.bin"), String::npos);
    EXPECT_NE(json.find("\"manifest_ordinal\":3"), String::npos);
}

TEST(CASObservability, CaInspectDecodesMountLeaseToJson)
{
    Layout layout("p");
    MountLease lease;
    lease.server_uuid = hexToU128("000000000000000000000000000000ab");
    lease.writer_epoch = 5;
    lease.write_attempt_id = hexToU128("00112233445566778899aabbccddeeff");
    lease.hostname = "host1";
    lease.pid = 123;

    const String key = layout.mountKey("srid1");
    const String json = caInspectToJson(layout, key, encodeMountLease(lease));
    EXPECT_NE(json.find("\"writer_epoch\":5"), String::npos);
    EXPECT_NE(json.find("\"write_attempt_id\":\"00112233445566778899aabbccddeeff\""), String::npos);
    EXPECT_NE(json.find("host1"), String::npos);
}

TEST(CASObservability, CaInspectDecodesGcStateToJson)
{
    Layout layout("p");
    GcState state;
    state.round = 42;
    state.gc_shards = 4;

    const String key = layout.gcStateKey();
    const String json = caInspectToJson(layout, key, encodeGcState(state));
    EXPECT_NE(json.find("\"round\":42"), String::npos);
    EXPECT_NE(json.find("\"gc_shards\":4"), String::npos);
}

TEST(CASObservability, CaInspectUnknownKeyThrows)
{
    Layout layout("p");
    EXPECT_THROW(caInspectToJson(layout, "p/not/a/ca/object", "xxxx"), DB::Exception);   /// BAD_ARGUMENTS
}
