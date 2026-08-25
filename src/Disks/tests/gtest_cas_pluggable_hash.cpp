#include <gtest/gtest.h>

/// P1-T2 (CAS pluggable-blob-hash Phase 1):
/// `PoolMeta` records the pool-wide `blob_hash_algo` and `PoolMeta::createOrValidate` fail-closes on a
/// disk config that disagrees with an existing pool's recorded algo -- the pool-wide durability
/// invariant (never silently re-hash an existing pool).
///
/// Phase 3 T4 RELAXES that single fail-closed
/// value into `PoolMeta::algos_used` (sorted, append-only): a config algo already a MEMBER is
/// accepted with no write (steady state); a non-member is admitted via a CAS-union ONLY when the
/// disk opts in (`blob_hash_allow_new`), and refused (`BAD_ARGUMENTS`, same as before) otherwise --
/// a changed config alone must never silently turn a pool mixed. See `AdmissionIsFlagGated` and
/// `ConcurrentAdmissionUnions` below.
///
/// P1-T3a (this file, extended): the pool's `blob_hash_algo` is threaded into the three hash sites
/// (spec §5/§6) -- `Cas::CaContentWriteBuffer` (streaming blob-body hash),
/// `PartWriteTxn`'s envelope `hash_algo` field, and (transitively, via `Cas::blobHashHexOneShot`) the
/// `poolContentHash` content-key mint on the write path. `poolContentHash` itself is a static
/// helper in `CasPartWriteTxn.cpp` and not directly reachable from a gtest; its production callers already
/// exercise the default `CityHash128` path, and it delegates to the SAME `Cas::blobHashHexOneShot`
/// this file tests directly below.

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasBlobHashingWriteBuffer.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasBlobInDegree.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasCodecUtil.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasBlobEnvelopeFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasFsck.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPartManifestFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPoolMetaFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include "cas_test_helpers.h"

#include <IO/WriteBufferFromFile.h>


#include <algorithm>
#include <filesystem>
#include <unordered_set>
#include <utility>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int UNKNOWN_FORMAT_VERSION;
}

using namespace DB::Cas;
using namespace DB::Cas::tests;


namespace
{

/// A deterministic, non-repeating-byte payload spanning several `DBMS_DEFAULT_HASHING_BLOCK_SIZE`
/// (2048 B) blocks, so a chunked-vs-one-shot divergence (the CityHash128 pitfall documented on
/// `poolContentHash`) would not accidentally go unnoticed.
std::string makeMultiBlockPayload(size_t size = 5000)
{
    std::string s;
    s.reserve(size);
    for (size_t i = 0; i < size; ++i)
        s.push_back(static_cast<char>('a' + (i % 23)));
    return s;
}

/// A blob written at its OWN algo's content key, plus the key it landed at.
struct SeededBlob
{
    BlobRef ref;
    String key;
};

/// Write a blob body of `algo` at its content key, reference it from a committed ref, and DROP that
/// ref — so the blob reaches a folded in-degree of zero and the ORDINARY pipeline condemns it by
/// transition-to-zero. The caller then runs the rounds that fold the `+1` and the `-1`.
///
/// These tests used to seed a blob no manifest ever named and lean on `rebuildBaseline`'s LIST/HEAD
/// sweep, which was the only path that could condemn such a blob. That sweep is GONE (spec §7: a
/// rebuild condemns nothing — it was the r5-finding-4 data-loss vector), and a blob nothing names is
/// now retained by design. What these tests actually guard — that a blob is recognized under its OWN
/// `<algo>` path segment by the fold's key codec, `previewDeletes`, the exact-token delete and fsck,
/// rather than silently skipped as foreign — is unaffected, and lives on the PRODUCTION path, which is
/// where it is now exercised. Reverting either per-algo port still turns these red.
SeededBlob seedReferencedBlob(Pool & store, Backend & backend, const RootNamespace & ns, BlobHashAlgo algo,
                              uint64_t build_sequence, size_t payload_size, const String & ref_name)
{
    const std::string payload = makeMultiBlockPayload(payload_size);
    const BlobRef ref{algo, codecFor(algo).fromHex(blobHashHexOneShot(algo, payload))};
    const String key = store.layout().blobKey(ref);

    EnvelopeHeader header;
    header.kind = ObjectKind::Blob;
    header.incarnation_tag = UInt128(0x1234);
    header.build_id = UInt128(0x5678);
    backend.putIfAbsent(key, encodeEnvelopeHeader(header, static_cast<uint32_t>(store.poolMeta().blob_header_len)) + payload);

    ManifestEntry entry;
    entry.path = "data_" + std::to_string(build_sequence) + ".bin";
    entry.placement = EntryPlacement::Blob;
    entry.ref = ref;   /// the entry carries the blob's OWN algo, not the pool's write algo
    entry.blob_size = 1;

    const ManifestRef mref{.writer_epoch = 1, .build_sequence = build_sequence, .manifest_ordinal = 1};
    writeManifestRaw(backend, store.layout(), ns, mref, {entry});
    publishCommittedTransition(backend, store.layout(), ns, ref_name, std::nullopt, mref);
    return SeededBlob{ref, key};
}

/// Drop the committed ref `seedReferencedBlob` published, so the blob's only edge disappears.
void dropSeededRef(Pool & store, Backend & backend, const RootNamespace & ns, uint64_t build_sequence,
                   const String & ref_name)
{
    const ManifestRef mref{.writer_epoch = 1, .build_sequence = build_sequence, .manifest_ordinal = 1};
    dropRefTransition(backend, store.layout(), ns, ref_name, mref);
}

}

TEST(CASPluggableHash, PoolMetaRoundTripsAlgosUsed)
{
    PoolMeta pm;
    pm.pool_id = u128Of("pool-a");
    pm.blob_header_len = 256;
    pm.algos_used = {static_cast<uint8_t>(BlobHashAlgo::CityHash128), static_cast<uint8_t>(BlobHashAlgo::XXH3_128)};

    const PoolMeta back = decodePoolMeta(encodePoolMeta(pm));
    EXPECT_EQ(back.algos_used, pm.algos_used);
    EXPECT_EQ(back.blob_header_len, 256u);
}

TEST(CASPluggableHash, CreateOrValidateRecordsConfigAlgoOnFreshPool)
{
    auto backend = std::make_shared<InMemoryBackend>();
    const Layout layout("p");

    const PoolMeta pm = PoolMeta::createOrValidate(*backend, layout, /*blob_header_len*/ 256, BlobHashAlgo::XXH3_128, /*allow_new*/ false, /*allow_mint*/ true);
    EXPECT_EQ(pm.algos_used, (std::vector<uint8_t>{static_cast<uint8_t>(BlobHashAlgo::XXH3_128)}));

    /// Reopening with the SAME algo is a no-op reopen: the recorded value comes back unchanged.
    const PoolMeta reopened = PoolMeta::createOrValidate(*backend, layout, 256, BlobHashAlgo::XXH3_128);
    EXPECT_EQ(reopened.algos_used, (std::vector<uint8_t>{static_cast<uint8_t>(BlobHashAlgo::XXH3_128)}));
    EXPECT_EQ(reopened.pool_id, pm.pool_id);
}

TEST(CASPluggableHash, CreateOrValidateDefaultsToCityHash128)
{
    auto backend = std::make_shared<InMemoryBackend>();
    const Layout layout("p");

    const PoolMeta pm = PoolMeta::createOrValidate(*backend, layout, 256, BlobHashAlgo::CityHash128, /*allow_new*/ false, /*allow_mint*/ true);
    EXPECT_EQ(pm.algos_used, (std::vector<uint8_t>{static_cast<uint8_t>(BlobHashAlgo::CityHash128)}));
}

/// Phase 3 T4 (spec §5, replaces the Phase 1/2 unconditional-fail-close test of the same shape):
/// admission of a NEW algo is EXPLICIT OPT-IN -- the default reopen with a non-member algo still
/// fails closed (`BAD_ARGUMENTS`), but the message names `<cas_blob_hash_allow_new>` and the pool
/// is truly extensible with the flag set. See `AdmissionIsFlagGated` below for the full flow.
TEST(CASPluggableHash, CreateOrValidateFailsClosedOnAlgoMismatchWithoutFlag)
{
    auto backend = std::make_shared<InMemoryBackend>();
    const Layout layout("p");

    PoolMeta::createOrValidate(*backend, layout, 256, BlobHashAlgo::CityHash128, /*allow_new*/ false, /*allow_mint*/ true);

    expectThrowsCodeWithMessage(
        DB::ErrorCodes::BAD_ARGUMENTS,
        "<cas_blob_hash_allow_new>1</cas_blob_hash_allow_new>",
        [&]
        {
            PoolMeta::createOrValidate(*backend, layout, 256, BlobHashAlgo::XXH3_128, /*allow_new*/ false);
        });

    /// The pool is untouched by the refused reopen: a subsequent open with the ORIGINAL algo still
    /// succeeds and returns the same pool_id.
    const PoolMeta reopened = PoolMeta::createOrValidate(*backend, layout, 256, BlobHashAlgo::CityHash128);
    EXPECT_EQ(reopened.algos_used, (std::vector<uint8_t>{static_cast<uint8_t>(BlobHashAlgo::CityHash128)}));
}

/// spec §9.1 at the unit level: admission of a new algo requires the flag; once admitted, membership
/// alone is the steady-state check (the flag is not needed again for the same algo).
TEST(CASPluggableHash, AdmissionIsFlagGated)
{
    auto backend = std::make_shared<InMemoryBackend>();
    const Layout layout("p");
    PoolMeta::createOrValidate(*backend, layout, 256, BlobHashAlgo::CityHash128, /*allow_new*/ false, /*allow_mint*/ true);

    /// without the flag: refuse, pool untouched
    expectThrowsCode(DB::ErrorCodes::BAD_ARGUMENTS, [&]
    { PoolMeta::createOrValidate(*backend, layout, 256, BlobHashAlgo::Sha256, false); });

    /// with the flag: admitted
    const PoolMeta admitted = PoolMeta::createOrValidate(*backend, layout, 256, BlobHashAlgo::Sha256, true);
    EXPECT_EQ(admitted.algos_used, (std::vector<uint8_t>{1, 3}));

    /// steady state: admitted algo reopens WITHOUT the flag
    const PoolMeta steady = PoolMeta::createOrValidate(*backend, layout, 256, BlobHashAlgo::Sha256, false);
    EXPECT_EQ(steady.algos_used, (std::vector<uint8_t>{1, 3}));
}

TEST(CASPluggableHash, ConcurrentAdmissionUnions)
{
    auto backend = std::make_shared<InMemoryBackend>();
    const Layout layout("p");
    PoolMeta::createOrValidate(*backend, layout, 256, BlobHashAlgo::CityHash128, false, /*allow_mint*/ true);
    PoolMeta::createOrValidate(*backend, layout, 256, BlobHashAlgo::XXH3_128, true);
    PoolMeta::createOrValidate(*backend, layout, 256, BlobHashAlgo::Sha256, true);
    const PoolMeta final_pm = PoolMeta::createOrValidate(*backend, layout, 256, BlobHashAlgo::CityHash128, false);
    EXPECT_EQ(final_pm.algos_used, (std::vector<uint8_t>{1, 2, 3}));   /// union, sorted, nothing lost
}

/// ---- P1-T3a: the pool's blob_hash_algo threaded into the streaming write-buffer hash site ----

/// `Cas::CaContentWriteBuffer`'s LOCAL-staging constructor (the everyday spill-to-temp-file
/// mode `ContentAddressedTransaction::writeFile` uses), built with `BlobHashAlgo::XXH3_128`, must hash
/// the streamed payload with xxh3 -- agreeing with the standalone `blobHashHexOneShot` one-shot helper
/// (the same convention `poolContentHash`'s re-hash uses).
TEST(CASPluggableHash, ContentWriteBufferLocalModeHashesWithSelectedAlgoXxh3)
{
    const std::string payload = makeMultiBlockPayload();
    const auto temp_dir = (std::filesystem::temp_directory_path() / "cas_pluggable_hash_xxh3_local").string();

    std::string got_hash_hex;
    size_t got_size = 0;
    auto buf = std::make_unique<DB::Cas::CaContentWriteBuffer>(
        temp_dir,
        BlobHashAlgo::XXH3_128,
        /*buf_size=*/8192,
        /*use_adaptive_buffer_size=*/false,
        /*adaptive_buffer_initial_size=*/0,
        [&](const std::string & hash_hex, size_t size, const std::string &)
        {
            got_hash_hex = hash_hex;
            got_size = size;
        });

    /// Write in two chunks so more than one nextImpl flush happens (exercises the streaming state, not
    /// just a single call).
    buf->write(payload.data(), 1234);
    buf->write(payload.data() + 1234, payload.size() - 1234);
    buf->finalize();

    EXPECT_EQ(got_size, payload.size());
    EXPECT_EQ(got_hash_hex, blobHashHexOneShot(BlobHashAlgo::XXH3_128, payload));
    /// A wrong-but-plausible result (e.g. accidentally still hashing with cityHash128) would silently
    /// produce a DIFFERENT hex string -- pin that the two algos disagree on this payload, so the
    /// assertion above is actually discriminating.
    EXPECT_NE(got_hash_hex, blobHashHexOneShot(BlobHashAlgo::CityHash128, payload));
}

/// The DEFAULT algo (`CityHash128`) through the SAME write buffer must stay byte-for-byte unchanged --
/// the CAS pluggable-blob-hash invariant (spec §8). Compares against `blobHashHexOneShot`, which
/// `gtest_cas_blob_hasher.cpp`'s `CityHash128ByteIdenticalToHashingWriteBuffer` already proves is
/// byte-identical to the pre-existing plain `HashingWriteBuffer` convention.
TEST(CASPluggableHash, ContentWriteBufferLocalModeCityHash128Unchanged)
{
    const std::string payload = makeMultiBlockPayload();
    const auto temp_dir = (std::filesystem::temp_directory_path() / "cas_pluggable_hash_ch128_local").string();

    std::string got_hash_hex;
    auto buf = std::make_unique<DB::Cas::CaContentWriteBuffer>(
        temp_dir,
        BlobHashAlgo::CityHash128,
        /*buf_size=*/8192,
        /*use_adaptive_buffer_size=*/false,
        /*adaptive_buffer_initial_size=*/0,
        [&](const std::string & hash_hex, size_t, const std::string &)
        {
            got_hash_hex = hash_hex;
        });

    buf->write(payload.data(), payload.size());
    buf->finalize();

    EXPECT_EQ(got_hash_hex, blobHashHexOneShot(BlobHashAlgo::CityHash128, payload));
}

/// (codecs-v3 phase 7) The two former `Pool...StampsEnvelopeHashAlgo...` tests were REMOVED: the v3
/// blob envelope no longer carries a `hash_algo` field (the algo identity lives in the blob KEY, spec
/// §blob-envelope). Algo correctness for the write path is covered by the P1-T3b blob-body-PATH-key
/// tests below (they assert the blob key uses the pool's algo), which is the surviving source of truth.

/// ---- P1-T3b: the pool's blob_hash_algo threaded into blob-body PATH keys (spec §3/§10) ----

/// A blob written and promoted through a live ref on an xxh3-128 pool lands under the
/// `blobs/xxh3/<shard>/<hex>` path segment (not the bare `blobs/<shard>/<hex>` shape), is readable at
/// that key, and `runFsck`'s LIST-based discovery (`Layout::blobsPrefix`, deliberately algo-agnostic)
/// finds it reachable and clean -- proving the GC/fsck key-parse (which takes only the LAST path
/// component as the hex digest, `CasGc.cpp`/`CasFsck.cpp`) still works with the extra segment.
TEST(CASPluggableHash, Xxh3BlobLandsUnderAlgoSegmentAndIsDiscoveredCleanByFsck)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend,
        PoolConfig{.pool_prefix = "p", .server_root_id = "test",
                   .blob_hash_algo = BlobHashAlgo::XXH3_128});

    const RootNamespace ns{"srv1/tbl"};
    const std::string payload = makeMultiBlockPayload();
    const BlobRef id{BlobHashAlgo::XXH3_128, codecFor(BlobHashAlgo::XXH3_128).fromHex(blobHashHexOneShot(BlobHashAlgo::XXH3_128, payload))};

    PartWriteInfo info;
    info.intended_ref = ns.string() + "/rb";
    auto build = store->beginPartWrite(info);

    ManifestEntry e;
    e.path = "data.bin";
    e.placement = EntryPlacement::Blob;
    e.ref = id;

    e.blob_size = payload.size();
    const ManifestId mid = build->stageManifest({e});
    build->precommitAdd(ns, "rb", mid);
    build->putBlob(id, BlobSource::fromString(payload));

    /// The blob body landed under the algo-segmented path -- readable there, not at the legacy
    /// no-segment shape.
    const String blob_key = store->layout().blobKey(id);
    EXPECT_NE(blob_key.find("/blobs/xxh3/"), String::npos) << blob_key;
    EXPECT_EQ(blob_key.find("/blobs/ch128/"), String::npos) << blob_key;
    EXPECT_TRUE(backend->head(blob_key).exists);

    build->promote(ns, "rb", build->buildId(), mid);
    store->renewWatermarkOnce();

    const FsckReport rep = runFsck(*store, /*detail=*/true);
    EXPECT_TRUE(rep.clean());
    EXPECT_EQ(rep.dangling, 0u);
    EXPECT_GE(rep.reachable, 1u);

    /// Not merely "clean by omission" (e.g. a bug that silently LISTed nothing): the physical listing
    /// actually walked the algo-segmented key.
    const bool found = std::any_of(rep.objects.begin(), rep.objects.end(),
        [](const FsckObject & o) { return o.key.find("/blobs/xxh3/") != String::npos; });
    EXPECT_TRUE(found);
}

/// ============================================================================================
/// CAS pluggable-blob-hash Phase 2 Task 5 -- THE CRUX (anti-silent-leak regression gate).
///
/// Two sites classify a blob by parsing its object-key hex into a hash set: `CasGc.cpp`'s condemn
/// path (the fold's transition-to-zero, and — until spec §7 removed it — `Gc::rebuildBaseline`'s
/// LIST/HEAD sweep) and `CasFsck.cpp`'s
/// present-but-unreferenced classification. Both used to route through the bare, fixed-width
/// `hexToU128` (32-hex-only) inside a `catch(...) continue` / no-catch-at-all — so a 64-hex `sha256`
/// key either (a) fell into the "foreign key shape — not ours" catch and was silently treated as
/// debris (the condemn sweep: the blob is NEVER condemned — a permanent GC leak), or (b) threw
/// uncaught out of fsck's present-but-unreferenced loop (a hard fsck failure on a live sha256 pool).
/// Phase 2 Task 5 ports both to the pool-scoped `DigestCodec::fromHex`, which parses a CORRECT-WIDTH
/// key (16 OR 32 bytes) — a genuinely foreign key shape (e.g. a `.meta` sibling) still falls into
/// the catch, but a real sha256 blob no longer does.
///
/// This test constructs a `sha256`-algo pool DIRECTLY via `PoolConfig` (this bypasses only the
/// disk-config *factory* guard in `MetadataStorageFactory.cpp`, which Task 6 removes — `Pool::open`
/// itself has never gated on algo) and writes a blob body straight at its 64-hex content-addressed key
/// (bypassing `PartWriteTxn::putBlob`, whose OWN internal `logical_hash` stays a fixed 128-bit
/// representation until a later task — see the Task 5 report), references it, and drops the reference
/// so the fold condemns it. It then drives BOTH crux sites and asserts the blob is CLASSIFIED, not
/// silently skipped as foreign.
///
/// MUST GO RED if either port is reverted to `hexToU128`: reverting `CasGc.cpp`'s fold leaves
/// `condemned_total == 0` (never condemned) and `previewDeletes()` empty; reverting `CasFsck.cpp`'s
/// sites either throws out of `runFsck` or leaves the blob unclassified/absent from `unreachable`.
TEST(CASPluggableHash, Sha256BlobSeenByCondemnSweepAndFsckNotSilentlySkipped)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend,
        PoolConfig{.pool_prefix = "p", .server_root_id = "test",
                   .blob_hash_algo = BlobHashAlgo::Sha256, .gc_fold_max_defer_rounds = 0});
    ASSERT_EQ(blobHashLenFor(store->writeAlgo()), 32u) << "sha256 must derive a 32-byte digest width";

    const DigestCodec codec = codecFor(store->writeAlgo());
    const std::string payload = makeMultiBlockPayload();
    const std::string hex = blobHashHexOneShot(BlobHashAlgo::Sha256, payload);
    ASSERT_EQ(hex.size(), 64u) << "sha256 renders 64 lowercase hex chars";
    const BlobDigest digest = codec.fromHex(hex);   // round-trip sanity: must not throw at width 32

    /// Reference the blob from a committed ref, then drop that ref: the fold sees `+1` then `-1`, the
    /// blob transitions to in-degree zero, and the ORDINARY condemn path claims it. Every per-algo
    /// parse this test guards sits on that path.
    const RootNamespace ns{"00/aa@cas@"};
    Gc gc(store, UInt128(1));
    const SeededBlob seeded = seedReferencedBlob(*store, *backend, ns, BlobHashAlgo::Sha256,
                                                 /*build_sequence=*/1, /*payload_size=*/5000, "tbl_sha");
    const BlobRef id = seeded.ref;
    const String blob_key = seeded.key;
    EXPECT_NE(blob_key.find("/blobs/sha256/"), String::npos) << blob_key;
    ASSERT_TRUE(backend->head(blob_key).exists) << "the sha256 blob body must be present before the fold";
    ASSERT_EQ(codecFor(store->writeAlgo()).fromHex(hex), digest) << "fixture sanity: the seeded digest is ours";

    /// ---- Site 1: the fold's condemn path ----
    runRegularRoundReclaiming(gc);   /// folds the +1
    dropSeededRef(*store, *backend, ns, /*build_sequence=*/1, "tbl_sha");
    runRegularRoundReclaiming(gc);   /// folds the -1: transition to zero => condemned

    const auto state_bytes = backend->get(store->layout().gcStateKey());
    ASSERT_TRUE(state_bytes.has_value());
    const GcState state = decodeGcState(state_bytes->bytes);
    ASSERT_GT(state.snap_generation, 0u);
    const auto seal_bytes = backend->get(store->layout().foldSealKey(state.snap_generation, state.snap_attempt));
    ASSERT_TRUE(seal_bytes.has_value());
    const CasFoldSeal seal = decodeFoldSeal(seal_bytes->bytes);
    ASSERT_TRUE(seal.condemned_summary.contains(0)) << "the seal's condemned_summary must be total over gc_shards";
    EXPECT_EQ(seal.condemned_summary.at(0).condemned_total, 1u)
        << "THE CRUX: the sha256 blob must be condemned by the fold -- a silent-leak regression (a "
           "reverted CasGc.cpp codec.fromHex port) leaves this at 0";

    /// previewDeletes streams the SAME adopted seal via the run's own SourceEdgeKeyCodec (never pool
    /// meta) and must report exactly our blob, at its real 32-byte digest.
    const std::vector<Gc::PreviewEntry> preview = gc.previewDeletes();
    ASSERT_EQ(preview.size(), 1u) << "THE CRUX: previewDeletes must surface the condemned sha256 blob";
    EXPECT_EQ(preview[0].ref, id);
    EXPECT_EQ(preview[0].key, blob_key);

    /// ---- Site 2: fsck's present-but-unreferenced classification ----
    /// Must complete without throwing (a reverted port either throws BAD_ARGUMENTS out of the
    /// no-try/catch parse sites, or silently drops the blob from every classified set) and must
    /// physically account for the blob.
    FsckReport frep;
    ASSERT_NO_THROW(frep = runFsck(*store, /*detail=*/true));
    EXPECT_GE(frep.unreachable, 1u)
        << "THE CRUX: fsck's physical listing must count the sha256 blob as unreachable-but-present, "
           "not silently omit it";
    const auto oit = std::find_if(frep.objects.begin(), frep.objects.end(),
        [&](const FsckObject & o) { return o.key == blob_key; });
    ASSERT_NE(oit, frep.objects.end()) << "the sha256 blob must appear in fsck's detailed object list";
    /// The fold above already condemned it into the GC snapshot, so fsck's GC-pipeline-view
    /// classification (not the generic Unaccounted bucket -- reachable only by width-correctly pairing
    /// the fsck-side hash against the run's kCondemned row hash) must recognize it as known-to-GC.
    EXPECT_EQ(oit->cls, FsckClass::PendingGc)
        << "THE CRUX: fsck must pair the sha256 blob against the GC snapshot's kCondemned row (a "
           "silent-leak regression in CasFsck.cpp's unref_hashes/in_run_hashes/retired_by_hash port "
           "leaves this as the generic Unaccounted bucket instead)";
}

/// ============================================================================================
/// CAS pluggable-blob-hash Phase 2 Task 6 -- end-to-end sha256 WRITE path (in-memory; the real
/// wiring-level integration + soak is Task 7).
///
/// Before this task, `PartWriteTxn`'s OWN write-path internals stayed a fixed 128-bit representation
/// downstream of the mint (`poolContentHash`/`PartWriteTxn::putBlob`'s `logical_hash`, the `deps` map key, the
/// event-log `object_hash` render, and `objectKey`) -- safe only because the disk-config factory guard
/// (`MetadataStorageFactory.cpp`) blocked any real sha256 pool from reaching `PartWriteTxn` at all (see the
/// Task 5 report and the "Task 6+" comments this task removes). Task 6 finishes those sites AND lifts
/// the guard in the SAME commit. This test drives a REAL `PartWriteTxn` (`putBlob` -> `stageManifest` ->
/// `precommitAdd` -> `promote`) on a `Sha256` pool and asserts:
///   1. the blob lands under `blobs/sha256/<64-hex>` and the manifest entry's `blob_hash`, read back via
///      `decodePartManifest`, is the FULL 32-byte digest (bytes beyond 16 are non-zero for a real sha256
///      digest, i.e. NOT truncated to `.toU128()`'s low 16 bytes);
///   2. an inline file and a standalone blob of IDENTICAL content get the SAME 32-byte `file_hash` under
///      sha256 -- mirroring the (fixed) `ContentAddressedTransaction.cpp` inline-candidate formula
///      (`blobHashHexOneShot(pool_algo, bytes)` -> pool-scoped `DigestCodec::fromHex`) directly at the
///      Core level, since exercising the wiring itself is Task 7's job;
///   3. `runFsck` on the pool is clean (no dangling, no foreign) -- the whole write -> GC -> fsck loop
///      agrees on the 64-hex key.
TEST(CASPluggableHash, Sha256BuildWritesFullWidthDigestAndInlineEqualsBlob)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend,
        PoolConfig{.pool_prefix = "p", .server_root_id = "test",
                   .blob_hash_algo = BlobHashAlgo::Sha256});
    ASSERT_EQ(blobHashLenFor(store->writeAlgo()), 32u) << "sha256 must derive a 32-byte digest width";
    const DigestCodec codec = codecFor(store->writeAlgo());

    const RootNamespace ns{"srv1/tbl"};
    const std::string payload = makeMultiBlockPayload();
    const std::string hex = blobHashHexOneShot(BlobHashAlgo::Sha256, payload);
    ASSERT_EQ(hex.size(), 64u) << "sha256 renders 64 lowercase hex chars";
    const BlobRef id{BlobHashAlgo::Sha256, codec.fromHex(hex)};

    PartWriteInfo info;
    info.intended_ref = ns.string() + "/part1";
    auto build = store->beginPartWrite(info);

    /// Mirror the (fixed) inline-candidate hash site directly: same content, same pool algo, via the
    /// SAME public formula ContentAddressedTransaction.cpp's writeFile now uses -- NOT the old hardcoded
    /// CityHash128 (which would produce a DIFFERENT, 128-bit-then-zero-padded value here).
    const BlobDigest inline_hash = codec.fromHex(blobHashHexOneShot(BlobHashAlgo::Sha256, payload));
    const BlobDigest blob_hash = codec.fromHex(hex);
    EXPECT_EQ(inline_hash, blob_hash) << "inline == blob: identical content must hash identically under sha256";

    /// THE CRUX (width): a genuine 32-byte sha256 digest must NOT be zero-padded past byte 16 -- the
    /// shape `BlobDigest::fromU128` (or a reverted hardcoded-CityHash128 inline site) would produce.
    const bool tail_nonzero = std::any_of(blob_hash.bytes.begin() + 16, blob_hash.bytes.end(),
        [](uint8_t b) { return b != 0; });
    EXPECT_TRUE(tail_nonzero) << "a genuine sha256 digest must not be zero-padded past byte 16";

    ManifestEntry blob_entry;
    blob_entry.path = "data.bin";
    blob_entry.placement = EntryPlacement::Blob;
    blob_entry.ref = BlobRef{BlobHashAlgo::Sha256, blob_hash};
    blob_entry.blob_size = payload.size();

    ManifestEntry inline_entry;
    inline_entry.path = "checksums.txt";
    inline_entry.placement = EntryPlacement::Inline;
    inline_entry.ref = BlobRef{BlobHashAlgo::Sha256, inline_hash};
    inline_entry.blob_size = payload.size();
    inline_entry.inline_bytes = payload;

    const ManifestId mid = build->stageManifest({blob_entry, inline_entry});
    build->precommitAdd(ns, "part1", mid);
    const PutBlobResult ref = build->putBlob(id, BlobSource::fromString(payload));
    EXPECT_EQ(ref.size, payload.size());

    /// THE CRUX (blob side): the blob body lands under the sha256-segmented path, addressed by the
    /// FULL 64-hex key -- `PartWriteTxn::putBlob`'s internal `logical_hash` must not have silently narrowed it
    /// to a 32-hex (128-bit) key before this task.
    const String blob_key = store->layout().blobKey(id);
    EXPECT_NE(blob_key.find("/blobs/sha256/"), String::npos) << blob_key;
    ASSERT_TRUE(backend->head(blob_key).exists);

    build->promote(ns, "part1", build->buildId(), mid);
    store->renewWatermarkOnce();

    /// Read the committed manifest back -- the on-disk `blob_hash` must be the FULL 32-byte digest, not
    /// truncated by the manifest codec or by anything upstream of `stageManifest`.
    const auto manifest_bytes = backend->get(store->layout().manifestKey(mid));
    ASSERT_TRUE(manifest_bytes.has_value());
    const PartManifest read_back = decodePartManifest(openObject(FormatId::PartManifest, manifest_bytes->bytes));
    ASSERT_EQ(read_back.entries.size(), 2u);
    const auto read_blob_it = std::find_if(read_back.entries.begin(), read_back.entries.end(),
        [](const ManifestEntry & e) { return e.placement == EntryPlacement::Blob; });
    ASSERT_NE(read_blob_it, read_back.entries.end());
    EXPECT_EQ(read_blob_it->ref.digest, blob_hash);
    const bool read_tail_nonzero = std::any_of(read_blob_it->ref.digest.bytes.begin() + 16,
        read_blob_it->ref.digest.bytes.end(), [](uint8_t b) { return b != 0; });
    EXPECT_TRUE(read_tail_nonzero) << "the manifest's on-disk blob_hash must not be truncated either";

    /// The write -> GC -> fsck loop must agree end-to-end on the 64-hex key: clean, no dangling.
    const FsckReport rep = runFsck(*store, /*detail=*/true);
    EXPECT_TRUE(rep.clean());
    EXPECT_EQ(rep.dangling, 0u);
    EXPECT_GE(rep.reachable, 1u);
}

/// ============================================================================================
/// CAS mixed-algo pools Phase 3 T5:
/// path-derived `BlobRef` in the sweep/fsck (`Layout::parseBlobKey`) and per-entry admission
/// validation at `foldManifestEdges` with refresh-on-miss.
/// ============================================================================================

/// spec §9.8 -- THE race regression this task exists to close. Each `Pool`'s `admitted_algos` cache
/// is a MONOTONE snapshot seeded once at `Pool::open` and never re-read on its own; if node A admits
/// a brand-new algo and publishes a manifest naming it, node B's stale cache must NOT fail the fold
/// closed forever -- `foldManifestEdges` must refresh `_pool_meta` on the very first miss and accept
/// once the fresh read proves the algo genuinely admitted. Node B is opened BEFORE node A performs the
/// admission on purpose: constructing B afterward would seed its cache already-fresh and never
/// exercise the race the fix targets.
TEST(CASPluggableHash, StaleAlgoRegistryRefreshOnMiss)
{
    auto backend = std::make_shared<InMemoryBackend>();

    /// Node B opens FIRST -- its admitted-cache seeds at {ch128} only, before sha256 exists anywhere.
    auto store_b = Pool::open(backend,
        PoolConfig{.pool_prefix = "p", .server_root_id = "b",
                   .blob_hash_algo = BlobHashAlgo::CityHash128});
    ASSERT_TRUE(store_b->isAlgoAdmitted(BlobHashAlgo::CityHash128));
    ASSERT_FALSE(store_b->isAlgoAdmitted(BlobHashAlgo::Sha256));

    /// Node A opens SECOND, admits sha256 via the opt-in flag, and publishes a manifest naming a
    /// sha256 blob through the real PartWriteTxn path (putBlob -> stageManifest -> precommitAdd -> promote).
    auto store_a = Pool::open(backend,
        PoolConfig{.pool_prefix = "p", .server_root_id = "a",
                   .blob_hash_algo = BlobHashAlgo::Sha256, .blob_hash_allow_new = true});
    ASSERT_TRUE(store_a->isAlgoAdmitted(BlobHashAlgo::Sha256));

    const RootNamespace ns{"srv1/tbl"};
    const std::string payload = makeMultiBlockPayload();
    const BlobRef id{BlobHashAlgo::Sha256, codecFor(BlobHashAlgo::Sha256).fromHex(blobHashHexOneShot(BlobHashAlgo::Sha256, payload))};

    PartWriteInfo info;
    info.intended_ref = ns.string() + "/part1";
    auto build = store_a->beginPartWrite(info);

    ManifestEntry e;
    e.path = "data.bin";
    e.placement = EntryPlacement::Blob;
    e.ref = id;
    e.blob_size = payload.size();
    const ManifestId mid = build->stageManifest({e});
    build->precommitAdd(ns, "part1", mid);
    build->putBlob(id, BlobSource::fromString(payload));
    build->promote(ns, "part1", build->buildId(), mid);
    store_a->renewWatermarkOnce();

    /// B's cache is STILL stale here -- it has never re-read `_pool_meta` since open.
    ASSERT_FALSE(store_b->isAlgoAdmitted(BlobHashAlgo::Sha256));

    /// B folds the committed ref naming the sha256 entry: without refresh-on-miss this throws
    /// CORRUPTED_DATA ("manifest entry algo sha256 not admitted"); with it, the miss triggers exactly
    /// one `refreshAdmittedAlgos()` and the fold proceeds.
    Gc gc(store_b, UInt128(1));
    const RebuildReport rep = gc.rebuildBaseline(/*force*/ true);
    ASSERT_TRUE(rep.performed) << rep.refusal;
    EXPECT_EQ(rep.committed_refs, 1u);
    EXPECT_TRUE(store_b->isAlgoAdmitted(BlobHashAlgo::Sha256)) << "the miss must have unioned B's cache";
}

/// spec §9.4 half: an object whose key names an algo THIS BUILD has never heard of (a genuinely
/// foreign top-level segment, e.g. planted by a different/future tool) must never be treated as one
/// of ours -- the GC must skip it (never condemn or delete it) and fsck must classify it into the
/// generic `Unaccounted` bucket (never throw, never silently drop it from the physical listing).
/// In the SAME pass, a 2-algo pool's OWN blobs under `blobs/ch128/` and `blobs/sha256/` must both
/// still be classified normally -- the foreign segment must not make the fold/fsck narrow to one
/// algo or blind them to the others.
TEST(CASPluggableHash, ForeignAlgoSegmentIsDebrisNotOurs)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend,
        PoolConfig{.pool_prefix = "p", .server_root_id = "test",
                   .blob_hash_algo = BlobHashAlgo::CityHash128, .gc_fold_max_defer_rounds = 0});
    /// Admit sha256 into the SAME pool from a second mount, then pull the union into `store`'s cache.
    Pool::open(backend,
        PoolConfig{.pool_prefix = "p", .server_root_id = "test2",
                   .blob_hash_algo = BlobHashAlgo::Sha256, .blob_hash_allow_new = true});
    store->refreshAdmittedAlgos();
    ASSERT_TRUE(store->isAlgoAdmitted(BlobHashAlgo::Sha256));

    /// Two of the pool's OWN blobs -- one per algo -- each referenced by a committed ref that is then
    /// DROPPED, so the fold condemns both by transition-to-zero.
    const RootNamespace ns{"00/aa@cas@"};
    Gc gc(store, UInt128(1));
    const SeededBlob ch = seedReferencedBlob(*store, *backend, ns, BlobHashAlgo::CityHash128,
                                             /*build_sequence=*/1, /*payload_size=*/5001, "tbl_ch");
    const SeededBlob sh = seedReferencedBlob(*store, *backend, ns, BlobHashAlgo::Sha256,
                                             /*build_sequence=*/2, /*payload_size=*/5002, "tbl_sh");
    const BlobRef ch_ref = ch.ref;
    const BlobRef sh_ref = sh.ref;
    const String ch_key = ch.key;
    const String sh_key = sh.key;

    /// A FOREIGN object under an algo segment `blobHashAlgoName` never renders ("md5") -- not one of
    /// ours under any circumstance.
    const String foreign_key = store->layout().blobsPrefix() + "md5/aa/" + std::string(32, 'a');
    backend->putIfAbsent(foreign_key, std::string("not a real envelope"));

    runRegularRoundReclaiming(gc);   /// folds both +1s
    dropSeededRef(*store, *backend, ns, /*build_sequence=*/1, "tbl_ch");
    dropSeededRef(*store, *backend, ns, /*build_sequence=*/2, "tbl_sh");
    runRegularRoundReclaiming(gc);   /// folds both -1s: both transition to zero

    /// The fold condemns exactly the two OWN blobs -- never the foreign object.
    const std::vector<Gc::PreviewEntry> preview = gc.previewDeletes();
    ASSERT_EQ(preview.size(), 2u);
    std::unordered_set<BlobRef, BlobRefHash> condemned_refs;
    for (const auto & p : preview)
    {
        condemned_refs.insert(p.ref);
        EXPECT_NE(p.key, foreign_key);
    }
    EXPECT_TRUE(condemned_refs.count(ch_ref));
    EXPECT_TRUE(condemned_refs.count(sh_ref));
    EXPECT_TRUE(backend->head(foreign_key).exists) << "the foreign object must never be touched by the fold";

    const FsckReport frep = runFsck(*store, /*detail=*/true);
    /// The physical listing counts all THREE unreferenced objects (two ours + one foreign).
    EXPECT_EQ(frep.unreachable, 3u);
    const auto foreign_obj = std::find_if(frep.objects.begin(), frep.objects.end(),
        [&](const FsckObject & o) { return o.key == foreign_key; });
    ASSERT_NE(foreign_obj, frep.objects.end()) << "the foreign object must still appear in the physical listing";
    /// ... but classified as generic Unaccounted -- it can never pair against the GC snapshot, which
    /// only ever knows about OUR two algo-segmented refs.
    EXPECT_EQ(foreign_obj->cls, FsckClass::Unaccounted);

    /// The two OWN blobs are recognized under their OWN algo segment in the SAME pass.
    const auto ch_obj = std::find_if(frep.objects.begin(), frep.objects.end(),
        [&](const FsckObject & o) { return o.key == ch_key; });
    const auto sh_obj = std::find_if(frep.objects.begin(), frep.objects.end(),
        [&](const FsckObject & o) { return o.key == sh_key; });
    ASSERT_NE(ch_obj, frep.objects.end());
    ASSERT_NE(sh_obj, frep.objects.end());
    EXPECT_EQ(ch_obj->cls, FsckClass::PendingGc);
    EXPECT_EQ(sh_obj->cls, FsckClass::PendingGc);
}

/// ============================================================================================
/// CAS reader-generation gate (`Core/Formats/CasFormat.h`'s `G_BUILD`) was raised to 4 for
/// per-namespace contiguous ref-log ids (INV-1) and has since moved again, to 5, for Stage B's
/// namespace-life-keyed ref layer ("format bump B", `kNamespaceLifeKeyedGeneration`) -- this test's
/// assertions read `G_BUILD` itself rather than a hardcoded generation number for exactly that reason,
/// so a THIRD bump does not silently make them false. `PoolMeta::createOrValidate`'s open-time
/// CAS-raise targets `G_BUILD`, and `decodePoolMeta` fail-closes BOTH on a FUTURE
/// `min_reader_generation` AND on a BACKWARD pool whose header `compatibility_version` is below
/// `kNamespaceLifeKeyedGeneration` (which, being the LATER of the two historical breaking-change
/// floors, subsumes `kContiguousRefStreamsGeneration` -- see `CasPoolMetaFormat.cpp`).
/// ============================================================================================

TEST(CASPluggableHash, ReaderGenerationIsRaisedToGBuild)
{
    EXPECT_GE(G_BUILD, kNamespaceLifeKeyedGeneration)
        << "the reader-generation gate must be at least the namespace-life-keyed floor it enforces";

    /// A freshly opened/created pool records `min_reader_generation == G_BUILD` (the open-time
    /// CAS-raise, `PoolMeta::createOrValidate`, always targets this build's own floor).
    {
        auto backend = std::make_shared<InMemoryBackend>();
        auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
        EXPECT_EQ(store->poolMeta().min_reader_generation, G_BUILD);

        const auto meta_bytes = backend->get(store->layout().poolMetaKey());
        ASSERT_TRUE(meta_bytes.has_value());
        EXPECT_EQ(decodePoolMeta(meta_bytes->bytes).min_reader_generation, G_BUILD);
    }

    /// FORWARD gate: a pool-meta carrying `min_reader_generation == G_BUILD + 1` (one generation past
    /// THIS build's floor) still fails closed at open -- the startup gate (`decodePoolMeta`) rejects it
    /// even though generation 4 is now understood.
    {
        auto backend = std::make_shared<InMemoryBackend>();
        const Layout layout("p");
        PoolMeta pm = PoolMeta::createOrValidate(*backend, layout, /*blob_header_len*/ 256, BlobHashAlgo::CityHash128, /*allow_new*/ false, /*allow_mint*/ true);
        pm.min_reader_generation = G_BUILD + 1;
        ASSERT_TRUE(backend->casPut(layout.poolMetaKey(), encodePoolMeta(pm), backend->get(layout.poolMetaKey())->token).outcome == CasOutcome::Committed);

        expectThrowsCode(DB::ErrorCodes::UNKNOWN_FORMAT_VERSION, [&]
        { Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"}); });
    }

    /// BACKWARD floor: a pool whose header `v` (compatibility_version) is BELOW `G_BUILD` was written
    /// by an older build this reader can no longer trust -- today that is one generation short of
    /// `kNamespaceLifeKeyedGeneration`, a pool whose ref-object keys carry no incarnation segment,
    /// which this build's parsers refuse as corruption rather than read. Craft it at the text layer:
    /// take a fresh pool-meta and rewrite its line-1 version gate down to `G_BUILD - 1` (an older
    /// build would have stamped exactly that).
    {
        auto backend = std::make_shared<InMemoryBackend>();
        const Layout layout("p");
        PoolMeta pm = PoolMeta::createOrValidate(*backend, layout, /*blob_header_len*/ 256, BlobHashAlgo::CityHash128, /*allow_new*/ false, /*allow_mint*/ true);
        const String fresh_bytes = encodePoolMeta(pm);

        const String from = "\"v\":" + std::to_string(G_BUILD);
        const String to = "\"v\":" + std::to_string(G_BUILD - 1);
        const auto pos = fresh_bytes.find(from);
        ASSERT_NE(pos, String::npos);   // sanity: a fresh pool stamps the header at the floor
        String downgraded = fresh_bytes;
        downgraded.replace(pos, from.size(), to);
        ASSERT_TRUE(backend->casPut(layout.poolMetaKey(), downgraded, backend->get(layout.poolMetaKey())->token).outcome == CasOutcome::Committed);

        /// `decodePoolMeta`'s backward floor rejects the downgraded bytes directly...
        expectThrowsCode(DB::ErrorCodes::UNKNOWN_FORMAT_VERSION, [&] { decodePoolMeta(downgraded); });
        /// ...and so does a full `Pool::open` (decoding the pool-meta is its first fail-closed step).
        expectThrowsCode(DB::ErrorCodes::UNKNOWN_FORMAT_VERSION, [&]
        { Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"}); });
    }
}

/// ============================================================================================
/// CAS mixed-algo pools Phase 3 T6:
/// cross-cutting cruxes over a pool that genuinely mixes algos end-to-end (reclaim + distinctness).
/// The no-bare-digest grep gates (design Step 3) are run separately, not as gtest bodies.
/// ============================================================================================

/// spec §9.3 -- THE reclaim crux. A pool admits BOTH `ch128` and `sha256`; a blob body is
/// planted directly under EACH algo's segment (mirrors `Sha256BlobSeenByCondemnSweepAndFsckNotSilentlySkipped`'s
/// fixture, widened to two algos). The fold must condemn BOTH into the SAME baseline
/// (`previewDeletes` surfaces both refs), and driving the round-paced pipeline to completion (graduate,
/// then the exact-token delete) must reclaim BOTH bodies -- the backend ends up holding ZERO blob
/// bytes of EITHER algo, and fsck reports clean.
///
/// MUST GO RED if any settlement/graduation/delete path silently narrows to one algo -- e.g. a fold
/// that only accounts `blobs/ch128/`, a graduation/delete loop that iterates a digest-only set and
/// coalesces the two algos' entries, or an fsck reachability check that stops after the first algo it
/// sees.
TEST(CASPluggableHash, TwoAlgoBlobsBothFullyReclaimed)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend,
        PoolConfig{.pool_prefix = "p", .server_root_id = "test",
                   .blob_hash_algo = BlobHashAlgo::CityHash128, .gc_fold_max_defer_rounds = 0});
    /// Admit sha256 into the SAME pool from a second mount, then pull the union into `store`'s cache
    /// (mirrors `ForeignAlgoSegmentIsDebrisNotOurs`'s admission fixture).
    Pool::open(backend,
        PoolConfig{.pool_prefix = "p", .server_root_id = "test2",
                   .blob_hash_algo = BlobHashAlgo::Sha256, .blob_hash_allow_new = true});
    store->refreshAdmittedAlgos();
    ASSERT_TRUE(store->isAlgoAdmitted(BlobHashAlgo::Sha256));

    /// Two of the pool's OWN blobs -- one per algo -- referenced then dropped, so the fold condemns
    /// both by transition-to-zero.
    const RootNamespace ns{"00/aa@cas@"};
    Gc gc(store, UInt128(1));
    const SeededBlob ch = seedReferencedBlob(*store, *backend, ns, BlobHashAlgo::CityHash128,
                                             /*build_sequence=*/1, /*payload_size=*/5001, "tbl_ch");
    const SeededBlob sh = seedReferencedBlob(*store, *backend, ns, BlobHashAlgo::Sha256,
                                             /*build_sequence=*/2, /*payload_size=*/5002, "tbl_sh");
    const String ch_key = ch.key;
    const String sh_key = sh.key;
    ASSERT_TRUE(backend->head(ch_key).exists);
    ASSERT_TRUE(backend->head(sh_key).exists);

    runRegularRoundReclaiming(gc);   /// folds both +1s
    dropSeededRef(*store, *backend, ns, /*build_sequence=*/1, "tbl_ch");
    dropSeededRef(*store, *backend, ns, /*build_sequence=*/2, "tbl_sh");
    runRegularRoundReclaiming(gc);   /// folds both -1s: both condemned in the same round

    /// previewDeletes covers BOTH refs from the adopted seal -- never just one algo.
    {
        const std::vector<Gc::PreviewEntry> preview = gc.previewDeletes();
        ASSERT_EQ(preview.size(), 2u);
        std::unordered_set<BlobRef, BlobRefHash> refs;
        for (const auto & p : preview)
            refs.insert(p.ref);
        EXPECT_TRUE(refs.count(ch.ref));
        EXPECT_TRUE(refs.count(sh.ref));
    }

    /// Drive the round-paced pipeline to actual physical deletion: the fold condemned both at its
    /// round; the VERY NEXT round graduates them (unconditionally, round-paced); the round after that
    /// executes the exact-token delete for both.
    {
        const RoundReport rep1 = runRegularRoundReclaiming(gc);
        EXPECT_EQ(rep1.graduated, 2u) << "both algos' blobs must graduate together in one round";
        EXPECT_TRUE(backend->head(ch_key).exists);   // pending: still present this pass
        EXPECT_TRUE(backend->head(sh_key).exists);
    }
    {
        const RoundReport rep2 = runRegularRoundReclaiming(gc);
        EXPECT_EQ(rep2.redeleted, 2u) << "both algos' pending deletes must execute together in one round";
    }

    /// THE CRUX: after graduation the backend holds ZERO blob bodies of EITHER algo.
    EXPECT_FALSE(backend->head(ch_key).exists) << "the ch128 blob must be physically reclaimed";
    EXPECT_FALSE(backend->head(sh_key).exists) << "the sha256 blob must be physically reclaimed";

    const FsckReport frep = runFsck(*store, /*detail=*/true);
    EXPECT_TRUE(frep.clean());
    EXPECT_EQ(frep.dangling, 0u);
}

/// spec §9.5 -- same-digest-different-algo end-to-end. `ch128:X` and `xxh3:X` share the SAME 16-byte
/// digest VALUE but are DISTINCT blob identities (`BlobRef` is the pair): distinct object keys, distinct
/// `.meta`, distinct bodies, distinct settlement rows (fold both -> distinct in-degree per ref), and
/// dropping ONE ref's committed manifest reclaims ONLY that algo's blob -- the other stays fully
/// readable throughout.
///
/// MUST GO RED if anything upstream of `BlobRef` ever collapses identity to the bare digest (e.g. a
/// settlement/meta/condemn site keyed on `BlobDigest` alone) -- the two blobs would alias into one row
/// and dropping one ref would (wrongly) reclaim or corrupt the other.
TEST(CASPluggableHash, SameDigestDifferentAlgoDistinctBodiesAndSettlement)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend,
        PoolConfig{.pool_prefix = "p", .server_root_id = "test",
                   .blob_hash_algo = BlobHashAlgo::CityHash128, .gc_fold_max_defer_rounds = 0});
    Pool::open(backend,
        PoolConfig{.pool_prefix = "p", .server_root_id = "test2",
                   .blob_hash_algo = BlobHashAlgo::XXH3_128, .blob_hash_allow_new = true});
    store->refreshAdmittedAlgos();
    ASSERT_TRUE(store->isAlgoAdmitted(BlobHashAlgo::XXH3_128));

    /// SAME 16-byte digest VALUE under two different algos -- deliberately NOT derived from either
    /// body's real content hash: the crux under test is identity distinctness (the pair), not hash
    /// correctness (already covered by the sha256/xxh3 write-path tests above).
    const BlobDigest shared_digest = BlobDigest::fromU128(UInt128(0xC0FFEE));
    const BlobRef ref_ch{BlobHashAlgo::CityHash128, shared_digest};
    const BlobRef ref_xx{BlobHashAlgo::XXH3_128, shared_digest};
    /// Distinct content, not merely distinct length: `makeMultiBlockPayload` at two different sizes
    /// would make the shorter body a byte-for-byte PREFIX of the longer one (same repeating pattern
    /// from the same phase), which would defeat the "must not contain" assertions below.
    const std::string body_ch = makeMultiBlockPayload(4001);
    std::string body_xx = makeMultiBlockPayload(4002);
    std::reverse(body_xx.begin(), body_xx.end());
    ASSERT_NE(body_ch, body_xx);

    const RootNamespace ns{"srv1/tbl"};

    PartWriteInfo info_a;
    info_a.intended_ref = ns.string() + "/part_a";
    auto build_a = store->beginPartWrite(info_a);
    ManifestEntry e_a;
    e_a.path = "a.bin"; e_a.placement = EntryPlacement::Blob; e_a.ref = ref_ch; e_a.blob_size = body_ch.size();
    const ManifestId mid_a = build_a->stageManifest({e_a});
    build_a->precommitAdd(ns, "part_a", mid_a);
    build_a->putBlob(ref_ch, BlobSource::fromString(body_ch));
    build_a->promote(ns, "part_a", build_a->buildId(), mid_a);

    PartWriteInfo info_b;
    info_b.intended_ref = ns.string() + "/part_b";
    auto build_b = store->beginPartWrite(info_b);
    ManifestEntry e_b;
    e_b.path = "b.bin"; e_b.placement = EntryPlacement::Blob; e_b.ref = ref_xx; e_b.blob_size = body_xx.size();
    const ManifestId mid_b = build_b->stageManifest({e_b});
    build_b->precommitAdd(ns, "part_b", mid_b);
    build_b->putBlob(ref_xx, BlobSource::fromString(body_xx));
    build_b->promote(ns, "part_b", build_b->buildId(), mid_b);
    store->renewWatermarkOnce();

    /// Distinct object keys and distinct bodies despite the SAME digest value.
    const String key_ch = store->layout().blobKey(ref_ch);
    const String key_xx = store->layout().blobKey(ref_xx);
    EXPECT_NE(key_ch, key_xx);
    const auto raw_ch = backend->get(key_ch);
    const auto raw_xx = backend->get(key_xx);
    ASSERT_TRUE(raw_ch.has_value());
    ASSERT_TRUE(raw_xx.has_value());
    EXPECT_NE(raw_ch->bytes.find(body_ch), String::npos);
    EXPECT_NE(raw_xx->bytes.find(body_xx), String::npos);
    EXPECT_EQ(raw_ch->bytes.find(body_xx), String::npos) << "the ch128 body must not contain the xxh3 payload";
    EXPECT_EQ(raw_xx->bytes.find(body_ch), String::npos) << "the xxh3 body must not contain the ch128 payload";

    /// Distinct `.meta` objects.
    const String meta_ch = store->layout().blobMetaKey(ref_ch);
    const String meta_xx = store->layout().blobMetaKey(ref_xx);
    EXPECT_NE(meta_ch, meta_xx);
    EXPECT_TRUE(backend->head(meta_ch).exists);
    EXPECT_TRUE(backend->head(meta_xx).exists);

    /// Distinct settlement (in-degree per ref, keyed on the FULL `BlobRef` pair -- never the shared
    /// bare digest, which would alias the two rows into one).
    Gc gc(store, UInt128(1));
    runRegularRoundReclaiming(gc);
    {
        const GcState st = decodeGcState(backend->get(store->layout().gcStateKey())->bytes);
        const CasFoldSeal seal = decodeFoldSeal(
            backend->get(store->layout().foldSealKey(st.snap_generation, st.snap_attempt))->bytes);
        EXPECT_EQ(inDegreeInRuns(*backend, seal.blob_target_runs, ref_ch), 1);
        EXPECT_EQ(inDegreeInRuns(*backend, seal.blob_target_runs, ref_xx), 1);
    }

    /// Dropping ONLY `part_a`'s committed ref condemns+reclaims ONLY `ch128:X`; `xxh3:X` (the SAME
    /// digest value, a DIFFERENT algo) stays referenced and fully readable throughout.
    store->dropRef(ns, "part_a");
    runRegularRoundReclaiming(gc);   // condemns ch128:X (in-degree drops to 0); xxh3:X is untouched (still ref'd)
    runRegularRoundReclaiming(gc);   // graduates ch128:X
    runRegularRoundReclaiming(gc);   // executes the exact-token delete for ch128:X

    EXPECT_FALSE(backend->head(key_ch).exists) << "ch128:X must be reclaimed once its ref is dropped";
    EXPECT_TRUE(backend->head(key_xx).exists)
        << "THE CRUX: xxh3:X (same digest value, different algo) must remain readable after ch128:X "
           "is reclaimed -- a digest-only settlement would have condemned/deleted both together";
    const auto still_readable = backend->get(key_xx);
    ASSERT_TRUE(still_readable.has_value());
    EXPECT_NE(still_readable->bytes.find(body_xx), String::npos);

    const FsckReport frep = runFsck(*store, /*detail=*/true);
    EXPECT_TRUE(frep.clean());
    EXPECT_EQ(frep.dangling, 0u);
}
