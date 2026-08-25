#pragma once

#include "config.h"

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedSettings.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasBlobMeta.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasBlobEnvelopeFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasGcStateFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasBlobInDegree.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFoldSealFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPartManifestFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefLogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefSnapshotFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCatalog.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCkpt.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefProtocol.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasBlobUploadPool.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/thread_local_rng.h>

#include <base/hex.h>
#include <city.h>

#include <gtest/gtest.h>
#include <IO/HashingReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
/// For `ChunkFaultBackend`'s `DefiniteFailure` mode, which needs a real S3-classified error, and for
/// the ambiguity it raises otherwise.
#include <IO/S3Common.h>
#include <Poco/Exception.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <chrono>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

/// Per-TU extern declarations for the `ContentAddressedSetting` entries this header's helpers use --
/// the established pattern for `BaseSettings`-derived classes in this codebase (see e.g.
/// `RegisterDiskCache.cpp`'s `namespace FileCacheSetting` block): the entries are DEFINED once in
/// `ContentAddressedSettings.cpp`, and each consumer TU declares only the ones it references.
namespace DB::ContentAddressedSetting
{
    extern const ContentAddressedSettingsString server_root_id;
    extern const ContentAddressedSettingsString scratch_path;
}

/// Same per-TU pattern for the error codes this header's fault backends raise (`ChunkFaultBackend`'s
/// non-S3 build of the `Definite` mode).
namespace DB::ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

namespace DB::Cas::tests
{

/// Deterministic two-phase barrier for worker-lifecycle tests. The worker calls `arriveAndWait` at
/// the exact operation boundary under test; the test waits for that arrival and later calls
/// `release`. The bounded waits are only hang protection -- correctness never depends on elapsed
/// time or a polling sleep.
class ManualBarrier
{
public:
    void arriveAndWait()
    {
        std::unique_lock lock(mutex);
        arrived = true;
        cv.notify_all();
        if (!cv.wait_for(lock, std::chrono::seconds(20), [this] { return released; }))
            throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "CAS test barrier timed out waiting for release");
    }

    void waitUntilArrived()
    {
        std::unique_lock lock(mutex);
        if (!cv.wait_for(lock, std::chrono::seconds(20), [this] { return arrived; }))
            throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "CAS test barrier timed out waiting for arrival");
    }

    void release()
    {
        std::lock_guard lock(mutex);
        released = true;
        cv.notify_all();
    }

private:
    std::mutex mutex;
    std::condition_variable cv;
    bool arrived = false;
    bool released = false;
};

/// Bring up the server-wide blob upload pool (stage-1 §1) if it is not already up, so any test that
/// drives a `ContentAddressedTransaction` commit -- whose `uploadPendingBlobs` fans out on this pool --
/// finds it initialized. ROBUST (init-if-not-initialized, NOT `call_once`): the raw-lifecycle suite in
/// `gtest_cas_blob_upload_pool.cpp` deliberately shuts the pool down, so a `call_once` helper would fail
/// to bring it back for a later test. A global test-event listener (`gtest_cas_blob_upload_pool_env.cpp`)
/// calls this before every test, which is what makes it robust to test ordering.
inline void ensureBlobUploadPoolForTest(size_t size = 8)
{
    if (!DB::Cas::blobUploadPoolInitializedForTest())
        DB::Cas::initializeBlobUploadPool(size);
}


/// Minimal `ContentAddressedSettings` for a direct-construction gtest fixture: sets only
/// `server_root_id` and `scratch_path` (the two values every positional-ctor call site used to pass
/// explicitly) and validates, so the cached enum-valued accessors (`stagingBackend`, `blobHashAlgo`,
/// `partFolderValidate`) are populated from their (default) string settings exactly as the disk-factory
/// path would populate them. Callers that need a non-default setting (e.g. `staging_backend=s3`) apply
/// the override via `settings[ContentAddressedSetting::x] = value;` and re-run `settings.validate()`
/// themselves before constructing.
inline DB::ContentAddressedSettings makeSettingsForTest(const std::string & server_root_id, const std::filesystem::path & scratch_path)
{
    DB::ContentAddressedSettings settings;
    settings[DB::ContentAddressedSetting::server_root_id] = server_root_id;
    settings[DB::ContentAddressedSetting::scratch_path] = scratch_path.string();
    settings.validate();
    return settings;
}

/// Run `fn`, expect a DB::Exception with EXACTLY `expected_code` (CORRUPTED_DATA-vs-NOT_IMPLEMENTED
/// is part of the fail-closed contract: an unknown future format must be NOT_IMPLEMENTED, never
/// misreported as corruption).
template <typename F>
void expectThrowsCode(int expected_code, F && fn)
{
    try
    {
        fn();
        FAIL() << "expected DB::Exception";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), expected_code);
    }
}

/// Build a `LocalObjectStorage` rooted at a fresh, unique temporary directory (one per call).
///
/// Used by the unit tests that exercise the `Cas::Backend` seam against a real on-disk object storage
/// (the `EmulatedSingleProcess` adapter mode and the capability probe). For `LocalObjectStorage` the
/// object key IS the local path verbatim, so the unique root keeps every test instance isolated even
/// under the parallel gtest runner.
inline DB::ObjectStoragePtr makeLocalObjectStorageForTest()
{
    static std::atomic<uint64_t> counter{0};
    const auto unique = std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1));
    const auto root = (std::filesystem::temp_directory_path() / ("cas_unit_" + unique)).string();

    /// A silently missing root would surface later as a bewildering storage-layer error (e.g. a
    /// failed bootstrap LIST), so a setup failure must be loud and named.
    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    if (ec)
        throw std::runtime_error("cannot clear test object storage root " + root + ": " + ec.message());
    std::filesystem::create_directories(root, ec);
    if (ec)
        throw std::runtime_error("cannot create test object storage root " + root + ": " + ec.message());

    DB::LocalObjectStorageSettings settings("test", root, /*read_only_=*/false);
    return std::make_shared<DB::LocalObjectStorage>(std::move(settings));
}

/// Anchor a key under an object storage's own root, for the `Mode::Native` tests.
///
/// `Mode::Native` uses a key VERBATIM as the physical `LocalObjectStorage` path — no root-prefix
/// mapping the way `EmulatedSingleProcess`'s `emuPath` does (`LocalObjectStorage::writeObject`/
/// `readObject` pass `object.remote_path` straight through). A bare relative key like `"some/key"`
/// would therefore resolve relative to the TEST PROCESS's working directory rather than the
/// backend's own unique temp root, leaking a real file on disk that outlives the run and that a
/// later run then observes as pre-existing state. Worse for an assertion of ABSENCE: it answers
/// "absent" for a reason that has nothing to do with the property under test.
inline String nativeKeyUnder(const DB::ObjectStoragePtr & storage, const String & suffix)
{
    String root = storage->getCommonKeyPrefix();
    while (!root.empty() && root.back() == '/')
        root.pop_back();
    return root + "/" + suffix;
}

/// ---- on-storage write fixtures (shared by the Pool read/lifecycle/build tests, Tasks 9-13) ----
///
/// These produce objects through the SAME codecs the Pool reads — the documented on-storage
/// interface, not white-box pokes — so a test asserts a real round trip across the format boundary.

/// CityHash128 of bytes, composed into the canonical lowercase-hex id.
inline String hexOf(const String & bytes)
{
    return getHexUIntLowercase(CityHash_v1_0_2::CityHash128(bytes.data(), bytes.size()));
}

/// The POOL-WIDE streaming content hash (the production `HashingWriteBuffer` convention: chunked
/// CityHash128, block = DBMS_DEFAULT_HASHING_BLOCK_SIZE). Tests that exercise the copy-forward
/// VERIFICATION path must mint blob ids with THIS — the plain `idOf`/`u128Of` below are a
/// test-local convention (fine everywhere hashes are opaque; refused by the verifier).
inline String streamingHexOf(const String & payload)
{
    DB::ReadBufferFromMemory in(payload.data(), payload.size());
    DB::HashingReadBuffer hashing(in);
    hashing.ignoreAll();
    return getHexUIntLowercase(hashing.getHash());
}

/// The content id of `bytes` as a UInt128 — definitionally consistent with `idOf` (parses the same hex).
inline DB::UInt128 u128Of(const String & bytes)
{
    return DB::Cas::hexToU128(hexOf(bytes));
}

/// The content id of `bytes` as a `BlobRef` (CityHash128 — every test pool's default write algo).
inline DB::Cas::BlobRef idOf(const String & bytes)
{
    return DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(u128Of(bytes))};
}

/// Write a Blob object: a fixed-length (blob_header_len) envelope followed by the raw payload, keyed
/// by content. Mirrors what PartWriteTxn::putBlob will emit (Task 11).
inline DB::Cas::BlobRef writeBlobRaw(
    DB::Cas::Backend & backend, const DB::Cas::Layout & layout, const String & payload,
    uint64_t blob_header_len, [[maybe_unused]] const DB::UInt128 & domain_id)
{
    const DB::Cas::BlobRef id = idOf(payload);

    /// v3 envelope: domain_id/hash_algo dropped (identity is the content key); the `domain_id` param
    /// is kept for call-site compatibility but no longer stamped.
    DB::Cas::EnvelopeHeader header;
    header.kind = DB::Cas::ObjectKind::Blob;
    header.incarnation_tag = DB::UInt128(0x1234);
    header.build_id = DB::UInt128(0x5678);

    const String head = DB::Cas::encodeEnvelopeHeader(header, static_cast<uint32_t>(blob_header_len));
    backend.putIfAbsent(layout.blobKey(id), head + payload);
    return id;
}

/// Forward declaration: `appendOwnerEvent` (below) calls `registerNamespaceRaw`, which after Task 4
/// is a no-op (LIST-based discovery needs no explicit registration) defined further down.
inline void registerNamespaceRaw(
    DB::Cas::Backend & backend, const DB::Cas::Layout & layout, const DB::Cas::RootNamespace & ns);

/// Write a part-manifest body object directly via the manifest codec, exactly as PartWriteTxn::stageManifest
/// emits it. Returns the ManifestId. Used by GC fold/retire/fsck tests to stage owner targets.
inline DB::Cas::ManifestId writeManifestRaw(
    DB::Cas::Backend & backend, const DB::Cas::Layout & layout,
    const DB::Cas::RootNamespace & ns, const DB::Cas::ManifestRef & ref,
    const std::vector<DB::Cas::ManifestEntry> & entries)
{
    const DB::Cas::ManifestId id{ns, ref};
    DB::Cas::PartManifest body;
    body.ref = ref;
    body.root_namespace_id = ns;
    body.entries = entries;
    body.payload_digest = DB::Cas::computePayloadDigest(body);
    backend.putIfAbsent(layout.manifestKey(id),
        DB::Cas::sealObject(DB::Cas::FormatId::PartManifest, DB::Cas::encodePartManifest(body)));
    return id;
}

/// A blob ManifestEntry referencing `hash` at `path` (size 1, the GC fold counts edges, not bytes).
inline DB::Cas::ManifestEntry blobEntryFor(const String & path, const DB::UInt128 & hash, uint64_t size = 1)
{
    DB::Cas::ManifestEntry e;
    e.path = path;
    e.placement = DB::Cas::EntryPlacement::Blob;
    e.ref = DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(hash)};
    e.blob_size = size;
    return e;
}

/// Forward declarations of the ref snapshot+log raw fixtures defined further down (they emit the
/// snapshot+log objects GC and recovery actually read); the seeding wrappers below emit through them.
inline void writeRefLogTxnRaw(
    DB::Cas::Backend & backend, const DB::Cas::Layout & layout, const DB::Cas::RefLogTxn & txn);
namespace fixture
{
    inline DB::Cas::NamespaceLifeId fixtureLife(const DB::Cas::RootNamespace & ns);
}
inline void publishRecoverableCkptForSemanticWrapper(
    DB::Cas::Backend & backend, const DB::Cas::Layout & layout, const DB::Cas::RootNamespace & ns,
    const DB::Cas::RefTxnId & txn_id);
inline DB::Cas::RefOp namespaceBirthOp();
inline std::vector<DB::Cas::RefOp> publishCommittedOps(
    const String & ref_name, const DB::Cas::ManifestRef & manifest_ref);

/// One `owner_transition` op built from an optional old/new `RefOwnerBinding` (removal = old set / new
/// unset; add-precommit = new set / old unset; promote = both set naming the SAME manifest).
inline DB::Cas::RefOp ownerTransitionOp(
    std::optional<DB::Cas::RefOwnerBinding> old_binding, std::optional<DB::Cas::RefOwnerBinding> new_binding)
{
    DB::Cas::RefOp op;
    op.kind = DB::Cas::RefOpKind::OwnerTransition;
    op.old_binding = std::move(old_binding);
    op.new_binding = std::move(new_binding);
    return op;
}

/// Seed ONE ref-log transaction directly into a table's `_log/` stream -- the snapshot+log replacement
/// for the removed mutable-shard `appendOwnerEvent`. LIST the table's ref prefix, find the greatest
/// existing log/snapshot `ref_sequence` (and whether ANY log or snapshot exists at all), prepend a
/// `namespace_birth` op iff the table has none yet, allocate `txn_id = {writer_epoch=1, greatest+1}`,
/// and write `RefLogTxn{ns, txn_id, ops}` (no `prev_epoch_seal` -- this fixture never crosses an
/// epoch transition) via `writeRefLogTxnRaw`. Returns the allocated `ref_sequence`.
/// `ops` must form a REPLAY-VALID transaction: `fsck`/recovery replay them through the same state
/// machine the writer uses, and the GC edge extractor reads their manifest edges. The bytes are real
/// wire-format (the same codec `Pool`'s recovery reads) -- never hand-rolled.
inline uint64_t appendRefLogSeed(
    DB::Cas::Backend & backend, const DB::Cas::Layout & layout,
    const DB::Cas::RootNamespace & ns, std::vector<DB::Cas::RefOp> ops)
{
    /// Stage B (Task 4-C): resolve to whichever life is ALREADY on record (real production birth or the
    /// sentinel), exactly as `writeRefLogTxnRaw` below now does -- otherwise this scan can miss a REAL
    /// incarnation's existing log/snap objects, wrongly conclude the table has none, and prepend a second
    /// `namespaceBirthOp` on top of a namespace that already has one.
    const NamespaceLifeId life = CasRefCatalog::lifeIfCataloged(backend, layout, ns).value_or(fixture::fixtureLife(ns));
    const String prefix = layout.namespaceStreamPrefix(life);
    uint64_t greatest_seq = 0;
    bool any_log_or_snap = false;
    String cursor;
    while (true)
    {
        const DB::Cas::ListPage page = backend.list(prefix, cursor, /*limit=*/1000);
        for (const DB::Cas::ListedKey & lk : page.keys)
        {
            const auto parsed = layout.parseRefObjectKey(lk.key);
            if (!parsed)
                continue;
            if (parsed->kind == DB::Cas::RefObjectKind::Log || parsed->kind == DB::Cas::RefObjectKind::Snap)
            {
                any_log_or_snap = true;
                greatest_seq = std::max(greatest_seq, parsed->txn_id.ref_sequence);
            }
        }
        if (page.next_cursor.empty())
            break;
        cursor = page.next_cursor;
    }

    if (!any_log_or_snap)
        ops.insert(ops.begin(), namespaceBirthOp());

    DB::Cas::RefLogTxn txn;
    txn.ns = ns.string();
    txn.txn_id = DB::Cas::RefTxnId{/*writer_epoch=*/1, /*ref_sequence=*/greatest_seq + 1};
    txn.ops = std::move(ops);
    writeRefLogTxnRaw(backend, layout, txn);
    return txn.txn_id.ref_sequence;
}

/// Append ONE `owner_transition` op as a standalone ref-log transaction. `shard` is ignored (the
/// immutable ref model has no per-shard journal); it stays in the signature so existing shard-passing
/// callers compile unchanged. Returns the allocated `ref_sequence`.
inline uint64_t appendOwnerEvent(
    DB::Cas::Backend & backend, const DB::Cas::Layout & layout,
    const DB::Cas::RootNamespace & ns, uint64_t /*shard*/,
    std::optional<DB::Cas::RefOwnerBinding> old_binding,
    std::optional<DB::Cas::RefOwnerBinding> new_binding)
{
    return appendRefLogSeed(backend, layout, ns, {ownerTransitionOp(std::move(old_binding), std::move(new_binding))});
}

/// Publish a committed ref over `ref_name` (no old unless `old_ref` set). Emits a REPLAY-VALID
/// transaction: an optional owner-removal of the old committed binding, then add-precommit + promote of
/// the new manifest (spec §State Transitions has no direct "add committed" shape). Edges: -1(old)+1(new)
/// or +1(new). Returns the allocated `ref_sequence`.
inline uint64_t publishCommittedTransition(
    DB::Cas::Backend & backend, const DB::Cas::Layout & layout, const DB::Cas::RootNamespace & ns,
    const String & ref_name, std::optional<DB::Cas::ManifestRef> old_ref, const DB::Cas::ManifestRef & new_ref,
    uint64_t /*shard*/ = 0)
{
    std::vector<DB::Cas::RefOp> ops;
    if (old_ref)
        ops.push_back(ownerTransitionOp(
            DB::Cas::RefOwnerBinding{DB::Cas::RefOwnerKind::Committed, ref_name, *old_ref}, std::nullopt));
    const std::vector<DB::Cas::RefOp> commit_ops = publishCommittedOps(ref_name, new_ref);
    ops.insert(ops.end(), commit_ops.begin(), commit_ops.end());
    const uint64_t sequence = appendRefLogSeed(backend, layout, ns, std::move(ops));
    publishRecoverableCkptForSemanticWrapper(backend, layout, ns, RefTxnId{1, sequence});
    return sequence;
}

/// Drop a committed ref (old committed / new none). Edge -1. Returns the allocated `ref_sequence`.
inline uint64_t dropRefTransition(
    DB::Cas::Backend & backend, const DB::Cas::Layout & layout, const DB::Cas::RootNamespace & ns,
    const String & ref_name, const DB::Cas::ManifestRef & old_ref, uint64_t /*shard*/ = 0)
{
    const uint64_t sequence = appendRefLogSeed(backend, layout, ns,
        {ownerTransitionOp(DB::Cas::RefOwnerBinding{DB::Cas::RefOwnerKind::Committed, ref_name, old_ref}, std::nullopt)});
    publishRecoverableCkptForSemanticWrapper(backend, layout, ns, RefTxnId{1, sequence});
    return sequence;
}

/// Add a precommit binding (optional owner-removal of a stale committed manifest, then add-precommit of
/// the new manifest). Edge -1(old)+1(new) or +1(new). `build_id` is dropped (RefLog bindings carry no
/// build_id; build identity lives in `manifest_ref`). Returns the allocated `ref_sequence`.
inline uint64_t addPrecommitTransition(
    DB::Cas::Backend & backend, const DB::Cas::Layout & layout, const DB::Cas::RootNamespace & ns,
    const DB::UInt128 & /*build_id*/, const String & final_ref_name, std::optional<DB::Cas::ManifestRef> old_ref,
    const DB::Cas::ManifestRef & new_ref, uint64_t /*shard*/ = 0)
{
    std::vector<DB::Cas::RefOp> ops;
    if (old_ref)
        ops.push_back(ownerTransitionOp(
            DB::Cas::RefOwnerBinding{DB::Cas::RefOwnerKind::Committed, final_ref_name, *old_ref}, std::nullopt));
    ops.push_back(ownerTransitionOp(
        std::nullopt, DB::Cas::RefOwnerBinding{DB::Cas::RefOwnerKind::Precommit, final_ref_name, new_ref}));
    const uint64_t sequence = appendRefLogSeed(backend, layout, ns, std::move(ops));
    publishRecoverableCkptForSemanticWrapper(backend, layout, ns, RefTxnId{1, sequence});
    return sequence;
}

/// Promote a precommit to committed at the SAME manifest_ref (old=Precommit, new=Committed). No edge
/// (net-zero owner move). `build_id` is dropped. Returns the allocated `ref_sequence`.
inline uint64_t promoteTransition(
    DB::Cas::Backend & backend, const DB::Cas::Layout & layout, const DB::Cas::RootNamespace & ns,
    const DB::UInt128 & /*build_id*/, const String & final_ref_name, const DB::Cas::ManifestRef & ref,
    uint64_t /*shard*/ = 0)
{
    const uint64_t sequence = appendRefLogSeed(backend, layout, ns,
        {ownerTransitionOp(
            DB::Cas::RefOwnerBinding{DB::Cas::RefOwnerKind::Precommit, final_ref_name, ref},
            DB::Cas::RefOwnerBinding{DB::Cas::RefOwnerKind::Committed, final_ref_name, ref})});
    publishRecoverableCkptForSemanticWrapper(backend, layout, ns, RefTxnId{1, sequence});
    return sequence;
}

/// Exact-token delete of a manifest body (HEAD then deleteExact). No-op when absent.
inline void deleteManifestBody(
    DB::Cas::Backend & backend, const DB::Cas::Layout & layout, const DB::Cas::ManifestId & id)
{
    const String key = layout.manifestKey(id);
    const DB::Cas::HeadResult h = backend.head(key);
    if (h.exists)
        backend.deleteExact(key, h.token);
}

/// Formerly wrote the namespace into `gc/registry`. Real write helpers now admit the authoritative
/// catalog row themselves, so this legacy fixture hook has no independent registration work.
inline void registerNamespaceRaw(
    DB::Cas::Backend & /*backend*/, const DB::Cas::Layout & /*layout*/, const DB::Cas::RootNamespace & /*ns*/)
{
    /// No-op: Task 4 deleted the registry; `cas/ref_catalog` is now the discovery authority.
}

/// Encode a CAGS document carrying only {round} — everything else defaulted. Callers that only care
/// about this field (e.g. `injectRetire`) use this shorthand.
inline String encodeMinimalGcState(uint64_t round)
{
    DB::Cas::GcState state;
    state.round = round;
    return DB::Cas::encodeGcState(state);
}

/// Inject condemned bookkeeping + gc/state directly (bypassing a real GC round) so a test can seed the
/// GC ledger's condemned state at an arbitrary round. Retired-in-snapshot: the condemned entries are
/// seeded the way a real round leaves them — as `kCondemned` sentinel rows inside an adopted fold seal's
/// shard run (there is no separate retired-list object). A synthetic +edge/-edge pair nets each blob to
/// in-degree 0 and a `seed_head` replays the captured token/size so the fold mints the `kCondemned` row.
/// Also sets {round} on gc/state. Entries carry a `condemn_round` (default 0 → uses `round`); callers
/// pass fresh (non-pending) condemns. An empty `entries` set just advances {round}.
inline void injectRetire(
    DB::Cas::Backend & backend, const DB::Cas::Layout & layout,
    uint64_t round, uint64_t shard, std::vector<DB::Cas::RetiredEntry> entries)
{
    DB::Cas::GcState gc_state;
    const DB::Cas::HeadResult head = backend.head(layout.gcStateKey());
    if (head.exists)
        gc_state = DB::Cas::decodeGcState(backend.get(layout.gcStateKey())->bytes);
    gc_state.round = round;

    if (!entries.empty())
    {
        const uint64_t generation = 1;
        const uint64_t attempt = 1;
        uint64_t condemn_round = round;
        std::unordered_map<DB::Cas::BlobRef, DB::Cas::HeadResult, DB::Cas::BlobRefHash> seeded;
        std::vector<DB::Cas::BlobDelta> synth;
        synth.reserve(entries.size() * 2);
        for (const DB::Cas::RetiredEntry & e : entries)
        {
            if (e.condemn_round)
                condemn_round = e.condemn_round;
            seeded.emplace(e.ref, DB::Cas::HeadResult{.exists = true, .size = e.size, .token = e.token, .attributes = {}});
            synth.push_back(DB::Cas::BlobDelta{.ref = e.ref, .source_id = DB::UInt128{1}, .remove = false});
            synth.push_back(DB::Cas::BlobDelta{.ref = e.ref, .source_id = DB::UInt128{1}, .remove = true});
        }
        const auto seed_head = [&seeded](const DB::Cas::BlobRef & h) -> std::optional<DB::Cas::HeadResult>
        {
            const auto it = seeded.find(h);
            return it == seeded.end() ? std::nullopt : std::optional<DB::Cas::HeadResult>(it->second);
        };
        std::vector<DB::Cas::RunRef> out;
        DB::Cas::foldDeltasIntoGeneration(backend, layout, /*prior_runs*/{}, generation, attempt,
            shard, std::move(synth), out, /*current_round*/0, condemn_round, seed_head,
            /*peek_head*/{}, /*confirm_condemned_marker*/{},
            /*out_retired*/nullptr, /*suppress_destructive*/false);

        DB::Cas::CasFoldSeal seal;
        seal.generation = generation;
        for (DB::Cas::RunRef & r : out)
            seal.blob_target_runs.push_back(std::move(r));
        /// Totality over gc_shards so a later real round's graduation/carry reads it zero-I/O.
        const uint64_t gc_shards = gc_state.gc_shards ? gc_state.gc_shards : 1;
        for (uint64_t s = 0; s < gc_shards; ++s)
            seal.condemned_summary[s] = DB::Cas::CondemnedSummary{};
        DB::Cas::CondemnedSummary cs;
        cs.condemned_total = entries.size();
        cs.oldest_nonpending_condemn_round = condemn_round;
        seal.condemned_summary[shard] = cs;
        backend.putIfAbsent(layout.foldSealKey(generation, attempt), DB::Cas::encodeFoldSeal(seal));

        gc_state.snap_generation = generation;
        gc_state.snap_attempt = attempt;
    }

    const String state = DB::Cas::encodeGcState(gc_state);
    if (!head.exists)
        backend.putIfAbsent(layout.gcStateKey(), state);
    else
        backend.putOverwrite(layout.gcStateKey(), state, head.token);
}

/// Adopt a fold seal carrying a given per-gc-shard `condemned_summary` (retired-in-snapshot T4) and point
/// gc/state at it (snap_generation / snap_attempt / gc_shards), bypassing a real GC round. If a seal
/// already exists at (generation, attempt) it is overwritten with the new summary (its other fields are
/// preserved); otherwise a fresh minimal seal is created. Read-modify-CAS on gc/state preserves the lease.
/// Used by graduationDue tests to drive the zero-I/O signal directly off a controlled seal.
inline void injectCondemnedSummarySeal(
    DB::Cas::Backend & backend, const DB::Cas::Layout & layout,
    uint64_t generation, uint64_t attempt, uint64_t gc_shards,
    const std::map<uint64_t, DB::Cas::CondemnedSummary> & summary)
{
    const String seal_key = layout.foldSealKey(generation, attempt);
    DB::Cas::CasFoldSeal seal;
    const auto existing = backend.get(seal_key);
    if (existing)
        seal = DB::Cas::decodeFoldSeal(existing->bytes);
    else
        seal.parent_generation = generation ? generation - 1 : 0;
    seal.generation = generation;
    seal.condemned_summary = summary;
    const String seal_bytes = DB::Cas::encodeFoldSeal(seal);
    if (existing)
        backend.putOverwrite(seal_key, seal_bytes, existing->token);
    else
        backend.putIfAbsent(seal_key, seal_bytes);

    DB::Cas::GcState gc_state;
    const DB::Cas::HeadResult head = backend.head(layout.gcStateKey());
    if (head.exists)
        gc_state = DB::Cas::decodeGcState(backend.get(layout.gcStateKey())->bytes);
    gc_state.gc_shards = gc_shards;
    gc_state.snap_generation = generation;
    gc_state.snap_attempt = attempt;
    const String state = DB::Cas::encodeGcState(gc_state);
    if (!head.exists)
        backend.putIfAbsent(layout.gcStateKey(), state);
    else
        backend.putOverwrite(layout.gcStateKey(), state, head.token);
}

/// Whether blob `hash` is absent from the backend (its exact-token content object is gone).
inline bool blobAbsent(DB::Cas::Backend & backend, const DB::Cas::Layout & layout, const DB::UInt128 & hash)
{
    return !backend.head(layout.blobKey(DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(hash)})).exists;
}

/// ONE round that is allowed to RECLAIM -- the name is the point, so that grepping for the tests whose
/// subject is reclamation finds exactly them.
///
/// The policy is spelled out rather than defaulted so that grepping this name finds every test whose
/// subject is reclamation, and so that a future change to the default cannot silently change what those
/// tests mean. It says the same thing the production default says (`UniversePolicy`); a test whose
/// subject is a SUPPRESSOR passes `StageA_Suppressed` explicitly instead.
inline DB::Cas::RoundReport runRegularRoundReclaiming(DB::Cas::Gc & gc)
{
    return gc.runRegularRound({}, /*allow_steal*/true, DB::Cas::UniversePolicy::Authoritative);
}

/// Reclaim loop (the canonical retired-cursor pipeline driver): run regular rounds, renewing the store's
/// own heartbeat after each round (`renewWatermarkOnce` — keeps the lease + build-watermark floor
/// current; unrelated to graduation, which paces on GC rounds alone). A blob condemned at round K is
/// deleted by round K+2 (condemn at K -> graduate to delete_pending at K+1, unconditionally -> physical
/// delete at K+2). Returns true as soon as the blob became absent. Reclamation is the whole point of the
/// loop, so every round it drives is an authoritative one.
inline bool runRoundsUntilAbsent(
    const DB::Cas::PoolPtr & store, DB::Cas::Gc & gc, DB::Cas::Backend & backend,
    const DB::Cas::Layout & layout, const DB::UInt128 & hash, int max_rounds = 8)
{
    for (int i = 0; i < max_rounds; ++i)
    {
        runRegularRoundReclaiming(gc);
        store->renewWatermarkOnce();
        if (blobAbsent(backend, layout, hash))
            return true;
    }
    return blobAbsent(backend, layout, hash);
}

/// The CURRENT condemned entries for `shard`, read from the adopted fold seal's `blob_target_runs`
/// (retired-in-snapshot T4): the round no longer writes a separate retired-list object — condemned
/// entries RIDE the source-edge run as `kCondemned` sentinel rows at the zero-sentinel key. This reads
/// the seal at (snap_generation, snap_attempt), opens every run for `shard`, and reconstructs the
/// `RetiredEntry` shape (hash from the run key, the rest from the decoded `CondemnedRow`). Empty when
/// gc/state / the seal / the runs are absent. Used by ack-floor tests to assert pending/condemn state.
inline std::vector<DB::Cas::RetiredEntry> currentRetiredSet(
    DB::Cas::Backend & backend, const DB::Cas::Layout & layout, uint64_t shard)
{
    const auto st = backend.get(layout.gcStateKey());
    if (!st)
        return {};
    const DB::Cas::GcState gc_state = DB::Cas::decodeGcState(st->bytes);
    if (gc_state.snap_generation == 0)
        return {};
    const auto seal_bytes = backend.get(layout.foldSealKey(gc_state.snap_generation, gc_state.snap_attempt));
    if (!seal_bytes)
        return {};
    const DB::Cas::CasFoldSeal seal = DB::Cas::decodeFoldSeal(seal_bytes->bytes);

    std::vector<DB::Cas::RetiredEntry> out;
    for (const DB::Cas::RunRef & run : seal.blob_target_runs)
    {
        if (run.shard != shard)
            continue;
        auto r = DB::Cas::openSourceEdgeRun(backend, run.key);
        String k;
        String p;
        while (r.next(k, p))
        {
            if (p.empty() || p[0] != DB::Cas::kCondemned)
                continue;
            DB::Cas::BlobRef ref;
            DB::UInt128 source_id{};
            DB::Cas::SourceEdgeKeyCodec::parse(k, ref, source_id);   // throws CORRUPTED_DATA on malformed (fail-closed)
            const DB::Cas::CondemnedRow row = DB::Cas::decodeCondemnedRow(p);
            out.push_back(DB::Cas::RetiredEntry{
                .kind = DB::Cas::ObjectKind::Blob,
                .ref = ref,
                .token = row.token,
                .size = row.size,
                .condemn_round = row.condemn_round,
                .delete_pending = row.delete_pending,
                .marker_confirmed = row.marker_confirmed});
        }
    }
    return out;
}

/// True iff ANY gc-shard's adopted-seal run still holds a `kCondemned` row — the ack-floor deletion
/// pipeline is in flight while this is true (retired-in-snapshot T4 replacement for the old
/// "iterate gc/state.retired_refs" probe). `gc_shards` is read from gc/state when 0 is passed.
inline bool anyCondemnedInSeal(
    DB::Cas::Backend & backend, const DB::Cas::Layout & layout, uint64_t gc_shards = 0)
{
    const auto st = backend.get(layout.gcStateKey());
    if (!st)
        return false;
    const DB::Cas::GcState gc_state = DB::Cas::decodeGcState(st->bytes);
    const uint64_t shards = gc_shards ? gc_shards : gc_state.gc_shards;
    for (uint64_t shard = 0; shard < shards; ++shard)
        if (!currentRetiredSet(backend, layout, shard).empty())
            return true;
    return false;
}

/// Displace a blob's incarnation out-of-band (as a racing writer would): GET it, mint a fresh
/// incarnation_tag in its envelope header (preserving header_len + payload), putOverwrite against the
/// current token, and return the NEW token. Used to drive the W-REVALIDATE adopt branch (current token
/// differs from the writer's stale observation).
inline DB::Cas::Token displaceObjectToken(
    DB::Cas::Backend & backend, const String & key, DB::Cas::ObjectKind kind)
{
    const auto got = backend.get(key);
    if (!got)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "displaceObjectToken: object {} absent", key);

    DB::Cas::EnvelopeHeader header =
        DB::Cas::decodeEnvelopeHeader(got->bytes, got->bytes.size(), kind);
    /// A fresh, distinct incarnation_tag forces a distinct body so the displaced token differs.
    header.incarnation_tag = header.incarnation_tag + DB::UInt128(1);
    /// Re-encode at the SAME header length the object was decoded with (the v3 pad target).
    const String new_head = DB::Cas::encodeEnvelopeHeader(header, header.header_len);
    const String body = new_head + got->bytes.substr(header.header_len);

    return backend.putOverwrite(key, body, got->token).token;
}

inline DB::Cas::Token displaceBlobToken(
    DB::Cas::Backend & backend, const DB::Cas::Layout & layout, const DB::Cas::BlobRef & id)
{
    return displaceObjectToken(backend, layout.blobKey(id), DB::Cas::ObjectKind::Blob);
}

/// ---- GC-core (Phase 1d) test helpers over the part-manifest model ----

/// Open a Pool over `backend`.
///
/// `gc_fold_max_defer_rounds` defaults to the PoolConfig default (8) -- unchanged behaviour for every
/// existing caller. A test that drives MANY consecutive genuinely-idle `runRegularRound` calls and
/// asserts each one performs a full fold (round/generation advance, trim/sweep/retention) -- exactly
/// what Phase-4 Lever A (spec 2026-07-06-cas-gc-round-skip-unchanged) is designed to skip -- passes 0
/// here to force fold-every-round (shouldDeferRound's liveness bound: rounds_since_last_fold(0) >= 0
/// is always true).
inline DB::Cas::PoolPtr openPoolForTest(
    std::shared_ptr<DB::Cas::InMemoryBackend> backend, uint64_t gc_fold_max_defer_rounds = 8)
{
    return DB::Cas::Pool::open(std::move(backend),
        DB::Cas::PoolConfig{.pool_prefix = "p", .server_root_id = "test",
                            .gc_fold_max_defer_rounds = gc_fold_max_defer_rounds});
}

/// Seed the mandatory control objects for an already-existing pool so a subsequent `Pool::open`
/// VALIDATES a restart instead of bootstrapping a fresh one. Recovery/replay tests seed ref-log,
/// snapshot, manifest, or gc-state residue directly into a bare backend; in production such residue
/// only ever exists inside a pool whose FIRST open already minted `_pool_meta` and explicitly
/// initialized `cas/ref_catalog`. Task 7's zero-write bootstrap check (spec §2 [C4][D2],
/// `probePoolBootstrapResidual`) REFUSES to bootstrap over residual data, so a raw restart fixture must
/// establish both mandatory objects itself rather than rely on a production fallback.
///
/// Idempotent: `createOrValidate` validates an existing `_pool_meta`. The catalog initializer is
/// deliberately narrower: it accepts only a canonical EMPTY conflict, so raw recovery fixtures that
/// have already populated their catalog must mandatory-read and validate it instead of re-running a
/// new-pool initializer. The default `blob_header_len`/`blob_hash_algo` match `PoolConfig`'s defaults,
/// so a later `Pool::open` with a default config validates cleanly.
inline void seedPoolMetaForRestart(
    DB::Cas::Backend & backend, const String & pool_prefix = "p", uint64_t gc_shards = 1)
{
    const DB::Cas::Layout layout(pool_prefix);
    DB::Cas::PoolMeta::createOrValidate(
        backend, layout, /*blob_header_len=*/256, gc_shards,
        DB::Cas::BlobHashAlgo::CityHash128, /*allow_new=*/false, /*allow_mint=*/true);
    if (!backend.get(layout.refCatalogKey()))
        DB::Cas::CasRefCatalog::initializeEmptyForNewPool(backend, layout);
    else
        (void)DB::Cas::CasRefCatalog::read(backend, layout);
}

/// Write a blob object (envelope + payload) addressed by `hash`, so a HEAD returns a token. The bytes
/// are arbitrary (GC never reads them); the hash is what the manifest entry references.
inline void writeBlobBody(
    DB::Cas::Backend & backend, const DB::Cas::Layout & layout, const DB::UInt128 & hash,
    uint64_t blob_header_len = 256)
{
    DB::Cas::EnvelopeHeader header;
    header.kind = DB::Cas::ObjectKind::Blob;
    header.incarnation_tag = DB::UInt128(0x1234);
    header.build_id = DB::UInt128(0x5678);
    const String head = DB::Cas::encodeEnvelopeHeader(header, static_cast<uint32_t>(blob_header_len));
    backend.putIfAbsent(layout.blobKey(DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(hash)}), head + String("x"));
}

/// Write a raw blob body (payload written verbatim, no envelope) — the raw-body-refinement shape
/// (Phase B): the meta descriptor (via the ops layer below) carries all state, the body carries none.
inline void writeRawBlobBody(DB::Cas::Backend & backend, const DB::Cas::Layout & layout,
                             const DB::UInt128 & hash, const String & payload)
{
    backend.casPut(layout.blobKey(DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(hash)}), payload, std::nullopt);
}

/// These `UInt128`-hash meta-op wrappers are the pre-mixed-algo 128-bit-only test convenience surface:
/// every existing caller operates on a 128-bit (`cityHash128`) test pool, so the ref is built at
/// `CityHash128` here. The shared `.meta` API (Phase 3 T3) is `BlobRef`-keyed directly and derives its
/// own codec internally — no codec is threaded from here anymore.
inline DB::Cas::BlobRef legacyMetaTestRef(const DB::UInt128 & hash)
{
    return DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(hash)};
}

/// Create a Clean meta descriptor for `hash` directly in a test backend. This setup helper deliberately
/// stays usable before a `Pool` is open; production writes use `putMetaIfAbsent` through the controller.
inline void writeMetaClean(DB::Cas::Backend & backend, const DB::Cas::Layout & layout,
                           const DB::UInt128 & hash, uint64_t size)
{
    const DB::Cas::BlobRef ref = legacyMetaTestRef(hash);
    backend.putIfAbsent(layout.blobMetaKey(ref), DB::Cas::encodeBlobMeta(
        DB::Cas::BlobMeta{.state = DB::Cas::MetaState::Clean, .condemn_round = 0, .size = size}));
}

/// Transition an existing meta descriptor to Condemned at `condemn_round`, via a read-modify-CAS on
/// its current token (asserts the meta exists — a direct test setup helper, not production code).
inline void condemnMeta(DB::Cas::Backend & backend, const DB::Cas::Layout & layout,
                        const DB::UInt128 & hash, uint64_t condemn_round)
{
    const DB::Cas::BlobRef ref = legacyMetaTestRef(hash);
    const auto lm = DB::Cas::loadMeta(backend, layout, ref);
    ASSERT_TRUE(lm.has_value());
    DB::Cas::BlobMeta c = lm->meta;
    c.state = DB::Cas::MetaState::Condemned;
    c.condemn_round = condemn_round;
    backend.putOverwrite(layout.blobMetaKey(ref), DB::Cas::encodeBlobMeta(c), lm->etag);
}

/// Load the meta descriptor for `hash` via the shared ops layer (nullopt = absent).
inline std::optional<DB::Cas::LoadedMeta> loadMetaForTest(DB::Cas::Backend & backend,
                                                          const DB::Cas::Layout & layout, const DB::UInt128 & hash)
{
    return DB::Cas::loadMeta(backend, layout, legacyMetaTestRef(hash));
}

/// The latest GC generation (snap_generation pointer in gc/state), or 0 when absent.
inline uint64_t currentGenerationOf(DB::Cas::Backend & backend, const DB::Cas::Layout & layout)
{
    const auto got = backend.get(layout.gcStateKey());
    if (!got)
        return 0;
    return DB::Cas::decodeGcState(got->bytes).snap_generation;
}

/// The adopted attempt (snap_attempt pointer in gc/state), or 0 when absent.
inline uint64_t currentAttemptOf(DB::Cas::Backend & backend, const DB::Cas::Layout & layout)
{
    const auto got = backend.get(layout.gcStateKey());
    if (!got)
        return 0;
    return DB::Cas::decodeGcState(got->bytes).snap_attempt;
}

/// The current seal's `blob_target_runs` filtered to `shard` (2026-07-02 T0: consumers resolve runs
/// through seal refs, not by key construction). Scans downward from the current generation for the most
/// recent existing fold seal (mirrors `foldCursorOf`'s reasoning); absent => empty.
inline std::vector<DB::Cas::RunRef> runsForShard(
    DB::Cas::Backend & backend, const DB::Cas::Layout & layout, uint64_t shard)
{
    const uint64_t gen = currentGenerationOf(backend, layout);
    const uint64_t attempt = currentAttemptOf(backend, layout);
    for (uint64_t g = gen; ; --g)
    {
        if (const auto got = backend.get(layout.foldSealKey(g, attempt)))
        {
            const DB::Cas::CasFoldSeal seal = DB::Cas::decodeFoldSeal(got->bytes);
            std::vector<DB::Cas::RunRef> out;
            for (const DB::Cas::RunRef & r : seal.blob_target_runs)
                if (r.shard == shard)
                    out.push_back(r);
            return out;
        }
        if (g == 0)
            return {};
    }
}

/// Stream the sealed in-degree run segments `runs` and count the active source edges (`kEdgeActive`
/// rows) for `ref`. Test-side replacement for the deleted per-blob point query `inDegreeInGeneration`
/// (codecs-v3 phase 5: a `cas_run` is a sequential NDJSON stream with no random access, so a blob's
/// in-degree is recomputed by a full stream-and-count rather than a seek). A condemned / zero-marker
/// row is not an active edge, so it contributes 0 — matching the old point query's semantics.
inline int64_t inDegreeInRuns(
    DB::Cas::Backend & backend, const std::vector<DB::Cas::RunRef> & runs, const DB::Cas::BlobRef & ref)
{
    int64_t degree = 0;
    for (const DB::Cas::RunRef & run : runs)
    {
        auto r = DB::Cas::openSourceEdgeRun(backend, run.key);
        String k;
        String p;
        while (r.next(k, p))
        {
            if (p.empty() || p[0] != DB::Cas::kEdgeActive)
                continue;
            DB::Cas::BlobRef row_ref;
            DB::UInt128 source_id{};
            DB::Cas::SourceEdgeKeyCodec::parse(k, row_ref, source_id);   // throws CORRUPTED_DATA on malformed (fail-closed)
            if (row_ref == ref)
                ++degree;
        }
    }
    return degree;
}

/// The in-degree of a blob in the current GC generation's sealed run (0 when absent/zeroed).
inline int64_t inDegreeOf(DB::Cas::Backend & backend, const DB::Cas::Layout & layout, const DB::UInt128 & hash)
{
    return inDegreeInRuns(backend, runsForShard(backend, layout, /*shard*/0), legacyMetaTestRef(hash));
}

/// The single named entry point for the nonproduction CA shapes this test tree constructs directly,
/// rather than through the production birth/write paths. Every raw fixture below is one of three
/// deliberate divergences from what production can ever produce, gathered here under one name so a
/// future change to any of them has exactly one place to change, not every call site that needs it:
///   1. `fixtureLife` returns a DETERMINISTIC namespace-derived life identity, never a fresh random
///      mint the way a real birth (`CasRefCatalog::createNamespace`) would -- opaque and catalog-born
///      in production, but every raw fixture below needs to derive the SAME identity a namespace's
///      catalog entry will carry before that entry exists, so its writes and a later read agree on
///      where to look.
///   2. `admitLive` reaches `Live` with NO `_ckpt` at all. Production only ever reaches `Live` through
///      `completeCreation`, which publishes `_ckpt` FIRST; this shape is kept deliberately, because
///      recovery and failure tests need to exercise a `Live` or `Removing` row missing that authority.
///   3. `writeRefLogRaw` writes ref-log bytes directly at the resolved fixture identity, bypassing the
///      writer's own birth/append lane entirely -- exercising the on-storage object shape a real writer
///      would emit without driving a real writer to produce it.
namespace fixture
{
    /// The deterministic identity a raw fixture uses for a namespace before any catalog entry exists:
    /// a stable hash of the namespace name, so two fixture writes against the same namespace (and a
    /// later read) always agree on where to look, without needing a catalog entry to agree through.
    /// Production incarnations are always catalog-minted (`CasRefCatalog::createNamespace`); this is
    /// deliberately not that, and every raw fixture below depends on it staying stable byte-for-byte.
    inline DB::Cas::NamespaceLifeId fixtureLife(const DB::Cas::RootNamespace & ns)
    {
        UInt128 fixture_incarnation = sipHash128(ns.string().data(), ns.string().size());
        if (fixture_incarnation == 0)
            fixture_incarnation = 1;
        return DB::Cas::NamespaceLifeId::fromCatalogEntry(ns, fixture_incarnation);
    }
}

/// Resolve the opaque life id that keys this namespace's single fold-coverage row.
inline UInt128 catalogLifeIdForTest(
    DB::Cas::Backend & backend, const DB::Cas::Layout & layout, const DB::Cas::RootNamespace & ns)
{
    const std::optional<DB::Cas::NamespaceLifeId> life =
        DB::Cas::CasRefCatalog::lifeIfCataloged(backend, layout, ns);
    chassert(life.has_value());
    return life->incarnation;
}

/// Seed the ADOPTED fold seal's catalog-life coverage row for `ns` and point `gc/state` at it, bypassing a
/// real round. This is the durable fact the sweep's §6 deletion premise reads
/// (`CasOrphanManifestSweep.cpp`): `cursor` is the namespace's `last_folded_ref_id`, and a manifest of
/// an epoch-`E` build is deletable only once that cursor sits in an epoch STRICTLY above `E`.
/// `hold`, when set, makes the row classification 4 — the strict grammar `encodeFoldSeal` enforces in
/// both directions, so a hold and a non-4 classification cannot be seeded together.
///
/// SHARP EDGE, HANDLED HERE SO NO CALLER HAS TO KNOW IT: a fold seal must carry a `condemned_summary`
/// entry for EVERY shard in `0..gc_shards-1`. A later real round adopts this object as its PARENT and
/// throws `CORRUPTED_DATA` — "parent fold seal (generation G, attempt A) lacks a condemned_summary
/// entry for gc-shard N — the seal is not total over gc_shards" — on a seal that is missing one. The
/// symptom is nowhere near the cause: the round fails at fold time, or (if it fails before taking the
/// lease) merely reports `acquired_lease == false`, so a test that seeds a partial seal looks like a
/// leadership problem. This helper fills the map from `gc/state`'s own `gc_shards`, so seeding a
/// coverage row is safe to combine with real rounds.
///
/// One thing it does NOT do: create `gc/state` in a state a first-ever `Gc` round can take the lease
/// over. `acquireOrRenewLease` only creates-and-owns when `gc/state` is ABSENT, so seeding before the
/// first round makes that round back off. Seed AFTER the first round (passing that round's
/// `currentGenerationOf`/`currentAttemptOf`) when a test drives real rounds.
inline void seedFoldCursorForTest(
    DB::Cas::Backend & backend, const DB::Cas::Layout & layout, const DB::Cas::RootNamespace & ns,
    DB::Cas::RefTxnId cursor, std::optional<DB::Cas::RefHold> hold = std::nullopt,
    uint64_t generation = 1, uint64_t attempt = 1)
{
    DB::Cas::NamespaceLifeId life = fixture::fixtureLife(ns);
    const DB::Cas::CasRefCatalog::Snapshot catalog_cut = DB::Cas::CasRefCatalog::read(backend, layout);
    const auto catalog_it = std::find_if(
        catalog_cut.catalog.entries.begin(), catalog_cut.catalog.entries.end(),
        [&](const DB::Cas::CatalogEntry & entry) { return entry.ns.string() == ns.string(); });
    if (catalog_it == catalog_cut.catalog.entries.end())
    {
        DB::Cas::CatalogEntry entry;
        entry.ns = ns;
        entry.state = DB::Cas::NsState::Live;
        entry.incarnation = fixture::fixtureLife(ns).incarnation;
        DB::Cas::CasRefCatalog::casAdmitEntry(backend, layout, 1, entry);
        life = DB::Cas::NamespaceLifeId::fromCatalogEntry(ns, entry.incarnation);
    }
    else
    {
        life = DB::Cas::NamespaceLifeId::fromCatalogEntry(ns, catalog_it->incarnation);
    }

    const String seal_key = layout.foldSealKey(generation, attempt);
    DB::Cas::CasFoldSeal seal;
    const auto existing = backend.get(seal_key);
    if (existing)
        seal = DB::Cas::decodeFoldSeal(existing->bytes);
    seal.generation = generation;

    DB::Cas::RefCoverage cov;
    cov.classification = hold ? 4 : 2;
    cov.last_folded_ref_id = cursor;
    cov.hold = hold;
    seal.ref_lives[life.incarnation].coverage = cov;

    DB::Cas::GcState gc_state;
    const DB::Cas::HeadResult head = backend.head(layout.gcStateKey());
    if (head.exists)
        gc_state = DB::Cas::decodeGcState(backend.get(layout.gcStateKey())->bytes);

    /// Totality over `gc_shards` — see the doc comment's SHARP EDGE note for what throws without it.
    const uint64_t gc_shards = gc_state.gc_shards ? gc_state.gc_shards : 1;
    for (uint64_t s = 0; s < gc_shards; ++s)
        seal.condemned_summary.emplace(s, DB::Cas::CondemnedSummary{});

    const String seal_bytes = DB::Cas::encodeFoldSeal(seal);
    if (existing)
        backend.putOverwrite(seal_key, seal_bytes, existing->token);
    else
        backend.putIfAbsent(seal_key, seal_bytes);

    gc_state.snap_generation = generation;
    gc_state.snap_attempt = attempt;
    const String state = DB::Cas::encodeGcState(gc_state);
    if (!head.exists)
        backend.putIfAbsent(layout.gcStateKey(), state);
    else
        backend.putOverwrite(layout.gcStateKey(), state, head.token);
}

/// The folded cursor sealed for (ns, shard) by the latest fold seal, or 0 when absent. After a COMPLETE
/// round the gc/state generation pointer is the recheck's COMPLETION generation (G+2 for a round started
/// at G), but the fold seal is written at the FOLD generation (G+1) — recheck writes a completion seal,
/// not a fold seal. So scan downward from the current generation for the most recent existing fold seal.
inline uint64_t foldCursorOf(
    DB::Cas::Backend & backend, const DB::Cas::Layout & layout, const DB::Cas::RootNamespace & ns, uint64_t shard)
{
    chassert(shard == 0);
    const std::optional<DB::Cas::NamespaceLifeId> life =
        DB::Cas::CasRefCatalog::lifeIfCataloged(backend, layout, ns);
    if (!life)
        return 0;
    const uint64_t gen = currentGenerationOf(backend, layout);
    const uint64_t attempt = currentAttemptOf(backend, layout);
    for (uint64_t g = gen; ; --g)
    {
        if (const auto got = backend.get(layout.foldSealKey(g, attempt)))
        {
            const DB::Cas::CasFoldSeal seal = DB::Cas::decodeFoldSeal(got->bytes);
            const auto it = seal.ref_lives.find(life->incarnation);
            /// Snapshot+log ref model: the per-table durable cursor is `last_folded_ref_id` (a RefTxnId).
            /// Seeds allocate `writer_epoch = 1`, so the `ref_sequence` is the monotone cursor the seeding
            /// wrappers return and tests compare against.
            return it != seal.ref_lives.end() ? it->second.coverage.last_folded_ref_id.ref_sequence : 0;
        }
        if (g == 0)
            return 0;
    }
}

/// Set a server root's durable floor (so orphan-sweep eligibility can be driven). After the ack-floor
/// merge the floor rides the mount lease body (`mountKey`), so this seeds a MountLease carrying
/// `{writer_epoch, min_active}` — exactly what `prefixEligible` reads.
inline void setWatermarkMinActive(
    DB::Cas::Backend & backend, const DB::Cas::Layout & layout, const String & server_root_id,
    uint64_t writer_epoch, uint64_t min_active)
{
    DB::Cas::MountLease m;
    m.server_uuid = DB::UInt128(0);
    m.writer_epoch = writer_epoch;
    m.min_active = min_active;
    m.seq = 1;
    m.write_attempt_id = DB::UInt128{1};
    const String key = layout.mountKey(server_root_id);
    const DB::Cas::HeadResult h = backend.head(key);
    if (h.exists)
        backend.putOverwrite(key, DB::Cas::encodeMountLease(m), h.token);
    else
        backend.putIfAbsent(key, DB::Cas::encodeMountLease(m));
}

/// ---- Task 10 ref snapshot+log raw fixtures ----
/// Mirror the pre-Task-10 `appendOwnerEvent`/`publishRaw` helpers above, but for the new snapshot+log
/// object layout: write a ref-object body directly via the SAME codecs `Pool`'s recovery reads,
/// bypassing the writer's own append lane entirely. Used to seed pre-existing table state before a
/// fresh `Pool` ever touches the namespace (recovery tests), and to control exact keys/bytes
/// (restart-on-vanish tests).

/// Writes `snapshot` at `_snap/<snapshot_id>.proto` (create-if-absent). Keys at whichever life the
/// namespace's catalog entry ALREADY names (a prior real birth's random incarnation, or the sentinel
/// if none exists yet -- see `writeRefLogTxnRaw`'s identical note); does NOT itself admit an entry, so
/// a namespace this helper is the ONLY writer for stays exactly as invisible to the catalog as it was
/// before Task 4-C (unchanged from this helper's own pre-existing scope).
inline void writeRefSnapshotRaw(
    DB::Cas::Backend & backend, const DB::Cas::Layout & layout, const DB::Cas::RefTableSnapshot & snapshot)
{
    const DB::Cas::RootNamespace ns{snapshot.ns};
    const NamespaceLifeId life = CasRefCatalog::lifeIfCataloged(backend, layout, ns).value_or(fixture::fixtureLife(ns));
    const String key = layout.refSnapshotKey(life, snapshot.snapshot_id);
    backend.putIfAbsent(key, DB::Cas::sealObject(DB::Cas::FormatId::RefSnapshot, DB::Cas::encodeRefTableSnapshot(snapshot)));
}

/// Admits `ns` into the catalog as a `Live` entry, IDEMPOTENTLY (a no-op once `ns` already carries
/// any entry, of any state -- a test that drove one there itself through the real catalog API is left
/// alone). Pinned to the deterministic `fixture::fixtureLife` incarnation, NOT
/// `CasRefCatalog::createNamespace`'s fresh-random mint: every raw fixture below keys its ref-log/
/// snapshot objects at that SAME derived id, so a randomly minted incarnation would not match them and
/// the fold's own R10 incarnation filter (`{#r10-groupref-alias}`) would drop every one of their keys
/// as belonging to a dead life.
///
/// All ten raw-write helpers place ref-log bytes at states production's real birth path structurally
/// cannot produce (INV-1 holes, out-of-order ids, a table with no `_ckpt` -- see the Task 4-B map), so
/// they can never route through `createNamespace` and mint a real incarnation of their own.
///
/// TWO DIVERGENCES from what `createNamespace`/`completeCreation` would produce, both deliberate and
/// both left as-is rather than "fixed":
///   1. the incarnation is a deterministic namespace-derived fixture id, not a fresh random mint;
///   2. this entry reaches `Live` with NO `_ckpt` at all, whereas production only ever reaches `Live`
///      through `completeCreation`, which publishes `_ckpt` FIRST (INV-4). Several fixtures exist
///      SPECIFICALLY to build a table with no `_ckpt`, but they must exercise that corruption directly:
///      lifecycle-authoritative recovery correctly rejects a `Live` or `Removing` row without a
///      readable `life_epoch`. Ordinary fixtures use `casAdmitRecoverableEntry` below instead.
inline void casAdmitEntry(DB::Cas::Backend & backend, const DB::Cas::Layout & layout, const DB::Cas::RootNamespace & ns)
{
    const CasRefCatalog::Snapshot snap = CasRefCatalog::read(backend, layout);
    for (const CatalogEntry & entry : snap.catalog.entries)
        if (entry.ns.string() == ns.string())
            return;   /// already admitted -- by an earlier raw write to the same namespace, or by the
                      /// test itself
    CatalogEntry entry;
    entry.ns = ns;
    entry.state = NsState::Live;
    entry.incarnation = fixture::fixtureLife(ns).incarnation;
    CasRefCatalog::casAdmitEntry(backend, layout, 1, entry);
}

namespace fixture
{
    /// The admit-Live-without-`_ckpt` pattern (divergence 2 above), reachable through the seam.
    inline void admitLive(DB::Cas::Backend & backend, const DB::Cas::Layout & layout, const DB::Cas::RootNamespace & ns)
    {
        casAdmitEntry(backend, layout, ns);
    }
}

/// Write the checkpoint frontier that makes a raw `Live` fixture a normal recoverable life. Raw logs
/// intentionally do not synthesize `_ckpt`: many tests need the missing-checkpoint corruption shape.
/// A test that invokes lifecycle-authoritative recovery therefore has to state its exact frontier here.
inline void writeRecoverableCkptForRawFixture(
    DB::Cas::Backend & backend, const DB::Cas::Layout & layout, const DB::Cas::RootNamespace & ns,
    const DB::Cas::RefCkpt & ckpt)
{
    const CasRefCatalog::Snapshot catalog_cut = CasRefCatalog::read(backend, layout);
    const auto it = std::find_if(
        catalog_cut.catalog.entries.begin(), catalog_cut.catalog.entries.end(),
        [&] (const CatalogEntry & entry) { return entry.ns == ns; });
    if (it == catalog_cut.catalog.entries.end())
        throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA,
            "raw recovery fixture for namespace '{}' has no catalog entry", ns.string());

    const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(it->ns, it->incarnation);
    const PutResult put = backend.putIfAbsent(layout.refCkptKey(life), encodeRefCkpt(ckpt));
    if (put.outcome != PutOutcome::Done)
        throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA,
            "raw recovery fixture for namespace '{}' could not publish its checkpoint", ns.string());
}

/// Advance an existing recoverable raw fixture's exact checkpoint frontier. This intentionally never
/// creates a missing `_ckpt` or repairs an invalid one: those are distinct raw corruption fixtures.
inline void advanceRecoverableCkptForRawFixture(
    DB::Cas::Backend & backend, const DB::Cas::Layout & layout, const DB::Cas::RootNamespace & ns,
    const DB::Cas::RefTxnId & through)
{
    const CasRefCatalog::Snapshot catalog_cut = CasRefCatalog::read(backend, layout);
    const auto it = std::find_if(
        catalog_cut.catalog.entries.begin(), catalog_cut.catalog.entries.end(),
        [&] (const CatalogEntry & entry) { return entry.ns == ns; });
    if (it == catalog_cut.catalog.entries.end())
        throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA,
            "raw recovery fixture for namespace '{}' has no catalog entry", ns.string());

    const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(it->ns, it->incarnation);
    const std::optional<CkptSample> sample = readCkpt(backend, layout, life);
    if (!sample)
        throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA,
            "raw recovery fixture for namespace '{}' has no checkpoint to advance", ns.string());

    chooseRecoveryGrounding(*it, sample->ckpt);
    if (!sample->ckpt.committed_through || through <= *sample->ckpt.committed_through)
        throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA,
            "raw recovery fixture for namespace '{}' cannot advance its checkpoint monotonically", ns.string());

    RefCkpt advanced = sample->ckpt;
    advanced.committed_through = through;
    if (backend.casPut(layout.refCkptKey(life), encodeRefCkpt(advanced), sample->token).outcome != CasOutcome::Committed)
        throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA,
            "raw recovery fixture for namespace '{}' could not advance its checkpoint", ns.string());
}

/// Replace an existing recoverable raw fixture checkpoint with the caller's complete next state.
/// Unlike `advanceRecoverableCkptForRawFixture`, this does not preserve any field implicitly: callers
/// that model a snapshot or epoch-seal change must name the entire authoritative checkpoint. Missing or
/// invalid current checkpoints stay corruption fixtures and are never repaired here.
inline void replaceRecoverableCkptForRawFixture(
    DB::Cas::Backend & backend, const DB::Cas::Layout & layout, const DB::Cas::RootNamespace & ns,
    const DB::Cas::RefCkpt & next)
{
    const CasRefCatalog::Snapshot catalog_cut = CasRefCatalog::read(backend, layout);
    const auto it = std::find_if(
        catalog_cut.catalog.entries.begin(), catalog_cut.catalog.entries.end(),
        [&] (const CatalogEntry & entry) { return entry.ns == ns; });
    if (it == catalog_cut.catalog.entries.end())
        throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA,
            "raw recovery fixture for namespace '{}' has no catalog entry", ns.string());

    const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(it->ns, it->incarnation);
    const std::optional<CkptSample> existing = readCkpt(backend, layout, life);
    if (!existing)
        throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA,
            "raw recovery fixture for namespace '{}' has no checkpoint to replace", ns.string());

    chooseRecoveryGrounding(*it, existing->ckpt);
    chooseRecoveryGrounding(*it, next);
    if (next.life_epoch != existing->ckpt.life_epoch)
        throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA,
            "raw recovery fixture for namespace '{}' cannot replace its checkpoint with a different life epoch", ns.string());
    if (existing->ckpt.committed_through
        && (!next.committed_through || *next.committed_through < *existing->ckpt.committed_through))
        throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA,
            "raw recovery fixture for namespace '{}' cannot regress its checkpoint frontier", ns.string());

    if (backend.casPut(layout.refCkptKey(life), encodeRefCkpt(next), existing->token).outcome != CasOutcome::Committed)
        throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA,
            "raw recovery fixture for namespace '{}' could not replace its checkpoint", ns.string());
}

/// Publish the checkpoint authority a semantic fixture wrapper owes immediately after its durable raw
/// log transaction. Raw writers deliberately do not call this: missing, stale, and malformed `_ckpt`
/// fixtures are meaningful corruption inputs. A semantic wrapper creates the first valid authority or
/// advances the existing exact checkpoint without discarding its snapshot or epoch-seal fields.
inline void publishRecoverableCkptForSemanticWrapper(
    DB::Cas::Backend & backend, const DB::Cas::Layout & layout, const DB::Cas::RootNamespace & ns,
    const DB::Cas::RefTxnId & txn_id)
{
    const std::optional<NamespaceLifeId> life = CasRefCatalog::lifeIfCataloged(backend, layout, ns);
    if (!life)
        throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA,
            "semantic ref fixture for namespace '{}' was not admitted", ns.string());

    if (!readCkpt(backend, layout, *life))
    {
        const PutResult put = backend.putIfAbsent(layout.refCkptKey(*life), encodeRefCkpt(RefCkpt{
            .life_epoch = 1,
            .committed_through = txn_id,
            .checkpoint_snapshot_id = std::nullopt,
            .last_epoch_seal = std::nullopt,
        }));
        if (put.outcome == PutOutcome::Done)
            return;
    }

    advanceRecoverableCkptForRawFixture(backend, layout, ns, txn_id);
}

/// Admit an otherwise empty `Live` fixture together with the immutable checkpoint authority that a
/// production-created life already has. This is deliberately a SEPARATE helper from `casAdmitEntry`:
/// raw fixtures that exercise a missing or corrupt `_ckpt` must keep constructing that invalid shape
/// explicitly. The empty frontier is valid because no raw log has been published yet; a fixture that
/// seeds logs instead has to name its own exact `committed_through` through
/// `writeRecoverableCkptForRawFixture`.
inline void casAdmitRecoverableEntry(
    DB::Cas::Backend & backend, const DB::Cas::Layout & layout, const DB::Cas::RootNamespace & ns,
    uint64_t life_epoch = 1)
{
    casAdmitEntry(backend, layout, ns);

    const std::optional<NamespaceLifeId> life = CasRefCatalog::lifeIfCataloged(backend, layout, ns);
    if (!life)
        throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA,
            "recoverable raw fixture for namespace '{}' was not admitted", ns.string());

    if (backend.head(layout.refCkptKey(*life)).exists)
        return;

    writeRecoverableCkptForRawFixture(backend, layout, ns, RefCkpt{
        .life_epoch = life_epoch,
        .committed_through = std::nullopt,
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });
}

/// Recover from the caller's catalog cut, reading `_ckpt` exactly once for the row in that same cut.
/// Keeping the cut an argument forces raw-fixture consumers to make the immutable authority visible;
/// this helper never resolves the namespace or re-reads the catalog on their behalf.
inline DB::Cas::RecoveredRefTable recoverRefTableDetailedAtCatalogCutForTest(
    DB::Cas::Backend & backend, const DB::Cas::Layout & layout, const CasRefCatalog::Snapshot & catalog_cut,
    const DB::Cas::RootNamespace & ns)
{
    std::optional<CatalogEntry> catalog_entry;
    const auto it = std::find_if(
        catalog_cut.catalog.entries.begin(), catalog_cut.catalog.entries.end(),
        [&] (const CatalogEntry & entry) { return entry.ns == ns; });
    if (it != catalog_cut.catalog.entries.end())
        catalog_entry = *it;

    std::optional<RefCkpt> ckpt;
    if (catalog_entry)
    {
        const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(catalog_entry->ns, catalog_entry->incarnation);
        if (const std::optional<CkptSample> sample = readCkpt(backend, layout, life))
            ckpt = sample->ckpt;
    }

    return recoverRefTableDetailedFromAuthority(backend, layout, catalog_entry, ckpt);
}

/// Writes `txn` at `_log/<txn_id>` (create-if-absent). Admits `txn.ns` into the catalog first
/// (`casAdmitEntry`, above) -- the fold's universe is catalog-authoritative (Task 4-C), so a raw
/// fixture that skipped this would be invisible to GC/rebuild/fsck no matter what it wrote to `_log`.
///
/// KEYS AT THE NAMESPACE'S CURRENT CATALOG LIFE, NOT UNCONDITIONALLY AT THE SENTINEL: a test that
/// mixes a REAL birth (`beginPartWrite`/`precommitAdd`, which mints a real random incarnation via
/// `CasRefLedger::resolveNamespaceLife`) with a raw follow-up write to the SAME namespace (a
/// repoint/removal simulation, say) needs this write to land where the real content already lives, not
/// at an unrelated sentinel prefix the fold never reads for that namespace. `casAdmitEntry` above is a
/// no-op once any entry exists, so resolving the catalog life here yields whichever life is ALREADY on
/// record -- the real one if a real birth landed first, the fixture identity if this call is what
/// admitted it (via `casAdmitEntry`, moments ago, in this same function).
inline void writeRefLogTxnRaw(
    DB::Cas::Backend & backend, const DB::Cas::Layout & layout, const DB::Cas::RefLogTxn & txn)
{
    const DB::Cas::RootNamespace ns{txn.ns};
    casAdmitEntry(backend, layout, ns);
    const NamespaceLifeId life = CasRefCatalog::lifeIfCataloged(backend, layout, ns).value_or(fixture::fixtureLife(ns));
    const String key = layout.refLogKey(life, txn.txn_id);
    backend.putIfAbsent(key, DB::Cas::sealObject(DB::Cas::FormatId::RefLog, DB::Cas::encodeRefLogTxn(txn)));
}

namespace fixture
{
    /// The raw ref-log write pattern (divergence 3 above), reachable through the seam.
    inline void writeRefLogRaw(DB::Cas::Backend & backend, const DB::Cas::Layout & layout, const DB::Cas::RefLogTxn & txn)
    {
        writeRefLogTxnRaw(backend, layout, txn);
    }
}

/// A `Live` snapshot naming exactly `committed` (already-sorted-by-ref_name input expected) with no
/// precommits — the common recovery-fixture shape.
inline DB::Cas::RefTableSnapshot minimalLiveSnapshot(
    const String & ns, DB::Cas::RefTxnId snapshot_id, std::vector<DB::Cas::RefCommittedRow> committed = {})
{
    DB::Cas::RefTableSnapshot s;
    s.ns = ns;
    s.snapshot_id = snapshot_id;
    s.committed = std::move(committed);
    return s;
}

/// One committed row naming `ref_name` -> `manifest_ref` with `published_at_ms` left at its default
/// (0, unset) — for tests that don't care about the publish stamp.
inline DB::Cas::RefCommittedRow committedRow(const String & ref_name, const DB::Cas::ManifestRef & manifest_ref)
{
    DB::Cas::RefCommittedRow row;
    row.ref_name = ref_name;
    row.manifest_ref = manifest_ref;
    return row;
}

/// A `namespace_birth` op — the first op any never-born table's first transaction needs.
inline DB::Cas::RefOp namespaceBirthOp()
{
    DB::Cas::RefOp op;
    op.kind = DB::Cas::RefOpKind::NamespaceBirth;
    return op;
}

/// An `epoch_seal` op — the record that CLOSES an epoch (INV-2). A seal transaction carries exactly
/// this op and nothing else (grammar enforced by the codec in both directions). The next epoch's first
/// transaction names the seal it consumed in `prev_epoch_seal`, and that back-chain is what lets a fold
/// cross epochs without trusting a listing.
inline DB::Cas::RefOp epochSealOp()
{
    DB::Cas::RefOp op;
    op.kind = DB::Cas::RefOpKind::EpochSeal;
    return op;
}

/// Write ONE ref-log transaction at an EXACT id. `appendRefLogSeed` and the wrappers above ALLOCATE
/// ids arithmetically inside writer epoch 1, so anything that needs a chosen id — a gap, an
/// out-of-order arrival, or an epoch CROSSING — writes through here instead. The bytes go through the
/// real codec, so every grammar rule the fold's decoder enforces is enforced here too.
inline void writeTxnAt(
    DB::Cas::Backend & backend, const DB::Cas::Layout & layout, const DB::Cas::RootNamespace & ns,
    const DB::Cas::RefTxnId & id, std::vector<DB::Cas::RefOp> ops,
    std::optional<DB::Cas::RefTxnId> prev_epoch_seal = std::nullopt)
{
    DB::Cas::RefLogTxn txn;
    txn.ns = ns.string();
    txn.txn_id = id;
    txn.ops = std::move(ops);
    txn.prev_epoch_seal = prev_epoch_seal;
    writeRefLogTxnRaw(backend, layout, txn);
}

/// Close an epoch at exactly `id`.
inline void writeSealAt(
    DB::Cas::Backend & backend, const DB::Cas::Layout & layout, const DB::Cas::RootNamespace & ns,
    const DB::Cas::RefTxnId & id, std::optional<DB::Cas::RefTxnId> prev_epoch_seal = std::nullopt)
{
    writeTxnAt(backend, layout, ns, id, {epochSealOp()}, prev_epoch_seal);
}

/// Publish `ref_name` -> a fresh manifest pinning `blob`, as ONE transaction at exactly `id`
/// (add-precommit + promote, the only shape that reaches a committed owner). `birth` prepends the
/// `namespace_birth` op the table's first transaction owes; `prev_epoch_seal` is required on sequence 1
/// of every epoch above the namespace's genesis. The manifest's prefix is
/// `{id.writer_epoch, build_sequence}`, so a caller controlling `build_sequence` also controls whether
/// the orphan sweep's watermark considers that manifest eligible.
inline void publishAt(
    DB::Cas::Backend & backend, const DB::Cas::Layout & layout, const DB::Cas::RootNamespace & ns,
    const DB::Cas::RefTxnId & id, const String & ref_name, uint64_t build_sequence, const DB::UInt128 & blob,
    bool birth = false, std::optional<DB::Cas::RefTxnId> prev_epoch_seal = std::nullopt)
{
    const DB::Cas::ManifestRef mref{.writer_epoch = id.writer_epoch, .build_sequence = build_sequence,
                                    .manifest_ordinal = 1};
    writeBlobBody(backend, layout, blob);
    writeManifestRaw(backend, layout, ns, mref, {blobEntryFor("data.bin", blob)});

    std::vector<DB::Cas::RefOp> ops;
    if (birth)
        ops.push_back(namespaceBirthOp());
    for (const DB::Cas::RefOp & op : publishCommittedOps(ref_name, mref))
        ops.push_back(op);
    writeTxnAt(backend, layout, ns, id, std::move(ops), prev_epoch_seal);
}

/// The two ops a fixture transaction needs to go straight from nothing to a committed ref (spec
/// §State Transitions has no direct "add committed" shape — only precommit -> promote): an
/// `owner_transition` add-precommit followed by an `owner_transition` promote of the SAME
/// (ref_name, manifest_ref). Legal as the tail of one transaction whose earlier ops (if any) left the
/// table `Live` (prepend `namespaceBirthOp()` for a never-born table).
inline std::vector<DB::Cas::RefOp> publishCommittedOps(const String & ref_name, const DB::Cas::ManifestRef & manifest_ref)
{
    DB::Cas::RefOp add;
    add.kind = DB::Cas::RefOpKind::OwnerTransition;
    add.new_binding = DB::Cas::RefOwnerBinding{DB::Cas::RefOwnerKind::Precommit, ref_name, manifest_ref};

    DB::Cas::RefOp promote;
    promote.kind = DB::Cas::RefOpKind::OwnerTransition;
    promote.old_binding = DB::Cas::RefOwnerBinding{DB::Cas::RefOwnerKind::Precommit, ref_name, manifest_ref};
    promote.new_binding = DB::Cas::RefOwnerBinding{DB::Cas::RefOwnerKind::Committed, ref_name, manifest_ref};

    return {add, promote};
}

/// Counts head/get/putIfAbsent per key for op-count assertions (Pillar B / A1 tests).
class CountingBackend : public DB::Cas::InMemoryBackend
{
public:
    /// Unhide the base convenience overloads (omitted Range/ObjectMeta/expected-token forms): the
    /// overrides below would otherwise shadow them for callers holding a concrete backend type.
    using DB::Cas::Backend::get;
    using DB::Cas::Backend::getStream;
    using DB::Cas::Backend::putIfAbsent;
    using DB::Cas::Backend::putOverwrite;
    using DB::Cas::Backend::casPut;

    DB::Cas::HeadResult head(const String & key) override
    {
        {
            std::lock_guard lock(count_mutex);
            ++head_counts[key];
            ++head_total;
        }
        return InMemoryBackend::head(key);
    }

    std::optional<DB::Cas::GetResult> get(const String & key, DB::Cas::Range range) override
    {
        {
            std::lock_guard lock(count_mutex);
            ++get_counts[key];
            ++get_total;
            /// Record the request-size shape per key so streaming-memory gates (Task 3/4) can assert
            /// the resident-memory bound at the seam: a whole-object read (range.whole()) is a
            /// violation for a run object; a ranged read tracks its MAX window length per key.
            if (range.whole())
                ++whole_get_counts[key];
            else
            {
                const uint64_t len = range.length.has_value() ? *range.length : 0;
                uint64_t & mx = max_ranged_get_len[key];
                mx = std::max(mx, len);
            }
        }
        return InMemoryBackend::get(key, range);
    }

    std::optional<DB::Cas::GetStreamResult> getStream(const String & key, DB::Cas::Range range) override
    {
        {
            std::lock_guard lock(count_mutex);
            ++get_stream_counts[key];
            ++get_stream_total;
        }
        return InMemoryBackend::getStream(key, range);
    }

    DB::Cas::ListPage list(const String & prefix, const String & cursor, size_t limit) override
    {
        {
            std::lock_guard lock(count_mutex);
            ++list_counts[prefix];
            ++list_total;
        }
        return InMemoryBackend::list(prefix, cursor, limit);
    }

    DB::Cas::PutResult putIfAbsent(const String & key, const String & bytes, const DB::Cas::ObjectMeta & meta) override
    {
        {
            std::lock_guard lock(count_mutex);
            ++put_counts[key];
            ++put_total;
        }
        return InMemoryBackend::putIfAbsent(key, bytes, meta);
    }


    /// Counted separately from `putIfAbsent` and `casPut`, for the same reason those two are separate: a
    /// replacement conditioned on an expected token is its own op with its own cost. The namespace-file
    /// request-profile goldens tell the create path from the replace path on exactly this counter.
    DB::Cas::PutResult putOverwrite(const String & key, const String & bytes, const DB::Cas::Token & expected,
                                    const DB::Cas::ObjectMeta & meta) override
    {
        {
            std::lock_guard lock(count_mutex);
            ++put_overwrite_counts[key];
            ++put_overwrite_total;
        }
        return InMemoryBackend::putOverwrite(key, bytes, expected, meta);
    }

    /// Counted separately from `putIfAbsent`: a token-CAS is a DIFFERENT op with a different cost, and
    /// the `_ckpt` no-op contract ("identical merged body issues no write") is asserted on exactly this
    /// counter -- a create-if-absent count would not see the replace path at all.
    DB::Cas::CasResult casPut(const String & key, const String & bytes,
                              const std::optional<DB::Cas::Token> & expected, const DB::Cas::ObjectMeta & meta) override
    {
        {
            std::lock_guard lock(count_mutex);
            ++cas_put_counts[key];
            ++cas_put_total;
        }
        return InMemoryBackend::casPut(key, bytes, expected, meta);
    }
    /// Every ATTEMPTED delete is counted, whatever the backend answers. The destructive gate's tests
    /// assert that a suppressed round issues NONE, and an attempt that came back `NotFound` is still an
    /// attempt -- counting only successful ones would let a gate that leaks deletes over already-absent
    /// keys read as green.
    DB::Cas::DeleteOutcome deleteExact(const String & key, const DB::Cas::Token & token) override
    {
        {
            std::lock_guard lock(count_mutex);
            ++delete_counts[key];
            ++delete_total;
        }
        return InMemoryBackend::deleteExact(key, token);

    }

    uint64_t headCount(const String & key) const { return lookup(head_counts, key); }
    uint64_t casPutCount(const String & key) const { return lookup(cas_put_counts, key); }
    uint64_t putOverwriteCount(const String & key) const { return lookup(put_overwrite_counts, key); }
    uint64_t getCount(const String & key) const { return lookup(get_counts, key); }
    uint64_t putCount(const String & key) const { return lookup(put_counts, key); }
    uint64_t deleteCount(const String & key) const { return lookup(delete_counts, key); }
    uint64_t deleteTotal() const { std::lock_guard lock(count_mutex); return delete_total; }
    /// Attempted deletes against any key whose path CONTAINS `substr` — the per-site assertion the
    /// destructive-gate tests make ("the generation prune deleted nothing", "the sweep deleted nothing").
    uint64_t deleteCountForKeysContaining(const String & substr) const
    {
        std::lock_guard lock(count_mutex);
        uint64_t total = 0;
        for (const auto & [key, n] : delete_counts)
            if (key.find(substr) != String::npos)
                total += n;
        return total;
    }
    /// Every key this backend was ever asked to delete, in sorted order — so a failing zero-delete
    /// assertion names the sites that leaked instead of just reporting a count.
    std::vector<String> deletedKeys() const
    {
        std::lock_guard lock(count_mutex);
        std::vector<String> keys;
        keys.reserve(delete_counts.size());
        for (const auto & [key, n] : delete_counts)
            keys.push_back(key);
        return keys;
    }
    uint64_t getStreamCount(const String & key) const { return lookup(get_stream_counts, key); }
    uint64_t listCount(const String & prefix) const { return lookup(list_counts, prefix); }
    /// The max ranged-get window length observed for `key` (0 if only whole-object gets, or none).
    uint64_t maxRangedGetLen(const String & key) const { return lookup(max_ranged_get_len, key); }
    /// How many whole-object gets (range.whole()) hit `key` — nonzero flags a resident-memory
    /// violation for a run/seal object that a streaming caller must never read whole.
    uint64_t wholeGetCount(const String & key) const { return lookup(whole_get_counts, key); }
    /// Every key any counted operation was issued against, plus every LIST prefix, sorted and
    /// de-duplicated. A request-profile gate asserts the SET, not only the totals, so a new request the
    /// profile does not allow names its own key in the failure instead of moving an anonymous counter.
    std::vector<String> touchedKeys() const
    {
        std::lock_guard lock(count_mutex);
        std::vector<String> keys;
        for (const std::map<String, uint64_t> * m :
             {&head_counts, &get_counts, &put_counts, &put_overwrite_counts, &cas_put_counts,
              &get_stream_counts, &list_counts, &delete_counts})
            for (const auto & [key, n] : *m)
                keys.push_back(key);
        std::sort(keys.begin(), keys.end());
        keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
        return keys;
    }

    uint64_t headTotal() const { std::lock_guard lock(count_mutex); return head_total; }
    uint64_t getTotal() const { std::lock_guard lock(count_mutex); return get_total; }
    uint64_t putTotal() const { std::lock_guard lock(count_mutex); return put_total; }
    uint64_t putOverwriteTotal() const { std::lock_guard lock(count_mutex); return put_overwrite_total; }
    uint64_t casPutTotal() const { std::lock_guard lock(count_mutex); return cas_put_total; }
    uint64_t getStreamTotal() const { std::lock_guard lock(count_mutex); return get_stream_total; }
    uint64_t listTotal() const { std::lock_guard lock(count_mutex); return list_total; }

    /// The total number of get + getStream + putIfAbsent operations against any key whose path
    /// CONTAINS `substr` (T0 idle-round gate: zero run I/O touches every `.../blob_target/...` key).
    uint64_t ioCountForKeysContaining(const String & substr) const
    {
        std::lock_guard lock(count_mutex);
        uint64_t total = 0;
        for (const auto & [key, n] : get_counts)
            if (key.find(substr) != String::npos) total += n;
        for (const auto & [key, n] : get_stream_counts)
            if (key.find(substr) != String::npos) total += n;
        for (const auto & [key, n] : put_counts)
            if (key.find(substr) != String::npos) total += n;
        return total;
    }

    void resetCounts()
    {
        std::lock_guard lock(count_mutex);
        head_counts.clear();
        get_counts.clear();
        put_counts.clear();
        put_overwrite_counts.clear();
        cas_put_counts.clear();
        get_stream_counts.clear();
        list_counts.clear();
        delete_counts.clear();
        max_ranged_get_len.clear();
        whole_get_counts.clear();
        head_total = get_total = put_total = cas_put_total = get_stream_total = list_total = delete_total = 0;
        put_overwrite_total = 0;

    }

private:
    uint64_t lookup(const std::map<String, uint64_t> & m, const String & key) const
    {
        std::lock_guard lock(count_mutex);
        const auto it = m.find(key);
        return it == m.end() ? 0 : it->second;
    }

    mutable std::mutex count_mutex;
    std::map<String, uint64_t> head_counts;
    std::map<String, uint64_t> get_counts;
    std::map<String, uint64_t> put_counts;
    std::map<String, uint64_t> put_overwrite_counts;
    std::map<String, uint64_t> cas_put_counts;
    std::map<String, uint64_t> get_stream_counts;
    std::map<String, uint64_t> list_counts;
    std::map<String, uint64_t> delete_counts;
    std::map<String, uint64_t> max_ranged_get_len;
    std::map<String, uint64_t> whole_get_counts;
    uint64_t head_total = 0;
    uint64_t get_total = 0;
    uint64_t put_total = 0;
    uint64_t put_overwrite_total = 0;
    uint64_t cas_put_total = 0;
    uint64_t get_stream_total = 0;
    uint64_t list_total = 0;
    uint64_t delete_total = 0;
};

/// Records the ORDER of body-PUT / `_ckpt`-CAS operations (so a test can compare indices) and lets a
/// test inject a persistent `Conflict` on one chosen `_ckpt` key -- the same technique
/// `gtest_cas_ref_writer.cpp`'s `RefWriterTestBackend::ckpt_conflict_key`/`ckpt_conflict_count` uses to
/// drive the ledger into `NeedsRecovery`, reproduced here so this suite has no dependency on that file's
/// internal (non-exported) test type. Delegates every operation to `CountingBackend` unchanged, so the
/// per-key counters (`putCount`/`casPutCount`) remain available as the positive control.
class OrderedFaultBackend : public CountingBackend
{
public:
    using CountingBackend::casPut;
    using CountingBackend::get;
    using CountingBackend::putIfAbsent;

    enum class Op : uint8_t { Put, Cas };
    struct Entry
    {
        Op op;
        String key;
    };

    PutResult putIfAbsent(const String & key, const String & bytes, const ObjectMeta & meta) override
    {
        record(Op::Put, key);
        if (fail_put_count > 0 && !fail_put_substr.empty() && key.find(fail_put_substr) != String::npos)
        {
            --fail_put_count;
            throw Poco::TimeoutException("OrderedFaultBackend: simulated PUT response lost, nothing landed");
        }
        return CountingBackend::putIfAbsent(key, bytes, meta);
    }

    CasResult casPut(const String & key, const String & bytes, const std::optional<Token> & expected,
                      const ObjectMeta & meta) override
    {
        record(Op::Cas, key);
        if (key == fail_cas_key && fail_cas_count > 0)
        {
            --fail_cas_count;
            /// A `Conflict` (not a thrown/ambiguous response): the caller's own re-read-and-merge loop
            /// (`publishCkpt`) treats this exactly like a concurrent writer that landed first, and
            /// exhausts `MAX_CKPT_CAS_ATTEMPTS` (100) without ever committing -- deterministically, with
            /// no wall-clock wait, since the loop is attempt-bounded rather than only deadline-bounded.
            return {CasOutcome::Conflict, {}};
        }
        return CountingBackend::casPut(key, bytes, expected, meta);
    }

    /// Arms a persistent CAS conflict at `key` for the next `count` attempts.
    void armCasConflict(const String & key, size_t count)
    {
        fail_cas_key = key;
        fail_cas_count = count;
    }

    /// Arms a persistent, never-committed PUT failure for the next `count` `putIfAbsent` calls whose key
    /// contains `substr`: the object is never actually written (unlike a real ambiguous response, which
    /// may or may not have landed), so the resolve-by-exact-GET a controlled `CasRequestBudget` with
    /// `max_attempts = 1` performs always finds the key absent and classifies the attempt a definite,
    /// non-`Committed` failure -- deterministically, with no internal retry and no wall-clock wait.
    void armPutFailure(const String & substr, int count)
    {
        fail_put_substr = substr;
        fail_put_count = count;
    }

    /// The current length of the journal -- a caller's baseline for `indicesFrom` below, so a query can
    /// be scoped to "since I last looked" rather than "since the pool opened" (whose earlier entries
    /// belong to unrelated setup writes, e.g. the birth transaction's own checkpoint CAS).
    size_t journalSize() const
    {
        std::lock_guard lock(mutex);
        return journal.size();
    }

    /// Every index at or after `from` where `op`/`key` matches, in order.
    std::vector<size_t> indicesFrom(Op op, const String & key, size_t from) const
    {
        std::lock_guard lock(mutex);
        std::vector<size_t> result;
        for (size_t i = from; i < journal.size(); ++i)
            if (journal[i].op == op && journal[i].key == key)
                result.push_back(i);
        return result;
    }

    /// The first index at or after `from` where `op`/`key` matches, if any.
    std::optional<size_t> firstIndexFrom(Op op, const String & key, size_t from) const
    {
        const auto indices = indicesFrom(op, key, from);
        return indices.empty() ? std::nullopt : std::make_optional(indices.front());
    }

private:
    void record(Op op, const String & key)
    {
        std::lock_guard lock(mutex);
        journal.push_back({op, key});
    }

    mutable std::mutex mutex;
    std::vector<Entry> journal;
    String fail_cas_key;
    size_t fail_cas_count = 0;
    String fail_put_substr;
    int fail_put_count = 0;
};

/// A backend whose LIST permanently omits every key under a chosen prefix while those keys stay fully
/// readable by exact key -- the lying-store shape observed in production (`0x1430c`/`0x1430d`), and the
/// premise of every arithmetic-walk test: a record a listing never mentions is still THERE, so a walk
/// that computes the id finds it and a walk that enumerates does not.
///
/// PERMANENT (not nth-call) omission is deliberate: a lying store need not ever recover the key, and the
/// arithmetic walk that finds it anyway is the property under test -- these fixtures are about the walk,
/// not about any one `list` call.
///
/// Erasing keys from a page cannot disturb pagination: `ListPage::next_cursor` is computed by the base
/// backend before the erase, so the next page still resumes strictly after the last key it returned.
///
/// Templated on the base so a suite that also needs request COUNTS composes it over `CountingBackend`
/// without a second copy of the hiding rule (which is a rule about what the store may legally do, and
/// must therefore read the same everywhere it is modelled).
template <typename Base>
class HintHoleBackendOn : public Base
{
public:
    /// Hide every key under `prefix` from LIST -- a whole namespace, including objects a later publish
    /// adds.
    void hidePrefix(const String & prefix)
    {
        std::lock_guard lock(hide_mutex);
        hidden_prefixes.push_back(prefix);
    }

    /// Hide exactly one key. Call AFTER seeding: a fixture that allocates ids by listing would
    /// otherwise allocate over a hidden record.
    void hide(const String & key)
    {
        std::lock_guard lock(hide_mutex);
        hidden_keys.insert(key);
    }

    /// Make the store's enumeration omit EXACTLY `keys` and nothing else -- the whole omission set in
    /// one call, replacing whatever was hidden before.
    ///
    /// This is the RustFS defect reproduced as an interface: every one of these keys stays durable and
    /// honestly served by `get` / `head` / `putIfAbsent` / `casPut` / `deleteExact`, and only
    /// enumeration pretends they are not there. Stating the omission as a SET is what lets a test say
    /// the thing the defect report says -- "ids 3 and 4 are invisible while the LATER id 5 is visible"
    /// -- in one line, instead of assembling it from repeated single-key calls whose combined effect a
    /// reader has to reconstruct.
    ///
    /// A setter rather than an adder: the omission set is the store's declared behaviour for the rest
    /// of the test, so a second call REPLACES it (pass `{}` to stop lying, same as `revealAll`).
    void setListOmissions(std::vector<String> keys)
    {
        std::lock_guard lock(hide_mutex);
        hidden_keys.clear();
        hidden_prefixes.clear();
        hidden_keys.insert(keys.begin(), keys.end());
    }

    /// How many LIST pages actually had a key erased. Every test that hides a key asserts this, so a
    /// mistyped key cannot let the test pass vacuously -- the hole has to have been SERVED.
    size_t holesServed() const
    {
        std::lock_guard lock(hide_mutex);
        return served;
    }

    /// The store stops lying: everything hidden is listed again.
    void revealAll()
    {
        std::lock_guard lock(hide_mutex);
        hidden_keys.clear();
        hidden_prefixes.clear();
    }

    DB::Cas::ListPage list(const String & prefix, const String & cursor, size_t limit) override
    {
        DB::Cas::ListPage page = Base::list(prefix, cursor, limit);
        std::lock_guard lock(hide_mutex);
        if (hidden_keys.empty() && hidden_prefixes.empty())
            return page;
        const size_t before = page.keys.size();
        std::erase_if(page.keys, [&](const DB::Cas::ListedKey & k)
        {
            if (hidden_keys.contains(k.key))
                return true;
            for (const String & hidden : hidden_prefixes)
                if (k.key.starts_with(hidden))
                    return true;
            return false;
        });
        if (page.keys.size() != before)
            ++served;
        return page;
    }

private:
    mutable std::mutex hide_mutex;
    std::set<String> hidden_keys;
    std::vector<String> hidden_prefixes;
    size_t served = 0;
};

/// The plain form, over a bare `InMemoryBackend`.
using HintHoleBackend = HintHoleBackendOn<DB::Cas::InMemoryBackend>;

/// Stand in for the self-remount that `Pool::reportImpossibleInterference` schedules. That reaction
/// trips the local write fence closed AND schedules a remount; a unit-test Pool runs no background
/// remount (`background_watermark` is off by design there), so without this the fence stays closed and
/// every later mutation is refused at the gate -- which is a test-harness artifact, not the production
/// behaviour. Re-arming directly is the smallest faithful stand-in: it restores writability without the
/// claim machinery and without discarding the cached ref runtimes, so a test can observe what happens
/// AFTER the reaction. It bumps the fence GENERATION, exactly as a real re-arm does.
inline void rearmMountFenceAfterAnomalyForTest(const DB::Cas::PoolPtr & store)
{
    store->armMountFence(DB::UInt128{0, 1}, store->liveWriterEpoch(), store->bootMsNow() + 600000);
}

/// Delegates the FIRST matching `putIfAbsent` to `CountingBackend` -- so the write actually LANDS --
/// and only THEN throws an ambiguous exception, modelling "our own PUT committed but its response was
/// lost". Every later call behaves normally, so a caller that retries the SAME (key, bytes) meets its
/// OWN earlier write as the occupant: the exact input the every-attempt rule's adoption arm adjudicates
/// (`slotOccupy` reports `Occupied` with bytes equal to the attempt's own).
///
/// `key_substr` empty means "the first putIfAbsent of any key"; set it to scope the fault to one key
/// family when the caller drives a whole Pool (whose bootstrap PUTs would otherwise consume the fault).
///
/// Shared rather than TU-local because two suites need exactly this shape: `gtest_cas_slot_occupy.cpp`
/// pins the primitive's same-call resolve, and `gtest_cas_ref_wedge_every_attempt.cpp` drives the
/// writer's wedge adoption through it.
class LandedButAckLostOnceBackend : public CountingBackend
{
public:
    using CountingBackend::putIfAbsent;
    using CountingBackend::get;
    String key_substr;
    bool fired = false;
    /// Also lose the caller's IMMEDIATE resolve read of the same key, once. Needed only by a caller
    /// whose conditional-write layer resolves before reissuing (`putIfAbsentControlled`): without it
    /// that resolve proves the object durable inside the very same attempt and reports `Committed`, so
    /// no wedge over a DURABLE object can ever form. `slotOccupy` needs no such thing -- it has no
    /// retry loop -- which is why this defaults off and this file's original caller is unaffected.
    bool lose_resolve_read = false;

    DB::Cas::PutResult putIfAbsent(const String & key, const String & bytes, const DB::Cas::ObjectMeta & meta) override
    {
        if (!fired && (key_substr.empty() || key.find(key_substr) != String::npos))
        {
            fired = true;
            CountingBackend::putIfAbsent(key, bytes, meta);   /// the write LANDS
            if (lose_resolve_read)
                fail_get_once_key = key;
            throw Poco::TimeoutException("LandedButAckLostOnceBackend: simulated lost PUT response");
        }
        return CountingBackend::putIfAbsent(key, bytes, meta);
    }

    std::optional<DB::Cas::GetResult> get(const String & key, DB::Cas::Range range) override
    {
        if (!fail_get_once_key.empty() && key == fail_get_once_key)
        {
            fail_get_once_key.clear();
            throw Poco::TimeoutException(
                "LandedButAckLostOnceBackend: simulated lost GET (read response never arrived)");
        }
        return CountingBackend::get(key, range);
    }

private:
    String fail_get_once_key;
};

/// A `CountingBackend` that can fault selected PUTs by key substring (skip the first `fault_skip`
/// matches, then fault the next `fault_count`), and can latch a matching PUT mid-flight. Same class of
/// seam as the wedge tests in `gtest_cas_ref_writer.cpp` use
/// (`fault_key_substr`/`corrupt_key_substr`/`armPutBlock`), narrowed to what the ref-lane tests need.
/// Shared (rather than TU-local) because the chunk-boundary tests and the post-durable install-safety
/// tests need exactly the same seam.
class ChunkFaultBackend : public CountingBackend
{
public:
    using CountingBackend::putIfAbsent;
    using CountingBackend::get;

    /// Unresolved       -> a lost-response ambiguity, NOTHING landed; with a single-attempt budget this
    ///                     wedges the lane and a later resolve proves the key ABSENT.
    /// LandedThenLost   -> our OWN exact bytes land and only the acknowledgement is lost, AND the
    ///                     controller's immediate resolve-before-reissue GET is lost too. Both legs are
    ///                     required to wedge over a DURABLE object: the resolve happens inside the same
    ///                     attempt, so a readable key would prove `Committed` there and no wedge would
    ///                     ever form. Real-world shape: the write succeeded server-side, the connection
    ///                     dropped, and the verification read hit the same transient outage. With a
    ///                     single-attempt budget the lane then wedges over an object that IS durable, so
    ///                     the NEXT flush's `resolveByExactGet` reports `Committed` and drives the
    ///                     wedge-RESOLUTION install (spec §A1 site 2) -- the only mode that reaches it.
    /// Definite         -> an S3-classified malformed request -> `CasWriteOutcome::DefiniteFailure`.
    /// ForeignConflict  -> a DIFFERENT object lands at the key, then the response is lost -> the
    ///                     controller's resolve-before-reissue GET observes foreign bytes and throws
    ///                     CORRUPTED_DATA straight out of the PUT (a proven conflict).
    enum class Mode { None, Unresolved, LandedThenLost, Definite, ForeignConflict };

    /// Fault matching is single-threaded during a flush (one leader per table PUTs `_log/`), so these
    /// need no lock; set them before driving the flush.
    String fault_substr;
    Mode mode = Mode::None;
    int fault_skip = 0;
    int fault_count = 0;
    /// One-shot: the next `get` of exactly this key throws, then it is cleared. Armed by
    /// `Mode::LandedThenLost` (see above); settable directly for a bare lost-read fault.
    String fail_get_once_key;

    std::optional<DB::Cas::GetResult> get(const String & key, DB::Cas::Range range) override
    {
        if (!fail_get_once_key.empty() && key == fail_get_once_key)
        {
            fail_get_once_key.clear();
            throw Poco::TimeoutException("ChunkFaultBackend: simulated lost GET (read response never arrived)");
        }
        return CountingBackend::get(key, range);
    }

    DB::Cas::PutResult putIfAbsent(const String & key, const String & bytes, const DB::Cas::ObjectMeta & meta) override
    {
        if (mode != Mode::None && !fault_substr.empty() && key.find(fault_substr) != String::npos)
        {
            if (fault_skip > 0)
            {
                --fault_skip;
            }
            else if (fault_count > 0)
            {
                --fault_count;
                switch (mode)
                {
                    case Mode::Unresolved:
                        throw Poco::TimeoutException("ChunkFaultBackend: simulated ambiguous _log PUT (response lost)");
                    case Mode::LandedThenLost:
                        /// The write SUCCEEDS -- byte-for-byte what the caller asked for, through the
                        /// counting path so the object is indistinguishable from a normal PUT -- and only
                        /// the acknowledgement is lost. The controller's resolve-before-reissue GET is
                        /// armed to fail ONCE for this key as well, or it would prove the object durable
                        /// inside this very attempt and the lane would never wedge; the wedge-resolution
                        /// GET a flush later then reads it normally.
                        CountingBackend::putIfAbsent(key, bytes, meta);
                        fail_get_once_key = key;
                        throw Poco::TimeoutException("ChunkFaultBackend: object landed; response lost");
                    case Mode::Definite:
#if USE_AWS_S3
                        throw DB::S3Exception("ChunkFaultBackend: simulated malformed request",
                                              Aws::S3::S3Errors::UNKNOWN, "MalformedXML");
#else
                        throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA,
                            "ChunkFaultBackend: DefiniteFailure requires S3 error classification (USE_AWS_S3 off)");
#endif
                    case Mode::ForeignConflict:
                        /// A foreign writer lands DIFFERENT bytes at this exact key; then our response is
                        /// lost, so resolve-before-reissue GETs foreign bytes -> CORRUPTED_DATA.
                        CountingBackend::putIfAbsent(key, bytes + String("\x01_FOREIGN_DIFFERENT"));
                        throw Poco::TimeoutException("ChunkFaultBackend: foreign different object landed; response lost");
                    case Mode::None:
                        break;
                }
            }
        }
        {
            std::unique_lock lk(block_mutex);
            if (block_armed && !block_substr.empty() && key.find(block_substr) != String::npos)
            {
                block_entered = true;
                block_cv.notify_all();
                /// Bounded (20s) so a wiring bug bounds the wait rather than hanging the whole suite.
                block_cv.wait_for(lk, std::chrono::seconds(20), [&] { return !block_armed; });
            }
        }
        return CountingBackend::putIfAbsent(key, bytes, meta);
    }

    void armBlock(const String & substr)
    {
        std::lock_guard lk(block_mutex);
        block_substr = substr;
        block_armed = true;
        block_entered = false;
    }
    void awaitBlockEntered()
    {
        std::unique_lock lk(block_mutex);
        /// Bounded (20s): if the latched publisher never reaches its PUT, fail LOUDLY rather than hang.
        /// The assertion is load-bearing -- without it a wiring regression that never parks the publisher
        /// would let `SnapshotPublisherLatchedAcrossChunks` pass VACUOUSLY (its final re-fire assertion
        /// can still hold via a direct, non-coalesced dispatch).
        block_cv.wait_for(lk, std::chrono::seconds(20), [&] { return block_entered; });
        ASSERT_TRUE(block_entered) << "latched publisher never entered its blocked PUT within 20s -- "
                                      "coalescing was not exercised";
    }
    void releaseBlock()
    {
        {
            std::lock_guard lk(block_mutex);
            block_armed = false;
        }
        block_cv.notify_all();
    }

private:
    std::mutex block_mutex;
    std::condition_variable block_cv;
    String block_substr;
    bool block_armed = false;
    bool block_entered = false;
};

/// Fault decorator for the condemn-marker gate tests (codex-review triage 2026-07-17 §3.4): while
/// armed, every conditional-write attempt against a blob `.meta` key throws. The request controller
/// exhausts its budget and reports `Unresolved`, so `writeCondemnedMeta` returns false while the round
/// still commits the unconfirmed retired entry. Every other write passes through. Armed by default;
/// disarm (`fail_meta_writes = false`) to model the backend healing.
class MetaWriteFaultBackend : public DB::Cas::InMemoryBackend
{
public:
    /// Unhide the base convenience overloads (omitted Range/ObjectMeta/expected-token forms): the
    /// overrides below would otherwise shadow them for callers holding a concrete backend type.
    using DB::Cas::Backend::get;
    using DB::Cas::Backend::getStream;
    using DB::Cas::Backend::putIfAbsent;
    using DB::Cas::Backend::putOverwrite;
    using DB::Cas::Backend::casPut;

    DB::Cas::PutResult putIfAbsent(
        const String & key, const String & bytes, const DB::Cas::ObjectMeta & meta) override
    {
        if (fail_meta_writes.load() && key.ends_with(".meta"))
            throw std::runtime_error("injected fault: blob meta write lost");
        return InMemoryBackend::putIfAbsent(key, bytes, meta);
    }

    DB::Cas::PutResult putOverwrite(
        const String & key, const String & bytes, const DB::Cas::Token & expected,
        const DB::Cas::ObjectMeta & meta) override
    {
        if (fail_meta_writes.load() && key.ends_with(".meta"))
            throw std::runtime_error("injected fault: blob meta write lost");
        return InMemoryBackend::putOverwrite(key, bytes, expected, meta);
    }

    DB::Cas::CasResult casPut(const String & key, const String & bytes,
                              const std::optional<DB::Cas::Token> & expected,
                              const DB::Cas::ObjectMeta & meta) override
    {
        if (fail_meta_writes.load() && key.ends_with(".meta"))
            throw std::runtime_error("injected fault: blob meta write lost");
        return InMemoryBackend::casPut(key, bytes, expected, meta);
    }

    std::atomic<bool> fail_meta_writes{true};
};

/// Blocks INSIDE a blob-meta mutation until `release` is called, so a test can hold a real meta job in
/// flight and observe that it got there. `entered` is set before blocking.
///
/// STARTS DISARMED, and that is load-bearing: the write path itself writes Clean blob meta
/// (`Pool/CasPartWriteTxn.cpp:314`), so a latch that blocked from construction would block the test's
/// own fixture instead of the job under test. Call `arm` only once the fixture is built.
class MetaWriteLatchBackend : public DB::Cas::InMemoryBackend
{
public:
    using DB::Cas::Backend::get;
    using DB::Cas::Backend::getStream;
    using DB::Cas::Backend::putIfAbsent;
    using DB::Cas::Backend::putOverwrite;
    using DB::Cas::Backend::casPut;

    std::atomic<bool> entered{false};

    void arm()
    {
        armed.store(true);
    }

    void release()
    {
        std::lock_guard lock(latch_mutex);
        released = true;
        latch_cv.notify_all();
    }

    DB::Cas::PutResult putIfAbsent(
        const String & key, const String & bytes, const DB::Cas::ObjectMeta & meta) override
    {
        waitIfMeta(key);
        return InMemoryBackend::putIfAbsent(key, bytes, meta);
    }

    DB::Cas::PutResult putOverwrite(
        const String & key, const String & bytes, const DB::Cas::Token & expected,
        const DB::Cas::ObjectMeta & meta) override
    {
        waitIfMeta(key);
        return InMemoryBackend::putOverwrite(key, bytes, expected, meta);
    }

    DB::Cas::CasResult casPut(const String & key, const String & bytes,
                              const std::optional<DB::Cas::Token> & expected,
                              const DB::Cas::ObjectMeta & meta) override
    {
        waitIfMeta(key);
        return InMemoryBackend::casPut(key, bytes, expected, meta);
    }

    DB::Cas::DeleteOutcome deleteExact(const String & key, const DB::Cas::Token & token) override
    {
        waitIfMeta(key);
        return InMemoryBackend::deleteExact(key, token);
    }

private:
    void waitIfMeta(const String & key)
    {
        if (!armed.load() || !key.ends_with(".meta"))
            return;
        entered.store(true);
        std::unique_lock lock(latch_mutex);
        latch_cv.wait(lock, [this] { return released; });
    }

    std::atomic<bool> armed{false};
    std::mutex latch_mutex;
    std::condition_variable latch_cv;
    bool released = false;
};

/// Makes a GC round throw at its outcome-log write -- after the round has scheduled its confirmed-meta
/// delete (`Gc/CasGc.cpp`) and before the round's meta-pool wait. Inherits the `.meta` latch so that
/// job can be held in flight across the throw. Both the fault and the latch start off.
class OutcomeLogFaultBackend : public MetaWriteLatchBackend
{
public:
    using DB::Cas::Backend::get;
    using DB::Cas::Backend::putIfAbsent;

    std::atomic<bool> fail_outcome_logs{false};

    DB::Cas::PutResult putIfAbsent(
        const String & key, const String & bytes, const DB::Cas::ObjectMeta & meta) override
    {
        if (fail_outcome_logs.load() && key.contains("outcomes/"))
            return DB::Cas::PutResult{.outcome = DB::Cas::PutOutcome::PreconditionFailed, .token = {}};
        return MetaWriteLatchBackend::putIfAbsent(key, bytes, meta);
    }

    std::optional<DB::Cas::GetResult> get(const String & key, DB::Cas::Range range) override
    {
        if (fail_outcome_logs.load() && key.contains("outcomes/"))
            return std::nullopt;
        return DB::Cas::InMemoryBackend::get(key, range);
    }
};

/// Wait until a latched job has provably reached the backend. A bounded wait that FAILS rather than
/// hangs: a job that never arrives is a broken fixture, and a test that hangs on it reports nothing.
inline void awaitLatchEntered(MetaWriteLatchBackend & backend)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!backend.entered.load())
    {
        ASSERT_LT(std::chrono::steady_clock::now(), deadline)
            << "no meta job reached the backend latch -- the fixture never scheduled one";
        std::this_thread::yield();
    }
}

/// Runs a caller-supplied action ONCE, immediately before the named backend call, so a test can make
/// the mount slot change inside a window `MountLeaseKeeper::claim` holds open. Each hook clears
/// itself after firing.
class MountSlotRaceBackend : public DB::Cas::InMemoryBackend
{
public:
    using DB::Cas::Backend::get;
    using DB::Cas::Backend::getStream;
    using DB::Cas::Backend::putIfAbsent;
    using DB::Cas::Backend::putOverwrite;
    using DB::Cas::Backend::casPut;

    std::function<void()> before_put_if_absent;
    std::function<void()> before_get;
    std::function<void()> before_put_overwrite;

    DB::Cas::PutResult putIfAbsent(
        const String & key, const String & bytes, const DB::Cas::ObjectMeta & meta) override
    {
        fire(before_put_if_absent);
        return InMemoryBackend::putIfAbsent(key, bytes, meta);
    }

    std::optional<DB::Cas::GetResult> get(const String & key, DB::Cas::Range range) override
    {
        fire(before_get);
        return InMemoryBackend::get(key, range);
    }

    DB::Cas::PutResult putOverwrite(
        const String & key, const String & bytes, const DB::Cas::Token & expected,
        const DB::Cas::ObjectMeta & meta) override
    {
        fire(before_put_overwrite);
        return InMemoryBackend::putOverwrite(key, bytes, expected, meta);
    }

private:
    static void fire(std::function<void()> & hook)
    {
        if (!hook)
            return;
        auto once = std::move(hook);
        hook = nullptr;
        once();
    }
};

/// Expect a DB::Exception with EXACTLY `expected_code` AND a message containing `expected_substring`.
/// Needed wherever several distinct branches share one code: the code alone does not identify which
/// one ran, so a test that silently takes the wrong branch would still pass.
template <typename F>
void expectThrowsCodeWithMessage(int expected_code, const String & expected_substring, F && fn)
{
    try
    {
        fn();
        FAIL() << "expected DB::Exception";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), expected_code);
        EXPECT_NE(e.message().find(expected_substring), String::npos)
            << "wrong branch: " << e.message();
    }
}

}
