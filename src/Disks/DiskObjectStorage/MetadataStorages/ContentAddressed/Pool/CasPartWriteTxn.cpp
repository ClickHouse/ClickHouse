#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasBlobMeta.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasBlobUploadPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasBlobHashingWriteBuffer.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasBlobEnvelopeFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequestControl.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <Common/thread_local_rng.h>
#include <Common/config_version.h>
#include <base/defines.h>
#include <fmt/format.h>
#include <algorithm>
#include <chrono>

namespace ProfileEvents
{
    extern const Event CASBlobDeduplicationCacheHit;
    extern const Event CASBlobHeadFirst;
    extern const Event CASBlobBodyPutAvoided;
    extern const Event CASBlobAdoptTrusted;
    extern const Event CASMetaCreateClean;
    extern const Event CASMetaAdoptBackfill;
    extern const Event CASMetaResurrectClean;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int FILE_DOESNT_EXIST;
    extern const int NOT_IMPLEMENTED;
    extern const int ABORTED;
    extern const int CORRUPTED_DATA;
    extern const int LIMIT_EXCEEDED;
}
}

namespace DB::Cas
{

namespace
{

/// Backpressure caps, enforced fail-closed in stageManifest before the body write returns.
constexpr uint64_t kMaxManifestEntries = 1048576;
constexpr uint64_t kMaxManifestEncodedBytes = 256ULL << 20;        /// 256 MiB
constexpr uint64_t kMaxManifestInlineBytesTotal = 16ULL << 20;     /// 16 MiB
constexpr uint64_t kMaxLargestInlineEntryBytes = 1ULL << 20;       /// 1 MiB

/// Two thread_local_rng draws composed into a UInt128. Used both to mint build ids and to mint, on
/// every upload/re-upload, a FRESH incarnation_tag (W-FRESH-TAG).
UInt128 mintU128()
{
    const UInt64 hi = thread_local_rng();
    const UInt64 lo = thread_local_rng();
    return (static_cast<UInt128>(hi) << 64) | lo;
}

uint64_t nowMs()
{
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
}

}

/// The pool-wide content-hash convention, with the selected `algo`: `Cas::blobHashHexOneShot` uses the same
/// streaming/chunked convention the write path uses for `CityHash128` (`ContentAddressedWriteBuffers`: `getHashHex` ->
/// `BlobId` -> `hexToU128`) and the one-shot `XXH3_128bits` call for `XXH3_128` (defined to agree
/// with its own streaming digest). The core otherwise never re-hashes payloads; any copy-forward
/// re-verification must use this convention. A one-shot `CityHash128` instead of the chunked convention
/// diverges for payloads larger than one hash block and can report valid stored content as `CORRUPTED_DATA`.
///
/// Returns the full `BlobRef` pair at `algo`'s own width (via `codecFor`) — never a bare digest. Exported
/// (declared in `CasPartWriteTxn.h`) so the wiring's inline-candidate hashing
/// (`ContentAddressedTransaction.cpp`) can mint the same way the
/// streaming blob path does, rather than reimplementing the hex round-trip inline.
BlobRef poolContentHash(BlobHashAlgo algo, std::string_view payload)
{
    return BlobRef{algo, codecFor(algo).fromHex(blobHashHexOneShot(algo, payload))};
}

BlobSource BlobSource::fromString(String bytes)
{
    BlobSource source;
    source.size = bytes.size();
    /// Owning reader: `ReadBufferFromString` only borrows, and this factory outlives no single call
    /// -- an owning copy per attempt removes the question entirely for a source that is small by
    /// construction (this helper exists for tests and inline payloads).
    source.open = [b = std::move(bytes)]() -> std::unique_ptr<ReadBuffer>
    { return std::make_unique<ReadBufferFromOwnString>(b); };
    return source;
}

PartWriteTxn::PartWriteTxn(PoolPtr store_, UInt128 build_id_,
             uint64_t build_seq_, uint64_t epoch_, PartWriteInfo info_)
    : store(std::move(store_))
    , build_id(build_id_)
    , build_seq(build_seq_)
    , epoch(epoch_)
    , info(std::move(info_))
{
    /// A build began. `build_id`, `build_seq`, and `epoch` identify it for token-join attribution
    /// against the GC delete rows.
    EventEmitter{*store}.emit([&](CasEvent & e)
    {
        e.type = CasEventType::BuildStart;
        e.ref_name = info.intended_ref.value_or("");
        e.token = u128ToHex(build_id);
        e.outcome = "started";
        e.reason = "beginPartWrite: build in-flight";
        e.detail = {{"build_seq", std::to_string(build_seq)}, {"epoch", std::to_string(epoch)}};
    });
}

PartWriteTxn::~PartWriteTxn()
{
    /// An attempted precommit whose terminal operation did not settle may still be a durable owner.
    /// Transfer that exact cleanup duty to the mount and KEEP the build active: the next mutation of
    /// this namespace first resolves the every-attempt wedge, then removes the owner if it exists, and
    /// retires the sequence only after that same state observation proves the duty discharged. If the
    /// process ends first, no false clean farewell is written and successor recovery seals the old
    /// epoch before sweeping its stale precommit.
    if (precommit_state == PrecommitState::Uncertain || precommit_state == PrecommitState::Durable)
    {
        store->enqueueWriterCleanupDuty(
            precommit_target_ns, precommit_final_ref, precommit_manifest, build_seq);
        return;
    }

    /// No owner duty remains: promotion/abandon settled it, or no precommit append was attempted.
    store->retireBuildSeq(build_seq);
}

void PartWriteTxn::requireAlive() const
{
    if (!alive)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PartWriteTxn has been abandoned; no further operations allowed");
    /// `dropNamespace` cancels in-flight builds once its removal transaction is
    /// durable. A cancelled build must not promote/precommit a fresh owner into the just-removed namespace
    /// (nor stage more debris there), so every further op fails closed here -- fast, before any backend
    /// work, with a clear diagnostic (the append lane would reject a promote/precommit anyway).
    if (cancelled.load(std::memory_order_acquire))
        throwCasWriteRetryLater(
            "PartWriteTxn cancelled: its owning namespace was removed (dropNamespace) while this build was "
            "in flight; restart the build only after the namespace is recreated");
    /// Self-remount (fence-out recovery) supersedes the mount incarnation this build was minted
    /// under; its write fence already interrupted the build mid-flight, so every further step fails
    /// closed and the caller restarts the build under the live epoch.
    if (const uint64_t live = store->liveWriterEpoch(); epoch != live)
        throwCasWriteRetryLater(fmt::format(
            "PartWriteTxn (writer_epoch {}) belongs to a superseded mount incarnation (live epoch {}) — "
            "the mount was fenced out and self-remounted; restart the build", epoch, live));
}

PutBlobResult PartWriteTxn::putBlob(const BlobRef & ref, BlobSource source)
{
    /// Serial API preserved for existing callers: run the transaction-detached primitive, then fold its
    /// single result into `build` on this (the owning writer) thread. Semantics are byte-for-byte those
    /// of the pre-carve `putBlob`. The fan-out path (spec §1) instead collects many results off-thread and
    /// folds them together via `mergeBlobUploadResults`. `source.size` is the streaming byte authority, so
    /// the request's `declared_size` mirrors it.
    const uint64_t declared_size = source.size;
    const BlobUploadResult r = uploadBlobDetached(BlobUploadRequest{ref, std::move(source), declared_size});
    deps[r.ref] = r.dep;
    return PutBlobResult{r.ref, r.dep.size};
}

BlobUploadResult PartWriteTxn::uploadBlobDetached(const BlobUploadRequest & req) const
{
    requireAlive();

    const PoolConfig & cfg = store->poolConfig();
    /// The caller (the write-mint site) already produced the full `BlobRef` pair (algo + digest), so there
    /// is no hex round-trip here. `logical_ref` is the blob identity end-to-end: the dedup cache and every
    /// downstream event render key off THIS value directly.
    const BlobRef & logical_ref = req.ref;
    const String key = store->layout().blobKey(req.ref);
    const BlobSource & source = req.source;

    /// The source is RE-READABLE (the caller's `open` re-reads a staged temp file, or re-emits a
    /// captured String): it can be invoked MULTIPLE times — the primary streaming PUT plus any INV-1
    /// re-upload — so we never materialize the whole blob into memory here. The byte count is verified
    /// against `source.size` at each streaming write site (via the sink buffer's `count()`), not by a
    /// full pre-materialization, so peak memory is bounded by the write-buffer, not the blob size.

    /// HEAD-before-PUT on a likely dedup hit (cache says present) or a large body (where a
    /// wasted body-PUT that 412s is expensive — and on a store that early-closes a doomed conditional
    /// PUT, the broken-pipe and retry storm caused by early rejection). A present HEAD ⇒ admit without streaming the body;
    /// a stale/absent HEAD ⇒ fall through to the normal conditional upload. SAFE by construction: we
    /// always genuinely observe present-at-round before skipping the body, so the cache can never cause
    /// a dangle (a stale hit just HEADs 404 and uploads). The cache membership is read ONCE here: it both
    /// arms the HEAD-first gate and distinguishes the `DeduplicationCacheHit` outcome from a size-triggered `HeadHit`.
    const bool cache_hit = store->dedupCacheContains(logical_ref);
    const bool head_first =
        cache_hit
        || (cfg.deduplication_head_first_min_bytes > 0 && source.size >= cfg.deduplication_head_first_min_bytes);
    if (head_first)
    {
        ProfileEvents::increment(ProfileEvents::CASBlobHeadFirst);
        const HeadResult hr = store->backend().head(key);
        if (hr.exists)
        {
            ProfileEvents::increment(ProfileEvents::CASBlobBodyPutAvoided);
            if (cache_hit)
                ProfileEvents::increment(ProfileEvents::CASBlobDeduplicationCacheHit);
            try
            {
                const BlobDepRecord dep = observeAndAdmit(ObjectKind::Blob, logical_ref, key, hr);
                store->dedupCacheAdd(logical_ref);
                return BlobUploadResult{req.ref, dep,
                    cache_hit ? BlobUploadOutcome::DeduplicationCacheHit : BlobUploadOutcome::HeadHit};
            }
            catch (const Exception & e)
            {
                /// INV-1: HEAD-first path hit a condemned token → re-upload from our OWN source bytes.
                if (e.code() != ErrorCodes::ABORTED)
                    throw;
                /// Fall through to uploadFromSource below.
            }
        }
        /// hr.exists == false OR condemned-ABORTED → fall through to uploadFromSource / fresh upload.
    }

    /// Fresh-upload path + condemned-dedup recovery under INV-1. Try the primary upload via
    /// uploadFromSource (which handles condemned-present via putOverwrite and absent via putIfAbsentStream
    /// without any backend().get). Bounded loop guards against rare concurrent-condemnation churn.
    constexpr int max_attempts = 8;
    for (int attempt = 0; attempt < max_attempts; ++attempt)
    {
        try
        {
            const BlobUploadResult r = uploadFromSource(ObjectKind::Blob, logical_ref, key, source);
            /// This hash is now known-present — future writers can HEAD-first and skip the body.
            store->dedupCacheAdd(logical_ref);
            return r;
        }
        catch (const Exception & e)
        {
            /// ABORTED from uploadFromSource is retryable — re-upload by re-streaming from our re-readable
            /// source (bounded). Two cases produce it:
            ///   • a racing writer displaced the condemned token before our putOverwrite landed and
            ///     their fresh incarnation is itself already condemned (observeAndAdmit → ABORTED); or
            ///   • the object was GC-deleted during the post-412 revival re-observe:
            ///     reviveObserve converts FILE_DOESNT_EXIST → ABORTED so the vanish re-uploads here
            ///     rather than escaping FATAL. Both are rare races covered by the bounded loop.
            if (e.code() != ErrorCodes::ABORTED || attempt + 1 == max_attempts)
                throw;
        }
    }

    /// Unreachable: the loop either returns or rethrows on the final attempt.
    throw Exception(ErrorCodes::LOGICAL_ERROR, "uploadBlobDetached: exhausted retries for {}", key);
}

void PartWriteTxn::mergeBlobUploadResults(std::span<const BlobUploadResult> results)
{
    /// Prevalidate EVERYTHING first, touching nothing but locals: a result without a token is
    /// incomplete (every `uploadBlobDetached` branch sets one; a tokenless dep only ever arrives
    /// through `adoptEvidence`'s separate direct-fold path, never through this method) -- a caller bug,
    /// failed closed rather than merged as a hole. Two results for the SAME ref must carry an
    /// IDENTICAL dep record (the fan-out's one-task-per-unique-ref invariant, spec §1); a conflict --
    /// most commonly a conflicting size -- means that invariant was violated upstream.
    std::map<DepKey, const BlobDepRecord *> seen;
    for (const auto & r : results)
    {
        if (!r.dep.token.has_value())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "PartWriteTxn::mergeBlobUploadResults: incomplete result for {} (no token) -- every "
                "uploadBlobDetached branch sets one; a tokenless dep must be recorded via adoptEvidence, "
                "never merged here", blobIdOf(r.ref));
        const auto [it, inserted] = seen.try_emplace(r.ref, &r.dep);
        if (!inserted && !(*it->second == r.dep))
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "PartWriteTxn::mergeBlobUploadResults: dep records differ for {} (sizes {} vs {}) -- "
                "the fan-out must launch exactly one task per unique ref and merge exactly one result; "
                "records for one ref that differ in ANY field (size, token, kind, or adopted) are a "
                "wiring error, so the sizes shown may be equal when the mismatch is in another field",
                blobIdOf(r.ref), it->second->size, r.dep.size);
    }

    /// Build-and-swap: apply every result into a COPY of the live `deps`. The live member is touched
    /// ONLY by the final `swap` below, which runs after every result has applied -- so a mid-loop
    /// exception (a `bad_alloc` from map-node allocation, or one injected by `setMergeHookForTest`'s
    /// hook) leaves `deps` byte-for-byte as it was before this call. Applying an already-validated
    /// duplicate a second time is idempotent (identical value, same key).
    std::map<DepKey, BlobDepRecord> candidate = deps;
    size_t applied = 0;
    for (const auto & r : results)
    {
        candidate[r.ref] = r.dep;
        ++applied;
        if (merge_hook_for_test)
            merge_hook_for_test(applied);
    }
    deps.swap(candidate);
}

bool PartWriteTxn::isTrustedAdopt(const BlobRef & ref) const
{
    /// §4: a leaf trusted at promote iff this build holds a TOKENLESS dep recorded by adoptEvidence
    /// (a committed-source W-EVIDENCE adopt: the source pins it, in-degree >= 1, not condemnable). A
    /// tokenless PENDING-upload dep (recordPendingBlobDep, adopted=false) is NOT trusted — it must be
    /// tokened by putBlob before promote; reaching promote un-tokened is a staging bug (fail closed).
    auto it = deps.find(ref);
    return it != deps.end() && !it->second.token.has_value() && it->second.adopted;
}

bool PartWriteTxn::depIsTokened(const BlobRef & ref) const
{
    /// Discriminator for B156b: a putBlob'd blob records a TOKENED dep (recreatable by retrying), an
    /// adoptFromTree carry-forward records a TOKENLESS W-EVIDENCE dep (not recreatable — pinned by a
    /// committed source). Returns false when this build has no dep for the ref (the caller decides
    /// the default; not-tokened is the fail-loud, INV-NO-LOSS-safe choice).
    auto it = deps.find(ref);
    return it != deps.end() && it->second.token.has_value();
}

BlobDepRecord PartWriteTxn::observeAndAdmit(ObjectKind kind, const BlobRef & ref, const String & key) const
{
    /// EDGE-BEFORE-OBSERVE: the durable-precommit guard
    /// lives in the 4-arg overload below, scoped to its ADOPT branch only — NOT here. A HEAD result
    /// reached through this wrapper (e.g. `reviveObserve`'s post-race re-observe) can resolve to EITHER
    /// the condemned/ABORTED branch or the adopt branch once the 4-arg overload point-reads the meta;
    /// gating here (before that point-read) would wrongly block the condemned branch too, which
    /// re-uploads under THIS build's own build_id via `uploadFromSource` and stays watermark-protected
    /// pre-precommit.
    const HeadResult hr = store->backend().head(key);
    if (!hr.exists)
        /// Object absent at observe time. The live caller of this overload is the revival re-observe in
        /// `uploadFromSource` (`reviveObserve` below, retryable): a GC-deleted object at re-observe time
        /// is a race under INV-3 — the caller HOLDS the source bytes, so it catches FILE_DOESNT_EXIST
        /// here and re-throws it as ABORTED so the INSERT layer sees the uniform retryable error and
        /// re-uploads from those bytes. This overload only ever raises FILE_DOESNT_EXIST; the caller
        /// decides fail-closed vs retry.
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST,
            "PartWriteTxn::observeAndAdmit: object {} vanished (GC-deleted) before observe; "
            "caller must re-upload/re-materialize from source", key);
    return observeAndAdmit(kind, ref, key, hr);
}

BlobDepRecord PartWriteTxn::observeAndAdmit(ObjectKind kind, const BlobRef & ref, const String & key, const HeadResult & hr) const
{
    /// `hr.exists` is guaranteed by the caller (the 3-arg wrapper checked it; the putBlob HEAD-first
    /// path only calls this on a present HEAD). Avoids a redundant second HEAD on the dedup-hit path.
    /// Logical (payload) size = object size minus the pool's fixed blob header. GUARD against
    /// unsigned underflow: a truncated/corrupt object whose size is below the header length must surface
    /// as CORRUPTED_DATA, never wrap to a huge value. Mirrors the GC path's `retiredLogicalSize`.
    const uint64_t header_len = store->poolMeta().blob_header_len;
    if (hr.size < header_len)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "PartWriteTxn: {} object {} size {} is below the pool blob header length {}",
            kind == ObjectKind::Blob ? "blob" : "manifest", key, hr.size, header_len);
    const uint64_t logical_size = hr.size - header_len;

    const CasEventObjectKind ev_kind = toEventKind(kind);

    /// The condemned decision is a per-hash META POINT-READ, not
    /// the retired-view snapshot. `absent` meta means "not condemned" — GC always writes a `Condemned`
    /// meta BEFORE it ever deletes a body, so an absent meta is exactly as live as a `Clean` one.
    /// The meta ops layer is `BlobRef`-keyed directly (derives its codec from `ref.algo`
    /// internally). Every event-log render below uses `blobIdOf(ref)` ("<algoName>:<hex>"), never a
    /// bare hex.
    const auto lm = loadMeta(store->backend(), store->layout(), ref);
    const bool condemned = lm && lm->meta.state == MetaState::Condemned;
    if (condemned)
    {
        /// INV-1 (revival-from-source): the observed token is condemned — we must NOT read the dying
        /// object via backend().get. Throw ABORTED so the caller can re-upload from its OWN source bytes.
        ///   • putBlob (has BlobSource): catches ABORTED, calls uploadFromSource from held bytes.
        ///   • a bodyless observe (no source): propagates ABORTED (retryable; caller retries op).
        EventEmitter{*store}.emit([&](CasEvent & e)
        {
            e.type = CasEventType::BlobReuseResurrect;
            e.object_kind = ev_kind;
            e.object_hash = blobIdOf(ref);
            e.token = hr.token.value;
            e.round = 0;   /// the round is no longer a writer concept (meta point-read replaces the retired view)
            e.outcome = "condemned";
            e.reason = "observed token is condemned (meta point-read); caller must re-upload from source (INV-1)";
        });
        throw Exception(ErrorCodes::ABORTED,
            "PartWriteTxn::observeAndAdmit: condemned token for {} — caller must re-upload from source bytes (INV-1)",
            key);
    }

    /// EDGE-BEFORE-OBSERVE: from here on we are about to
    /// ADOPT the live (non-condemned) incarnation as our own dependency — safe ONLY under this build's
    /// durable precommit closure, because an adopted blob carries the ORIGINAL writer's build_id, so the
    /// newborn-debris watermark does not cover it. (The condemned branch above is unaffected — it
    /// re-uploads under THIS build's own build_id via `uploadFromSource`, which stays watermark-protected
    /// pre-precommit.) See the TLA+ order sabotage (Gate A). A4: a real throw, not chassert — chassert is
    /// compiled out in release, and a wiring/retry bug that reached adopt without a durable precommit
    /// would silently drop watermark protection (later dangling ref / data loss with no production
    /// signal).
    /// `Durable` and nothing else. `Uncertain` is a precommit that MAY be live and may equally not
    /// exist at all, and "may" is not the closure this invariant needs: the adopted blob would carry
    /// the original writer's build_id with only a possibly-absent edge protecting it. It fails closed
    /// here exactly as `NotAttempted` does, and for the same reason.
    if (precommit_state != PrecommitState::Durable)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "PartWriteTxn::observeAndAdmit: EDGE-BEFORE-OBSERVE invariant violated — adopting an existing "
            "incarnation before this build's precommit is durable would admit {} ({}) under the original "
            "writer's build_id with no newborn-debris watermark protection",
            key, kind == ObjectKind::Blob ? "blob" : "manifest");

    /// `!lm`: no meta yet for this hash (a pre-existing blob from before this protocol, or a lost race
    /// with a concurrent fresh-uploader's own meta write). Best-effort create it as Clean so future
    /// point-readers (writers and GC) never have to fall back to a HEAD-only guess. A Conflict here just
    /// means a racing writer already created it — both agree on the same Clean steady state.
    if (!lm)
    {
        ProfileEvents::increment(ProfileEvents::CASMetaAdoptBackfill);
        putMetaIfAbsent(*store, ref,
            BlobMeta{.state = MetaState::Clean, .condemn_round = 0, .size = logical_size});
    }

    /// Adopt the current incarnation — free, no bytes moved.
    /// Reuse an ADOPTED existing incarnation's token as NOT condemned (per the meta point-read).
    /// Was the CAREUSE adopt audit line. Token-join this against a later blob_delete
    /// of the same hash/token to pin a reuse-of-an-object-being-deleted race.
    EventEmitter{*store}.emit([&](CasEvent & e)
    {
        e.type = CasEventType::BlobReuseAdopt;
        e.object_kind = ev_kind;
        e.object_hash = blobIdOf(ref);
        e.token = hr.token.value;
        e.round = 0;   /// the round is no longer a writer concept (meta point-read replaces the retired view)
        e.outcome = "adopt";
        e.reason = "observed token not condemned (meta point-read); adopted the live incarnation (no bytes moved)";
    });
    /// Build-neutral: RETURN the adopt dep (tokened, adopted=false) instead of folding it into `deps`.
    return BlobDepRecord{kind, hr.token, logical_size, /*adopted=*/false};
}

BlobUploadResult PartWriteTxn::uploadFromSource(ObjectKind kind, const BlobRef & ref, const String & key, const BlobSource & source) const
{
    /// StagingPromoted vs FreshUpload discriminator for the write-once create terminals: the source
    /// carries a server-side-copy descriptor iff the bytes already live in an S3 staging object.
    const BlobUploadOutcome fresh_outcome
        = source.server_side_copy_from ? BlobUploadOutcome::StagingPromoted : BlobUploadOutcome::FreshUpload;

    /// INV-1 (revival-from-source): re-upload a condemned or absent object from the writer's OWN
    /// re-readable source — NEVER calls backend().get to read the dying object. W-FRESH-TAG: fresh
    /// incarnation_tag and this build's build_id so the new incarnation is owned by THIS live build
    /// (the source-based resurrection rule closes the prior ownership gap). The payload is STREAMED into the put sink
    /// (`source.open`), never materialized into a full in-memory copy on the common
    /// If-None-Match path; `source.open` is re-invoked on each attempt (it re-reads the staged
    /// temp file), which is exactly what preserves INV-1 across retries.
    const PoolMeta & meta = store->poolMeta();
    const PoolConfig & cfg = store->poolConfig();
    /// `ref` is the full blob identity (algo + digest) end-to-end -- the `.meta` API is
    /// `BlobRef`-keyed directly, and the dep map + every event render below key off `ref` too.

    auto buildHeader = [&]() -> String
    {
        EnvelopeHeader header;
        header.kind = kind;
        header.incarnation_tag = mintU128();
        header.build_id = build_id;
        /// ch = the real ClickHouse VERSION_INTEGER (diagnostic-only; no decision reads it) — the v3
        /// envelope drops writer_version/hash_algo/domain_id, so forensics ride on ch + bld.
        header.provenance = Provenance{nowMs(), cfg.server_id, VERSION_INTEGER, info.op};
        if (kind == ObjectKind::Blob)
            header.intended_ref = info.intended_ref;
        /// The v3 codec pads to the pool's fixed header length and TRUNCATES a too-long intended_ref
        /// internally (it is diagnostic-only), so the old drop-and-retry is gone — one encode call.
        return encodeEnvelopeHeader(header, static_cast<uint32_t>(meta.blob_header_len));
    };

    const CasEventObjectKind ev_kind = toEventKind(kind);

    /// Revival-local wrapper for the post-412 re-observe (INV-3). On the post-412 path a
    /// racing writer is assumed to have (re-)created the object, so we adopt its token via the 3-arg
    /// observeAndAdmit. But the object can be GC-deleted in the window (present at the conditional PUT
    /// → 412, gone at the subsequent HEAD), making the 3-arg overload throw FILE_DOESNT_EXIST. The
    /// caller (putBlob's fresh-upload path, via `uploadFromSource`) HOLDS the source bytes, so a vanish
    /// here is a retryable race: convert FILE_DOESNT_EXIST → ABORTED so putBlob's bounded retry loop
    /// re-uploads from those bytes. Without this, FILE_DOESNT_EXIST escaped putBlob's ABORTED-only catch
    /// as a FATAL INSERT failure — the sibling of the gate bug.
    auto reviveObserve = [&](const String & k_) -> BlobDepRecord
    {
        try
        {
            return observeAndAdmit(kind, ref, k_);
        }
        catch (const Exception & e_)
        {
            if (e_.code() != ErrorCodes::FILE_DOESNT_EXIST)
                throw;
            throw Exception(ErrorCodes::ABORTED,
                "uploadFromSource: object {} vanished (GC-deleted) during revival re-observe; "
                "retry the operation — re-upload from held source bytes (INV-3)", k_);
        }
    };

    /// Build-neutral: emit the BlobPut audit event and RETURN the fresh/resurrect dep record (tokened,
    /// adopted=false) for the caller to compose into its `BlobUploadResult` — it folds nothing into `deps`.
    auto makeDepAndEmit = [&](Token tok) -> BlobDepRecord
    {
        EventEmitter{*store}.emit([&](CasEvent & e)
        {
            e.type = CasEventType::BlobPut;
            e.object_kind = ev_kind;
            e.object_hash = blobIdOf(ref);
            e.token = tok.value;
            e.round = 0;   /// the round is no longer a writer concept (meta point-read replaces the retired view)
            e.outcome = "ok";
            e.reason = "uploadFromSource: fresh incarnation streamed from writer's own re-readable source (INV-1)";
            e.detail = {{"size", std::to_string(source.size)}, {"build_id", u128ToHex(build_id)}};
        });
        return BlobDepRecord{kind, tok, source.size, /*adopted=*/false};
    };

    /// Meta write for the RESURRECT (condemned-displacement) case: flip the now-stale Condemned meta
    /// back to Clean now that a live incarnation has displaced the condemned body. `lm_before` is the
    /// point-read taken just before the condemned decision — its etag is the CAS precondition (absent
    /// when there is no prior point-read at all, the FRESH upload case below). Each outer attempt
    /// routes through the Pool's request controller (putMetaIfAbsent/casMeta), which already absorbs
    /// a transient transport error (SlowDown/429/5xx) within its own budget — this outer loop reacts
    /// only to a genuine `Conflict` (the marker's current token AND bytes both differ from what this
    /// call intended: a racing writer or GC re-condemning) by reloading and retrying against the
    /// fresh state. `Unresolved` (the controller's own budget/fence exhausted for one attempt) is
    /// retried the same way — reloading may simply observe the write actually landed. After
    /// max_meta_attempts of this outer loop WITHOUT a Committed result, this is a persistent failure,
    /// not a blip (each outer attempt already burned its own inner retry budget) — the RCA requires
    /// this reach the caller as a controlled retry-later signal, never a silent skip: a dropped
    /// freshness marker leaves stale state for the next point-reader.
    auto writeResurrectMetaClean = [&](std::optional<LoadedMeta> lm_before)
    {
        ProfileEvents::increment(ProfileEvents::CASMetaResurrectClean);
        const BlobMeta clean{.state = MetaState::Clean, .condemn_round = 0, .size = source.size};
        constexpr int max_meta_attempts = 8;
        for (int attempt = 0; attempt < max_meta_attempts; ++attempt)
        {
            const bool committed = lm_before
                ? casMeta(*store, ref, lm_before->etag, clean).outcome == CasOverwriteOutcome::Committed
                : putMetaIfAbsent(*store, ref, clean).outcome == CasOverwriteOutcome::Committed;
            if (committed)
                return;
            lm_before = loadMeta(store->backend(), store->layout(), ref);
        }
        throwCasWriteRetryLater(fmt::format(
            "writeResurrectMetaClean: freshness-meta transition to Clean for {} did not land within "
            "{} attempts (each already budget-controlled) — the body incarnation is durable, but the "
            "meta marker is stuck; retry", store->layout().blobMetaKey(ref), max_meta_attempts));
    };

    /// Meta write for the FRESH (absent -> present) upload cases: the body just transitioned via
    /// If-None-Match, so the freshness meta is created as Clean. Delegates to writeResurrectMetaClean
    /// with no prior point-read: a pre-existing marker there is NOT always "a racing writer creating
    /// the same Clean state" (the retired comment this replaced assumed) -- it can be a stale
    /// Condemned marker left over from before this exact body vanished and was freshly re-uploaded
    /// (proven reachable by CasPartWriteTxn.PutBlobResurrectVanishedReUploadsHeldBody), which must be
    /// reconciled to Clean the same way a resurrect does, not silently ignored.
    auto writeFreshMetaClean = [&]()
    {
        ProfileEvents::increment(ProfileEvents::CASMetaCreateClean);
        writeResurrectMetaClean(std::nullopt);
    };

    /// Stream header + payload into a fresh putIfAbsentStream sink WITHOUT materializing the whole blob.
    /// `source.open` re-reads the staged temp file (INV-1: the writer's own source, never the
    /// dying object). The payload byte count is verified against `source.size` via the sink buffer's
    /// `count()` (total bytes written so far) — the streaming equivalent of the old pre-materialized
    /// size check, with no full in-memory copy. A mismatch is a LOGICAL_ERROR (a buggy/racing source).
    ///
    /// The whole conditional create rides the Pool's shared request controller
    /// (`conditionalCreateControlled` — the retry controller's "any other
    /// controller-bypassing conditional-write call site"): budgeted attempts + fence-gated backoff +
    /// exact-key OCCUPANCY resolve, replacing the old bare single attempt whose whole S3-blip tolerance
    /// was ONE ~3s adaptive-timeout attempt. Reissue is sound for BOTH primitives: the streaming PUT
    /// re-invokes `source.open` (the REPLAYABLE source contract — a fresh re-upload, never a
    /// GET-revive), and the server-side copy re-reads the intact staging object. Each re-stream mints a
    /// fresh incarnation_tag (W-FRESH-TAG), so byte-exact resolve is impossible by design and the
    /// controller resolves by occupancy instead: an occupant at this content-addressed key IS the
    /// intended content (whether our own landed ambiguous attempt or a twin), surfaced as
    /// PreconditionFailed so every existing gate branch below is UNCHANGED.
    ///
    /// The size-check LOGICAL_ERROR below stays instant and loud: `conditionalCreateControlled`
    /// propagates every deterministic local failure — LOGICAL_ERROR, NOT_IMPLEMENTED (the promoteStaged
    /// mode guard), BAD_ARGUMENTS (escaping buildHeader's second encode), CORRUPTED_DATA — from the
    /// attempt unchanged (a caller/config bug reissue would only replay — pinned by
    /// `CasPartWriteTxn.PutBlobWrongSizeFailsClosed` and the controller-level
    /// `DeterministicLocalFailuresPropagateInstantly`).
    auto streamIfAbsent = [&]() -> PutResult
    {
        const auto one_attempt = [&]() -> PutResult
        {
            /// S3-native staging promote: when the source
            /// carries a server-side-copy descriptor, the write-once CREATE primitive is a conditional
            /// server-side copy of the staging object to `key` (`If-None-Match:*`) instead of a
            /// client-side streaming PUT. Same write-once contract (`Done` + dest-ETag token on created,
            /// `PreconditionFailed` when `key` already exists). The staging object IS the promote source
            /// — no envelope is streamed here.
            if (source.server_side_copy_from)
                return store->backend().promoteStaged(*source.server_side_copy_from, key);

            /// No owner metadata is needed: protection is the precommit edge — reachability, not `cas_owner`.
            /// A throw mid-stream abandons the sink (its dtor cancels): nothing is ever published by a
            /// failed attempt except via the storage's own late-landing ambiguity, which the controller
            /// resolves.
            WriteSinkPtr sink = store->backend().putIfAbsentStream(key);
            WriteBuffer & out = sink->buffer();
            writeString(buildHeader(), out);
            const size_t before = out.count();
            copyData(*source.open(), out);
            const size_t written = out.count() - before;
            if (written != source.size)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "uploadFromSource: source streamed {} bytes, declared {}", written, source.size);
            return sink->finalize();
        };

        const CasCreateResult res = store->stagingConditionalCreate(key, one_attempt);
        switch (res.outcome)
        {
            case CasCreateOutcome::Committed:
                return PutResult{PutOutcome::Done, res.token};
            case CasCreateOutcome::Occupied:
                return PutResult{PutOutcome::PreconditionFailed, {}};
            case CasCreateOutcome::Unresolved:
                break;
        }
        /// Unresolved = budget exhausted or fence lost without a definite outcome. Nothing referenced
        /// this incarnation (deps/meta are recorded only on a definite outcome); a late-landing body is
        /// inert debris behind the content-addressed key — a future writer of the same content adopts
        /// or displaces it through the normal occupancy machinery. NETWORK_ERROR = the same retryable
        /// abort class stageManifest and the ref lane map their exhausted budgets to.
        throwCasWriteRetryLater(fmt::format(
            "uploadFromSource: conditional create at '{}' is UNCERTAIN (retry budget exhausted or mount "
            "fence lost) — nothing was acknowledged; retry re-uploads from the writer's own source (INV-1)",
            key));
    };

    /// Try the If-None-Match upload (object absent or race with another writer).
    {
        const PutResult res = streamIfAbsent();
        if (res.outcome == PutOutcome::Done)
        {
            const BlobDepRecord dep = makeDepAndEmit(res.token);
            writeFreshMetaClean();
            return BlobUploadResult{ref, dep, fresh_outcome};
        }
    }

    /// PreconditionFailed: an incarnation exists. HEAD it to check whether it is condemned or live.
    /// We do NOT read the body (no backend().get) — we only need the token to decide:
    ///   • live (not condemned)  → adopt; this is the standard dedup case.
    ///   • condemned             → displace via putOverwrite(If-Match: current_token) by re-reading our
    ///                            own source, so we never read the dying object. This is the
    ///                            equivalent of the old `resurrect` minus the GET.
    const HeadResult hr = store->backend().head(key);
    if (!hr.exists)
    {
        /// Object vanished between putIfAbsentStream (412d) and our HEAD — a concurrent GC delete.
        /// The object is now absent; re-stream from our re-readable source (bounded by caller).
        const PutResult res2 = streamIfAbsent();
        if (res2.outcome == PutOutcome::Done)
        {
            const BlobDepRecord dep = makeDepAndEmit(res2.token);
            writeFreshMetaClean();
            return BlobUploadResult{ref, dep, fresh_outcome};
        }
        /// Still 412 after the vanish-and-retry: a racing writer re-created it. Adopt their token.
        /// reviveObserve converts FILE_DOESNT_EXIST (deleted again in the window) → ABORTED (retryable).
        return BlobUploadResult{ref, reviveObserve(key), BlobUploadOutcome::HeadMissAdopted};
    }

    /// The condemned decision is a per-hash META POINT-READ, not
    /// the retired-view snapshot. `lm` (and its etag) is reused below to flip a condemned meta back to
    /// Clean once the resurrect displacement lands.
    const auto lm = loadMeta(store->backend(), store->layout(), ref);
    const bool condemned = lm && lm->meta.state == MetaState::Condemned;
    if (!condemned)
    {
        /// Live (not condemned): adopt the current incarnation — free, no bytes moved.
        return BlobUploadResult{ref, observeAndAdmit(kind, ref, key, hr), BlobUploadOutcome::HeadMissAdopted};
    }

    /// Condemned: displace the condemned incarnation with our fresh source.
    ///
    /// rev.7 [C2] (backlog {#c2-resurrect-putoverwrite-fence-check}): the two displacement calls below
    /// (`resurrect` / `putOverwrite`) are RAW backend writes with NO controller/fence coupling —
    /// unlike `streamIfAbsent`, which rides the request controller's fence gate. They were the only
    /// durable-effect writes left outside Task 4's fence-generation gate. Capture the mount fence
    /// generation now, at the displacement DECISION, and re-check it (and `mayMutate()`) immediately before
    /// whichever raw write we issue: a lease lost — or re-armed under a fresh incarnation — since we
    /// observed the condemned state aborts with the typed transient refusal before any
    /// stale-incarnation displacement can land. Mirrors `CasPlainObjects::casPutObject`'s
    /// capture-at-admission-then-check-before-the-durable-write shape.
    const uint64_t displace_admitted_generation = store->fenceGeneration();
    if (source.server_side_copy_from)
    {
        /// S3-native staging RESURRECT (INV-NO-RETURN): re-establish a fresh
        /// incarnation by re-uploading OUR OWN staging PAYLOAD under a FRESHLY-tagged envelope header —
        /// NEVER a read/copy of the condemned `key` (`feedback_ca_resurrect_invariant`). We reach here
        /// ONLY after the per-hash meta point-read observed `Condemned` just above, so this overwrites a
        /// condemned body -- or, on a lost race, an equivalent FRESH resurrection of it, which is
        /// accepted: payloads are content-identical, so the overwrite rotates only envelope and token.
        /// A live incarnation of DIFFERENT content is unreachable here by the content address itself.
        ///
        /// CRITICAL — a VERBATIM server-side copy of the create-time staging object would reproduce the
        /// condemned incarnation's exact bytes ⇒ identical ETag ⇒ the queued exact-token delete of the
        /// condemned incarnation would kill the live resurrection (data loss). `buildHeader` mints a
        /// FRESH `incarnation_tag` distinct from the staging header's tag, so the resurrected body (and
        /// hence its ETag) differs from the condemned incarnation regardless of edge-before-observe. The
        /// backend reads the payload from the staging object skipping its own `blob_header_len` envelope
        /// header and prepends this fresh header.
        const String fresh_header = buildHeader();
        /// rev.7 [C2]: a raw `backend()` call with NO controller fence coupling (unlike
        /// `promoteStaged`/`putIfAbsentStream` above, reached only through `stagingConditionalCreate`'s
        /// controller+fence_ok). The `checkFenceOrThrow` re-checks the fence generation captured at the
        /// displacement decision, closing the same gap Task 4 closed on the plain-object surface.
        Token tok{};
        store->checkFenceOrThrow(displace_admitted_generation);
        /// The staging object's own envelope header is skipped HERE, by whoever knows its shape: the
        /// backend is handed a reader already positioned at the payload.
        auto staged = store->backend().getStream(*source.server_side_copy_from);
        if (!staged)
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST,
                "CAS resurrect: staging object {} is absent", *source.server_side_copy_from);
        staged->stream->ignore(meta.blob_header_len);
        tok = store->backend().resurrect(*staged->stream, source.size, key, fresh_header);
        const BlobDepRecord dep = makeDepAndEmit(tok);
        writeResurrectMetaClean(lm);
        return BlobUploadResult{ref, dep, BlobUploadOutcome::ResurrectedS3};
    }

    /// CRITICAL: we re-read the writer's OWN source (NOT backend().get) — no GET of the dying object.
    /// W-FRESH-TAG: a fresh incarnation_tag minted inside buildHeader() ensures INV-NO-RETURN.
    ///
    /// UNCONDITIONAL, exactly like the staging arm above. An `If-Match` on the condemned token would
    /// save a redundant re-upload when another writer resurrects the same blob first, and would prevent
    /// nothing: two racing resurrections write payload-identical bodies, no consumer reads a dep token's
    /// VALUE, and durable references name content hashes rather than incarnations. What protects the
    /// resurrection is the fresh tag — it makes this body's ETag differ from the condemned one, so every
    /// already-queued exact-token delete of that incarnation misses.
    ///
    /// Blob bodies have no size cap, so a body larger than memory has to remain writable here: a
    /// Native backend streams the payload from the reader; the emulated (local) backend materializes
    /// one body at a time, serialized inside `Backend::resurrect`.
    auto payload = source.open();
    /// rev.7 [C2]: a raw, uncoupled backend call; fence-checked against the displacement-decision
    /// generation immediately before the durable write.
    store->checkFenceOrThrow(displace_admitted_generation);
    /// `source.size` rides into the write, which counts while streaming and aborts WITHOUT publishing
    /// on a mismatch -- with an unconditional overwrite, a post-write check would fire only after a
    /// truncated body had already displaced the condemned incarnation (and could even inspect a racing
    /// writer's fresh incarnation instead of ours).
    const Token resurrected = store->backend().resurrect(*payload, source.size, key, buildHeader());

    const BlobDepRecord dep = makeDepAndEmit(resurrected);
    writeResurrectMetaClean(lm);
    return BlobUploadResult{ref, dep, BlobUploadOutcome::ResurrectedLocal};
}

void PartWriteTxn::adoptEvidence(const ManifestEntry & entry)
{
    requireAlive();

    /// W-EVIDENCE: record a TOKENLESS dependency — liveness evidence is the live source manifest, not a
    /// token. Inline entries reference no standalone object, so they record nothing. NO backend call
    /// (no HEAD, no GET, no PUT) — the caller already holds the resolved entry. Part manifests have only
    /// Inline / Blob placements (no Subtree): only blobs are content-addressed.
    if (entry.placement == EntryPlacement::Blob)
    {
        /// Carry `entry.ref` WHOLE (the pair, never re-derived) — this is what makes a
        /// mixed-algo manifest's entries each dep-track under their OWN algo. §4: adopted=true marks this a
        /// committed-source W-EVIDENCE dep, trusted at promote via the durable manifest edge (no probe).
        deps[entry.ref] = BlobDepRecord{ObjectKind::Blob, std::nullopt, entry.blob_size, /*adopted=*/true};
    }
}

void PartWriteTxn::recordPendingBlobDep(const BlobRef & ref, uint64_t size)
{
    requireAlive();
    deps[ref] = BlobDepRecord{ObjectKind::Blob, std::nullopt, size};
}

RootNamespace PartWriteTxn::manifestNamespace() const
{
    /// The wiring sets the owning namespace EXPLICITLY (PartWriteInfo::intended_namespace). This is the
    /// authoritative source: a ref can itself contain `/` (the `detached/<part>` fold), so we
    /// must NOT recover the namespace by splitting intended_ref on the last `/` — that would yield a
    /// spurious `<ns>/detached` namespace and the precommit namespace-match check would throw.
    if (info.intended_namespace)
        return *info.intended_namespace;

    /// Fallback (Core tests that set only the diagnostic intended_ref, where the ref has no `/`): the
    /// owning namespace is intended_ref minus its last `/`-segment (the ref name). A CA namespace itself
    /// contains `/` (e.g. "srv1/<uuid>@cas@"), so split on the LAST slash only.
    const String intended = info.intended_ref.value_or("");
    const size_t slash = intended.find_last_of('/');
    if (slash == String::npos || slash == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "stageManifest: intended_ref '{}' has no namespace/ref split", intended);
    return RootNamespace{intended.substr(0, slash)};
}

ManifestId PartWriteTxn::stageManifest(std::vector<ManifestEntry> entries)
{
    requireAlive();

    /// Fail-closed caps — checked BEFORE the body write so no owner transition can ever name a
    /// manifest that breaches a cap. Inline payload is read on every part-open and every owner
    /// transition, so cap the total, not only per-entry.
    if (entries.size() > kMaxManifestEntries)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED,
            "stageManifest: {} entries exceeds cap {}", entries.size(), kMaxManifestEntries);
    uint64_t inline_total = 0;
    for (const ManifestEntry & e : entries)
    {
        if (e.placement == EntryPlacement::Inline)
        {
            if (e.inline_bytes.size() > kMaxLargestInlineEntryBytes)
                throw Exception(ErrorCodes::LIMIT_EXCEEDED,
                    "stageManifest: inline entry '{}' of {} bytes exceeds cap {}",
                    e.path, e.inline_bytes.size(), kMaxLargestInlineEntryBytes);
            inline_total += e.inline_bytes.size();
        }
    }
    if (inline_total > kMaxManifestInlineBytesTotal)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED,
            "stageManifest: total inline {} bytes exceeds cap {}", inline_total, kMaxManifestInlineBytesTotal);

    /// Mint the identity. `epoch` is the Pool's durable writer_epoch; `build_seq` is monotone inside
    /// that epoch; `manifest_ordinal` is monotone inside this PartWriteTxn. Together with the owning namespace
    /// this gives NoManifestIdReuse by construction, with no random manifest instance id.
    if (next_manifest_ordinal > kMaxManifestOrdinal)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED,
            "stageManifest: manifest ordinal cap {} exceeded for build_seq {}", kMaxManifestOrdinal, build_seq);
    const ManifestRef ref{epoch, build_seq, next_manifest_ordinal++};

    /// Build the body. payload_digest is integrity/debug only — never a key, never dedup, never
    /// in-degree. The body repeats its own ref + namespace for fail-closed RefMatchesBody /
    /// ManifestNamespaceMatches at read/fold/promote time.
    const RootNamespace owning_ns = manifestNamespace();
    PartManifest body;
    body.ref = ref;
    body.root_namespace_id = owning_ns;
    body.entries = std::move(entries);
    body.payload_digest = computePayloadDigest(body);
    /// The cap is measured over the canonical text before sealing, not the compressed PUT bytes.
    /// 256 MiB comfortably bounds the largest realistic manifest, including its JSON structure.
    const String encoded_text = encodePartManifest(body);
    if (encoded_text.size() > kMaxManifestEncodedBytes)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED,
            "stageManifest: encoded manifest {} bytes exceeds cap {}", encoded_text.size(), kMaxManifestEncodedBytes);
    const String encoded = sealObject(FormatId::PartManifest, encoded_text);

    const ManifestId id{owning_ns, ref};
    const String key = store->layout().manifestKey(id);

    /// Body PUT through the Pool's shared request controller:
    /// budgeted attempts + resolve-before-reissue, replacing the old bare single-attempt write whose
    /// whole S3-blip tolerance was ONE ~3s adaptive-timeout attempt (a 19s object-store pause killed an
    /// INSERT through it while every plain read/write path survived — v3 soak evidence). Reissuing this
    /// conditional PUT is sound: the body bytes are fixed for the whole operation (`encoded` is built
    /// once; `encodePartManifest` is canonical/deterministic), so `resolveByExactGet` can prove whether
    /// an ambiguous attempt landed. Still NO preliminary HEAD. A DIFFERENT object at this key is a
    /// ManifestId collision — the controller's resolve raises CORRUPTED_DATA (a proven conflict,
    /// fail-closed before any owner transition can name this id), subsuming the old
    /// PreconditionFailed->LOGICAL_ERROR mapping.
    ///
    /// fence_ok is the ref lane's own mount predicate (`refAppendFenceOk`: fence not lost + enough
    /// lease left for one more attempt): staging runs on this writable Pool under that same mount
    /// lease, and a fenced writer must not keep PUTting bodies ahead of a precommitAdd that would fail
    /// the same fence anyway. There is no ref-table runtime here, so the lane's extra
    /// `superseded_by_remount` term does not apply.
    Token manifest_token;
    const CasWriteOutcome put_outcome = store->stagingPutIfAbsent(key, encoded, &manifest_token);
    if (put_outcome == CasWriteOutcome::DefiniteFailure)
        throwCasWriteRetryLater(fmt::format(
            "stageManifest: part-manifest PUT at '{}' definitively failed (non-retryable rejection); "
            "nothing was named — the caller re-stages with a fresh ManifestId", key));
    /// Unresolved = budget exhausted (or fence lost) without a definite outcome. Unlike the ref-log
    /// lane there is nothing to wedge: this id was never named by any owner transition
    /// (`next_manifest_ordinal` is already past it, so no re-stage ever reuses the key), and a
    /// late-landing body is inert unreferenced debris for the orphan-manifest sweep. NETWORK_ERROR =
    /// the same retryable abort class the ref lane's exhausted budget maps to.
    if (put_outcome == CasWriteOutcome::Unresolved)
        throwCasWriteRetryLater(fmt::format(
            "stageManifest: part-manifest PUT at '{}' is UNCERTAIN (retry budget exhausted) — "
            "nothing conclusive was named; the caller re-stages with a fresh ManifestId", key));

    EventEmitter{*store}.emit([&](CasEvent & e)
    {
        e.type = CasEventType::ManifestPut;
        e.namespace_ = owning_ns.string();
        e.object_kind = CasEventObjectKind::Manifest;
        e.object_hash = manifestRefDebugString(id.ref);
        e.token = manifest_token.value;
        e.reason = "stageManifest: part-manifest body written";
    });

    staged_manifests.push_back(id);
    staged_manifest_ids.insert(id);   /// A3 mint-tightening: `precommitAdd`'s only legal fresh-ownership source
    return id;
}

void PartWriteTxn::precommitAdd(const RootNamespace & target_ns, const String & final_ref_name, const ManifestId & id)
{
    requireAlive();

    /// ManifestNamespaceMatches at the source: the precommit's manifest must belong to the target
    /// namespace (its key is built from id.root_namespace). A cross-namespace precommit is a bug.
    if (id.root_namespace != target_ns)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "precommitAdd: manifest namespace '{}' != target namespace '{}'",
            id.root_namespace.string(), target_ns.string());

    /// A3 mint-tightening (ABA barrier for the relink confirm's exact-`ManifestRef` equality): an
    /// unowned `ManifestId` may enter ownership only from the transaction that freshly staged it.
    /// Evaluated HERE (synchronously, off this transaction's own private `staged_manifest_ids` --
    /// never mutated concurrently, so no snapshot races) rather than read live inside the closure
    /// below, because it does not depend on ledger state at all. The bool is captured BY VALUE into
    /// the closure -- consistent with every other capture there -- rather than capturing `this`,
    /// which would let a closure that outlives this stack (see the capture comment below) dereference
    /// a dead transaction. Enforcement, however, is NOT done here: see the closure for why.
    const bool id_staged_by_this_txn = staged_manifest_ids.contains(id);

    /// One `owner_transition` create-precommit op.
    /// No body HEAD — a missing body is a legal fail-closed, non-activating intent, unchanged from the
    /// old protocol. Precommit ownership carries NO separate build token: `id.ref` (writer_epoch,
    /// build_sequence, manifest_ordinal) IS the build identity (the tuple
    /// §Transaction Log Format: "there is no second build token"). `build_ops` is invoked from inside
    /// the per-namespace flush, so it sees the table's CURRENT state including any earlier item of the
    /// same batch; when that state is not `Live` (never born, or `Removed`), it prepends
    /// `namespace_birth` in the SAME transaction (the birth transaction
    /// normally also adds the first precommit").
    ///
    /// THE INTENT IS RECORDED BEFORE THE APPEND, and that ordering is the whole point: an `Unresolved`
    /// append MAY HAVE LANDED (`CasRefLedger.cpp`, the `Unresolved` arm says so explicitly), so a
    /// `precommitAdd` that throws can still have made `id.ref` a live precommit owner. Setting the
    /// fields afterwards left that case with an object which believed it had never precommitted:
    /// `abandon` queued no removal and `cleanupStagedManifestDebrisBestEffort` -- deciding from the same
    /// unset state -- writer-DELETED the body, so a wedge that later resolved as committed installed a
    /// live precommit with no cleanup owner and no body. Same discipline, and same reason, as the ref
    /// lane's preconstructed wedge. The three fields are plain assignments of already-materialized
    /// values, so this cannot throw between the record and the append.
    precommit_target_ns = target_ns;
    precommit_final_ref = final_ref_name;
    precommit_manifest = id.ref;
    precommit_state = PrecommitState::Uncertain;

    store->appendRefOps(target_ns, MutationScope::ref(final_ref_name),
        /// Capture everything the closure reads BY VALUE (the `store` handle, target namespace, ref name,
        /// and manifest id) rather than `[&]`: defense-in-depth so a closure that ever outlives this
        /// stack frame (e.g. if the append lane stranded its item) never dereferences a dead stack. The
        /// ref-lane leadership guard is the real fix; this makes the closure self-contained regardless.
        [pool = store, target_ns, final_ref_name, id, id_staged_by_this_txn](const RefTableState & state) -> std::vector<RefOp>
        {
            /// Idempotent re-add: the target ref is ALREADY committed to this EXACT manifest_ref (a
            /// legitimate re-drive calling precommitAdd+promote again for content that is already
            /// live -- see `promote`'s matching no-op guard). Nothing to append: re-adding a precommit
            /// for an already-owned manifest would violate "no conflicting owner may name the same
            /// manifest", which the state machine enforces for every OTHER case.
            ///
            /// A3 mint-tightening's enforcement lives HERE, gated on this SAME live-state read, rather
            /// than unconditionally before the closure: a legitimate re-drive can be handed an id that
            /// this exact `PartWriteTxn` object never staged (e.g. a fresh build re-precommitting
            /// content a PRIOR, since-destroyed build already promoted -- `CasPromoteRepublish.
            /// PromoteSameManifestIsIdempotent`). That case is not a fresh ownership grant: the manifest
            /// is already `final_ref_name`'s live, currently-protected binding, so re-affirming it
            /// creates no new ABA surface. Checking membership-or-committed-match atomically under the
            /// SAME state read (rather than a separate pre-check against a possibly-stale resolve) is
            /// what keeps this race-free: a snapshot taken before the closure could see "already
            /// committed to id.ref" and let a later closure invocation append a fresh owner_transition
            /// after a concurrent repoint moved the ref away from id.ref in between -- exactly the
            /// re-ownership of a dropped identity this check exists to prevent.
            if (const auto it = state.getCommitted().find(final_ref_name);
                it != state.getCommitted().end() && it->second.manifest_ref == id.ref)
                return {};
            if (!id_staged_by_this_txn)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "precommitAdd: manifest '{}' was not staged by this transaction and is not the current "
                    "committed manifest of ref '{}' -- refusing to re-own a foreign or previously-dropped identity",
                    manifestRefDebugString(id.ref), final_ref_name);

            std::vector<RefOp> ops;
            if (state.getLifecycle() != RefLifecycle::Live)
            {
                /// Reaching an empty runtime here means catalog resolution admitted a new life. A
                /// predecessor still `Removing` was refused before recovery, so rebirth needs no
                /// physical marker or empty-prefix proof.
                RefOp birth;
                birth.kind = RefOpKind::NamespaceBirth;
                ops.push_back(birth);
            }
            RefOp add;
            add.kind = RefOpKind::OwnerTransition;
            add.new_binding = RefOwnerBinding{RefOwnerKind::Precommit, final_ref_name, id.ref};
            ops.push_back(add);
            return ops;
        },
        RootMutationOrigin::Writer, RootMutationKind::Precommit);

    /// The append returned: the precommit binding is durably this build's, so the duty settles from
    /// `Uncertain` to `Durable` and `observeAndAdmit`'s EDGE-BEFORE-OBSERVE gate opens.
    precommit_state = PrecommitState::Durable;

    EventEmitter{*store}.emit([&](CasEvent & e)
    {
        e.type = CasEventType::Precommit;
        e.namespace_ = target_ns.string();
        e.ref_name = final_ref_name;
        e.token = u128ToHex(build_id);
        e.outcome = "ok";
        e.reason = "precommitAdd: build-intent owner add in the target shard (owner = precommit(build_id))";
        e.detail = {{"build_seq", std::to_string(build_seq)}};
    });
}

bool PartWriteTxn::promote(const RootNamespace & target_ns, const String & final_ref_name, UInt128 promote_build_id, const ManifestId & id, bool allow_repoint)
{
    requireAlive();

    if (id.root_namespace != target_ns)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "promote: manifest namespace '{}' != target namespace '{}'",
            id.root_namespace.string(), target_ns.string());

    /// Read + validate the manifest body ONCE (O(manifest entries), one streaming read). Absent or
    /// invalid ⇒ fail closed: a committed ref must never name a missing/mismatched manifest.
    const String manifest_key = store->layout().manifestKey(id);
    const auto body_got = store->backend().get(manifest_key);
    if (!body_got)
        throwCasWriteRetryLater(fmt::format(
            "promote: manifest body absent at {} — failing closed (retry with a fresh ManifestId)", manifest_key));
    const PartManifest body = decodePartManifest(openObject(FormatId::PartManifest, body_got->bytes));
    if (!refMatchesBody(id.ref, body))
        throwCasWriteRetryLater(fmt::format("promote: RefMatchesBody failed for {}", manifest_key));
    if (!manifestNamespaceMatches(target_ns, body))
        throwCasWriteRetryLater(fmt::format("promote: ManifestNamespaceMatches failed for {}", manifest_key));

    /// The copy-forward pre-pass is removed — the
    /// in-closure blob revalidation below is now the SINGLE copy-forward site. Trade-off: the rare
    /// condemned-tokenless copy-forward (a GET+PUT) now runs inside the append lane's flush, briefly
    /// blocking the per-namespace batching queue; it is idempotent under a re-run (a retry sees its own
    /// fresh token). The meta CAS is the only remaining coordination for this rare case.

    /// The intended-repoint operation is an atomic composition of `WDropRef` and `WPromote`: the
    /// manifest the ref currently commits,
    /// when this promote is retiring it via an intended repoint (`allow_repoint`) rather than the
    /// ordinary first-time-commit path. Set inside the closure below; read after it returns to decide
    /// whether the `RefRepoint` audit event fires.
    std::optional<ManifestRef> repoint_old;
    /// `created`: whether `final_ref_name` has NO committed row as of THIS builder's read of `state`
    /// -- set as the very first statement of the closure (before any other branch), so it is correct
    /// on every path: idempotent re-promote no-op (a committed row for `id.ref` already existed ->
    /// false), an intended repoint (a committed row for a DIFFERENT manifest already existed -> false),
    /// or a genuine first-time bind (no committed row -> true). Same in-closure-output pattern as
    /// `repoint_old` immediately below; read by the caller only after `appendRefOps` returns.
    bool created = false;
    /// Recorded BEFORE the append for the same reason `precommitAdd` records its intent before its own:
    /// the append is the point past which failure stops being proof of the negative. Everything above
    /// this line is validation that rejects without publishing anything, so a throw there leaves
    /// `NotAttempted` -- and a caller may safely conclude the ref was not committed. From here on it
    /// may not.
    commit_state = CommitState::Uncertain;
    store->appendRefOps(target_ns, MutationScope::ref(final_ref_name),
        /// Capture the closure's INPUTS by value (ref name, manifest id, promote build id, repoint flag,
        /// and the manifest `body` it revalidates) rather than `[&]`, as defense-in-depth against a
        /// closure outliving this stack. Unlike `precommitAdd`, this closure cannot be made fully
        /// self-contained: `depIsTokened`/`isTrustedAdopt` read this build's `deps` member (so it must
        /// keep `this`), and `repoint_old`/`created` are OUTPUTS read after the call returns (so they stay
        /// references). The ref-lane leadership guard is the real fix; both residual by-reference captures
        /// are safe because the guard guarantees the closure is never invoked after this frame unwinds.
        [this, final_ref_name, id, promote_build_id, allow_repoint, body, &repoint_old, &created]
        (const RefTableState & state) -> std::vector<RefOp>
        {
            created = !state.getCommitted().contains(final_ref_name);

            /// Idempotent re-promote: the target ref is ALREADY committed to this EXACT manifest_ref --
            /// a legitimate re-drive (a crash/retry between a prior promote and its caller's own
            /// follow-up, or a direct repeat call) that must complete as a no-op, not require a live
            /// precommit that a first successful call already consumed. Mirrors precommitAdd's matching
            /// guard; the DIFFERENT-manifest case below remains the BUG-1a fail-closed leak guard.
            if (const auto it = state.getCommitted().find(final_ref_name);
                it != state.getCommitted().end() && it->second.manifest_ref == id.ref)
                return {};

            /// NO writer-side view refresh here:
            /// Gate A): promote-time view freshness is not load-bearing — tokened leaves are edge-protected
            /// (EDGE-BEFORE-OBSERVE) and the tokenless K3 gate below reads the live view, which the floor
            /// guarantees contains every graduated entry (in EVERY view >= condemn round + 1).

            /// The `WPromote` owner guard (`owner[m] = bld`): a promote is a PURE owner MOVE that emits
            /// NO blob delta (Δ=0) — it restores no blob in-degree. It is therefore only sound when the
            /// precommit is STILL the live owner of the ref: if an abandon or GC reclaim already appended
            /// a removal of the precommit binding, the blobs' in-degree was already decremented and a Δ=0
            /// move would re-publish a committed ref over to-be-deleted blobs ⇒ a dangling committed
            /// manifest (INV_NO_DANGLE). `RefTableState::getPrecommits` materializes live ownership directly:
            /// `id.ref` alone identifies the build (there is no second
            /// build token"), so an exact-binding lookup answers the same question directly.
            if (!state.getPrecommits().contains({final_ref_name, id.ref}))
                throwCasWriteRetryLater(fmt::format(
                    "promote: precommit owner binding for ref '{}' (build {}) was removed (abandon or GC "
                    "reclaim) and is no longer the live owner — failing closed; the build must restart "
                    "(WPromote owner==bld)",
                    final_ref_name, u128ToHex(promote_build_id)));

            /// Blob-leaf revalidation. TOKENED leaves are
            /// edge-protected — EDGE-BEFORE-OBSERVE: the precommit closure was durable BEFORE putBlob
            /// observed them, so a condemnation in the putBlob→promote window cannot graduate (the next fold
            /// sees the edge, d >= 1, spared), and putBlob's gate already validated them against the installed
            /// view under that edge. They are NOT re-checked here. A NON-tokened leaf is EITHER a
            /// committed-source W-EVIDENCE adopt (adoptEvidence ⇒ adopted=true) or a no-dep / pending-upload
            /// staging bug. There is NO per-file probe on this path: an adopted leaf is TRUSTED via the
            /// durable manifest edge (the live source pins the blob, in-degree >= 1, not condemnable) and this
            /// build's precommit edge is durable — matching the relink trust model (ordinary
            /// ReplicatedMergeTree interserver trust). A genuinely-absent adopted blob is an invariant
            /// violation caught by fsck, not here.
            for (const ManifestEntry & e : body.entries)
            {
                if (e.placement != EntryPlacement::Blob)
                    continue;
                if (depIsTokened(e.ref))
                    continue;   /// edge-protected (EDGE-BEFORE-OBSERVE); putBlob validated under the durable edge
                /// §4 manifest-trust: a tokenless adoptEvidence leaf is trusted — no HEAD, no loadMeta, no
                /// copy-forward; the durable manifest edge is the liveness evidence. EDGE-BEFORE-TRUST: this
                /// build's precommit edge was durably appended (`precommitAdd`, the `Precommit`
                /// `OwnerTransition` above) BEFORE we get here, and the owner-liveness check at the top of this
                /// closure ("WPromote owner==bld") already re-proved it is the LIVE owner — so the dst manifest
                /// is a live precommit owner input and GC's fold pins every blob it names at in-degree >= 1
                /// (the barrier-activated create-precommit +1). GC is the sole deleter and respects
                /// in-degree, so a trusted-promote leaf cannot have been condemned/deleted. The backstop for
                /// the (production-unreachable) genuinely-absent case is fsck's reachable-but-absent scan
                /// (`CasFsck.cpp`, `++report.dangling`), NOT this gate. A tokenless PENDING-upload dep
                /// (adopted=false) or a no-dep leaf never reaches promote un-resolved legitimately: it is a
                /// staging bug (a pending upload that never completed) and fails closed (LOGICAL_ERROR).
                if (!isTrustedAdopt(e.ref))
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "promote: blob leaf {} has no tokened and no adopted dep at commit — a staging bug "
                        "(a pending upload never completed); failing closed",
                        store->layout().blobKey(e.ref));
                ProfileEvents::increment(ProfileEvents::CASBlobAdoptTrusted);
                EventEmitter{*store}.emit([&](CasEvent & ev)
                {
                    ev.type = CasEventType::BlobReuseAdopt;
                    ev.object_kind = CasEventObjectKind::Blob;
                    ev.object_hash = blobIdOf(e.ref);
                    ev.outcome = "adopt";
                    ev.reason = "manifest-trust";   /// distinguishable trusted-adopt class (empty token)
                });
            }

            /// BUG 1a: refuse to overwrite a live committed ref that already names a DIFFERENT manifest —
            /// that would orphan the old manifest (its owner-removal `-1` is never emitted) UNLESS the
            /// caller opted into an intended repoint (`allow_repoint`, modeled as the `WDropRef`+`WPromote`
            /// composition) -- a standalone
            /// write/remove on an already-committed part. Without the flag
            /// this enforces the model's `RefFreeFor` guard (`WPromote` requires it) exactly as before. A
            /// re-promote of the SAME manifest_ref is idempotent and allowed regardless (the state
            /// machine's own promote precondition below would reject it as "precommit absent" anyway once
            /// idempotent republish skips this call — see `republishRef`). Fail-closed with ABORTED (not
            /// LOGICAL_ERROR): a conflicting durable state the caller handles, never a must-not-happen
            /// invariant.
            if (const auto it = state.getCommitted().find(final_ref_name);
                it != state.getCommitted().end() && !(it->second.manifest_ref == id.ref))
            {
                if (!allow_repoint)
                    throwCasWriteRetryLater(fmt::format(
                        "promote: ref '{}' already names a different committed manifest — refusing to overwrite "
                        "(unique-ref invariant; use republishRef for an intended repoint)", final_ref_name));
                repoint_old = it->second.manifest_ref;
            }

            /// Promotion is a PURE OWNER MOVE: the SAME manifest_ref T moves from
            /// precommit to committed in one atomic transaction, together with the SetPublishedAt op
            /// that stamps `published_at_ms` (the initial stamp arrives via a separate set_published_at
            /// op, in the same transaction or a later one -- here, the same one). It emits NO blob
            /// deltas; the activating `+1` came from GC's barrier-activation of the create-precommit op.
            std::vector<RefOp> ops;
            /// An intended repoint additionally retires the OLD committed
            /// binding in this SAME ref-log record -- a separate OwnerTransition (old=Committed(repoint_old),
            /// new=absent), since one RefOwnerBinding cannot carry both the retired-committed and the
            /// promoted-precommit manifests at once (CasRefLogCodec.h). Together with the transition op
            /// below, this record atomically composes two verified model shapes -- this removal is `WDropRef`'s
            /// and the transition is `WPromote`'s; `applyRefLogTxn`'s whole-record scratch apply makes the
            /// composition a sound refinement (no intra-record intermediate state is observable;
            /// corrected note). NOT `WRepoint` -- that action requires an unowned destination, while this
            /// trigger always promotes a live precommit. Ref still moves directly
            /// from the old manifest to the new one. Mirrors `publishCommittedTransition`'s old-removal
            /// shape (cas_test_helpers.h) minus the add-precommit step, which already landed earlier as
            /// this build's own `precommitAdd`.
            if (repoint_old)
            {
                RefOp old_removal;
                old_removal.kind = RefOpKind::OwnerTransition;
                old_removal.old_binding = RefOwnerBinding{RefOwnerKind::Committed, final_ref_name, *repoint_old};
                ops.push_back(old_removal);
            }
            RefOp transition;
            transition.kind = RefOpKind::OwnerTransition;
            transition.old_binding = RefOwnerBinding{RefOwnerKind::Precommit, final_ref_name, id.ref};
            transition.new_binding = RefOwnerBinding{RefOwnerKind::Committed, final_ref_name, id.ref};
            ops.push_back(transition);

            RefOp set_published_at;
            set_published_at.kind = RefOpKind::SetPublishedAt;
            set_published_at.ref_name = final_ref_name;
            set_published_at.expected_manifest_ref = id.ref;
            set_published_at.published_at_ms = nowMs();
            ops.push_back(set_published_at);
            return ops;
        },
        RootMutationOrigin::Writer, RootMutationKind::Promote);

    commit_state = CommitState::Durable;
    /// The owed terminal operation is discharged. `Settled`, not `NotAttempted`: the manifest body now
    /// belongs to a committed ref, so it stays out of `cleanupStagedManifestDebrisBestEffort`'s reach.
    precommit_state = PrecommitState::Settled;
    store->retireBuildSeq(build_seq);
    try
    {
        EventEmitter{*store}.emit([&](CasEvent & e)
        {
            e.type = CasEventType::BuildPublish;
            e.namespace_ = target_ns.string();
            e.ref_name = final_ref_name;
            e.object_hash = manifestRefDebugString(id.ref);
            e.token = u128ToHex(promote_build_id);
            e.outcome = "promoted";
            e.reason = "promote: atomic owner move precommit(build_id) -> ref(final_ref_name) after fail-closed reval";
            e.detail = {{"build_seq", std::to_string(build_seq)}};
        });
    }
    catch (...)
    {
        tryLogCurrentException(getLogger("CasPartWriteTxn"), "CAS event emission after durable promote");
    }

    /// The low-level audit event for an intended repoint. The ProfileEvent counter + LOG_WARNING are
    /// owned by `CachedPartFolderAccess::repointRef`,
    /// the user-facing primitive that calls into this promote -- adding them here too would double-fire
    /// once the caller-level instrumentation is installed. This event alone is per-call-site accurate regardless of which caller opted
    /// into `allow_repoint`.
    if (repoint_old)
    {
        try
        {
            EventEmitter{*store}.emit([&](CasEvent & e)
            {
                e.type = CasEventType::RefRepoint;
                e.namespace_ = target_ns.string();
                e.ref_name = final_ref_name;
                e.object_kind = CasEventObjectKind::Manifest;
                e.object_hash = manifestRefDebugString(id.ref);
                e.outcome = "repointed";
                e.reason = "promote(allow_repoint=true): committed ref retargeted from an old manifest to a "
                           "new one in one ref-log record -- standalone write/remove on a committed part";
                e.detail = {{"old_manifest", manifestRefDebugString(*repoint_old)}};
            });
        }
        catch (...)
        {
            tryLogCurrentException(getLogger("CasPartWriteTxn"), "CAS event emission after durable promote");
        }
    }
    return created;
}

void PartWriteTxn::abandon()
{
    if (cancelled.load(std::memory_order_acquire))
    {
        /// `dropNamespace` cancelled this build once its removal transaction became durable: that same
        /// transaction already removed EVERY precommit binding for the (now Removed) namespace, so
        /// re-appending a precommit removal here is redundant AND impossible (`owner_transition` on a
        /// non-Live namespace is rejected by the state machine -- and `requireAlive` would throw first
        /// anyway). Do ONLY the best-effort staged-debris cleanup + seq retire (both idempotent, both on
        /// this build's OWN thread); leave the precommit body, if any, for GC's
        /// delete-after-sealed-decrements exactly as the normal path does.
        alive = false;
        precommit_state = PrecommitState::Settled;
        store->retireBuildSeq(build_seq);
        cleanupStagedManifestDebrisBestEffort();
        /// Audit emission is best-effort: a throwing sink (e.g. a bad_alloc growing the system-log
        /// queue, or a Context/log-shutdown edge) must never turn the already-durable cleanup above
        /// into a reported failure. Mirrors `promote`'s post-durable emit guard.
        try
        {
            EventEmitter{*store}.emit([&](CasEvent & e)
            {
                e.type = CasEventType::BuildAbort;
                e.token = u128ToHex(build_id);
                e.outcome = "cancelled";
                e.reason = "cancelForNamespaceRemoval: owning namespace removed (dropNamespace); best-effort "
                           "deleted staged _manifests debris; precommit body (if any) left for GC";
                e.detail = {{"build_seq", std::to_string(build_seq)}, {"staged", std::to_string(staged_manifests.size())}};
            });
        }
        catch (...)
        {
            tryLogCurrentException(getLogger("CasPartWriteTxn"), "CAS event emission after durable abandon");
        }
        return;
    }

    requireAlive();

    /// BUG 2 / TLA+ `WAbandonPrecommit`: if this build made a manifest a LIVE precommit owner input
    /// (`precommitAdd` ran), abandoning it must NOT writer-delete that body. Instead append an exact
    /// precommit-removal ref-log transaction, mirroring EXACTLY the removal
    /// shape `Pool::dropRef` and the fenced-successor stale-precommit sweep use (an old_binding naming
    /// the exact precommit, no new_binding). GC then folds the `-1` blob decrements and deletes the
    /// body only after they are sealed (delete-after-sealed-decrements). Deleting a live precommit body
    /// here would strand GC's fold barrier (live precommit, missing body → clamp forever) or lose the
    /// activating `+1`. This is the correctness-bearing step of abandon, so it runs through
    /// `appendRefOps` (the reliable append lane), not best-effort. It precedes the best-effort debris
    /// deletion below so a partial cleanup can never leave the precommit binding live without its
    /// body's GC release queued.
    ///
    /// `Uncertain` is treated EXACTLY as `Durable` here, and that is the point of the state existing: a
    /// precommit that may be live owes the same removal a precommit that is live owes. The one
    /// difference is the closure's tolerance below.
    if (precommit_state == PrecommitState::Uncertain || precommit_state == PrecommitState::Durable)
    {
        /// A removal whose `old_binding` names an ABSENT precommit is rejected by the state machine
        /// (`RefTableState::applyOwnerTransition`, `RemovePrecommit`: "exact precommit binding to remove
        /// is absent") -- so the removal is NOT unconditionally idempotent, and an `Uncertain` append
        /// that in fact never landed would make every `abandon` of this build fail forever, destructor
        /// retries included. Under `Uncertain` the closure therefore reads the CURRENT table state and
        /// emits NO op when the binding is not there: same read, same flush, no race. Under `Durable`
        /// the binding is known to have been appended, so an absent one is a genuine anomaly and the
        /// strict form is kept -- a fail-closed error is the right report for it.
        const bool tolerate_absent = precommit_state == PrecommitState::Uncertain;
        store->appendRefOps(precommit_target_ns, MutationScope::ref(precommit_final_ref),
            /// By-value capture of the values the closure reads (ref name, manifest and the tolerance
            /// flag) rather than `[&]`: defense-in-depth so the closure is self-contained if it ever
            /// outlives this stack.
            [ref_name = precommit_final_ref, manifest = precommit_manifest, tolerate_absent]
            (const RefTableState & state) -> std::vector<RefOp>
            {
                if (tolerate_absent && !state.getPrecommits().contains({ref_name, manifest}))
                    return {};
                RefOp op;
                op.kind = RefOpKind::OwnerTransition;
                op.old_binding = RefOwnerBinding{RefOwnerKind::Precommit, ref_name, manifest};
                return {op};
            },
            RootMutationOrigin::Writer, RootMutationKind::Abandon);
        precommit_state = PrecommitState::Settled;

        try
        {
            EventEmitter{*store}.emit([&](CasEvent & e)
            {
                e.type = CasEventType::PrecommitRemoved;
                e.namespace_ = precommit_target_ns.string();
                e.ref_name = precommit_final_ref;
                e.object_kind = CasEventObjectKind::Root;
                e.object_hash = manifestRefDebugString(precommit_manifest);
                e.reason = "abandon: precommit binding removed";
            });
        }
        catch (...)
        {
            tryLogCurrentException(getLogger("CasPartWriteTxn"), "CAS event emission after durable abandon");
        }
    }

    /// `alive` flips to false only AFTER the correctness-bearing precommit removal above is durable: if
    /// `appendRefOps` threw, `alive` stays true and `precommit_state` keeps owing the removal, so a
    /// caller that catches the failure can retry `abandon()` on this same object. A retry after an
    /// ambiguous already-appended failure re-validates `old_binding` and errors (or, under the
    /// `Uncertain` tolerance above, no-ops) -- it never corrupts.
    alive = false;

    /// No longer in-flight: retire the seq so the per-server active-build floor (`min_active`) can advance
    /// (idempotent). This runs AFTER the precommit removal above (mirrors `PartWriteTxn::promote`, which retires
    /// after its commit) so the build stays active until its precommit binding's removal is durable:
    /// retiring first would advance `min_active` past a build whose precommit binding is still live in the
    /// ref log, letting a freshness-window consumer judge the manifest build-dead while an un-removed
    /// precommit still names it. Ordering removal-before-retire keeps that happens-before clean.
    store->retireBuildSeq(build_seq);

    cleanupStagedManifestDebrisBestEffort();

    /// The build was abandoned; its uploads become GC-reclaimable debris. Best-effort audit emission:
    /// the durable work (precommit removal + seq retire) is already done, so a throwing sink must not
    /// propagate a failure out of a successful abandon.
    try
    {
        EventEmitter{*store}.emit([&](CasEvent & e)
        {
            e.type = CasEventType::BuildAbort;
            e.token = u128ToHex(build_id);
            e.outcome = "abandoned";
            e.reason = "abandon: appended a precommit-removal event for the live precommit (body left for GC); "
                       "best-effort deleted the never-precommitted _manifests debris; remainder reaped by the orphan sweep";
            e.detail = {{"build_seq", std::to_string(build_seq)}, {"staged", std::to_string(staged_manifests.size())}};
        });
    }
    catch (...)
    {
        tryLogCurrentException(getLogger("CasPartWriteTxn"), "CAS event emission after durable abandon");
    }
}

void PartWriteTxn::cleanupStagedManifestDebrisBestEffort()
{
    /// Best-effort writer cleanup of THIS build's pre-precommit/staged `_manifests` debris. The common case
    /// is writer cleanup; a missed object is benign — the namespace-scoped orphan sweep reclaims it. Exact-token delete only; never
    /// throws. SKIP the manifest that became a live precommit owner: its body is a live precommit input
    /// whose deletion is GC's job after the sealed decrement (never writer-delete it).
    ///
    /// "Became a live precommit owner" is decided from the ATTEMPT, not from a confirmed append: any
    /// state other than `NotAttempted` -- including `Uncertain`, where the precommit may or may not
    /// exist -- protects the body. Deleting it under uncertainty is the unrecoverable direction (a
    /// precommit that turns out to be live and whose body is gone clamps GC's fold barrier forever),
    /// while keeping it is not: an unreferenced body is ordinary orphan-sweep debris.
    const bool precommit_attempted = precommit_state != PrecommitState::NotAttempted;
    for (const ManifestId & id : staged_manifests)
    {
        if (precommit_attempted && id.ref == precommit_manifest && id.root_namespace == precommit_target_ns)
            continue;   /// the (possibly) live precommit body — left for GC (delete-after-sealed-decrements)
        try
        {
            const String key = store->layout().manifestKey(id);
            const HeadResult hr = store->backend().head(key);
            if (hr.exists)
                store->backend().deleteExact(key, hr.token);
        }
        catch (...) // NOLINT(bugprone-empty-catch)
        {
            /// Best-effort cleanup: the GC backstop sweep is the durable guarantee.
        }
    }
}

void PartWriteTxn::cancelForNamespaceRemoval(const RootNamespace & removed_ns)
{
    /// Only cancel a build that actually targets the removed namespace. `manifestNamespace` reads only
    /// the immutable `info` set at construction, so this is safe to call from `dropNamespace`'s thread
    /// while the build's own thread runs. A build whose owning namespace cannot be determined (a
    /// malformed diagnostic-only PartWriteInfo) is left alone — the append lane still fails closed on any op
    /// it later attempts against the now-Removed namespace.
    std::optional<RootNamespace> mine;
    try
    {
        mine = manifestNamespace();
    }
    catch (...)
    {
        return;
    }
    if (mine != removed_ns)
        return;
    cancelled.store(true, std::memory_order_release);
}

}
