#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasBlobEnvelopeFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromFileView.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/copyData.h>
#include <Disks/IO/WriteBufferWithFinalizeCallback.h>
#include <Disks/IDiskTransaction.h>
#include <Common/thread_local_rng.h>
#include <Common/config_version.h>
#include <algorithm>
#include <filesystem>
#include <unordered_set>
#include <Common/Exception.h>
#include <Common/HashTable/Hash.h>
#include <Common/getRandomASCIIString.h>
#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadPool.h>
#include <Common/setThreadName.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasBlobUploadPool.h>
#include <base/hex.h>
#include <base/scope_guard.h>
#include <ctime>
#include <map>
#include <span>
#include <vector>

namespace ProfileEvents
{
    extern const Event CASBlobUploadFanoutBatches;
    extern const Event CASBlobUploadFanoutTasks;
}

namespace fs = std::filesystem;

namespace DB
{
namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int FILE_DOESNT_EXIST;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}
}

namespace DB::Cas
{

namespace
{

bool hasSuffix(std::string_view s, std::string_view suffix)
{
    return s.size() >= suffix.size() && s.substr(s.size() - suffix.size()) == suffix;
}

}

bool partFileMustStayBlob(std::string_view file_name)
{
    if (file_name == "primary.idx")
        return true;
    for (std::string_view suffix : {".bin", ".mrk", ".mrk2", ".mrk3", ".cmrk", ".cmrk2", ".cmrk3"})
        if (hasSuffix(file_name, suffix))
            return true;
    return false;
}

}

namespace DB
{

namespace
{

[[noreturn]] void notYet(const char * op)
{
    /// These operations are part of the generic disk-transaction interface but have no
    /// content-addressed equivalent or are not wired for this storage yet. Keep the message
    /// self-explanatory because it is visible to operators.
    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
        "The operation '{}' is not implemented for a content-addressed disk: it belongs to the "
        "generic disk-transaction surface that the content-addressed write path does not use. "
        "Hitting it usually means the disk is wrapped by a layer that bypasses the "
        "content-addressed write path.", op);
}

/// Inline candidates above this size spill to a blob instead of riding the tree object — a tuning
/// knob (could become a disk setting later). Keeps the tree object bounded against an unexpectedly
/// large eager file.
constexpr size_t INLINE_CAP = 1024 * 1024;   /// 1 MiB

}

ContentAddressedTransaction::ContentAddressedTransaction(ContentAddressedMetadataStorage & metadata_storage_)
    : metadata_storage(metadata_storage_)
{
}

ContentAddressedTransaction::~ContentAddressedTransaction()
{
    /// Always clean up pending staging (whether committed or not). On the success path
    /// cleanupPendingTempFiles was already called at the end of commit(); this call is the defensive
    /// backstop for aborted/exception-unwound transactions whose publishStaging never ran.
    cleanupPendingTempFiles();

    /// An uncommitted transaction's uploads become min_active-spared debris: abandon every
    /// still-open PartWriteTxn so its build_seq is retired. This replaces the former pin machinery.
    if (committed)
        return;

    /// No refs are published before `commit`; moving a part from a temporary to a final path is only
    /// a re-key in this overlay. An abandoned transaction therefore has no early-published ref to
    /// compensate for; it only needs to abandon still-open builds below.
    for (auto & [key, st] : parts)
    {
        if (!st.build)
            continue;
        try
        {
            st.build->abandon();
        }
        catch (...)
        {
            /// A destructor must not throw. But a failed abandon can leave a LIVE-epoch precommit
            /// binding that neither GC nor the (prior-epoch-scoped) stale-precommit sweep reclaims
            /// until this mount remounts -- that must be diagnosable, not silently swallowed.
            tryLogCurrentException(getLogger("ContentAddressedTransaction"),
                                   "abandoning a build during transaction destruction "
                                   "(a live precommit binding may persist until remount)");
        }
    }
}

ContentAddressedTransaction::PartStaging &
ContentAddressedTransaction::stagingFor(const ContentAddressedMetadataStorage::Route & r)
{
    return parts[{r.ns.string(), r.ref}];
}

Cas::PartWriteTxn & ContentAddressedTransaction::buildFor(
    const ContentAddressedMetadataStorage::Route & r, PartStaging & st)
{
    if (!st.build)
        st.build = metadata_storage.store()->beginPartWrite(
            Cas::PartWriteInfo{.intended_ref = r.ns.string() + "/" + r.ref,
                           .intended_namespace = r.ns, .op = Cas::ProvenanceOp::Insert});
    return *st.build;
}

ContentAddressedTransaction::PartStaging * ContentAddressedTransaction::findStaging(
    const ContentAddressedMetadataStorage::Route & r)
{
    auto it = parts.find({r.ns.string(), r.ref});
    return it == parts.end() ? nullptr : &it->second;
}

void ContentAddressedTransaction::cleanupPendingTempFiles() noexcept
{
    for (auto & [key, st] : parts)
    {
        for (const auto & pb : st.pending_blobs)
        {
            if (pb.backend == Cas::StagingBackend::Local)
            {
                std::error_code ec;
                std::filesystem::remove(pb.staging_key, ec);
            }
            else if (committed)
            {
                /// A successful commit deletes the S3 staging object
                /// HERE — `committed` is only ever true when EVERY part's `publishStaging` ran to
                /// completion (commit() sets it right before this call), which means every referenced
                /// pending blob was already promoted (`PartWriteTxn::putBlob` → `promoteStaged`/`resurrect`)
                /// or, for an orphaned pending blob (its entry removed by `unlinkFile`/`replaceFile`
                /// before commit), was never going to be promoted at all — either way the staging object
                /// is no longer needed as a resurrect source, so it is safe to reclaim now.
                ///
                /// An ABORTED/exception-unwound transaction (`committed == false`, including a partial
                /// multi-part commit failure where an EARLIER part's blobs were already promoted) leaves
                /// its S3 staging objects in place — `staging_key` is a remote object-storage key, never a
                /// bare `fs::remove` target, and is reclaimed by the mount-lease-scoped sweeper
                /// (`Cas::sweepOwnMountStaging`), never here. This mirrors the local path's own asymmetry:
                /// `Local` staging is a private per-transaction scratch file removed unconditionally on
                /// both commit and abort (nobody else can ever read it), whereas an `S3` staging object
                /// is the sanctioned resurrect source for the promote gate and must outlive an aborted
                /// transaction so a later attempt (or the sweeper) can still account for it.
                try
                {
                    metadata_storage.objectStorage()->removeObjectIfExists(StoredObject(pb.staging_key));
                }
                catch (...) // NOLINT(bugprone-empty-catch)
                {
                    /// Best-effort (noexcept context): a stubborn delete just leaves debris for the
                    /// mount-lease sweeper to reclaim on a later mount.
                }
            }
            /// else: an S3-mode pending blob of an ABORTED transaction — intentionally left in place
            /// (see above); the mount-lease sweeper (`Cas::sweepOwnMountStaging`) is its reclaimer.
        }
        st.pending_blobs.clear();
    }
}

const ContentAddressedTransaction::PartStaging::PendingBlob *
ContentAddressedTransaction::findPendingBlob(const PartStaging & st, const Cas::BlobRef & ref) const
{
    /// Locate a pending blob by ref. Returns nullptr when the blob has already been uploaded
    /// (post-precommit, pending_blobs is cleared) or was never staged as pending.
    for (const auto & pb : st.pending_blobs)
        if (pb.ref == ref)
            return &pb;
    return nullptr;
}

void ContentAddressedTransaction::adoptStagedBlob(
    const PartStaging::PendingBlob * pb, const Cas::ManifestEntry & entry,
    PartStaging & dst_st, Cas::PartWriteTxn & dst_build, bool copy_pending)
{
    if (pb)
    {
        /// Pending blob (not yet uploaded): record a tokenless dependency without any pool operation.
        /// If copy_pending, push a copy of the pb record into dst_st so publishStaging uploads it
        /// for the dst part too (hardlink = copy semantics). If !copy_pending, the record is already
        /// in dst_st (moved by caller) — skip the push.
        if (copy_pending)
            dst_st.pending_blobs.push_back(*pb);
        dst_build.recordPendingBlobDep(entry.ref, entry.blob_size);
    }
    else
    {
        /// Uploaded / committed: record a tokenless W-EVIDENCE dep — no pool HEAD/GET before precommit.
        /// §4 manifest-trust: the publish gate (promote) TRUSTS this committed-source adopted leaf via the
        /// durable manifest edge — it does NOT observe/resurrect it. Only tokened / pending-upload leaves
        /// are resurrected (by putBlob, before promote); a genuinely-absent adopted blob is an fsck finding.
        dst_build.adoptEvidence(entry);
    }
}

std::optional<ContentAddressedMetadataStorage::Route>
ContentAddressedTransaction::routeOf(const std::string & path) const
{
    auto p = Cas::parsePartFilePath(path);
    if (!p)
        return std::nullopt;
    return metadata_storage.route(*p);
}

void ContentAddressedTransaction::uploadPendingBlobs(PartStaging & st)
{
    /// Build the set of blob hashes actually referenced by the staged manifest. Only Blob
    /// entries represent pending content uploads — Inline are not pending blobs. A pending_blob whose
    /// hash is NOT in this set had its entry removed by unlinkFile/replaceFile and must not be uploaded
    /// (it is an orphan). Its temp file is still cleaned by cleanupPendingTempFiles at commit end.
    std::unordered_set<Cas::BlobRef, Cas::BlobRefHash> referenced_hashes;
    for (const auto & entry : st.entries)
        if (entry.placement == Cas::EntryPlacement::Blob)
            referenced_hashes.insert(entry.ref);

    /// Build one upload request per referenced pending blob. Duplicate refs (staged-hardlink copies push
    /// a copy of the record) are collapsed by `fanOutBlobUploads`' grouping, which SUBSUMES the former
    /// duplicate-membership filter here — the fan-out launches one task per unique ref and merges one
    /// dep. The upload primitive differs by staging backend exactly as before:
    ///   - `Cas::StagingBackend::Local`: `open` re-reads the local staged temp file and streams
    ///     it into a write-once `putIfAbsentStream` create; the local-staging path remains byte-for-byte
    ///     compatible with its previous behavior.
    ///   - `Cas::StagingBackend::S3`: the bytes already live in an S3 staging object (`pb.staging_key`);
    ///     `server_side_copy_from` drives a WRITE-ONCE conditional SERVER-SIDE COPY (and an unconditional
    ///     resurrect copy FROM the staging object for a condemned incarnation). No local read-back —
    ///     `open` is left unset.
    std::vector<Cas::BlobUploadRequest> requests;
    requests.reserve(st.pending_blobs.size());
    for (const auto & pb : st.pending_blobs)
    {
        if (!referenced_hashes.contains(pb.ref))
            continue;   /// The entry was removed by unlinkFile/replaceFile; skip this orphan.
        Cas::BlobSource source;
        source.size = pb.size;
        if (pb.backend == Cas::StagingBackend::S3)
        {
            source.server_side_copy_from = pb.staging_key;
        }
        else
        {
            const std::string staging_key = pb.staging_key;
            source.open = [staging_key]() -> std::unique_ptr<ReadBuffer>
            {
                return std::make_unique<ReadBufferFromFile>(staging_key);
            };
        }
        /// `declared_size` mirrors `source.size` (both are `pb.size`); the fan-out fail-closes if they
        /// ever diverge, so build them together from the one authority.
        requests.push_back(Cas::BlobUploadRequest{pb.ref, std::move(source), pb.size});
    }

    if (requests.empty())
        return;

    /// Fan out the uploads on the server-wide pool (spec §1). The fan-out enforces
    /// one-task-per-unique-ref grouping, the merge-nothing failure contract, and merges every result
    /// into `st.build`'s dep set on THIS (the owning writer) thread after the join.
    Cas::fanOutBlobUploads(*st.build, requests, Cas::blobUploadPool());
}

void ContentAddressedTransaction::publishStaging(const Cas::RootNamespace & ns, const std::string & ref, PartStaging & st,
                                                 std::optional<Cas::CommitOutcome> & out_slot)
{
    if (st.published)
        return;   /// this staging was already published earlier in this commit loop — never re-publish

    if (!st.build && st.entries.empty() && st.content_removed.empty())
    {
        /// Nothing staged for this ref this transaction -- a touched-but-empty PartStaging (e.g. the
        /// harmless residue of a removeDirectory that already superseded this staging's marks,
        /// content_removed cleared to empty). Benign no-op; `out_slot` stays `std::nullopt`.
        st.published = true;
        return;
    }

    /// For committed-ref standalone writes and removal marks, `st.entries` holds only the
    /// CHANGED/ADDED entries (see stageBlobPartFile / the inline writeFile path) — never the whole
    /// part once the ref already exists; `st.content_removed` holds paths a same-transaction
    /// unlinkFile staged for removal (§6). Carry every OTHER committed entry forward (minus any
    /// content_removed path) and republish once via the repoint path, rather than letting the
    /// PartWriteTxn path below replace the manifest with just the delta. That would either reject a
    /// genuine content change or silently drop untouched files. This handles both sub-cases the
    /// interface allows: entries staged
    /// WITH a PartWriteTxn (this transaction uploaded new content) and WITHOUT one (a former mutable
    /// per-part file that is now an ordinary tree entry, or a marks-only removal with no writes).
    if (!st.entries.empty() || !st.content_removed.empty())
    {
        if (auto view = metadata_storage.partAccess()->getView({ns, ref}, Cas::Freshness::ForceFresh))
        {
            if (st.build)
            {
                /// EDGE-BEFORE-OBSERVE is still load-
                /// bearing here: a fresh blob's hash must be durably NAMED by a live precommit's
                /// manifest body before `putBlob` makes its first backend observation. `repointRef`
                /// below promotes through its OWN internal build (`adoptEvidence`, no `putBlob`) — it
                /// protects entries whose content ALREADY exists (the carried-forward ones, and this
                /// transaction's uploads once they land), but cannot itself protect a brand-new upload
                /// made mid-repoint. So THIS build stages+precommits a SCRATCH manifest over
                /// `st.entries` (BEFORE it is merged/moved below) — exactly the same closure the normal
                /// (non-repoint) path further down establishes with `st.build->stageManifest(st.entries)`
                /// + `precommitAdd`, which already names every hash this transaction is about to upload
                /// — purely to hold that edge across the upload loop. Once `repointRef`'s own promote
                /// makes the real (merged) manifest live, this scratch precommit is abandoned; it never
                /// gets promoted. A marks-only removal never enters this sub-block (`st.build` is null).
                const Cas::ManifestId scratch_id = st.build->stageManifest(st.entries);
                st.build->precommitAdd(ns, ref, scratch_id);
                uploadPendingBlobs(st);
            }

            std::vector<Cas::ManifestEntry> merged;
            for (const auto & e : view->manifest()->entries)
                if (!st.content_removed.contains(e.path)
                    && std::none_of(st.entries.begin(), st.entries.end(),
                                     [&](const Cas::ManifestEntry & s) { return s.path == e.path; }))
                    merged.push_back(e);
            for (auto & s : st.entries)
                merged.push_back(std::move(s));

            /// Test-only fault seam (Task 3 TDD): simulate a promote-time backend failure for `ref`
            /// right before the durable repoint call. A no-op in production (nothing ever arms it).
            if (metadata_storage.shouldFailPromoteForTest({ns, ref}))
                throw Exception(ErrorCodes::ABORTED,
                    "ContentAddressedTransaction: test-injected promote failure for {}/{}", ns.string(), ref);

            /// Capture the exact `CommitOutcome` IMMEDIATELY -- into the caller-provided slot, before
            /// the scratch-build `abandon()` below (which can itself throw), and before the test-only
            /// after-promote hook (which can run arbitrary test code) -- so a later throw in either
            /// cannot lose it (Task 2/3's publish-before-any-throwable-post-commit-work ordering).
            const Cas::CommitOutcome oc = metadata_storage.partAccess()->repointRef({ns, ref}, std::move(merged), Cas::ProvenanceOp::Other);
            out_slot = oc;   /// always created=false here: this block only runs once `view` already resolved
            /// Test-only: models a concurrent writer racing in right after this transaction's own
            /// confirm (e.g. repointing `ref` again). A no-op in production.
            metadata_storage.runAfterPromoteHookForTest({ns, ref});
            if (st.build)
            {
                st.build->abandon();   /// scratch precommit's protecting job is done; the real manifest is live
                st.build.reset();      /// never re-abandon this build from the destructor
            }
            st.published = true;
            return;
        }
    }

    if (!st.build)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "ContentAddressedTransaction: staged entries or removal marks for {}/{} without a Build", ns.string(), ref);

    /// Write path (rev. 15): stage the part manifest body (mints a ManifestId), precommitAdd a
    /// build-intent owner (closure now protected by reachability), upload the pending blobs, then
    /// promote — an atomic owner move that revalidates every non-tokened blob fail-closed.
    ///
    /// ORDERING IS LOAD-BEARING (EDGE-BEFORE-OBSERVE):
    /// precommitAdd's durable closure names EVERY blob hash BEFORE putBlob makes the first backend
    /// observation. This is what lets promote skip re-validating tokened leaves (a condemnation in the
    /// putBlob→promote window cannot graduate — the next fold sees the edge). Moving putBlob before
    /// precommitAdd would adopt an incarnation with no protecting edge and
    /// trips the EDGE-BEFORE-OBSERVE fail-closed throw in PartWriteTxn::observeAndAdmit; the TLA+ order
    /// sabotage (Gate A) is the formal guard.
    const Cas::ManifestId id = st.build->stageManifest(st.entries);
    st.build->precommitAdd(ns, ref, id);
    uploadPendingBlobs(st);

    /// Test-only fault seam (Task 3 TDD): simulate a promote-time backend failure for `ref` right
    /// before the durable promote call. A no-op in production (nothing ever arms it).
    if (metadata_storage.shouldFailPromoteForTest({ns, ref}))
        throw Exception(ErrorCodes::ABORTED,
            "ContentAddressedTransaction: test-injected promote failure for {}/{}", ns.string(), ref);

    /// The exact, in-lane-derived `created` from `promoteBuild` replaces the racy pre-check this used
    /// to be (`existsRef` before promote, which a concurrent writer could invalidate in the window
    /// before the promote's own append confirms). Captured into `out_slot` IMMEDIATELY -- before the
    /// test-only after-promote hook below (which can run arbitrary test code) or `st.build.reset()`
    /// -- so a later throw there cannot lose it.
    const Cas::CommitOutcome oc = metadata_storage.partAccess()->promoteBuild(*st.build, {ns, ref}, st.build->buildId(), id);
    out_slot = oc;
    /// Test-only: models a concurrent writer racing in right after this transaction's own confirm. A
    /// no-op in production.
    metadata_storage.runAfterPromoteHookForTest({ns, ref});
    st.build.reset();   /// the build is consumed (promoted); never re-abandon it from the destructor
    st.published = true;
}

void ContentAddressedTransaction::commit(const TransactionCommitOptionsVariant &)
{
    if (failed)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "retrying a failed content-addressed transaction is not supported");

    /// Operation gate (rev.7 §1). A commit with staged parts to publish is a `Write` (throws the typed
    /// Vanished [D5] refusal on a Vanished disk); a commit with nothing to publish is the DROP/rename path -- its ref mutations
    /// already applied immediately (`removeRecursive`/`dropNamespace`/`moveDirectory`), so it is a `Remove`
    /// that no-op-succeeds on a Vanished disk, which is what lets a vanished-disk table's DROP finish.
    /// Both throw 668 while the backing is uncertain (transient / IdentityLost).
    const CasOpClass commit_class = parts.empty() ? CasOpClass::Remove : CasOpClass::Write;
    if (metadata_storage.checkOpAdmitted(commit_class) == CasOpAdmission::TruthAbsent)
    {
        /// Vanished + nothing to publish: complete as a no-op so the DROP/rename finishes. There are no
        /// staged parts to publish and no local staging to keep; run the same idempotent epilogue a
        /// normal empty commit runs.
        committed = true;
        cleanupPendingTempFiles();
        force_fresh_validated_refs.clear();
        return;
    }

    /// Publish each staged part. [TXN-ONE-PIPELINE] This is the ONLY place a ref becomes durable — the
    /// tmp->final rename is a pure overlay re-key. Commit
    /// atomicity: there is no multi-ref atomic publish, so a publish that throws after
    /// earlier parts already published would leave a PARTIAL commit — some refs durably visible while
    /// the transaction reports failure, diverging the durable pool from the disk layer's all-or-nothing
    /// expectation. Track the refs THIS commit creates and, on any exception, best-effort unpublish
    /// them before rethrowing. A partial commit is NOT a protocol violation (each publish/dropRef is
    /// individually gate-checked and journalled; the leftover uploads are GC-reclaimable debris) — this
    /// restores the wiring-layer transaction contract, not a CAS invariant.
    ///
    /// Fail-closed (CLAUDE.md): only refs that were ABSENT before we published them are rolled back. A
    /// ref that already existed is pre-existing data this commit must never destroy on its error path.
    /// Publishing over a live ref does not occur in the MergeTree write path (unique part names), but
    /// the rollback must not assume it. updateRefPublishedAt mutations (autocommit one-shots on a
    /// COMMITTED part) are individually durable by design and are deliberately NOT rolled back.
    ///
    /// Task 3: the rollback keys on the EXACT manifest each `publishStaging` call committed
    /// (`Cas::CommitOutcome`), not merely on "this part's (ns, ref) name" -- an unconditional `dropRef`
    /// would clobber a DIFFERENT writer's repoint of the same ref name that lands in the window between
    /// this part's publish and a later part's failure (see `CasCommitRollback.RepointByOtherWriterSurvivesRollback`).
    /// `part_outcomes` is snapshotted and preallocated up front (one allocation, index-addressed, no
    /// per-part growth) so `publishStaging` can write `part_outcomes[i]` with a no-throw slot write --
    /// the precondition for the precise per-part rollback below. Parts are published SERIALLY by the loop
    /// that follows; only the blob uploads within each part fan out (`fanOutBlobUploads`). The snapshot
    /// preserves `parts`' own iteration order (the map's (ns, ref) sort order); there is no dependency
    /// between parts that would require a different order. Concurrent cross-part publication is future
    /// scope and is NOT done here.
    struct IndexedPart { Cas::RootNamespace ns; std::string ref; PartStaging * st; };
    std::vector<IndexedPart> ordered;
    ordered.reserve(parts.size());
    for (auto & [key, st] : parts)
        ordered.push_back({Cas::RootNamespace{key.first}, key.second, &st});

    std::vector<std::optional<Cas::CommitOutcome>> part_outcomes;
    part_outcomes.assign(ordered.size(), std::nullopt);

    try
    {
        for (size_t i = 0; i < ordered.size(); ++i)
            publishStaging(ordered[i].ns, ordered[i].ref, *ordered[i].st, part_outcomes[i]);
    }
    catch (...)
    {
        failed = true;
        /// Compensating rollback. Best-effort: a ref we cannot unpublish becomes unreferenced debris
        /// (GC-reclaimed); never mask the original failure with a rollback failure. Only a slot whose
        /// outcome `created` is true names a ref THIS commit made durable for the first time; a
        /// repoint of an already-committed ref (created=false) is pre-existing data and is never
        /// dropped. `dropRefIfMatches` additionally guards against a concurrent repoint of the SAME ref
        /// since this call's own publish: it removes the ref only if it still names the exact
        /// `manifest_ref` this commit bound, leaving a newer binding untouched.
        for (const auto & oc : part_outcomes)
            if (oc && oc->created)
                metadata_storage.partAccess()->dropRefIfMatches({oc->ns, oc->ref}, oc->manifest_ref);
        throw;
    }
    committed = true;
    /// All pending blobs have been uploaded in publishStaging; remove their staging resources now.
    cleanupPendingTempFiles();
    /// This transaction's unlinkFile ForceFresh-proof memoization is scoped to this transaction only;
    /// clear it alongside the other per-transaction state resets above.
    force_fresh_validated_refs.clear();
}

TransactionCommitOutcomeVariant ContentAddressedTransaction::tryCommit(const TransactionCommitOptionsVariant & options)
{
    if (!std::holds_alternative<NoCommitOptions>(options))
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "ContentAddressed transaction supports only tryCommit without options");
    commit(options);
    return true;
}

ObjectStorageKey ContentAddressedTransaction::generateObjectKeyForPath(const std::string &)
{
    notYet("generateObjectKeyForPath");
}

StoredObjects ContentAddressedTransaction::getSubmittedForRemovalBlobs()
{
    /// CA never hands shared backing objects to the disk layer for removal — reclamation is the
    /// GC's. Empty unconditionally because shared content-addressed objects are never owned by the
    /// disk-layer removal list.
    return {};
}

const Cas::ManifestEntry * ContentAddressedTransaction::findStagedEntry(
    const ContentAddressedMetadataStorage::Route & r) const
{
    auto it = parts.find({r.ns.string(), r.ref});
    if (it == parts.end())
        return nullptr;
    auto eit = std::find_if(it->second.entries.begin(), it->second.entries.end(),
        [&](const Cas::ManifestEntry & e) { return e.path == r.file; });
    return eit == it->second.entries.end() ? nullptr : &*eit;
}

std::optional<StoredObjects> ContentAddressedTransaction::tryGetInFlightStorageObjects(const std::string & path) const
{
    /// Read-your-writes: a projection spill-and-merge reads back its own staged blocks before
    /// the parent part's single commit. Staged content blobs may still be pending (not yet uploaded).
    auto r = const_cast<ContentAddressedTransaction *>(this)->routeOf(path);
    if (!r || r->file.empty())
        return {};
    auto it = parts.find({r->ns.string(), r->ref});
    if (it == parts.end())
        return {};
    if (const auto * entry = findStagedEntry(*r))
    {
        if (entry->placement == Cas::EntryPlacement::Blob)
        {
            /// A pending blob has not been uploaded yet — its storage object does not exist in
            /// the pool. Return empty so the caller falls back to tryReadFileInFlight (local temp read).
            if (findPendingBlob(it->second, entry->ref))
                return {};
            const auto location = metadata_storage.store()->locate(*entry);
            return StoredObjects{StoredObject(location.key, path, location.length)};
        }
        /// An Inline entry carries its bytes in `inline_bytes`; `size()` (not `blob_size`, which is 0
        /// for an inline entry carried forward from a decoded source manifest — createHardLink) reports
        /// the real inline byte count, so an in-flight read of a carried-forward inline sidecar (e.g. a
        /// MATERIALIZE-PROJECTION projection marks file) resolves to its real size, matching the
        /// committed getStorageObjects path.
        return StoredObjects{StoredObject("", path, entry->size())};
    }
    return {};
}

std::unique_ptr<ReadBufferFromFileBase> ContentAddressedTransaction::tryReadFileInFlight(
    const std::string & path, const ReadSettings & settings, std::optional<size_t> /*read_hint*/) const
{
    auto r = const_cast<ContentAddressedTransaction *>(this)->routeOf(path);
    if (!r || r->file.empty())
        return nullptr;
    auto it = parts.find({r->ns.string(), r->ref});
    if (it == parts.end())
        return nullptr;
    if (const auto * entry = findStagedEntry(*r))
    {
        if (entry->placement == Cas::EntryPlacement::Inline)
            return std::make_unique<ReadBufferFromOwnMemoryFile>(path, entry->inline_bytes);
        if (entry->placement == Cas::EntryPlacement::Blob)
        {
            /// A pending blob has not been uploaded yet — serve reads from the staging area (the
            /// same bytes that will be promoted to the pool in publishStaging post-precommit): a local
            /// temp file for `Cas::StagingBackend::Local`, or the S3 staging object for `Cas::StagingBackend::S3`
            /// (`staging_key` is a remote object key there, never a
            /// local path, so `ReadBufferFromFile` would misinterpret it as a filesystem path).
            if (const auto * pb = findPendingBlob(it->second, entry->ref))
            {
                if (pb->backend == Cas::StagingBackend::S3)
                {
                    /// The staging object holds `[header][payload]`
                    /// (the fixed-length `blob_header_len` CABL envelope, so the promote can stay a
                    /// verbatim server-side copy). Read-your-writes must serve the PAYLOAD ONLY — wrap the
                    /// object read in a `ReadBufferFromFileView` windowed to `[header_len, header_len+size)`
                    /// so position 0 is the payload start, else the reader would see 256 bytes of header
                    /// prepended to the payload (corruption). The LOCAL staging temp file holds the payload
                    /// verbatim (no header), so its path is unchanged.
                    const uint64_t header_len = metadata_storage.store()->poolMeta().blob_header_len;
                    const uint64_t payload_end = header_len + pb->size;
                    auto impl = metadata_storage.objectStorage()->readObject(
                        StoredObject(pb->staging_key, path, payload_end), settings);
                    return std::make_unique<ReadBufferFromFileView>(
                        std::move(impl), path, header_len, payload_end);
                }
                return std::make_unique<ReadBufferFromFile>(pb->staging_key);
            }
            return metadata_storage.readBlobPayload(metadata_storage.store()->locate(*entry), path, settings);
        }
    }
    return nullptr;
}

std::optional<uint64_t> ContentAddressedTransaction::tryGetInFlightFileSize(const std::string & path) const
{
    auto r = const_cast<ContentAddressedTransaction *>(this)->routeOf(path);
    if (!r || r->file.empty())
        return {};
    auto it = parts.find({r->ns.string(), r->ref});
    if (it == parts.end())
        return {};
    if (const auto * entry = findStagedEntry(*r))
        /// `size()` (not `blob_size` directly, which is 0 for an inline entry carried forward via
        /// createHardLink from a decoded source manifest). Without this, an in-flight size query for a
        /// carried-forward inline sidecar returns 0 — the 02941 MATERIALIZE-PROJECTION "Empty marks
        /// file: 0, must be: 144" corruption on a same-session read.
        return entry->size();
    return {};
}

bool ContentAddressedTransaction::hasInFlightDirectory(const std::string & path) const
{
    /// The directory overlay is true iff at least one staged file lives under `path` for `path`'s
    /// part - what makes a carried-forward projection dir visible to loadProjections.
    auto r = const_cast<ContentAddressedTransaction *>(this)->routeOf(path);
    /// INNER directories only: the overlay exists for staged projection dirs
    /// The overlay is used by `loadProjections` during finalize. The PART DIR ITSELF answers FALSE - a
    /// dedup-rejected temporary part still holds its uncommitted transaction at destruction, and
    /// an overlay "exists" for the bare part dir sends removeIfNeeded into remove(), whose
    /// bare-disk check then logs the "part to remove doesn't exist" warning.
    if (!r || r->ref.empty() || r->file.empty())
        return false;
    auto it = parts.find({r->ns.string(), r->ref});
    if (it == parts.end())
        return false;
    const std::string prefix = r->file + "/";
    for (const auto & entry : it->second.entries)
        if (entry.path.starts_with(prefix))
            return true;
    return false;
}

std::vector<std::string> ContentAddressedTransaction::listInFlightDirectory(const std::string & path) const
{
    /// Immediate-child names staged directly under `path` (one level) - loadProjections'
    /// withPartFormatFromDisk iterates a staged projection dir to find its mark file.
    auto r = const_cast<ContentAddressedTransaction *>(this)->routeOf(path);
    std::vector<std::string> result;
    if (!r || r->ref.empty())
        return result;
    auto it = parts.find({r->ns.string(), r->ref});
    if (it == parts.end())
        return result;
    const std::string prefix = r->file.empty() ? "" : r->file + "/";
    std::set<std::string> names;
    auto add = [&](const std::string & name)
    {
        if (!name.starts_with(prefix) || name.size() <= prefix.size())
            return;
        const auto rest = name.substr(prefix.size());
        const auto slash = rest.find('/');
        names.insert(slash == std::string::npos ? rest : rest.substr(0, slash));
    };
    for (const auto & entry : it->second.entries)
        add(entry.path);
    return {names.begin(), names.end()};
}

void ContentAddressedTransaction::createMetadataFile(const std::string &, const StoredObjects &)
{
    notYet("createMetadataFile");
}

void ContentAddressedTransaction::stageBlobPartFile(
    const ContentAddressedMetadataStorage::Route & route,
    const Cas::BlobRef & ref, size_t size, const std::string & staging_key, Cas::StagingBackend backend)
{
    /// Do not upload here. Record the pending blob (uploaded post-precommit in publishStaging)
    /// and a tokenless dependency; putBlob later overwrites it with the tokened dependency.
    /// The staging bytes are kept (the transaction owns them) — a local temp file for
    /// `Cas::StagingBackend::Local`, or an S3 staging object for `Cas::StagingBackend::S3`.
    auto & st = stagingFor(route);
    st.pending_blobs.push_back({ref, staging_key, size, backend});
    buildFor(route, st).recordPendingBlobDep(ref, size);

    Cas::ManifestEntry entry;
    entry.path = route.file;
    entry.placement = Cas::EntryPlacement::Blob;
    entry.ref = ref;
    entry.blob_size = size;
    std::erase_if(st.entries, [&](const Cas::ManifestEntry & e) { return e.path == entry.path; });
    st.entries.push_back(std::move(entry));
}

std::string ContentAddressedTransaction::buildS3StagingBlobHeader(
    const ContentAddressedMetadataStorage::Route & route) const
{
    /// Mirror `PartWriteTxn::uploadFromSource`'s `buildHeader` (minus the dropped `logical_size`/`logical_hash`
    /// fields and minus `build_id`, which is not known until commit and is diagnostic-only). A FRESH
    /// `incarnation_tag` per staging object keeps the incarnation zone unique; the header is padded to
    /// the pool's fixed `blob_header_len` so the payload starts at a constant offset.
    const Cas::PoolPtr & store = metadata_storage.store();
    const Cas::PoolMeta & meta = store->poolMeta();
    const Cas::PoolConfig & cfg = store->poolConfig();

    Cas::EnvelopeHeader header;
    header.kind = Cas::ObjectKind::Blob;
    header.incarnation_tag = (static_cast<UInt128>(thread_local_rng()) << 64) | thread_local_rng();
    header.build_id = 0;   /// not known at stream time; diagnostic-only (not read by GC/read paths)
    /// ch = the real ClickHouse VERSION_INTEGER (diagnostic-only; consistent with `PartWriteTxn::buildHeader`).
    /// The v3 envelope drops hash_algo/domain_id/writer_version, so forensics ride on ch + bld.
    header.provenance = Cas::Provenance{
        /*created_at_ms*/ 0, cfg.server_id, VERSION_INTEGER, Cas::ProvenanceOp::Other};
    header.intended_ref = route.ns.string() + "/" + route.ref;
    /// The v3 codec pads to the pool's fixed header length and TRUNCATES a too-long intended_ref
    /// internally (it is diagnostic-only), so the old drop-and-retry is gone — one encode call.
    return Cas::encodeEnvelopeHeader(header, static_cast<uint32_t>(meta.blob_header_len));
}

std::unique_ptr<WriteBufferFromFileBase> ContentAddressedTransaction::tryCreateWriteBuffer(
    const std::shared_ptr<IDiskTransaction> & owner,
    const std::string & path, size_t buf_size, WriteMode mode,
    const WriteSettings & settings, bool autocommit)
{
    /// This transaction owns the write because the blob key is known only after hashing the payload.
    /// Append is serviceable (read-modify-rewrite) only for a non-part / table-level verbatim file
    /// (handled inside writeFile). A part file is a content blob or a whole-rewritten inline entry, so
    /// append on a part-file path is unsupported.
    if (mode == WriteMode::Append && Cas::isPartFilePath(path))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Disk does not support WriteMode::Append for content part files");

    /// Autocommit cannot work for a CONTENT BLOB part file (column data/marks, primary.idx): a part's
    /// blobs are always written together as one build, whose manifest + ref publish only when commit()
    /// runs. A small INLINE-eligible part file IS autocommittable (a standalone one-shot write): the write
    /// lands as an ordinary manifest entry and, if the ref is already committed, `publishStaging`'s repoint
    /// branch carries the rest of the part forward and republishes once (the transactional-INSERT
    /// creation-CSN fill-in / removal-TID rewrite / rollback path). Verbatim / table-level files (not part
    /// files) are durable on finalize regardless of `autocommit`.
    if (autocommit && Cas::isPartFilePath(path))
    {
        auto p = Cas::parsePartFilePath(path);
        if (!p || p->file.empty() || Cas::partFileMustStayBlob(p->file))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Autocommit writes are not supported for content part files on a content-addressed disk");

        auto inner = writeFile(path, buf_size, mode, settings);
        auto commit_callback = [owner](size_t) mutable { owner->commit(); };
        return std::make_unique<WriteBufferWithFinalizeCallback>(
            std::move(inner), std::move(commit_callback), path, /*create_blob_if_empty=*/true);
    }

    /// Non-autocommit (or verbatim autocommit): pin the owning disk transaction for the returned buffer's
    /// lifetime. The CA write buffers capture a bare `this` in their deferred finalize / pin-blob callbacks;
    /// MergedBlockOutputStream may finalize them LATER (another thread, or after the part storage /
    /// transaction would otherwise be torn down on async-insert / cancel / exception-unwind). Holding
    /// `owner` (which owns this ContentAddressedTransaction by shared_ptr) keeps that `this` valid until the
    /// buffer — and so this callback — is destroyed after finalize (the lifetime guarantee now
    /// expressed generically via `owner`). No cycle: the transaction does not hold the buffer.
    auto inner = writeFile(path, buf_size, mode, settings);
    auto keep_alive_callback = [owner](size_t) mutable {};
    return std::make_unique<WriteBufferWithFinalizeCallback>(
        std::move(inner), std::move(keep_alive_callback), path, /*create_blob_if_empty=*/true);
}

std::unique_ptr<WriteBufferFromFileBase> ContentAddressedTransaction::writeFile(
    const std::string & path, size_t buf_size, WriteMode mode, const WriteSettings & settings)
{
    /// Write gate (rev.7 §1): the single chokepoint every write buffer (both direct and via
    /// `tryCreateWriteBuffer`) is created through -- refuse on a Vanished (typed [D5]) or transient/
    /// IdentityLost (668) disk before staging any content or opening a verbatim buffer.
    metadata_storage.checkOpAdmitted(CasOpClass::Write);
    /// Non-part files are VERBATIM namespace files, durable on finalize (no commit involvement -
    /// the disk layer's autocommit contract for them rides exactly this). Append is serviced by
    /// read-modify-rewrite: the existing bytes are carried forward (the MVCC mutation-entry CSN
    /// append depends on this). The `carried` prefix below is read ONCE here, at buffer-open time, and
    /// frozen into the write callback; `casPutObject`'s CAS loop (invoked from the callback via
    /// `putNamespaceFile`/`putMountpointObject`) only re-reads the TOKEN on conflict, not this base
    /// content — see the single-appender invariant documented at `CasPlainObjects::casPutObject`. Safe
    /// only because the sole production appender (the mutation-entry CSN write) never has a second
    /// concurrent appender on the same key.
    if (!Cas::isPartFilePath(path))
    {
        if (auto tf = Cas::parseTableFilePath(path))
        {
            /// The LIFE is resolved once, here at buffer-open time, and captured by value below, so a
            /// finalize that runs later writes to the incarnation this open was admitted under -- never
            /// into whatever life the namespace name happens to denote when the callback fires.
            const Cas::NamespaceLifeId life
                = metadata_storage.store()->namespaceLife(metadata_storage.liveNamespace(tf->table_uuid));
            const std::string name = tf->tail;
            std::string prefix_bytes;
            if (mode == WriteMode::Append)
                if (auto existing = metadata_storage.store()->getNamespaceFile(life, name))
                    prefix_bytes = std::move(*existing);
            return std::make_unique<Cas::CaInlineWriteBuffer>(
                [this, life, name, carried = std::move(prefix_bytes)](std::string bytes)
                {
                    metadata_storage.store()->putNamespaceFile(life, name, carried + bytes);
                });
        }
        /// A loose disk file, including the startup write probe, is a plain mountpoint object.
        const std::string key = metadata_storage.serverRootId() + "/" + path;
        std::string prefix_bytes;
        if (mode == WriteMode::Append)
            if (auto existing = metadata_storage.store()->getMountpointObject(key))
                prefix_bytes = std::move(*existing);
        return std::make_unique<Cas::CaInlineWriteBuffer>(
            [this, key, carried = std::move(prefix_bytes)](std::string bytes)
            {
                metadata_storage.store()->putMountpointObject(key, carried + bytes);
            });
    }

    auto p = Cas::parsePartFilePath(path);
    auto r = p ? metadata_storage.route(*p) : std::nullopt;
    if (!r || r->file.empty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "ContentAddressedTransaction::writeFile: not a part file path: {}", path);

    /// The former
    /// mutable-per-part-file branch (uuid.txt/metadata_version.txt/txn_version.txt staging directly
    /// into a separate mutable payload) is DELETED here — these three names fall through to the
    /// ordinary content path below like any other tree file. There is no filename left to special-case:
    /// `kMutablePerPartFiles`/`isMutablePerPartFile` predicate itself is gone too — there is no
    /// filename left to special-case. During part build these files land in the initial manifest with
    /// every other staged file; a standalone write on an already-committed part repoints.

    /// A CONTENT part file that must stay a blob (per-column data/marks, primary.idx): spill + hash,
    /// then stage the blob as PENDING (precommit-first). The blob is NOT uploaded/promoted here;
    /// `publishStaging` uploads it (Local) or promotes the S3 staging object post-precommit.
    /// recordPendingBlobDep (inside stageBlobPartFile) records a tokenless dependency without any
    /// pool operation at staging time.
    if (Cas::partFileMustStayBlob(r->file))
    {
        /// S3-native staging:
        /// when this disk opted in (`staging_backend=s3`) AND the mount-time capability probe
        /// a capability probe proved the object storage enforces write-once conditional copy, stream directly
        /// to a fresh per-mount S3 staging object while hashing — no local-disk round trip. Otherwise
        /// (the OFF BY DEFAULT global constraint, or a probe fail-close) fall through to the existing,
        /// byte-for-byte-unchanged local-temp-file path below.
        /// Hash with this pool's node-local write algorithm rather than a hardcoded city hash;
        /// `PoolMeta` no longer records a single pool-wide algorithm --
        /// mixed-algo pools track `algos_used`; `writeAlgo()` is the write-mint accessor now).
        const auto hash_algo = metadata_storage.store()->writeAlgo();
        /// `hash_hex` is rendered by the streaming write buffer at `hash_algo`'s own width —
        /// parse it back at that SAME width via `Cas::codecFor(hash_algo)` (never a pool-wide
        /// `DigestCodec`, which no longer exists) into a full `BlobRef` pair.

        if (metadata_storage.stagingBackend() == Cas::StagingBackend::S3 && metadata_storage.conditionalCopySupported())
        {
            const std::string staging_key = metadata_storage.stagingKeyPrefix() + "/" + getRandomASCIIString(32) + ".tmp";
            auto object_sink = metadata_storage.objectStorage()->writeObject(StoredObject(staging_key), WriteMode::Rewrite);
            /// Build the fixed-length CABL envelope header now (before
            /// the payload is streamed) so the staging object holds `[header][payload]` and the promote
            /// stays a verbatim server-side copy. The header carries a FRESH `incarnation_tag`; `build_id`
            /// is left 0 (not known at stream time — diagnostic-only, not read by GC/read paths). The
            /// buffer writes this header first, UNHASHED and excluded from the reported size, so the
            /// content key stays the pool's hash of `payload` and `blob_size` stays the payload size.
            std::string envelope_header = buildS3StagingBlobHeader(*r);
            /// rev.7 [C2]: capture the fence generation now, re-checked immediately before the durable
            /// `sink->finalize()` in `finalizeImpl` (the streaming upload becomes durable there).
            const Cas::PoolPtr pool = metadata_storage.store();
            const uint64_t admitted_generation = pool->fenceGeneration();
            return std::make_unique<Cas::CaContentWriteBuffer>(
                std::move(object_sink),
                staging_key,
                std::move(envelope_header),
                hash_algo,
                buf_size,
                settings.use_adaptive_write_buffer,
                settings.adaptive_write_buffer_initial_size,
                [this, route = *r, hash_algo](const std::string & hash_hex, size_t size, const std::string & key)
                {
                    const Cas::BlobRef ref{hash_algo, Cas::codecFor(hash_algo).fromHex(hash_hex)};
                    stageBlobPartFile(route, ref, size, key, Cas::StagingBackend::S3);
                },
                [pool, admitted_generation] { pool->checkFenceOrThrow(admitted_generation); });
        }

        return std::make_unique<Cas::CaContentWriteBuffer>(
            metadata_storage.scratchPath(),
            hash_algo,
            buf_size,
            settings.use_adaptive_write_buffer,
            settings.adaptive_write_buffer_initial_size,
            [this, route = *r, hash_algo](const std::string & hash_hex, size_t size, const std::string & temp_path)
            {
                const Cas::BlobRef ref{hash_algo, Cas::codecFor(hash_algo).fromHex(hash_hex)};
                stageBlobPartFile(route, ref, size, temp_path, Cas::StagingBackend::Local);
            });
    }

    /// Inline candidate (small eager metadata): buffer in memory, decide at finalize. <= INLINE_CAP
    /// rides the single tree object as an Inline entry (one-GET part open); an oversized
    /// candidate spills to a blob (the safety net).
    return std::make_unique<Cas::CaInlineWriteBuffer>(
        [this, route = *r](std::string bytes)
        {
            /// Mint via the one write hash function, `Cas::poolContentHash` (algorithm, payload) -> BlobRef
            /// (`CasPartWriteTxn.h`) -- the SAME mint the streaming blob path's callers use, so an inline file
            /// and a standalone blob of identical content get the same ref (same content hash identity)
            /// under EVERY algo, including sha256.
            const auto hash_algo = metadata_storage.store()->writeAlgo();
            const Cas::BlobRef ref = Cas::poolContentHash(hash_algo, bytes);
            if (bytes.size() <= INLINE_CAP)
            {
                auto & st = stagingFor(route);
                /// An inline (no-blob) entry still requires a PartWriteTxn. `publishStaging` stages the
                /// manifest body, precommits, and promotes the ref even for a part with NO blob uploads;
                /// it asserts `st.build != nullptr` whenever `st.entries` is non-empty. Without this, a
                /// part whose files are ALL inline (a tiny/empty merge output, every file <= INLINE_CAP)
                /// reaches `publishStaging` with entries but no PartWriteTxn -> LOGICAL_ERROR "staged entries
                /// without a PartWriteTxn" -> a logical-error exception under abort_on_logical_error
                /// since the inline-files feature). The blob path already establishes the PartWriteTxn via
                /// `buildFor`; the inline path must do the same.
                buildFor(route, st);
                Cas::ManifestEntry entry;
                entry.path = route.file;
                entry.placement = Cas::EntryPlacement::Inline;
                entry.ref = ref;   /// content hash identity (same for inline and blob of same content)
                /// `blob_size` stays 0 (its default) for an Inline entry — matching decode, which never
                /// fills it for Inline. `entry.size()` is the logical size, derived from `inline_bytes`.
                entry.inline_bytes = std::move(bytes);
                std::erase_if(st.entries, [&](const Cas::ManifestEntry & e) { return e.path == entry.path; });
                st.entries.push_back(std::move(entry));
            }
            else
            {
                /// Safety fallback: an unexpectedly large candidate spills to a blob (preserves the
                /// invariant that big files are not held inline). Write the buffered bytes to a unique
                /// local temp file (same scratchPath + random-name scheme as CaContentWriteBuffer), then
                /// stage exactly like a streaming blob.
                std::filesystem::create_directories(metadata_storage.scratchPath());
                const std::string temp_path =
                    metadata_storage.scratchPath() + "/inline_overflow_" + getRandomASCIIString(32) + ".tmp";
                {
                    WriteBufferFromFile tmp(temp_path);
                    tmp.write(bytes.data(), bytes.size());
                    tmp.finalize();
                }
                /// Until stageBlobPartFile takes ownership, WE own the temp file — mirror the blob path,
                /// where CaContentWriteBuffer's dtor removes it unless the callback succeeded. If
                /// stageBlobPartFile throws, drop the orphan instead of leaking it into scratch. This
                /// fallback is always `Cas::StagingBackend::Local` — an oversized inline candidate is rare
                /// enough (a safety net, not a hot path) that S3-staging mode does not cover it.
                bool staged = false;
                SCOPE_EXIT({ if (!staged) { std::error_code ec; std::filesystem::remove(temp_path, ec); } });
                stageBlobPartFile(route, ref, bytes.size(), temp_path, Cas::StagingBackend::Local);
                staged = true;
            }
        });
}

void ContentAddressedTransaction::createDirectory(const std::string &)
{
    /// Object storage has no real directories (mirrors the plain-rewritable transaction) -- but this is a
    /// Write: the gate makes it throw typed on a Vanished disk / 668 while uncertain, so a mutation is
    /// never silently accepted against an erased or unreachable backing (rev.7 §1 previously-no-op site).
    metadata_storage.checkOpAdmitted(CasOpClass::Write);
}

void ContentAddressedTransaction::createDirectoryRecursive(const std::string &)
{
    metadata_storage.checkOpAdmitted(CasOpClass::Write);
}

void ContentAddressedTransaction::removeDirectory(const std::string & path)
{
    /// CONTRACT: `removeDirectory`/`moveDirectory` mutate durable refs at CALL TIME, not at commit —
    /// this is the everything-immediate model, not a missed "defer to commit" opportunity. `renameParts`
    /// is the actual commit point; anything that goes wrong after one of these calls is undone by a
    /// COMPENSATING operation over already-committed state, the same way upstream MergeTree's own
    /// `rollbackPartsToTemporaryState` and outdated-part cleanup run over committed disk state rather
    /// than an in-memory intent log. Recording these as staged intents and applying them at commit would
    /// duplicate that compensation machinery for no correctness gain.
    ///
    /// Remove gate (rev.7 §1): a Vanished disk answers no-op success (nothing to remove -- truth), so the
    /// enclosing DROP completes; a transient / IdentityLost disk throws 668 (the DROP re-queues).
    if (metadata_storage.checkOpAdmitted(CasOpClass::Remove) == CasOpAdmission::TruthAbsent)
        return;

    /// The MergeTree fast-removal path unlinks a part's files one by one (no-ops here) and then
    /// calls removeDirectory(<part>) - the SINGLE authoritative point at which the part's ref must
    /// be unlinked. Part dirs route to dropRef; anything else is a no-op (object
    /// storage has no real directories; tables/detached/shadow are removed via removeRecursive).
    if (auto r = routeOf(path); r && !r->ref.empty() && r->file.empty())
    {
        metadata_storage.partAccess()->dropRefIfPresent(r->refKey());
        /// This transaction's staged removal marks for the same ref (content_removed, populated by
        /// unlinkFile's per-file unlinks that the MergeTree fast-removal path issues right before this
        /// call) are superseded by the whole-part ref-drop just performed above — discard them so
        /// publishStaging's committed-ref repoint branch never chases an already-dropped ref, and the
        /// dominant removal path pays zero repoints (one ref-drop only).
        if (auto * st = findStaging(*r))
        {
            st->content_removed.clear();
            st->entries.clear();
            if (st->build)
            {
                st->build->abandon();
                st->build.reset();
            }
        }
        return;
    }
}

void ContentAddressedTransaction::removeRecursive(const std::string & path, const ShouldRemoveObjectsPredicate & /*should_remove_objects*/)
{
    /// Removal = pointer-unlink + deferred GC: only refs and verbatim files go; the shared
    /// blobs/trees are reclaimed by Cas::Gc once unreachable. The predicate gates backing-object
    /// deletion, which CA always defers, so it is intentionally ignored here.

    /// Remove gate (rev.7 §1): a Vanished disk answers no-op success (nothing to remove), so a
    /// vanished-disk table's DROP -- which reaches here via `removeSharedRecursive` -- completes; a
    /// transient / IdentityLost disk throws 668 (the DROP re-queues, drains after recovery/FORGET).
    if (metadata_storage.checkOpAdmitted(CasOpClass::Remove) == CasOpAdmission::TruthAbsent)
        return;

    /// FREEZE shadow shapes first (a shadow table dir also satisfies parseTableUuid).
    if (Cas::isShadowPath(path))
    {
        if (auto p = Cas::parsePartFilePath(path); p && !p->backup_name.empty() && p->file.empty())
        {
            const auto ns = metadata_storage.shadowNamespace(p->shadow_table_dir);
            metadata_storage.partAccess()->dropRefIfPresent({ns, p->part_name});
            return;
        }
        if (Cas::endsWithTableUuidPair(path))
        {
            metadata_storage.partAccess()->dropNamespace(metadata_storage.shadowNamespace(path));
            return;
        }
        /// Backup root / intermediate dir (SYSTEM UNFREEZE WITH NAME): drop every shadow
        /// namespace under it. Canonicalize because callers hand trailing-slash dirs.
        std::string prefix = path;
        while (!prefix.empty() && prefix.back() == '/')
            prefix.pop_back();
        /// RECORD AND CONTINUE, and the reason is the shape of the alternative. Refusing the DROP would
        /// make the obstacle permanent, blocked by the one operation that could have cleared it. The GC
        /// round does not clear it either. For a ref-family key that is a test's claim rather than this
        /// comment's: `CasRefGc.UnIncarnatedRefKeyAbortsRefFoldingWithoutWedgingTheRound` runs a round
        /// over such a key and holds that the round deletes nothing and the key survives it. For a
        /// `_files`-family key the round never gets the chance: `Cas::Gc`'s fold only LISTs
        /// `casRefsPrefix()`, never `rootsPrefix()`, so such a key sits outside anything a round scans.
        /// Continuing drops every namespace the enumeration DID name; the offending key stays as
        /// reported debris. It cannot hide a namespace that has any well-formed key of its own, because
        /// attribution is per key.
        const Cas::NamespaceListing listing
            = metadata_storage.store()->listNamespaces(metadata_storage.shadowScope(prefix));
        for (const Cas::UnattributableNamespaceKey & bad : listing.skipped)
            LOG_ERROR(getLogger("ContentAddressedTransaction"),
                "removeRecursive('{}'): key '{}' names no namespace life and was left in place ({}). "
                "Every namespace this enumeration did name is still dropped; run `cas-fsck` to enumerate "
                "such keys.", path, bad.key, bad.reason);
        for (const auto & ns : listing.namespaces)
            metadata_storage.partAccess()->dropNamespace(Cas::RootNamespace{ns});
        return;
    }

    /// Table dir: the table's namespace (live + folded-in detached refs) and every verbatim
    /// file go in one dropNamespace.
    if (auto uuid = Cas::parseTableUuid(path))
    {
        metadata_storage.partAccess()->dropNamespace(metadata_storage.liveNamespace(*uuid));
        return;
    }

    if (auto p = Cas::parsePartFilePath(path))
    {
        auto r = metadata_storage.route(*p);
        /// The detached CONTAINER dir (DROP DETACHED / table-detach): drop all detached refs.
        if (r && r->ref.empty() && p->part_name == Cas::kDetachedDirName)
        {
            for (const auto & ref : metadata_storage.detachedRefNames(r->ns))
                metadata_storage.partAccess()->dropRefIfPresent({r->ns, ref});
            return;
        }
        /// The moving CONTAINER dir (MOVE-to-CA fix, mirrors detached): the mover's crash-cleanup
        /// (MergeTreeData.cpp, MOVING_DIR_NAME) calls this at table load to reclaim every staging
        /// ref an interrupted move left behind.
        if (r && r->ref.empty() && p->part_name == Cas::kMovingDirName)
        {
            for (const auto & ref : metadata_storage.movingRefNames(r->ns))
                metadata_storage.partAccess()->dropRefIfPresent({r->ns, ref});
            return;
        }
        /// A single part dir (live or detached): drop its ref.
        if (r && !r->ref.empty() && r->file.empty())
        {
            metadata_storage.partAccess()->dropRefIfPresent(r->refKey());
            return;
        }
        /// A projection subdir: virtual (nested in the parent tree) - removal is a no-op; the
        /// blobs go when the part's ref does.
        if (r && !r->ref.empty())
            return;
    }

    /// Table-level SUBDIRECTORY (deduplication_logs/): remove every verbatim file under it.
    if (auto tf = Cas::parseTableFilePath(path))
    {
        /// The READABLE resolution, which is the non-creating one (`namespaceFilesLifeIfReadable` answers
        /// an uncataloged namespace from a catalog-only lookup and writes nothing): a removal must never
        /// birth the namespace it is removing from. No life means there is nothing here to remove.
        const auto life = metadata_storage.readableNamespaceFilesLife(
            metadata_storage.liveNamespace(tf->table_uuid));
        if (!life)
            return;
        const std::string prefix = tf->tail + "/";
        for (const auto & name : metadata_storage.store()->listNamespaceFiles(*life))
            if (name.starts_with(prefix))
                metadata_storage.store()->removeNamespaceFile(*life, name);
        return;
    }
}

void ContentAddressedTransaction::createHardLink(const std::string & path_from, const std::string & path_to)
{
    /// Write gate (rev.7 §1).
    metadata_storage.checkOpAdmitted(CasOpClass::Write);
    auto src = routeOf(path_from);
    auto dst = routeOf(path_to);
    if (!src || src->file.empty() || !dst || dst->file.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "ContentAddressed: createHardLink requires two part-file paths: {} -> {}", path_from, path_to);

    auto & dst_st = stagingFor(*dst);

    /// Content file. Prefer an entry staged earlier in THIS transaction (the destination PartWriteTxn
    /// re-observes the blob via cold reuse — its own dependency); else carry forward from the
    /// COMMITTED source part (adoptFromTree: tokenless evidence pinned by the witnessed live
    /// source tree, W-EVIDENCE).
    Cas::ManifestEntry entry;
    if (auto * src_st = findStaging(*src))
    {
        auto it = std::find_if(src_st->entries.begin(), src_st->entries.end(),
            [&](const Cas::ManifestEntry & e) { return e.path == src->file; });
        if (it != src_st->entries.end())
        {
            entry = *it;
            if (entry.placement == Cas::EntryPlacement::Blob)
            {
                /// Unified adopt dispatch. copy_pending=(&dst_st != src_st) so the pending
                /// blob record is copied into dst_st only when the destination is a different part
                /// (hardlink = copy semantics; same-part is a self-ref that shouldn't duplicate the record).
                const auto * pb = findPendingBlob(*src_st, entry.ref);
                adoptStagedBlob(pb, entry, dst_st, buildFor(*dst, dst_st), /*copy_pending=*/(&dst_st != src_st));
            }
            else if (entry.placement != Cas::EntryPlacement::Inline)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "ContentAddressed: staged hardlink of unsupported placement for {}", path_from);
            entry.path = dst->file;
            std::erase_if(dst_st.entries, [&](const Cas::ManifestEntry & e) { return e.path == entry.path; });
            dst_st.entries.push_back(std::move(entry));
            return;
        }
    }

    /// Carry forward from the COMMITTED source part: read the source manifest, find the named entry,
    /// record a TOKENLESS W-EVIDENCE dep for its blob (no HEAD before precommit; promote re-proves it).
    /// ForceFresh getView == resolveRef(allow_stale=false) + readManifestShared, so this is the same
    /// request pattern as before, now instrumented via the facade.
    auto view = metadata_storage.partAccess()->getView(src->refKey(), Cas::Freshness::ForceFresh);
    if (!view)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST,
            "ContentAddressed: createHardLink source part missing: {}", path_from);
    const auto * src_entry = view->findFile(src->file);
    if (!src_entry)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST,
            "ContentAddressed: createHardLink source file missing in manifest: {}", path_from);
    buildFor(*dst, dst_st).adoptEvidence(*src_entry);
    entry = *src_entry;
    entry.path = dst->file;
    std::erase_if(dst_st.entries, [&](const Cas::ManifestEntry & e) { return e.path == entry.path; });
    dst_st.entries.push_back(std::move(entry));
}

void ContentAddressedTransaction::setLastModified(const std::string &, const Poco::Timestamp &)
{
    /// Timestamps are derived for content addressing (the publish stamp), so accept and ignore them -- but
    /// gate as a Write (previously-no-op site, rev.7 §1): never silently accept it on a Vanished/uncertain
    /// disk.
    metadata_storage.checkOpAdmitted(CasOpClass::Write);
}

void ContentAddressedTransaction::chmod(const String &, mode_t)
{
    notYet("chmod");
}

void ContentAddressedTransaction::setReadOnly(const std::string &)
{
    /// Read-only flags have no content-addressed representation — accept and ignore them -- but gate as a
    /// Write (previously-no-op site, rev.7 §1).
    metadata_storage.checkOpAdmitted(CasOpClass::Write);
}

void ContentAddressedTransaction::moveDirectory(const std::string & path_from, const std::string & path_to)
{
    /// Write gate (rev.7 §1): mutates durable refs immediately -- throw before touching them on a
    /// Vanished/uncertain disk.
    metadata_storage.checkOpAdmitted(CasOpClass::Write);
    /// Same call-time-durability-plus-compensation contract as `removeDirectory` above: this mutates
    /// durable refs immediately rather than staging an intent for commit; see the contract note there.
    auto src_p = Cas::parsePartFilePath(path_from);
    auto dst_p = Cas::parsePartFilePath(path_to);
    auto src = src_p ? metadata_storage.route(*src_p) : std::nullopt;
    auto dst = dst_p ? metadata_storage.route(*dst_p) : std::nullopt;

    /// RENAME TABLE / cross-engine move: both endpoints are TABLE dirs. Republish every ref (live
    /// and folded-in `detached/`-prefixed refs) plus every verbatim file under the new table
    /// identity, then drop the old namespace (the blobs/trees are content-addressed and untouched).
    ///
    /// There is no native cross-namespace atomicity (object storage has no directory rename, unlike a
    /// non-CAS disk where RENAME TABLE is a single atomic directory rename). This is a best-effort
    /// multi-op move, but it is RE-DRIVABLE/IDEMPOTENT: `republishRef` no-ops when the source ref is
    /// already gone (resolveRef miss after a prior drive moved it), `putNamespaceFile` is
    /// last-writer-wins (re-putting identical bytes is a no-op), and `dropNamespace` of an
    /// already-empty/absent namespace is a no-op. So a mid-loop throw leaves the table SPLIT across the
    /// two namespaces, but re-driving the SAME rename completes it. There is no in-call compensation;
    /// true atomicity would need a durable move-journal (deliberately out of scope — it would touch the
    /// tested GC/journal layer). On partial failure we log loudly so the split state is diagnosable.
    if (auto src_table = Cas::parseTableUuid(path_from))
    {
        if (auto dst_table = Cas::parseTableUuid(path_to))
        {
            const auto from_ns = metadata_storage.liveNamespace(*src_table);
            const auto to_ns = metadata_storage.liveNamespace(*dst_table);
            try
            {
                for (const auto & [ref, _] : metadata_storage.store()->listRefs(from_ns))
                    metadata_storage.partAccess()->republishRef({from_ns, ref}, {to_ns, ref});
                /// Asymmetric by necessity: the SOURCE is read, so it resolves readably and contributes
                /// nothing when it has no life; the DESTINATION is written, so it resolves the minting way
                /// and is born here if the rename is what first creates it. Resolved only once the source
                /// actually has files to move, so a rename of a table with none does not mint a life for a
                /// destination nothing is written to.
                const auto from_life = metadata_storage.readableNamespaceFilesLife(from_ns);
                const std::vector<String> file_names
                    = from_life ? metadata_storage.store()->listNamespaceFiles(*from_life) : std::vector<String>{};
                if (!file_names.empty())
                {
                    const Cas::NamespaceLifeId to_life = metadata_storage.store()->namespaceLife(to_ns);
                    for (const auto & name : file_names)
                        if (auto bytes = metadata_storage.store()->getNamespaceFile(*from_life, name))
                            metadata_storage.store()->putNamespaceFile(to_life, name, *bytes);
                }
                metadata_storage.partAccess()->dropNamespace(from_ns);
            }
            catch (...)
            {
                LOG_ERROR(getLogger("ContentAddressedTransaction"),
                    "RENAME TABLE move was only partially applied: the table is SPLIT across namespaces "
                    "'{}' and '{}'. The move is idempotent — retrying the same RENAME re-drives it to "
                    "completion (already-moved refs/files are no-ops). Underlying error: {}",
                    from_ns.string(), to_ns.string(), getCurrentExceptionMessage(/*with_stacktrace=*/false));
                throw;
            }
            return;
        }
    }

    if (!src || !dst)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "ContentAddressed: moveDirectory cannot classify {} -> {}", path_from, path_to);

    /// Projection MATERIALIZE/merge renames the staged <proj>_<n>.tmp_proj subdir to <proj>.proj
    /// inside the SAME staged part: re-key the staged entry-name prefixes so the published tree
    /// carries the final keys.
    if (!src->file.empty() && !dst->file.empty()
        && src->ns.string() == dst->ns.string() && src->ref == dst->ref
        && src->file.ends_with(".tmp_proj") && dst->file.ends_with(".proj"))
    {
        if (auto * st = findStaging(*src))
        {
            const std::string old_prefix = src->file + "/";
            const std::string new_prefix = dst->file + "/";
            for (auto & entry : st->entries)
                if (entry.path.starts_with(old_prefix))
                    entry.path = new_prefix + entry.path.substr(old_prefix.size());
            return;
        }
    }

    /// Every remaining shape is a PART-DIR move: (ns, ref) -> (ns', ref') with empty files. This
    /// uniformly covers tmp->final (staged), committed renames (delete_tmp_, merge results),
    /// DETACH (live -> detached ns), detached renames (attaching_/deleting_), and ATTACH
    /// (detached -> live ns) - in the new layout they are all the same two moves: re-key any
    /// staging, then move any committed ref.
    if (!src->ref.empty() && src->file.empty() && !dst->ref.empty() && dst->file.empty())
    {
        const std::pair<std::string, std::string> src_key{src->ns.string(), src->ref};
        const std::pair<std::string, std::string> dst_key{dst->ns.string(), dst->ref};
        if (src_key == dst_key)
            return;

        /// Re-key a STAGED source into the destination. A move carries the
        /// SOURCE's content to the destination — the POSIX `rename` semantic the rest of MergeTree
        /// assumes, and exactly what `moveFile` does (`dst[file] = src_bytes`). On the happy path the
        /// destination staging is freshly-created/empty so there is no collision at all; this only
        /// matters if some future op-order stages the same mutable file under BOTH keys.
        bool had_staged_source = false;
        if (auto src_it = parts.find(src_key); src_it != parts.end())
        {
            had_staged_source = true;
            PartStaging & dst_st = parts[dst_key];
            PartStaging & src_st = src_it->second;
            for (auto & entry : src_st.entries)
            {
                /// A genuine collision (both src and dst independently
                /// staged DIFFERING bytes for the SAME path) is a fail-loud LOGICAL_ERROR rather than a
                /// silent lost-update — the same defensive rule this loop used to apply only to the
                /// three legacy mutable names now applies uniformly to every entry (that scoping was
                /// itself a leftover of the mutable-file/entry split; there is only one kind of staged
                /// file left). Identical bytes are a benign idempotent re-key; distinct paths are the
                /// ordinary source-wins merge (a genuine collision is not expected in normal operation
                /// — only some future op-order re-keying the same file under both stagings).
                if (const auto existing = std::find_if(dst_st.entries.begin(), dst_st.entries.end(),
                        [&](const Cas::ManifestEntry & e) { return e.path == entry.path; });
                    existing != dst_st.entries.end() && !(*existing == entry))
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "ContentAddressed: moveDirectory file collision on '{}' ({} -> {}): "
                        "source and destination staged different bytes for the same file",
                        entry.path, src->ref, dst->ref);
                std::erase_if(dst_st.entries, [&](const Cas::ManifestEntry & e) { return e.path == entry.path; });
                dst_st.entries.push_back(std::move(entry));
            }
            /// Carry any staged removal marks forward too — a re-key of the
            /// staging key must not silently drop them.
            for (const auto & file : src_st.content_removed)
                dst_st.content_removed.insert(file);
            /// Move pending blobs from src to dst — they will be uploaded in dst's publishStaging.
            for (auto & pb : src_st.pending_blobs)
                dst_st.pending_blobs.push_back(std::move(pb));
            src_st.pending_blobs.clear();
            if (!dst_st.build)
            {
                dst_st.build = std::move(src_st.build);
            }
            else if (src_st.build)
            {
                /// Two Builds for one destination part: keep the destination's; the source build's
                /// deps ride the staged entries (re-observed by the destination build at adopt
                /// time is unnecessary - entries staged via putBlob/adopt carry deps in the SOURCE
                /// build... merge conservatively by abandoning nothing and re-observing):
                for (const auto & entry : dst_st.entries)
                    if (entry.placement == Cas::EntryPlacement::Blob)
                    {
                        /// Unified adopt dispatch. Pending blob records were already moved
                        /// to dst_st.pending_blobs above (MOVE semantics), so copy_pending=false.
                        adoptStagedBlob(findPendingBlob(dst_st, entry.ref), entry, dst_st, *dst_st.build, /*copy_pending=*/false);
                    }
                src_st.build->abandon();
            }
            parts.erase(src_it);

            /// A freshly-written part finalized tmp->final is re-keyed in the
            /// overlay above (entries/marks/pending blobs/build moved src->dst). The durable publish
            /// happens only in this transaction's commit (the existing publishStaging loop), not in
            /// this method. `MergeTree` calls that commit from `Transaction::renameParts` while off
            /// the `data_parts` lock and before the
            /// Keeper multi. No early-published ref to compensate on abort within this method
            /// (see ~ContentAddressedTransaction).
        }

        if (had_staged_source)
        {
            /// A nested text-index sub-storage (MergeTask/MutateTask createTemporaryTextIndexStorage)
            /// may have DURABLY published a committed scratch ref at THIS part's own path holding only
            /// `<part>/text_index_tmp/` files. That ref is not ours and is not staged; drop it now so the
            /// overlay we publish in commit() is the authoritative manifest. Independent of our publish
            /// timing (it targets an already-committed foreign ref), so it stays a call-time drop.
            metadata_storage.partAccess()->dropRefIfPresent(src->refKey());
            return;
        }

        /// Move any COMMITTED source ref (a merge/mutation result rename, DETACH, ATTACH, a
        /// delete_tmp_ rename, an early-committed child ref being renamed away). Absent = a pure
        /// staged/tmp move - nothing durable to touch.
        metadata_storage.partAccess()->republishRef(src->refKey(), dst->refKey());
        return;
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR,
        "ContentAddressed: moveDirectory from {} to {} has an unsupported shape", path_from, path_to);
}

void ContentAddressedTransaction::moveFile(const std::string & path_from, const std::string & path_to)
{
    /// Write gate (rev.7 §1). (`replaceFile` delegates here, so its own gate below is defense in depth.)
    metadata_storage.checkOpAdmitted(CasOpClass::Write);
    /// Verbatim table-level files and loose mountpoint files: physically move the object (the
    /// mutation entry tmp_mutation_N.txt -> mutation_N.txt rename; already durable from its finalize).
    if (!Cas::isPartFilePath(path_from) && !Cas::isPartFilePath(path_to))
    {
        auto move_table_verbatim = [&](const Cas::TableFilePath & src_tf,
                                       const Cas::TableFilePath & dst_tf)
        {
            const Cas::RootNamespace src_ns = metadata_storage.liveNamespace(src_tf.table_uuid);
            const Cas::RootNamespace dst_ns = metadata_storage.liveNamespace(dst_tf.table_uuid);
            if (src_ns.string() == dst_ns.string() && src_tf.tail == dst_tf.tail)
                return;
            /// A verbatim rename is emulated as get(src) -> put(dst) -> remove(src) because object
            /// storage has no atomic rename. SINGLE-WRITER CONTRACT: only the owning server renames its
            /// own table-level verbatim files (mutation entries), so there is no concurrent writer to
            /// race the blind put(dst) against — the put's last-writer-wins is safe under that contract.
            /// Idempotent re-drive: if the source is already gone but the destination is present, a
            /// previous drive completed this move — treat as done (matches a re-driven FS rename, which
            /// is an ENOENT-tolerant no-op) instead of throwing FILE_DOESNT_EXIST. An unrelated
            /// pre-existing destination can never reach this branch: destination names derive
            /// deterministically from source names, and the SINGLE-WRITER contract means only this
            /// move's own prior drive can have produced it.
            /// The source resolves readably (a move reads it) and the destination the minting way (a move
            /// writes it). A source namespace with no readable life has no file to move, which is the same
            /// outcome as an absent object and takes the identical already-moved / genuinely-missing split
            /// below -- so absence of a life is not a separate error path.
            const auto src_life = metadata_storage.readableNamespaceFilesLife(src_ns);
            const auto src_bytes = src_life
                ? metadata_storage.store()->getNamespaceFile(*src_life, src_tf.tail)
                : std::nullopt;
            if (!src_bytes)
            {
                const auto dst_probe = metadata_storage.readableNamespaceFilesLife(dst_ns);
                if (dst_probe && metadata_storage.store()->getNamespaceFile(*dst_probe, dst_tf.tail))
                    return;
                throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "ContentAddressed: moveFile source missing: {}", path_from);
            }
            metadata_storage.store()->putNamespaceFile(
                metadata_storage.store()->namespaceLife(dst_ns), dst_tf.tail, *src_bytes);
            metadata_storage.store()->removeNamespaceFile(*src_life, src_tf.tail);
        };
        auto src_tf = Cas::parseTableFilePath(path_from);
        auto dst_tf = Cas::parseTableFilePath(path_to);
        if (src_tf && dst_tf)
        {
            move_table_verbatim(*src_tf, *dst_tf);
            return;
        }
        /// Loose mountpoint files (rare): read + put + remove plain objects. The same single-writer
        /// contract + idempotent re-drive as the table-verbatim branch above.
        const std::string src_key = metadata_storage.serverRootId() + "/" + path_from;
        const std::string dst_key = metadata_storage.serverRootId() + "/" + path_to;
        if (src_key == dst_key)
            return;
        auto bytes = metadata_storage.store()->getMountpointObject(src_key);
        if (!bytes)
        {
            if (metadata_storage.store()->getMountpointObject(dst_key))
                return;
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "ContentAddressed: moveFile source missing: {}", path_from);
        }
        metadata_storage.store()->putMountpointObject(dst_key, *bytes);
        metadata_storage.store()->removeMountpointObject(src_key);
        return;
    }

    auto src = routeOf(path_from);
    auto dst = routeOf(path_to);
    /// A part-DIRECTORY rename reaching moveFile (PartsTemporaryRename::rollBackAll undoes an
    /// attach via moveFile): delegate to moveDirectory, which owns directory shapes.
    if (src && dst && !src->ref.empty() && src->file.empty() && !dst->ref.empty() && dst->file.empty())
    {
        moveDirectory(path_from, path_to);
        return;
    }
    if (!src || src->file.empty() || !dst || dst->file.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "ContentAddressed: moveFile requires two part-file paths: {} -> {}", path_from, path_to);

    auto & src_st = stagingFor(*src);
    auto & dst_st = stagingFor(*dst);

    /// Staged content entry re-keys in place (cross-part included; dependencies follow the entries).
    /// canonical policy: SOURCE-wins — a move/rename carries the source's content to the destination,
    /// overwriting any prior dest bytes (the POSIX `rename` semantic, and what the atomic-write
    /// `.tmp -> final` rename requires). moveDirectory's staged merge is aligned to this same policy.
    auto it = std::find_if(src_st.entries.begin(), src_st.entries.end(),
        [&](const Cas::ManifestEntry & e) { return e.path == src->file; });
    if (it != src_st.entries.end())
    {
        auto entry = std::move(*it);
        src_st.entries.erase(it);
        entry.path = dst->file;
        if (&src_st != &dst_st && entry.placement == Cas::EntryPlacement::Blob)
        {
            /// Unified adopt dispatch. MOVE semantics — physically move the pending blob
            /// record from src_st to dst_st FIRST (so dst_st owns the upload), then call adoptStagedBlob
            /// with copy_pending=false (the record is already in dst_st; no additional copy needed).
            auto pb_it = std::find_if(src_st.pending_blobs.begin(), src_st.pending_blobs.end(),
                [&](const PartStaging::PendingBlob & pb) { return pb.ref == entry.ref; });
            if (pb_it != src_st.pending_blobs.end())
            {
                dst_st.pending_blobs.push_back(std::move(*pb_it));
                src_st.pending_blobs.erase(pb_it);
            }
            adoptStagedBlob(findPendingBlob(dst_st, entry.ref), entry, dst_st, buildFor(*dst, dst_st), /*copy_pending=*/false);
        }
        std::erase_if(dst_st.entries, [&](const Cas::ManifestEntry & e) { return e.path == entry.path; });
        dst_st.entries.push_back(std::move(entry));
        return;
    }
    /// Source not staged in this transaction: this would cover a standalone one-shot rename of a
    /// committed `txn_version.txt` file. Atomic-write storages (including CA) bypass that rename:
    /// `VersionMetadataOnDisk::storeInfoToDataPartStorage` writes `txn_version.txt` directly, with no
    /// `.tmp` + `replaceFile` dance. This branch therefore has no live caller and is retained only as
    /// a fail-loud guard for an unsupported mutation shape.
    throw Exception(ErrorCodes::LOGICAL_ERROR, "ContentAddressed: moveFile source not staged: {}", path_from);
}

void ContentAddressedTransaction::replaceFile(const std::string & path_from, const std::string & path_to)
{
    /// Write gate (rev.7 §1): refuse before dropping staged destination state on a Vanished/uncertain disk.
    metadata_storage.checkOpAdmitted(CasOpClass::Write);
    /// replaceFile = moveFile that overwrites the destination. Drop any staged destination state
    /// first, then delegate (the verbatim branch's putNamespaceFile already overwrites).
    if (auto dst = routeOf(path_to); dst && !dst->file.empty())
    {
        auto & dst_st = stagingFor(*dst);
        /// A matching pending_blobs record (if any) is left in place — its temp file is cleaned by
        /// cleanupPendingTempFiles at commit end, and the orphaned record is filtered out of the
        /// publish upload by the staged-tree-hash check in publishStaging. We do NOT purge it
        /// eagerly because the same hash may still be referenced by another staged entry.
        std::erase_if(dst_st.entries, [&](const Cas::ManifestEntry & e) { return e.path == dst->file; });
    }
    moveFile(path_from, path_to);
}

void ContentAddressedTransaction::unlinkFile(const std::string & path, bool if_exists, bool /*should_remove_objects*/)
{
    /// Part file. Two sub-cases:
    ///   1. A file STAGED in this transaction (content entry or legacy mutable bytes): drop the
    ///      staged state so it never reaches the published tree.
    ///   2. A COMMITTED CONTENT file (not staged here): stage a REMOVAL MARK
    ///      (`content_removed`). The mark is resolved at publish (`publishStaging`): a repoint
    ///      republishes the manifest minus the removed paths, UNLESS this same transaction also
    ///      drops the whole part directory (`removeDirectory`), in which case the mark is
    ///      superseded — see `removeDirectory` below.
    ///
    /// This is a load-bearing invariant; do not "fix" it with a blanket fail-closed assert:
    /// On a content-addressed disk a committed part is ONE atomic ref (its manifest tree); the removal
    /// UNIT is the whole-part ref-drop done by `removeDirectory(<part>)`, NOT per-file unlinks. The
    /// MergeTree fast-removal path (IMergeTreeDataPart::remove) unlinks EVERY part file one-by-one and
    /// THEN calls `removeDirectory` — so a batched per-file unlink storm immediately followed by a
    /// ref-drop in the SAME transaction must cost exactly one ref-drop and zero repoints, not one
    /// repoint per unlinked file. `removeDirectory` clears any marks staged here for the same ref
    /// before the transaction publishes, which is what makes the storm-then-drop shape free. A lone
    /// surgical unlink NOT followed by a ref-drop in the same transaction (ATTACH's
    /// `removeVersionMetadata`, a future backfill/repair delete) resolves to one repoint-remove —
    /// this closes the file's former fail-open (a committed content file could never actually be
    /// deleted on its own; this behavior now closes that earlier fail-open.
    ///
    /// Remove gate (rev.7 §1): a Vanished disk answers no-op success; a transient / IdentityLost disk
    /// throws 668.
    if (metadata_storage.checkOpAdmitted(CasOpClass::Remove) == CasOpAdmission::TruthAbsent)
        return;
    if (auto r = routeOf(path); r && !r->file.empty())
    {
        auto & st = stagingFor(*r);
        const bool staged_here = std::any_of(st.entries.begin(), st.entries.end(),
                           [&](const Cas::ManifestEntry & e) { return e.path == r->file; });
        /// A matching pending_blobs record (if any) is left in place — its temp file is cleaned by
        /// cleanupPendingTempFiles at commit end, and the orphaned record is filtered out of the
        /// publish upload by the staged-tree-hash check in publishStaging. We do NOT purge it
        /// eagerly because the same hash may still be referenced by another staged entry.
        std::erase_if(st.entries, [&](const Cas::ManifestEntry & e) { return e.path == r->file; });
        if (!staged_here)
        {
            /// One mandatory body-HEAD per (transaction, ref), not per file: the MergeTree fast-removal
            /// path unlinks every file of the part through THIS transaction right before removeDirectory.
            /// The first unlink re-proves the body ForceFresh; the rest of the burst reuses that proof.
            const String memo_key = r->refKey().cacheKey();
            const bool already_proven = force_fresh_validated_refs.contains(memo_key);
            const auto view = metadata_storage.partAccess()->getView(
                r->refKey(), already_proven ? Cas::Freshness::CachedForLoad : Cas::Freshness::ForceFresh);
            if (view && !already_proven)
                force_fresh_validated_refs.insert(memo_key);
            if (!view || !view->hasFile(r->file))
            {
                if (if_exists)
                    return;
                throw Exception(
                    ErrorCodes::FILE_DOESNT_EXIST,
                    "ContentAddressed: unlinkFile target does not exist: {}",
                    path);
            }
            st.content_removed.insert(r->file);
        }
        return;
    }

    /// Verbatim table-level / loose mountpoint file: reclaim the object NOW (GC never scans them;
    /// a pruned mutation entry would otherwise leak until DROP.
    if (auto tf = Cas::parseTableFilePath(path))
    {
        /// Readable resolution, i.e. the non-creating one: an unlink must not birth a namespace -- and
        /// `unlinkFile(..., if_exists = true)` is called from cleanup paths whose whole contract is to be
        /// a no-op. No life means no such file, exactly the absent case the branch below already handles.
        const auto life = metadata_storage.readableNamespaceFilesLife(
            metadata_storage.liveNamespace(tf->table_uuid));
        if (!life || !metadata_storage.store()->getNamespaceFile(*life, tf->tail))
        {
            if (if_exists)
                return;
            throw Exception(
                ErrorCodes::FILE_DOESNT_EXIST,
                "ContentAddressed: unlinkFile target does not exist: {}",
                path);
        }
        metadata_storage.store()->removeNamespaceFile(*life, tf->tail);
        return;
    }
    /// Loose mountpoint file: exact-token delete of the plain object.
    const String key = metadata_storage.serverRootId() + "/" + path;
    if (!metadata_storage.store()->getMountpointObject(key))
    {
        if (if_exists)
            return;
        throw Exception(
            ErrorCodes::FILE_DOESNT_EXIST,
            "ContentAddressed: unlinkFile target does not exist: {}",
            path);
    }
    metadata_storage.store()->removeMountpointObject(key);
}

void ContentAddressedTransaction::truncateFile(const std::string &, size_t)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
        "truncateFile is not supported on a content-addressed disk (blobs are immutable; "
        "whole-file rewrites replace the staged entry instead)");
}

}

namespace DB::Cas
{

namespace
{
/// Ceiling for a CAS write-buffer allocation. An extreme `max_compress_block_size` (or any other
/// out-of-range buffer-size setting -- fuzzed or misconfigured) flows into `writeFile`'s `buf_size` /
/// `adaptive_write_buffer_initial_size` and, unclamped, reaches `Memory::alloc` where the allocator's
/// `checkSize` (>= 0x8000000000000000) fires a `LOGICAL_ERROR` and aborts the server in
/// debug/sanitizer builds. The ordinary MergeTree writers clamp compress-block sizes to 256 MiB
/// (`MergeTreeWriterSettings::MAX_COMPRESS_BLOCK_SIZE`) for exactly this reason; the CAS write path
/// received the value unclamped. Mirror that ceiling here, at the allocation site, so no caller can
/// pass an absurd size to the allocator. 256 MiB is duplicated (not #included) to keep the Disks layer
/// free of a Storages/MergeTree dependency; the regression guard is
/// `04070_no_crash_extreme_compress_block_size` run on a content-addressed storage policy.
constexpr size_t kMaxCasWriteBufferBytes = 256ULL * 1024 * 1024;

size_t clampCasWriteBufferSize(size_t size)
{
    return std::min(size, kMaxCasWriteBufferBytes);
}
}

void fanOutBlobUploads(
    PartWriteTxn & build,
    std::span<const BlobUploadRequest> requests,
    ThreadPool & pool,
    const BlobUploadFanoutHooksForTest * hooks)
{
    /// Group by unique ref. Staged-hardlink copies push a DUPLICATE pending-blob record for one BlobRef,
    /// and the fan-out must launch exactly ONE task per unique ref (spec §1 "One task per unique ref").
    /// An ordered map gives a DETERMINISTIC dispatch order (ascending `BlobRef`), which fixes the "first
    /// error" of the merge-nothing contract to a stable task so a failure is reproducible.
    std::map<BlobRef, BlobUploadRequest> grouped;
    for (const auto & req : requests)
    {
        /// Fail-close: the fan-out groups and conflict-checks on `declared_size`, while `source.size` is
        /// the per-attempt streaming byte authority. A wiring bug that let them diverge would group on
        /// one value while streaming another — reject it rather than upload a wrong-length body.
        if (req.declared_size != req.source.size)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "fanOutBlobUploads: request for {} declares size {} but its source is sized {} -- the "
                "grouping key and the streaming byte authority must agree",
                blobIdOf(req.ref), req.declared_size, req.source.size);
        const auto [it, inserted] = grouped.try_emplace(req.ref, req);
        if (!inserted && it->second.declared_size != req.declared_size)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "fanOutBlobUploads: conflicting declared sizes for {} ({} vs {}) -- staged-hardlink "
                "copies of one ref must agree on size (one task, one dep per unique ref)",
                blobIdOf(req.ref), it->second.declared_size, req.declared_size);
    }

    if (grouped.empty())
        return;

    ProfileEvents::increment(ProfileEvents::CASBlobUploadFanoutBatches);

    /// One result slot per unique ref, pre-sized so element addresses are STABLE: each task writes ONLY
    /// its own slot (no data race on the vector) and the vector never reallocates. Declared BEFORE the
    /// runner and the drain guard so it OUTLIVES them — the drain guard joins every scheduled task before
    /// `results` is destroyed on EVERY path, including a throw raised during the dispatch loop below (the
    /// B90 lesson, `threadPoolCallbackRunner.h:68`). Tasks capture only owning/value state: a stable slot
    /// pointer, the request by value (its source is captured by value too), and the txn pointer whose
    /// pointee outlives the runner.
    std::vector<BlobUploadResult> results(grouped.size());

    {
        /// `pool` is disjoint from the S3 writer pool an upload may itself use, and the calling thread
        /// only submits and joins — it never occupies a pool slot — so a size-1 pool degenerates to a
        /// correct serial run and can never deadlock. `ThreadName::UNKNOWN`: stage-1 must not add a
        /// CAS-specific `ThreadName` to the shared enum (`setThreadName.h` is outside the allowed file
        /// set), and UNKNOWN is the no-op name; it also stays clear of the pool-thread self-join
        /// assertions in `CasPool`. The query `ThreadGroup` is still propagated per task by the runner.
        using RunnerTask = ThreadPoolCallbackRunnerLocal<void>::Task;
        ThreadPoolCallbackRunnerLocal<void> runner(pool, ThreadName::UNKNOWN);
        const PartWriteTxn * txn = &build;   /// `uploadBlobDetached` is const + build-neutral: safe off-thread
        /// We track scheduled tasks in OUR OWN vector (via `enqueueAndGiveOwnership`) rather than the
        /// runner's `enqueueAndKeepTrack`: that helper schedules a task and only THEN appends its handle
        /// to an UNRESERVED tracking vector, so a `bad_alloc` at the append would leave a
        /// scheduled-but-untracked task the runner's destructor cannot join — it would run later against
        /// the already-destroyed `results`/txn (a use-after-free; codex stage-1 review, Critical).
        /// PRE-RESERVING `handles` to the exact task count makes the append after each schedule a
        /// no-throw operation, so a task is NEVER scheduled without being tracked in the SAME expression.
        std::vector<std::shared_ptr<RunnerTask>> handles;
        handles.reserve(grouped.size());
        /// Drain on EVERY path: the runner's destructor only joins tasks IT owns (we use
        /// `enqueueAndGiveOwnership`, so its own set stays empty), so WE must join every scheduled task
        /// before `results` and the upload sources are destroyed — including when the dispatch loop
        /// throws (an `on_dispatch`/`after_enqueue` seam, or a scheduling failure mid-loop). Declared
        /// AFTER `handles`/`runner` so it runs FIRST on scope exit, joining while both are still alive;
        /// `waitForAllToFinish` only waits (never throws), so it is safe on the unwinding path (the B90
        /// lesson, `threadPoolCallbackRunner.h:68`).
        SCOPE_EXIT_SAFE({ ThreadPoolCallbackRunnerLocal<void>::waitForAllToFinish(handles); });
        size_t idx = 0;
        for (const auto & [ref, req] : grouped)
        {
            BlobUploadResult * slot = &results[idx++];
            BlobUploadRequest task_req = req;
            const BlobUploadFanoutHooksForTest * task_hooks = hooks;
            if (hooks && hooks->on_dispatch)
                hooks->on_dispatch(ref);   /// may throw ⇒ the drain guard joins already-scheduled tasks
            /// Schedule and track in ONE no-throw step: `enqueueAndGiveOwnership` returns the handle (the
            /// task is now runnable) and the pre-reserved `emplace_back` records it without allocating, so
            /// there is no window in which a scheduled task is untracked.
            handles.emplace_back(runner.enqueueAndGiveOwnership([slot, txn, req_by_value = std::move(task_req), task_hooks]
            {
                if (task_hooks && task_hooks->in_task)
                    task_hooks->in_task(req_by_value.ref);
                *slot = txn->uploadBlobDetached(req_by_value);
            }));
            if (task_hooks && task_hooks->after_enqueue)
                task_hooks->after_enqueue(ref);   /// task already tracked ⇒ the drain guard joins it too
            ProfileEvents::increment(ProfileEvents::CASBlobUploadFanoutTasks);
        }
        /// Drain ALL tasks, then rethrow the FIRST (ascending-ref dispatch order) that failed. A rethrow
        /// bypasses the merge below, so NOTHING is merged: `build` stays at its pre-fan-out pending-dep
        /// state (merge-nothing). On success this clears `handles`, so the drain guard above then waits an
        /// empty set; on any throw it leaves them and the guard joins the survivors (already all done).
        ThreadPoolCallbackRunnerLocal<void>::waitForAllToFinishAndRethrowFirstError(handles);
    }

    /// Every task succeeded (else we rethrew above): fold all results into `build` on this (the owning
    /// writer) thread, all-or-nothing (`mergeBlobUploadResults` prevalidates, then build-and-swaps).
    build.mergeBlobUploadResults(results);
}


CaContentWriteBuffer::CaContentWriteBuffer(
    std::string temp_dir,
    Cas::BlobHashAlgo hash_algo,
    size_t buf_size,
    bool use_adaptive_buffer_size,
    size_t adaptive_buffer_initial_size,
    OnFinalized on_finalized_)
    : WriteBufferFromFileBase(clampCasWriteBufferSize(use_adaptive_buffer_size ? adaptive_buffer_initial_size : buf_size), nullptr, 0)
    , on_finalized(std::move(on_finalized_))
{
    fs::create_directories(temp_dir);
    temp_path = temp_dir + "/" + getRandomASCIIString(32) + ".tmp";

    /// The spill buffer is a SECOND per-stream buffer; thread the adaptive flag into it too so a
    /// wide part keeps its footprint small. Its IO is a local temp file, not the remote stream.
    sink = std::make_unique<WriteBufferFromFile>(
        temp_path,
        clampCasWriteBufferSize(buf_size),
        /*flags=*/-1,
        /*throttler=*/nullptr,
        /*mode=*/0666,
        /*existing_memory=*/nullptr,
        /*alignment=*/0,
        use_adaptive_buffer_size,
        clampCasWriteBufferSize(adaptive_buffer_initial_size));
    hashing = Cas::makeBlobHashingWriteBuffer(hash_algo, *sink);
}

CaContentWriteBuffer::CaContentWriteBuffer(
    std::unique_ptr<WriteBufferFromFileBase> object_store_sink,
    std::string object_key,
    std::string envelope_header,
    Cas::BlobHashAlgo hash_algo,
    size_t buf_size,
    bool use_adaptive_buffer_size,
    size_t adaptive_buffer_initial_size,
    OnFinalized on_finalized_,
    std::function<void()> check_fence_before_finalize_)
    : WriteBufferFromFileBase(clampCasWriteBufferSize(use_adaptive_buffer_size ? adaptive_buffer_initial_size : buf_size), nullptr, 0)
    , on_finalized(std::move(on_finalized_))
    , temp_path(std::move(object_key))
    , is_s3_staging(true)
    , sink(std::move(object_store_sink))
    , check_fence_before_finalize(std::move(check_fence_before_finalize_))
{
    /// The sink is ALREADY opened against the staging object by the caller (writeFile) — this
    /// constructor wraps it in the hashing chain, exactly like the local-temp-file mode.
    ///
    /// Write the CABL envelope header to the sink first, directly —
    /// bypassing `hashing` (so it is excluded from the content hash) and this outer buffer's `count()`
    /// (so the reported size is the payload only). The staging object therefore holds `[header][payload]`
    /// and the promote stays a verbatim server-side copy. Only the payload the caller subsequently writes
    /// through THIS buffer flows through `hashing`. The header write precedes any payload write, so the
    /// on-object byte order is header-then-payload.
    if (!envelope_header.empty())
        sink->write(envelope_header.data(), envelope_header.size());

    /// The adaptive-sizing params only affect THIS outer buffer (mirroring the Local ctor above); the
    /// sink's own buffering was decided by the caller when it opened the object-store write.
    hashing = Cas::makeBlobHashingWriteBuffer(hash_algo, *sink);
}

CaContentWriteBuffer::~CaContentWriteBuffer()
{
    /// Best-effort cleanup if finalize was never reached (exception unwind / cancel).
    cancel();
    /// If on_finalized ran successfully the transaction (Local mode) or a later promote
    /// path (S3 mode) owns the staged bytes and cleans them up. Do not remove them here. S3-mode
    /// staging objects are never removed by this class at all (see cancelImpl / removeTempFile).
    if (!temp_ownership_transferred && !is_s3_staging)
        removeTempFile();
}

void CaContentWriteBuffer::nextImpl()
{
    if (!offset())
        return;
    hashing->write(working_buffer.begin(), offset());
}

void CaContentWriteBuffer::finalizeImpl()
{
    next();
    const size_t size = count();

    /// getHashHex flushes the chain and returns the streaming digest (the pool's selected algo) of
    /// everything written, as 32 lowercase hex chars.
    const std::string hash_hex = hashing->getHashHex();

    hashing->finalize();

    /// rev.7 [C2]: re-check the fence-generation admission IMMEDIATELY before the durable backend call
    /// (S3 mode's `sink->finalize()` completes the staging object -- Local mode never sets this
    /// callback). A fence trip or re-arm since construction aborts here with the typed transient error,
    /// before the upload becomes durable.
    if (check_fence_before_finalize)
        check_fence_before_finalize();

    sink->finalize();

    /// On successful finalize, ownership of temp_path (Local: the local temp path; S3: the
    /// staging object key) transfers to the caller (the transaction uploads/promotes it and cleans
    /// up). cancel() still removes/cancels it.
    if (on_finalized)
    {
        on_finalized(hash_hex, size, temp_path);
        temp_ownership_transferred = true;
    }
}

void CaContentWriteBuffer::cancelImpl() noexcept
{
    if (hashing)
        hashing->cancel();
    if (sink)
        sink->cancel();
    /// S3 mode: `temp_path` is a remote object key, not a path on this filesystem — do NOT attempt
    /// to delete the (possibly partially-written) staging object here. Cancelling `sink` above is
    /// enough to make sure no partial finalize happens; reclaiming an orphaned staging object is the
    /// mount-lease sweeper's job.
    if (!is_s3_staging)
        removeTempFile();
}

void CaContentWriteBuffer::removeTempFile() noexcept
{
    std::error_code ec;
    fs::remove(temp_path, ec);
}

void CaContentWriteBuffer::sync()
{
    next();
    hashing->next();
    sink->sync();
}

std::string CaContentWriteBuffer::getFileName() const
{
    return temp_path;
}

CaInlineWriteBuffer::CaInlineWriteBuffer(OnInlined on_inlined_)
    : WriteBufferFromFileBase(DBMS_DEFAULT_BUFFER_SIZE, nullptr, 0)
    , on_inlined(std::move(on_inlined_))
{
}

CaInlineWriteBuffer::~CaInlineWriteBuffer()
{
    cancel();
}

void CaInlineWriteBuffer::nextImpl()
{
    if (!offset())
        return;
    accumulated.append(working_buffer.begin(), offset());
}

void CaInlineWriteBuffer::finalizeImpl()
{
    next();
    if (on_inlined)
        on_inlined(std::move(accumulated));
}

void CaInlineWriteBuffer::sync()
{
    next();
}

std::string CaInlineWriteBuffer::getFileName() const
{
    return "ca_inline";
}

}
