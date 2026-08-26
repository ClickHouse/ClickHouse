#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasDecommission.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasServerRootFormats.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEvent.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasOrphanManifestSweep.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCatalog.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <base/scope_guard.h>
#include <algorithm>
#include <chrono>
#include <limits>
#include <set>
#include <tuple>

namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}
}

namespace DB::Cas
{

namespace
{

uint64_t nowMs()
{
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
}

/// Delete every object listed under `prefix` by its listed (or, absent a list-token backend, HEAD'd)
/// token. This backs the staging and roots drain phases below: the victim's writers are fenced by the
/// decommission claim (`Pool::openForDecommission`), so nothing should be racing these deletes, and a
/// plain exact-token delete of every listed object is race-free.
///
/// A per-object failure — a backend exception, a `TokenMismatch` or `NotFound` outcome, or an object
/// disappearing between `LIST` and `HEAD` — is recorded as a warning and does not prevent the remaining
/// objects from being attempted. The caller keeps the pool slot whenever warnings are present, so the
/// terminated slot remains available as a resume anchor instead of being deleted after an unconfirmed
/// drain. Returns only the objects whose exact-token delete was reported as `Deleted`.
uint64_t deleteListedPrefix(Backend & backend, const String & prefix, std::vector<String> & warnings)
{
    uint64_t deleted = 0;
    forEachListedKey(backend, prefix, [&](const ListedKey & listed)
    {
        try
        {
            Token token;
            if (listed.token)
                token = *listed.token;
            else
            {
                const HeadResult head = backend.head(listed.key);
                if (!head.exists)
                {
                    warnings.push_back("decommission drain: " + listed.key + " vanished before delete");
                    return;
                }
                token = head.token;
            }

            const DeleteOutcome outcome = backend.deleteExact(listed.key, token);
            const DeleteClass outcome_class = classifyDeleteOutcome(outcome);
            if (outcome_class == DeleteClass::Deleted)
                ++deleted;
            else
                warnings.push_back("decommission drain: " + listed.key + " delete outcome "
                                    + String(deleteClassName(outcome_class)));
        }
        catch (...)
        {
            warnings.push_back("decommission drain: " + listed.key + " delete failed: "
                                + getCurrentExceptionMessage(/*with_stacktrace=*/false));
        }
    });
    return deleted;
}

/// Delete one slot control object by a token captured at the protocol-defined fence point. Slot
/// retirement is fail-closed: unlike the debris drains above, any non-`Deleted` outcome or exception
/// stops the tail before it can touch the next control object.
bool deleteSlotObject(Backend & backend, const String & key, const Token & token, std::vector<String> & warnings)
{
    try
    {
        const DeleteOutcome outcome = backend.deleteExact(key, token);
        const DeleteClass outcome_class = classifyDeleteOutcome(outcome);
        if (outcome_class == DeleteClass::Deleted)
            return true;

        warnings.push_back("slot delete failed: " + key + ": delete outcome "
                           + String(deleteClassName(outcome_class)));
    }
    catch (...)
    {
        warnings.push_back("slot delete failed: " + key + ": "
                           + getCurrentExceptionMessage(/*with_stacktrace=*/false));
    }
    return false;
}

}

DecommissionReport decommissionPoolMember(BackendPtr backend, PoolConfig config,
                                          const String & victim_srid, const CasEventSink & sink,
                                          const std::function<void()> & request_gc_round)
{
    DecommissionReport report;
    report.srid = victim_srid;
    bool gc_round_needed = false;
    /// A namespace may have reached `Removing` before a later namespace fails closed. Preserve the
    /// already-earned liveness signal on every exit: the callback only wakes the existing serialized
    /// GC worker and cannot perform catalog work itself.
    SCOPE_EXIT({
        if (gc_round_needed && request_gc_round)
            request_gc_round();
    });

    /// Validate one required immutable ownership cut before impersonating the victim. The admin open
    /// performs its own fresh catalog observation for mount safety, but namespace selection below
    /// must reuse this exact pre-mutation decision rather than read a later authority set.
    const Layout catalog_layout(config.pool_prefix);
    const CasRefCatalog::Snapshot catalog_cut = CasRefCatalog::read(*backend, catalog_layout);
    catalog_cut.life_index.throwIfAmbiguous("CAS decommission");

    config.event_sink = sink;
    PoolPtr admin = Pool::openForDecommission(std::move(backend), std::move(config), victim_srid);

    EventEmitter{*admin}.emit([&](CasEvent & e)
    {
        e.type = CasEventType::MemberDecommission;
        e.outcome = "begin";
        e.reason = "operator decommission of pool member";
        e.detail = {{"server_root_id", victim_srid}};
    });

    /// The pre-impersonation catalog cut is the complete ownership universe. Physical life keys carry
    /// no logical path, and raw string prefixes such as `victim` must not select the distinct owner
    /// `victim2`; the slash makes `victim` one canonical path component.
    const String victim_namespace_prefix = victim_srid + "/";
    std::vector<std::pair<CatalogEntry, NamespaceLifeId>> owned_lives;
    for (const CatalogEntry & entry : catalog_cut.catalog.entries)
    {
        if (entry.ns.string() != victim_srid && !entry.ns.string().starts_with(victim_namespace_prefix))
            continue;
        const auto life = catalog_cut.life_index.resolve(entry.incarnation);
        if (!life)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "ca-decommission: catalog entry '{}' has no physical life resolution", entry.ns.string());
        owned_lives.emplace_back(entry, *life);
    }

    for (const auto & [selected_entry, life] : owned_lives)
    {
        const RootNamespace & ns = life.ns;
        const String & ns_str = ns.string();

        /// Refuse a same-name lifecycle move that landed after the immutable selection cut. The
        /// exact-life overloads below also pin recovery to `life`, closing the race after this check:
        /// a later replacement can never redirect a removal to its new incarnation.
        const CasRefCatalog::Snapshot current_catalog = CasRefCatalog::read(admin->backend(), admin->layout());
        const auto current_entry = std::find_if(
            current_catalog.catalog.entries.begin(), current_catalog.catalog.entries.end(),
            [&](const CatalogEntry & entry) { return entry.ns.string() == ns_str; });
        if (current_entry == current_catalog.catalog.entries.end() || *current_entry != selected_entry)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "ca-decommission: namespace '{}' changed incarnation after the validated catalog cut; "
                "refusing destructive work",
                ns_str);

        if (selected_entry.state == NsState::Removing)
        {
            if (!admin->backend().head(admin->layout().refCkptKey(life)).exists)
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "ca-decommission: namespace '{}' is Removing but its exact checkpoint is absent; "
                    "the catalog row remains owned and the victim slot cannot be retired",
                    ns_str);

            /// `dropNamespace` is the sole terminal writer. On an already-complete removal this is an
            /// idempotent observation; on a pre-terminal `Removing` life it resumes the exact terminal
            /// append under the administrative writer fence. Catalog deletion remains GC's job.
            (void)admin->dropNamespace(life);
            ++report.namespaces_already_removed;
            gc_round_needed = true;
            continue;
        }

        const auto stats = admin->dropNamespace(life);
        ++report.namespaces_removed;
        report.committed_refs_removed += stats.committed_refs;
        report.precommits_removed += stats.precommits;
        report.edge_deltas_emitted += stats.committed_refs + stats.precommits;
        if (selected_entry.state != NsState::Creating)
            gc_round_needed = true;

        EventEmitter{*admin}.emit([&](CasEvent & e)
        {
            e.type = CasEventType::MemberDecommission;
            e.outcome = "namespace_removed";
            e.reason = "decommission dropped a victim namespace";
            e.detail = {{"server_root_id", victim_srid}, {"namespace", ns_str},
                        {"committed", std::to_string(stats.committed_refs)},
                        {"precommits", std::to_string(stats.precommits)}};
        });
    }

    /// Manifest debris must be removed before the mount slot: deleting the mount body removes the
    /// watermark authority, after which `floorForNamespace` returns no value and the ordinary orphan
    /// sweep cannot prove that old-epoch debris is eligible. The decommission claim has advanced the
    /// writer epoch, so every build prefix with `prefix.writer_epoch < w.writer_epoch` is eligible here.
    /// Group the listed keys by namespace and build prefix so each group can use the exact-token orphan
    /// sweep while the mount body still supplies its authority.
    {
        const String debris_prefix = admin->layout().casManifestsServerPrefix(victim_srid);
        std::set<std::tuple<String, uint64_t, uint64_t>> groups;   /// (namespace, writer epoch, build sequence)
        forEachListedKey(admin->backend(), debris_prefix, [&](const ListedKey & listed)
        {
            if (const auto parsed = admin->layout().parseManifestKey(listed.key))
                groups.emplace(parsed->root_namespace.string(), parsed->ref.writer_epoch, parsed->ref.build_sequence);
        });
        for (const auto & [ns_str, writer_epoch, build_sequence] : groups)
            report.manifest_debris_removed += sweepNamespace(
                *admin, RootNamespace(ns_str), BuildPrefix{writer_epoch, build_sequence}, &report.warnings);
    }

    /// Drain the victim's own `<pool_prefix>/staging/<srid>/` area. The live-mount staging helper uses
    /// an `IObjectStorage`, while this command intentionally works at the `Backend` layer, so the same
    /// prefix is listed and deleted directly. The claim fences the victim's writers during this sweep.
    report.staging_objects_removed += deleteListedPrefix(
        admin->backend(), admin->poolConfig().pool_prefix + "/staging/" + victim_srid + "/", report.warnings);

    /// Drain the victim's mountpoint objects. These are loose, non-content-addressed files under
    /// `Layout::serverRootDataPrefix`; they have no writer epoch of their own, so the claim is what
    /// prevents a returning victim from racing this deletion.
    report.mountpoint_objects_removed += deleteListedPrefix(
        admin->backend(), admin->layout().serverRootDataPrefix(victim_srid), report.warnings);

    /// The catalog, not physical debris, owns the slot-retirement decision. A terminal append only
    /// moves a row to `Removing`; GC must fold/prune/delete it before the member's ownership anchor can
    /// disappear. Capture one exact whole-catalog cut after every drain, then revalidate its token and
    /// canonical value immediately before entering the retirement tail. The administrative claim fences
    /// the victim writer between those observations.
    std::optional<CasRefCatalog::Snapshot> retirement_catalog_cut;
    if (report.warnings.empty())
    {
        retirement_catalog_cut = CasRefCatalog::read(admin->backend(), admin->layout());
        const uint64_t victim_owned_count = std::count_if(
            retirement_catalog_cut->catalog.entries.begin(), retirement_catalog_cut->catalog.entries.end(),
            [&](const CatalogEntry & entry)
            {
                return entry.ns.string() == victim_srid
                    || entry.ns.string().starts_with(victim_namespace_prefix);
            });
        if (victim_owned_count > 0)
            report.warnings.push_back(
                "pool member decommission underway: " + std::to_string(victim_owned_count)
                + " namespace(s) are still owned by this member; upcoming GC rounds perform the final "
                  "cleanup — re-run this command afterwards to retire the slot");
    }

    /// Retire the slot strictly last and only after a clean drain. Copy the layout and shared backend
    /// before `admin.reset()`: graceful close destroys the `Pool`, while the backend must remain alive to
    /// retire the slot objects afterwards.
    const Layout layout = admin->layout();
    const BackendPtr pool_backend = admin->poolBackendPtr();
    if (report.warnings.empty())
    {
        const CasRefCatalog::Snapshot fresh_retirement_catalog
            = CasRefCatalog::read(admin->backend(), admin->layout());
        if (!retirement_catalog_cut
            || fresh_retirement_catalog.token != retirement_catalog_cut->token
            || fresh_retirement_catalog.catalog != retirement_catalog_cut->catalog)
        {
            report.warnings.push_back(
                "catalog changed after the victim ownership check; refusing slot retirement against a stale cut");
        }
    }
    if (report.warnings.empty())
    {
        const String mount_key = layout.mountKey(victim_srid);
        const String epoch_key = layout.epochKey(victim_srid);
        const String owner_key = layout.ownerKey(victim_srid);

        /// Capture both the epoch value and its exact token while the decommission claim still fences
        /// the victim. A successor can only bump this object after the farewell below releases the
        /// claim, so this token is the epoch-side successor fence for the retirement tail.
        std::optional<GetResult> claimed_epoch;
        try
        {
            claimed_epoch = pool_backend->get(epoch_key);
            if (!claimed_epoch)
                report.warnings.push_back("slot capture failed: " + epoch_key + " is absent under the admin claim");
        }
        catch (...)
        {
            report.warnings.push_back("slot capture failed: " + epoch_key + ": "
                                      + getCurrentExceptionMessage(/*with_stacktrace=*/false));
        }

        /// Graceful close stamps an already-expired lease and the watermark farewell
        /// (`min_active = UINT64_MAX`), making the slot `terminated` before its mutable control objects
        /// are removed and its owner anchor is tombstoned.
        admin.reset();

        /// Read the farewell immediately after `finishTeardown` wrote it. Its exact token is the
        /// mount-side fence: deleting by this token can remove only THIS decommission's farewell, not
        /// a successor reclaim. Validate the body against the epoch value captured under the claim so
        /// a successor that completed before this GET is also recognized and left untouched.
        std::optional<GetResult> farewell_mount;
        try
        {
            farewell_mount = pool_backend->get(mount_key);
            if (!farewell_mount)
                report.warnings.push_back("slot capture failed: " + mount_key + " farewell is absent");
        }
        catch (...)
        {
            report.warnings.push_back("slot capture failed: " + mount_key + ": "
                                      + getCurrentExceptionMessage(/*with_stacktrace=*/false));
        }

        bool captures_match = claimed_epoch && farewell_mount;
        if (captures_match)
        {
            try
            {
                const ServerEpoch epoch_value = decodeServerEpoch(claimed_epoch->bytes);
                const MountLease mount_value = decodeMountLease(farewell_mount->bytes);
                captures_match = epoch_value.next_writer_epoch != 0
                    && mount_value.writer_epoch == epoch_value.next_writer_epoch - 1
                    && mount_value.min_active == std::numeric_limits<uint64_t>::max()
                    && !mount_value.gc_fenced;
                if (!captures_match)
                {
                    report.warnings.push_back(
                        "slot capture failed: " + mount_key
                        + " is not this decommission's farewell for the epoch captured under the admin claim");
                }
            }
            catch (...)
            {
                report.warnings.push_back("slot capture failed while validating " + mount_key + " and " + epoch_key + ": "
                                          + getCurrentExceptionMessage(/*with_stacktrace=*/false));
                captures_match = false;
            }
        }

        /// Mount first: if a successor reclaimed it after the farewell capture, the stale farewell
        /// token yields `TokenMismatch` and the tail stops before touching epoch or owner. Epoch second:
        /// its under-claim token similarly detects a successor allocation. Before touching owner, re-read
        /// both mutable objects: a same-UUID successor can recreate them after both deletes without
        /// rewriting the owner identity anchor. Mere presence proves that the slot is live again. Every
        /// delete must be explicitly confirmed as `Deleted`, and the final owner tombstone rewrite must
        /// succeed against the exact token read immediately before it.
        ///
        /// ACCEPTED RESIDUAL WINDOW (final review, not closed by this recheck): a same-UUID successor
        /// can still recreate epoch/mount in the narrow gap strictly AFTER this liveness recheck but
        /// BEFORE the owner CAS below reads its own token -- the successor's owner anchor (same
        /// server_uuid, not yet retired) then gets tombstoned by this decommission run. The successor's
        /// live process is not deleted (only its owner anchor is marked retired), but a LATER restart of
        /// that same identity would refuse to reclaim it (claimOwnerOrThrow's tombstone guard). This is
        /// a narrow, low-probability window, deliberately not closed here: T5's owner-tombstone design
        /// (finding #9) intentionally stopped short of making concurrent decommission-vs-recreate
        /// airtight to the microsecond, since that was explicitly not the priority for this fix.
        report.slot_removed = false;
        if (captures_match && deleteSlotObject(*pool_backend, mount_key, farewell_mount->token, report.warnings)
            && deleteSlotObject(*pool_backend, epoch_key, claimed_epoch->token, report.warnings))
        {
            std::optional<GetResult> current_mount;
            std::optional<GetResult> current_epoch;
            bool liveness_recheck_succeeded = true;
            try
            {
                current_mount = pool_backend->get(mount_key);
            }
            catch (...)
            {
                report.warnings.push_back("slot liveness recheck failed: " + mount_key + ": "
                                          + getCurrentExceptionMessage(/*with_stacktrace=*/false));
                liveness_recheck_succeeded = false;
            }
            try
            {
                current_epoch = pool_backend->get(epoch_key);
            }
            catch (...)
            {
                report.warnings.push_back("slot liveness recheck failed: " + epoch_key + ": "
                                          + getCurrentExceptionMessage(/*with_stacktrace=*/false));
                liveness_recheck_succeeded = false;
            }

            if (liveness_recheck_succeeded && (current_mount || current_epoch))
            {
                report.warnings.push_back(
                    "slot delete aborted: successor reappeared after mutable control-object deletion; owner kept");
            }
            else if (liveness_recheck_succeeded)
            {
                try
                {
                    if (const auto owner = pool_backend->get(owner_key))
                    {
                        OwnerObject tombstoned = decodeOwner(owner->bytes);
                        tombstoned.retired_at_ms = nowMs();
                        /// Controlled, not a bare putOverwrite: a transient transport error here (or
                        /// one whose response was simply lost) must not be reported as a hard failure
                        /// when the write actually landed. A standalone controller (decommission is an
                        /// administrative, non-hot-path operation; no mount-lease fence applies to it
                        /// -- the exact-token CAS itself is the safety mechanism, same as the mount/
                        /// epoch deletes above) resolves an ambiguous attempt with one GET: unchanged
                        /// token means the write never applied (legitimately retryable within budget);
                        /// matching bytes means this exact tombstone already landed (Committed, not a
                        /// failure); anything else is a genuine successor reclaim (Conflict).
                        CasRequestController controller(pool_backend, CasRequestBudget{});
                        const CasOverwriteResult result = controller.putOverwriteControlled(
                            owner_key, encodeOwner(tombstoned), owner->token, [] { return true; });
                        if (result.outcome == CasOverwriteOutcome::Committed)
                            report.slot_removed = true;
                        else if (result.outcome == CasOverwriteOutcome::Conflict)
                            report.warnings.push_back(
                                "slot tombstone failed: " + owner_key
                                + ": successor reclaimed the owner anchor before this decommission's tombstone write");
                        else
                            report.warnings.push_back(
                                "slot tombstone failed: " + owner_key
                                + ": tombstone write outcome could not be resolved (retry budget exhausted "
                                  "or the resolve GET itself failed) -- rerun the command to retry");
                    }
                    else
                        report.warnings.push_back(
                            "slot tombstone failed: " + owner_key + ": object absent before tombstone write");
                }
                catch (...)
                {
                    report.warnings.push_back("slot tombstone failed: " + owner_key + ": "
                                              + getCurrentExceptionMessage(/*with_stacktrace=*/false));
                }
            }
        }
    }
    else
    {
        report.slot_removed = false;
        LOG_WARNING(getLogger("CasDecommission"),
            "CAS decommission '{}': drain incomplete ({} warnings) — mount slot kept (terminated); "
            "re-run the command to finish", victim_srid, report.warnings.size());
        admin.reset();   /// Graceful close still stamps the farewell, leaving the slot `terminated`.
    }

    /// The `end` event is emitted via `sink` directly, not `EventEmitter{*admin}`: `admin` is gone by
    /// now. This also means its `warnings` count reflects the FINAL total, including a slot-retirement
    /// failure appended just above -- `EventEmitter`'s own zero-cost-when-absent guard is reproduced by
    /// the `if (sink)` below.
    if (sink)
    {
        CasEvent e;
        e.type = CasEventType::MemberDecommission;
        e.outcome = "end";
        e.reason = "decommission finished";
        e.detail = {{"server_root_id", victim_srid},
                    {"namespaces_removed", std::to_string(report.namespaces_removed)},
                    {"warnings", std::to_string(report.warnings.size())},
                    {"slot_removed", report.slot_removed ? "1" : "0"}};
        sink(std::move(e));
    }
    return report;
}

}
