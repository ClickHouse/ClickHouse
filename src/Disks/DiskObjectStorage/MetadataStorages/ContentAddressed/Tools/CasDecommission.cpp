#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasDecommission.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasServerRootFormats.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEvent.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasOrphanManifestSweep.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCatalog.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <base/defines.h>
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

std::string_view removalName(Removal r)
{
    switch (r)
    {
        case Removal::Removed:  return "removed";
        case Removal::Gone:     return "gone";
        case Removal::Mismatch: return "mismatch";
    }
    UNREACHABLE();
}

/// Delete every object listed under `prefix` by its listed (or, absent a list-incarnation backend,
/// HEAD'd) incarnation. This backs the staging and roots drain phases below: the victim's writers are
/// fenced by the decommission claim (`Pool::openForDecommission`), so nothing should be racing these
/// deletes, and a plain exact-incarnation delete of every listed object is race-free.
///
/// A per-object failure — a backend exception, a `Mismatch` or `Gone` outcome, or an object
/// disappearing between `LIST` and `HEAD` — is recorded as a warning and does not prevent the remaining
/// objects from being attempted. The caller keeps the pool slot whenever warnings are present, so the
/// terminated slot remains available as a resume anchor instead of being deleted after an unconfirmed
/// drain. Returns only the objects whose exact-incarnation delete was reported as `Removed`.
uint64_t deleteListedPrefix(CasOperation & op, const String & prefix, std::vector<String> & warnings)
{
    uint64_t deleted = 0;
    op.forEachListedKey(prefix, [&](const ListedKey & listed)
    {
        try
        {
            std::optional<Etag> etag = listed.etag;
            if (!etag)
            {
                const std::optional<Meta> head = op.head(listed.key, Retry::standard());
                if (!head)
                {
                    warnings.push_back("decommission drain: " + listed.key + " vanished before delete");
                    return true;
                }
                etag = head->etag;
            }

            const Removal outcome = op.remove(listed.key, *etag, Retry::standard());
            if (outcome == Removal::Removed)
                ++deleted;
            else
                warnings.push_back("decommission drain: " + listed.key + " delete outcome "
                                    + String(removalName(outcome)));
        }
        catch (...)
        {
            warnings.push_back("decommission drain: " + listed.key + " delete failed: "
                                + getCurrentExceptionMessage(/*with_stacktrace=*/false));
        }
        return true;
    }, Retry::standard());
    return deleted;
}

/// Delete one slot control object by an incarnation captured at the protocol-defined fence point. Slot
/// retirement is fail-closed: unlike the debris drains above, any non-`Removed` outcome or exception
/// stops the tail before it can touch the next control object.
bool deleteSlotObject(CasOperation & op, const String & key, const Etag & etag, std::vector<String> & warnings)
{
    try
    {
        const Removal outcome = op.remove(key, etag, Retry::standard());
        if (outcome == Removal::Removed)
            return true;

        warnings.push_back("slot delete failed: " + key + ": delete outcome "
                           + String(removalName(outcome)));
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
                                          const std::function<void()> & request_gc_round,
                                          const std::function<uint64_t()> & drain_now_fn,
                                          const std::function<void(uint64_t)> & drain_sleep_fn)
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

    /// Decommission is administrative and non-hot-path: it has no mount-lease fence of its own -- the
    /// exact-incarnation compare on every write is the safety mechanism -- so it opens its own
    /// always-admitted request engine rather than one of `Pool`'s fenced planes. The pre-impersonation
    /// cut below runs against the raw backend passed in, because `Pool::openForDecommission` has not
    /// yet wrapped it for instrumentation and no `Pool` exists yet to route through.
    CasRequests preflight_requests(backend, Fence::open());
    CasOperation preflight_op = preflight_requests.admit();

    /// Validate one required immutable ownership cut before impersonating the victim. The admin open
    /// performs its own fresh catalog observation for mount safety, but namespace selection below
    /// must reuse this exact pre-mutation decision rather than read a later authority set.
    const Layout catalog_layout(config.pool_prefix);
    const CasRefCatalog::Snapshot catalog_cut = CasRefCatalog::read(preflight_op, catalog_layout);
    catalog_cut.life_index.throwIfAmbiguous("CAS decommission");

    /// `drain_now_fn`/`drain_sleep_fn` below replace the clock and sleep the STANDALONE `requests`
    /// engine (opened further down) paces its own retries on, but `config.boot_ms_fn`/
    /// `config.retry_sleep_fn` -- the seams `Pool::openForDecommission` itself constructs its
    /// mount/farewell/GC planes with -- are distinct. Left unset, those planes retry on the real boot
    /// clock and a real sleep, and a caller that fakes only the standalone engine's clock ends up
    /// comparing it against an unrelated one at the mount lease's farewell bound (or, worse, against a
    /// clock that only the standalone engine's sleep advances: a retry loop on `mount_requests`/
    /// `gc_requests` bound to that frozen clock while actually sleeping for real would never see its
    /// own deadline elapse). Fold BOTH into `config` here, together, unless the caller already asked
    /// for a specific clock or sleep of its own -- installing only one of the two is exactly the
    /// half-fix that leaves the other seam retrying forever.
    if (drain_now_fn && drain_sleep_fn)
    {
        if (!config.boot_ms_fn)
            config.boot_ms_fn = drain_now_fn;
        if (!config.retry_sleep_fn)
            config.retry_sleep_fn = drain_sleep_fn;
    }

    config.event_sink = sink;
    PoolPtr admin = Pool::openForDecommission(std::move(backend), std::move(config), victim_srid);
    if (drain_now_fn && drain_sleep_fn)
    {
        /// Re-affirms the same values `config` above already installed on `mount_requests`/
        /// `farewell_requests`/`gc_requests` at construction, and additionally wires `ref_ledger`'s own
        /// retry sleep, which has no construction-time seam of its own. `sweepNamespace` below issues
        /// its deletes on `admin`'s own GC plane.
        admin->setCasRequestNowFnForTest(drain_now_fn);
        admin->setCasRetrySleepForTest(drain_sleep_fn);
    }

    /// A second engine over the pool's own (now instrumented) backend: `CasRequests` keeps its own
    /// shared_ptr to it, so `op` stays usable after `admin.reset()` retires the `Pool` below.
    CasRequests requests(admin->poolBackendPtr(), Fence::open());
    if (drain_now_fn && drain_sleep_fn)
    {
        requests.setNowFnForTest(drain_now_fn);
        requests.setSleepFnForTest(drain_sleep_fn);
    }
    CasOperation op = requests.admit();

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
        const CasRefCatalog::Snapshot current_catalog = CasRefCatalog::read(op, admin->layout());
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
            if (!op.head(admin->layout().refCkptKey(life), Retry::standard()).has_value())
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
        op.forEachListedKey(debris_prefix, [&](const ListedKey & listed)
        {
            if (const auto parsed = admin->layout().parseManifestKey(listed.key))
                groups.emplace(parsed->root_namespace.string(), parsed->ref.writer_epoch, parsed->ref.build_sequence);
            return true;
        }, Retry::standard());
        for (const auto & [ns_str, writer_epoch, build_sequence] : groups)
            report.manifest_debris_removed += sweepNamespace(
                *admin, RootNamespace(ns_str), BuildPrefix{writer_epoch, build_sequence}, &report.warnings);
    }

    /// Drain the victim's own `<pool_prefix>/staging/<srid>/` area. The live-mount staging helper uses
    /// an `IObjectStorage`, while this command intentionally works at the `Backend` layer, so the same
    /// prefix is listed and deleted directly. The claim fences the victim's writers during this sweep.
    report.staging_objects_removed += deleteListedPrefix(
        op, admin->poolConfig().pool_prefix + "/staging/" + victim_srid + "/", report.warnings);

    /// Drain the victim's mountpoint objects. These are loose, non-content-addressed files under
    /// `Layout::serverRootDataPrefix`; they have no writer epoch of their own, so the claim is what
    /// prevents a returning victim from racing this deletion.
    report.mountpoint_objects_removed += deleteListedPrefix(
        op, admin->layout().serverRootDataPrefix(victim_srid), report.warnings);

    /// The catalog, not physical debris, owns the slot-retirement decision. A terminal append only
    /// moves a row to `Removing`; GC must fold/prune/delete it before the member's ownership anchor can
    /// disappear. Capture one exact whole-catalog cut after every drain, then revalidate its incarnation
    /// and canonical value immediately before entering the retirement tail. The administrative claim
    /// fences the victim writer between those observations.
    std::optional<CasRefCatalog::Snapshot> retirement_catalog_cut;
    if (report.warnings.empty())
    {
        retirement_catalog_cut = CasRefCatalog::read(op, admin->layout());
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

    /// Retire the slot strictly last and only after a clean drain. Copy the layout before
    /// `admin.reset()`: graceful close destroys the `Pool`, while `op` (holding its own shared_ptr to
    /// the backend) remains usable to retire the slot objects afterwards.
    const Layout layout = admin->layout();
    if (report.warnings.empty())
    {
        const CasRefCatalog::Snapshot fresh_retirement_catalog = CasRefCatalog::read(op, admin->layout());
        if (!retirement_catalog_cut
            || fresh_retirement_catalog.etag != retirement_catalog_cut->etag
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

        /// Capture both the epoch value and its exact incarnation while the decommission claim still
        /// fences the victim. A successor can only bump this object after the farewell below releases
        /// the claim, so this incarnation is the epoch-side successor fence for the retirement tail.
        std::optional<Object> claimed_epoch;
        try
        {
            claimed_epoch = op.read(epoch_key, Retry::standard());
            if (!claimed_epoch)
                report.warnings.push_back("slot capture failed: " + epoch_key + " is absent under the admin claim");
        }
        catch (...)
        {
            report.warnings.push_back("slot capture failed: " + epoch_key + ": "
                                      + getCurrentExceptionMessage(/*with_stacktrace=*/false));
        }

        /// Graceful close stamps an already-expired lease and the watermark farewell
        /// (`min_active_build_sequence = UINT64_MAX`), making the slot `terminated` before its mutable control objects
        /// are removed and its owner anchor is tombstoned.
        admin.reset();

        /// Read the farewell immediately after `finishTeardown` wrote it. Its exact incarnation is the
        /// mount-side fence: deleting by this incarnation can remove only THIS decommission's farewell,
        /// not a successor reclaim. Validate the body against the epoch value captured under the claim
        /// so a successor that completed before this read is also recognized and left untouched.
        std::optional<Object> farewell_mount;
        try
        {
            farewell_mount = op.read(mount_key, Retry::standard());
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
                    && mount_value.min_active_build_sequence == std::numeric_limits<uint64_t>::max()
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
        /// incarnation yields `Mismatch` and the tail stops before touching epoch or owner. Epoch
        /// second: its under-claim incarnation similarly detects a successor allocation. Before
        /// touching owner, re-read both mutable objects: a same-UUID successor can recreate them after
        /// both deletes without rewriting the owner identity anchor. Mere presence proves that the slot
        /// is live again. Every delete must be explicitly confirmed as `Removed`, and the final owner
        /// tombstone rewrite must succeed against the exact incarnation read immediately before it.
        ///
        /// ACCEPTED RESIDUAL WINDOW (final review, not closed by this recheck): a same-UUID successor
        /// can still recreate epoch/mount in the narrow gap strictly AFTER this liveness recheck but
        /// BEFORE the owner CAS below reads its own incarnation -- the successor's owner anchor (same
        /// server_uuid, not yet retired) then gets tombstoned by this decommission run. The successor's
        /// live process is not deleted (only its owner anchor is marked retired), but a LATER restart of
        /// that same identity would refuse to reclaim it (claimOwnerOrThrow's tombstone guard). This is
        /// a narrow, low-probability window, deliberately not closed here: T5's owner-tombstone design
        /// (finding #9) intentionally stopped short of making concurrent decommission-vs-recreate
        /// airtight to the microsecond, since that was explicitly not the priority for this fix.
        report.slot_removed = false;
        if (captures_match && deleteSlotObject(op, mount_key, farewell_mount->etag, report.warnings)
            && deleteSlotObject(op, epoch_key, claimed_epoch->etag, report.warnings))
        {
            std::optional<Object> current_mount;
            std::optional<Object> current_epoch;
            bool liveness_recheck_succeeded = true;
            try
            {
                current_mount = op.read(mount_key, Retry::standard());
            }
            catch (...)
            {
                report.warnings.push_back("slot liveness recheck failed: " + mount_key + ": "
                                          + getCurrentExceptionMessage(/*with_stacktrace=*/false));
                liveness_recheck_succeeded = false;
            }
            try
            {
                current_epoch = op.read(epoch_key, Retry::standard());
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
                    if (const auto owner = op.read(owner_key, Retry::standard()))
                    {
                        OwnerObject tombstoned = decodeOwner(owner->bytes);
                        tombstoned.retired_at_ms = nowMs();
                        /// `op.replace` resolves an ambiguous attempt with a resolve read on its own:
                        /// a transient transport error here (or one whose response was simply lost)
                        /// must not be reported as a hard failure when the write actually landed.
                        /// Unchanged incarnation means the write never applied (legitimately retryable
                        /// within budget); matching bytes means this exact tombstone already landed
                        /// (`Committed`, not a failure); a genuine successor reclaim is `Conflict`;
                        /// `Refused` is the store's own definite answer (a denial, a malformed
                        /// request, an expired credential) and carries its own code and message,
                        /// which is worth more here than the generic retry advice below.
                        WriteResult result = op.replace(owner_key, encodeOwner(tombstoned), owner->etag, Retry::standard());
                        if (std::holds_alternative<Committed>(result))
                            report.slot_removed = true;
                        else if (std::holds_alternative<Conflict>(result))
                            report.warnings.push_back(
                                "slot tombstone failed: " + owner_key
                                + ": successor reclaimed the owner anchor before this decommission's tombstone write");
                        else if (const Refused * refused = std::get_if<Refused>(&result))
                            report.warnings.push_back(
                                "slot tombstone failed: " + owner_key + ": the store refused the write ("
                                + std::to_string(refused->store_error) + "): " + refused->message);
                        else
                            report.warnings.push_back(
                                "slot tombstone failed: " + owner_key
                                + ": tombstone write outcome could not be resolved (retry budget exhausted "
                                  "or the resolve read itself failed) -- rerun the command to retry");
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
