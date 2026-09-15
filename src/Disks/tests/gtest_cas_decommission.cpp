#include "cas_test_helpers.h"
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasDecommission.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h>
#include <algorithm>
#include <atomic>
#include <limits>
#include <stdexcept>
#include <tuple>

namespace DB::ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

using namespace DB;
using namespace DB::Cas;
using namespace DB::Cas::tests;

namespace
{

/// Open a store for the VICTIM srid over `backend` (the pool's future dead member).
PoolPtr openVictim(std::shared_ptr<InMemoryBackend> backend)
{
    return Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "victim"});
}

void drainCompletedNamespaceRemovals(const std::shared_ptr<InMemoryBackend> & backend)
{
    PoolConfig config{
        .pool_prefix = "p",
        .server_root_id = "gc",
        .gc_fold_threshold = 1,
        .gc_fold_max_defer_rounds = 0};
    auto store = Pool::open(backend, config);
    Gc gc(store, UInt128{991});
    ASSERT_FALSE(runRegularRoundReclaiming(gc).deferred);
    ASSERT_FALSE(runRegularRoundReclaiming(gc).deferred);
}

/// Fails a delete for one or two designated keys -- either by throwing (a transient backend hiccup) or
/// by returning a synthetic `Mismatch` (a "listed but raced" outcome) -- delegating every other key to
/// the base `InMemoryBackend` untouched. Drives the drain phases' per-object fail-close path
/// (`deleteListedPrefix`/`sweepNamespace`, `CasDecommission.cpp`/`CasOrphanManifestSweep.cpp`): a
/// failure on one listed object must record a warning and let the rest of the sweep proceed, never
/// abort the whole phase. Injects on the `remove` PRIMITIVE, not the legacy `deleteExact`, so it
/// intercepts a caller on either surface.
///
/// The thrown fault is a `Poco::TimeoutException`, the class the request engine classifies as a
/// transport failure and reissues (`Retry::standard()`); a `std::runtime_error` propagates immediately
/// and never exercises the retry path this test's own name claims to drive. `latch` keeps it armed
/// across every reissue of the SAME logical call, so the engine reaches its own retry deadline and
/// gives up rather than recovering on a later attempt -- a one-shot throw would be outlived by the
/// reissue and the delete would simply succeed.
class FailingDeleteBackend : public InMemoryBackend
{
public:
    void failWithThrow(const String & key) { throw_key = key; }
    void failWithTokenMismatch(const String & key) { mismatch_key = key; }
    void latch() { latched = true; }
    /// Clears every injected failure -- the resume half of a fail-then-retry test (Task 4).
    void disarm() { throw_key.clear(); mismatch_key.clear(); latched = false; }

    RawRemoval remove(const String & key, const String & expected_value, TransportAccess & access) override
    {
        if (key == throw_key)
        {
            if (!latched)
                throw_key.clear();
            throw Poco::TimeoutException("injected transient delete failure for " + key);
        }
        if (key == mismatch_key)
            return RawRemoval::Mismatch;
        return InMemoryBackend::remove(key, expected_value, access);
    }

private:
    String throw_key;
    String mismatch_key;
    bool latched = false;
};

/// Fails a read of one designated key a fixed number of times with a transient `Poco::TimeoutException`
/// -- the class the request engine classifies as a transport failure and reissues -- before delegating
/// to the base `InMemoryBackend`. Drives the owner-object read `Pool::openForDecommission` itself issues
/// on the OPEN plane (`claimOwnerOrThrow` -> `readOwnerObject`, CasServerRoot.cpp) through a bounded
/// number of paced retries DURING opening, before `decommissionPoolMember` gets a chance to install
/// anything on the already-open `Pool`.
class FlakyReadBackend : public InMemoryBackend
{
public:
    void failReadNTimes(const String & key, int times) { flaky_key = key; remaining = times; }

    std::optional<Raw> read(const String & key, DB::Cas::TransportAccess & access) override
    {
        if (key == flaky_key && remaining > 0)
        {
            --remaining;
            throw Poco::TimeoutException("injected transient read failure for " + key);
        }
        return InMemoryBackend::read(key, access);
    }

private:
    String flaky_key;
    int remaining = 0;
};

/// Replaces the durable catalog immediately after returning the first armed catalog read. This
/// distinguishes the immutable cut validated before decommission impersonation from a later mount
/// safety observation without assuming those two decisions share one GET.
class CatalogChangesAfterFirstReadBackend : public InMemoryBackend
{
public:
    void armCatalogReplacement(
        const String & key, RefCatalog replacement_, size_t completed_reads_before_replacement = 0)
    {
        catalog_key = key;
        replacement = std::move(replacement_);
        reads_to_skip = completed_reads_before_replacement;
        armed = true;
    }

    bool fired() const { return replacement_fired; }

    /// Injects on the `read` PRIMITIVE: every caller reaches this key through `CasOperation::read`,
    /// which funnels through here.
    std::optional<Raw> read(const String & key, TransportAccess & access) override
    {
        auto got = InMemoryBackend::read(key, access);
        if (!armed || replacement_fired || key != catalog_key)
            return got;
        if (reads_to_skip > 0)
        {
            --reads_to_skip;
            return got;
        }
        if (!got)
            throw std::runtime_error("catalog replacement fixture: catalog is absent");

        replacement_fired = true;
        const auto put = InMemoryBackend::write(key, encodeRefCatalog(replacement), got->value, access);
        if (!put.has_value())
            throw std::runtime_error("catalog replacement fixture: rewrite conflicted");
        return got;
    }

private:
    String catalog_key;
    RefCatalog replacement;
    size_t reads_to_skip = 0;
    bool armed = false;
    bool replacement_fired = false;
};

std::vector<std::tuple<String, String, Etag>> snapshotPrefixObjects(
    InMemoryBackend & backend, const String & prefix)
{
    std::vector<std::tuple<String, String, Etag>> objects;
    auto requests = openRequestsForTest(backend);
    auto op = requests.admit();
    String cursor;
    while (true)
    {
        const ListPage page = op.list(prefix, cursor, 1000, Retry::once());
        for (const ListedKey & listed : page.keys)
        {
            const auto got = op.read(listed.key, Retry::once());
            if (!got)
                throw std::runtime_error("prefix snapshot fixture: listed object disappeared");
            objects.emplace_back(listed.key, got->bytes, got->etag);
        }
        if (page.next_cursor.empty())
            return objects;
        cursor = page.next_cursor;
    }
}

/// Installs a same-UUID successor deterministically in the retirement tail's read/delete window.
/// Once armed, the backend recognizes the admin's clean farewell `putOverwrite`. On the next read of
/// either mutable control object it first captures the value that read observed, then bumps `epoch`
/// and reclaims `mount` with fresh tokens before returning the captured result. Thus the caller holds
/// exactly the stale token it would have obtained immediately before a concurrent restart reclaimed
/// the slot, without threads or sleeps.
class SuccessorReclaimAfterFarewellBackend : public InMemoryBackend
{
public:
    void armForSuccessorReclaim() { armed = true; }

    /// Injects on the `read` PRIMITIVE: the retirement tail's own reads of `mount_key`/`epoch_key`
    /// (`CasDecommission.cpp`) go through `CasOperation::read`, not the legacy `get`.
    std::optional<Raw> read(const String & key, TransportAccess & access) override
    {
        std::optional<Raw> result = InMemoryBackend::read(key, access);
        if (farewell_seen && !successor_injected && (key == mount_key || key == epoch_key))
            injectSuccessor(access);
        return result;
    }

    /// Injects on the `write` PRIMITIVE: `putOverwrite` is not one of the two verb-identity
    /// exceptions (`putIfAbsent`/`casPut`), so both the legacy caller and `CasOperation::replace`
    /// reach it here.
    std::expected<String, RawConflict> write(const String & key, const String & bytes,
                                             const std::optional<String> & expected_value, TransportAccess & access) override
    {
        const auto result = InMemoryBackend::write(key, bytes, expected_value, access);
        if (armed && result.has_value() && key == mount_key)
        {
            const MountLease mount = decodeMountLease(bytes);
            if (mount.min_active_build_sequence == std::numeric_limits<uint64_t>::max())
                farewell_seen = true;
        }
        return result;
    }

    bool successorInjected() const { return successor_injected; }
    const String & successorMountValue() const { return successor_mount_value; }
    const String & successorEpochValue() const { return successor_epoch_value; }
    const String & successorMountBytes() const { return successor_mount_bytes; }
    const String & successorEpochBytes() const { return successor_epoch_bytes; }

private:
    /// Every request here is issued on the PRIMITIVE `write`, using the `access` token the caller's
    /// own in-flight request already holds -- no `CasRequests`/`CasOperation` exists inside this
    /// reentrant hook to mint one. The captured `successor_*_value` fields are the raw wire values
    /// `write` returned, which the tests compare against a real incarnation's rendered value
    /// (`PersistedEtag::capture`) taken through their own `CasOperation`.
    void injectSuccessor(TransportAccess & access)
    {
        const auto epoch = InMemoryBackend::read(epoch_key, access);
        const auto mount = InMemoryBackend::read(mount_key, access);
        if (!epoch || !mount)
            throw std::runtime_error("successor-reclaim fixture: control object disappeared before reclaim");

        ServerEpoch epoch_value = decodeServerEpoch(epoch->bytes);
        const uint64_t successor_writer_epoch = epoch_value.next_writer_epoch;
        ++epoch_value.next_writer_epoch;
        successor_epoch_bytes = encodeServerEpoch(epoch_value);
        const auto epoch_written = InMemoryBackend::write(epoch_key, successor_epoch_bytes, epoch->value, access);
        if (!epoch_written)
            throw std::runtime_error("successor-reclaim fixture: epoch bump conflicted");
        successor_epoch_value = *epoch_written;

        MountLease mount_value = decodeMountLease(mount->bytes);
        mount_value.writer_epoch = successor_writer_epoch;
        ++mount_value.seq;
        ++mount_value.started_at_ms;
        mount_value.expires_at_ms = mount_value.started_at_ms + 30'000;
        mount_value.min_active_build_sequence = 0;
        mount_value.gc_fenced = false;
        successor_mount_bytes = encodeMountLease(mount_value);
        const auto mount_written = InMemoryBackend::write(mount_key, successor_mount_bytes, mount->value, access);
        if (!mount_written)
            throw std::runtime_error("successor-reclaim fixture: mount reclaim conflicted");
        successor_mount_value = *mount_written;
        successor_injected = true;
    }

    inline static const String mount_key = "p/gc/server-roots/victim/mount";
    inline static const String epoch_key = "p/gc/server-roots/victim/epoch";
    bool armed = false;
    bool farewell_seen = false;
    bool successor_injected = false;
    String successor_mount_value;
    String successor_epoch_value;
    String successor_mount_bytes;
    String successor_epoch_bytes;
};

/// Recreates the mutable slot objects immediately after decommission successfully deletes `epoch`.
/// This models a same-UUID successor starting in the final retirement window: `owner` remains the
/// unchanged identity anchor, while the successor legitimately creates a fresh `epoch` and `mount`.
class SuccessorReclaimAfterEpochDeleteBackend : public InMemoryBackend
{
public:
    void armForSuccessorReclaim() { armed = true; }

    /// Injects on the `remove` PRIMITIVE: `deleteSlotObject`'s epoch delete (`CasDecommission.cpp`)
    /// goes through `CasOperation::remove`, not the legacy `deleteExact`.
    RawRemoval remove(const String & key, const String & expected_value, TransportAccess & access) override
    {
        const RawRemoval result = InMemoryBackend::remove(key, expected_value, access);
        if (armed && !successor_injected && key == epoch_key && result == RawRemoval::Removed)
            injectSuccessor(access);
        return result;
    }

    bool successorInjected() const { return successor_injected; }
    uint64_t ownerRewriteAttempts() const { return owner_rewrite_attempts; }
    const String & successorMountValue() const { return successor_mount_value; }
    const String & successorEpochValue() const { return successor_epoch_value; }
    const String & successorMountBytes() const { return successor_mount_bytes; }
    const String & successorEpochBytes() const { return successor_epoch_bytes; }

private:
    /// Counts on the `write` PRIMITIVE: the owner tombstone write this test asserts is never
    /// attempted (`CasDecommission.cpp`'s `op.replace`) reaches the store through here, not through
    /// the legacy `putOverwrite`. Guarded on `expected_value`: the primitive also sees the owner
    /// anchor's own CREATE during this fixture's `openVictim`, which `putOverwrite` (a conditional
    /// REPLACE only) never did -- counting it would start this counter at 1 before the interesting
    /// part of the test begins.
    std::expected<String, RawConflict> write(const String & key, const String & bytes,
                                             const std::optional<String> & expected_value, TransportAccess & access) override
    {
        if (key == owner_key && expected_value)
            ++owner_rewrite_attempts;
        return InMemoryBackend::write(key, bytes, expected_value, access);
    }

    /// On the `write` PRIMITIVE, using the caller's own in-flight `access`: this hook is reentrant
    /// (called from inside another primitive override on the same object), so it reaches the store
    /// directly through `InMemoryBackend::write` rather than admitting a fresh request of its own.
    void injectSuccessor(TransportAccess & access)
    {
        successor_epoch_bytes = encodeServerEpoch(ServerEpoch{.next_writer_epoch = 102});
        const auto epoch_written = InMemoryBackend::write(epoch_key, successor_epoch_bytes, std::nullopt, access);
        if (!epoch_written)
            throw std::runtime_error("late-successor fixture: epoch recreation conflicted");
        successor_epoch_value = *epoch_written;

        successor_mount_bytes = encodeMountLease(MountLease{
            .server_uuid = UInt128(0x1234),
            .writer_epoch = 101,
            .hostname = "successor",
            .pid = 42,
            .started_at_ms = 1'000,
            .seq = 1,
            .expires_at_ms = 31'000,
            .min_active_build_sequence = 0,
        });
        const auto mount_written = InMemoryBackend::write(mount_key, successor_mount_bytes, std::nullopt, access);
        if (!mount_written)
            throw std::runtime_error("late-successor fixture: mount recreation conflicted");
        successor_mount_value = *mount_written;
        successor_injected = true;
    }

    inline static const String mount_key = "p/gc/server-roots/victim/mount";
    inline static const String epoch_key = "p/gc/server-roots/victim/epoch";
    inline static const String owner_key = "p/gc/server-roots/victim/owner";
    bool armed = false;
    bool successor_injected = false;
    uint64_t owner_rewrite_attempts = 0;
    String successor_mount_value;
    String successor_epoch_value;
    String successor_mount_bytes;
    String successor_epoch_bytes;
};

/// Rewrites the owner anchor after decommission reads it but before its conditional tombstone write.
/// Returning the captured result gives decommission a stale owner token, deterministically modeling
/// the successor race without threads or sleeps.
class SuccessorOwnerRewriteBeforeTombstoneBackend : public InMemoryBackend
{
public:
    void armForSuccessorRewrite() { armed = true; }

    /// Injects on the `read` PRIMITIVE: the tombstone tail's owner read (`CasDecommission.cpp`)
    /// goes through `CasOperation::read`, not the legacy `get`.
    std::optional<Raw> read(const String & key, TransportAccess & access) override
    {
        std::optional<Raw> result = InMemoryBackend::read(key, access);
        if (armed && epoch_deleted && !successor_injected && key == owner_key && result)
        {
            successor_owner_bytes = encodeOwner(OwnerObject{
                .server_uuid = decodeOwner(result->bytes).server_uuid,
                .retired_at_ms = std::nullopt,
            });
            const auto put = InMemoryBackend::write(owner_key, successor_owner_bytes, result->value, access);
            if (!put.has_value())
                throw std::runtime_error("owner-successor fixture: owner rewrite conflicted");
            successor_owner_value = *put;
            successor_injected = true;
        }
        return result;
    }

    /// Injects on the `remove` PRIMITIVE: `deleteSlotObject`'s epoch delete (`CasDecommission.cpp`)
    /// goes through `CasOperation::remove`, not the legacy `deleteExact`.
    RawRemoval remove(const String & key, const String & expected_value, TransportAccess & access) override
    {
        const RawRemoval result = InMemoryBackend::remove(key, expected_value, access);
        if (armed && key == epoch_key && result == RawRemoval::Removed)
            epoch_deleted = true;
        return result;
    }

    bool successorInjected() const { return successor_injected; }
    const String & successorOwnerValue() const { return successor_owner_value; }
    const String & successorOwnerBytes() const { return successor_owner_bytes; }

private:
    inline static const String epoch_key = "p/gc/server-roots/victim/epoch";
    inline static const String owner_key = "p/gc/server-roots/victim/owner";
    bool armed = false;
    bool epoch_deleted = false;
    bool successor_injected = false;
    String successor_owner_value;
    String successor_owner_bytes;
};


/// Seed one victim table with `committed` committed refs and `precommits` dangling precommit bindings,
/// via the raw ref-log seeding helpers (fixture idiom of e.g. `gtest_cas_gc_fold.cpp`: `writeManifestRaw`
/// + `publishCommittedTransition`/`addPrecommitTransition` against `victim`'s own backend/layout) -- this
/// fixture only needs the ref-table SHAPE `dropNamespace` erases, not a real build. Precommit bindings
/// are seeded at an artificially high `writer_epoch` so the writer's own stale-precommit sweep (armed
/// unconditionally by this table's recovery, unrelated to decommission -- spec §Clean Up Old Precommits)
/// never reclaims them, in its OWN separate transaction, ahead of `dropNamespace`'s removal.
void makeTableWithRefs(Pool & victim, const String & ns_str, uint64_t committed, uint64_t precommits)
{
    const RootNamespace ns(ns_str);
    Backend & backend = *victim.poolBackendPtr();
    const Layout & layout = victim.layout();

    /// A throwaway open-fence operation, for the two `CasRefCatalog` calls below only: this fixture
    /// writes everything else directly against `backend` via the raw-write helpers, unrelated to any
    /// mount fence.
    CasRequests requests(victim.poolBackendPtr(), Fence::open());
    CasOperation op = requests.admit();

    /// Final physical ids are pool-wide. The generic raw-write helper intentionally uses one shared
    /// transition sentinel, so this multi-namespace fixture admits a distinct deterministic test life
    /// before invoking it; the helper then resolves and preserves that existing catalog identity.
    const CasRefCatalog::Snapshot catalog = CasRefCatalog::read(op, layout);
    const auto existing = std::find_if(catalog.catalog.entries.begin(), catalog.catalog.entries.end(),
        [&](const CatalogEntry & entry) { return entry.ns.string() == ns.string(); });
    if (existing == catalog.catalog.entries.end())
    {
        static std::atomic<uint64_t> next_test_life{1000};
        CatalogEntry entry;
        entry.ns = ns;
        entry.state = NsState::Live;
        entry.incarnation = UInt128{next_test_life.fetch_add(1)};
        CasRefCatalog::casAdmitEntry(op, layout, 1, entry);
    }

    uint64_t last_ref_sequence = 0;
    for (uint64_t i = 0; i < committed; ++i)
    {
        const ManifestRef ref{.writer_epoch = 1, .build_sequence = i + 1, .manifest_ordinal = 1};
        writeManifestRaw(backend, layout, ns, ref, {});
        last_ref_sequence = publishCommittedTransition(backend, layout, ns, "committed_" + std::to_string(i), std::nullopt, ref);
    }
    for (uint64_t i = 0; i < precommits; ++i)
    {
        const ManifestRef ref{.writer_epoch = 999999, .build_sequence = i + 1, .manifest_ordinal = 1};
        writeManifestRaw(backend, layout, ns, ref, {});
        last_ref_sequence = addPrecommitTransition(backend, layout, ns, UInt128(1), "precommit_" + std::to_string(i), std::nullopt, ref);
    }

    /// Semantic transition helpers already publish `_ckpt`; replace their final checkpoint through
    /// the exact token-CAS fixture helper to make this fixture's complete intended state explicit.
    replaceRecoverableCkptForRawFixture(backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = last_ref_sequence ? std::optional<RefTxnId>{RefTxnId{1, last_ref_sequence}} : std::nullopt,
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    /// Self-checking: `listRefs` must observe exactly `committed` committed refs before returning.
    ASSERT_EQ(victim.listRefs(ns).size(), committed);
}

/// Pre-precommit manifest debris: a staged manifest body under `ns_str`, at the store's own
/// `writer_epoch`, named by NO owner event -- a build the writer staged and never finished (fixture
/// idiom of `gtest_cas_orphan_manifest_sweep.cpp`'s `EligibleAndUnownedIsDeleted`). `build_sequence = 99`
/// is picked well clear of `makeTableWithRefs`'s own committed/precommit build sequences so it can never
/// collide with a real owned manifest key. Returns the seeded body's `ManifestId` so a caller can target
/// it (e.g. its exact object key) for further fixture setup.
ManifestId seedOrphanManifestBody(Pool & victim, const String & ns_str)
{
    const RootNamespace ns(ns_str);
    const ManifestRef ref{.writer_epoch = victim.writerEpoch(), .build_sequence = 99, .manifest_ordinal = 1};
    const ManifestId id = writeManifestRaw(*victim.poolBackendPtr(), victim.layout(), ns, ref, {});
    /// EXPECT, not ASSERT: this function returns a value now, and ASSERT_* expands to a bare `return;`
    /// -- invalid in a non-void function.
    OperationForTest op(*victim.poolBackendPtr());
    EXPECT_TRUE((*op).head(victim.layout().manifestKey(id), Retry::once()).has_value());
    return id;
}

/// THE MANIFEST-DEBRIS DRAIN NO LONGER DELETES, AND THE FIXTURES BELOW SAY SO RATHER THAN WORKING
/// AROUND IT. The drain goes through `sweepNamespace`, which is subject to the §6 deletion premise: a
/// manifest of an epoch-`E` build is deletable only once the namespace's sealed fold cursor sits in an
/// epoch STRICTLY above `E`. Every object in these fixtures -- the table's ref stream, the debris, and
/// the removal transaction decommission itself appends -- lives in ONE writer epoch, and a single-epoch
/// pool cannot satisfy that: any cursor high enough to clear the debris's epoch also sits above the
/// removal record, which would strip the tail-removal protection off the table's real manifests. The
/// two facts are mutually exclusive here, so there is no honest seeding that restores the deletions;
/// the tests assert retention, and the drain's reclaim path returns with registers R2/R3 (Stage B).

}

TEST(CASDecommission, RefusesLiveMember)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto victim = openVictim(backend);   /// keeps its mount lease unexpired — the member is alive

    expectThrowsCode(ErrorCodes::ABORTED, [&]
    {
        Pool::openForDecommission(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");
    });
}

TEST(CASDecommission, ClaimsDeadMemberAndBumpsEpoch)
{
    auto backend = std::make_shared<InMemoryBackend>();
    uint64_t victim_epoch = 0;
    {
        auto victim = openVictim(backend);
        victim_epoch = victim->writerEpoch();
    }   /// graceful close: lease stamped already-expired + farewell — the slot is claimable

    auto admin = Pool::openForDecommission(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");
    ASSERT_TRUE(admin != nullptr);
    EXPECT_GT(admin->writerEpoch(), victim_epoch);
    /// The admin store IS the victim server root now (impersonation).
    EXPECT_EQ(admin->poolConfig().server_root_id, "victim");
}

TEST(CASDecommission, AlwaysRenewsAdminClaimEvenWhenHostDiskIsObserveOnly)
{
    auto backend = std::make_shared<InMemoryBackend>();
    {
        auto victim = openVictim(backend);
    }   /// graceful close: lease stamped already-expired + farewell — the slot is claimable

    /// The calling (host) disk may be observe-only, i.e. its own PoolConfig carries
    /// background_watermark = false. The decommission admin claim must renew its lease
    /// regardless -- a long drain must not expire midway just because the host mount doesn't
    /// run a background renewer for its OWN mount.
    auto admin = Pool::openForDecommission(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin", .background_watermark = false}, "victim");
    ASSERT_TRUE(admin != nullptr);
    EXPECT_TRUE(admin->poolConfig().background_watermark);
}

TEST(CASDecommission, RefusesUnknownMember)
{
    auto backend = std::make_shared<InMemoryBackend>();
    expectThrowsCode(ErrorCodes::BAD_ARGUMENTS, [&]
    {
        Pool::openForDecommission(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "never_existed");
    });
}

TEST(CASDecommission, SecondConcurrentDecommissionRefused)
{
    auto backend = std::make_shared<InMemoryBackend>();
    { auto victim = openVictim(backend); }

    auto first = Pool::openForDecommission(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");
    expectThrowsCode(ErrorCodes::ABORTED, [&]
    {
        Pool::openForDecommission(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin2"}, "victim");
    });
}

TEST(CASDecommission, DuplicateLifeIdRefusesBeforeAnyNamespaceOrSlotMutation)
{
    auto backend = std::make_shared<InMemoryBackend>();
    { auto victim = openVictim(backend); }
    const Layout layout("p");
    RefCatalog catalog;
    catalog.entries = {
        CatalogEntry{.ns = RootNamespace{"victim/a"}, .state = NsState::Live, .incarnation = UInt128{77}},
        CatalogEntry{
            .ns = RootNamespace{"victim/b"},
            .state = NsState::Removing,
            .incarnation = UInt128{77},
            .removal_started_round = 1},
    };
    OperationForTest raw_op(*backend);
    const auto empty_catalog = (*raw_op).read(layout.refCatalogKey(), Retry::once());
    ASSERT_TRUE(empty_catalog);
    ASSERT_TRUE(std::holds_alternative<Committed>((*raw_op).replace(
        layout.refCatalogKey(), encodeRefCatalog(catalog), empty_catalog->etag, Retry::once())));
    const auto owner_before = (*raw_op).read(layout.ownerKey("victim"), Retry::once());
    const auto epoch_before = (*raw_op).read(layout.epochKey("victim"), Retry::once());
    const auto mount_before = (*raw_op).read(layout.mountKey("victim"), Retry::once());
    ASSERT_TRUE(owner_before);
    ASSERT_TRUE(epoch_before);
    ASSERT_TRUE(mount_before);

    EXPECT_THROW(decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim"), DB::Exception);
    const auto owner_after = (*raw_op).read(layout.ownerKey("victim"), Retry::once());
    const auto epoch_after = (*raw_op).read(layout.epochKey("victim"), Retry::once());
    const auto mount_after = (*raw_op).read(layout.mountKey("victim"), Retry::once());
    ASSERT_TRUE(owner_after);
    ASSERT_TRUE(epoch_after);
    ASSERT_TRUE(mount_after);
    EXPECT_EQ(owner_after->bytes, owner_before->bytes);
    EXPECT_EQ(owner_after->etag, owner_before->etag);
    EXPECT_EQ(epoch_after->bytes, epoch_before->bytes);
    EXPECT_EQ(epoch_after->etag, epoch_before->etag);
    EXPECT_EQ(mount_after->bytes, mount_before->bytes);
    EXPECT_EQ(mount_after->etag, mount_before->etag);
}

TEST(CASDecommission, CatalogCutIsValidatedBeforeImpersonationAndReusedForSelection)
{
    auto backend = std::make_shared<CatalogChangesAfterFirstReadBackend>();
    { auto victim = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "victim"}); }
    const Layout layout("p");

    RefCatalog ambiguous;
    ambiguous.entries = {
        CatalogEntry{.ns = RootNamespace{"other/a"}, .state = NsState::Live, .incarnation = UInt128{77}},
        CatalogEntry{
            .ns = RootNamespace{"other/b"},
            .state = NsState::Removing,
            .incarnation = UInt128{77},
            .removal_started_round = 1},
    };

    OperationForTest raw_op(*backend);
    const auto owner_before = (*raw_op).read(layout.ownerKey("victim"), Retry::once());
    const auto epoch_before = (*raw_op).read(layout.epochKey("victim"), Retry::once());
    const auto mount_before = (*raw_op).read(layout.mountKey("victim"), Retry::once());
    ASSERT_TRUE(owner_before);
    ASSERT_TRUE(epoch_before);
    ASSERT_TRUE(mount_before);
    backend->armCatalogReplacement(layout.refCatalogKey(), std::move(ambiguous));

    EXPECT_THROW(decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim"), DB::Exception);
    ASSERT_TRUE(backend->fired());

    const auto owner_after = (*raw_op).read(layout.ownerKey("victim"), Retry::once());
    const auto epoch_after = (*raw_op).read(layout.epochKey("victim"), Retry::once());
    const auto mount_after = (*raw_op).read(layout.mountKey("victim"), Retry::once());
    ASSERT_TRUE(owner_after);
    ASSERT_TRUE(epoch_after);
    ASSERT_TRUE(mount_after);
    EXPECT_EQ(owner_after->bytes, owner_before->bytes);
    EXPECT_EQ(owner_after->etag, owner_before->etag);
    EXPECT_EQ(epoch_after->bytes, epoch_before->bytes);
    EXPECT_EQ(epoch_after->etag, epoch_before->etag);
    EXPECT_EQ(mount_after->bytes, mount_before->bytes);
    EXPECT_EQ(mount_after->etag, mount_before->etag);
}

TEST(CASDecommission, NamespaceSelectionUsesThePreImpersonationCut)
{
    auto backend = std::make_shared<CatalogChangesAfterFirstReadBackend>();
    { auto victim = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "victim"}); }
    const Layout layout("p");

    RefCatalog later;
    later.entries = {
        CatalogEntry{.ns = RootNamespace{"victim/late"}, .state = NsState::Live, .incarnation = UInt128{88}},
    };
    backend->armCatalogReplacement(layout.refCatalogKey(), std::move(later));

    const DecommissionReport report = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");
    ASSERT_TRUE(backend->fired());
    EXPECT_EQ(report.namespaces_removed, 0u)
        << "a namespace visible only to mount safety's later observation is outside the validated cut";
    EXPECT_EQ(report.namespaces_already_removed, 0u);
}

TEST(CASDecommission, SameNameRebirthAfterTheCutIsRefusedWithoutTouchingTheNewLife)
{
    auto backend = std::make_shared<CatalogChangesAfterFirstReadBackend>();
    { auto victim = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "victim"}); }
    const Layout layout("p");
    const RootNamespace ns{"victim/same"};
    const NamespaceLifeId old_life = NamespaceLifeId::fromCatalogEntry(ns, UInt128{70});
    const NamespaceLifeId new_life = NamespaceLifeId::fromCatalogEntry(ns, UInt128{71});

    RefCatalog old_catalog;
    old_catalog.entries = {
        CatalogEntry{.ns = ns, .state = NsState::Live, .incarnation = old_life.incarnation},
    };
    OperationForTest raw_op(*backend);
    const auto empty_catalog = (*raw_op).read(layout.refCatalogKey(), Retry::once());
    ASSERT_TRUE(empty_catalog);
    ASSERT_TRUE(std::holds_alternative<Committed>((*raw_op).replace(
        layout.refCatalogKey(), encodeRefCatalog(old_catalog), empty_catalog->etag, Retry::once())));

    RefLogTxn new_birth;
    new_birth.ns = ns.string();
    new_birth.txn_id = RefTxnId{1, 1};
    new_birth.ops = {namespaceBirthOp()};
    ASSERT_TRUE(std::holds_alternative<Committed>((*raw_op).create(
        layout.refLogKey(new_life, new_birth.txn_id),
        sealObject(FormatId::RefLog, encodeRefLogTxn(new_birth)), Retry::once())));
    RefLogTxn new_seal;
    new_seal.ns = ns.string();
    new_seal.txn_id = RefTxnId{1, 2};
    new_seal.ops = {epochSealOp()};
    ASSERT_TRUE(std::holds_alternative<Committed>((*raw_op).create(
        layout.refLogKey(new_life, new_seal.txn_id),
        sealObject(FormatId::RefLog, encodeRefLogTxn(new_seal)), Retry::once())));
    const auto new_life_before = snapshotPrefixObjects(*backend, layout.namespaceStreamPrefix(new_life));

    RefCatalog replacement;
    replacement.entries = {
        CatalogEntry{.ns = ns, .state = NsState::Live, .incarnation = new_life.incarnation},
    };
    /// Read 1 captures the immutable selection cut. Read 2 is mount safety; replace immediately
    /// after returning that old observation, so the name-only call is the first consumer of the
    /// same-name new incarnation.
    backend->armCatalogReplacement(
        layout.refCatalogKey(), std::move(replacement), /*completed_reads_before_replacement=*/1);

    String refusal;
    try
    {
        (void)decommissionPoolMember(
            backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");
    }
    catch (const DB::Exception & e)
    {
        refusal = e.message();
    }
    EXPECT_NE(refusal.find("changed incarnation after the validated catalog cut"), String::npos)
        << refusal;
    ASSERT_TRUE(backend->fired());
    EXPECT_EQ(snapshotPrefixObjects(*backend, layout.namespaceStreamPrefix(new_life)), new_life_before)
        << "decommission must not append a removal transaction to the post-cut incarnation";
}

TEST(CASDecommission, VictimNameMatchesOneCanonicalPathComponent)
{
    auto backend = std::make_shared<InMemoryBackend>();
    { auto victim = openVictim(backend); }

    const RootNamespace neighbor_ns{"victim2/db/t1"};
    {
        auto neighbor = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "victim2"});
        makeTableWithRefs(*neighbor, neighbor_ns.string(), /*committed=*/1, /*precommits=*/0);
    }

    const auto report = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");
    EXPECT_EQ(report.namespaces_removed, 0u);

    auto neighbor = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "victim2"});
    EXPECT_EQ(neighbor->listRefs(neighbor_ns).size(), 1u)
        << "decommissioning victim must not select victim2 by raw string prefix";
}

TEST(CASDecommission, ErasesAllVictimNamespaces)
{
    auto backend = std::make_shared<InMemoryBackend>();
    {
        auto victim = openVictim(backend);
        /// Two tables: ns "victim/db/t1" with 2 committed refs, ns "victim/db/t2" with 1 committed
        /// ref + 1 stale precommit (fixture idiom of gtest_cas_ref_writer.cpp).
        makeTableWithRefs(*victim, "victim/db/t1", /*committed=*/2, /*precommits=*/0);
        makeTableWithRefs(*victim, "victim/db/t2", /*committed=*/1, /*precommits=*/1);
    }

    const auto report = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");

    EXPECT_EQ(report.srid, "victim");
    EXPECT_EQ(report.namespaces_removed, 2u);
    EXPECT_EQ(report.namespaces_already_removed, 0u);
    EXPECT_EQ(report.committed_refs_removed, 3u);
    EXPECT_EQ(report.precommits_removed, 1u);
    EXPECT_EQ(report.edge_deltas_emitted, 4u);

    /// Terminal publication is writer work; exact catalog-row deletion remains GC work. The first
    /// command therefore keeps the slot as an ownership anchor, and a retry may retire it only after
    /// GC's next invocation drains the completed `Removing` rows.
    EXPECT_FALSE(report.warnings.empty());
    EXPECT_FALSE(report.slot_removed);
    drainCompletedNamespaceRemovals(backend);
    const auto retired = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin2"}, "victim");
    EXPECT_TRUE(retired.warnings.empty());
    EXPECT_TRUE(retired.slot_removed);
}

/// Task 2 review finding 1: `makeTableWithRefs`'s precommit seed uses an artificially high
/// `writer_epoch` (999999) specifically to dodge the writer's OWN stale-precommit sweep -- which
/// means it never exercised the path a REAL victim precommit takes. A genuine writer stamps
/// `manifest_ref.writer_epoch` from its OWN `liveWriterEpoch()` at precommit time
/// (`PartWriteTxn::precommitAdd`, CasPool.cpp:2087), i.e. the victim's era -- always LOWER than the admin
/// mount's freshly-minted epoch (`openForDecommission` always bumps strictly higher). `appendRefOps`
/// hoists `maybeSweepStalePrecommits` at its top (CasPool.cpp:1716), so without the
/// `skip_stale_precommit_sweep` fix that sweep would reclaim this realistic-epoch precommit in its
/// OWN transaction before `dropNamespace`'s removal transaction ever counts it, leaving
/// `precommits_removed` at 0 for exactly the case that matters.
TEST(CASDecommission, CountsRealisticEpochPrecommit)
{
    auto backend = std::make_shared<InMemoryBackend>();
    uint64_t victim_epoch = 0;
    {
        auto victim = openVictim(backend);
        victim_epoch = victim->writerEpoch();
        makeTableWithRefs(*victim, "victim/db/t1", /*committed=*/1, /*precommits=*/0);

        const RootNamespace ns("victim/db/t1");
        /// `build_sequence = 2`: distinct from `makeTableWithRefs`'s committed ref (`build_sequence = 1`)
        /// -- a REAL build's `ManifestRef` is unique per build, and a colliding one would trip the ref
        /// state machine's "manifest already has a conflicting owner" guard.
        const ManifestRef ref{.writer_epoch = victim_epoch, .build_sequence = 2, .manifest_ordinal = 1};
        writeManifestRaw(*victim->poolBackendPtr(), victim->layout(), ns, ref, {});
        addPrecommitTransition(*victim->poolBackendPtr(), victim->layout(), ns, UInt128(1), "precommit_0", std::nullopt, ref);
    }

    const auto report = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");

    EXPECT_EQ(report.namespaces_removed, 1u);
    EXPECT_EQ(report.committed_refs_removed, 1u);
    EXPECT_EQ(report.precommits_removed, 1u);
    EXPECT_EQ(report.edge_deltas_emitted, 2u);
}

/// Task 2 review finding 2: the `member_decommission` begin/namespace_removed/end events
/// (CasDecommission.cpp) had no assertion at all. Wire a capturing sink (the `gtest_cas_event_log.cpp`
/// idiom) into `decommissionPoolMember` and check the emitted sequence and its per-namespace detail.
TEST(CASDecommission, EmitsMemberDecommissionEvents)
{
    auto backend = std::make_shared<InMemoryBackend>();
    {
        auto victim = openVictim(backend);
        makeTableWithRefs(*victim, "victim/db/t1", /*committed=*/1, /*precommits=*/0);
    }

    std::vector<CasEvent> seen;
    (void)decommissionPoolMember(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim",
        [&](const CasEvent & e) { seen.push_back(e); });

    std::vector<CasEvent> member_events;
    for (const auto & e : seen)
        if (e.type == CasEventType::MemberDecommission)
            member_events.push_back(e);

    ASSERT_EQ(member_events.size(), 3u);
    EXPECT_EQ(member_events[0].outcome, "begin");
    EXPECT_EQ(member_events[1].outcome, "namespace_removed");
    EXPECT_EQ(member_events[1].detail.at("namespace"), "victim/db/t1");
    EXPECT_EQ(member_events[1].detail.at("committed"), "1");
    EXPECT_EQ(member_events[1].detail.at("precommits"), "0");
    EXPECT_EQ(member_events[2].outcome, "end");
    EXPECT_EQ(member_events[2].detail.at("namespaces_removed"), "1");
}

/// Task 3: the manifest-debris / staging / roots drain phases fill their three `DecommissionReport`
/// counters and leave nothing of the victim behind under `staging/` or `roots/`.
TEST(CASDecommission, DrainsDebrisStagingAndRoots)
{
    auto backend = std::make_shared<InMemoryBackend>();
    {
        auto victim = openVictim(backend);
        makeTableWithRefs(*victim, "victim/db/t1", 1, 0);
        seedOrphanManifestBody(*victim, "victim/db/t1");
    }
    /// Foreign staging + mountpoint objects, written raw (no writer machinery needed): the victim's
    /// writers are fenced by the claim before decommission ever gets here, so these are ordinary debris,
    /// not a live in-flight write.
    {
        OperationForTest seed_op(*backend);
        (*seed_op).create("p/staging/victim/upload1.tmp", "x", Retry::once());
        (*seed_op).create("p/staging/victim/upload2.tmp", "x", Retry::once());
        (*seed_op).create("p/roots/victim/clickhouse_access_check_abc", "x", Retry::once());
    }

    const auto report = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");

    /// The staging and mountpoint phases are unchanged and still drain completely. The manifest-debris
    /// phase retains under the §6 premise (see the note on the helpers above) and reports why, which is
    /// what keeps the slot; `RetainsDebrisWhoseEpochSealIsUnconsumed` is that outcome's own test.
    EXPECT_EQ(report.manifest_debris_removed, 0u);
    EXPECT_EQ(report.staging_objects_removed, 2u);
    EXPECT_EQ(report.mountpoint_objects_removed, 1u);
    EXPECT_FALSE(report.warnings.empty())
        << "the retained debris is reported, so the incomplete drain is visible";

    /// Nothing of the victim remains under staging/ or roots/ (scoped LISTs are empty). Those two phases
    /// run to completion even though the debris phase retained -- the drain is per-phase, not all-or-nothing.
    OperationForTest raw_op(*backend);
    EXPECT_TRUE((*raw_op).list("p/staging/victim/", "", 10, Retry::once()).keys.empty());
    EXPECT_TRUE((*raw_op).list("p/roots/victim/", "", 10, Retry::once()).keys.empty());
}

/// The §6 deletion premise applies to the decommission drain too, and this pins what that COSTS. With no
/// sealed fold cursor for the victim's namespace — the state of a pool whose GC has not folded past the
/// victim's closed epoch — the drain cannot show the debris is unreferenced, so it RETAINS it, says why
/// in `warnings`, and therefore keeps the slot for a later re-run. Delay, not damage: the objects are
/// untouched and a re-run after GC catches up drains them (`DrainsDebrisStagingAndRoots`).
///
/// This is the visible edge of a real Stage-A limitation, not a test-only artifact: debris under a
/// namespace GC never folds — the pure pre-precommit orphan, whose whole point is that no ref record was
/// ever appended for it — has no cursor to consume any seal, so the premise retains it indefinitely.
/// Reclaiming it needs the sweep's own rework (registers R2/R3, Stage B), which is why the premise ships
/// as the safety floor and not as the reclaim policy.
TEST(CASDecommission, RetainsDebrisWhoseEpochSealIsUnconsumed)
{
    auto backend = std::make_shared<InMemoryBackend>();
    String debris_key;
    {
        auto victim = openVictim(backend);
        makeTableWithRefs(*victim, "victim/db/t1", 1, 0);
        const ManifestId debris_id = seedOrphanManifestBody(*victim, "victim/db/t1");
        debris_key = victim->layout().manifestKey(debris_id);
        /// Deliberately NO `seedFoldedPastVictimEpoch` here — that absence is the subject.
    }

    const auto report = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");

    EXPECT_EQ(report.manifest_debris_removed, 0u);
    OperationForTest raw_op(*backend);
    EXPECT_TRUE((*raw_op).head(debris_key, Retry::once()).has_value())
        << "the body is retained untouched, not deleted and not corrupted";
    ASSERT_FALSE(report.warnings.empty())
        << "a retained manifest is a visible decision -- the operator must be able to see why the drain "
           "did not complete";
    bool named = false;
    for (const String & w : report.warnings)
        if (w.find(debris_key) != String::npos && w.find("seal") != String::npos)
            named = true;
    EXPECT_TRUE(named) << "the warning names the object and the premise that retained it";
    EXPECT_FALSE(report.slot_removed)
        << "an incomplete drain keeps the slot as the resume anchor, exactly as a per-key failure does";
}

/// Task 3 fail-close nuance (spec §core "Fail-close"): a per-object failure in the staging/roots drain
/// -- a thrown exception (a transient hiccup) or a `TokenMismatch` outcome (a "listed but raced" miss)
/// -- must record a warning and let the rest of the sweep proceed, never abort the whole phase or the
/// whole command. One staging object throws, the roots object comes back `TokenMismatch`; the OTHER
/// staging object must still be deleted and counted.
TEST(CASDecommission, PerObjectFailureWarnsAndContinuesDrain)
{
    auto backend = std::make_shared<FailingDeleteBackend>();
    {
        auto victim = openVictim(backend);
        makeTableWithRefs(*victim, "victim/db/t1", 1, 0);
    }
    {
        OperationForTest seed_op(*backend);
        (*seed_op).create("p/staging/victim/upload_ok.tmp", "x", Retry::once());
        (*seed_op).create("p/staging/victim/upload_throws.tmp", "x", Retry::once());
        (*seed_op).create("p/roots/victim/clickhouse_access_check_abc", "x", Retry::once());
    }
    backend->failWithThrow("p/staging/victim/upload_throws.tmp");
    backend->latch();
    backend->failWithTokenMismatch("p/roots/victim/clickhouse_access_check_abc");

    /// The engine reissues an unresolved delete until its own retry window closes, measured on this
    /// clock, so the latched fault reaches a genuine give-up with no real time passing.
    /// Heap-owned, not a plain stack local: `decommissionPoolMember` installs this clock into the
    /// Pool's `boot_ms_fn` background mount-lease renewer, which can still be running on a detached
    /// thread after this function returns, so a by-reference capture of a local would dangle. Wrapped
    /// (rather than passing `clock->nowFn()`/`clock->sleepFn()` directly) so the closures stored in
    /// `PoolConfig` hold the shared_ptr itself, not just the raw `FakeClock*` those methods capture.
    auto clock = std::make_shared<DB::Cas::tests::FakeClock>();
    const auto report = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim",
        /*sink=*/{}, /*request_gc_round=*/{},
        [clock]
        {
            return clock->nowFn()();
        },
        [clock](uint64_t ms)
        {
            clock->sleepFn()(ms);
        });

    EXPECT_EQ(report.staging_objects_removed, 1u)
        << "the OTHER staging object must still be deleted despite the injected failure on its sibling";
    EXPECT_EQ(report.mountpoint_objects_removed, 0u);
    EXPECT_EQ(report.warnings.size(), 2u)
        << "one warning for the thrown exception, one for the TokenMismatch outcome";

    OperationForTest raw_op(*backend);
    EXPECT_FALSE((*raw_op).head("p/staging/victim/upload_ok.tmp", Retry::once()).has_value())
        << "the healthy staging object was actually deleted, not merely skipped";
    EXPECT_TRUE((*raw_op).head("p/staging/victim/upload_throws.tmp", Retry::once()).has_value())
        << "the failing object is left behind (untouched) so a re-run can retry it";
    EXPECT_TRUE((*raw_op).head("p/roots/victim/clickhouse_access_check_abc", Retry::once()).has_value())
        << "TokenMismatch means nothing was actually deleted -- the object survives";
}

/// Opaque physical debris carries no logical owner and therefore cannot widen or redirect
/// decommission's catalog-derived victim set. Task 5's ownership-tree janitor owns that debris.
TEST(CASDecommission, LifelessPhysicalKeyCannotRedirectCatalogOwnedDecommission)
{
    auto backend = std::make_shared<InMemoryBackend>();
    String lifeless;
    {
        auto victim = openVictim(backend);
        makeTableWithRefs(*victim, "victim/db/t1", 1, 0);
        /// Hand-built: no helper can mint the un-incarnated shape any more.
        lifeless = victim->layout().casRefsPrefix() + String("victim/db/t1/_log/")
            + renderRefTxnId(RefTxnId{1, 1}) + ".zst";
        OperationForTest seed_op(*backend);
        ASSERT_TRUE(std::holds_alternative<Committed>((*seed_op).create(lifeless, "garbage", Retry::once())));
    }

    const auto report = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");
    EXPECT_EQ(report.namespaces_removed, 1u);
    OperationForTest raw_op(*backend);
    EXPECT_TRUE((*raw_op).head(lifeless, Retry::once()).has_value())
        << "decommission must neither adopt nor delete an unowned physical life key";
}

/// The manifest-debris drain honors the same tolerate-and-continue contract as
/// `deleteListedPrefix`: a per-key `deleteExact` failure becomes a warning, while the namespace
/// erasure and subsequent staging drain continue. Protection reads now use opaque physical life
/// prefixes, so a logical-name substring can no longer target an otherwise unlisted namespace.
TEST(CASDecommission, ManifestDebrisDeleteFailureWarnsAndContinues)
{
    auto backend = std::make_shared<FailingDeleteBackend>();
    String debris_key;
    {
        auto victim = openVictim(backend);
        makeTableWithRefs(*victim, "victim/db/t1", 1, 0);
        const ManifestId debris_id = seedOrphanManifestBody(*victim, "victim/db/t1");
        debris_key = victim->layout().manifestKey(debris_id);
    }
    backend->failWithThrow(debris_key);
    backend->latch();
    {
        OperationForTest seed_op(*backend);
        (*seed_op).create("p/staging/victim/upload_ok.tmp", "x", Retry::once());
    }

    /// The engine reissues an unresolved delete until its own retry window closes, measured on this
    /// clock, so the latched fault reaches a genuine give-up with no real time passing.
    /// Heap-owned, not a plain stack local: `decommissionPoolMember` installs this clock into the
    /// Pool's `boot_ms_fn` background mount-lease renewer, which can still be running on a detached
    /// thread after this function returns, so a by-reference capture of a local would dangle. Wrapped
    /// (rather than passing `clock->nowFn()`/`clock->sleepFn()` directly) so the closures stored in
    /// `PoolConfig` hold the shared_ptr itself, not just the raw `FakeClock*` those methods capture.
    auto clock = std::make_shared<DB::Cas::tests::FakeClock>();
    const auto report = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim",
        /*sink=*/{}, /*request_gc_round=*/{},
        [clock]
        {
            return clock->nowFn()();
        },
        [clock](uint64_t ms)
        {
            clock->sleepFn()(ms);
        });

    EXPECT_EQ(report.namespaces_removed, 1u)
        << "victim/db/t1's namespace erasure (Task 2) is untouched by either injected failure";
    EXPECT_EQ(report.manifest_debris_removed, 0u);
    EXPECT_EQ(report.warnings.size(), 1u)
        << "the thrown per-key delete must keep the retirement tail fail-closed";
    EXPECT_EQ(report.staging_objects_removed, 1u)
        << "the staging phase still ran to completion after the manifest-debris phase's failures -- "
           "the whole command did not abort";

    OperationForTest raw_op(*backend);
    EXPECT_TRUE((*raw_op).head(debris_key, Retry::once()).has_value())
        << "the failing object is left behind (untouched) so a re-run can retry it";
}

/// GC owns the completed catalog-row deletion. Once it drains the row, a clean decommission retry
/// removes the mutable slot objects and tombstones the owner anchor.
TEST(CASDecommission, RemovesMutableSlotAndRefusesTombstonedRerun)
{
    auto backend = std::make_shared<InMemoryBackend>();
    {
        auto victim = openVictim(backend);
        makeTableWithRefs(*victim, "victim/db/t1", 1, 0);
    }
    const auto pending = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");
    EXPECT_FALSE(pending.slot_removed);
    EXPECT_FALSE(pending.warnings.empty());
    drainCompletedNamespaceRemovals(backend);
    const auto report = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin2"}, "victim");
    EXPECT_TRUE(report.slot_removed);
    EXPECT_TRUE(report.warnings.empty());
    OperationForTest raw_op(*backend);
    EXPECT_FALSE((*raw_op).head("p/gc/server-roots/victim/mount", Retry::once()).has_value());
    const auto owner = (*raw_op).read("p/gc/server-roots/victim/owner", Retry::once());
    ASSERT_TRUE(owner.has_value());
    EXPECT_TRUE(decodeOwner(owner->bytes).retired_at_ms.has_value());
    EXPECT_FALSE((*raw_op).head("p/gc/server-roots/victim/epoch", Retry::once()).has_value());

    expectThrowsCode(ErrorCodes::CORRUPTED_DATA, [&]
    {
        decommissionPoolMember(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "a2"}, "victim");
    });
}

/// Triage #9: a successor may reclaim the same UUID immediately after the decommission admin writes
/// its farewell. The retirement tail must use the farewell/claimed-epoch tokens captured around that
/// release, delete `mount` first, and stop on its `TokenMismatch`; re-reading current tokens would
/// delete the live successor's control objects and falsely report the slot removed.
TEST(CASDecommission, SuccessorReclaimFencesSlotRetirementTail)
{
    auto backend = std::make_shared<SuccessorReclaimAfterFarewellBackend>();
    { auto victim = openVictim(backend); }
    backend->armForSuccessorReclaim();

    std::vector<CasEvent> seen;
    const auto report = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim",
        [&](const CasEvent & event) { seen.push_back(event); });

    ASSERT_TRUE(backend->successorInjected());
    EXPECT_FALSE(report.slot_removed);
    ASSERT_EQ(report.warnings.size(), 1u);
    EXPECT_NE(report.warnings.front().find("p/gc/server-roots/victim/mount"), String::npos);
    EXPECT_NE(report.warnings.front().find("mismatch"), String::npos);

    OperationForTest raw_op(*backend);
    const auto mount = (*raw_op).read("p/gc/server-roots/victim/mount", Retry::once());
    ASSERT_TRUE(mount.has_value());
    EXPECT_EQ(PersistedEtag::capture(mount->etag).value, backend->successorMountValue());
    EXPECT_EQ(mount->bytes, backend->successorMountBytes());

    const auto epoch = (*raw_op).read("p/gc/server-roots/victim/epoch", Retry::once());
    ASSERT_TRUE(epoch.has_value());
    EXPECT_EQ(PersistedEtag::capture(epoch->etag).value, backend->successorEpochValue());
    EXPECT_EQ(epoch->bytes, backend->successorEpochBytes());
    EXPECT_TRUE((*raw_op).read("p/gc/server-roots/victim/owner", Retry::once()).has_value());

    ASSERT_FALSE(seen.empty());
    EXPECT_EQ(seen.back().outcome, "end");
    EXPECT_EQ(seen.back().detail.at("slot_removed"), "0");
}

/// A successor can also restart after both stale mutable objects were deleted but before `owner` is
/// retired. Mere presence of either freshly recreated mutable object must stop owner retirement.
TEST(CASDecommission, SuccessorReclaimAfterEpochDeleteKeepsOwnerAnchor)
{
    auto backend = std::make_shared<SuccessorReclaimAfterEpochDeleteBackend>();
    { auto victim = openVictim(backend); }

    const String owner_key = "p/gc/server-roots/victim/owner";
    OperationForTest raw_op(*backend);
    const auto original_owner = (*raw_op).read(owner_key, Retry::once());
    ASSERT_TRUE(original_owner.has_value());
    backend->armForSuccessorReclaim();

    const auto report = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");

    ASSERT_TRUE(backend->successorInjected());
    EXPECT_FALSE(report.slot_removed);
    EXPECT_FALSE(report.warnings.empty());
    EXPECT_EQ(backend->ownerRewriteAttempts(), 0u);

    const auto owner = (*raw_op).read(owner_key, Retry::once());
    ASSERT_TRUE(owner.has_value());
    EXPECT_EQ(owner->etag, original_owner->etag);
    EXPECT_EQ(owner->bytes, original_owner->bytes);

    const auto mount = (*raw_op).read("p/gc/server-roots/victim/mount", Retry::once());
    ASSERT_TRUE(mount.has_value());
    EXPECT_EQ(PersistedEtag::capture(mount->etag).value, backend->successorMountValue());
    EXPECT_EQ(mount->bytes, backend->successorMountBytes());

    const auto epoch = (*raw_op).read("p/gc/server-roots/victim/epoch", Retry::once());
    ASSERT_TRUE(epoch.has_value());
    EXPECT_EQ(PersistedEtag::capture(epoch->etag).value, backend->successorEpochValue());
    EXPECT_EQ(epoch->bytes, backend->successorEpochBytes());
}

/// Triage #9 control: absent a successor interleaving, the fenced tail removes both mutable control
/// objects, tombstones the owner anchor, and preserves the existing successful `slot_removed=1` result.
TEST(CASDecommission, FencedSlotRetirementTailRetiresUncontendedSlot)
{
    auto backend = std::make_shared<InMemoryBackend>();
    { auto victim = openVictim(backend); }

    const auto report = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");

    EXPECT_TRUE(report.warnings.empty());
    EXPECT_TRUE(report.slot_removed);
    OperationForTest raw_op(*backend);
    EXPECT_FALSE((*raw_op).head("p/gc/server-roots/victim/mount", Retry::once()).has_value());
    EXPECT_FALSE((*raw_op).head("p/gc/server-roots/victim/epoch", Retry::once()).has_value());
    const auto owner = (*raw_op).read("p/gc/server-roots/victim/owner", Retry::once());
    ASSERT_TRUE(owner.has_value());
    EXPECT_TRUE(decodeOwner(owner->bytes).retired_at_ms.has_value());
}

/// One decommission command spans several requests on the SAME open-fence engine: the
/// pre-impersonation catalog cut (before any `Pool` exists), the namespace drop's own catalog re-read,
/// and -- once the row it dropped is no longer owned -- the full retirement tail's reads, deletes and
/// final owner tombstone, all issued after `admin.reset()` destroys the `Pool`. Catalog-row deletion
/// is GC's job (`dropNamespace` only reaches `Removing`), so this is necessarily two commands: the
/// first proves the drop and its catalog read landed, the second (after GC folds the row) proves the
/// retirement tail's requests landed past the `Pool`'s own lifetime.
TEST(CASDecommission, RunsOnAnOpenFence)
{
    auto backend = std::make_shared<InMemoryBackend>();
    {
        auto victim = openVictim(backend);
        makeTableWithRefs(*victim, "victim/db/t1", 1, 0);
    }

    const auto pending = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");
    EXPECT_EQ(pending.namespaces_removed, 1u);
    EXPECT_FALSE(pending.slot_removed);
    EXPECT_FALSE(pending.warnings.empty());

    drainCompletedNamespaceRemovals(backend);

    const auto report = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin2"}, "victim");
    EXPECT_TRUE(report.warnings.empty());
    EXPECT_TRUE(report.slot_removed);
    OperationForTest raw_op(*backend);
    EXPECT_FALSE((*raw_op).head("p/gc/server-roots/victim/mount", Retry::once()).has_value());
    EXPECT_FALSE((*raw_op).head("p/gc/server-roots/victim/epoch", Retry::once()).has_value());
    const auto owner = (*raw_op).read("p/gc/server-roots/victim/owner", Retry::once());
    ASSERT_TRUE(owner.has_value());
    EXPECT_TRUE(decodeOwner(owner->bytes).retired_at_ms.has_value());
}

TEST(CASDecommission, SuccessfulDecommissionLeavesTombstonedOwnerAnchor)
{
    auto backend = std::make_shared<InMemoryBackend>();
    { auto victim = openVictim(backend); }

    const String owner_key = "p/gc/server-roots/victim/owner";
    OperationForTest raw_op(*backend);
    const auto before = (*raw_op).read(owner_key, Retry::once());
    ASSERT_TRUE(before.has_value());
    EXPECT_FALSE(decodeOwner(before->bytes).retired_at_ms.has_value());

    const auto report = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");

    EXPECT_TRUE(report.warnings.empty());
    EXPECT_TRUE(report.slot_removed);
    const auto after = (*raw_op).read(owner_key, Retry::once());
    ASSERT_TRUE(after.has_value());
    EXPECT_NE(after->etag, before->etag);
    EXPECT_EQ(decodeOwner(after->bytes).server_uuid, decodeOwner(before->bytes).server_uuid);
    EXPECT_TRUE(decodeOwner(after->bytes).retired_at_ms.has_value());
}

TEST(CASDecommission, SuccessorOwnerRewriteWinsBeforeTombstone)
{
    auto backend = std::make_shared<SuccessorOwnerRewriteBeforeTombstoneBackend>();
    { auto victim = openVictim(backend); }
    backend->armForSuccessorRewrite();

    const auto report = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");

    ASSERT_TRUE(backend->successorInjected());
    EXPECT_FALSE(report.slot_removed);
    ASSERT_EQ(report.warnings.size(), 1u);
    EXPECT_NE(report.warnings.front().find("successor reclaimed"), String::npos);

    OperationForTest raw_op(*backend);
    const auto owner = (*raw_op).read("p/gc/server-roots/victim/owner", Retry::once());
    ASSERT_TRUE(owner.has_value());
    EXPECT_EQ(PersistedEtag::capture(owner->etag).value, backend->successorOwnerValue());
    EXPECT_EQ(owner->bytes, backend->successorOwnerBytes());
    EXPECT_FALSE(decodeOwner(owner->bytes).retired_at_ms.has_value());
}

/// Final whole-branch review finding (Important):
/// an ambiguous outcome on the owner tombstone write -- the write lands but its response is lost, the
/// same shape as a real SDK timeout after a landed write -- must not be reported as a hard failure.
/// `op.replace`'s own resolve read settles this (current bytes already match the intended tombstone)
/// and reports `Committed`. `injectAmbiguousLandedWrite` throws `Poco::TimeoutException` after
/// applying the write: the engine's write loop treats a `Poco::Exception` as a transport fault (never
/// a caller bug), which is exactly the class this scenario models -- a bare `std::runtime_error` here
/// would propagate unchanged instead of being resolved.
TEST(CASDecommission, OwnerTombstoneAmbiguousSuccessResolvesToCommitted)
{
    auto backend = std::make_shared<InMemoryBackend>();
    { auto victim = openVictim(backend); }
    backend->injectAmbiguousLandedWrite("p/gc/server-roots/victim/owner");

    const auto report = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");

    EXPECT_TRUE(report.slot_removed) << "the ambiguous write actually landed and must resolve to Committed";
    EXPECT_TRUE(report.warnings.empty());

    OperationForTest raw_op(*backend);
    const auto owner = (*raw_op).read("p/gc/server-roots/victim/owner", Retry::once());
    ASSERT_TRUE(owner.has_value());
    EXPECT_TRUE(decodeOwner(owner->bytes).retired_at_ms.has_value());
}

/// Delegates every op to `inner`, except `remove`: while `armed`, any key starting with `fail_prefix`
/// throws an injected transient failure instead of deleting -- models a real backend transiently
/// failing to delete under one whole prefix. `disarm()` clears the failure (the resume half of
/// `FailedDrainKeepsSlotThenResumes`). Forwards every pure-virtual `Backend` member (the `CasBackend.h`
/// list) to `inner` untouched. Injects on the `remove` PRIMITIVE, not the legacy `deleteExact`:
/// `deleteListedPrefix`'s mountpoint drain (`CasDecommission.cpp`) goes through `CasOperation::remove`.
class FailDeletesUnderPrefixBackend : public Backend
{
public:
    FailDeletesUnderPrefixBackend(std::shared_ptr<InMemoryBackend> inner_, String fail_prefix_)
        : inner(std::move(inner_)), fail_prefix(std::move(fail_prefix_))
    {
    }

    void disarm() { armed = false; }

    bool supportsListTokens() const override { return inner->supportsListTokens(); }

    /// The transport primitives forward to `inner`, except `remove`, which is what this double
    /// injects through. Declared because `Backend` declares them pure.
    std::optional<Raw> read(const String & key, TransportAccess & access) override { return inner->read(key, access); }
    std::optional<RawMeta> head(const String & key, TransportAccess & access) override { return inner->head(key, access); }
    RawListPage list(const String & prefix, const String & cursor, size_t limit, TransportAccess & access) override { return inner->list(prefix, cursor, limit, access); }
    RawRemoval remove(const String & key, const String & expected_value, TransportAccess & access) override
    {
        if (armed && key.starts_with(fail_prefix))
            /// `Poco::TimeoutException`, the class the request engine classifies as a transport fault
            /// and reissues (`Retry::standard()`): this fixture models a real backend transiently
            /// failing, and `armed` stays set across every reissue of the same call, so the engine
            /// reaches its own retry deadline and gives up rather than recovering on a later attempt.
            throw Poco::TimeoutException("injected transient delete failure for " + key);
        return inner->remove(key, expected_value, access);
    }
    void removeManyWriteOnce(const std::vector<WriteOnceKey> & keys, TransportAccess & access) override { inner->removeManyWriteOnce(keys, access); }
    std::expected<String, RawConflict> write(const String & key, const String & bytes,
                                             const std::optional<String> & expected_value, TransportAccess & access) override
    {
        return inner->write(key, bytes, expected_value, access);
    }
    std::unique_ptr<DB::ReadBuffer> stream(const String & key, TransportAccess & access) override { return inner->stream(key, access); }
    void publish(const BlobPublishRequest & request, TransportAccess & access) override { inner->publish(request, access); }
    DB::Cas::Dialect dialect() const override { return inner->dialect(); }

private:
    std::shared_ptr<InMemoryBackend> inner;
    String fail_prefix;
    bool armed = true;
};

/// Task 4 fail-close: a drain failure under the roots prefix keeps the slot terminated-but-present
/// (`report.slot_removed == false`, the mount object survives as the resume anchor). Once the fault is
/// cleared, a re-run finishes the job: the already-erased namespace is counted as
/// `namespaces_already_removed`, the leftover roots object is finally swept, and the slot is removed.
TEST(CASDecommission, FailedDrainKeepsSlotThenResumes)
{
    auto inner = std::make_shared<InMemoryBackend>();
    {
        auto victim = Pool::open(inner, PoolConfig{.pool_prefix = "p", .server_root_id = "victim"});
        makeTableWithRefs(*victim, "victim/db/t1", 1, 0);
    }
    OperationForTest raw_op(*inner);
    (*raw_op).create("p/roots/victim/loose_file", "x", Retry::once());

    auto failing = std::make_shared<FailDeletesUnderPrefixBackend>(inner, "p/roots/victim/");
    /// The engine reissues an unresolved delete until its own retry window closes, measured on this
    /// clock, so the fault (armed across every reissue) reaches a genuine give-up with no real time
    /// passing -- `clock` fast-forwards through the whole 90 s `Retry::standard()` window in one call.
    /// `drain_now_fn` is also this session's boot clock (decommissionPoolMember unifies the two), so the
    /// admin's own mount lease needs a TTL well past that fast-forward or the farewell it attempts on
    /// the way out would refuse against a deadline the retry exhaustion already ran past -- a real
    /// decommission's background renewer would have kept the deadline current over 90 real seconds, but
    /// nothing here advances real time to let it.
    auto clock = std::make_shared<DB::Cas::tests::FakeClock>();
    const auto first = decommissionPoolMember(
        failing,
        PoolConfig{.pool_prefix = "p", .server_root_id = "a1", .mount_lease_ttl_ms = std::chrono::milliseconds(300'000)},
        "victim", /*sink=*/{}, /*request_gc_round=*/{},
        [clock]
        {
            return clock->nowFn()();
        },
        [clock](uint64_t ms)
        {
            clock->sleepFn()(ms);
        });
    EXPECT_FALSE(first.warnings.empty());
    EXPECT_FALSE(first.slot_removed);
    EXPECT_TRUE((*raw_op).head("p/gc/server-roots/victim/mount", Retry::once()).has_value())
        << "slot kept -- resume anchor";

    failing->disarm();
    const auto second = decommissionPoolMember(
        failing, PoolConfig{.pool_prefix = "p", .server_root_id = "a2"}, "victim");
    EXPECT_FALSE(second.warnings.empty());
    EXPECT_FALSE(second.slot_removed);
    EXPECT_EQ(second.namespaces_already_removed, 1u);
    EXPECT_EQ(second.mountpoint_objects_removed, 1u);

    drainCompletedNamespaceRemovals(inner);
    const auto third = decommissionPoolMember(
        failing, PoolConfig{.pool_prefix = "p", .server_root_id = "a3"}, "victim");
    EXPECT_TRUE(third.warnings.empty());
    EXPECT_TRUE(third.slot_removed);
}

/// Task 4 fail-close, manifest-debris variant (review follow-up: the plan's own example only exercises
/// a roots-phase failure). A per-key `deleteExact` throw inside the manifest-debris drain must ALSO
/// keep the slot: `report.slot_removed == false`, the mount object survives, and once the injected
/// failure is cleared a re-run drains the leftover debris and removes the slot.
TEST(CASDecommission, ManifestDebrisFailureKeepsSlotThenResumes)
{
    auto backend = std::make_shared<FailingDeleteBackend>();
    String debris_key;
    {
        auto victim = openVictim(backend);
        makeTableWithRefs(*victim, "victim/db/t1", 1, 0);
        const ManifestId debris_id = seedOrphanManifestBody(*victim, "victim/db/t1");
        debris_key = victim->layout().manifestKey(debris_id);
    }
    backend->failWithThrow(debris_key);
    backend->latch();

    /// The engine reissues an unresolved delete until its own retry window closes, measured on this
    /// clock, so the latched fault reaches a genuine give-up with no real time passing.
    auto clock = std::make_shared<DB::Cas::tests::FakeClock>();
    const auto first = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "a1"}, "victim",
        /*sink=*/{}, /*request_gc_round=*/{},
        [clock]
        {
            return clock->nowFn()();
        },
        [clock](uint64_t ms)
        {
            clock->sleepFn()(ms);
        });
    EXPECT_FALSE(first.warnings.empty());
    EXPECT_FALSE(first.slot_removed);
    EXPECT_EQ(first.manifest_debris_removed, 0u);
    OperationForTest raw_op(*backend);
    EXPECT_TRUE((*raw_op).head("p/gc/server-roots/victim/mount", Retry::once()).has_value())
        << "slot kept -- resume anchor";
    EXPECT_TRUE((*raw_op).head(debris_key, Retry::once()).has_value())
        << "the failing object is left behind (untouched) so a re-run can retry it";

    /// COVERAGE LOST HERE, DELIBERATELY NAMED. Before the §6 premise, clearing the injected failure let
    /// a re-run drain the debris and retire the slot, which is what proved the per-key fail-close path
    /// RESUMES rather than merely refuses. Under the premise the sweep never reaches `deleteExact` for
    /// this body at all (single-epoch pool -- see the note on the helpers above), so disarming changes
    /// nothing and the resume half of this test is no longer expressible. What survives is the half that
    /// still has a mechanism: the slot stays kept across the re-run, and the object stays untouched.
    /// The resume assertion comes back with the drain's reclaim path (registers R2/R3, Stage B).
    backend->disarm();
    const auto second = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "a2"}, "victim");
    EXPECT_EQ(second.namespaces_already_removed, 1u);
    EXPECT_EQ(second.manifest_debris_removed, 0u);
    EXPECT_FALSE(second.slot_removed);
    EXPECT_TRUE((*raw_op).head(debris_key, Retry::once()).has_value());
    EXPECT_TRUE((*raw_op).head("p/gc/server-roots/victim/mount", Retry::once()).has_value())
        << "the slot is still the resume anchor -- nothing was retired against unreclaimed debris";
}

/// CI run 7 (PR #2300): `drain_now_fn` fakes the drain's own request clock, but the mount lease's
/// farewell deadline is bound to `PoolConfig::boot_ms_fn`, a distinct clock that -- before this test's
/// fix -- stayed on the real boot clock regardless. On a freshly booted CI host (real boot time below
/// `FakeClock`'s starting instant) the two clocks disagreed enough that the farewell's own bound looked
/// already-expired, so `admin.reset()` released nothing and the slot was never retired -- passing locally
/// only because a long-lived dev box's real uptime dwarfs the fake clock. Pin the fake clock far beyond
/// ANY real host's boot time (rather than relying on the host actually being fresh) so the mismatch --
/// and the fix -- are exercised deterministically on every machine.
TEST(CASDecommission, DrainClockUnifiesWithTheFarewellBootClockRegardlessOfHostUptime)
{
    auto backend = std::make_shared<InMemoryBackend>();
    { auto victim = openVictim(backend); }   /// identity only -- no namespace, so retirement runs straight
                                              /// to the farewell instead of stopping on an unrelated warning

    auto clock = std::make_shared<DB::Cas::tests::FakeClock>();
    clock->now = 1'000'000'000'000'000ULL;   /// dwarfs any real CLOCK_BOOTTIME on any host
    const auto report = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "a1"}, "victim",
        /*sink=*/{}, /*request_gc_round=*/{},
        [clock]
        {
            return clock->nowFn()();
        },
        [clock](uint64_t ms)
        {
            clock->sleepFn()(ms);
        });

    EXPECT_TRUE(report.warnings.empty());
    EXPECT_TRUE(report.slot_removed);
    OperationForTest raw_op(*backend);
    EXPECT_FALSE((*raw_op).head("p/gc/server-roots/victim/mount", Retry::once()).has_value())
        << "one clock for both the request engine and the mount lease -- the farewell must commit and the "
           "slot must retire in a single call, with no leftover mount to resume against";
}

/// Review round 9r2: folding `drain_now_fn` into `config.boot_ms_fn` alone left `Pool::openForDecommission`'s
/// own mount/farewell/GC planes -- constructed and used DURING opening, before `decommissionPoolMember`
/// gets a chance to call `setCasRequestNowFnForTest`/`setCasRetrySleepForTest` on the already-open `Pool`
/// -- retrying on a clock that only a fake SLEEP advances, while those planes still slept for REAL between
/// attempts. A transient failure during opening (the owner-object read on the open plane) would then never
/// see its own `Retry::standard()` deadline elapse, because the bound is measured against a clock frozen
/// at the value it had when the retry loop started: nothing calls the fake clock's `sleepFn` from a path
/// that still sleeps for real. `PoolConfig::retry_sleep_fn` closes this by installing the matching fake
/// sleep on those same planes AT CONSTRUCTION, together with `boot_ms_fn`.
///
/// Failing-first without risking an actual hang: this fixture flakes the owner read 5 times, so a correct
/// fix paces exactly 5 retries on the fake clock and returns in well under a second of real time; the
/// pre-fix code either never returns (the bound never elapses) or, if it did return, would show an empty
/// `clock.sleeps` (nothing ever called the fake sleep) and real wall time consumed by 5 real backoffs.
TEST(CASDecommission, OpeningRetriesPaceOnTheSameFakeClockAndSleepAsTheDrain)
{
    auto backend = std::make_shared<FlakyReadBackend>();
    { auto victim = openVictim(backend); }   /// identity only

    const Layout layout("p");
    backend->failReadNTimes(layout.ownerKey("victim"), /*times=*/5);

    auto clock = std::make_shared<DB::Cas::tests::FakeClock>();
    clock->now = 1'000'000'000'000'000ULL;

    const auto started = std::chrono::steady_clock::now();
    const auto report = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "a1"}, "victim",
        /*sink=*/{}, /*request_gc_round=*/{},
        [clock]
        {
            return clock->nowFn()();
        },
        [clock](uint64_t ms)
        {
            clock->sleepFn()(ms);
        });
    const auto wall_elapsed = std::chrono::steady_clock::now() - started;

    EXPECT_TRUE(report.warnings.empty());
    EXPECT_TRUE(report.slot_removed);
    EXPECT_FALSE(clock->sleeps.empty())
        << "the opening retries must have been paced on the injected clock, not a real sleep";
    EXPECT_LT(wall_elapsed, std::chrono::seconds(5))
        << "paced on the fake clock, five retries during opening should cost no real wall time at all";
}

/// Task 5 (Task-1 carry-forward, escalated by review): preserve recovery from the legacy partial
/// hand-cleanup shape where owner and epoch are absent but the mount lease remains. Triage #9 changed
/// new retirements to delete `mountKey`/`epochKey` and tombstone `ownerKey`, so the current tail no
/// longer creates this shape, but `openForDecommission`'s owner-anchor-absent +
/// mount-lease-present fallback ("partial hand-cleanup: adopt from the lease", `CasPool.cpp`) remains
/// compatibility-critical for slots left by older binaries or manual repair.
///
/// `claimOwnerOrThrow` (`CasServerRoot.cpp`) gates the owner-absent path a SECOND, stricter way: the
/// same catalog cut must name no `Creating`, `Live` or `Removing` namespace under this canonical root,
/// and the name-bearing `cas/manifests/<srid>/` and `roots/<srid>/` families must be empty. Opaque
/// stream/state debris cannot be attributed to a server root and is deliberately inert. This test
/// therefore uses a victim with NO namespaces at all: identity persisted
/// (mount/owner/epoch exist from a real graceful close), data subtree genuinely empty -- the exact
/// precondition the fallback is designed for. Simulate the crash directly: claim the slot once (exactly
/// `decommissionPoolMember`'s own first step), let it close gracefully (the mount-lease renewer's
/// farewell stamp, same as a real `admin.reset()`), then manually strike `epochKey`+`ownerKey`, leaving
/// `mountKey`. A `decommissionPoolMember` re-run must resolve identity via the mount-lease fallback and
/// finish retiring the slot; a further re-run then sees the tombstone and refuses to resume it.
TEST(CASDecommission, MidRetirementCrashResumesViaMountLeaseFallback)
{
    auto backend = std::make_shared<InMemoryBackend>();
    { auto victim = openVictim(backend); }   /// identity only -- no namespace, so the subtree stays empty

    const Layout layout("p");
    /// Claim the slot once, exactly as `decommissionPoolMember`'s own first step would -- this (re)writes
    /// fresh epoch/owner/mount control objects. Closing gracefully (scope exit) stamps the mount lease's
    /// farewell, matching what a real slot retirement's `admin.reset()` does right before its delete loop.
    {
        auto admin = Pool::openForDecommission(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "chk"}, "victim");
    }

    /// Manually strike epoch + owner, leaving the mount -- the legacy partial hand-cleanup shape.
    OperationForTest raw_op(*backend);
    for (const String & key : {layout.epochKey("victim"), layout.ownerKey("victim")})
    {
        const auto head = (*raw_op).head(key, Retry::once());
        ASSERT_TRUE(head.has_value());
        ASSERT_EQ((*raw_op).remove(key, head->etag, Retry::once()), Removal::Removed);
    }
    ASSERT_FALSE((*raw_op).head(layout.epochKey("victim"), Retry::once()).has_value());
    ASSERT_FALSE((*raw_op).head(layout.ownerKey("victim"), Retry::once()).has_value());
    ASSERT_TRUE((*raw_op).head(layout.mountKey("victim"), Retry::once()).has_value())
        << "the mount lease must survive -- it is the resume anchor the fallback reads";

    const auto report = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "a2"}, "victim");

    EXPECT_TRUE(report.warnings.empty());
    EXPECT_EQ(report.namespaces_removed, 0u);
    EXPECT_TRUE(report.slot_removed);
    EXPECT_FALSE((*raw_op).head(layout.epochKey("victim"), Retry::once()).has_value());
    const auto owner = (*raw_op).read(layout.ownerKey("victim"), Retry::once());
    ASSERT_TRUE(owner.has_value());
    EXPECT_TRUE(decodeOwner(owner->bytes).retired_at_ms.has_value());
    EXPECT_FALSE((*raw_op).head(layout.mountKey("victim"), Retry::once()).has_value());

    expectThrowsCode(ErrorCodes::CORRUPTED_DATA, [&]
    {
        decommissionPoolMember(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "a3"}, "victim");
    });
}
