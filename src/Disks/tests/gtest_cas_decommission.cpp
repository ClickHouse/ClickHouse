#include "cas_test_helpers.h"
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasDecommission.h>
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

/// Fails `deleteExact` for one or two designated keys -- either by throwing (a transient backend
/// hiccup) or by returning a synthetic `TokenMismatch` (a "listed but raced" outcome) -- delegating
/// every other key to the base `InMemoryBackend` untouched. Drives the drain phases' per-object
/// fail-close path (`deleteListedPrefix`/`sweepNamespace`, `CasDecommission.cpp`/
/// `CasOrphanManifestSweep.cpp`): a failure on one listed object must record a warning and let the rest
/// of the sweep proceed, never abort the whole phase.
///
class FailingDeleteBackend : public InMemoryBackend
{
public:
    void failWithThrow(const String & key) { throw_key = key; }
    void failWithTokenMismatch(const String & key) { mismatch_key = key; }
    /// Clears every injected failure -- the resume half of a fail-then-retry test (Task 4).
    void disarm() { throw_key.clear(); mismatch_key.clear(); }

    DeleteOutcome deleteExact(const String & key, const Token & token) override
    {
        if (key == throw_key)
            throw std::runtime_error("injected transient delete failure for " + key);
        if (key == mismatch_key)
            return DeleteOutcome{.kind = DeleteOutcome::Kind::TokenMismatch};
        return InMemoryBackend::deleteExact(key, token);
    }

private:
    String throw_key;
    String mismatch_key;
};

/// Replaces the durable catalog immediately after returning the first armed catalog read. This
/// distinguishes the immutable cut validated before decommission impersonation from a later mount
/// safety observation without assuming those two decisions share one GET.
class CatalogChangesAfterFirstReadBackend : public InMemoryBackend
{
public:
    using Backend::get;

    void armCatalogReplacement(
        const String & key, RefCatalog replacement_, size_t completed_reads_before_replacement = 0)
    {
        catalog_key = key;
        replacement = std::move(replacement_);
        reads_to_skip = completed_reads_before_replacement;
        armed = true;
    }

    bool fired() const { return replacement_fired; }

    std::optional<GetResult> get(const String & key, Range range) override
    {
        auto got = InMemoryBackend::get(key, range);
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
        const PutResult put = InMemoryBackend::putOverwrite(
            key, encodeRefCatalog(replacement), got->token, {});
        if (put.outcome != PutOutcome::Done)
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

std::vector<std::tuple<String, String, Token>> snapshotPrefixObjects(
    InMemoryBackend & backend, const String & prefix)
{
    std::vector<std::tuple<String, String, Token>> objects;
    String cursor;
    while (true)
    {
        const ListPage page = backend.list(prefix, cursor, 1000);
        for (const ListedKey & listed : page.keys)
        {
            const auto got = backend.get(listed.key);
            if (!got)
                throw std::runtime_error("prefix snapshot fixture: listed object disappeared");
            objects.emplace_back(listed.key, got->bytes, got->token);
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
    using Backend::get;
    using Backend::putOverwrite;

    void armForSuccessorReclaim() { armed = true; }

    std::optional<GetResult> get(const String & key, Range range) override
    {
        std::optional<GetResult> result = InMemoryBackend::get(key, range);
        if (farewell_seen && !successor_injected && (key == mount_key || key == epoch_key))
            injectSuccessor();
        return result;
    }

    PutResult putOverwrite(
        const String & key, const String & bytes, const Token & expected, const ObjectMeta & meta) override
    {
        const PutResult result = InMemoryBackend::putOverwrite(key, bytes, expected, meta);
        if (armed && key == mount_key && result.outcome == PutOutcome::Done)
        {
            const MountLease mount = decodeMountLease(bytes);
            if (mount.min_active == std::numeric_limits<uint64_t>::max())
                farewell_seen = true;
        }
        return result;
    }

    bool successorInjected() const { return successor_injected; }
    const Token & successorMountToken() const { return successor_mount_token; }
    const Token & successorEpochToken() const { return successor_epoch_token; }
    const String & successorMountBytes() const { return successor_mount_bytes; }
    const String & successorEpochBytes() const { return successor_epoch_bytes; }

private:
    void injectSuccessor()
    {
        const auto epoch = InMemoryBackend::get(epoch_key, {});
        const auto mount = InMemoryBackend::get(mount_key, {});
        if (!epoch || !mount)
            throw std::runtime_error("successor-reclaim fixture: control object disappeared before reclaim");

        ServerEpoch epoch_value = decodeServerEpoch(epoch->bytes);
        const uint64_t successor_writer_epoch = epoch_value.next_writer_epoch;
        ++epoch_value.next_writer_epoch;
        successor_epoch_bytes = encodeServerEpoch(epoch_value);
        const CasResult epoch_put = InMemoryBackend::casPut(
            epoch_key, successor_epoch_bytes, std::optional<Token>{epoch->token}, {});
        if (epoch_put.outcome != CasOutcome::Committed)
            throw std::runtime_error("successor-reclaim fixture: epoch bump conflicted");
        successor_epoch_token = epoch_put.token;

        MountLease mount_value = decodeMountLease(mount->bytes);
        mount_value.writer_epoch = successor_writer_epoch;
        ++mount_value.seq;
        ++mount_value.started_at_ms;
        mount_value.expires_at_ms = mount_value.started_at_ms + 30'000;
        mount_value.min_active = 0;
        mount_value.gc_fenced = false;
        successor_mount_bytes = encodeMountLease(mount_value);
        const PutResult mount_put = InMemoryBackend::putOverwrite(
            mount_key, successor_mount_bytes, mount->token, {});
        if (mount_put.outcome != PutOutcome::Done)
            throw std::runtime_error("successor-reclaim fixture: mount reclaim conflicted");
        successor_mount_token = mount_put.token;
        successor_injected = true;
    }

    inline static const String mount_key = "p/gc/server-roots/victim/mount";
    inline static const String epoch_key = "p/gc/server-roots/victim/epoch";
    bool armed = false;
    bool farewell_seen = false;
    bool successor_injected = false;
    Token successor_mount_token;
    Token successor_epoch_token;
    String successor_mount_bytes;
    String successor_epoch_bytes;
};

/// Recreates the mutable slot objects immediately after decommission successfully deletes `epoch`.
/// This models a same-UUID successor starting in the final retirement window: `owner` remains the
/// unchanged identity anchor, while the successor legitimately creates a fresh `epoch` and `mount`.
class SuccessorReclaimAfterEpochDeleteBackend : public InMemoryBackend
{
public:
    using Backend::get;
    using Backend::putOverwrite;

    void armForSuccessorReclaim() { armed = true; }

    DeleteOutcome deleteExact(const String & key, const Token & token) override
    {
        const DeleteOutcome result = InMemoryBackend::deleteExact(key, token);
        if (armed && !successor_injected && key == epoch_key
            && classifyDeleteOutcome(result) == DeleteClass::Deleted)
        {
            injectSuccessor();
        }
        return result;
    }

    bool successorInjected() const { return successor_injected; }
    uint64_t ownerRewriteAttempts() const { return owner_rewrite_attempts; }
    const Token & successorMountToken() const { return successor_mount_token; }
    const Token & successorEpochToken() const { return successor_epoch_token; }
    const String & successorMountBytes() const { return successor_mount_bytes; }
    const String & successorEpochBytes() const { return successor_epoch_bytes; }

private:
    PutResult putOverwrite(
        const String & key, const String & bytes, const Token & expected, const ObjectMeta & meta) override
    {
        if (key == owner_key)
            ++owner_rewrite_attempts;
        return InMemoryBackend::putOverwrite(key, bytes, expected, meta);
    }

    void injectSuccessor()
    {
        successor_epoch_bytes = encodeServerEpoch(ServerEpoch{.next_writer_epoch = 102});
        const PutResult epoch_put = InMemoryBackend::putIfAbsent(epoch_key, successor_epoch_bytes, {});
        if (epoch_put.outcome != PutOutcome::Done)
            throw std::runtime_error("late-successor fixture: epoch recreation conflicted");
        successor_epoch_token = epoch_put.token;

        successor_mount_bytes = encodeMountLease(MountLease{
            .server_uuid = UInt128(0x1234),
            .writer_epoch = 101,
            .hostname = "successor",
            .pid = 42,
            .started_at_ms = 1'000,
            .seq = 1,
            .expires_at_ms = 31'000,
            .min_active = 0,
        });
        const PutResult mount_put = InMemoryBackend::putIfAbsent(mount_key, successor_mount_bytes, {});
        if (mount_put.outcome != PutOutcome::Done)
            throw std::runtime_error("late-successor fixture: mount recreation conflicted");
        successor_mount_token = mount_put.token;
        successor_injected = true;
    }

    inline static const String mount_key = "p/gc/server-roots/victim/mount";
    inline static const String epoch_key = "p/gc/server-roots/victim/epoch";
    inline static const String owner_key = "p/gc/server-roots/victim/owner";
    bool armed = false;
    bool successor_injected = false;
    uint64_t owner_rewrite_attempts = 0;
    Token successor_mount_token;
    Token successor_epoch_token;
    String successor_mount_bytes;
    String successor_epoch_bytes;
};

/// Rewrites the owner anchor after decommission reads it but before its conditional tombstone write.
/// Returning the captured result gives decommission a stale owner token, deterministically modeling
/// the successor race without threads or sleeps.
class SuccessorOwnerRewriteBeforeTombstoneBackend : public InMemoryBackend
{
public:
    using Backend::get;

    void armForSuccessorRewrite() { armed = true; }

    std::optional<GetResult> get(const String & key, Range range) override
    {
        std::optional<GetResult> result = InMemoryBackend::get(key, range);
        if (armed && epoch_deleted && !successor_injected && key == owner_key && result)
        {
            successor_owner_bytes = encodeOwner(OwnerObject{
                .server_uuid = decodeOwner(result->bytes).server_uuid,
                .retired_at_ms = std::nullopt,
            });
            const PutResult put = InMemoryBackend::putOverwrite(
                owner_key, successor_owner_bytes, result->token, {});
            if (put.outcome != PutOutcome::Done)
                throw std::runtime_error("owner-successor fixture: owner rewrite conflicted");
            successor_owner_token = put.token;
            successor_injected = true;
        }
        return result;
    }

    DeleteOutcome deleteExact(const String & key, const Token & token) override
    {
        const DeleteOutcome result = InMemoryBackend::deleteExact(key, token);
        if (armed && key == epoch_key && classifyDeleteOutcome(result) == DeleteClass::Deleted)
            epoch_deleted = true;
        return result;
    }

    bool successorInjected() const { return successor_injected; }
    const Token & successorOwnerToken() const { return successor_owner_token; }
    const String & successorOwnerBytes() const { return successor_owner_bytes; }

private:
    inline static const String epoch_key = "p/gc/server-roots/victim/epoch";
    inline static const String owner_key = "p/gc/server-roots/victim/owner";
    bool armed = false;
    bool epoch_deleted = false;
    bool successor_injected = false;
    Token successor_owner_token;
    String successor_owner_bytes;
};

/// Models an "ambiguous success" on the final owner tombstone write: the conditional overwrite
/// actually lands (InMemoryBackend applies it), but the response is then lost (a transient
/// exception is thrown on the SAME call, exactly as a real SDK timeout after a landed write would
/// look). Before the fix, decommission caught any exception here and reported failure
/// unconditionally; the controlled overwrite must resolve this via a GET (the current bytes match
/// what was intended) and report Committed instead.
class AmbiguousOwnerTombstoneBackend : public InMemoryBackend
{
public:
    using Backend::putOverwrite;

    void armForAmbiguousTombstone() { armed = true; }

    PutResult putOverwrite(const String & key, const String & bytes, const Token & expected, const ObjectMeta & meta) override
    {
        const PutResult result = InMemoryBackend::putOverwrite(key, bytes, expected, meta);
        if (armed && !fired && key == owner_key && result.outcome == PutOutcome::Done)
        {
            fired = true;
            throw std::runtime_error("ambiguous-tombstone fixture: response lost after the write landed");
        }
        return result;
    }

private:
    inline static const String owner_key = "p/gc/server-roots/victim/owner";
    bool armed = false;
    bool fired = false;
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
    Backend & backend = victim.backend();
    const Layout & layout = victim.layout();

    /// Final physical ids are pool-wide. The generic raw-write helper intentionally uses one shared
    /// transition sentinel, so this multi-namespace fixture admits a distinct deterministic test life
    /// before invoking it; the helper then resolves and preserves that existing catalog identity.
    const CasRefCatalog::Snapshot catalog = CasRefCatalog::read(backend, layout);
    const auto existing = std::find_if(catalog.catalog.entries.begin(), catalog.catalog.entries.end(),
        [&](const CatalogEntry & entry) { return entry.ns.string() == ns.string(); });
    if (existing == catalog.catalog.entries.end())
    {
        static std::atomic<uint64_t> next_test_life{1000};
        CatalogEntry entry;
        entry.ns = ns;
        entry.state = NsState::Live;
        entry.incarnation = UInt128{next_test_life.fetch_add(1)};
        CasRefCatalog::casAdmitEntry(backend, layout, 1, entry);
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
    const ManifestId id = writeManifestRaw(victim.backend(), victim.layout(), ns, ref, {});
    /// EXPECT, not ASSERT: this function returns a value now, and ASSERT_* expands to a bare `return;`
    /// -- invalid in a non-void function.
    EXPECT_TRUE(victim.backend().head(victim.layout().manifestKey(id)).exists);
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
    const auto empty_catalog = backend->get(layout.refCatalogKey());
    ASSERT_TRUE(empty_catalog);
    ASSERT_EQ(backend->putOverwrite(
        layout.refCatalogKey(), encodeRefCatalog(catalog), empty_catalog->token).outcome, PutOutcome::Done);
    const auto owner_before = backend->get(layout.ownerKey("victim"));
    const auto epoch_before = backend->get(layout.epochKey("victim"));
    const auto mount_before = backend->get(layout.mountKey("victim"));
    ASSERT_TRUE(owner_before);
    ASSERT_TRUE(epoch_before);
    ASSERT_TRUE(mount_before);

    EXPECT_THROW(decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim"), DB::Exception);
    const auto owner_after = backend->get(layout.ownerKey("victim"));
    const auto epoch_after = backend->get(layout.epochKey("victim"));
    const auto mount_after = backend->get(layout.mountKey("victim"));
    ASSERT_TRUE(owner_after);
    ASSERT_TRUE(epoch_after);
    ASSERT_TRUE(mount_after);
    EXPECT_EQ(owner_after->bytes, owner_before->bytes);
    EXPECT_EQ(owner_after->token, owner_before->token);
    EXPECT_EQ(epoch_after->bytes, epoch_before->bytes);
    EXPECT_EQ(epoch_after->token, epoch_before->token);
    EXPECT_EQ(mount_after->bytes, mount_before->bytes);
    EXPECT_EQ(mount_after->token, mount_before->token);
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

    const auto owner_before = backend->get(layout.ownerKey("victim"));
    const auto epoch_before = backend->get(layout.epochKey("victim"));
    const auto mount_before = backend->get(layout.mountKey("victim"));
    ASSERT_TRUE(owner_before);
    ASSERT_TRUE(epoch_before);
    ASSERT_TRUE(mount_before);
    backend->armCatalogReplacement(layout.refCatalogKey(), std::move(ambiguous));

    EXPECT_THROW(decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim"), DB::Exception);
    ASSERT_TRUE(backend->fired());

    const auto owner_after = backend->get(layout.ownerKey("victim"));
    const auto epoch_after = backend->get(layout.epochKey("victim"));
    const auto mount_after = backend->get(layout.mountKey("victim"));
    ASSERT_TRUE(owner_after);
    ASSERT_TRUE(epoch_after);
    ASSERT_TRUE(mount_after);
    EXPECT_EQ(owner_after->bytes, owner_before->bytes);
    EXPECT_EQ(owner_after->token, owner_before->token);
    EXPECT_EQ(epoch_after->bytes, epoch_before->bytes);
    EXPECT_EQ(epoch_after->token, epoch_before->token);
    EXPECT_EQ(mount_after->bytes, mount_before->bytes);
    EXPECT_EQ(mount_after->token, mount_before->token);
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
    const auto empty_catalog = backend->get(layout.refCatalogKey());
    ASSERT_TRUE(empty_catalog);
    ASSERT_EQ(backend->putOverwrite(
        layout.refCatalogKey(), encodeRefCatalog(old_catalog), empty_catalog->token).outcome,
        PutOutcome::Done);

    RefLogTxn new_birth;
    new_birth.ns = ns.string();
    new_birth.txn_id = RefTxnId{1, 1};
    new_birth.ops = {namespaceBirthOp()};
    ASSERT_EQ(backend->putIfAbsent(
        layout.refLogKey(new_life, new_birth.txn_id),
        sealObject(FormatId::RefLog, encodeRefLogTxn(new_birth))).outcome,
        PutOutcome::Done);
    RefLogTxn new_seal;
    new_seal.ns = ns.string();
    new_seal.txn_id = RefTxnId{1, 2};
    new_seal.ops = {epochSealOp()};
    ASSERT_EQ(backend->putIfAbsent(
        layout.refLogKey(new_life, new_seal.txn_id),
        sealObject(FormatId::RefLog, encodeRefLogTxn(new_seal))).outcome,
        PutOutcome::Done);
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
        writeManifestRaw(victim->backend(), victim->layout(), ns, ref, {});
        addPrecommitTransition(victim->backend(), victim->layout(), ns, UInt128(1), "precommit_0", std::nullopt, ref);
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
    backend->putIfAbsent("p/staging/victim/upload1.tmp", "x");
    backend->putIfAbsent("p/staging/victim/upload2.tmp", "x");
    backend->putIfAbsent("p/roots/victim/clickhouse_access_check_abc", "x");

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
    EXPECT_TRUE(backend->list("p/staging/victim/", "", 10).keys.empty());
    EXPECT_TRUE(backend->list("p/roots/victim/", "", 10).keys.empty());
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
    EXPECT_TRUE(backend->head(debris_key).exists)
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
    backend->putIfAbsent("p/staging/victim/upload_ok.tmp", "x");
    backend->putIfAbsent("p/staging/victim/upload_throws.tmp", "x");
    backend->putIfAbsent("p/roots/victim/clickhouse_access_check_abc", "x");
    backend->failWithThrow("p/staging/victim/upload_throws.tmp");
    backend->failWithTokenMismatch("p/roots/victim/clickhouse_access_check_abc");

    const auto report = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");

    EXPECT_EQ(report.staging_objects_removed, 1u)
        << "the OTHER staging object must still be deleted despite the injected failure on its sibling";
    EXPECT_EQ(report.mountpoint_objects_removed, 0u);
    EXPECT_EQ(report.warnings.size(), 2u)
        << "one warning for the thrown exception, one for the TokenMismatch outcome";

    EXPECT_FALSE(backend->head("p/staging/victim/upload_ok.tmp").exists)
        << "the healthy staging object was actually deleted, not merely skipped";
    EXPECT_TRUE(backend->head("p/staging/victim/upload_throws.tmp").exists)
        << "the failing object is left behind (untouched) so a re-run can retry it";
    EXPECT_TRUE(backend->head("p/roots/victim/clickhouse_access_check_abc").exists)
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
        ASSERT_EQ(backend->putIfAbsent(lifeless, "garbage").outcome, PutOutcome::Done);
    }

    const auto report = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");
    EXPECT_EQ(report.namespaces_removed, 1u);
    EXPECT_TRUE(backend->head(lifeless).exists)
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
    backend->putIfAbsent("p/staging/victim/upload_ok.tmp", "x");

    const auto report = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");

    EXPECT_EQ(report.namespaces_removed, 1u)
        << "victim/db/t1's namespace erasure (Task 2) is untouched by either injected failure";
    EXPECT_EQ(report.manifest_debris_removed, 0u);
    EXPECT_EQ(report.warnings.size(), 1u)
        << "the thrown per-key delete must keep the retirement tail fail-closed";
    EXPECT_EQ(report.staging_objects_removed, 1u)
        << "the staging phase still ran to completion after the manifest-debris phase's failures -- "
           "the whole command did not abort";

    EXPECT_TRUE(backend->head(debris_key).exists)
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
    EXPECT_FALSE(backend->get("p/gc/server-roots/victim/mount").has_value());
    const auto owner = backend->get("p/gc/server-roots/victim/owner");
    ASSERT_TRUE(owner.has_value());
    EXPECT_TRUE(decodeOwner(owner->bytes).retired_at_ms.has_value());
    EXPECT_FALSE(backend->get("p/gc/server-roots/victim/epoch").has_value());

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
    EXPECT_NE(report.warnings.front().find("replaced"), String::npos);

    const auto mount = backend->get("p/gc/server-roots/victim/mount");
    ASSERT_TRUE(mount.has_value());
    EXPECT_EQ(mount->token, backend->successorMountToken());
    EXPECT_EQ(mount->bytes, backend->successorMountBytes());

    const auto epoch = backend->get("p/gc/server-roots/victim/epoch");
    ASSERT_TRUE(epoch.has_value());
    EXPECT_EQ(epoch->token, backend->successorEpochToken());
    EXPECT_EQ(epoch->bytes, backend->successorEpochBytes());
    EXPECT_TRUE(backend->get("p/gc/server-roots/victim/owner").has_value());

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
    const auto original_owner = backend->get(owner_key);
    ASSERT_TRUE(original_owner.has_value());
    backend->armForSuccessorReclaim();

    const auto report = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");

    ASSERT_TRUE(backend->successorInjected());
    EXPECT_FALSE(report.slot_removed);
    EXPECT_FALSE(report.warnings.empty());
    EXPECT_EQ(backend->ownerRewriteAttempts(), 0u);

    const auto owner = backend->get(owner_key);
    ASSERT_TRUE(owner.has_value());
    EXPECT_EQ(owner->token, original_owner->token);
    EXPECT_EQ(owner->bytes, original_owner->bytes);

    const auto mount = backend->get("p/gc/server-roots/victim/mount");
    ASSERT_TRUE(mount.has_value());
    EXPECT_EQ(mount->token, backend->successorMountToken());
    EXPECT_EQ(mount->bytes, backend->successorMountBytes());

    const auto epoch = backend->get("p/gc/server-roots/victim/epoch");
    ASSERT_TRUE(epoch.has_value());
    EXPECT_EQ(epoch->token, backend->successorEpochToken());
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
    EXPECT_FALSE(backend->get("p/gc/server-roots/victim/mount").has_value());
    EXPECT_FALSE(backend->get("p/gc/server-roots/victim/epoch").has_value());
    const auto owner = backend->get("p/gc/server-roots/victim/owner");
    ASSERT_TRUE(owner.has_value());
    EXPECT_TRUE(decodeOwner(owner->bytes).retired_at_ms.has_value());
}

TEST(CASDecommission, SuccessfulDecommissionLeavesTombstonedOwnerAnchor)
{
    auto backend = std::make_shared<InMemoryBackend>();
    { auto victim = openVictim(backend); }

    const String owner_key = "p/gc/server-roots/victim/owner";
    const auto before = backend->get(owner_key);
    ASSERT_TRUE(before.has_value());
    EXPECT_FALSE(decodeOwner(before->bytes).retired_at_ms.has_value());

    const auto report = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");

    EXPECT_TRUE(report.warnings.empty());
    EXPECT_TRUE(report.slot_removed);
    const auto after = backend->get(owner_key);
    ASSERT_TRUE(after.has_value());
    EXPECT_NE(after->token, before->token);
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

    const auto owner = backend->get("p/gc/server-roots/victim/owner");
    ASSERT_TRUE(owner.has_value());
    EXPECT_EQ(owner->token, backend->successorOwnerToken());
    EXPECT_EQ(owner->bytes, backend->successorOwnerBytes());
    EXPECT_FALSE(decodeOwner(owner->bytes).retired_at_ms.has_value());
}

/// Final whole-branch review finding (Important):
/// a transient exception on the owner tombstone write must not be reported as a hard failure when the
/// write actually landed -- the controlled overwrite resolves this via GET (current bytes already
/// match the intended tombstone) instead of the old bare putOverwrite's "any exception = failure".
TEST(CASDecommission, OwnerTombstoneAmbiguousSuccessResolvesToCommitted)
{
    auto backend = std::make_shared<AmbiguousOwnerTombstoneBackend>();
    { auto victim = openVictim(backend); }
    backend->armForAmbiguousTombstone();

    const auto report = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");

    EXPECT_TRUE(report.slot_removed) << "the ambiguous write actually landed and must resolve to Committed";
    EXPECT_TRUE(report.warnings.empty());

    const auto owner = backend->get("p/gc/server-roots/victim/owner");
    ASSERT_TRUE(owner.has_value());
    EXPECT_TRUE(decodeOwner(owner->bytes).retired_at_ms.has_value());
}

/// Delegates every op to `inner`, except `deleteExact`: while `armed`, any key starting with
/// `fail_prefix` throws an injected transient failure instead of deleting -- models a real backend
/// transiently failing to delete under one whole prefix. `disarm()` clears the failure (the resume
/// half of `FailedDrainKeepsSlotThenResumes`). Forwards every pure-virtual `Backend` member (the
/// `CasBackend.h` list) to `inner` untouched.
class FailDeletesUnderPrefixBackend : public Backend
{
public:
    using Backend::get;
    using Backend::getStream;
    using Backend::putIfAbsent;
    using Backend::putIfAbsentStream;
    using Backend::putOverwrite;
    using Backend::casPut;

    FailDeletesUnderPrefixBackend(std::shared_ptr<InMemoryBackend> inner_, String fail_prefix_)
        : inner(std::move(inner_)), fail_prefix(std::move(fail_prefix_))
    {
    }

    void disarm() { armed = false; }

    std::optional<GetResult> get(const String & key, Range range) override { return inner->get(key, range); }
    std::optional<GetStreamResult> getStream(const String & key, Range range) override { return inner->getStream(key, range); }
    HeadResult head(const String & key) override { return inner->head(key); }
    PutResult putIfAbsent(const String & key, const String & bytes, const ObjectMeta & meta) override
    {
        return inner->putIfAbsent(key, bytes, meta);
    }
    WriteSinkPtr putIfAbsentStream(const String & key, const ObjectMeta & meta) override
    {
        return inner->putIfAbsentStream(key, meta);
    }
    PutResult putOverwrite(const String & key, const String & bytes, const Token & expected, const ObjectMeta & meta) override
    {
        return inner->putOverwrite(key, bytes, expected, meta);
    }
    CasResult casPut(const String & key, const String & bytes, const std::optional<Token> & expected, const ObjectMeta & meta) override
    {
        return inner->casPut(key, bytes, expected, meta);
    }
    DeleteOutcome deleteExact(const String & key, const Token & token) override
    {
        if (armed && key.starts_with(fail_prefix))
            throw Exception(ErrorCodes::S3_ERROR, "injected transient delete failure for {}", key);
        return inner->deleteExact(key, token);
    }
    ListPage list(const String & prefix, const String & cursor, size_t limit) override { return inner->list(prefix, cursor, limit); }
    bool supportsListTokens() const override { return inner->supportsListTokens(); }

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
    inner->putIfAbsent("p/roots/victim/loose_file", "x");

    auto failing = std::make_shared<FailDeletesUnderPrefixBackend>(inner, "p/roots/victim/");
    const auto first = decommissionPoolMember(
        failing, PoolConfig{.pool_prefix = "p", .server_root_id = "a1"}, "victim");
    EXPECT_FALSE(first.warnings.empty());
    EXPECT_FALSE(first.slot_removed);
    EXPECT_TRUE(inner->get("p/gc/server-roots/victim/mount").has_value())
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

    const auto first = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "a1"}, "victim");
    EXPECT_FALSE(first.warnings.empty());
    EXPECT_FALSE(first.slot_removed);
    EXPECT_EQ(first.manifest_debris_removed, 0u);
    EXPECT_TRUE(backend->get("p/gc/server-roots/victim/mount").has_value())
        << "slot kept -- resume anchor";
    EXPECT_TRUE(backend->head(debris_key).exists)
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
    EXPECT_TRUE(backend->head(debris_key).exists);
    EXPECT_TRUE(backend->get("p/gc/server-roots/victim/mount").has_value())
        << "the slot is still the resume anchor -- nothing was retired against unreclaimed debris";
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
/// `decommissionPoolMember`'s own first step), let it close gracefully (the mount-lease keeper's
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
    for (const String & key : {layout.epochKey("victim"), layout.ownerKey("victim")})
    {
        const auto head = backend->head(key);
        ASSERT_TRUE(head.exists);
        backend->deleteExact(key, head.token);
    }
    ASSERT_FALSE(backend->get(layout.epochKey("victim")).has_value());
    ASSERT_FALSE(backend->get(layout.ownerKey("victim")).has_value());
    ASSERT_TRUE(backend->get(layout.mountKey("victim")).has_value())
        << "the mount lease must survive -- it is the resume anchor the fallback reads";

    const auto report = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "a2"}, "victim");

    EXPECT_TRUE(report.warnings.empty());
    EXPECT_EQ(report.namespaces_removed, 0u);
    EXPECT_TRUE(report.slot_removed);
    EXPECT_FALSE(backend->get(layout.epochKey("victim")).has_value());
    const auto owner = backend->get(layout.ownerKey("victim"));
    ASSERT_TRUE(owner.has_value());
    EXPECT_TRUE(decodeOwner(owner->bytes).retired_at_ms.has_value());
    EXPECT_FALSE(backend->get(layout.mountKey("victim")).has_value());

    expectThrowsCode(ErrorCodes::CORRUPTED_DATA, [&]
    {
        decommissionPoolMember(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "a3"}, "victim");
    });
}
