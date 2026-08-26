#include "cas_test_helpers.h"

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasDecommission.h>

#include <algorithm>
#include <atomic>
#include <optional>

namespace DB::ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

using namespace DB;
using namespace DB::Cas;
using namespace DB::Cas::tests;

namespace
{

PoolPtr openVictim(const std::shared_ptr<InMemoryBackend> & backend)
{
    return Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "victim"});
}

CatalogEntry catalogEntry(Backend & backend, const Layout & layout, const RootNamespace & ns)
{
    const RefCatalog catalog = CasRefCatalog::read(backend, layout).catalog;
    const auto it = std::find_if(catalog.entries.begin(), catalog.entries.end(),
        [&](const CatalogEntry & entry) { return entry.ns == ns; });
    if (it == catalog.entries.end())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "fixture catalog entry '{}' is absent", ns.string());
    return *it;
}

void makeRemoving(Backend & backend, const Layout & layout, const CatalogEntry & live)
{
    CasRefCatalog::casUpdate(backend, layout, [&](const RefCatalog & current)
    {
        RefCatalog next = current;
        const auto it = std::find(next.entries.begin(), next.entries.end(), live);
        if (it == next.entries.end())
            throw Exception(ErrorCodes::CORRUPTED_DATA, "fixture exact Live row changed");
        it->state = NsState::Removing;
        it->removal_started_round = 0;
        return next;
    });
}

bool slotObjectExists(Backend & backend, const String & leaf)
{
    return backend.head("p/gc/server-roots/victim/" + leaf).exists;
}

class AddVictimEntryDuringRootDrainBackend final : public InMemoryBackend
{
public:
    void arm() { armed = true; }
    bool fired() const { return added; }

    ListPage list(const String & prefix, const String & cursor, size_t limit) override
    {
        ListPage page = InMemoryBackend::list(prefix, cursor, limit);
        if (armed && !added && prefix == "p/roots/victim/" && cursor.empty())
        {
            added = true;
            CasRefCatalog::casAdmitEntry(
                *this, Layout("p"), 1,
                CatalogEntry{
                    .ns = RootNamespace("victim/db/late"),
                    .state = NsState::Live,
                    .incarnation = UInt128{707}});
        }
        return page;
    }

private:
    bool armed = false;
    bool added = false;
};

/// Admits the late catalog entry between the retirement tail's two exact catalog reads
/// (`retirement_catalog_cut`, then `fresh_retirement_catalog`), never before. The mountpoint drain's
/// `list("p/roots/victim/", ...)` is the last LIST call in `decommissionPoolMember` before either
/// read, so it orders the two `get("p/cas/ref_catalog")` calls that follow it: the first is
/// `retirement_catalog_cut`, the second is `fresh_retirement_catalog`. Mutating on the second call
/// makes that read observe a catalog the first read did not.
class MutateCatalogBetweenRetirementReadsBackend final : public InMemoryBackend
{
public:
    void arm() { armed = true; }
    bool fired() const { return added; }

    ListPage list(const String & prefix, const String & cursor, size_t limit) override
    {
        ListPage page = InMemoryBackend::list(prefix, cursor, limit);
        if (armed && !past_mountpoint_drain && prefix == "p/roots/victim/" && cursor.empty())
            past_mountpoint_drain = true;
        return page;
    }

    std::optional<GetResult> get(const String & key, Range range) override
    {
        if (armed && past_mountpoint_drain && !added && key == "p/cas/ref_catalog")
        {
            if (!seen_retirement_catalog_cut)
                seen_retirement_catalog_cut = true;
            else
            {
                added = true;
                CasRefCatalog::casAdmitEntry(
                    *this, Layout("p"), 1,
                    CatalogEntry{
                        .ns = RootNamespace("victim/db/late"),
                        .state = NsState::Live,
                        .incarnation = UInt128{707}});
            }
        }
        return InMemoryBackend::get(key, range);
    }

private:
    bool armed = false;
    bool past_mountpoint_drain = false;
    bool seen_retirement_catalog_cut = false;
    bool added = false;
};

TEST(CASDecommissionCatalogDuties, RemovingWithoutCheckpointIsCorruptionAndKeepsSlot)
{
    auto backend = std::make_shared<InMemoryBackend>();
    {
        auto victim = openVictim(backend);
        const CatalogEntry live{
            .ns = RootNamespace("victim/db/missing_ckpt"),
            .state = NsState::Live,
            .incarnation = UInt128{701}};
        CasRefCatalog::casAdmitEntry(
            *backend, victim->layout(), victim->poolConfig().gc_shards,
            live);
        makeRemoving(*backend, victim->layout(), live);
    }

    expectThrowsCode(ErrorCodes::CORRUPTED_DATA, [&]
    {
        (void)decommissionPoolMember(
            backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");
    });

    EXPECT_TRUE(slotObjectExists(*backend, "owner"));
    EXPECT_TRUE(slotObjectExists(*backend, "epoch"));
    EXPECT_TRUE(slotObjectExists(*backend, "mount"));
    EXPECT_EQ(catalogEntry(*backend, Layout("p"), RootNamespace("victim/db/missing_ckpt")).state,
        NsState::Removing);
}

TEST(CASDecommissionCatalogDuties, RemovingWithCheckpointResumesTerminalAndKeepsSlotForGc)
{
    auto backend = std::make_shared<InMemoryBackend>();
    const RootNamespace ns("victim/db/pending_terminal");
    std::optional<NamespaceLifeId> life;
    {
        auto victim = openVictim(backend);
        life = victim->namespaceLife(ns);
        const CatalogEntry live = catalogEntry(*backend, victim->layout(), ns);
        makeRemoving(*backend, victim->layout(), live);
        ASSERT_TRUE(backend->head(victim->layout().refCkptKey(*life)).exists);
        ASSERT_TRUE(backend->list(victim->layout().namespaceStreamPrefix(*life), "", 100).keys.empty());
    }

    std::atomic<uint64_t> wake_requests{0};
    const DecommissionReport report = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim", {},
        [&] { wake_requests.fetch_add(1); });

    EXPECT_EQ(report.namespaces_already_removed, 1u);
    EXPECT_EQ(wake_requests.load(), 1u);
    EXPECT_FALSE(report.slot_removed);
    EXPECT_FALSE(report.warnings.empty());
    EXPECT_TRUE(slotObjectExists(*backend, "owner"));

    const ListPage stream = backend->list(Layout("p").namespaceStreamPrefix(*life), "", 100);
    ASSERT_EQ(stream.keys.size(), 1u);
    const auto parsed = Layout("p").parseRefObjectKey(stream.keys.front().key);
    ASSERT_TRUE(parsed);
    const auto body = backend->get(stream.keys.front().key);
    ASSERT_TRUE(body);
    const RefLogTxn terminal = decodeRefLogTxn(
        openObject(FormatId::RefLog, body->bytes), ns.string(), parsed->txn_id);
    ASSERT_EQ(terminal.ops.size(), 2u);
    EXPECT_EQ(terminal.ops.front().kind, RefOpKind::NamespaceBirth);
    EXPECT_EQ(terminal.ops.back().kind, RefOpKind::RemoveNamespace);
}

TEST(CASDecommissionCatalogDuties, PartialRemovalProgressStillWakesGcWhenLaterNamespaceFails)
{
    auto backend = std::make_shared<InMemoryBackend>();
    const RootNamespace progressed_ns("victim/db/a_progressed");
    const RootNamespace broken_ns("victim/db/z_missing_ckpt");
    std::optional<NamespaceLifeId> progressed_life;
    {
        auto victim = openVictim(backend);
        progressed_life = victim->namespaceLife(progressed_ns);
        const CatalogEntry progressed_live = catalogEntry(*backend, victim->layout(), progressed_ns);
        makeRemoving(*backend, victim->layout(), progressed_live);

        const CatalogEntry broken_live{
            .ns = broken_ns,
            .state = NsState::Live,
            .incarnation = UInt128{713}};
        CasRefCatalog::casAdmitEntry(
            *backend, victim->layout(), victim->poolConfig().gc_shards, broken_live);
        makeRemoving(*backend, victim->layout(), broken_live);
        ASSERT_FALSE(backend->head(victim->layout().refCkptKey(
            NamespaceLifeId::fromCatalogEntry(broken_ns, broken_live.incarnation))).exists);
    }

    std::atomic<uint64_t> wake_requests{0};
    expectThrowsCode(ErrorCodes::CORRUPTED_DATA, [&]
    {
        (void)decommissionPoolMember(
            backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim", {},
            [&] { wake_requests.fetch_add(1); });
    });

    EXPECT_EQ(wake_requests.load(), 1u)
        << "progress already made for an earlier life must wake GC even when a later life fails closed";
    EXPECT_TRUE(slotObjectExists(*backend, "owner"));
    const ListPage progressed_stream
        = backend->list(Layout("p").namespaceStreamPrefix(*progressed_life), "", 100);
    ASSERT_EQ(progressed_stream.keys.size(), 1u);
}

TEST(CASDecommissionCatalogDuties, VictimEntryAppearingBeforeTheOwnershipCutKeepsSlot)
{
    auto backend = std::make_shared<AddVictimEntryDuringRootDrainBackend>();
    { auto victim = openVictim(backend); }
    backend->arm();

    const DecommissionReport report = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");

    EXPECT_TRUE(backend->fired());
    EXPECT_FALSE(report.slot_removed);
    ASSERT_FALSE(report.warnings.empty());
    EXPECT_NE(report.warnings.front().find("pool member decommission underway: 1 namespace(s)"), String::npos)
        << report.warnings.front();
    EXPECT_TRUE(slotObjectExists(*backend, "owner"));
    EXPECT_EQ(catalogEntry(*backend, Layout("p"), RootNamespace("victim/db/late")).state, NsState::Live);
}

TEST(CASDecommissionCatalogDuties, CatalogTokenMovedBetweenOwnershipCutAndRetirementKeepsSlot)
{
    auto backend = std::make_shared<MutateCatalogBetweenRetirementReadsBackend>();
    { auto victim = openVictim(backend); }
    backend->arm();

    const DecommissionReport report = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");

    EXPECT_TRUE(backend->fired());
    EXPECT_FALSE(report.slot_removed);
    ASSERT_FALSE(report.warnings.empty());
    EXPECT_NE(report.warnings.front().find("catalog changed after the victim ownership check"), String::npos)
        << report.warnings.front();
    EXPECT_TRUE(slotObjectExists(*backend, "owner"));
    EXPECT_EQ(catalogEntry(*backend, Layout("p"), RootNamespace("victim/db/late")).state, NsState::Live);
}

TEST(CASDecommissionCatalogDuties, FoldedTerminalRemainsGcOwnedAndOnlyRequestsAnotherRound)
{
    auto backend = std::make_shared<InMemoryBackend>();
    const RootNamespace ns("victim/db/folded_terminal");
    std::optional<NamespaceLifeId> life;
    std::vector<String> stream_before;
    {
        PoolConfig config{
            .pool_prefix = "p",
            .server_root_id = "victim",
            .gc_fold_threshold = 1,
            .gc_fold_max_defer_rounds = 0};
        auto victim = Pool::open(backend, config);
        life = victim->namespaceLife(ns);
        victim->putNamespaceFile(*life, "format_version.txt", "1\n");
        victim->dropNamespace(ns);

        Gc gc(victim, UInt128{811});
        ASSERT_FALSE(runRegularRoundReclaiming(gc).deferred);
        ASSERT_EQ(catalogEntry(*backend, victim->layout(), ns).state, NsState::Removing);
        for (const ListedKey & key : backend->list(victim->layout().namespaceStreamPrefix(*life), "", 100).keys)
            stream_before.push_back(key.key);
        ASSERT_FALSE(stream_before.empty());
    }

    std::atomic<uint64_t> wake_requests{0};
    const DecommissionReport report = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim", {},
        [&] { wake_requests.fetch_add(1); });

    EXPECT_EQ(wake_requests.load(), 1u);
    EXPECT_EQ(report.namespaces_already_removed, 1u);
    EXPECT_FALSE(report.slot_removed);
    EXPECT_EQ(catalogEntry(*backend, Layout("p"), ns).state, NsState::Removing);
    std::vector<String> stream_after;
    for (const ListedKey & key : backend->list(Layout("p").namespaceStreamPrefix(*life), "", 100).keys)
        stream_after.push_back(key.key);
    EXPECT_EQ(stream_after, stream_before)
        << "decommission must not append a second terminal or become a catalog deletion driver";
}

TEST(CASDecommissionCatalogDuties, OpaqueLifeDebrisWithoutCatalogOwnershipDoesNotBlockRetirement)
{
    auto backend = std::make_shared<InMemoryBackend>();
    { auto victim = openVictim(backend); }
    const Layout layout("p");
    const NamespaceLifeId dead_life
        = NamespaceLifeId::fromCatalogEntry(RootNamespace("historical/name"), UInt128{709});
    const String debris_key = layout.refCkptKey(dead_life);
    ASSERT_EQ(backend->putIfAbsent(debris_key, "debris").outcome, PutOutcome::Done);

    const DecommissionReport report = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");

    EXPECT_TRUE(report.warnings.empty());
    EXPECT_TRUE(report.slot_removed);
    EXPECT_TRUE(backend->head(debris_key).exists);
}

}
