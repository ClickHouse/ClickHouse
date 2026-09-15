#include "cas_test_helpers.h"
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasNamespaceJanitor.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcMaintenanceState.h>

using namespace DB::Cas;
using namespace DB::Cas::tests;

namespace DB::ErrorCodes
{
    extern const int NETWORK_ERROR;
}

namespace
{

/// `readGcMaintenanceState` now takes an admitted `CasOperation`, which cannot bind to an rvalue: every
/// call site below goes through this helper rather than materializing its own throwaway operation.
GcMaintenanceReadResult readState(CasRequests & requests, const Layout & layout)
{
    auto op = requests.admit();
    return readGcMaintenanceState(op, layout);
}

class OrderedJanitorBackend : public CountingBackend
{
public:
    std::vector<String> events;

    DB::Cas::Backend::RawListPage list(const String & prefix, const String & cursor, size_t limit,
                                       DB::Cas::TransportAccess & access) override
    {
        if (prefix.ends_with("/cas/ns/"))
            events.push_back("list");
        return CountingBackend::list(prefix, cursor, limit, access);
    }

    std::optional<DB::Cas::Backend::Raw> read(const String & key, DB::Cas::TransportAccess & access) override
    {
        if (key.ends_with("/cas/ref_catalog"))
            events.push_back("catalog");
        return CountingBackend::read(key, access);
    }
};

class OmitFirstNamespacePageBackend : public CountingBackend
{
public:
    DB::Cas::Backend::RawListPage list(const String & prefix, const String & cursor, size_t limit,
                                       DB::Cas::TransportAccess & access) override
    {
        if (omit && prefix.ends_with("/cas/ns/"))
        {
            omit = false;
            return {};
        }
        return CountingBackend::list(prefix, cursor, limit, access);
    }
private:
    bool omit = true;
};

/// Flips `delete_done` right after its one REMOVE returns, so a liveness predicate closing over it stays
/// true through every request up to and including that delete -- whatever their number or order -- and
/// only refuses the very next one. `liveness` is sampled before every request now, so driving a fence
/// loss at an exact point robustly (rather than by counting samples, which would couple this test to how
/// many reads the catalog snapshot happens to take) means keying it to an observable EVENT instead.
class FlipAfterFirstDeleteBackend : public CountingBackend
{
public:
    DB::Cas::Backend::RawRemoval remove(const String & key, const String & expected_value,
                                        DB::Cas::TransportAccess & access) override
    {
        DB::Cas::Backend::RawRemoval outcome = CountingBackend::remove(key, expected_value, access);
        delete_done = true;
        return outcome;
    }
    bool delete_done = false;
};

class ReplaceBeforeJanitorDeleteBackend : public CountingBackend
{
public:
    DB::Cas::Backend::RawRemoval remove(const String & key, const String & expected_value,
                                        DB::Cas::TransportAccess & access) override
    {
        if (!replaced)
        {
            replaced = true;
            /// The qualified primitive, exactly as the sibling concurrent-actor doubles in this file: a
            /// simulated concurrent write must not be counted as the janitor's own.
            const auto current = InMemoryBackend::read(key, access);
            if (current)
                (void)InMemoryBackend::write(key, "winner", current->value, access);
        }
        return CountingBackend::remove(key, expected_value, access);
    }
private:
    bool replaced = false;
};

class TokenlessListBackend : public CountingBackend
{
public:
    DB::Cas::Backend::RawListPage list(const String & prefix, const String & cursor, size_t limit,
                                       DB::Cas::TransportAccess & access) override
    {
        DB::Cas::Backend::RawListPage page = CountingBackend::list(prefix, cursor, limit, access);
        for (auto & key : page.keys)
            key.value.reset();
        return page;
    }

    bool supportsListTokens() const override { return false; }

    std::optional<DB::Cas::Backend::RawMeta> head(const String & key, DB::Cas::TransportAccess & access) override
    {
        std::optional<DB::Cas::Backend::RawMeta> result = CountingBackend::head(key, access);
        if (!replaced && result && key == replace_on_head)
        {
            replaced = true;
            /// The qualified primitive `write` -- not the legacy `head`/`casPut` convenience pair -- so
            /// this simulated concurrent actor neither re-enters the counted `head` override (the legacy
            /// forwarder calls back through the virtual primitive) nor is itself counted as a write the
            /// janitor made.
            (void)InMemoryBackend::write(key, "winner", result->value, access);
        }
        return result;
    }

    String replace_on_head;

private:
    bool replaced = false;
};

class FenceLossDuringHeadBackend : public TokenlessListBackend
{
public:
    std::optional<DB::Cas::Backend::RawMeta> head(const String & key, DB::Cas::TransportAccess & access) override
    {
        std::optional<DB::Cas::Backend::RawMeta> result = TokenlessListBackend::head(key, access);
        fence_held = false;
        return result;
    }

    bool fence_held = true;
};

class CatalogAfterListBackend : public CountingBackend
{
public:
    explicit CatalogAfterListBackend(NamespaceLifeId life_) : protected_life(std::move(life_)) {}

    DB::Cas::Backend::RawListPage list(const String & prefix, const String & cursor, size_t limit,
                                       DB::Cas::TransportAccess & access) override
    {
        DB::Cas::Backend::RawListPage page = CountingBackend::list(prefix, cursor, limit, access);
        if (!published && prefix.ends_with("/cas/ns/"))
        {
            published = true;
            const String catalog_key = "p/cas/ref_catalog";
            /// This models a CONCURRENT actor's read, not the janitor's own. It must go through the
            /// qualified PRIMITIVE, not the virtual `read`/`write` this class's base counts: the janitor's
            /// own catalog read reaches the store through that same virtual dispatch, and a call routed
            /// through it here would be indistinguishable from the janitor's -- doubling the count
            /// `PostListCatalogCutProtectsConcurrentCreationWithOneGet` asserts is exactly one.
            const auto current = InMemoryBackend::read(catalog_key, access); // NOLINT(bugprone-parent-virtual-call)
            if (current)
            {
                RefCatalog catalog;
                catalog.entries.push_back(CatalogEntry{.ns = protected_life.ns, .state = NsState::Live,
                    .incarnation = protected_life.incarnation});
                (void)InMemoryBackend::write(catalog_key, encodeRefCatalog(catalog), current->value, access);
            }
        }
        return page;
    }
private:
    NamespaceLifeId protected_life;
    bool published = false;
};

class RejectCursorBackend : public CountingBackend
{
public:
    DB::Cas::Backend::RawListPage list(const String & prefix, const String & cursor, size_t limit,
                                       DB::Cas::TransportAccess & access) override
    {
        if (prefix.ends_with("/cas/ns/") && !cursor.empty())
            throw std::runtime_error("backend rejected cursor");
        return CountingBackend::list(prefix, cursor, limit, access);
    }
};

class FailMaintenancePublicationBackend : public CountingBackend
{
public:
    std::expected<String, DB::Cas::Backend::RawConflict> write(const String & key, const String & bytes,
                                                               const std::optional<String> & expected_value,
                                                               DB::Cas::TransportAccess & access) override
    {
        if (fail_publication && key.ends_with("/gc/maintenance_state"))
            throw std::runtime_error("maintenance publication failed");
        return CountingBackend::write(key, bytes, expected_value, access);
    }
    bool fail_publication = false;
};

/// The catch-path reset in `NamespaceJanitor::runOnePage` fires on any LIST failure. Both faults here
/// throw a `DB::Exception` classified `NETWORK_ERROR`: a `std::runtime_error` is not a `Poco::Exception`,
/// so `CasOperation`'s engine treats it as an unmodeled local bug and surfaces it immediately on every
/// path (read or write) without ever reaching the ambiguity-resolving machinery this test needs -- a
/// `NETWORK_ERROR` is a genuine transient-looking store answer instead. The write additionally counts
/// its own attempts (`CountingBackend::writeTotal()` stays 0 here: this override throws before ever
/// delegating to the base `write`).
class ThrowingListAndAmbiguousWriteBackend : public CountingBackend
{
public:
    DB::Cas::Backend::RawListPage list(const String &, const String &, size_t,
                                       DB::Cas::TransportAccess &) override
    {
        throw DB::Exception(DB::ErrorCodes::NETWORK_ERROR, "list failed");
    }
    std::expected<String, DB::Cas::Backend::RawConflict> write(const String &, const String &,
                                                               const std::optional<String> &,
                                                               DB::Cas::TransportAccess &) override
    {
        ++write_attempts;
        throw DB::Exception(DB::ErrorCodes::NETWORK_ERROR, "ambiguous write");
    }
    uint64_t write_attempts = 0;
};

/// A one-shot `create`, asserting it committed (mirrors the retired `backend->putIfAbsent(key, bytes)`).
void createObj(Backend & backend, const String & key, const String & bytes)
{
    OperationForTest op(backend);
    ASSERT_TRUE(std::holds_alternative<Committed>((*op).create(key, bytes, Retry::once())));
}

/// An exact read (mirrors the retired `backend->get(key)`).
std::optional<Object> readObj(Backend & backend, const String & key)
{
    OperationForTest op(backend);
    return (*op).read(key, Retry::standard());
}

void seedCatalog(Backend & backend, const Layout & layout, RefCatalog catalog = {})
{
    createObj(backend, layout.refCatalogKey(), encodeRefCatalog(catalog));
}

NamespaceLifeId life(const char * name, uint64_t id)
{
    const RootNamespace ns{name};
    return NamespaceLifeId::fromCatalogEntry(ns, UInt128{id});
}

}

TEST(CASNamespaceJanitor, DeletesDeadFilesAndCheckpointFromOnePostListCatalogCut)
{
    auto backend = std::make_shared<CountingBackend>();
    CasRequests requests(backend, Fence::open());
    const Layout layout("p");
    seedCatalog(*backend, layout);
    const auto dead = life("dead", 41);
    const String file = layout.namespaceFilesPrefix(dead) + "part/data.bin";
    const String ckpt = layout.refCkptKey(dead);
    createObj(*backend, file, "file-bytes");
    createObj(*backend, ckpt, "ckpt-bytes");
    backend->resetCounts();

    NamespaceJanitor janitor(requests, layout, 100);
    const NamespaceJanitorResult result = janitor.runOnePage(false, [] { return true; });

    EXPECT_EQ(result.pages, 1u);
    EXPECT_EQ(result.keys, 2u);
    EXPECT_EQ(result.deleted, 2u);
    EXPECT_FALSE(readObj(*backend, file).has_value());
    EXPECT_FALSE(readObj(*backend, ckpt).has_value());
    EXPECT_EQ(backend->listCount(layout.namespaceRootPrefix()), 1u);
    EXPECT_EQ(backend->getCount(layout.refCatalogKey()), 1u);
    EXPECT_EQ(readState(requests, layout).state, GcMaintenanceState{});
}

TEST(CASNamespaceJanitor, RetainsEveryCurrentLifecycleAndSuppressesAmbiguousCut)
{
    auto backend = std::make_shared<CountingBackend>();
    CasRequests requests(backend, Fence::open());
    const Layout layout("p");
    RefCatalog catalog;
    CatalogEntry creating{.ns = RootNamespace{"creating"}, .state = NsState::Creating, .incarnation = UInt128{51},
        .creator = CreatorFence{.server_root_id = "srv", .writer_epoch = 1, .fence_generation = 1}};
    CatalogEntry live{.ns = RootNamespace{"live"}, .state = NsState::Live, .incarnation = UInt128{52}};
    CatalogEntry removing{.ns = RootNamespace{"removing"}, .state = NsState::Removing, .incarnation = UInt128{53},
        .removal_started_round = 1};
    catalog.entries = {creating, live, removing};
    seedCatalog(*backend, layout, catalog);
    for (const auto & entry : catalog.entries)
        createObj(*backend, layout.refCkptKey(
            NamespaceLifeId::fromCatalogEntry(entry.ns, entry.incarnation)), "keep");

    NamespaceJanitor janitor(requests, layout, 100);
    const auto result = janitor.runOnePage(false, [] { return true; });
    EXPECT_EQ(result.deleted, 0u);
    EXPECT_EQ(backend->deleteTotal(), 0u);
}

TEST(CASNamespaceJanitor, CatalogFirstCreatingRetainsEveryObjectOfTheNewLife)
{
    auto backend = std::make_shared<CountingBackend>();
    CasRequests requests(backend, Fence::open());
    const Layout layout("p");
    const CatalogEntry creating{
        .ns = RootNamespace{"catalog-first"},
        .state = NsState::Creating,
        .incarnation = UInt128{54},
        .creator = CreatorFence{.server_root_id = "srv", .writer_epoch = 2, .fence_generation = 3}};
    seedCatalog(*backend, layout, RefCatalog{.entries = {creating}});

    /// The production creation order is the point: the catalog row is durable before either object.
    const NamespaceLifeId creating_life
        = NamespaceLifeId::fromCatalogEntry(creating.ns, creating.incarnation);
    const String ckpt = layout.refCkptKey(creating_life);
    const String file = layout.namespaceFilesPrefix(creating_life) + "data";
    createObj(*backend, ckpt, "checkpoint");
    createObj(*backend, file, "file");
    backend->resetCounts();

    const NamespaceJanitorResult result
        = NamespaceJanitor(requests, layout, 100).runOnePage(false, [] { return true; });

    EXPECT_EQ(result.deleted, 0u);
    EXPECT_EQ(backend->deleteTotal(), 0u);
    EXPECT_EQ(backend->getCount(layout.refCatalogKey()), 1u);
    EXPECT_TRUE(readObj(*backend, ckpt).has_value());
    EXPECT_TRUE(readObj(*backend, file).has_value());
}

TEST(CASNamespaceJanitor, CancelledCreatingCheckpointIsReclaimedThroughPublicLifecycle)
{
    auto backend = std::make_shared<CountingBackend>();
    CasRequests requests(backend, Fence::open());
    const Layout layout("p");
    const CatalogEntry creating{
        .ns = RootNamespace{"cancelled"},
        .state = NsState::Creating,
        .incarnation = UInt128{55},
        .creator = CreatorFence{.server_root_id = "dead-srv", .writer_epoch = 4, .fence_generation = 5}};
    seedCatalog(*backend, layout, RefCatalog{.entries = {creating}});
    const String ckpt = layout.refCkptKey(
        NamespaceLifeId::fromCatalogEntry(creating.ns, creating.incarnation));
    createObj(*backend, ckpt, "cancelled-checkpoint");

    auto cancel_op = requests.admit();
    ASSERT_EQ(CasRefCatalog::cancelStalledCreating(
        cancel_op, layout, creating, [](const CreatorFence &) { return true; }),
        CasRefCatalog::StalledCreatingCancelOutcome::Cancelled);
    auto read_op = requests.admit();
    EXPECT_TRUE(CasRefCatalog::read(read_op, layout).catalog.entries.empty());

    const NamespaceJanitorResult result
        = NamespaceJanitor(requests, layout, 100).runOnePage(false, [] { return true; });
    EXPECT_EQ(result.deleted, 1u);
    EXPECT_FALSE(readObj(*backend, ckpt).has_value());
}

TEST(CASNamespaceJanitor, SuppressionAndFenceLossDeleteNothing)
{
    auto backend = std::make_shared<CountingBackend>();
    CasRequests requests(backend, Fence::open());
    const Layout layout("p");
    seedCatalog(*backend, layout);
    const String first = layout.refCkptKey(life("dead-a", 61));
    const String second = layout.refCkptKey(life("dead-b", 62));
    createObj(*backend, first, "first");
    createObj(*backend, second, "second");

    /// The seeding above (the catalog + the two checkpoints) lands through the same write primitive
    /// CountingBackend counts, so reset before measuring what the suppressed page itself does.
    backend->resetCounts();

    NamespaceJanitor janitor(requests, layout, 1);
    EXPECT_EQ(janitor.runOnePage(true, [] { return true; }).deleted, 0u);
    EXPECT_EQ(readState(requests, layout).status, GcMaintenanceReadStatus::Absent)
        << "a globally suppressed page is undecided and must not mint cleanup progress";
    EXPECT_EQ(backend->writeTotal(), 0u);

    /// `liveness` is sampled before every request the page makes, starting with the maintenance read
    /// itself -- a sample false from the start therefore ends the call by exception rather than by a
    /// quiet no-op result.
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR,
        [&] { (void)janitor.runOnePage(false, [] { return false; }); });
    EXPECT_EQ(readState(requests, layout).status, GcMaintenanceReadStatus::Absent)
        << "fence loss must not mint progress past a page whose deletion was not authorized";
    EXPECT_TRUE(readObj(*backend, first).has_value());
    EXPECT_TRUE(readObj(*backend, second).has_value());
    EXPECT_EQ(backend->deleteTotal(), 0u);
}

TEST(CASNamespaceJanitor, FenceLossOnRetainedOnlyPageDoesNotAdvanceCursor)
{
    auto backend = std::make_shared<CountingBackend>();
    CasRequests requests(backend, Fence::open());
    const Layout layout("p");
    const CatalogEntry current{
        .ns = RootNamespace{"current"}, .state = NsState::Live, .incarnation = UInt128{63}};
    seedCatalog(*backend, layout, RefCatalog{.entries = {current}});
    const NamespaceLifeId current_life
        = NamespaceLifeId::fromCatalogEntry(current.ns, current.incarnation);
    const String ckpt = layout.refCkptKey(current_life);
    const String file = layout.namespaceFilesPrefix(current_life) + "data";
    createObj(*backend, ckpt, "checkpoint");
    createObj(*backend, file, "file");

    /// A liveness sample false from the start is refused at the maintenance read, before the page ever
    /// gets to examine an object -- retained-only or not; the page ends by exception.
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR,
        [&] { (void)NamespaceJanitor(requests, layout, 1).runOnePage(false, [] { return false; }); });

    EXPECT_EQ(backend->deleteTotal(), 0u);
    EXPECT_TRUE(readObj(*backend, ckpt).has_value());
    EXPECT_TRUE(readObj(*backend, file).has_value());
    EXPECT_EQ(readState(requests, layout).status, GcMaintenanceReadStatus::Absent)
        << "a tenure that observes fence loss cannot publish progress even when every object was retained";
}

TEST(CASNamespaceJanitor, FenceLossAfterLastDeleteRetainsCursorWithoutRollingBackDelete)
{
    auto backend = std::make_shared<FlipAfterFirstDeleteBackend>();
    CasRequests requests(backend, Fence::open());
    const Layout layout("p");
    seedCatalog(*backend, layout);
    const String dead = layout.refCkptKey(life("dead-after-delete", 64));
    createObj(*backend, dead, "dead");

    const NamespaceJanitorResult result = NamespaceJanitor(requests, layout, 1).runOnePage(
        false, [&] { return !backend->delete_done; });

    EXPECT_EQ(result.deleted, 1u);
    EXPECT_FALSE(readObj(*backend, dead).has_value())
        << "the exact delete completed under the fence and is never rolled back";
    EXPECT_EQ(readState(requests, layout).status, GcMaintenanceReadStatus::Absent)
        << "losing the fence after the delete keeps this page selected for an idempotent retry";
}

TEST(CASNamespaceJanitor, CursorResumesThenResetsAtEnd)
{
    auto backend = std::make_shared<CountingBackend>();
    CasRequests requests(backend, Fence::open());
    const Layout layout("p");
    seedCatalog(*backend, layout);
    const auto dead = life("dead", 71);
    createObj(*backend, layout.namespaceFilesPrefix(dead) + "a", "a");
    createObj(*backend, layout.namespaceFilesPrefix(dead) + "b", "b");

    NamespaceJanitor first_process(requests, layout, 1);
    EXPECT_EQ(first_process.runOnePage(false, [] { return true; }).deleted, 1u);
    const auto mid = readState(requests, layout);
    ASSERT_EQ(mid.status, GcMaintenanceReadStatus::Valid);
    ASSERT_TRUE(mid.state);
    EXPECT_FALSE(mid.state->janitor_cursor.empty());
    NamespaceJanitor restarted_process(requests, layout, 1);
    EXPECT_EQ(restarted_process.runOnePage(false, [] { return true; }).deleted, 1u);
    EXPECT_TRUE(readState(requests, layout).state->janitor_cursor.empty());
}

TEST(CASNamespaceJanitor, TakesOneCatalogCutAfterListingAndContinuesPastMalformedKey)
{
    auto backend = std::make_shared<OrderedJanitorBackend>();
    CasRequests requests(backend, Fence::open());
    const Layout layout("p");
    seedCatalog(*backend, layout);
    const auto dead = life("dead", 81);
    const String valid = layout.namespaceFilesPrefix(dead) + "data";
    const String malformed = layout.namespaceStreamRootPrefix() + "not-a-life/_log/1-1.zst";
    const String malformed_state = layout.namespaceStateRootPrefix() + "not-a-life/_ckpt";
    createObj(*backend, valid, "v");
    createObj(*backend, malformed, "bad");
    createObj(*backend, malformed_state, "bad-state");
    backend->resetCounts();
    backend->events.clear();

    const auto result = NamespaceJanitor(requests, layout, 100).runOnePage(false, [] { return true; });
    EXPECT_EQ(result.deleted, 1u);
    EXPECT_FALSE(result.anomalies.empty());
    EXPECT_TRUE(readObj(*backend, malformed).has_value());
    EXPECT_TRUE(readObj(*backend, malformed_state).has_value());
    ASSERT_EQ(backend->events.size(), 2u);
    EXPECT_EQ(backend->events[0], "list");
    EXPECT_EQ(backend->events[1], "catalog");
    EXPECT_EQ(backend->getCount(layout.refCatalogKey()), 1u);
}

TEST(CASNamespaceJanitor, MalformedKeyIsFinalAndAdvancesCursor)
{
    auto backend = std::make_shared<CountingBackend>();
    CasRequests requests(backend, Fence::open());
    const Layout layout("p");
    seedCatalog(*backend, layout);
    const String first = layout.namespaceStreamRootPrefix() + "bad-a/_log/1-1.zst";
    const String second = layout.namespaceStreamRootPrefix() + "bad-b/_log/1-1.zst";
    createObj(*backend, first, "first");
    createObj(*backend, second, "second");

    const NamespaceJanitorResult result
        = NamespaceJanitor(requests, layout, 1).runOnePage(false, [] { return true; });

    EXPECT_EQ(result.deleted, 0u);
    EXPECT_FALSE(result.anomalies.empty());
    EXPECT_TRUE(readObj(*backend, first).has_value());
    EXPECT_TRUE(readObj(*backend, second).has_value());
    const GcMaintenanceReadResult progress = readState(requests, layout);
    ASSERT_EQ(progress.status, GcMaintenanceReadStatus::Valid);
    ASSERT_TRUE(progress.state);
    EXPECT_FALSE(progress.state->janitor_cursor.empty())
        << "malformed keys are surfaced and skipped, but do not pin the cleanup cycle";
}

TEST(CASNamespaceJanitor, DuplicateCurrentLifeSuppressesWholePage)
{
    auto backend = std::make_shared<CountingBackend>();
    CasRequests requests(backend, Fence::open());
    const Layout layout("p");
    RefCatalog catalog;
    catalog.entries = {
        CatalogEntry{.ns = RootNamespace{"a"}, .state = NsState::Live, .incarnation = UInt128{91}},
        CatalogEntry{.ns = RootNamespace{"b"}, .state = NsState::Live, .incarnation = UInt128{91}}};
    seedCatalog(*backend, layout, catalog);
    const String dead_a = layout.refCkptKey(life("dead-a", 92));
    const String dead_b = layout.refCkptKey(life("dead-b", 93));
    createObj(*backend, dead_a, "a");
    createObj(*backend, dead_b, "b");
    const auto result = NamespaceJanitor(requests, layout, 1).runOnePage(false, [] { return true; });
    EXPECT_EQ(result.deleted, 0u);
    EXPECT_EQ(backend->deleteTotal(), 0u);
    EXPECT_TRUE(readObj(*backend, dead_a).has_value());
    EXPECT_TRUE(readObj(*backend, dead_b).has_value());
    EXPECT_EQ(readState(requests, layout).status, GcMaintenanceReadStatus::Absent)
        << "an ambiguous catalog cut leaves the selected page undecided for an authoritative retry";
}

TEST(CASNamespaceJanitor, CorruptProgressResetsWithoutDeletingAndFilesOnlyOmittedCycleRetries)
{
    auto backend = std::make_shared<OmitFirstNamespacePageBackend>();
    CasRequests requests(backend, Fence::open());
    const Layout layout("p");
    seedCatalog(*backend, layout);
    const String dead = layout.namespaceFilesPrefix(life("dead", 101)) + "only-residue";
    createObj(*backend, dead, "bytes");
    createObj(*backend, layout.gcMaintenanceStateKey(), "corrupt");
    EXPECT_EQ(NamespaceJanitor(requests, layout, 100).runOnePage(false, [] { return true; }).deleted, 0u);
    EXPECT_TRUE(readObj(*backend, dead).has_value());
    EXPECT_EQ(readState(requests, layout).status, GcMaintenanceReadStatus::Valid);
    EXPECT_EQ(NamespaceJanitor(requests, layout, 100).runOnePage(false, [] { return true; }).deleted, 0u);
    EXPECT_TRUE(readObj(*backend, dead).has_value());
    EXPECT_EQ(NamespaceJanitor(requests, layout, 100).runOnePage(false, [] { return true; }).deleted, 1u);
    EXPECT_FALSE(readObj(*backend, dead).has_value());
}

TEST(CASNamespaceJanitor, ExactTokenMismatchRetainsConcurrentReplacement)
{
    auto backend = std::make_shared<ReplaceBeforeJanitorDeleteBackend>();
    CasRequests requests(backend, Fence::open());
    const Layout layout("p");
    seedCatalog(*backend, layout);
    const String dead = layout.refCkptKey(life("dead-a", 111));
    const String later = layout.refCkptKey(life("dead-b", 112));
    createObj(*backend, dead, "old");
    createObj(*backend, later, "later");
    const auto result = NamespaceJanitor(requests, layout, 1).runOnePage(false, [] { return true; });
    EXPECT_EQ(result.deleted, 0u);
    ASSERT_TRUE(readObj(*backend, dead).has_value());
    EXPECT_EQ(readObj(*backend, dead)->bytes, "winner");
    EXPECT_TRUE(readObj(*backend, later).has_value());
    const GcMaintenanceReadResult progress = readState(requests, layout);
    ASSERT_EQ(progress.status, GcMaintenanceReadStatus::Valid);
    ASSERT_TRUE(progress.state);
    EXPECT_FALSE(progress.state->janitor_cursor.empty())
        << "an exact-token mismatch retains the rewrite but completes this page's decision";
}

TEST(CASNamespaceJanitor, TokenlessListHeadsDeadKeysAndRetainsConcurrentReplacement)
{
    auto backend = std::make_shared<TokenlessListBackend>();
    CasRequests requests(backend, Fence::open());
    const Layout layout("p");
    const CatalogEntry current{
        .ns = RootNamespace{"current"}, .state = NsState::Live, .incarnation = UInt128{161}};
    seedCatalog(*backend, layout, RefCatalog{.entries = {current}});
    const String live_key = layout.refCkptKey(NamespaceLifeId::fromCatalogEntry(current.ns, current.incarnation));
    const String dead_key = layout.refCkptKey(life("dead", 162));
    const String raced_key = layout.namespaceFilesPrefix(life("raced", 163)) + "data";
    createObj(*backend, live_key, "live");
    createObj(*backend, dead_key, "dead");
    createObj(*backend, raced_key, "old");
    backend->replace_on_head = raced_key;
    backend->resetCounts();

    const auto result = NamespaceJanitor(requests, layout, 100).runOnePage(false, [] { return true; });

    EXPECT_EQ(result.deleted, 1u);
    EXPECT_TRUE(result.anomalies.empty());
    EXPECT_TRUE(readObj(*backend, live_key).has_value());
    EXPECT_FALSE(readObj(*backend, dead_key).has_value());
    ASSERT_TRUE(readObj(*backend, raced_key).has_value());
    EXPECT_EQ(readObj(*backend, raced_key)->bytes, "winner");
    EXPECT_EQ(backend->headCount(live_key), 0u);
    EXPECT_EQ(backend->headCount(dead_key), 1u);
    EXPECT_EQ(backend->headCount(raced_key), 1u);
    EXPECT_EQ(backend->deleteCount(dead_key), 1u);
    EXPECT_EQ(backend->deleteCount(raced_key), 1u);
}

TEST(CASNamespaceJanitor, TokenlessListRechecksFenceAfterHeadBeforeDelete)
{
    auto backend = std::make_shared<FenceLossDuringHeadBackend>();
    CasRequests requests(backend, Fence::open());
    const Layout layout("p");
    seedCatalog(*backend, layout);
    const String dead_key = layout.refCkptKey(life("dead", 164));
    createObj(*backend, dead_key, "dead");
    backend->resetCounts();

    const auto result = NamespaceJanitor(requests, layout, 100).runOnePage(
        false, [&] { return backend->fence_held; });

    EXPECT_EQ(result.deleted, 0u);
    EXPECT_EQ(backend->headCount(dead_key), 1u);
    EXPECT_EQ(backend->deleteCount(dead_key), 0u);
    EXPECT_TRUE(readObj(*backend, dead_key).has_value());
}

TEST(CASNamespaceJanitor, PostListCatalogCutProtectsConcurrentCreationWithOneGet)
{
    const auto created = life("created", 121);
    auto backend = std::make_shared<CatalogAfterListBackend>(created);
    CasRequests requests(backend, Fence::open());
    const Layout layout("p");
    seedCatalog(*backend, layout);
    const String first = layout.refCkptKey(created);
    const String second = layout.namespaceFilesPrefix(created) + "data";
    createObj(*backend, first, "ckpt");
    createObj(*backend, second, "file");
    backend->resetCounts();
    const auto result = NamespaceJanitor(requests, layout, 100).runOnePage(false, [] { return true; });
    EXPECT_EQ(result.deleted, 0u);
    EXPECT_EQ(backend->deleteTotal(), 0u);
    EXPECT_EQ(backend->getCount(layout.refCatalogKey()), 1u);
    EXPECT_TRUE(readObj(*backend, first).has_value());
    EXPECT_TRUE(readObj(*backend, second).has_value());
}

TEST(CASNamespaceJanitor, BackendRejectedCursorResetsExactlyAndDeletesNothing)
{
    auto backend = std::make_shared<RejectCursorBackend>();
    CasRequests requests(backend, Fence::open());
    const Layout layout("p");
    seedCatalog(*backend, layout);
    const String dead = layout.refCkptKey(life("dead", 131));
    createObj(*backend, dead, "bytes");
    createObj(*backend, layout.gcMaintenanceStateKey(),
        encodeGcMaintenanceState({.janitor_cursor = "rejected"}));
    EXPECT_THROW(NamespaceJanitor(requests, layout, 100).runOnePage(false, [] { return true; }), std::runtime_error);
    EXPECT_EQ(backend->deleteTotal(), 0u);
    EXPECT_TRUE(readObj(*backend, dead).has_value());
    EXPECT_TRUE(readState(requests, layout).state->janitor_cursor.empty());
}

TEST(CASNamespaceJanitor, CursorPublicationFailureIsLeakOnly)
{
    auto backend = std::make_shared<FailMaintenancePublicationBackend>();
    CasRequests requests(backend, Fence::open());
    const Layout layout("p");
    seedCatalog(*backend, layout);
    const String dead = layout.refCkptKey(life("dead", 141));
    createObj(*backend, dead, "bytes");
    backend->fail_publication = true;
    const auto result = NamespaceJanitor(requests, layout, 100).runOnePage(false, [] { return true; });
    EXPECT_EQ(result.deleted, 1u);
    EXPECT_FALSE(result.anomalies.empty());
    EXPECT_FALSE(readObj(*backend, dead).has_value());
}

/// The write inside `catch (...)` (the reset after a LIST failure) is admitted `once`: an unmodeled,
/// unresolvable write attempt must give up after its one exact resolve read rather than looping through
/// `Retry::standard()`'s backoff. `write_attempts == 1` is the discriminator: under `standard`, the same
/// unresolvable write would keep reissuing until the ninety-second policy window (the LIST failure is
/// itself a genuine `NETWORK_ERROR`, which the read engine retries to its OWN deadline before this catch
/// path is even entered -- so a raw sleep count is not a usable signal here, it is dirtied by the LIST's
/// unrelated retries regardless of which policy the catch-path write uses; the injected clock exists only
/// to keep both retry loops instant rather than to prove anything by its own emptiness).
TEST(CASGcMaintenanceState, CatchPathWriteIsOnce)
{
    auto backend = std::make_shared<ThrowingListAndAmbiguousWriteBackend>();
    FakeClock clock;
    CasRequests requests(backend, Fence::open(), clock.nowFn(), clock.sleepFn());
    const Layout layout("p");

    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR,
        [&] { (void)NamespaceJanitor(requests, layout, 100).runOnePage(false, [] { return true; }); });
    EXPECT_EQ(backend->write_attempts, 1u)
        << "the catch-path reset settles by its one resolve read and gives up rather than reissuing";
}

TEST(CASNamespaceJanitorIntegration, RegularGcRoundDeletesDeadNamespaceBytes)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace live_namespace{"00/live@cas@"};
    fixture::admitLive(*backend, layout, live_namespace);
    createObj(*backend, layout.refCkptKey(fixture::fixtureLife(live_namespace)),
        encodeRefCkpt(RefCkpt{.life_epoch = std::optional<uint64_t>{1},
                              .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt}));
    const String dead = layout.refCkptKey(life("dead", 151));
    createObj(*backend, dead, "checkpoint");

    std::map<String, UInt64> namespace_cleanup;
    Gc gc(store, UInt128{152});
    gc.setPhaseSink([&](const GcPhaseRecord & record)
    {
        if (record.phase == "namespace_cleanup")
            namespace_cleanup = record.metrics;
    });
    const RoundReport report = runRegularRoundReclaiming(gc);
    gc.setPhaseSink({});

    ASSERT_TRUE(report.acquired_lease);
    EXPECT_FALSE(readObj(*backend, dead).has_value());
    ASSERT_FALSE(namespace_cleanup.empty());
    EXPECT_EQ(namespace_cleanup["janitor_pages"], 1u);
    EXPECT_GE(namespace_cleanup["janitor_keys"], 1u);
    EXPECT_EQ(namespace_cleanup["janitor_deleted"], 1u);
}
