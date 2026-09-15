#include "cas_test_helpers.h"
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCatalogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCatalog.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCkpt.h>
/// Explicit rather than relying on a transitive path: `DEBUG_OR_SANITIZER_BUILD` (used below to gate
/// the `*DeathTest` split) must resolve in THIS translation unit.
#include <base/defines.h>
#include <fmt/format.h>

using namespace DB::Cas;

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NETWORK_ERROR;
}

/// Stage B Task 3 (spec §3, the ref-chain catalog's creation lifecycle): the three-conditional-write
/// sequence that carries a namespace from nothing to `Live` --
///   1. catalog CAS: insert `{ns, Creating, fresh incarnation, creator}` (`CasRefCatalog::createNamespace`,
///      built on Task 2's `casAdmitEntry`);
///   2. `_ckpt` create (`CasRefCatalog::completeCreation`, step 2 -- Stage A's `publishCkpt` unchanged);
///   3. catalog CAS: `Creating -> Live`, re-presenting the creator's admission GENERATION and
///      value-CASing the OBSERVED entry (`completeCreation`, step 3 -- the "ZombieGoLive" guard) --
/// plus stale-`Creating` reconciliation (`CasRefCatalog::reconcileStaleCreator`) and the publication
/// gate (`checkPublicationAdmittedOrThrow`).
///
/// The suite name is prefixed `Cas` so it is covered by the `Cas*` unit-test gate filter.

namespace
{

CreatorFence creatorFence(const String & srid, uint64_t writer_epoch, uint64_t fence_generation = 1)
{
    return CreatorFence{.server_root_id = srid, .writer_epoch = writer_epoch, .fence_generation = fence_generation};
}

/// A `is_creator_fence_terminal` stub that answers the same fixed verdict for every fence -- for tests
/// whose subject is not terminality itself (that predicate's own tests live in `gtest_cas_mount.cpp`,
/// next to `isCreatorFenceTerminal`, the real implementation this stub stands in for).
std::function<bool(const CreatorFence &)> fixedTerminality(bool terminal)
{
    return [terminal](const CreatorFence &) { return terminal; };
}

const CatalogEntry * findEntryForTest(const RefCatalog & catalog, const RootNamespace & ns)
{
    for (const CatalogEntry & e : catalog.entries)
        if (e.ns.string() == ns.string())
            return &e;
    return nullptr;
}

/// Withdraws an operation's admission, and lets a test smuggle a real concurrent write, at one chosen
/// point of the creation sequence: once this namespace's `_ckpt` is durable (step 2 landed, step 3 has
/// not run), or inside step 3's own read-then-write window. The `_ckpt` key carries an incarnation a
/// test cannot know before the creation mints it, so that arm names the object kind rather than a key.
///
/// The read arm fires BEFORE the store is consulted, so the body the caller's `decide` receives already
/// carries whatever the hook wrote. That is what lets a test make the observed body stale on BOTH axes
/// -- a changed entry and a withdrawn admission -- inside ONE `decide` invocation, which is the only
/// place the two can be told apart.
class CreationHookBackend : public InMemoryBackend
{
public:
    bool admitted = true;
    /// Withdraw once any `_ckpt` key has been written.
    bool withdraw_after_ckpt_write = false;
    /// Fires once before this key is read, then `withdraw_on_read` is applied.
    String hook_before_read_of;
    std::function<void()> on_read;
    bool withdraw_on_read = false;

    std::optional<Raw> read(const String & key, TransportAccess & access) override
    {
        if (!hook_before_read_of.empty() && key == hook_before_read_of && !hook_fired)
        {
            /// Latched before running: the hook reads and writes through this same backend, and an
            /// unguarded re-entry would run the test's concurrent actor again against its own result.
            hook_fired = true;
            if (on_read)
                on_read();
            if (withdraw_on_read)
                admitted = false;
        }
        return InMemoryBackend::read(key, access);
    }

    std::expected<String, RawConflict> write(const String & key, const String & bytes,
                                             const std::optional<String> & expected_value,
                                             TransportAccess & access) override
    {
        auto result = InMemoryBackend::write(key, bytes, expected_value, access);
        if (withdraw_after_ckpt_write && Layout{"p"}.parseRefCkptKey(key))
            admitted = false;
        return result;
    }

private:
    bool hook_fired = false;
};

/// Raw lifecycle tests operate below `Pool::open`, so model an already-bootstrapped pool explicitly.
std::shared_ptr<CreationHookBackend> initializedCatalogBackend()
{
    auto backend = std::make_shared<CreationHookBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    CasRefCatalog::initializeEmptyForNewPool(op, Layout("p"));
    return backend;
}

}

/// ---------------------------------------------------------------------------------------------
/// Happy path: all three writes land
/// ---------------------------------------------------------------------------------------------

TEST(CASNsCreationLifecycle, HappyPathReachesLiveWithADurableCkptAndAStableIncarnation)
{
    auto backend = initializedCatalogBackend();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    Layout layout("p");
    const RootNamespace ns{"a"};
    const CreatorFence creator = creatorFence("srv1", /*writer_epoch=*/5);

    const auto outcome = CasRefCatalog::createNamespace(
        op, layout, 1, ns, creator);
    EXPECT_EQ(outcome, CasRefCatalog::NamespaceCreationOutcome::Live);

    const CasRefCatalog::Snapshot snap = CasRefCatalog::read(op, layout);
    const CatalogEntry * entry = findEntryForTest(snap.catalog, ns);
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry->state, NsState::Live);
    EXPECT_EQ(entry->creator, std::nullopt) << "creator is forbidden outside Creating (strict grammar)";
    const UInt128 incarnation = entry->incarnation;
    EXPECT_NE(incarnation, UInt128(0));

    const std::optional<CkptSample> ckpt = readCkpt(op, layout, NamespaceLifeId::fromCatalogEntry(entry->ns, incarnation));
    ASSERT_TRUE(ckpt.has_value()) << "step 2's _ckpt must be durable";
    EXPECT_EQ(ckpt->ckpt.life_epoch, 5u) << "INV-4's genesis epoch is the creator's writer_epoch";

    /// Re-reading the catalog again must show the SAME incarnation -- nothing mints a second one.
    EXPECT_EQ(CasRefCatalog::read(op, layout).catalog.entries.at(0).incarnation, incarnation);
}

/// ---------------------------------------------------------------------------------------------
/// `createNamespace`'s pre-check reports `Superseded` for a namespace that already has an entry, in
/// EVERY state -- not a caller bug, but a sibling opener of the same namespace (CI PR#2300 run 3,
/// `tiered_storage_cas`: seven concurrent `MergeTreeBackgroundExecutor` movers) that landed somewhere in
/// its own three-step sequence between the caller's "no entry" read and this pre-check's read.
/// ---------------------------------------------------------------------------------------------

TEST(CASNsCreationLifecycle, CreateNamespaceRacingASiblingsLiveEntryReportsSupersededNotAbort)
{
    auto backend = initializedCatalogBackend();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    Layout layout("p");
    const RootNamespace ns{"a"};
    const CreatorFence creator = creatorFence("srv1", 1);
    ASSERT_EQ(CasRefCatalog::createNamespace(op, layout, 1, ns, creator),
               CasRefCatalog::NamespaceCreationOutcome::Live);

    EXPECT_EQ(CasRefCatalog::createNamespace(op, layout, 1, ns, creatorFence("srv2", 2)),
              CasRefCatalog::NamespaceCreationOutcome::Superseded);

    /// The refused call left the winner's `Live` entry exactly as it was.
    const CasRefCatalog::Snapshot snap = CasRefCatalog::read(op, layout);
    const CatalogEntry * entry = findEntryForTest(snap.catalog, ns);
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry->state, NsState::Live);
    EXPECT_EQ(entry->creator, std::nullopt);
}

TEST(CASNsCreationLifecycle, CreateNamespaceRacingASiblingsRemovingEntryReportsSupersededNotAbort)
{
    auto backend = initializedCatalogBackend();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    Layout layout("p");
    const RootNamespace ns{"a"};
    const CreatorFence creator = creatorFence("srv1", 1);
    ASSERT_EQ(CasRefCatalog::createNamespace(op, layout, 1, ns, creator),
               CasRefCatalog::NamespaceCreationOutcome::Live);
    ASSERT_EQ(CasRefCatalog::beginRemoving(op, layout, *findEntryForTest(CasRefCatalog::read(op, layout).catalog, ns),
                                          /*removal_started_round=*/1),
              CasRefCatalog::BeginRemovingOutcome::Transitioned);

    EXPECT_EQ(CasRefCatalog::createNamespace(op, layout, 1, ns, creatorFence("srv2", 2)),
              CasRefCatalog::NamespaceCreationOutcome::Superseded);

    /// The refused call left the concurrent drop's `Removing` entry exactly as it was.
    const CasRefCatalog::Snapshot snap = CasRefCatalog::read(op, layout);
    const CatalogEntry * entry = findEntryForTest(snap.catalog, ns);
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry->state, NsState::Removing);
}

/// ---------------------------------------------------------------------------------------------
/// `Creating` forbids publication
/// ---------------------------------------------------------------------------------------------

TEST(CASNsCreationLifecycle, CreatingForbidsPublication)
{
    RefCatalog catalog;
    catalog.entries.push_back(CatalogEntry{.ns = RootNamespace{"a"}, .state = NsState::Creating,
        .incarnation = UInt128(1), .creator = creatorFence("srv1", 1)});
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR,
        [&] { CasRefCatalog::checkPublicationAdmittedOrThrow(catalog, RootNamespace{"a"}); });
}

TEST(CASNsCreationLifecycle, LiveAndRemovingAndAbsentAllAdmitPublication)
{
    RefCatalog catalog;
    catalog.entries.push_back(CatalogEntry{.ns = RootNamespace{"live"}, .state = NsState::Live, .incarnation = UInt128(1)});
    catalog.entries.push_back(CatalogEntry{.ns = RootNamespace{"removing"}, .state = NsState::Removing, .incarnation = UInt128(2)});
    EXPECT_NO_THROW(CasRefCatalog::checkPublicationAdmittedOrThrow(catalog, RootNamespace{"live"}));
    EXPECT_NO_THROW(CasRefCatalog::checkPublicationAdmittedOrThrow(catalog, RootNamespace{"removing"}));
    EXPECT_NO_THROW(CasRefCatalog::checkPublicationAdmittedOrThrow(catalog, RootNamespace{"never-heard-of"}));
}

/// ---------------------------------------------------------------------------------------------
/// ZombieGoLive: fenced-out between the `_ckpt` publish and the `Creating -> Live` CAS
/// ---------------------------------------------------------------------------------------------

TEST(CASNsCreationLifecycle, FencedOutBetweenTheCkptPublishAndGoLiveRefusesAndLeavesEntryCreating)
{
    auto backend = initializedCatalogBackend();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    Layout layout("p");
    const RootNamespace ns{"a"};
    const CreatorFence creator = creatorFence("srv1", 5);

    /// Admission is withdrawn the instant step 2's `_ckpt` is durable, so step 3 never runs --
    /// "fenced out between the `_ckpt` create and the `Creating -> Live` write", without a second
    /// thread.
    backend->withdraw_after_ckpt_write = true;
    CasOperation creating_op = requests.admit([&backend] { return backend->admitted; });
    const auto outcome = CasRefCatalog::createNamespace(creating_op, layout, 1, ns, creator);
    EXPECT_EQ(outcome, CasRefCatalog::NamespaceCreationOutcome::FencedOut);
    backend->withdraw_after_ckpt_write = false;
    backend->admitted = true;

    const CasRefCatalog::Snapshot snap = CasRefCatalog::read(op, layout);
    const CatalogEntry * entry = findEntryForTest(snap.catalog, ns);
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry->state, NsState::Creating) << "step 3 never ran its CAS -- ZombieGoLive refuses before sending it";
    ASSERT_TRUE(entry->creator.has_value());
    EXPECT_EQ(*entry->creator, creator);

    /// Step 2's _ckpt DID land (it is not what the fence check gates) -- CKPT-FAILED-BIRTH-DEBRIS is a
    /// different mechanism (the OLD `RefOpKind::NamespaceBirth` writer, `Pool/CasRefLedger.cpp`); this
    /// driver's own `_ckpt` is simply left in place for whichever actor next reconciles this entry.
    EXPECT_TRUE(readCkpt(op, layout, NamespaceLifeId::fromCatalogEntry(entry->ns, entry->incarnation)).has_value());
}

/// Regression (CI PR#2073, `03611_freeze_partition_parallel_verbose` under `amd_tsan, cas s3 storage`):
/// sibling openers of the SAME namespace race `resolveNamespaceLife`'s "no entry" read the same way
/// concurrent per-part `ALTER TABLE ... FREEZE` threads race the table's one shadow-store namespace.
/// The loser's own `createNamespace` read lands AFTER the winner's step 1, observing `Creating` -- that
/// must send the loser back through the resume loop (`Superseded`), never abort the server.
TEST(CASNsCreationLifecycle, CreateNamespaceRacingASiblingsStillCreatingEntryReportsSupersededNotAbort)
{
    auto backend = initializedCatalogBackend();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    Layout layout("p");
    const RootNamespace ns{"a"};
    const CreatorFence winner = creatorFence("srv1", 1);

    /// Leaves the entry in `Creating` without reaching `Live` -- the same shape `resolveNamespaceLife`
    /// observes when a sibling thread's `casAdmitEntry` has landed but its `completeCreation` has not.
    backend->withdraw_after_ckpt_write = true;
    CasOperation winner_op = requests.admit([&backend] { return backend->admitted; });
    const auto winner_outcome = CasRefCatalog::createNamespace(winner_op, layout, 1, ns, winner);
    ASSERT_EQ(winner_outcome, CasRefCatalog::NamespaceCreationOutcome::FencedOut);
    backend->withdraw_after_ckpt_write = false;
    backend->admitted = true;
    ASSERT_EQ(CasRefCatalog::read(op, layout).catalog.entries.at(0).state, NsState::Creating);

    /// The loser: a second call, as if a sibling thread's own outer "no entry" read had raced ahead of
    /// this one. Same fence as the winner (sibling threads of one query share a mount's fence) --
    /// exercising exactly the case `resolveNamespaceLife`'s "own fence -> completeCreation" branch is
    /// built to resume, never a `LOGICAL_ERROR` abort.
    const auto loser_outcome = CasRefCatalog::createNamespace(
        op, layout, 1, ns, winner);
    EXPECT_EQ(loser_outcome, CasRefCatalog::NamespaceCreationOutcome::Superseded);

    /// Nothing about the winner's own still-`Creating` entry was disturbed by the loser's refused call.
    const CasRefCatalog::Snapshot snap = CasRefCatalog::read(op, layout);
    const CatalogEntry * entry = findEntryForTest(snap.catalog, ns);
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry->state, NsState::Creating);
    ASSERT_TRUE(entry->creator.has_value());
    EXPECT_EQ(*entry->creator, winner);
}

/// Second catch-point of the same CI PR#2073 race, distinct from the test above. That test starts the
/// winner FIRST, so the loser's own outer pre-check read (`createNamespace`'s `read(...)` before step
/// 1) already observes `Creating` and takes the fast top-of-function refusal. This test instead lands
/// the winner's ENTIRE `createNamespace` call inside the window between the loser's pre-check read
/// (which observes NOTHING) and the loser's own step 1 read -- the shape CI actually hit as an
/// encode-time `LOGICAL_ERROR` ("entries are not canonically ordered ... no duplicate namespace"), not
/// the top-of-function one: both openers pass the pre-check, so both proceed to admit a row for the
/// same namespace, and only `createNamespaceStep1`'s own per-read recheck (not `createNamespace`'s
/// single upfront read) can catch it.
TEST(CASNsCreationLifecycle, CreateNamespaceRacingASiblingsFullCreateBetweenPreCheckAndStep1ReportsSupersededNotAbort)
{
    auto backend = initializedCatalogBackend();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    Layout layout("p");
    const RootNamespace ns{"a"};
    const CreatorFence winner = creatorFence("srv1", 1);
    const CreatorFence loser = creatorFence("srv1", 2);

    /// Fires exactly once, inside the LOSER's `createNamespace` call, after its pre-check read already
    /// observed no entry -- synchronously runs the winner's own full `createNamespace` to completion
    /// (all the way to `Live`) before the loser's step 1 performs its own first read. The production
    /// call site swaps the hook into a local before invoking it, so the global is already empty by the
    /// time this body runs: the winner's own nested call, and every later call in the test, run
    /// hook-free without this body needing to clear it itself.
    CasRefCatalog::setCreateNamespaceStep1PreReadHookForTest([&]
    {
        const auto winner_outcome = CasRefCatalog::createNamespace(
            op, layout, 1, ns, winner);
        ASSERT_EQ(winner_outcome, CasRefCatalog::NamespaceCreationOutcome::Live);
    });

    const auto loser_outcome = CasRefCatalog::createNamespace(
        op, layout, 1, ns, loser);
    EXPECT_EQ(loser_outcome, CasRefCatalog::NamespaceCreationOutcome::Superseded);

    /// Exactly one row for `ns`, owned by the winner, at `Live` -- the loser's refused admission left
    /// no trace and did not disturb it.
    const CasRefCatalog::Snapshot snap = CasRefCatalog::read(op, layout);
    size_t rows_for_ns = 0;
    for (const CatalogEntry & e : snap.catalog.entries)
        if (e.ns.string() == ns.string())
            ++rows_for_ns;
    EXPECT_EQ(rows_for_ns, 1u);
    const CatalogEntry * entry = findEntryForTest(snap.catalog, ns);
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry->state, NsState::Live);
    EXPECT_FALSE(entry->creator.has_value()) << "Live entries carry no creator fence";
}

/// ---------------------------------------------------------------------------------------------
/// Entry-stale: the observed entry no longer matches at the `Creating -> Live` write
/// ---------------------------------------------------------------------------------------------

TEST(CASNsCreationLifecycle, EntryStolenByAConcurrentReconcilerRefusesGoLiveAndLeavesTheStolenEntryAlone)
{
    auto backend = initializedCatalogBackend();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    Layout layout("p");
    const RootNamespace ns{"a"};
    const CreatorFence original_creator = creatorFence("srv1", 5);
    const CreatorFence thief = creatorFence("srv2", 9);

    /// Write 1 only -- models "crash after write 1": no _ckpt yet, entry still Creating.
    const CatalogEntry entry{.ns = ns, .state = NsState::Creating, .incarnation = UInt128(42), .creator = original_creator};
    CasRefCatalog::casAdmitEntry(op, layout, 1, entry);

    /// A REAL concurrent write, smuggled in just before step 3's own catalog read -- the first one
    /// `completeCreation` performs, since step 2 touches only the `_ckpt`. `decide` therefore receives
    /// the POST-steal body and refuses it without sending anything, which is what the entry check is
    /// for. The steal itself must succeed (asserted), so the mismatch is the entry ACTUALLY changing,
    /// not a contrived stub. It runs on its own operation and its own `CasRequests` (a rival is
    /// another server; one server's writers never race each other on the pool's hot-key lane).
    CasRequests rival_requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation thief_op = rival_requests.admit();
    backend->hook_before_read_of = layout.refCatalogKey();
    backend->on_read = [&]
    {
        ASSERT_EQ(CasRefCatalog::reconcileStaleCreator(thief_op, layout, entry, thief, fixedTerminality(true)),
                   CasRefCatalog::ReconcileCreatorOutcome::Reconciled);
    };

    const auto outcome = CasRefCatalog::completeCreation(op, layout, entry);
    EXPECT_EQ(outcome, CasRefCatalog::NamespaceCreationOutcome::Superseded);
    backend->on_read = nullptr;

    /// `read`'s `Snapshot` is bound to a name here, not chained through a temporary: a `const
    /// CatalogEntry *` taken from `.catalog` of an unbound temporary dangles the instant the full
    /// expression ends, which every other site in this file (and the copy/paste that spread it) got
    /// wrong until ASan caught it.
    const CasRefCatalog::Snapshot snap_after = CasRefCatalog::read(op, layout);
    const CatalogEntry * after = findEntryForTest(snap_after.catalog, ns);
    ASSERT_NE(after, nullptr);
    EXPECT_EQ(after->state, NsState::Creating) << "the ORIGINAL creator's attempt wrote nothing -- only the thief's CAS did";
    ASSERT_TRUE(after->creator.has_value());
    EXPECT_EQ(*after->creator, thief) << "the entry is exactly what the thief left it as, untouched by our refused attempt";
    EXPECT_EQ(after->incarnation, entry.incarnation) << "reconciliation never mints a fresh incarnation";
}

/// ---------------------------------------------------------------------------------------------
/// Both stale at once: fence moved AND the entry was stolen -- refused (fence checked first).
/// ---------------------------------------------------------------------------------------------

TEST(CASNsCreationLifecycle, BothFenceAndEntryStaleRefusesGoLiveViaTheFenceCheckWhichRunsFirst)
{
    auto backend = initializedCatalogBackend();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    Layout layout("p");
    const RootNamespace ns{"a"};
    const CreatorFence original_creator = creatorFence("srv1", 5);
    const CreatorFence thief = creatorFence("srv2", 9);

    const CatalogEntry entry{.ns = ns, .state = NsState::Creating, .incarnation = UInt128(42), .creator = original_creator};
    CasRefCatalog::casAdmitEntry(op, layout, 1, entry);

    /// The same steal as the test above and in the same window, but this one ALSO withdraws admission
    /// there. Because the hook runs BEFORE the read, the single `decide` invocation that follows sees a
    /// body that is stale on both axes at once -- and that is the only situation in which the two
    /// checks are distinguishable. `completeCreation` consults admission before it compares the entry,
    /// so the answer is `FencedOut`.
    ///
    /// This is what makes the test discriminate rather than merely pass: delete the `op.admitted()`
    /// check from that `mutate` and the entry check answers `Superseded` instead, because `decide`
    /// refuses the stale entry before any write is sent and the engine's own gate never speaks. The
    /// assertions below confirm the entry really did change too.
    /// The rival gets its own `CasRequests`, on its own hot-key lane (a rival is another server; one
    /// server's writers never race each other on the pool's lane).
    CasRequests rival_requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation thief_op = rival_requests.admit();
    CasOperation creator_op = requests.admit([&backend] { return backend->admitted; });
    backend->hook_before_read_of = layout.refCatalogKey();
    backend->withdraw_on_read = true;
    backend->on_read = [&]
    {
        ASSERT_EQ(CasRefCatalog::reconcileStaleCreator(thief_op, layout, entry, thief, fixedTerminality(true)),
                   CasRefCatalog::ReconcileCreatorOutcome::Reconciled);
    };

    const auto outcome = CasRefCatalog::completeCreation(creator_op, layout, entry);
    EXPECT_EQ(outcome, CasRefCatalog::NamespaceCreationOutcome::FencedOut)
        << "both would refuse; admission speaks first by the documented ordering";
    backend->on_read = nullptr;
    backend->withdraw_on_read = false;
    backend->admitted = true;

    const CasRefCatalog::Snapshot snap_after = CasRefCatalog::read(op, layout);
    const CatalogEntry * after = findEntryForTest(snap_after.catalog, ns);
    ASSERT_NE(after, nullptr);
    ASSERT_TRUE(after->creator.has_value());
    EXPECT_EQ(*after->creator, thief) << "confirms the entry axis really did go stale too, not just the fence";
}

/// ---------------------------------------------------------------------------------------------
/// Stale-`Creating` reconciliation
/// ---------------------------------------------------------------------------------------------

TEST(CASNsCreationLifecycle, ReconcileRefusedWhileTheOriginalCreatorFenceIsStillLive)
{
    auto backend = initializedCatalogBackend();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    Layout layout("p");
    const RootNamespace ns{"a"};
    const CatalogEntry entry{.ns = ns, .state = NsState::Creating, .incarnation = UInt128(7),
                              .creator = creatorFence("srv1", 5)};
    CasRefCatalog::casAdmitEntry(op, layout, 1, entry);

    const auto outcome = CasRefCatalog::reconcileStaleCreator(
        op, layout, entry, creatorFence("srv2", 9), fixedTerminality(false));
    EXPECT_EQ(outcome, CasRefCatalog::ReconcileCreatorOutcome::CreatorFenceStillLive);

    const CasRefCatalog::Snapshot snap_after = CasRefCatalog::read(op, layout);
    const CatalogEntry * after = findEntryForTest(snap_after.catalog, ns);
    ASSERT_NE(after, nullptr);
    EXPECT_EQ(*after, entry) << "refused -- nothing written";
}

TEST(CASNsCreationLifecycle, ReconcileSucceedsTokenExactlyAfterTheOriginalCreatorFenceIsTerminalThenResumesToLive)
{
    auto backend = initializedCatalogBackend();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    Layout layout("p");
    const RootNamespace ns{"a"};
    const CreatorFence original_creator = creatorFence("srv1", 5);
    const CreatorFence new_creator = creatorFence("srv2", 9);
    const CatalogEntry entry{.ns = ns, .state = NsState::Creating, .incarnation = UInt128(7), .creator = original_creator};
    CasRefCatalog::casAdmitEntry(op, layout, 1, entry);   /// "crash after write 1" -- no _ckpt yet

    ASSERT_EQ(CasRefCatalog::reconcileStaleCreator(op, layout, entry, new_creator, fixedTerminality(true)),
               CasRefCatalog::ReconcileCreatorOutcome::Reconciled);

    CatalogEntry taken_over = entry;
    taken_over.creator = new_creator;
    const CasRefCatalog::Snapshot snap_mid = CasRefCatalog::read(op, layout);
    const CatalogEntry * mid = findEntryForTest(snap_mid.catalog, ns);
    ASSERT_NE(mid, nullptr);
    EXPECT_EQ(*mid, taken_over) << "creator moved to the new actor; state and incarnation unchanged";

    const auto outcome = CasRefCatalog::completeCreation(
        op, layout, taken_over);
    EXPECT_EQ(outcome, CasRefCatalog::NamespaceCreationOutcome::Live);

    const CasRefCatalog::Snapshot snap_final = CasRefCatalog::read(op, layout);
    const CatalogEntry * final_entry = findEntryForTest(snap_final.catalog, ns);
    ASSERT_NE(final_entry, nullptr);
    EXPECT_EQ(final_entry->state, NsState::Live);
    EXPECT_EQ(final_entry->incarnation, entry.incarnation) << "the SAME incarnation throughout -- resumption, not rebirth";
    const std::optional<CkptSample> ckpt = readCkpt(op, layout, NamespaceLifeId::fromCatalogEntry(final_entry->ns, final_entry->incarnation));
    ASSERT_TRUE(ckpt.has_value());
    EXPECT_EQ(ckpt->ckpt.life_epoch, new_creator.writer_epoch)
        << "the RESUMING actor's writer_epoch is the genesis epoch that actually landed";
}

/// "Stale token at reconciliation -> fail closed": a SECOND reconciler racing the first, both reading
/// the SAME stale `observed` before either writes.
TEST(CASNsCreationLifecycle, ReconcileFailsClosedWhenTheEntryAlreadyChanged)
{
    auto backend = initializedCatalogBackend();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    Layout layout("p");
    const RootNamespace ns{"a"};
    const CatalogEntry entry{.ns = ns, .state = NsState::Creating, .incarnation = UInt128(7),
                              .creator = creatorFence("srv1", 5)};
    CasRefCatalog::casAdmitEntry(op, layout, 1, entry);

    const CreatorFence first_reconciler = creatorFence("srv2", 9);
    const CreatorFence second_reconciler = creatorFence("srv3", 11);

    ASSERT_EQ(CasRefCatalog::reconcileStaleCreator(op, layout, entry, first_reconciler, fixedTerminality(true)),
               CasRefCatalog::ReconcileCreatorOutcome::Reconciled);

    /// The second reconciler still holds the ORIGINAL `entry` it read before either of them wrote --
    /// token-exactness must refuse it even though the terminality predicate would still say yes.
    const auto outcome = CasRefCatalog::reconcileStaleCreator(
        op, layout, entry, second_reconciler, fixedTerminality(true));
    EXPECT_EQ(outcome, CasRefCatalog::ReconcileCreatorOutcome::EntryChanged);

    const CasRefCatalog::Snapshot snap_after = CasRefCatalog::read(op, layout);
    const CatalogEntry * after = findEntryForTest(snap_after.catalog, ns);
    ASSERT_NE(after, nullptr);
    ASSERT_TRUE(after->creator.has_value());
    EXPECT_EQ(*after->creator, first_reconciler) << "the second reconciler's refused attempt changed nothing";
}

/// ---------------------------------------------------------------------------------------------
/// Preconditions: `completeCreation`/`reconcileStaleCreator` refuse anything but a `Creating` entry
/// with a creator fence -- a caller bug, not a race, hence `LOGICAL_ERROR`.
/// ---------------------------------------------------------------------------------------------

#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(CASNsCreationLifecycle, CompleteCreationRejectsANonCreatingEntry)
{
    auto backend = initializedCatalogBackend();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    Layout layout("p");
    const CatalogEntry live{.ns = RootNamespace{"a"}, .state = NsState::Live, .incarnation = UInt128(1)};
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR, [&]
    {
        CasRefCatalog::completeCreation(op, layout, live);
    });
}

TEST(CASNsCreationLifecycle, ReconcileStaleCreatorRejectsANonCreatingEntry)
{
    auto backend = initializedCatalogBackend();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    Layout layout("p");
    const CatalogEntry live{.ns = RootNamespace{"a"}, .state = NsState::Live, .incarnation = UInt128(1)};
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR, [&]
    {
        CasRefCatalog::reconcileStaleCreator(op, layout, live, creatorFence("srv2", 2), fixedTerminality(true));
    });
}
#endif

#if defined(DEBUG_OR_SANITIZER_BUILD)
TEST(CASNsCreationLifecycleDeathTest, CompleteCreationRejectsANonCreatingEntryAborts)
{
    auto backend = initializedCatalogBackend();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    Layout layout("p");
    const CatalogEntry live{.ns = RootNamespace{"a"}, .state = NsState::Live, .incarnation = UInt128(1)};
    EXPECT_DEATH(
        { CasRefCatalog::completeCreation(op, layout, live); },
        "not a Creating entry");
}

TEST(CASNsCreationLifecycleDeathTest, ReconcileStaleCreatorRejectsANonCreatingEntryAborts)
{
    auto backend = initializedCatalogBackend();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    Layout layout("p");
    const CatalogEntry live{.ns = RootNamespace{"a"}, .state = NsState::Live, .incarnation = UInt128(1)};
    EXPECT_DEATH(
        {
            CasRefCatalog::reconcileStaleCreator(op, layout, live, creatorFence("srv2", 2), fixedTerminality(true));
        },
        "not a Creating entry");
}
#endif
