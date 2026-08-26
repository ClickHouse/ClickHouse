#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCatalogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCkptFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCkpt.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCatalog.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefProtocol.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>
#include <algorithm>
#include <functional>
#include <limits>
#include <map>

namespace DB::ErrorCodes
{
extern const int CORRUPTED_DATA;
extern const int INVALID_STATE;
}

using namespace DB::Cas;

namespace
{

using DB::Cas::tests::CountingBackend;
using DB::Cas::tests::minimalLiveSnapshot;
using DB::Cas::tests::namespaceBirthOp;
using DB::Cas::tests::publishCommittedOps;
using DB::Cas::tests::seedPoolMetaForRestart;
using DB::Cas::tests::writeRefSnapshotRaw;

enum class ListingMode : uint8_t
{
    Full,
    Empty,
    Partial,
    Reordered,
};

class RecoveryListingBackend : public CountingBackend
{
public:
    explicit RecoveryListingBackend(ListingMode mode_) : mode(mode_) { seedPoolMetaForRestart(*this); }

    size_t list_calls = 0;

    ListPage list(const String & prefix, const String & cursor, size_t limit) override
    {
        ++list_calls;
        ListPage page = CountingBackend::list(prefix, cursor, limit);
        if (mode == ListingMode::Empty)
            page.keys.clear();
        else if (mode == ListingMode::Partial)
        {
            page.keys.erase(std::remove_if(page.keys.begin(), page.keys.end(), [](const ListedKey & key)
            {
                return key.key.find("/_log/") != String::npos;
            }), page.keys.end());
        }
        else if (mode == ListingMode::Reordered)
            std::reverse(page.keys.begin(), page.keys.end());
        return page;
    }

private:
    ListingMode mode;
};

RefLogTxn txn(const RootNamespace & ns, RefTxnId id, std::vector<RefOp> ops,
              std::optional<RefTxnId> previous_seal = std::nullopt)
{
    return RefLogTxn{.ns = ns.string(), .txn_id = id, .ops = std::move(ops), .prev_epoch_seal = previous_seal};
}

std::map<String, ManifestRef> committedOf(const RefTableState & state)
{
    std::map<String, ManifestRef> result;
    for (const auto [name, row] : state.getCommitted())
        result.emplace(name, row.manifest_ref);
    return result;
}

void seedAuthoritativeStream(Backend & backend, const Layout & layout, const RootNamespace & ns,
                             RefTxnId committed_through, bool include_f_plus_one = false)
{
    const ManifestRef first{1, 1, 1};
    std::vector<RefOp> birth{namespaceBirthOp()};
    const auto first_publish = publishCommittedOps("a", first);
    birth.insert(birth.end(), first_publish.begin(), first_publish.end());
    const RefLogTxn first_txn = txn(ns, {1, 1}, std::move(birth));
    DB::Cas::tests::fixture::writeRefLogRaw(backend, layout, first_txn);

    if (committed_through > RefTxnId{1, 1})
    {
        RefOp seal_op;
        seal_op.kind = RefOpKind::EpochSeal;
        DB::Cas::tests::fixture::writeRefLogRaw(backend, layout, txn(ns, {1, 2}, {std::move(seal_op)}));
        const ManifestRef second{2, 1, 1};
        DB::Cas::tests::fixture::writeRefLogRaw(backend, layout,
            txn(ns, {2, 1}, publishCommittedOps("b", second), RefTxnId{1, 2}));
    }
    if (include_f_plus_one)
    {
        const ManifestRef extra{1, 2, 1};
        DB::Cas::tests::fixture::writeRefLogRaw(backend, layout, txn(ns, {1, 2}, publishCommittedOps("uncommitted", extra)));
    }

    RefTableState snapshot_state;
    applyRefLogTxn(snapshot_state, first_txn);
    writeRefSnapshotRaw(backend, layout, snapshotOf(snapshot_state, ns.string()));

    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(backend, layout, ns);
    const RefCkpt authority{
        .life_epoch = 1,
        .committed_through = committed_through,
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = committed_through.writer_epoch > 1
            ? std::optional<RefTxnId>{RefTxnId{1, 2}} : std::nullopt};
    backend.putIfAbsent(layout.refCkptKey(life), encodeRefCkpt(authority));
}

/// This is deliberately caller-side plumbing, not a convenience overload in `CasRefProtocol`: production
/// callers obtain `entry` from their frozen `RefPlan::catalogCut` and sample `_ckpt` in the same plan.
/// The API under test receives those exact values and performs no catalog or checkpoint resolution itself.
RecoveredRefTable recoverFromCurrentCatalogCut(Backend & backend, const Layout & layout, const RootNamespace & ns)
{
    const CasRefCatalog::Snapshot cut = CasRefCatalog::read(backend, layout);
    std::optional<CatalogEntry> entry;
    for (const CatalogEntry & candidate : cut.catalog.entries)
    {
        if (candidate.ns == ns)
        {
            entry = candidate;
            break;
        }
    }
    std::optional<RefCkpt> checkpoint;
    if (entry)
    {
        const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(entry->ns, entry->incarnation);
        if (const std::optional<CkptSample> sample = readCkpt(backend, layout, life))
            checkpoint = sample->ckpt;
    }
    return recoverRefTableDetailedFromAuthority(backend, layout, entry, checkpoint);
}

CatalogEntry catalog(NsState state)
{
    return CatalogEntry{.ns = RootNamespace{"srv1/recovery_grounding"}, .state = state, .incarnation = 1};
}

RefCkpt ckpt(uint64_t life_epoch, std::optional<RefTxnId> committed_through,
             std::optional<RefTxnId> checkpoint_snapshot_id = std::nullopt,
             std::optional<RefTxnId> last_epoch_seal = std::nullopt)
{
    return RefCkpt{.life_epoch = life_epoch,
                   .committed_through = committed_through,
                   .checkpoint_snapshot_id = checkpoint_snapshot_id,
                   .last_epoch_seal = last_epoch_seal};
}

void expectCode(const std::function<void()> & f, int code)
{
    try
    {
        f();
        FAIL() << "expected DB::Exception";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), code);
    }
}

TEST(CASRecoveryGrounding, CreatingAndAbsentCatalogEntriesAreNotRecovered)
{
    expectCode([&] { chooseRecoveryGrounding(catalog(NsState::Creating), ckpt(7, RefTxnId{7, 3})); },
               DB::ErrorCodes::INVALID_STATE);
    expectCode([&] { chooseRecoveryGrounding(std::nullopt, ckpt(7, RefTxnId{7, 3})); },
               DB::ErrorCodes::INVALID_STATE);
}

TEST(CASRecoveryGrounding, LiveAndRemovingRequireCheckpointAndLifeEpoch)
{
    expectCode([&] { chooseRecoveryGrounding(catalog(NsState::Live), std::nullopt); },
               DB::ErrorCodes::CORRUPTED_DATA);
    expectCode([&] { chooseRecoveryGrounding(catalog(NsState::Removing), RefCkpt{}); },
               DB::ErrorCodes::CORRUPTED_DATA);
}

TEST(CASRecoveryGrounding, MissingFrontierMeansNoCommittedTransaction)
{
    const RecoveryGrounding grounding = chooseRecoveryGrounding(catalog(NsState::Live), ckpt(7, std::nullopt));
    EXPECT_FALSE(grounding.base);
    EXPECT_FALSE(grounding.committed_through);
}

TEST(CASRecoveryGrounding, ChoosesCheckpointBaseAndArithmeticWalkStart)
{
    const RecoveryGrounding grounding = chooseRecoveryGrounding(
        catalog(NsState::Live), ckpt(7, RefTxnId{7, 8}, RefTxnId{7, 4}));
    EXPECT_EQ(grounding.base, (RefTxnId{7, 4}));
    EXPECT_EQ(grounding.walk_from, (RefTxnId{7, 5}));
    EXPECT_EQ(grounding.committed_through, (RefTxnId{7, 8}));
}

TEST(CASRecoveryGrounding, BaseAtFrontierStillStartsAtItsExactSuccessor)
{
    /// A writer recovery probes exactly this slot for its sole possible unfrontiered successor. The
    /// grounding contract must supply the arithmetic start even when the committed replay tail is empty.
    const RecoveryGrounding grounding = chooseRecoveryGrounding(
        catalog(NsState::Live), ckpt(7, RefTxnId{7, 8}, RefTxnId{7, 8}));

    EXPECT_EQ(grounding.base, (RefTxnId{7, 8}));
    EXPECT_EQ(grounding.walk_from, (RefTxnId{7, 9}));
    EXPECT_EQ(grounding.committed_through, (RefTxnId{7, 8}));
}

TEST(CASRecoveryGrounding, WalksFromLifeEpochWithoutCheckpointBase)
{
    const RecoveryGrounding grounding = chooseRecoveryGrounding(catalog(NsState::Removing), ckpt(9, RefTxnId{9, 3}));
    EXPECT_EQ(grounding.walk_from, (RefTxnId{9, 1}));
}

TEST(CASRecoveryGrounding, RejectsBaseWithoutARepresentableSuccessor)
{
    expectCode([&]
    {
        chooseRecoveryGrounding(catalog(NsState::Live),
            ckpt(7, RefTxnId{8, 1}, RefTxnId{7, std::numeric_limits<uint64_t>::max()}, RefTxnId{8, 1}));
    }, DB::ErrorCodes::CORRUPTED_DATA);
}

TEST(CASRecoveryGrounding, RejectsCheckpointFieldsAboveCommittedFrontier)
{
    expectCode([&]
    {
        chooseRecoveryGrounding(catalog(NsState::Live), ckpt(7, RefTxnId{7, 3}, RefTxnId{7, 4}));
    }, DB::ErrorCodes::CORRUPTED_DATA);
    expectCode([&]
    {
        chooseRecoveryGrounding(catalog(NsState::Live), ckpt(7, RefTxnId{7, 3}, std::nullopt, RefTxnId{7, 4}));
    }, DB::ErrorCodes::CORRUPTED_DATA);
}

TEST(CASRecoveryGrounding, RejectsIncoherentEpochBoundaryInCheckpointAuthority)
{
    expectCode([&]
    {
        chooseRecoveryGrounding(catalog(NsState::Live), ckpt(7, RefTxnId{10, 1}, std::nullopt, RefTxnId{7, 9}));
    }, DB::ErrorCodes::CORRUPTED_DATA);
    expectCode([&]
    {
        chooseRecoveryGrounding(catalog(NsState::Live), ckpt(7, RefTxnId{8, 5}, std::nullopt, RefTxnId{8, 1}));
    }, DB::ErrorCodes::CORRUPTED_DATA);
    expectCode([&]
    {
        chooseRecoveryGrounding(catalog(NsState::Live), ckpt(7, RefTxnId{8, 1}));
    }, DB::ErrorCodes::CORRUPTED_DATA);
}

/// A life starts in its own writer epoch. Letting it start after the checkpoint's writer epoch makes
/// `walk_from > committed_through`, so recovery silently returns an empty table instead of refusing the
/// impossible authority. The codec and pure grounding entry point must reject the same sabotage.
TEST(CASRecoveryGrounding, RejectsLifeEpochAboveCommittedFrontierOnDecodeAndGrounding)
{
    const RefCkpt invalid = ckpt(2, RefTxnId{1, 5});
    String encoded = encodeRefCkpt(ckpt(1, RefTxnId{1, 5}));
    const size_t life_epoch = encoded.find(R"("le":"1")");
    ASSERT_NE(life_epoch, String::npos);
    encoded.replace(life_epoch, String{R"("le":"1")"}.size(), R"("le":"2")");

    expectCode([&] { (void)decodeRefCkpt(encoded); }, DB::ErrorCodes::CORRUPTED_DATA);
    expectCode([&] { (void)chooseRecoveryGrounding(catalog(NsState::Live), invalid); },
               DB::ErrorCodes::CORRUPTED_DATA);
}

TEST(CASRecoveryGrounding, RecoveryIsEquivalentUnderFullEmptyPartialAndReorderedList)
{
    struct Observation
    {
        std::map<String, ManifestRef> committed;
        RefTxnId greatest_applied;
        std::optional<RefTxnId> last_epoch_seal;
        RefTxnId next_id;
        uint64_t log_gets = 0;
        uint64_t snapshot_gets = 0;
        uint64_t list_calls = 0;
    };

    std::vector<Observation> observations;
    for (const ListingMode mode : {ListingMode::Full, ListingMode::Empty, ListingMode::Partial, ListingMode::Reordered})
    {
        auto backend = std::make_shared<RecoveryListingBackend>(mode);
        const Layout layout("p");
        const RootNamespace ns{"srv1/list_equivalence"};
        const RefTxnId frontier{2, 1};
        seedAuthoritativeStream(*backend, layout, ns, frontier);
        const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(*backend, layout, ns);
        backend->resetCounts();
        backend->list_calls = 0;

        const RecoveredRefTable recovered = recoverFromCurrentCatalogCut(*backend, layout, ns);
        const uint64_t log_gets = backend->getCount(layout.refLogKey(life, {1, 1}))
            + backend->getCount(layout.refLogKey(life, {1, 2}))
            + backend->getCount(layout.refLogKey(life, {2, 1}));
        const uint64_t snapshot_gets = backend->getCount(layout.refSnapshotKey(life, {1, 1}));
        observations.push_back(Observation{
            .committed = committedOf(recovered.state),
            .greatest_applied = recovered.state.getGreatestApplied(),
            .last_epoch_seal = recovered.last_epoch_seal,
            .next_id = recovered.state.nextTxnId(/*live_epoch=*/3),
            .log_gets = log_gets,
            .snapshot_gets = snapshot_gets,
            .list_calls = backend->list_calls});
    }

    ASSERT_EQ(observations.size(), 4u);
    for (size_t i = 1; i < observations.size(); ++i)
    {
        EXPECT_EQ(observations[i].committed, observations[0].committed);
        EXPECT_EQ(observations[i].greatest_applied, observations[0].greatest_applied);
        EXPECT_EQ(observations[i].last_epoch_seal, observations[0].last_epoch_seal);
        EXPECT_EQ(observations[i].next_id, observations[0].next_id);
    }
    for (const Observation & observation : observations)
        EXPECT_EQ(observation.log_gets, 3u)
            << "recovery must fetch every exact log in the checkpoint-bounded frontier";
    for (const Observation & observation : observations)
        EXPECT_EQ(observation.snapshot_gets, 0u)
            << "a snapshot not named by `_ckpt` is not a recovery base";
    for (const Observation & observation : observations)
        EXPECT_EQ(observation.list_calls, 0u)
            << "recovery must not enumerate a stream whose exact checkpoint already supplies its base and frontier";
}

TEST(CASRecoveryGrounding, CatalogLifecycleAndCheckpointAreMandatoryForReadOnlyRecovery)
{
    const Layout layout("p");
    const RootNamespace ns{"srv1/mandatory_authority"};

    {
        auto backend = std::make_shared<RecoveryListingBackend>(ListingMode::Full);
        DB::Cas::tests::fixture::writeRefLogRaw(*backend, layout, txn(ns, {1, 1}, {namespaceBirthOp()}));
        expectCode([&] { (void)recoverFromCurrentCatalogCut(*backend, layout, ns); }, DB::ErrorCodes::CORRUPTED_DATA);
    }
    {
        auto backend = std::make_shared<RecoveryListingBackend>(ListingMode::Full);
        CasRefCatalog::casAdmitEntry(
            *backend, layout, 1, CatalogEntry{.ns = ns, .state = NsState::Live, .incarnation = 8});
        expectCode([&] { (void)recoverFromCurrentCatalogCut(*backend, layout, ns); }, DB::ErrorCodes::CORRUPTED_DATA);
    }
    {
        auto backend = std::make_shared<RecoveryListingBackend>(ListingMode::Full);
        const CatalogEntry live{.ns = ns, .state = NsState::Live, .incarnation = 9};
        CasRefCatalog::casAdmitEntry(*backend, layout, 1, live);
        const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(live.ns, live.incarnation);
        ASSERT_EQ(backend->putIfAbsent(layout.refCkptKey(life), "not a sealed checkpoint").outcome,
                  PutOutcome::Done);
        expectCode([&] { (void)recoverFromCurrentCatalogCut(*backend, layout, ns); }, DB::ErrorCodes::CORRUPTED_DATA);
    }
    {
        auto backend = std::make_shared<RecoveryListingBackend>(ListingMode::Full);
        CatalogEntry creating{.ns = ns, .state = NsState::Creating, .incarnation = 7,
            .creator = CreatorFence{"srv1", 1, 1}};
        CasRefCatalog::casAdmitEntry(*backend, layout, 1, creating);
        backend->putIfAbsent(layout.refCkptKey(NamespaceLifeId::fromCatalogEntry(creating.ns, creating.incarnation)),
            encodeRefCkpt(RefCkpt{.life_epoch = 1, .committed_through = RefTxnId{1, 1},
                                  .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt}));
        expectCode([&] { (void)recoverFromCurrentCatalogCut(*backend, layout, ns); }, DB::ErrorCodes::INVALID_STATE);
    }
    {
        auto backend = std::make_shared<RecoveryListingBackend>(ListingMode::Full);
        backend->putIfAbsent(layout.refCkptKey(DB::Cas::tests::fixture::fixtureLife(ns)),
            encodeRefCkpt(RefCkpt{.life_epoch = 1, .committed_through = RefTxnId{1, 1},
                                  .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt}));
        expectCode([&] { (void)recoverFromCurrentCatalogCut(*backend, layout, ns); }, DB::ErrorCodes::INVALID_STATE);
    }
}

TEST(CASRecoveryGrounding, NonrecoverableAuthorityPerformsNoBackendRecoveryIo)
{
    const Layout layout("p");
    const RootNamespace ns{"srv1/nonrecoverable_authority"};
    const RefCkpt valid_ckpt{
        .life_epoch = 1,
        .committed_through = std::nullopt,
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt};

    {
        auto backend = std::make_shared<RecoveryListingBackend>(ListingMode::Full);
        backend->resetCounts();
        const CatalogEntry creating{
            .ns = ns, .state = NsState::Creating, .incarnation = 1, .creator = CreatorFence{"srv1", 1, 1}};
        expectCode(
            [&] { (void)recoverRefTableDetailedFromAuthority(*backend, layout, creating, valid_ckpt); },
            DB::ErrorCodes::INVALID_STATE);
        EXPECT_EQ(backend->list_calls, 0u);
        EXPECT_EQ(backend->getTotal(), 0u);
    }
    {
        auto backend = std::make_shared<RecoveryListingBackend>(ListingMode::Full);
        backend->resetCounts();
        const CatalogEntry live{.ns = ns, .state = NsState::Live, .incarnation = 2};
        expectCode(
            [&] { (void)recoverRefTableDetailedFromAuthority(*backend, layout, live, std::nullopt); },
            DB::ErrorCodes::CORRUPTED_DATA);
        EXPECT_EQ(backend->list_calls, 0u);
        EXPECT_EQ(backend->getTotal(), 0u);
    }
    {
        auto backend = std::make_shared<RecoveryListingBackend>(ListingMode::Full);
        backend->resetCounts();
        expectCode(
            [&] { (void)recoverRefTableDetailedFromAuthority(*backend, layout, std::nullopt, valid_ckpt); },
            DB::ErrorCodes::INVALID_STATE);
        EXPECT_EQ(backend->list_calls, 0u);
        EXPECT_EQ(backend->getTotal(), 0u);
    }
}

TEST(CASRecoveryGrounding, ReadOnlyRecoveryNeverAdoptsFPlusOne)
{
    auto backend = std::make_shared<RecoveryListingBackend>(ListingMode::Full);
    const Layout layout("p");
    const RootNamespace ns{"srv1/read_only_excludes_f_plus_one"};
    seedAuthoritativeStream(*backend, layout, ns, RefTxnId{1, 1}, /*include_f_plus_one=*/true);

    const RecoveredRefTable recovered = recoverFromCurrentCatalogCut(*backend, layout, ns);
    EXPECT_EQ(recovered.state.getGreatestApplied(), (RefTxnId{1, 1}));
    EXPECT_TRUE(recovered.state.getCommitted().contains("a"));
    EXPECT_FALSE(recovered.state.getCommitted().contains("uncommitted"));
}

/// A well-formed snapshot can describe a real but uncommitted transaction. If recovery merely treated
/// `LIST` as a performance hint, it could still select this false base and skip the exact first log.
/// The checkpoint names no snapshot, so every listing behaviour must leave the forged object unread.
TEST(CASRecoveryGrounding, ForgedWellFormedListedSnapshotIsUnobservedAndRecoveryDoesNotList)
{
    for (const ListingMode mode : {ListingMode::Full, ListingMode::Empty, ListingMode::Partial, ListingMode::Reordered})
    {
        auto backend = std::make_shared<RecoveryListingBackend>(mode);
        const Layout layout("p");
        const RootNamespace ns{"srv1/forged_listed_snapshot"};
        seedAuthoritativeStream(*backend, layout, ns, RefTxnId{1, 1}, /*include_f_plus_one=*/true);
        const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(*backend, layout, ns);

        RefTableState forged_state;
        std::vector<RefOp> birth{namespaceBirthOp()};
        const auto first_publish = publishCommittedOps("a", ManifestRef{1, 1, 1});
        birth.insert(birth.end(), first_publish.begin(), first_publish.end());
        applyRefLogTxn(forged_state, txn(ns, {1, 1}, std::move(birth)));
        applyRefLogTxn(forged_state, txn(ns, {1, 2}, publishCommittedOps("uncommitted", ManifestRef{1, 2, 1})));
        writeRefSnapshotRaw(*backend, layout, snapshotOf(forged_state, ns.string()));
        const String forged_key = layout.refSnapshotKey(life, {1, 2});

        backend->resetCounts();
        backend->list_calls = 0;
        const RecoveredRefTable recovered = recoverFromCurrentCatalogCut(*backend, layout, ns);

        EXPECT_EQ(backend->list_calls, 0u);
        EXPECT_EQ(backend->getCount(forged_key), 0u);
        EXPECT_EQ(recovered.state.getGreatestApplied(), (RefTxnId{1, 1}));
        EXPECT_TRUE(recovered.state.getCommitted().contains("a"));
        EXPECT_FALSE(recovered.state.getCommitted().contains("uncommitted"));
    }
}

/// A checkpoint-named snapshot is immutable lifecycle authority, not a list candidate. Its exact GET
/// and semantic decode must therefore fail closed rather than falling back to replaying the same log.
TEST(CASRecoveryGrounding, SemanticallyMalformedCheckpointSnapshotIsCorruptionAfterExactRead)
{
    auto backend = std::make_shared<RecoveryListingBackend>(ListingMode::Empty);
    const Layout layout("p");
    const RootNamespace ns{"srv1/semantically_malformed_checkpoint"};
    const ManifestRef manifest{1, 1, 1};
    std::vector<RefOp> ops{namespaceBirthOp()};
    const auto publish = publishCommittedOps("committed", manifest);
    ops.insert(ops.end(), publish.begin(), publish.end());
    DB::Cas::tests::fixture::writeRefLogRaw(*backend, layout, txn(ns, {1, 1}, std::move(ops)));

    RefTableSnapshot malformed = minimalLiveSnapshot(
        ns.string(), {1, 1}, {DB::Cas::tests::committedRow("committed", manifest)});
    malformed.precommits.push_back(RefOwnerBinding{RefOwnerKind::Precommit, "precommit", manifest});
    writeRefSnapshotRaw(*backend, layout, malformed);

    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(*backend, layout, ns);
    const String snapshot_key = layout.refSnapshotKey(life, {1, 1});
    ASSERT_EQ(backend->putIfAbsent(layout.refCkptKey(life), encodeRefCkpt(RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 1},
        .checkpoint_snapshot_id = RefTxnId{1, 1},
        .last_epoch_seal = std::nullopt})).outcome, PutOutcome::Done);

    backend->resetCounts();
    try
    {
        (void)recoverFromCurrentCatalogCut(*backend, layout, ns);
        FAIL() << "expected checkpoint-named malformed snapshot to fail closed";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::CORRUPTED_DATA);
        EXPECT_NE(String(e.message()).find("stateFromSnapshot"), String::npos);
    }
    EXPECT_EQ(backend->getCount(snapshot_key), 1u)
        << "the corruption must come from the checkpoint snapshot's exact decode";
}

TEST(CASRecoveryGrounding, CheckpointSnapshotEqualToLastEpochSealIsRejectedBeforeReadingItsLog)
{
    auto backend = std::make_shared<RecoveryListingBackend>(ListingMode::Full);
    const Layout layout("p");
    const RootNamespace ns{"srv1/checkpoint_base_seal"};
    /// The checkpoint directly contradicts itself: its sole snapshot base names its terminal seal.
    seedAuthoritativeStream(*backend, layout, ns, RefTxnId{1, 2});
    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(*backend, layout, ns);

    RefTableState through_seal;
    std::vector<RefOp> birth{namespaceBirthOp()};
    const auto first_publish = publishCommittedOps("a", ManifestRef{1, 1, 1});
    birth.insert(birth.end(), first_publish.begin(), first_publish.end());
    applyRefLogTxn(through_seal, txn(ns, {1, 1}, std::move(birth)));
    RefOp seal;
    seal.kind = RefOpKind::EpochSeal;
    applyRefLogTxn(through_seal, txn(ns, {1, 2}, {std::move(seal)}));
    writeRefSnapshotRaw(*backend, layout, snapshotOf(through_seal, ns.string()));

    const CkptSample before = *readCkpt(*backend, layout, life);
    const RefCkpt with_sealed_base{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 2},
        .checkpoint_snapshot_id = RefTxnId{1, 2},
        .last_epoch_seal = RefTxnId{1, 2}};
    ASSERT_EQ(backend->casPut(layout.refCkptKey(life), encodeRefCkpt(with_sealed_base), before.token).outcome,
              CasOutcome::Committed);

    backend->resetCounts();
    expectCode([&] { (void)recoverFromCurrentCatalogCut(*backend, layout, ns); }, DB::ErrorCodes::CORRUPTED_DATA);
    EXPECT_EQ(backend->getCount(layout.refLogKey(life, {1, 2})), 0u)
        << "the contradictory checkpoint metadata is rejected before any matching-log read";
    EXPECT_EQ(backend->getCount(layout.refSnapshotKey(life, {1, 2})), 0u)
        << "the seal-kind witness must be checked before reading the forged same-id snapshot";
}

/// An `EpochSeal` terminates its numeric epoch.  A checkpoint frontier one sequence later in that
/// same epoch is not an empty tail: no record can occupy that slot.  Recovery must diagnose the
/// malformed authority instead of advancing to `{E+1,1}` and terminating because that id sorts above
/// the bogus same-epoch frontier.
TEST(CASRecoveryGrounding, SameEpochFrontierAfterDecodedEpochSealIsCorruption)
{
    auto backend = std::make_shared<RecoveryListingBackend>(ListingMode::Full);
    const Layout layout("p");
    const RootNamespace ns{"srv1/frontier_after_seal"};

    DB::Cas::tests::fixture::writeRefLogRaw(*backend, layout, txn(ns, {1, 1}, {namespaceBirthOp()}));
    RefOp seal;
    seal.kind = RefOpKind::EpochSeal;
    DB::Cas::tests::fixture::writeRefLogRaw(*backend, layout, txn(ns, {1, 2}, {std::move(seal)}));

    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(*backend, layout, ns);
    String malformed_ckpt = encodeRefCkpt(RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 2},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = RefTxnId{1, 2}});
    const size_t frontier_sequence = malformed_ckpt.find(R"("cts":"2")");
    ASSERT_NE(frontier_sequence, String::npos);
    malformed_ckpt.replace(frontier_sequence, String{R"("cts":"2")"}.size(), R"("cts":"3")");
    ASSERT_EQ(backend->putIfAbsent(layout.refCkptKey(life), malformed_ckpt).outcome, PutOutcome::Done);

    expectCode([&] { (void)recoverFromCurrentCatalogCut(*backend, layout, ns); }, DB::ErrorCodes::CORRUPTED_DATA);
}

TEST(CASRecoveryGrounding, OlderCheckpointSnapshotAtSealIsCorruption)
{
    auto backend = std::make_shared<RecoveryListingBackend>(ListingMode::Full);
    const Layout layout("p");
    const RootNamespace ns{"srv1/older_checkpoint_base_seal"};
    seedAuthoritativeStream(*backend, layout, ns, RefTxnId{2, 1});
    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(*backend, layout, ns);

    RefOp second_seal;
    second_seal.kind = RefOpKind::EpochSeal;
    DB::Cas::tests::fixture::writeRefLogRaw(*backend, layout, txn(ns, {2, 2}, {std::move(second_seal)}));
    DB::Cas::tests::fixture::writeRefLogRaw(*backend, layout,
        txn(ns, {3, 1}, publishCommittedOps("c", ManifestRef{3, 1, 1}), RefTxnId{2, 2}));

    RefTableState through_first_seal;
    std::vector<RefOp> birth{namespaceBirthOp()};
    const auto first_publish = publishCommittedOps("a", ManifestRef{1, 1, 1});
    birth.insert(birth.end(), first_publish.begin(), first_publish.end());
    applyRefLogTxn(through_first_seal, txn(ns, {1, 1}, std::move(birth)));
    RefOp first_seal;
    first_seal.kind = RefOpKind::EpochSeal;
    applyRefLogTxn(through_first_seal, txn(ns, {1, 2}, {std::move(first_seal)}));
    writeRefSnapshotRaw(*backend, layout, snapshotOf(through_first_seal, ns.string()));

    const CkptSample before = *readCkpt(*backend, layout, life);
    const RefCkpt with_old_sealed_base{
        .life_epoch = 1,
        .committed_through = RefTxnId{3, 1},
        .checkpoint_snapshot_id = RefTxnId{1, 2},
        .last_epoch_seal = RefTxnId{2, 2}};
    ASSERT_EQ(backend->casPut(layout.refCkptKey(life), encodeRefCkpt(with_old_sealed_base), before.token).outcome,
              CasOutcome::Committed);

    backend->resetCounts();
    expectCode([&] { (void)recoverFromCurrentCatalogCut(*backend, layout, ns); }, DB::ErrorCodes::CORRUPTED_DATA);
    EXPECT_EQ(backend->getCount(layout.refLogKey(life, {1, 2})), 1u)
        << "the old seal differs from `last_epoch_seal`, so only the matching-log proof can reject it";
    EXPECT_EQ(backend->getCount(layout.refSnapshotKey(life, {1, 2})), 0u)
        << "the old seal must be rejected before the forged same-id snapshot is read";
}

TEST(CASRecoveryGrounding, TerminalGapBelowFrontierIsCorruptionNotARebirth)
{
    auto backend = std::make_shared<RecoveryListingBackend>(ListingMode::Full);
    const Layout layout("p");
    const RootNamespace ns{"srv1/terminal_gap"};
    const RefLogTxn birth = txn(ns, {1, 1}, {namespaceBirthOp()});
    DB::Cas::tests::fixture::writeRefLogRaw(*backend, layout, birth);
    RefOp remove;
    remove.kind = RefOpKind::RemoveNamespace;
    DB::Cas::tests::fixture::writeRefLogRaw(*backend, layout, txn(ns, {1, 2}, {std::move(remove)}));
    DB::Cas::tests::fixture::writeRefLogRaw(*backend, layout, txn(ns, {2, 1}, {namespaceBirthOp()}));

    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(*backend, layout, ns);
    String malformed_ckpt = encodeRefCkpt(RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 1},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt});
    const size_t frontier_epoch = malformed_ckpt.find(R"("cte":"1")");
    ASSERT_NE(frontier_epoch, String::npos);
    malformed_ckpt.replace(frontier_epoch, String{R"("cte":"1")"}.size(), R"("cte":"2")");
    ASSERT_EQ(backend->putIfAbsent(layout.refCkptKey(life), malformed_ckpt).outcome, PutOutcome::Done);

    expectCode([&] { (void)recoverFromCurrentCatalogCut(*backend, layout, ns); }, DB::ErrorCodes::CORRUPTED_DATA);
}

TEST(CASRecoveryGrounding, LaterEpochCheckpointBaseRequiresItsContextualBacklink)
{
    auto backend = std::make_shared<RecoveryListingBackend>(ListingMode::Full);
    const Layout layout("p");
    const RefTxnId seal_id{1, 2};
    const RefTxnId base_id{2, 1};

    const auto expect_rejected = [&](const RootNamespace & ns, std::optional<RefTxnId> backlink)
    {
        DB::Cas::tests::fixture::writeRefLogRaw(*backend, layout, txn(ns, {1, 1}, {namespaceBirthOp()}));
        RefOp seal;
        seal.kind = RefOpKind::EpochSeal;
        DB::Cas::tests::fixture::writeRefLogRaw(*backend, layout, txn(ns, seal_id, {std::move(seal)}));
        DB::Cas::tests::fixture::writeRefLogRaw(*backend, layout, txn(ns, base_id, {}, backlink));
        writeRefSnapshotRaw(*backend, layout, minimalLiveSnapshot(ns.string(), base_id));

        const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(*backend, layout, ns);
        ASSERT_EQ(backend->putIfAbsent(layout.refCkptKey(life), encodeRefCkpt(RefCkpt{
            .life_epoch = 1,
            .committed_through = base_id,
            .checkpoint_snapshot_id = base_id,
            .last_epoch_seal = seal_id})).outcome, PutOutcome::Done);

        expectCode(
            [&] { (void)recoverFromCurrentCatalogCut(*backend, layout, ns); },
            DB::ErrorCodes::CORRUPTED_DATA);
    };

    expect_rejected(RootNamespace{"srv1/base_missing_backlink"}, std::nullopt);
    expect_rejected(RootNamespace{"srv1/base_wrong_backlink"}, RefTxnId{1, 99});

    const auto expect_predecessor_rejected = [&](const RootNamespace & ns, bool write_ordinary_predecessor)
    {
        DB::Cas::tests::fixture::writeRefLogRaw(*backend, layout, txn(ns, {1, 1}, {namespaceBirthOp()}));
        if (write_ordinary_predecessor)
            DB::Cas::tests::fixture::writeRefLogRaw(*backend, layout, txn(ns, seal_id, {}));
        DB::Cas::tests::fixture::writeRefLogRaw(*backend, layout, txn(ns, base_id, {}, seal_id));
        writeRefSnapshotRaw(*backend, layout, minimalLiveSnapshot(ns.string(), base_id));

        const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(*backend, layout, ns);
        ASSERT_EQ(backend->putIfAbsent(layout.refCkptKey(life), encodeRefCkpt(RefCkpt{
            .life_epoch = 1,
            .committed_through = base_id,
            .checkpoint_snapshot_id = base_id,
            .last_epoch_seal = seal_id})).outcome, PutOutcome::Done);

        expectCode(
            [&] { (void)recoverFromCurrentCatalogCut(*backend, layout, ns); },
            DB::ErrorCodes::CORRUPTED_DATA);
    };

    expect_predecessor_rejected(RootNamespace{"srv1/base_predecessor_absent"}, false);
    expect_predecessor_rejected(RootNamespace{"srv1/base_predecessor_not_seal"}, true);
}

}
