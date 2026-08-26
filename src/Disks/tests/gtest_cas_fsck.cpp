#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasFsck.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefSnapshotFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefProtocol.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.h>
#include "cas_test_helpers.h"

#include <algorithm>
#include <filesystem>
#include <functional>
#include <mutex>
#include <optional>
#include <string_view>

namespace DB::ErrorCodes
{
extern const int INVALID_STATE;
extern const int NETWORK_ERROR;
}

using namespace DB::Cas;
using namespace DB::Cas::tests;

namespace
{
constexpr uint64_t kWriterEpoch = 7;
const String kServerRoot = "00";
ManifestRef ref(uint64_t seq, uint64_t inst)
{
    return ManifestRef{.writer_epoch = kWriterEpoch, .build_sequence = seq, .manifest_ordinal = static_cast<uint32_t>(inst)};
}

/// B207 race-simulation harness: `InMemoryBackend` is documented "not final: tests subclass it to
/// distort single behaviors". `runFsck`'s ref-walk and its physical blob listing (`listAll` over
/// `layout.blobsPrefix()`) are two separate calls to `Backend::list` minutes apart in production; here
/// we fire an injected mutation the FIRST time `list` is called against the armed prefix — i.e.
/// strictly AFTER the ref-walk has captured its (now stale) `reachable_blobs`/`blob_labels` view, and
/// strictly BEFORE the HEAD-confirm loop sees the physical listing. That reproduces the race
/// deterministically, without any real timing.
class RepublishOnListBackend : public InMemoryBackend
{
public:
    void armOnFirstList(String prefix, std::function<void()> mutation)
    {
        std::lock_guard<std::mutex> lock(arm_mutex);
        armed_prefix = std::move(prefix);
        pending_mutation = std::move(mutation);
    }

    ListPage list(const String & prefix, const String & cursor, size_t limit) override
    {
        std::function<void()> to_run;
        {
            std::lock_guard<std::mutex> lock(arm_mutex);
            if (pending_mutation && prefix == armed_prefix)
            {
                to_run = std::move(pending_mutation);
                pending_mutation = nullptr;
            }
        }
        if (to_run)
            to_run();
        return InMemoryBackend::list(prefix, cursor, limit);
    }

private:
    std::mutex arm_mutex;
    String armed_prefix;
    std::function<void()> pending_mutation;
};

/// Companion to `RepublishOnListBackend` for the MANIFEST phantom-dangle race: the ref-walk's
/// per-namespace recovery captures each committed `(ref -> manifest)` minutes before the per-ref
/// `backend.get(mkey)` that confirms the manifest body. This backend fires an injected mutation the
/// FIRST time `get` is called for the armed manifest key — strictly AFTER the walk captured its (now
/// stale) row and AT the GET that would otherwise read the manifest — reproducing "ref republished/
/// dropped + old manifest legitimately GC-deleted" deterministically, with no real timing.
class MutateOnFirstGetBackend : public InMemoryBackend
{
public:
    void armOnFirstGet(String key, std::function<void()> mutation)
    {
        std::lock_guard<std::mutex> lock(arm_mutex);
        armed_key = std::move(key);
        pending_mutation = std::move(mutation);
    }

    std::optional<GetResult> get(const String & key, Range range) override
    {
        std::function<void()> to_run;
        {
            std::lock_guard<std::mutex> lock(arm_mutex);
            if (pending_mutation && key == armed_key)
            {
                to_run = std::move(pending_mutation);
                pending_mutation = nullptr;
            }
        }
        if (to_run)
            to_run();
        return InMemoryBackend::get(key, range);
    }

private:
    std::mutex arm_mutex;
    String armed_key;
    std::function<void()> pending_mutation;
};

enum class FsckListingMode : uint8_t
{
    Full,
    Empty,
    Partial,
    Reordered,
};

/// Distort only one namespace stream LIST after fixture deposition. Exact GET/HEAD and every other
/// prefix retain ordinary backend semantics, so the test varies the hint and nothing authoritative.
class FsckListingBackend : public InMemoryBackend
{
public:
    void distort(String prefix_, FsckListingMode mode_)
    {
        prefix = std::move(prefix_);
        mode = mode_;
    }

    ListPage list(const String & listed_prefix, const String & cursor, size_t limit) override
    {
        ListPage page = InMemoryBackend::list(listed_prefix, cursor, limit);
        if (listed_prefix != prefix)
            return page;
        if (mode == FsckListingMode::Empty)
            page.keys.clear();
        else if (mode == FsckListingMode::Partial && !page.keys.empty())
            page.keys.erase(page.keys.begin());
        else if (mode == FsckListingMode::Reordered)
            std::reverse(page.keys.begin(), page.keys.end());
        return page;
    }

private:
    String prefix;
    FsckListingMode mode = FsckListingMode::Full;
};

/// Fail one exact GET without disturbing LIST or any other object read. This keeps the checkpoint
/// authority stable while proving that fsck distinguishes a transport failure from durable corruption.
class FailExactGetBackend : public InMemoryBackend
{
public:
    void fail(String key_)
    {
        key = std::move(key_);
    }

    std::optional<GetResult> get(const String & requested_key, Range range) override
    {
        if (requested_key == key)
            throw DB::Exception(DB::ErrorCodes::NETWORK_ERROR, "injected exact GET failure");
        return InMemoryBackend::get(requested_key, range);
    }

private:
    String key;
};

/// Publish the exact `_ckpt` authority an ordinary Live test life would have after its first committed
/// record. Raw ref-log helpers deliberately do not do this: several protocol tests need malformed or
/// pre-creation states. Fsck tests that exercise a recoverable Live life must make the durable authority
/// explicit instead of accidentally borrowing the legacy LIST-only recovery rule.
void writeFsckCheckpoint(Backend & backend, const Layout & layout, const RootNamespace & ns, RefTxnId committed_through)
{
    const CasRefCatalog::Snapshot cut = CasRefCatalog::read(backend, layout);
    const auto it = std::find_if(cut.catalog.entries.begin(), cut.catalog.entries.end(),
        [&](const CatalogEntry & entry) { return entry.ns == ns; });
    ASSERT_NE(it, cut.catalog.entries.end());
    const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(it->ns, it->incarnation);
    const String key = layout.refCkptKey(life);
    const String body = encodeRefCkpt(RefCkpt{
        .life_epoch = committed_through.writer_epoch,
        .committed_through = committed_through,
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt});
    const HeadResult current = backend.head(key);
    const PutResult put = current.exists
        ? backend.putOverwrite(key, body, current.token)
        : backend.putIfAbsent(key, body);
    ASSERT_EQ(put.outcome, PutOutcome::Done);
}

void writeFsckCheckpointWithBase(
    Backend & backend, const Layout & layout, const RootNamespace & ns, RefTxnId base,
    std::optional<RefTxnId> last_epoch_seal = std::nullopt)
{
    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(backend, layout, ns);
    ASSERT_EQ(backend.putIfAbsent(layout.refCkptKey(life), encodeRefCkpt(RefCkpt{
        .life_epoch = 1,
        .committed_through = base,
        .checkpoint_snapshot_id = base,
        .last_epoch_seal = last_epoch_seal})).outcome, PutOutcome::Done);
}

void expectCheckpointBaseVerdict(
    const FsckReport & report, const String & exact_base_key, FsckClass expected_class,
    std::string_view expected_reason)
{
    EXPECT_EQ(report.chain_broken, expected_class == FsckClass::ChainBroken ? 1u : 0u);
    EXPECT_EQ(report.unchecked, expected_class == FsckClass::Unchecked ? 1u : 0u);
    EXPECT_EQ(report.clean(), expected_class != FsckClass::ChainBroken);

    size_t matching = 0;
    for (const FsckObject & object : report.objects)
    {
        if (object.cls != expected_class || object.key != exact_base_key)
            continue;
        ++matching;
        ASSERT_EQ(object.reachable_from.size(), 1u);
        EXPECT_NE(object.reachable_from.front().find(expected_reason), String::npos);
    }
    EXPECT_EQ(matching, 1u) << "the checkpoint-base verdict must identify its exact named base and cause";
}

/// Test-only external catalog writer. It changes the namespace's current logical life after fsck took
/// its catalog cut, precisely the competing-cut mutation that fsck must not splice into its verdict.
void replaceCatalogLife(Backend & backend, const Layout & layout, const RootNamespace & ns, UInt128 incarnation)
{
    CasRefCatalog::Snapshot current = CasRefCatalog::read(backend, layout);
    const auto it = std::find_if(current.catalog.entries.begin(), current.catalog.entries.end(),
        [&](const CatalogEntry & entry) { return entry.ns == ns; });
    ASSERT_NE(it, current.catalog.entries.end());
    it->incarnation = incarnation;
    it->state = NsState::Live;
    it->creator.reset();
    it->removal_started_round.reset();
    ASSERT_TRUE(current.token.has_value());
    ASSERT_EQ(backend.putOverwrite(layout.refCatalogKey(), encodeRefCatalog(current.catalog), *current.token).outcome,
        PutOutcome::Done);
}

FsckReport runFsckWithListingMode(FsckListingMode mode, std::string_view suffix)
{
    auto backend = std::make_shared<FsckListingBackend>();
    auto store = openPoolForTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/listing_" + String(suffix) + "@cas@"};
    const ManifestRef r1 = ref(1, 0xD1);
    const ManifestRef r2 = ref(2, 0xD2);
    const DB::UInt128 h1 = u128Of("fsck-listing-old-" + String(suffix));
    const DB::UInt128 h2 = u128Of("fsck-listing-new-" + String(suffix));
    writeBlobBody(*backend, layout, h1);
    writeBlobBody(*backend, layout, h2);
    writeManifestRaw(*backend, layout, ns, r1, {blobEntryFor("a", h1)});
    writeManifestRaw(*backend, layout, ns, r2, {blobEntryFor("a", h2)});
    publishCommittedTransition(*backend, layout, ns, "tbl", std::nullopt, r1);
    const uint64_t frontier = publishCommittedTransition(*backend, layout, ns, "tbl", r1, r2);
    writeFsckCheckpoint(*backend, layout, ns, RefTxnId{1, frontier});

    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(*backend, layout, ns);
    backend->distort(layout.namespaceStreamPrefix(life), mode);
    return runFsck(*store, /*detail=*/true);
}

void expectListingIndependentFsck(const FsckReport & report)
{
    EXPECT_EQ(report.chain_broken, 0u);
    EXPECT_EQ(report.unchecked, 0u);
    EXPECT_EQ(report.dangling, 0u);
    EXPECT_EQ(report.ref_records_walked, 2u);
    EXPECT_EQ(report.reachable, 1u);
}

struct FsckAuthorityVerdict
{
    bool clean = false;
    uint64_t hard_findings = 0;
    uint64_t chain_broken = 0;
    uint64_t unchecked = 0;
    uint64_t ref_records_walked = 0;
    uint64_t reachable = 0;
    uint64_t dangling = 0;

    bool operator==(const FsckAuthorityVerdict &) const = default;
};

FsckAuthorityVerdict authorityVerdict(const FsckReport & report)
{
    uint64_t hard_findings = 0;
    for (const FsckHardFinding & finding : kFsckHardFindings)
        hard_findings += report.*(finding.value);
    return FsckAuthorityVerdict{
        .clean = report.clean(),
        .hard_findings = hard_findings,
        .chain_broken = report.chain_broken,
        .unchecked = report.unchecked,
        .ref_records_walked = report.ref_records_walked,
        .reachable = report.reachable,
        .dangling = report.dangling,
    };
}

FsckReport runCheckpointBaseFsckWithListingMode(
    FsckListingMode mode, std::string_view suffix, bool corrupt_exact_base)
{
    auto backend = std::make_shared<FsckListingBackend>();
    auto store = openPoolForTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/listing_base_" + String(suffix) + "@cas@"};
    fixture::admitLive(*backend, layout, ns);

    const RefTxnId base{1, 1};
    const RefLogTxn birth{
        .ns = ns.string(), .txn_id = base, .ops = {namespaceBirthOp()}, .prev_epoch_seal = std::nullopt};
    fixture::writeRefLogRaw(*backend, layout, birth);
    RefTableState base_state;
    applyRefLogTxn(base_state, birth);
    writeRefSnapshotRaw(*backend, layout, snapshotOf(base_state, ns.string()));
    writeFsckCheckpointWithBase(*backend, layout, ns, base);

    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(*backend, layout, ns);
    if (corrupt_exact_base)
    {
        const String base_snapshot_key = layout.refSnapshotKey(life, base);
        const HeadResult head = backend->head(base_snapshot_key);
        EXPECT_TRUE(head.exists);
        if (head.exists)
            EXPECT_EQ(backend->deleteExact(base_snapshot_key, head.token).kind, DeleteOutcome::Kind::Deleted);
    }
    else
    {
        /// This newer pair is deliberately outside `_ckpt.committed_through`. It is inert garbage:
        /// changing whether LIST happens to reveal it must not add or remove an fsck finding.
        const RefTxnId unadopted{1, 2};
        const RefOwnerBinding listed_binding{RefOwnerKind::Precommit, "listed", ref(9, 0xE9)};
        const RefLogTxn listed_log{
            .ns = ns.string(),
            .txn_id = unadopted,
            .ops = {ownerTransitionOp(std::nullopt, listed_binding)},
            .prev_epoch_seal = std::nullopt};
        fixture::writeRefLogRaw(*backend, layout, listed_log);
        RefTableState listed_state = base_state;
        applyRefLogTxn(listed_state, listed_log);
        RefTableSnapshot unadopted_snapshot = snapshotOf(listed_state, ns.string());
        unadopted_snapshot.precommits.push_back(
            RefOwnerBinding{RefOwnerKind::Precommit, "unlisted", ref(10, 0xEA)});
        writeRefSnapshotRaw(*backend, layout, unadopted_snapshot);
    }

    backend->distort(layout.namespaceStreamPrefix(life), mode);
    return runFsck(*store, /*detail=*/true);
}
}

/// A committed ref whose manifest body is present and whose blobs exist => clean.
TEST(CASFsck, CleanManifestPoolHasNoDangling)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(1, 0xAA);
    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    const uint64_t sequence = publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);
    writeFsckCheckpoint(*backend, store->layout(), ns, RefTxnId{1, sequence});
    const FsckReport rep = runFsck(*store, /*detail*/true);
    EXPECT_TRUE(rep.clean());
    EXPECT_EQ(rep.dangling, 0u);
}

/// A committed ref naming a MISSING manifest body is an ERROR (Dangling).
TEST(CASFsck, OwnerVisibleMissingManifestBodyIsError)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(1, 0xAA);
    const uint64_t sequence = publishCommittedTransition(
        *backend, store->layout(), ns, "tbl", std::nullopt, r);  // no body written
    writeFsckCheckpoint(*backend, store->layout(), ns, RefTxnId{1, sequence});
    const FsckReport rep = runFsck(*store, /*detail*/true);
    EXPECT_FALSE(rep.clean());
    EXPECT_GE(rep.dangling, 1u);
}

/// A committed ref whose blob body is missing is an ERROR (Dangling).
TEST(CASFsck, ReachableBlobMissingIsError)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(1, 0xAA);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});  // no blob body
    const uint64_t sequence = publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);
    writeFsckCheckpoint(*backend, store->layout(), ns, RefTxnId{1, sequence});
    const FsckReport rep = runFsck(*store, /*detail*/true);
    EXPECT_FALSE(rep.clean());
    EXPECT_GE(rep.dangling, 1u);
}

/// fsck RECORDS AND CONTINUES over a key that names no namespace life. It is the forensic tool an
/// operator reaches for after something has already gone wrong, so one bad key must not make it report
/// NOTHING -- including about the healthy namespaces it would never reach. The finding is hard (an
/// un-incarnated key is corruption behind the format bump) and counted once per key, not once per sweep.
TEST(CASFsck, LifelessKeyIsRecordedAndTheHealthyNamespaceIsStillReported)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(1, 0xA1);
    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    const uint64_t sequence = publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);
    writeFsckCheckpoint(*backend, store->layout(), ns, RefTxnId{1, sequence});

    /// Hand-built: no helper can mint the un-incarnated shape any more.
    const String lifeless = store->layout().casRefsPrefix() + ns.string() + "/_log/"
        + renderRefTxnId(RefTxnId{1, 1}) + ".zst";
    ASSERT_EQ(backend->putIfAbsent(lifeless, "garbage").outcome, PutOutcome::Done);

    FsckReport rep;
    ASSERT_NO_THROW(rep = runFsck(*store, /*detail*/true))
        << "the audit must not be taken out by the damage it exists to report";

    /// The finding, named, and counted ONCE even though several sweeps enumerate namespaces.
    EXPECT_EQ(rep.lifeless_keys, 1u);
    EXPECT_FALSE(rep.clean());
    bool saw = false;
    for (const FsckObject & o : rep.objects)
        if (o.cls == FsckClass::LifelessKey)
        {
            saw = true;
            EXPECT_EQ(o.key, lifeless);
        }
    EXPECT_TRUE(saw) << "a counted finding with no row is a number nobody can act on";

    /// And the healthy namespace was still reached: its committed ref resolved to a present manifest and
    /// a present blob, which only a sweep that ran can report.
    EXPECT_GE(rep.reachable, 1u);
    EXPECT_EQ(rep.dangling, 0u);
}

/// A COMPLETE, canonical namespace-life key (a real `_files` write under a real admitted life) whose
/// catalog row is then removed entirely -- exactly what a fenced GC's exact-CAS row deletion leaves
/// behind, before the perpetual namespace janitor's next page reaches it -- must classify as
/// `janitor_pending`, a SOFT finding, never `lifeless_keys`. The report stays clean.
TEST(CASFsck, CanonicalDeadLifeResidueIsJanitorPendingNotHardFinding)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const NamespaceLifeId life = store->namespaceLife(ns);
    store->putNamespaceFile(life, "format_version.txt", "1\n");

    /// Simulate a fenced GC's exact-CAS catalog-row deletion: the row is gone, the life-owned physical
    /// object above survives it (the janitor's own job, not GC's own round). `casUpdate` deliberately
    /// refuses to add or delete rows (there is no generic catalog remove-by-name API -- deletion is
    /// only `deleteCompletedRemoving`/`cancelStalledCreating`, both requiring the full fenced-GC
    /// protocol this fixture is not driving), so inject the post-deletion catalog snapshot directly,
    /// mirroring `DuplicateLifeIdIsReportedWhileAnUnrelatedUniqueNamespaceStillProgresses` below.
    {
        CasRefCatalog::Snapshot snapshot = CasRefCatalog::read(*backend, store->layout());
        const auto it = std::find_if(snapshot.catalog.entries.begin(), snapshot.catalog.entries.end(),
            [&](const CatalogEntry & entry) { return entry.ns == ns; });
        ASSERT_NE(it, snapshot.catalog.entries.end());
        snapshot.catalog.entries.erase(it);
        const auto catalog_head = backend->head(store->layout().refCatalogKey());
        ASSERT_TRUE(catalog_head.exists);
        ASSERT_EQ(backend->putOverwrite(store->layout().refCatalogKey(), encodeRefCatalog(snapshot.catalog),
            catalog_head.token).outcome, PutOutcome::Done);
    }

    FsckReport rep;
    ASSERT_NO_THROW(rep = runFsck(*store, /*detail*/true))
        << "janitor-pending residue must never abort the scan";

    EXPECT_EQ(rep.lifeless_keys, 0u);
    EXPECT_GE(rep.namespace_janitor_pending, 1u);
    EXPECT_EQ(rep.namespace_janitor_pending_lives, 1u);
    EXPECT_TRUE(rep.clean()) << "janitor-pending residue is not a hard finding";
    bool saw = false;
    for (const FsckObject & o : rep.objects)
        if (o.cls == FsckClass::JanitorPending)
            saw = true;
    EXPECT_TRUE(saw) << "a counted soft finding with no row is a number nobody can act on";
}

/// The observe-then-cut race: a life admitted between fsck's namespace-tree LIST and the catalog cut
/// it takes AFTER that listing must NOT be misread as residue. Mirrors
/// `CASNamespaceJanitor.PostListCatalogCutProtectsConcurrentCreationWithOneGet` -- the same ordering,
/// the same reason: creation admits `Creating` before writing any life-owned object, so a life visible
/// only in the LATER cut cannot have raced this listing.
namespace
{
class AdmitLifeAfterNamespaceListingBackend : public InMemoryBackend
{
public:
    explicit AdmitLifeAfterNamespaceListingBackend(NamespaceLifeId life_) : protected_life(std::move(life_)) {}

    ListPage list(const String & prefix, const String & cursor, size_t limit) override
    {
        ListPage page = InMemoryBackend::list(prefix, cursor, limit);
        if (!published && prefix.ends_with("/cas/ns/"))
        {
            published = true;
            CasRefCatalog::casAdmitEntry(*this, Layout("p"), /*gc_shards*/1,
                CatalogEntry{.ns = protected_life.ns, .state = NsState::Live,
                    .incarnation = protected_life.incarnation});
        }
        return page;
    }

private:
    NamespaceLifeId protected_life;
    bool published = false;
};
}

TEST(CASFsck, LifeAdmittedBetweenNamespaceListingAndLaterCutIsNotResidue)
{
    const RootNamespace ns{"00/late@cas@"};
    const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(ns, UInt128{909});
    auto backend = std::make_shared<AdmitLifeAfterNamespaceListingBackend>(life);
    auto store = openPoolForTest(backend);
    /// The physical object exists before the listing runs, exactly as a legitimate late admission would
    /// leave it: written only after `casAdmitEntry` above, but here pre-seeded since the injected
    /// backend admits the CATALOG row, not the physical file, on the list callback.
    ASSERT_EQ(backend->putIfAbsent(store->layout().namespaceFilesPrefix(life) + "format_version.txt", "1\n").outcome,
        PutOutcome::Done);

    FsckReport rep;
    ASSERT_NO_THROW(rep = runFsck(*store, /*detail*/true));
    EXPECT_EQ(rep.namespace_janitor_pending, 0u)
        << "a life visible in the post-listing cut must not be misclassified as residue";
    EXPECT_EQ(rep.lifeless_keys, 0u);
    EXPECT_TRUE(rep.clean());
}

/// Malformed or non-canonical namespace-tree shapes must stay HARD findings even after the
/// janitor-pending split: a dirty `_files` relative name (the parser-asymmetry fix), a zero life id, an
/// uppercase life id, and an unrecognized kind directory all name no current writer's grammar.
TEST(CASFsck, MalformedNamespaceTreeShapesStayHardFindings)
{
    const Layout layout("p");
    const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(RootNamespace{"00/bb@cas@"}, UInt128{909});
    const struct { String key; String description; } cases[] = {
        {layout.namespaceFilesPrefix(life) + "../escape", "dirty _files relative name"},
        {"p/cas/ns/state/" + String(32, '0') + "/_files/format_version.txt", "zero life id"},
        {"p/cas/ns/state/112233445566778899AABBCCDDEEFF01/_files/format_version.txt", "uppercase life id"},
        {"p/cas/ns/stream/" + renderIncarnation(UInt128{909}) + "/_unknown_kind/x.zst", "unknown kind directory"},
    };
    for (const auto & c : cases)
    {
        auto backend = std::make_shared<InMemoryBackend>();
        auto store = openPoolForTest(backend);
        ASSERT_EQ(backend->putIfAbsent(c.key, "garbage").outcome, PutOutcome::Done) << c.description;

        FsckReport rep;
        ASSERT_NO_THROW(rep = runFsck(*store, /*detail*/true)) << c.description;
        EXPECT_GE(rep.lifeless_keys, 1u) << c.description;
        EXPECT_EQ(rep.namespace_janitor_pending, 0u) << c.description;
        EXPECT_FALSE(rep.clean()) << c.description;
    }
}

/// Mutation caught: calling the destructive consumer's global `throwIfAmbiguous` from fsck aborts
/// before the unique row is audited. The read-only tool reports the ambiguous physical id and
/// continues through an unrelated unique namespace.
TEST(CASFsck, DuplicateLifeIdIsReportedWhileAnUnrelatedUniqueNamespaceStillProgresses)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace unique_ns{"00/unique@cas@"};
    const ManifestRef r = ref(1, 0xA1);
    writeBlobBody(*backend, layout, DB::UInt128(1));
    writeManifestRaw(*backend, layout, unique_ns, r, {blobEntryFor("a", DB::UInt128(1))});
    const uint64_t sequence = publishCommittedTransition(*backend, layout, unique_ns, "tbl", std::nullopt, r);
    writeFsckCheckpoint(*backend, layout, unique_ns, RefTxnId{1, sequence});

    CasRefCatalog::Snapshot snapshot = CasRefCatalog::read(*backend, layout);
    snapshot.catalog.entries.push_back(CatalogEntry{
        .ns = RootNamespace{"bad/a"}, .state = NsState::Live, .incarnation = UInt128{777}});
    snapshot.catalog.entries.push_back(CatalogEntry{
        .ns = RootNamespace{"bad/b"},
        .state = NsState::Removing,
        .incarnation = UInt128{777},
        .removal_started_round = 1});
    std::sort(snapshot.catalog.entries.begin(), snapshot.catalog.entries.end(),
        [](const CatalogEntry & lhs, const CatalogEntry & rhs) { return lhs.ns.string() < rhs.ns.string(); });
    const auto catalog_head = backend->head(layout.refCatalogKey());
    ASSERT_TRUE(catalog_head.exists);
    ASSERT_EQ(backend->putOverwrite(layout.refCatalogKey(), encodeRefCatalog(snapshot.catalog), catalog_head.token).outcome,
        PutOutcome::Done);

    FsckReport report;
    ASSERT_NO_THROW(report = runFsck(*store, /*detail=*/true));
    EXPECT_GE(report.lifeless_keys, 1u);
    EXPECT_GE(report.reachable, 1u) << "the unrelated unique namespace must still be audited";
    EXPECT_EQ(report.dangling, 0u);
}

/// A physical namespace-life key whose life id is ambiguous in the POST-LISTING cut (two catalog rows
/// share one incarnation) must be recorded as a `lifeless_keys` finding and must NOT abort the scan:
/// `CatalogLifeIndex::resolve` throws `CORRUPTED_DATA` on a duplicate, and the janitor-pending
/// classification loop must catch it exactly like every other catalog-authority failure in this scan.
/// Mirrors `DuplicateLifeIdIsReportedWhileAnUnrelatedUniqueNamespaceStillProgresses`, but that fixture
/// has no physical object under the duplicated life id, so it never drives a candidate into the new
/// post-listing loop at all -- this is the case that actually exercises it.
TEST(CASFsck, AmbiguousLifeUnderAPhysicalKeyIsRecordedNotAborted)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace unique_ns{"00/unique@cas@"};
    const ManifestRef r = ref(1, 0xA1);
    writeBlobBody(*backend, layout, DB::UInt128(1));
    writeManifestRaw(*backend, layout, unique_ns, r, {blobEntryFor("a", DB::UInt128(1))});
    const uint64_t sequence = publishCommittedTransition(*backend, layout, unique_ns, "tbl", std::nullopt, r);
    writeFsckCheckpoint(*backend, layout, unique_ns, RefTxnId{1, sequence});

    const NamespaceLifeId duplicated_life = NamespaceLifeId::fromCatalogEntry(RootNamespace{"bad/a"}, UInt128{777});
    ASSERT_EQ(backend->putIfAbsent(layout.namespaceFilesPrefix(duplicated_life) + "format_version.txt", "1\n").outcome,
        PutOutcome::Done);

    CasRefCatalog::Snapshot snapshot = CasRefCatalog::read(*backend, layout);
    snapshot.catalog.entries.push_back(CatalogEntry{
        .ns = RootNamespace{"bad/a"}, .state = NsState::Live, .incarnation = UInt128{777}});
    snapshot.catalog.entries.push_back(CatalogEntry{
        .ns = RootNamespace{"bad/b"},
        .state = NsState::Removing,
        .incarnation = UInt128{777},
        .removal_started_round = 1});
    std::sort(snapshot.catalog.entries.begin(), snapshot.catalog.entries.end(),
        [](const CatalogEntry & lhs, const CatalogEntry & rhs) { return lhs.ns.string() < rhs.ns.string(); });
    const auto catalog_head = backend->head(layout.refCatalogKey());
    ASSERT_TRUE(catalog_head.exists);
    ASSERT_EQ(backend->putOverwrite(layout.refCatalogKey(), encodeRefCatalog(snapshot.catalog), catalog_head.token).outcome,
        PutOutcome::Done);

    FsckReport report;
    ASSERT_NO_THROW(report = runFsck(*store, /*detail=*/true))
        << "an ambiguous life under a physical key must be a recorded finding, never an abort";
    EXPECT_GE(report.lifeless_keys, 1u);
    EXPECT_GE(report.reachable, 1u) << "the unrelated unique namespace must still be audited";
    EXPECT_EQ(report.dangling, 0u);
}

/// Fsck's namespace universe is catalog-authoritative. Admit `ns`, publish one real ref-log record,
/// then hide its whole stream prefix from LIST. Fsck must still retain the namespace from the catalog
/// cut and use the checkpoint-anchored arithmetic walk to read the record.
///
/// Proves `ref_records_walked`, not `dangling`/`clean()`: `checkRefStream` (which this proves runs) has
/// its own `_ckpt`-anchored arithmetic walk and so is reachable here. The distinct
/// `manifestStillReferenced` recheck now receives the same frozen catalog row and exact `_ckpt` authority;
/// its competing-cut regression is pinned separately by `MissingManifestRecheckStaysOnInitialCatalogCut`.
TEST(CASFsck, CatalogLiveNamespaceHiddenFromListIsStillWalked)
{
    auto backend = std::make_shared<HintHoleBackend>();
    auto store = openPoolForTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/hidden_from_list@cas@"};

    fixture::admitLive(*backend, layout, ns);
    const uint64_t sequence = appendRefLogSeed(
        *backend, layout, ns, {});   // one real record: a birth-only ref-log transaction

    /// `casAdmitEntry` never publishes a `_ckpt` (by its own design), and the write above used
    /// `appendRefLogSeed`'s hardcoded writer_epoch 1. `checkRefStream`'s own walk needs SOME anchor -- a
    /// `_ckpt.life_epoch`, a listed snapshot, or a listed log -- to know where to start reading, and this
    /// test is about to hide every listed one. Without an anchor the walk sees nothing at all and
    /// correctly treats the namespace as never-born, the same "nothing to probe" trap the I4 replacement
    /// controls hit and were restructured around (fold-before-hide). fsck has no "fold" step to run
    /// first, so the anchor is published directly, by exact key, before the hide -- the exact-key GET
    /// this enables is unaffected by list-hiding either way.
    writeFsckCheckpoint(*backend, layout, ns, RefTxnId{1, sequence});
    const NamespaceLifeId life = fixture::fixtureLife(ns);

    backend->hidePrefix(layout.namespaceStreamPrefix(life));

    FsckReport rep;
    ASSERT_NO_THROW(rep = runFsck(*store, /*detail*/true));
    EXPECT_GT(backend->holesServed(), 0u)
        << "the hide must actually have been exercised by the stream LIST, or this test passes vacuously";
    EXPECT_GE(rep.ref_records_walked, 1u)
        << "the namespace must be discovered and its stream actually read even when LIST omits every one "
           "of its keys, or the catalog-authoritative universe supplement did not run";
}

TEST(CASFsckAuthority, FullListingDoesNotDefineStreamGeometry)
{
    expectListingIndependentFsck(runFsckWithListingMode(FsckListingMode::Full, "full"));
}

TEST(CASFsckAuthority, EmptyListingDoesNotDefineStreamGeometry)
{
    expectListingIndependentFsck(runFsckWithListingMode(FsckListingMode::Empty, "empty"));
}

TEST(CASFsckAuthority, PartialListingDoesNotDefineStreamGeometry)
{
    expectListingIndependentFsck(runFsckWithListingMode(FsckListingMode::Partial, "partial"));
}

TEST(CASFsckAuthority, ReorderedListingDoesNotDefineStreamGeometry)
{
    expectListingIndependentFsck(runFsckWithListingMode(FsckListingMode::Reordered, "reordered"));
}

/// Stream LIST is not fsck authority. The same exact catalog + `_ckpt` + checkpoint-base triple must
/// yield the same result when LIST is complete, empty, partial, or reordered. A newer unadopted log and
/// snapshot are inert garbage, while damage to the exact checkpoint base remains a hard finding under
/// every listing. Mutation caught: the old LIST-derived snapshot oracle makes only listings that reveal
/// the unadopted pair non-clean.
TEST(CASFsckAuthority, StreamListingDoesNotChangeCheckpointBaseVerdict)
{
    const std::array modes{
        FsckListingMode::Full,
        FsckListingMode::Empty,
        FsckListingMode::Partial,
        FsckListingMode::Reordered,
    };

    std::optional<FsckAuthorityVerdict> clean_reference;
    std::optional<FsckAuthorityVerdict> corrupt_reference;
    for (size_t i = 0; i < modes.size(); ++i)
    {
        const FsckAuthorityVerdict clean = authorityVerdict(runCheckpointBaseFsckWithListingMode(
            modes[i], "clean_" + std::to_string(i), /*corrupt_exact_base=*/false));
        if (!clean_reference)
            clean_reference = clean;
        EXPECT_EQ(clean, *clean_reference);
        EXPECT_TRUE(clean.clean);
        EXPECT_EQ(clean.hard_findings, 0u);

        const FsckAuthorityVerdict corrupt = authorityVerdict(runCheckpointBaseFsckWithListingMode(
            modes[i], "corrupt_" + std::to_string(i), /*corrupt_exact_base=*/true));
        if (!corrupt_reference)
            corrupt_reference = corrupt;
        EXPECT_EQ(corrupt, *corrupt_reference);
        EXPECT_FALSE(corrupt.clean);
        EXPECT_EQ(corrupt.chain_broken, 1u);
        EXPECT_EQ(corrupt.unchecked, 0u);
        EXPECT_EQ(corrupt.hard_findings, 1u);
    }
}

/// A durable but unfrontiered F+1 is not part of this fsck cut. Mutation caught: probing one position
/// beyond `_ckpt.committed_through` walks the visible record and changes both coverage and reachability.
TEST(CASFsckAuthority, VisibleFPlusOneDoesNotAffectVerdict)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/visible_f_plus_one@cas@"};
    const ManifestRef committed_ref = ref(1, 0xE1);
    const ManifestRef unfrontiered_ref = ref(2, 0xE2);
    const DB::UInt128 committed_blob = u128Of("fsck-frontier-committed");
    writeBlobBody(*backend, layout, committed_blob);
    writeManifestRaw(*backend, layout, ns, committed_ref, {blobEntryFor("a", committed_blob)});
    const uint64_t frontier = publishCommittedTransition(
        *backend, layout, ns, "tbl", std::nullopt, committed_ref);
    writeFsckCheckpoint(*backend, layout, ns, RefTxnId{1, frontier});

    /// The object is durable and visible, but `_ckpt` is deliberately NOT advanced to it. The semantic
    /// convenience wrapper advances `_ckpt`, so deposit this unfrontiered F+1 as the raw transaction
    /// shape that a stopped writer can leave behind. Its missing manifest would become a false dangle if
    /// either fsck leg adopted F+1.
    std::vector<RefOp> unfrontiered_ops;
    unfrontiered_ops.push_back(ownerTransitionOp(
        RefOwnerBinding{RefOwnerKind::Committed, "tbl", committed_ref}, std::nullopt));
    const std::vector<RefOp> commit_ops = publishCommittedOps("tbl", unfrontiered_ref);
    unfrontiered_ops.insert(unfrontiered_ops.end(), commit_ops.begin(), commit_ops.end());
    ASSERT_EQ(appendRefLogSeed(*backend, layout, ns, std::move(unfrontiered_ops)), frontier + 1);

    const FsckReport report = runFsck(*store, /*detail=*/true);
    EXPECT_EQ(report.chain_broken, 0u);
    EXPECT_EQ(report.unchecked, 0u);
    EXPECT_EQ(report.ref_records_walked, 1u);
    EXPECT_EQ(report.dangling, 0u);
    EXPECT_EQ(report.reachable, 1u);
}

/// INV-2 materializes every burned global epoch, including an empty one, as a sequence-1 seal. A
/// direct `{1,2}` -> `{7,1}` chain that omits `{2,1}` is therefore data loss, not a legal sparse epoch
/// transition. Mutation caught: accepting the later head as a shortcut blesses the missing seal.
TEST(CASFsckAuthority, MissingBurnedEpochSealIsChainBroken)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/skipped_writer_epoch@cas@"};

    fixture::writeRefLogRaw(*backend, layout, RefLogTxn{
        .ns = ns.string(), .txn_id = RefTxnId{1, 1}, .ops = {namespaceBirthOp()},
        .prev_epoch_seal = std::nullopt});
    RefOp seal;
    seal.kind = RefOpKind::EpochSeal;
    fixture::writeRefLogRaw(*backend, layout, RefLogTxn{
        .ns = ns.string(), .txn_id = RefTxnId{1, 2}, .ops = {seal},
        .prev_epoch_seal = std::nullopt});

    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(*backend, layout, ns);
    /// The codec now rejects this skip. Deposit its old on-disk corruption shape by changing only the
    /// fixed-width epoch token of an otherwise encodable body, so fsck still proves that a missing
    /// intermediate epoch is reported rather than treated as a sparse legal transition.
    String skipped_bytes = encodeRefLogTxn(RefLogTxn{
        .ns = ns.string(), .txn_id = RefTxnId{7, 1}, .ops = {}, .prev_epoch_seal = RefTxnId{6, 1}});
    const String old_epoch_token = R"("!pse":"6")";
    const auto old_epoch = skipped_bytes.find(old_epoch_token);
    ASSERT_NE(old_epoch, String::npos);
    skipped_bytes.replace(old_epoch, old_epoch_token.size(), R"("!pse":"1")");
    ASSERT_EQ(backend->putIfAbsent(layout.refLogKey(life, RefTxnId{7, 1}),
        sealObject(FormatId::RefLog, skipped_bytes)).outcome, PutOutcome::Done);

    ASSERT_EQ(backend->putIfAbsent(layout.refCkptKey(life), encodeRefCkpt(RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{7, 1},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = RefTxnId{6, 1}})).outcome, PutOutcome::Done);

    const FsckReport report = runFsck(*store, /*detail=*/true);
    EXPECT_EQ(report.chain_broken, 1u);
    EXPECT_EQ(report.unchecked, 0u);
    EXPECT_EQ(report.ref_records_walked, 2u);
}

/// The checkpoint base is the inclusive frontier, so there is no replay tail in which another hole
/// could satisfy this test. The missing same-id log itself makes the stable exact authority corrupt.
/// Mutation caught: mapping every `readCheckpointSnapshotBase` failure to `Unchecked` leaves `clean` true.
TEST(CASFsckAuthority, MissingCheckpointBaseLogIsChainBroken)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/missing_checkpoint_base_log@cas@"};
    fixture::admitLive(*backend, layout, ns);

    const RefTxnId base{1, 1};
    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(*backend, layout, ns);
    writeFsckCheckpointWithBase(*backend, layout, ns, base);

    const FsckReport report = runFsck(*store, /*detail=*/true);
    EXPECT_EQ(report.ref_records_walked, 0u);
    expectCheckpointBaseVerdict(
        report, layout.refSnapshotKey(life, base), FsckClass::ChainBroken, "has no matching log");
}

/// A present, valid non-seal base log rules out a stream hole; only its checkpoint-named same-id
/// snapshot is absent. Stable exact absence is damage, not lost diagnostic coverage.
TEST(CASFsckAuthority, MissingCheckpointBaseSnapshotIsChainBroken)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/missing_checkpoint_base_snapshot@cas@"};
    fixture::admitLive(*backend, layout, ns);

    const RefTxnId base{1, 1};
    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(*backend, layout, ns);
    fixture::writeRefLogRaw(*backend, layout, RefLogTxn{
        .ns = ns.string(), .txn_id = base, .ops = {namespaceBirthOp()}, .prev_epoch_seal = std::nullopt});
    writeFsckCheckpointWithBase(*backend, layout, ns, base);

    const FsckReport report = runFsck(*store, /*detail=*/true);
    EXPECT_EQ(report.ref_records_walked, 0u);
    expectCheckpointBaseVerdict(
        report, layout.refSnapshotKey(life, base), FsckClass::ChainBroken,
        "is absent under the supplied immutable lifecycle authority");
}

/// `_ckpt.checkpoint_snapshot_id` names a state snapshot, never an `EpochSeal`. The forged base is an
/// OLDER seal, deliberately different from `last_epoch_seal`, so comparing checkpoint metadata cannot
/// expose it: the stream audit must exact-read the base log and reject it before recovery can bless the
/// same-id snapshot.
TEST(CASFsckAuthority, CheckpointSnapshotAtOlderEpochSealIsChainBroken)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/checkpoint_base_seal@cas@"};
    fixture::admitLive(*backend, layout, ns);

    const RefLogTxn birth{
        .ns = ns.string(), .txn_id = RefTxnId{1, 1}, .ops = {namespaceBirthOp()},
        .prev_epoch_seal = std::nullopt};
    fixture::writeRefLogRaw(*backend, layout, birth);
    RefOp seal;
    seal.kind = RefOpKind::EpochSeal;
    const RefLogTxn seal_txn{
        .ns = ns.string(), .txn_id = RefTxnId{1, 2}, .ops = {seal},
        .prev_epoch_seal = std::nullopt};
    fixture::writeRefLogRaw(*backend, layout, seal_txn);
    RefOp later_seal;
    later_seal.kind = RefOpKind::EpochSeal;
    fixture::writeRefLogRaw(*backend, layout, RefLogTxn{
        .ns = ns.string(), .txn_id = RefTxnId{2, 1}, .ops = {later_seal},
        .prev_epoch_seal = RefTxnId{1, 2}});

    RefTableState through_seal;
    applyRefLogTxn(through_seal, birth);
    applyRefLogTxn(through_seal, seal_txn);
    writeRefSnapshotRaw(*backend, layout, snapshotOf(through_seal, ns.string()));

    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(*backend, layout, ns);
    ASSERT_EQ(backend->putIfAbsent(layout.refCkptKey(life), encodeRefCkpt(RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{2, 1},
        .checkpoint_snapshot_id = RefTxnId{1, 2},
        .last_epoch_seal = RefTxnId{2, 1}})).outcome, PutOutcome::Done);

    const FsckReport report = runFsck(*store, /*detail=*/true);
    EXPECT_EQ(report.ref_records_walked, 0u)
        << "the seal is rejected as the checkpoint base, not walked as a normal replay record";
    expectCheckpointBaseVerdict(
        report, layout.refSnapshotKey(life, RefTxnId{1, 2}), FsckClass::ChainBroken,
        "names an EpochSeal, not a snapshot base");
}

/// An unstable transport failure while exact-reading the same valid checkpoint base proves neither
/// presence nor absence. It remains the honest third answer and must not become a hard finding.
TEST(CASFsckAuthority, CheckpointBaseTransportFailureIsUnchecked)
{
    auto backend = std::make_shared<FailExactGetBackend>();
    auto store = openPoolForTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/checkpoint_base_transport@cas@"};
    fixture::admitLive(*backend, layout, ns);

    const RefTxnId base{1, 1};
    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(*backend, layout, ns);
    fixture::writeRefLogRaw(*backend, layout, RefLogTxn{
        .ns = ns.string(), .txn_id = base, .ops = {namespaceBirthOp()}, .prev_epoch_seal = std::nullopt});
    RefTableState state;
    applyRefLogTxn(state, RefLogTxn{
        .ns = ns.string(), .txn_id = base, .ops = {namespaceBirthOp()}, .prev_epoch_seal = std::nullopt});
    writeRefSnapshotRaw(*backend, layout, snapshotOf(state, ns.string()));
    writeFsckCheckpointWithBase(*backend, layout, ns, base);
    backend->fail(layout.refLogKey(life, base));

    const FsckReport report = runFsck(*store, /*detail=*/true);
    EXPECT_EQ(report.ref_records_walked, 0u);
    expectCheckpointBaseVerdict(
        report, layout.refSnapshotKey(life, base), FsckClass::Unchecked, "injected exact GET failure");
}

/// The sampled checkpoint is immutable input, but cleanup may advance `_ckpt` after that sample and
/// retire its old base before fsck exact-reads it. The miss is then authority instability, not evidence
/// that either durable checkpoint incarnation was internally corrupt.
/// Mutation caught: classifying `CORRUPTED_DATA` without rechecking the sampled checkpoint token.
TEST(CASFsckAuthority, CheckpointBaseVanishingAfterAuthorityAdvanceIsUnchecked)
{
    auto backend = std::make_shared<MutateOnFirstGetBackend>();
    auto store = openPoolForTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/checkpoint_base_advanced@cas@"};
    fixture::admitLive(*backend, layout, ns);

    const RefTxnId old_base{1, 1};
    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(*backend, layout, ns);
    writeFsckCheckpointWithBase(*backend, layout, ns, old_base);
    backend->armOnFirstGet(layout.refLogKey(life, old_base), [&]
    {
        const String ckpt_key = layout.refCkptKey(life);
        const HeadResult head = backend->head(ckpt_key);
        ASSERT_TRUE(head.exists);
        ASSERT_EQ(backend->putOverwrite(ckpt_key, encodeRefCkpt(RefCkpt{
            .life_epoch = 1,
            .committed_through = std::nullopt,
            .checkpoint_snapshot_id = std::nullopt,
            .last_epoch_seal = std::nullopt}), head.token).outcome, PutOutcome::Done);
    });

    const FsckReport report = runFsck(*store, /*detail=*/true);
    EXPECT_EQ(report.ref_records_walked, 0u);
    expectCheckpointBaseVerdict(
        report, layout.refSnapshotKey(life, old_base), FsckClass::Unchecked,
        "checkpoint authority changed while validating its snapshot base");
}

/// A Live catalog row without `_ckpt` is not a recoverable table, even when a listing happens to show a
/// complete ref log. Mutation caught: replacing the authority-taking recovery with the old LIST replay
/// makes this audit look clean and silently blesses a life whose durable frontier is unknown.
TEST(CASFsck, LiveNamespaceWithoutCheckpointIsUnchecked)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/live_without_checkpoint@cas@"};
    const ManifestRef r = ref(1, 0xC1);
    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    /// `publishCommittedTransition` correctly advances `_ckpt`; this test instead deposits the raw
    /// missing-checkpoint corruption shape that fsck must refuse to recover.
    std::vector<RefOp> ops = publishCommittedOps("tbl", r);
    appendRefLogSeed(*backend, store->layout(), ns, std::move(ops));

    const FsckReport report = runFsck(*store, /*detail=*/true);
    EXPECT_GE(report.unchecked, 1u)
        << "a Live life with no exact checkpoint cannot be recovered from a convenient LIST";
    EXPECT_EQ(report.reachable, 0u)
        << "fsck must not consume refs after the mandatory recovery authority was absent";
}

/// fsck's missing-manifest recheck runs after its primary walk. If it re-resolves the name from a second
/// catalog cut, a concurrent rebirth can make the old durable owner disappear from the recheck and hide
/// a real dangle. The initial cut's row and exact checkpoint must remain the sole authority throughout
/// the whole fsck call.
///
/// Mutation caught: re-resolve `ns` from `manifestStillReferenced`. The first manifest GET changes the
/// catalog to a fresh, empty life; the second resolution then sees no owner and suppresses the dangle. A
/// recovery from the original cut continues to see the original owner and reports it.
TEST(CASFsck, MissingManifestRecheckStaysOnInitialCatalogCut)
{
    auto backend = std::make_shared<MutateOnFirstGetBackend>();
    auto store = openPoolForTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/frozen_fsck_cut@cas@"};
    const ManifestRef r = ref(1, 0xC2);
    const uint64_t sequence = publishCommittedTransition(*backend, layout, ns, "tbl", std::nullopt, r);
    /// `publishCommittedTransition`'s raw fixture log writer uses its documented epoch 1.
    writeFsckCheckpoint(*backend, layout, ns, RefTxnId{1, sequence});

    backend->armOnFirstGet(layout.manifestKey(ManifestId{ns, r}), [&]
    {
        replaceCatalogLife(*backend, layout, ns, UInt128{0xC3});
    });

    const FsckReport report = runFsck(*store, /*detail=*/true);
    EXPECT_GE(report.dangling, 1u)
        << "the initial catalog-cut owner still names the absent manifest despite a later rebirth";
}

/// A pre-precommit body in an eligible prefix (no owner) is INFO (Unreachable), not an error.
TEST(CASFsck, ReclaimablePrePrecommitBodyIsInfo)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    /// Seed a birth-only ref log through the fixture helper, which also admits the catalog row, while
    /// leaving NO committed owner; the manifest body below is orphan debris.
    const uint64_t sequence = appendRefLogSeed(*backend, store->layout(), ns, {});
    writeFsckCheckpoint(*backend, store->layout(), ns, RefTxnId{1, sequence});
    const ManifestRef r = ref(5, 0xAB);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});   // body, no owner
    setWatermarkMinActive(*backend, store->layout(), kServerRoot, kWriterEpoch, 6);   // eligible
    const FsckReport rep = runFsck(*store, /*detail*/true);
    EXPECT_TRUE(rep.clean());            // not an error
    EXPECT_GE(rep.unreachable, 1u);      // counted as info/unreachable
}

/// Pipeline classification (2026-07-02): a condemned-but-present blob is PendingGc — an EXPECTED
/// pipeline state (deletion is scheduled), never the suspicious "unreachable" lump beta testers
/// read as a leak. clean() is unaffected.
TEST(CASFsck, CondemnedBlobClassifiesPendingGc)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(1, 0xA1);
    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    const uint64_t publish_sequence = publishCommittedTransition(
        *backend, store->layout(), ns, "tbl", std::nullopt, r);
    writeFsckCheckpoint(*backend, store->layout(), ns, RefTxnId{1, publish_sequence});
    Gc gc(store, hexToU128("00000000000000000000000000000001"));
    gc.runRegularRound();
    const uint64_t drop_sequence = dropRefTransition(*backend, store->layout(), ns, "tbl", r);
    writeFsckCheckpoint(*backend, store->layout(), ns, RefTxnId{1, drop_sequence});
    gc.runRegularRound();   /// -1 folds => zero => condemned into the retired list; blob still present

    const FsckReport rep = runFsck(*store, /*detail*/true);
    EXPECT_TRUE(rep.clean());
    EXPECT_EQ(rep.pending_gc, 1u);
    EXPECT_EQ(rep.unaccounted, 0u);
    bool saw = false;
    for (const FsckObject & o : rep.objects)
        if (o.cls == FsckClass::PendingGc)
        {
            saw = true;
            ASSERT_FALSE(o.reachable_from.empty());
            EXPECT_NE(o.reachable_from[0].find("condemned at round"), String::npos);
        }
    EXPECT_TRUE(saw);
}

/// A drop whose -1 has NOT folded yet: the blob's edges are still in the GC snapshot => AwaitingGc
/// (expected), not Unaccounted.
TEST(CASFsck, DroppedButUnfoldedBlobClassifiesAwaitingGc)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(1, 0xA1);
    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    const uint64_t publish_sequence = publishCommittedTransition(
        *backend, store->layout(), ns, "tbl", std::nullopt, r);
    writeFsckCheckpoint(*backend, store->layout(), ns, RefTxnId{1, publish_sequence});
    Gc gc(store, hexToU128("00000000000000000000000000000001"));
    gc.runRegularRound();                                        /// +1 folded into the snapshot
    const uint64_t drop_sequence = dropRefTransition(
        *backend, store->layout(), ns, "tbl", r);  /// -1 NOT folded (no round)
    writeFsckCheckpoint(*backend, store->layout(), ns, RefTxnId{1, drop_sequence});

    const FsckReport rep = runFsck(*store, /*detail*/true);
    EXPECT_TRUE(rep.clean());
    EXPECT_EQ(rep.awaiting_gc, 1u);
    EXPECT_EQ(rep.unaccounted, 0u);
}

/// Stale-edge cross-check, NEGATIVE side: the residual edge's source manifest body is still PRESENT in
/// the pool, so its removal still has a `-1` to fold (and the orphan sweep still has a body to reclaim).
/// That is a genuine mid-pipeline backlog and must keep the `AwaitingGc` verdict — the new check may
/// never turn an ordinary unfolded drop into a hard finding.
TEST(CASFsck, UnfoldedDropWithPresentSourceManifestStaysAwaitingGc)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(1, 0xA1);
    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    const uint64_t publish_sequence = publishCommittedTransition(
        *backend, store->layout(), ns, "tbl", std::nullopt, r);
    writeFsckCheckpoint(*backend, store->layout(), ns, RefTxnId{1, publish_sequence});
    Gc gc(store, hexToU128("00000000000000000000000000000001"));
    gc.runRegularRound();                                        /// +1 folded into the snapshot
    const uint64_t drop_sequence = dropRefTransition(
        *backend, store->layout(), ns, "tbl", r);  /// -1 NOT folded; the BODY survives
    writeFsckCheckpoint(*backend, store->layout(), ns, RefTxnId{1, drop_sequence});

    const FsckReport rep = runFsck(*store, /*detail*/true);
    EXPECT_TRUE(rep.clean());
    EXPECT_EQ(rep.awaiting_gc, 1u);
    EXPECT_EQ(rep.stale_edge, 0u);
    bool saw = false;
    for (const FsckObject & o : rep.objects)
        if (o.cls == FsckClass::AwaitingGc)
            saw = true;
    EXPECT_TRUE(saw);
}

/// Stale-edge cross-check, POSITIVE side: the blob's only residual `+1` names a manifest that no longer
/// exists anywhere in the pool, so no `-1` is left to fold — the in-degree stays at 1 for every future
/// round and the incremental GC can never nominate the blob. It must NOT be labeled `AwaitingGc`
/// ("expected, no action needed", the sentence that hid 56 permanently retained blobs); it is the hard
/// `StaleEdge` finding and the report is not `clean()`.
TEST(CASFsck, ResidualEdgeNamingAnAbsentManifestClassifiesStaleEdge)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(1, 0xA1);
    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    const ManifestId id = writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    const uint64_t publish_sequence = publishCommittedTransition(
        *backend, store->layout(), ns, "tbl", std::nullopt, r);
    writeFsckCheckpoint(*backend, store->layout(), ns, RefTxnId{1, publish_sequence});
    Gc gc(store, hexToU128("00000000000000000000000000000001"));
    gc.runRegularRound();                                        /// +1 folded into the snapshot
    const uint64_t drop_sequence = dropRefTransition(
        *backend, store->layout(), ns, "tbl", r);  /// the owner is gone ...
    writeFsckCheckpoint(*backend, store->layout(), ns, RefTxnId{1, drop_sequence});
    deleteManifestBody(*backend, store->layout(), id);           /// ... and so is the body, un-folded

    const FsckReport rep = runFsck(*store, /*detail*/true);
    EXPECT_EQ(rep.stale_edge, 1u);
    EXPECT_EQ(rep.awaiting_gc, 0u);
    EXPECT_EQ(rep.dangling, 0u) << "no committed ref names the manifest any more — this is not a dangle";
    EXPECT_FALSE(rep.clean());
    bool saw = false;
    for (const FsckObject & o : rep.objects)
        if (o.cls == FsckClass::StaleEdge)
        {
            saw = true;
            ASSERT_FALSE(o.reachable_from.empty());
            EXPECT_NE(o.reachable_from[0].find("no longer exist"), String::npos);
        }
    EXPECT_TRUE(saw);
}

/// GC never ran on the pool: nothing is classifiable through the GC view — everything unreferenced
/// is AwaitingGc ("GC has not run yet"), never a false Unaccounted alarm.
TEST(CASFsck, GcNeverRanClassifiesAwaitingGc)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    writeBlobBody(*backend, store->layout(), DB::UInt128(5));   /// present, never referenced, no gc/state

    const FsckReport rep = runFsck(*store, /*detail*/true);
    EXPECT_TRUE(rep.clean());
    EXPECT_EQ(rep.awaiting_gc, 1u);
    EXPECT_EQ(rep.unaccounted, 0u);
}

/// A blob outside the WHOLE GC view on a pool where GC runs: Unaccounted — expected only as a
/// transient (fast create+drop between rounds); persistent occurrences violate INV-2.
TEST(CASFsck, ForeignBlobClassifiesUnaccounted)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(1, 0xA1);
    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    const uint64_t sequence = publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);
    writeFsckCheckpoint(*backend, store->layout(), ns, RefTxnId{1, sequence});
    Gc gc(store, hexToU128("00000000000000000000000000000001"));
    gc.runRegularRound();

    writeBlobBody(*backend, store->layout(), DB::UInt128(0xF0F0));   /// never referenced anywhere

    const FsckReport rep = runFsck(*store, /*detail*/true);
    EXPECT_TRUE(rep.clean());
    EXPECT_EQ(rep.unaccounted, 1u);
    EXPECT_EQ(rep.pending_gc, 0u);
}

/// A `.meta` descriptor whose body is missing is ADVISORY (meta_without_body), NOT a hard finding:
/// GC deletes the body FIRST and drops the `.meta` afterwards on a bounded, error-suppressed advisory
/// pool that may drop the op, so a single raw LIST legitimately observes a body-less `.meta` mid-
/// graduation and no finite grace makes a persistent one hard evidence. It is still counted/reported;
/// it must NOT be a `dangling` (nothing referenced it) and NOT one of the present-but-unreferenced blob
/// pipeline classes (the `.meta` key is excluded from body classification entirely). `clean()` stays TRUE.
TEST(CASFsck, MetaWithoutBodyIsAdvisoryNotHard)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const DB::UInt128 h = u128Of("meta-without-body");
    writeMetaClean(*backend, store->layout(), h, /*size*/ 10);   /// meta only, no body written

    const FsckReport rep = runFsck(*store, /*detail*/true);
    EXPECT_GE(rep.meta_without_body, 1u);   // still counted and reported in the full report
    EXPECT_EQ(rep.dangling, 0u);
    EXPECT_EQ(rep.unreachable, 0u);
    EXPECT_EQ(rep.pending_gc, 0u);
    EXPECT_EQ(rep.awaiting_gc, 0u);
    EXPECT_EQ(rep.unaccounted, 0u);
    EXPECT_TRUE(rep.clean());   // meta_without_body is advisory — excluded from clean()
}

/// A body with no `.meta` sibling is a BENIGN not-yet-adopted (or crashed-birth) artifact — NOT a
/// dangle, and it must still classify through the ordinary present-but-unreferenced pipeline.
TEST(CASFsck, BodyWithoutMetaIsBenign)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const DB::UInt128 h = u128Of("body-without-meta");
    writeBlobBody(*backend, store->layout(), h);   /// body only, no meta written

    const FsckReport rep = runFsck(*store, /*detail*/true);
    EXPECT_GE(rep.body_without_meta, 1u);
    EXPECT_EQ(rep.dangling, 0u);
    EXPECT_EQ(rep.meta_without_body, 0u);
    EXPECT_TRUE(rep.clean());
}

/// A scan whose deadline is already in the past: partial_on_deadline=false keeps the old
/// throw-on-timeout contract; partial_on_deadline=true returns the accumulated lower-bound counts
/// instead of failing empty-handed (the 2026-07-05 campaign lost 5 verdicts to this).
TEST(CASFsckPartial, DeadlineReturnsAccumulatedCountsInsteadOfThrowing)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(1, 0xAA);
    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    const uint64_t sequence = publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);
    writeFsckCheckpoint(*backend, store->layout(), ns, RefTxnId{1, sequence});

    const auto past = std::chrono::steady_clock::now() - std::chrono::seconds(1);
    /// partial_on_deadline=false keeps the old contract:
    EXPECT_THROW(DB::Cas::runFsck(*store, /*detail=*/false, {}, past), DB::Exception);
    /// partial_on_deadline=true returns a flagged report:
    const auto report = DB::Cas::runFsck(*store, false, {}, past, /*partial_on_deadline=*/true);
    EXPECT_TRUE(report.partial);
    EXPECT_FALSE(report.partial_reason.empty());
}

/// A `namespace_prefix` scopes the scan to only the matching namespaces' refs (dangling-only): no
/// pool-wide unreachable/pending/awaiting/unaccounted classification, since that needs the whole pool.
TEST(CASFsckScoped, NamespacePrefixChecksOnlyMatchingRefsDanglingOnly)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);

    const RootNamespace ns_a{"nsa"};
    const ManifestRef r_a = ref(1, 0xA1);
    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns_a, r_a, {blobEntryFor("a", DB::UInt128(1))});
    const uint64_t sequence_a = publishCommittedTransition(
        *backend, store->layout(), ns_a, "tbl", std::nullopt, r_a);
    writeFsckCheckpoint(*backend, store->layout(), ns_a, RefTxnId{1, sequence_a});

    const RootNamespace ns_b{"nsb"};
    const ManifestRef r_b = ref(1, 0xB1);
    writeBlobBody(*backend, store->layout(), DB::UInt128(2));
    writeManifestRaw(*backend, store->layout(), ns_b, r_b, {blobEntryFor("b", DB::UInt128(2))});
    const uint64_t sequence_b = publishCommittedTransition(
        *backend, store->layout(), ns_b, "tbl", std::nullopt, r_b);
    writeFsckCheckpoint(*backend, store->layout(), ns_b, RefTxnId{1, sequence_b});

    const auto scoped = DB::Cas::runFsck(*store, false, {}, {}, false, /*namespace_prefix=*/"nsa");
    EXPECT_EQ(scoped.dangling, 0u);
    EXPECT_GT(scoped.reachable, 0u);
    /// Scoped mode skips only the POOL-WIDE physical/pipeline classification; the manifest-debris
    /// pass stays active for the scoped namespaces, so `unreachable` here counts THEIR orphan
    /// manifest bodies — zero in this clean setup, legitimately nonzero on a churned pool.
    EXPECT_EQ(scoped.unreachable, 0u);
    EXPECT_EQ(scoped.pending_gc + scoped.awaiting_gc + scoped.unaccounted, 0u);
}

/// B207: the ref-walk and the HEAD-confirm run minutes apart with no snapshot. A ref that gets
/// RE-PUBLISHED to a different manifest in that window, combined with a legitimate GC delete of the
/// blob it used to name, must NOT surface as a phantom `dangling` — only a CURRENT ref over an absent
/// object is a real dangle.
TEST(CASFsck, PhantomDanglingFromRepublishedRefIsReresolvedAway)
{
    auto backend = std::make_shared<RepublishOnListBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r1 = ref(1, 0xA1);
    const ManifestRef r2 = ref(2, 0xA2);
    const DB::UInt128 h1 = u128Of("b207-phantom-old");
    const DB::UInt128 h2 = u128Of("b207-phantom-new");

    writeBlobBody(*backend, store->layout(), h1);
    writeManifestRaw(*backend, store->layout(), ns, r1, {blobEntryFor("a", h1)});
    const uint64_t initial_sequence = publishCommittedTransition(
        *backend, store->layout(), ns, "tbl", std::nullopt, r1);
    writeFsckCheckpoint(*backend, store->layout(), ns, RefTxnId{1, initial_sequence});

    /// Fires strictly between the ref-walk (which captures ref "tbl" -> r1, blob h1, as reachable) and
    /// the HEAD-confirm's physical listing — exactly the window B207 is about.
    backend->armOnFirstList(store->layout().blobsPrefix(), [&]
    {
        writeBlobBody(*backend, store->layout(), h2);
        writeManifestRaw(*backend, store->layout(), ns, r2, {blobEntryFor("a", h2)});
        const uint64_t repoint_sequence = publishCommittedTransition(
            *backend, store->layout(), ns, "tbl", r1, r2);   /// re-publish
        writeFsckCheckpoint(*backend, store->layout(), ns, RefTxnId{1, repoint_sequence});

        const String old_key = store->layout().blobKey(BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(h1)});
        const HeadResult head = backend->head(old_key);
        ASSERT_TRUE(head.exists);
        backend->deleteExact(old_key, head.token);   /// legitimate GC delete of the now-unreferenced blob
    });

    const FsckReport rep = runFsck(*store, /*detail*/true);
    EXPECT_EQ(rep.dangling, 0u);
    EXPECT_TRUE(rep.clean());
}

/// Same race, but the ref is DROPPED (not re-published) in the window between the walk and the
/// HEAD-confirm — also must not surface as a phantom dangle.
TEST(CASFsck, PhantomDanglingFromDroppedRefIsReresolvedAway)
{
    auto backend = std::make_shared<RepublishOnListBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r1 = ref(1, 0xA1);
    const DB::UInt128 h1 = u128Of("b207-phantom-dropped");

    writeBlobBody(*backend, store->layout(), h1);
    writeManifestRaw(*backend, store->layout(), ns, r1, {blobEntryFor("a", h1)});
    const uint64_t initial_sequence = publishCommittedTransition(
        *backend, store->layout(), ns, "tbl", std::nullopt, r1);
    writeFsckCheckpoint(*backend, store->layout(), ns, RefTxnId{1, initial_sequence});

    backend->armOnFirstList(store->layout().blobsPrefix(), [&]
    {
        const uint64_t drop_sequence = dropRefTransition(
            *backend, store->layout(), ns, "tbl", r1);   /// ref dropped since the walk
        writeFsckCheckpoint(*backend, store->layout(), ns, RefTxnId{1, drop_sequence});

        const String old_key = store->layout().blobKey(BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(h1)});
        const HeadResult head = backend->head(old_key);
        ASSERT_TRUE(head.exists);
        backend->deleteExact(old_key, head.token);   /// legitimate GC delete after the drop folds
    });

    const FsckReport rep = runFsck(*store, /*detail*/true);
    EXPECT_EQ(rep.dangling, 0u);
    EXPECT_TRUE(rep.clean());
}

/// Companion: the fix must never HIDE a real loss. A blob that a CURRENT ref still names, but whose
/// object is genuinely gone (an operator error, a storage-layer bug — NOT a legitimate GC delete),
/// stays `dangling` after the re-resolve.
TEST(CASFsck, RealDanglingStillCaughtAfterReresolve)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(1, 0xA1);
    const DB::UInt128 h = u128Of("b207-real-dangle");

    writeBlobBody(*backend, store->layout(), h);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", h)});
    const uint64_t sequence = publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);
    writeFsckCheckpoint(*backend, store->layout(), ns, RefTxnId{1, sequence});

    const String key = store->layout().blobKey(BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(h)});
    const HeadResult head = backend->head(key);
    ASSERT_TRUE(head.exists);
    backend->deleteExact(key, head.token);   /// genuine loss — the ref is UNCHANGED, still names this blob

    const FsckReport rep = runFsck(*store, /*detail*/true);
    EXPECT_EQ(rep.dangling, 1u);
    EXPECT_FALSE(rep.clean());
}

/// The MANIFEST analogue of the blob phantom-dangle. The ref-walk captures "tbl" -> r1's manifest, then
/// the ref is RE-PUBLISHED to a different manifest r2 and the OLD r1 manifest body is legitimately
/// GC-deleted before the per-ref body GET. The missing OLD manifest must be revalidated away — a fresh
/// re-resolve shows the CURRENT ref no longer names it — never surfacing as a phantom `dangling`.
TEST(CASFsck, PhantomDanglingManifestFromRepublishedRefIsReresolvedAway)
{
    auto backend = std::make_shared<MutateOnFirstGetBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r1 = ref(1, 0xA1);
    const ManifestRef r2 = ref(2, 0xA2);
    const DB::UInt128 h1 = u128Of("phantom-manifest-old");
    const DB::UInt128 h2 = u128Of("phantom-manifest-new");

    writeBlobBody(*backend, store->layout(), h1);
    writeManifestRaw(*backend, store->layout(), ns, r1, {blobEntryFor("a", h1)});
    const uint64_t initial_sequence = publishCommittedTransition(
        *backend, store->layout(), ns, "tbl", std::nullopt, r1);
    writeFsckCheckpoint(*backend, store->layout(), ns, RefTxnId{1, initial_sequence});

    const String m1_key = store->layout().manifestKey(ManifestId{ns, r1});
    /// Fires strictly between the ref-walk (captures "tbl" -> r1) and the per-ref GET of r1's manifest:
    /// re-publish "tbl" to r2 and legitimately GC-delete the now-superseded r1 manifest body.
    backend->armOnFirstGet(m1_key, [&]
    {
        writeBlobBody(*backend, store->layout(), h2);
        writeManifestRaw(*backend, store->layout(), ns, r2, {blobEntryFor("a", h2)});
        const uint64_t repoint_sequence = publishCommittedTransition(
            *backend, store->layout(), ns, "tbl", r1, r2);   /// re-publish
        writeFsckCheckpoint(*backend, store->layout(), ns, RefTxnId{1, repoint_sequence});

        const HeadResult head = backend->head(m1_key);
        ASSERT_TRUE(head.exists);
        backend->deleteExact(m1_key, head.token);   /// legitimate GC delete of the superseded manifest
    });

    const FsckReport rep = runFsck(*store, /*detail*/true);
    EXPECT_EQ(rep.dangling, 0u);
    EXPECT_TRUE(rep.clean());
}

namespace
{
/// Build a real `ContentAddressedMetadataStorage` over Local object storage and start it (Mounted) --
/// the same harness gtest_cas_operation_gate.cpp uses. Each call gets an isolated pool root.
std::shared_ptr<DB::ContentAddressedMetadataStorage> openRunningStorageForTest()
{
    auto settings = makeSettingsForTest("test", std::filesystem::temp_directory_path() / "ca_fsck_running_scratch");
    auto storage = std::make_shared<DB::ContentAddressedMetadataStorage>(
        makeLocalObjectStorageForTest(), "pool", "srv1", "", nullptr, settings);
    storage->startup();
    return storage;
}

/// Commit one real part (tmp -> final rename -> commit) so a RUNNING FSCK has live committed content.
void commitOneRunningPart(DB::ContentAddressedMetadataStorage & storage)
{
    const std::string table_dir = "g80/g80g80g8-0808-4808-8808-080808080808";
    auto tx = storage.createTransaction();
    auto & ca_tx = dynamic_cast<DB::ContentAddressedTransaction &>(*tx);
    auto buf = ca_tx.writeFile(table_dir + "/tmp_insert_all_1_1_0/data.bin", 65536, DB::WriteMode::Rewrite, {});
    const std::string bytes = "content-of-the-part";
    buf->write(bytes.data(), bytes.size());
    buf->finalize();
    tx->moveDirectory(table_dir + "/tmp_insert_all_1_1_0", table_dir + "/all_1_1_0");
    tx->commit(DB::NoCommitOptions{});
}
}

/// (rev.8) FSCK runs on a RUNNING disk: scanning a live pool with one committed part succeeds and reports
/// its content (the one-row summary the SQL verb renders from this report).
TEST(CASFsckRunning, FsckOnMountedDiskSucceeds)
{
    auto storage = openRunningStorageForTest();
    commitOneRunningPart(*storage);

    FsckReport rep;
    EXPECT_NO_THROW(rep = storage->runFsckNow(/*detail=*/false));
    EXPECT_TRUE(rep.clean());
    EXPECT_GE(rep.distinct_blobs, 1u) << "the running scan must see the live committed part's blob";
    EXPECT_EQ(rep.dangling, 0u);
}

/// (rev.8) FSCK is Admin-class: on a not-live pool (a lease blip / IdentityLost) it refuses before
/// scanning, exactly like the GC entry points -- an FSCK of a disk whose data root may be gone or replaced
/// is meaningless (the operator has the snapshot / FORGET path). The two states refuse in DIFFERENT
/// classes, and the pairing is the point: a lease blip is transient unavailability (upstream-retryable),
/// an identity loss is terminal (668).
TEST(CASFsckRunning, FsckOnNotLiveDiskRefusesTransientRetryableAndIdentityLostTerminal)
{
    for (const auto & [lc, code] : {std::pair{PoolLifecycle::TransientNotLive, DB::ErrorCodes::NETWORK_ERROR},
                                    std::pair{PoolLifecycle::IdentityLost, DB::ErrorCodes::INVALID_STATE}})
    {
        auto storage = openRunningStorageForTest();
        storage->store()->setLifecycleForTest(lc);   /// one force from Live; no later store() call
        expectThrowsCode(code, [&] { storage->runFsckNow(/*detail=*/false); });
    }
}

/// The summary line is the ONLY thing most consumers ever read: the soak harness parses it, an operator
/// eyeballs it, and `exit_code` gates CI on it. So a field that `clean()` treats as a hard finding but the
/// summary omits is invisible in practice, however faithfully it is counted -- which is exactly what
/// happened to `corrupted_runs`: counted since the seal check landed, part of `clean()`, rendered in
/// `--detail` rows, and absent from the summary, so no run has ever reported one.
///
/// This test ITERATES `kFsckHardFindings` -- the list `clean()` is computed from -- and never names a
/// finding itself, so a term added to that list and not rendered fails HERE. It used to claim exactly
/// that while its body was a hand-listed set of five names, and the claim was false: `lifeless_keys` was
/// added to `clean()` and nothing failed anywhere, which is how it reached the SQL row's absence too.
/// `formatFsckSummary` exists to be testable at all: the line used to be built inline in
/// `CommandFsck::executeImpl`, where nothing could reach it.
///
/// A per-finding DISTINCT value is what makes this more than a substring sweep: it catches a formatter
/// that prints the right names against the wrong counters.
TEST(CASFsckSummary, EveryHardFindingAppearsOnTheSummaryLine)
{
    FsckReport rep;
    uint64_t value = 11;
    for (const FsckHardFinding & finding : kFsckHardFindings)
    {
        rep.*finding.value = value;
        value += 11;
    }

    const String line = formatFsckSummary(rep);

    value = 11;
    for (const FsckHardFinding & finding : kFsckHardFindings)
    {
        const String token = String(finding.name) + "=" + std::to_string(value);
        EXPECT_NE(line.find(token), String::npos)
            << "hard finding '" << finding.name << "' is missing from the summary line (expected `"
            << token << "`); the line was: " << line;
        value += 11;
    }

    /// A report carrying these values is NOT clean; the line must not be mistakable for a clean one.
    EXPECT_FALSE(rep.clean());
}

/// A zero must be PRINTED, not omitted. The harness's `stale_edge_verdict` fails closed on an absent key
/// precisely because "field missing" and "field zero" are different facts, and a formatter that skips
/// zeros would turn every clean pool into an unparseable one.
TEST(CASFsckSummary, ZeroValuedHardFindingsAreStillPrinted)
{
    const String line = formatFsckSummary(FsckReport{});
    /// Iterated for the same reason the test above is: a new hard finding printed only when nonzero is a
    /// finding the harness's fail-closed-on-absence consumers would read as missing.
    for (const FsckHardFinding & finding : kFsckHardFindings)
        EXPECT_NE(line.find(String(finding.name) + "=0"), String::npos)
            << "hard finding '" << finding.name << "' prints no zero; the line was: " << line;
    EXPECT_EQ(line.find("partial="), String::npos) << "a non-partial report must not claim partial: " << line;
}

/// A partial scan is a lower bound over the visited subset, so the flag and its reason must travel WITH
/// the counts -- a consumer that sees the numbers but not `partial=1` reads a truncated walk as the pool
/// truth.
TEST(CASFsckSummary, PartialFlagAndReasonTravelWithTheCounts)
{
    FsckReport rep;
    rep.partial = true;
    rep.partial_reason = "deadline exceeded after 180s";
    const String line = formatFsckSummary(rep);
    EXPECT_NE(line.find("partial=1"), String::npos) << line;
    EXPECT_NE(line.find("reason='deadline exceeded after 180s'"), String::npos) << line;
}
