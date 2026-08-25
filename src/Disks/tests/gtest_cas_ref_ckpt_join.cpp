#include <gtest/gtest.h>

#include "config.h"

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCatalogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCkptFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCatalog.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCkpt.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefProtocol.h>
#include <Disks/tests/cas_test_helpers.h>

#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <type_traits>
#include <vector>

/// The `_ckpt` JOIN law and its `O(1)` SIZE invariant.
///
/// `mergeCkpt` already has a suite (`CasRefCkpt` in `gtest_cas_ref_ckpt.cpp`) covering it as one step of
/// the publish algorithm. This suite's subject is narrower and different: the JOIN LAW itself, per
/// field, stated so that a later change to any one field's rule fails here rather than being absorbed
/// into a publish-path assertion; plus the size invariant, which no existing test constrains at all.
///
/// The size half is a REGRESSION FENCE, not a fix -- nothing about today's `_ckpt` is non-`O(1)`. It
/// exists to fail the day someone adds a map, a collection, or any per-ref/per-file term to an object
/// that has no repair path and gates destructive cleanup.
///
/// Constraint 15 names four dimensions (refs, files, transactions, writer epochs) and they do NOT
/// behave the same way, so they get two different assertions rather than one claim covering both:
///
///   - REFS and FILES never enter the body in any form, so the encoded size is BYTE-EQUAL between a
///     namespace holding one and a namespace holding ten thousand. That is `EncodedCkptSizeIs...`
///     below, and it drives the REAL append lane on purpose: a hand-built pair of `RefCkpt` structs
///     would leave a newly-added collection field EMPTY in both and the equality would still hold,
///     so the fence would not fire on the very change it exists to catch. Only a real producer
///     populates a real field.
///   - TRANSACTIONS and WRITER EPOCHS enter as the DECIMAL WIDTH of the two id pairs. That is not
///     equality: `{cse=1,css=1}` and `{cse=1,css=10000}` differ by four bytes. It is `O(1)` because
///     the fields are `uint64_t` and so the width is ceilinged at twenty digits, which is a bound a
///     test asserts on a constructed worst case -- `EncodedCkptSizeHasAConstantCeiling...` below.

namespace DB::ErrorCodes
{
extern const int CORRUPTED_DATA;
extern const int NETWORK_ERROR;
}

using namespace DB::Cas;
using DB::Cas::tests::CountingBackend;
using DB::Cas::tests::namespaceBirthOp;
using DB::Cas::tests::publishCommittedOps;

/// Constraint 15's COMPILE-TIME half: `_ckpt` is a fixed-size product of scalar monotone facts. Any
/// field that owns heap storage -- a map, a vector, a `String` -- makes `RefCkpt` non-trivially-copyable
/// and fails the build here, which is the earliest and cheapest place the constraint can be enforced.
/// The two runtime size tests below are the rest of the fence: this one cannot see a fixed-capacity
/// array, and they cannot see a field that is never populated by the producers they drive.
static_assert(std::is_trivially_copyable_v<RefCkpt>,
              "Constraint 15: _ckpt is a fixed-size product of scalar monotone facts, so its encoded size is "
              "O(1) in refs, files, transactions and writer epochs. A field with heap storage (a map, a "
              "vector, a String) breaks that and belongs in a separate immutable object or ledger.");

namespace
{

constexpr uint64_t U64_MAX = std::numeric_limits<uint64_t>::max();

/// Constraint 15's bound, as a number: the encoded size of the WIDEST `_ckpt` this build can produce
/// (all three fields present, every integer component at `UINT64_MAX`). Pinned as a literal so that
/// adding a field, or widening one, fails a test rather than quietly moving the bound. Generation 10
/// added one byte to the shared format-version header (`9` became `10`); the scalar body is unchanged.
constexpr size_t CKPT_WORST_CASE_ENCODED_BYTES = 235;

/// The high-cardinality side of the size fence, in ONE transaction. Bounded above by the append lane's
/// 5000-operation cap on a normal-class item (`publishCommittedOps` emits two ops per ref), and kept at
/// one transaction on purpose: spreading a larger namespace over several of them costs tens of seconds
/// in a debug build, and a size fence that cannot finish inside the harness budget fences nothing. Any
/// per-ref term in `_ckpt` is as visible at this count as at any larger one.
constexpr size_t MANY_REFS = 2000;

/// Fixed-width, so the refs themselves cannot be what differs between the two namespaces: the claim
/// under test is that ref cardinality does not reach `_ckpt`, and a name that grew with `i` would
/// confound a size comparison if it ever did.
String refName(size_t i)
{
    return fmt::format("r{:08}", i);
}

/// A fence that never refuses, and a deadline far enough out that only the test's own contention
/// decides the outcome -- each `_ckpt`/catalog test file defines its own copy, matching the precedent
/// `gtest_cas_ns_creation_lifecycle.cpp` states explicitly.
const std::function<void(uint64_t)> ALWAYS_ADMITTED = [](uint64_t) {};

CkptDeadline generousDeadline()
{
    return CkptDeadline{[] { return uint64_t{1000}; }, 60000};
}

/// Admits the FIRST call (spent by `completeCreation`'s step-2 `publishCkpt`) and refuses every call
/// after (step 3's own `mutate`): "fenced out between the `_ckpt` create and the `Creating -> Live`
/// CAS", deterministically and without a second thread. That is the durable shape a stalled creator
/// leaves behind, and the starting state the resumption test needs.
std::function<void(uint64_t)> admittedOnceThenFenced()
{
    auto calls = std::make_shared<int>(0);
    return [calls](uint64_t admitted)
    {
        if (++*calls > 1)
            throw DB::Exception(DB::ErrorCodes::NETWORK_ERROR, "fence generation moved since admission ({})", admitted);
    };
}

CreatorFence creatorFence(const String & srid, uint64_t writer_epoch, uint64_t fence_generation = 1)
{
    return CreatorFence{.server_root_id = srid, .writer_epoch = writer_epoch, .fence_generation = fence_generation};
}

/// A `is_creator_fence_terminal` stub answering one fixed verdict: terminality itself is not this
/// suite's subject (its tests live next to the real predicate in `gtest_cas_mount.cpp`).
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

/// `life`'s durable `life_epoch`, failing the current test rather than dereferencing a disengaged
/// optional -- a bare `->` on one aborts the whole binary and takes every later suite's result with it.
uint64_t lifeEpochOrFail(Backend & backend, const Layout & layout, const NamespaceLifeId & life)
{
    const std::optional<CkptSample> sample = readCkpt(backend, layout, life);
    if (!sample || !sample->ckpt.life_epoch)
    {
        ADD_FAILURE() << "expected a _ckpt carrying a life_epoch for namespace '" << life.ns.string() << "'";
        return 0;
    }
    return *sample->ckpt.life_epoch;
}

/// `boot_ms_fn` defaults to the real clock. A caller whose test body does enough CPU-bound work
/// against ONE open pool to risk outrunning `mount_lease_ttl_ms` on a slow sanitizer build should
/// pass a frozen one instead of widening the TTL: the mount fence and the ref-log request controller
/// both read time through this same seam (see `CasRefLedger`'s `controller_boot_ms_fn`), so freezing
/// it removes the wall-clock race rather than merely giving it more room.
PoolPtr openPool(const BackendPtr & backend, std::function<uint64_t()> boot_ms_fn = {})
{
    DB::Cas::tests::seedPoolMetaForRestart(*backend);
    PoolConfig config{.pool_prefix = "p", .server_root_id = "test"};
    config.boot_ms_fn = std::move(boot_ms_fn);
    return Pool::open(backend, std::move(config));
}

/// The incarnation the production birth wiring minted for `ns`, learned back from the catalog the way a
/// real reader does. Fails the current test rather than dereferencing a disengaged optional, so one
/// regression cannot abort the binary and take every later suite's result with it.
NamespaceLifeId liveLifeOrFail(Backend & backend, const Layout & layout, const RootNamespace & ns)
{
    const CasRefCatalog::Snapshot snap = CasRefCatalog::read(backend, layout);
    for (const CatalogEntry & entry : snap.catalog.entries)
        if (entry.ns.string() == ns.string())
            return NamespaceLifeId::fromCatalogEntry(entry.ns, entry.incarnation);
    ADD_FAILURE() << "expected a catalog entry for namespace '" << ns.string() << "', found none";
    return DB::Cas::tests::fixture::fixtureLife(ns);
}

/// Births `ns` and publishes `ref_count` committed refs through the REAL append lane, in ONE
/// transaction, and returns that namespace's durable `_ckpt` as encoded bytes.
///
/// One transaction also holds every OTHER dimension fixed while `ref_count` varies: two namespaces
/// built this way end at the same transaction id, so a difference in their `_ckpt` bodies can only be
/// the refs. `ref_count` must therefore stay within the append lane's per-item operation cap.
String encodedCkptOfNamespaceWithRefs(const PoolPtr & store, Backend & backend, const Layout & layout,
                                      const RootNamespace & ns, size_t ref_count)
{
    store->appendRefOps(ns, MutationScope::wholeShard(),
        [ref_count](const RefTableState & state)
        {
            std::vector<RefOp> ops;
            if (state.getLifecycle() != RefLifecycle::Live)
                ops.push_back(namespaceBirthOp());
            for (size_t i = 0; i < ref_count; ++i)
                for (const RefOp & op : publishCommittedOps(refName(i), ManifestRef{1, i + 1, 1}))
                    ops.push_back(op);
            return ops;
        },
        RootMutationOrigin::Writer, RootMutationKind::Publish);

    const NamespaceLifeId life = liveLifeOrFail(backend, layout, ns);
    const std::optional<CkptSample> sample = readCkpt(backend, layout, life);
    if (!sample)
    {
        ADD_FAILURE() << "expected a _ckpt for namespace '" << ns.string() << "' after its birth transaction";
        return {};
    }
    return encodeRefCkpt(sample->ckpt);
}

}

/// ---------------------------------------------------------------------------------------------
/// The join law, per field
/// ---------------------------------------------------------------------------------------------

/// An absence is "this writer knew nothing", never "this writer says none". Exactly one writer ever
/// knows a namespace's genesis epoch, so every other contribution is `nullopt` and must leave what is
/// on record alone -- in BOTH argument orders, because the two `_ckpt` writers have no ordering
/// between them and the merge is what makes that safe.
TEST(CASRefCheckpointJoin, JoinUnknownLifeEpochWithPresentYieldsPresent)
{
    const RefCkpt unknown{.life_epoch = std::nullopt, .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt};
    const RefCkpt present{.life_epoch = 7, .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt};

    EXPECT_EQ(mergeCkpt(unknown, present).life_epoch, std::optional<uint64_t>{7});
    EXPECT_EQ(mergeCkpt(present, unknown).life_epoch, std::optional<uint64_t>{7})
        << "the merge is commutative -- a writer that knows nothing must not be able to erase the "
           "genesis epoch, whichever side it is on";

    /// The other half of "absent loses": two absences stay absent. `life_epoch` has no floor to fall
    /// back to, and a fabricated one is permanent -- the semantic-max merge can never lower it again.
    EXPECT_EQ(mergeCkpt(unknown, unknown).life_epoch, std::nullopt);
}

/// The ordinary steady state: both writers agree. Asserted for its own sake because it is what
/// `publishCkpt`'s may-not-decrease rule must keep admitting -- an equal republish is not a decrease --
/// and it is also what `publishCkpt` turns into "no write at all".
TEST(CASRefCheckpointJoin, JoinEqualLifeEpochsYieldsSame)
{
    const RefCkpt a{.life_epoch = 9, .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt};
    const RefCkpt b{.life_epoch = 9, .checkpoint_snapshot_id = RefTxnId{9, 4}, .last_epoch_seal = std::nullopt};

    EXPECT_EQ(mergeCkpt(a, b).life_epoch, std::optional<uint64_t>{9});
    EXPECT_EQ(mergeCkpt(b, a).life_epoch, std::optional<uint64_t>{9});
    const std::optional<RefTxnId> b_checkpoint = RefTxnId{9, 4};
    EXPECT_EQ(mergeCkpt(a, b).checkpoint_snapshot_id, b_checkpoint)
        << "an equal life_epoch must not disturb the other fields' own join";
}

TEST(CASRefCheckpointJoin, CrossEpochFrontierRequiresAnImmediatelyAdjacentSeal)
{
    const RefCkpt older{.life_epoch = std::nullopt, .committed_through = RefTxnId{7, 9},
                         .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt};
    const RefCkpt transitioned{.life_epoch = std::nullopt, .committed_through = RefTxnId{8, 1},
                                .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = RefTxnId{8, 1}};
    EXPECT_EQ(mergeCkpt(older, transitioned).committed_through, transitioned.committed_through);
    EXPECT_EQ(mergeCkpt(transitioned, older).committed_through, transitioned.committed_through);

    /// Every committed epoch is materialized. A later frontier may advance only to the immediately
    /// following numeric writer epoch, otherwise a missing epoch would be mistaken for a proved
    /// boundary. The log grammar rejects this same skip at the record boundary; `_ckpt` must not
    /// reintroduce it through its semantic merge.
    const RefCkpt skipped_epoch{.life_epoch = std::nullopt, .committed_through = RefTxnId{10, 1},
                                    .checkpoint_snapshot_id = std::nullopt,
                                    .last_epoch_seal = RefTxnId{7, 9}};
    EXPECT_THROW(mergeCkpt(older, skipped_epoch), DB::Exception);

    const RefCkpt advanced{.life_epoch = std::nullopt, .committed_through = RefTxnId{8, 5},
                            .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = RefTxnId{7, 9}};
    EXPECT_EQ(mergeCkpt(advanced, older).committed_through, advanced.committed_through);

    const RefCkpt unsealed{.life_epoch = std::nullopt, .committed_through = RefTxnId{8, 1},
                           .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt};
    EXPECT_THROW(mergeCkpt(older, unsealed), DB::Exception);
    const RefCkpt stale_prior_seal{.life_epoch = std::nullopt, .committed_through = RefTxnId{10, 1},
                                   .checkpoint_snapshot_id = std::nullopt,
                                   .last_epoch_seal = RefTxnId{7, 8}};
    EXPECT_THROW(mergeCkpt(older, stale_prior_seal), DB::Exception)
        << "a seal below the lower durable frontier does not connect the two histories";
    const RefCkpt seal_above_frontier{.life_epoch = std::nullopt, .committed_through = RefTxnId{8, 5},
                                      .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = RefTxnId{8, 6}};
    EXPECT_THROW(mergeCkpt(older, seal_above_frontier), DB::Exception);
}

/// THE FIRST OF THE TWO SEQUENCES THAT RAISE `life_epoch` HONESTLY, end to end through the production
/// primitives rather than at the merge: a creator publishes `_ckpt` at E1 (`completeCreation` step 2)
/// and dies before its `Creating -> Live` CAS (step 3), and a later actor reconciles the stalled entry
/// and resumes over the SAME incarnation, contributing E2. Two different present values in one
/// incarnation, and NOT a conflict -- the stored value must simply become E2, which is also what
/// `CASNsCreationLifecycle.ReconcileSucceedsTokenExactlyAfterTheOriginalCreatorFenceIsTerminalThenResumesToLive`
/// already pins from the catalog side ("the RESUMING actor's writer_epoch is the genesis epoch that
/// actually landed"). Had the directive's literal rule landed, this sequence would raise
/// `CORRUPTED_DATA` and, since `_ckpt` has no repair path, wedge the namespace forever.
///
/// Both fences share ONE `server_root_id` on purpose: every live namespace is rooted at its own pool
/// member's `server_root_id`, so a creator and its reconciler are always actors of the same server root
/// and draw from the same durable-monotone epoch counter. That is the whole basis for "contributions
/// only ever rise", so the fixture must not quietly model two roots.
TEST(CASRefCheckpointJoin, ResumedCreationRaisesLifeEpochWithoutRefusal)
{
    InMemoryBackend backend;
    Layout layout("p");
    DB::Cas::tests::seedPoolMetaForRestart(backend);
    const RootNamespace ns{"a"};

    ASSERT_EQ(CasRefCatalog::createNamespace(backend, layout, 1, ns, creatorFence("srv1", 5),
                                             /*admitted_generation=*/1, admittedOnceThenFenced(), generousDeadline()),
              CasRefCatalog::NamespaceCreationOutcome::FencedOut);

    /// Bound to a name, never chained through a temporary: a `const CatalogEntry *` taken from an
    /// unbound `Snapshot` dangles the instant the full expression ends.
    const CasRefCatalog::Snapshot stalled = CasRefCatalog::read(backend, layout);
    const CatalogEntry * entry = findEntryForTest(stalled.catalog, ns);
    ASSERT_NE(entry, nullptr);
    ASSERT_EQ(entry->state, NsState::Creating);
    const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(entry->ns, entry->incarnation);
    EXPECT_EQ(lifeEpochOrFail(backend, layout, life), 5u) << "step 2 landed before the creator stalled";

    const CreatorFence resumer = creatorFence("srv1", 9);
    ASSERT_EQ(CasRefCatalog::reconcileStaleCreator(backend, layout, *entry, resumer, fixedTerminality(true),
                                                   /*admitted_generation=*/1, ALWAYS_ADMITTED),
              CasRefCatalog::ReconcileCreatorOutcome::Reconciled);

    CatalogEntry resumed = *entry;
    resumed.creator = resumer;
    EXPECT_EQ(CasRefCatalog::completeCreation(backend, layout, resumed, /*admitted_generation=*/1,
                                              ALWAYS_ADMITTED, generousDeadline()),
              CasRefCatalog::NamespaceCreationOutcome::Live)
        << "the resumption must not be refused by the join";
    EXPECT_EQ(lifeEpochOrFail(backend, layout, life), 9u)
        << "the genesis epoch that actually landed is the resuming actor's, and the join must let it rise";
}

/// THE SECOND SEQUENCE, at the seam where the two `life_epoch`-knowing writers actually meet -- both of
/// them reach this object only through `publishCkpt`, so driving that twice over one key IS the
/// production interleaving, not a stand-in for it. `completeCreation` contributes the catalog creator's
/// epoch; the mount's writer epoch then advances (a restart, a remount); the first precommit's birth
/// chunk contributes the `NamespaceBirth` record's epoch. CREATE TABLE, restart, INSERT.
TEST(CASRefCheckpointJoin, RestartBetweenCreationAndFirstWriteRaisesLifeEpochWithoutRefusal)
{
    InMemoryBackend backend;
    Layout layout("p");
    const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(RootNamespace{"a"}, UInt128(42));

    const RefCkpt from_creation{.life_epoch = 4, .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt};
    ASSERT_EQ(publishCkpt(backend, layout, life, from_creation, 1, ALWAYS_ADMITTED, generousDeadline()),
              CkptPublishOutcome::Published);

    const RefCkpt from_birth_chunk{.life_epoch = 7, .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt};
    EXPECT_EQ(publishCkpt(backend, layout, life, from_birth_chunk, 1, ALWAYS_ADMITTED, generousDeadline()),
              CkptPublishOutcome::Published)
        << "the birth chunk's later epoch must be publishable, not refused as a conflict";
    EXPECT_EQ(lifeEpochOrFail(backend, layout, life), 7u);
}

/// THE REFUSAL, and the state it constructs IS UNREACHABLE ON ANY HONEST PATH -- that is the point of
/// the test, not a caveat on it. `writer_epoch` is durable-monotone per server root
/// (`allocateWriterEpoch` CAS-bumps `<prefix>/gc/server-roots/<srid>/epoch`) and a namespace belongs to
/// exactly one server root, so no live writer can contribute an epoch below one already durable. The
/// only way to reach this is for the fence discipline itself to have failed and a SUPERSEDED writer's
/// contribution to have landed anyway.
///
/// So this test does not model an operating condition; it asserts what happens if the guarantee above
/// is ever violated -- `publishCkpt` refuses and names both values, rather than absorbing the violation
/// into a maximum and leaving no trace. The state is built by publishing the two contributions in the
/// order the fence discipline is supposed to prevent, which needs no seam that manufactures impossible
/// states: `publishCkpt` is a public entry point and the order of two calls is the test's to choose.
///
/// It is driven through `publishCkpt` for a second reason, not just convenience: that IS where the rule
/// lives and the only place it CAN live. `mergeCkpt` is commutative -- the stated reason the two writers
/// need no ordering between them -- so it cannot tell a decrease from an increase, having no idea which
/// of its arguments is durable. There is deliberately no merge-level counterpart to this test.
TEST(CASRefCheckpointJoin, JoinDecreasingLifeEpochIsCorruptionAndPublishesNothing)
{
    CountingBackend backend;
    Layout layout("p");
    const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(RootNamespace{"a"}, UInt128(42));
    const String key = layout.refCkptKey(life);

    const RefCkpt durable{.life_epoch = 9, .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt};
    ASSERT_EQ(publishCkpt(backend, layout, life, durable, 1, ALWAYS_ADMITTED, generousDeadline()),
              CkptPublishOutcome::Published);
    const uint64_t cas_puts_before = backend.casPutCount(key);

    const RefCkpt superseded{.life_epoch = 3, .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt};
    String message;
    try
    {
        publishCkpt(backend, layout, life, superseded, 1, ALWAYS_ADMITTED, generousDeadline());
        ADD_FAILURE() << "a contribution below the durable life_epoch must not be published";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::CORRUPTED_DATA);
        message = e.message();
    }

    /// BOTH values, not just the offending one: an operator reading this has to be able to tell which
    /// writer is the superseded one without going to the object. Matched as RENDERED substrings rather
    /// than as bare digits -- a lone "9" would also be satisfied by a key or a byte count that happened
    /// to contain it, so a bare-digit match would keep passing after the message stopped saying this.
    EXPECT_NE(message.find("9 is durable"), String::npos) << "the durable value must be named: " << message;
    EXPECT_NE(message.find("contributed 3"), String::npos) << "the contributed value must be named: " << message;
    EXPECT_NE(message.find(key), String::npos) << "the key must be named: " << message;
    /// And that the object cannot be repaired in place, which is the part an operator cannot derive
    /// from the two numbers.
    EXPECT_NE(message.find("NO in-place repair"), String::npos)
        << "the message must say the object has no in-place repair: " << message;

    /// And nothing was written. The refusal is decided before the body is built, so the durable object
    /// is untouched and no write was even attempted.
    EXPECT_EQ(backend.casPutCount(key), cas_puts_before) << "the publisher must not CAS on a refused publish";
    EXPECT_EQ(lifeEpochOrFail(backend, layout, life), 9u) << "the durable value is unchanged";
}

/// The other half of the refusal, and the reason it consults the fence before classifying: the SAME
/// decrease from a writer the fence is about to refuse is not corruption. That writer landed nothing
/// anywhere, so what it gets is the transient control signal every other refusal in `publishCkpt`
/// returns rather than throws. Reporting corruption for it would turn "your incarnation moved, retry"
/// into a permanent verdict on the namespace, which is the opposite of what the detector means: the
/// violation is a STILL-ADMITTED writer contributing a superseded epoch.
TEST(CASRefCheckpointJoin, ADecreasingLifeEpochFromAFencedOutWriterIsReportedFencedOutNotCorruption)
{
    CountingBackend backend;
    Layout layout("p");
    const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(RootNamespace{"a"}, UInt128(42));
    const String key = layout.refCkptKey(life);

    const RefCkpt durable{.life_epoch = 9, .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt};
    ASSERT_EQ(publishCkpt(backend, layout, life, durable, 1, ALWAYS_ADMITTED, generousDeadline()),
              CkptPublishOutcome::Published);
    const uint64_t cas_puts_before = backend.casPutCount(key);

    const std::function<void(uint64_t)> always_fenced = [](uint64_t admitted)
    {
        throw DB::Exception(DB::ErrorCodes::NETWORK_ERROR, "fence generation moved since admission ({})", admitted);
    };
    const RefCkpt superseded{.life_epoch = 3, .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt};
    EXPECT_EQ(publishCkpt(backend, layout, life, superseded, 1, always_fenced, generousDeadline()),
              CkptPublishOutcome::FencedOut);

    EXPECT_EQ(backend.casPutCount(key), cas_puts_before);
    EXPECT_EQ(lifeEpochOrFail(backend, layout, life), 9u);
}

/// `checkpoint_snapshot_id` and `last_epoch_seal` continue to merge by SEMANTIC MAXIMUM. Unlike
/// `life_epoch` these two genuinely advance over a namespace's life, and the max is what stops a writer
/// that sampled an older body from regressing the other writer's progress (TLC counterexample
/// `_sab_sealclobbersbase`, which costs an acked transaction). Both directions and present-beats-absent,
/// since the two writers have no ordering between them.
TEST(CASRefCheckpointJoin, CheckpointAndSealStillMergeBySemanticMaximum)
{
    const RefCkpt lower{.life_epoch = std::nullopt, .checkpoint_snapshot_id = RefTxnId{3, 5}, .last_epoch_seal = RefTxnId{3, 4}};
    const RefCkpt higher{.life_epoch = std::nullopt, .checkpoint_snapshot_id = RefTxnId{4, 1}, .last_epoch_seal = RefTxnId{4, 2}};

    const std::optional<RefTxnId> higher_checkpoint = higher.checkpoint_snapshot_id;
    const std::optional<RefTxnId> higher_seal = higher.last_epoch_seal;
    const std::optional<RefTxnId> lower_checkpoint = lower.checkpoint_snapshot_id;
    const std::optional<RefTxnId> lower_seal = lower.last_epoch_seal;

    /// Ordered by writer_epoch FIRST: `{4,1}` beats `{3,5}` even though its sequence is smaller, which
    /// is the intended timeline across an epoch restart that resets the sequence.
    EXPECT_EQ(mergeCkpt(lower, higher).checkpoint_snapshot_id, higher_checkpoint);
    EXPECT_EQ(mergeCkpt(higher, lower).checkpoint_snapshot_id, higher_checkpoint);
    EXPECT_EQ(mergeCkpt(lower, higher).last_epoch_seal, higher_seal);
    EXPECT_EQ(mergeCkpt(higher, lower).last_epoch_seal, higher_seal);

    /// Present beats absent, both directions and both fields.
    const RefCkpt nothing;
    EXPECT_EQ(mergeCkpt(nothing, lower).checkpoint_snapshot_id, lower_checkpoint);
    EXPECT_EQ(mergeCkpt(lower, nothing).checkpoint_snapshot_id, lower_checkpoint);
    EXPECT_EQ(mergeCkpt(nothing, lower).last_epoch_seal, lower_seal);
    EXPECT_EQ(mergeCkpt(lower, nothing).last_epoch_seal, lower_seal);
    EXPECT_EQ(mergeCkpt(nothing, nothing).checkpoint_snapshot_id, std::nullopt);
    EXPECT_EQ(mergeCkpt(nothing, nothing).last_epoch_seal, std::nullopt);
}

/// ---------------------------------------------------------------------------------------------
/// Constraint 15: the `O(1)` size invariant
/// ---------------------------------------------------------------------------------------------

/// REFS and FILES: byte-equal, because they never enter the body. Driven through the REAL append lane
/// (see `encodedCkptOfNamespaceWithRefs` on why a hand-built struct pair would not fence anything).
TEST(CASRefCheckpointJoin, EncodedCkptSizeIsIndependentOfCardinality)
{
    auto backend = std::make_shared<InMemoryBackend>();
    /// `MANY_REFS` committed through ONE `appendRefOps` call is CPU-bound encoding, not I/O -- on a
    /// slow sanitizer build (msan in particular) it can outrun the real-clock `mount_lease_ttl_ms`
    /// this pool was opened under and trip the mount fence mid-publish. Freeze the pool's clock
    /// instead of racing it (see `openPool`'s doc comment).
    auto store = openPool(backend, [] { return uint64_t{0}; });
    Layout layout("p");

    const String one = encodedCkptOfNamespaceWithRefs(store, *backend, layout, RootNamespace{"srv1/one"}, 1);
    const String many = encodedCkptOfNamespaceWithRefs(store, *backend, layout, RootNamespace{"srv1/many"}, MANY_REFS);

    ASSERT_FALSE(one.empty());
    ASSERT_FALSE(many.empty());
    EXPECT_EQ(one, many)
        << "not merely equal in SIZE: refs and files reach `_ckpt` in no form at all, so the two bodies "
           "are byte-identical.";
    /// The same claim stated so that it does not depend on the chosen cardinality at all: no ref
    /// PUBLISHED into the namespace appears anywhere in its `_ckpt`. A count-based comparison can only
    /// catch a term that grows; this catches one that is merely there.
    EXPECT_EQ(many.find(refName(0)), String::npos)
        << "a published ref's NAME appears in `_ckpt`: " << many;
    EXPECT_EQ(many.find(refName(MANY_REFS - 1)), String::npos)
        << "a published ref's NAME appears in `_ckpt`: " << many;
    EXPECT_EQ(one.size(), many.size())
        << "Constraint 15: `_ckpt`'s encoded size must not grow with the number of refs or files in the "
           "namespace. A collection or per-ref term was added to an object that has NO repair path and "
           "gates destructive cleanup; it belongs in a separate immutable object or ledger instead.\n"
           "  1 ref:            " << one
        << "  " << MANY_REFS << " refs: " << many;
}

/// TRANSACTIONS and WRITER EPOCHS: not equality -- they enter as the decimal width of the id pairs --
/// but ceilinged, because the fields are `uint64_t`. The worst case is constructible exactly (every
/// field present at `UINT64_MAX`), so the bound is asserted on it rather than believed about it.
TEST(CASRefCheckpointJoin, EncodedCkptSizeHasAConstantCeilingAcrossTransactionsAndEpochs)
{
    /// The true worst case over every namespace history: all three fields present, every component at
    /// the widest value its type can hold. No real `_ckpt` can encode larger, because there is no field
    /// that is not one of these five integers.
    const RefCkpt worst{.life_epoch = U64_MAX,
                        .committed_through = RefTxnId{U64_MAX, U64_MAX},
                        .checkpoint_snapshot_id = RefTxnId{U64_MAX, U64_MAX},
                        .last_epoch_seal = RefTxnId{U64_MAX, U64_MAX}};
    const size_t worst_bytes = encodeRefCkpt(worst).size();

    /// Pinned as a literal, not merely compared against itself: this is the number Constraint 15's
    /// `O(1)` claim reduces to, and a change to it means a field was added, removed or rewidened.
    EXPECT_EQ(worst_bytes, CKPT_WORST_CASE_ENCODED_BYTES)
        << "the widest `_ckpt` this build can encode changed size -- a field was added, removed, or "
           "given a wider type. Constraint 15's O(1) bound is exactly this constant.";

    /// The growth term is the decimal width, and it is bounded by that ceiling rather than proportional
    /// to the number of transactions: four orders of magnitude of `ref_sequence` cost four bytes.
    const RefCkpt at_sequence_1{.life_epoch = 1, .committed_through = RefTxnId{1, 1}, .checkpoint_snapshot_id = RefTxnId{1, 1}, .last_epoch_seal = RefTxnId{1, 1}};
    const RefCkpt at_sequence_10k{.life_epoch = 1, .committed_through = RefTxnId{1, 10000}, .checkpoint_snapshot_id = RefTxnId{1, 10000}, .last_epoch_seal = RefTxnId{1, 10000}};
    EXPECT_EQ(encodeRefCkpt(at_sequence_10k).size(), encodeRefCkpt(at_sequence_1).size() + 12);
    EXPECT_LE(encodeRefCkpt(at_sequence_10k).size(), worst_bytes);
    EXPECT_LE(encodeRefCkpt(at_sequence_1).size(), worst_bytes);

    /// And the ceiling is far below the format registry's own object cap, so the cap is what it is
    /// documented to be -- a corruption brake this object cannot approach -- and never the thing that
    /// makes the size bounded.
    EXPECT_LT(worst_bytes, traitsFor(FormatId::RefCkpt).object_cap);
}
