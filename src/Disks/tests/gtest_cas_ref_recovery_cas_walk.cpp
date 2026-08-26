#include <gtest/gtest.h>

#include "config.h"

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCkptFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefLogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCkpt.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefProtocol.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/ProfileEvents.h>

#include <Poco/Exception.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

/// Stage A task 6: recovery is `_ckpt` + an ARITHMETIC tail + a seal CAS-walk, and it installs nothing
/// without presenting the fence generation it was admitted under.
///
/// The one sentence this suite exists to defend: **recovery performs no stream `LIST`.** Everything the
/// old recovery knew about a table's durable stream came from one `LIST`, so a listing that silently
/// omitted a key produced a table missing an ACKED transaction and looked perfectly healthy. The
/// checkpoint now supplies the only base and finite frontier; arithmetic exact GETs decide recovery.
/// These list-liar fixtures are retained as sentinels: hiding or fabricating a listed key cannot affect
/// recovery because recovery sends zero stream LIST requests.
///
/// The other half is INV-2: a dead epoch is closed IN-BAND, by a seal transaction the store's own
/// conditional create places at exactly `{E, T+1}` -- the key a dying predecessor's in-flight PUT would
/// have taken. That is why the walk WRITES, and why every write it performs is gated on the ONE fence
/// generation captured when the recovery was admitted (slot-occupy, the `_ckpt` CAS, and the install
/// recheck -- one capture, three checks).
///
/// The suite name is prefixed `Cas` so it is covered by the `Cas*` unit-test gate filter.

namespace DB::ErrorCodes
{
extern const int CORRUPTED_DATA;
extern const int NETWORK_ERROR;
}

namespace ProfileEvents
{
extern const Event CASRefRecoveryRestarts;
extern const Event CASRefRecoveryEpochSealed;
extern const Event CASRefRecoveryEpochSealAdopted;
extern const Event CASRefRecoveryStragglerAdopted;
extern const Event CASRefRecoveryCancelled;
extern const Event CASRefCheckpointPublished;
}

using namespace DB::Cas;
using DB::Cas::tests::committedRow;
using DB::Cas::tests::CountingBackend;
using DB::Cas::tests::expectThrowsCode;
using DB::Cas::tests::minimalLiveSnapshot;
using DB::Cas::tests::namespaceBirthOp;
using DB::Cas::tests::publishCommittedOps;
using DB::Cas::tests::rearmMountFenceAfterAnomalyForTest;
using DB::Cas::tests::writeRefSnapshotRaw;

namespace
{

ManifestRef manifestRef(uint64_t epoch, uint64_t build_sequence, uint32_t ordinal)
{
    return ManifestRef{epoch, build_sequence, ordinal};
}

/// Make the durable mount immediately reclaimable so a test that deliberately moved the local fence
/// generation can drive the production remount boundary without paying a live-lease expiry wait.
void fenceOutMountForRemount(Backend & backend, const String & mount_key)
{
    const auto got = backend.get(mount_key);
    ASSERT_TRUE(got.has_value());
    MountLease mount = decodeMountLease(got->bytes);
    mount.gc_fenced = true;
    mount.seq += 1;
    ASSERT_EQ(backend.putOverwrite(mount_key, encodeMountLease(mount), got->token).outcome,
        PutOutcome::Done);
}

/// A backend whose `LIST` can lie by omission. `hidden_keys` remain readable by exact key, so these
/// fixtures prove the stronger modern rule: recovery sends no stream `LIST` at all and therefore cannot
/// be affected by an enumeration inconsistency.
///
/// Deliberately NOT a "delete the object" fixture: an object that is genuinely gone is a different
/// (and already covered) case. The blocker is an object that EXISTS and is invisible to enumeration.
class HidingListBackend : public CountingBackend
{
public:
    explicit HidingListBackend(bool seed_pool_meta = true)
    {
        if (seed_pool_meta)
            DB::Cas::tests::seedPoolMetaForRestart(*this);
    }

    using CountingBackend::get;
    using CountingBackend::list;
    using CountingBackend::putIfAbsent;
    using CountingBackend::casPut;

    std::set<String> hidden_keys;
    std::set<String> phantom_list_keys;

    /// Every `putIfAbsent` of a key containing this substring throws a PLAIN (non-`DB::Exception`)
    /// error, which `classifyConditionalWriteResult` can only ever classify `Unresolved` -- never
    /// `DefiniteFailure`. Persistent rather than one-shot on purpose: the subject is what recovery does
    /// when the store KEEPS refusing to say whether the write landed.
    String ambiguous_put_substr;

    /// Persistent thrown response for a matching mutable checkpoint CAS. The ref-log PUT has already
    /// completed when tests arm this, producing the exact one-successor recovery window.
    String ambiguous_cas_substr;
    int ambiguous_cas_count = 0;

    /// Runs after a checkpoint publisher read its expected token but before that publisher presents
    /// its CAS. This is the exact window in which another admitted writer can advance the frontier.
    std::function<void(const String &, const String &, const std::optional<Token> &)> before_cas_put;

    ListPage list(const String & prefix, const String & cursor, size_t limit) override
    {
        ListPage page = CountingBackend::list(prefix, cursor, limit);
        std::vector<ListedKey> kept;
        kept.reserve(page.keys.size());
        for (ListedKey & lk : page.keys)
            if (!hidden_keys.contains(lk.key))
                kept.push_back(std::move(lk));
        if (cursor.empty())
        {
            for (const String & key : phantom_list_keys)
            {
                if (key.starts_with(prefix))
                    kept.push_back(ListedKey{.key = key, .size = 0, .token = std::nullopt});
            }
        }
        page.keys = std::move(kept);
        return page;
    }

    PutResult putIfAbsent(const String & key, const String & bytes, const ObjectMeta & meta) override
    {
        if (!ambiguous_put_substr.empty() && key.find(ambiguous_put_substr) != String::npos)
            throw std::runtime_error("injected ambiguous putIfAbsent");
        return CountingBackend::putIfAbsent(key, bytes, meta);
    }

    CasResult casPut(const String & key, const String & bytes, const std::optional<Token> & expected,
                     const ObjectMeta & meta) override
    {
        if (before_cas_put)
            before_cas_put(key, bytes, expected);
        if (ambiguous_cas_count > 0 && !ambiguous_cas_substr.empty()
            && key.find(ambiguous_cas_substr) != String::npos)
        {
            --ambiguous_cas_count;
            throw Poco::TimeoutException("HidingListBackend: simulated ambiguous checkpoint CAS");
        }
        return CountingBackend::casPut(key, bytes, expected, meta);
    }
};

/// Fires `on_key` immediately AFTER a `putIfAbsent` whose key contains `watched_substr` -- the
/// deterministic way to act inside recovery's own write window (bump a fence, land a straggler) with no
/// sleep and no second thread. `skip` lets a test target the Nth such write.
class PutHookBackend : public HidingListBackend
{
public:
    using HidingListBackend::putIfAbsent;

    using HidingListBackend::casPut;

    String watched_substr;
    uint64_t skip = 0;
    std::function<void()> on_key;

    PutResult putIfAbsent(const String & key, const String & bytes, const ObjectMeta & meta) override
    {
        PutResult result = HidingListBackend::putIfAbsent(key, bytes, meta);
        fireIfWatched(key);
        return result;
    }

    /// The `_ckpt` advance is a token-CAS, not a create, whenever the object already exists -- which is
    /// the normal case, since the namespace birth creates it. Hooking only `putIfAbsent` would silently
    /// never fire for it.
    CasResult casPut(const String & key, const String & bytes, const std::optional<Token> & expected,
                     const ObjectMeta & meta) override
    {
        CasResult result = HidingListBackend::casPut(key, bytes, expected, meta);
        fireIfWatched(key);
        return result;
    }

private:
    void fireIfWatched(const String & key)
    {
        if (!on_key || watched_substr.empty() || key.find(watched_substr) == String::npos)
            return;
        if (skip > 0)
        {
            --skip;
            return;
        }
        auto hook = on_key;
        on_key = nullptr;   /// one-shot: a hook that re-enters its own trigger would recurse
        hook();
    }
};

/// Materializes `late_bytes` at `late_key` at the instant the walk READS that key and finds it absent --
/// i.e. strictly between the read and the conditional create that follows it.
///
/// This is the only faithful way to construct the race the `Occupied` arms exist for. Seeding the object
/// up front does NOT work, and finding that out is the point: the walk fetches every id by EXACT KEY, so
/// an object hidden from the listing is simply FOUND by the read and applied there. To meet it as an
/// OCCUPANT of the slot, it has to arrive after the read said absent -- which is exactly what a
/// straggler, or a concurrent recoverer's seal, does.
class LateMaterializeBackend : public HidingListBackend
{
public:
    using HidingListBackend::get;

    String late_key;
    String late_bytes;

    std::optional<GetResult> get(const String & key, Range range) override
    {
        std::optional<GetResult> result = HidingListBackend::get(key, range);
        if (!result && !late_key.empty() && key == late_key)
        {
            CountingBackend::putIfAbsent(late_key, late_bytes);
            late_key.clear();   /// one-shot: the walk must see it present from here on
        }
        return result;
    }
};

/// Fires `on_key` immediately BEFORE a `get` whose key contains `watched_substr`, and can additionally
/// FAULT that read with a transient object-store error -- the I/O seam the remount-barrier test pauses
/// recovery at.
class GetSeamBackend : public HidingListBackend
{
public:
    using HidingListBackend::get;

    String watched_substr;

    /// Assigned from the test thread and read from whatever thread the recovery runs on, so the
    /// read-and-move below is guarded. Today's tests all assign before starting the recovery thread and
    /// clear after joining it, so there is no race to fix -- but this is the same seam that already
    /// produced one use-after-free, and "the current tests happen not to race" is not a property a
    /// future test author can see. The mutex makes the constraint enforced rather than remembered.
    std::mutex hook_mutex;
    std::function<void(const String &)> on_key;

    std::optional<GetResult> get(const String & key, Range range) override
    {
        std::unique_lock hook_lock(hook_mutex);
        if (on_key && !watched_substr.empty() && key.find(watched_substr) != String::npos)
        {
            /// ONE-SHOT by moving the callback OUT before invoking it, and that is a correctness
            /// requirement rather than a convenience. A hook that cleared `on_key` from inside its own
            /// body would destroy the `std::function` whose closure it is still executing, and every
            /// by-reference capture it touched afterwards would read freed heap. That is not
            /// theoretical: it is what the first version of these tests did, and the ASan gate caught
            /// it as a `heap-use-after-free` while a hook was parked on a condition variable.
            auto hook = std::move(on_key);
            on_key = nullptr;
            /// Released before the hook runs: it parks on a condition variable, and holding the seam's
            /// own mutex across that would deadlock the very thread meant to release it.
            hook_lock.unlock();
            hook(key);
        }
        return HidingListBackend::get(key, range);
    }
};

/// Fires once after an exact GET has already fixed its result. This is the recovery authority seam:
/// another actor advances the log+checkpoint after the walk observed its old end, but before the walk
/// performs its final catalog/checkpoint validation.
class AfterGetHookBackend : public HidingListBackend
{
public:
    using HidingListBackend::get;

    String watched_key;
    std::function<void()> after_get;

    std::optional<GetResult> get(const String & key, Range range) override
    {
        std::optional<GetResult> result = HidingListBackend::get(key, range);
        if (after_get && key == watched_key)
        {
            auto hook = std::move(after_get);
            after_get = nullptr;
            hook();
        }
        return result;
    }
};

CasRequestBudget tinyBudget()
{
    return CasRequestBudget{
        .attempt_timeout_ms = 50, .operation_deadline_ms = 500, .max_attempts = 1, .lease_safety_margin_ms = 50};
}

PoolConfig walkTestConfig()
{
    PoolConfig config;
    config.pool_prefix = "p";
    config.server_root_id = "test";
    config.server_id = DB::UInt128(1);
    config.cas_request_budget = tinyBudget();
    config.wait_sleep_fn = [](uint64_t) {};
    /// No background publication: every test here drives its own, so a threshold-triggered snapshot can
    /// never move the base under an assertion about which base recovery chose.
    config.snapshot_log_count_threshold = 1ULL << 40;
    config.snapshot_log_bytes_threshold = 1ULL << 40;
    return config;
}

PoolPtr openWalkPool(const BackendPtr & backend, PoolConfig config = walkTestConfig())
{
    DB::Cas::tests::seedPoolMetaForRestart(*backend, config.pool_prefix);
    return Pool::open(backend, std::move(config));
}

/// Burns durable writer epochs so a subsequent `Pool::open` allocates `target_live_epoch`. Epochs are
/// minted, never reclaimed (`CasPool.cpp`'s allocator), so this is exactly what a pool that has been
/// mounted `n` times looks like -- including the burned epochs in which nothing was ever written, which
/// the seal chain must cross.
void burnEpochsUpTo(Backend & backend, const Layout & layout, uint64_t target_live_epoch)
{
    for (uint64_t e = 1; e < target_live_epoch; ++e)
        allocateWriterEpoch(backend, layout, "test", EpochMintPolicy::NormalMount, 0, [] { return RefCatalog{}; });
}

/// One ordinary transaction at `id`, publishing `ref` (prepending the birth op when `birth`).
RefLogTxn makeOrdinaryTxn(const RootNamespace & ns, RefTxnId id, const String & ref, bool birth,
                          std::optional<RefTxnId> prev_epoch_seal = std::nullopt)
{
    RefLogTxn txn;
    txn.ns = ns.string();
    txn.txn_id = id;
    if (birth)
        txn.ops.push_back(namespaceBirthOp());
    for (const RefOp & op : publishCommittedOps(ref, manifestRef(id.writer_epoch, id.ref_sequence, 1u)))
        txn.ops.push_back(op);
    txn.prev_epoch_seal = prev_epoch_seal;
    return txn;
}

/// The terminal `remove_namespace` op (this project's warning set requires every field named, so it is
/// built field-by-field rather than by designated init).
RefOp removeNamespaceOp()
{
    RefOp op;
    op.kind = RefOpKind::RemoveNamespace;
    return op;
}

/// One EPOCH SEAL transaction at `id` -- what a concurrent recoverer leaves behind.
RefLogTxn makeSealTxn(const RootNamespace & ns, RefTxnId id,
                      std::optional<RefTxnId> prev_epoch_seal = std::nullopt)
{
    RefLogTxn seal;
    seal.ns = ns.string();
    seal.txn_id = id;
    RefOp op;
    op.kind = RefOpKind::EpochSeal;
    seal.ops.push_back(op);
    seal.prev_epoch_seal = prev_epoch_seal;
    return seal;
}

void seedTxn(Backend & backend, const Layout & layout, const RootNamespace & ns, RefTxnId id,
             const String & ref, bool birth)
{
    DB::Cas::tests::fixture::writeRefLogRaw(backend, layout, makeOrdinaryTxn(ns, id, ref, birth));
}

/// Seeds the `_ckpt` a real namespace birth would have created, so recovery can ground its walk at the
/// namespace's `life_epoch` without consulting the (untrusted) listing. Raw, because these fixtures
/// never run a birth through the append lane.
void seedCkpt(Backend & backend, const Layout & layout, const RootNamespace & ns, const RefCkpt & ckpt)
{
    backend.putIfAbsent(layout.refCkptKey(DB::Cas::tests::fixture::fixtureLife(ns)), encodeRefCkpt(ckpt));
}

RefCkpt lifeEpochCkpt(uint64_t life_epoch, std::optional<RefTxnId> committed_through = std::nullopt)
{
    return RefCkpt{.life_epoch = std::optional<uint64_t>{life_epoch},
                   .committed_through = committed_through,
                   .checkpoint_snapshot_id = std::nullopt,
                   .last_epoch_seal = std::nullopt};
}

/// The decoded transaction at `id`, or `nullopt` when the object is absent. Never dereferences a
/// disengaged optional: an aborted binary would take every later suite's result with it.
std::optional<RefLogTxn> readLogTxn(Backend & backend, const Layout & layout, const RootNamespace & ns, RefTxnId id)
{
    const auto got = backend.get(layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), id));
    if (!got)
        return std::nullopt;
    return decodeRefLogTxn(openObject(FormatId::RefLog, got->bytes), ns.string(), id);
}

uint64_t counterOf(ProfileEvents::Event event)
{
    return ProfileEvents::global_counters[event].load();
}

NamespaceLifeId catalogLife(Backend & backend, const Layout & layout, const RootNamespace & ns)
{
    const CasRefCatalog::Snapshot catalog = CasRefCatalog::read(backend, layout);
    for (const CatalogEntry & entry : catalog.catalog.entries)
        if (entry.ns == ns)
            return NamespaceLifeId::fromCatalogEntry(entry.ns, entry.incarnation);
    throw std::runtime_error("test namespace has no catalog life");
}

NamespaceLifeId strandOneUnfrontieredSuccessor(
    HidingListBackend & backend, const PoolPtr & store, const Layout & layout, const RootNamespace & ns)
{
    store->appendRefOps(ns, MutationScope::ref("a"),
        [](const RefTableState & state)
        {
            std::vector<RefOp> ops;
            if (state.getLifecycle() != RefLifecycle::Live)
                ops.push_back(namespaceBirthOp());
            for (const RefOp & op : publishCommittedOps("a", manifestRef(1, 1, 1)))
                ops.push_back(op);
            return ops;
        }, RootMutationOrigin::Writer, RootMutationKind::Publish);

    const NamespaceLifeId life = catalogLife(backend, layout, ns);
    backend.ambiguous_cas_substr = layout.refCkptKey(life);
    backend.ambiguous_cas_count = 200;
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&]
    {
        store->appendRefOps(ns, MutationScope::ref("b"),
            [](const RefTableState &) { return publishCommittedOps("b", manifestRef(1, 2, 1)); },
            RootMutationOrigin::Writer, RootMutationKind::Publish);
    });
    backend.ambiguous_cas_count = 0;
    return life;
}

CatalogEntry replaceCatalogLifeForTest(
    Backend & backend, const Layout & layout, const CatalogEntry & predecessor, UInt128 successor_incarnation)
{
    const CasRefCatalog::Snapshot before_delete = CasRefCatalog::read(backend, layout);
    RefCatalog without_predecessor = before_delete.catalog;
    std::erase_if(without_predecessor.entries, [&](const CatalogEntry & entry)
    {
        return entry.ns == predecessor.ns && entry.incarnation == predecessor.incarnation;
    });
    if (backend.casPut(layout.refCatalogKey(), encodeRefCatalog(without_predecessor), before_delete.token).outcome
        != CasOutcome::Committed)
        throw std::runtime_error("test failed to retire exact predecessor catalog life");

    CatalogEntry successor{
        .ns = predecessor.ns,
        .state = NsState::Live,
        .incarnation = successor_incarnation,
        .creator = std::nullopt};
    const CasRefCatalog::Snapshot after_delete = CasRefCatalog::read(backend, layout);
    RefCatalog reborn = after_delete.catalog;
    reborn.entries.push_back(successor);
    if (backend.casPut(layout.refCatalogKey(), encodeRefCatalog(reborn), after_delete.token).outcome
        != CasOutcome::Committed)
        throw std::runtime_error("test failed to publish successor catalog life");
    return successor;
}

}

/// ---------------------------------------------------------------------------------------------
/// The checkpoint-bounded arithmetic tail: no recovery LIST
/// ---------------------------------------------------------------------------------------------

/// The durable stream is `{1,1} {1,2} {1,3}` while the backend hides the middle key from LIST.
/// Recovery must make zero stream LIST requests and recover the same exact checkpoint range.
TEST(CASRefRecoveryCasWalk, HiddenMiddleLogDoesNotAffectCheckpointRecovery)
{
    auto backend = std::make_shared<HidingListBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/hint_middle"};

    seedCkpt(*backend, layout, ns, lifeEpochCkpt(1, RefTxnId{1, 3}));
    seedTxn(*backend, layout, ns, RefTxnId{1, 1}, "a", /*birth=*/true);
    seedTxn(*backend, layout, ns, RefTxnId{1, 2}, "b", /*birth=*/false);
    seedTxn(*backend, layout, ns, RefTxnId{1, 3}, "c", /*birth=*/false);
    backend->hidden_keys.insert(layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), RefTxnId{1, 2}));

    auto store = openWalkPool(backend);
    ASSERT_TRUE(store);
    backend->resetCounts();

    const auto refs = store->listRefs(ns);
    EXPECT_EQ(backend->listCount(layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns))), 0u);
    EXPECT_EQ(refs.size(), 3u) << "the arithmetic walk must fetch {1,2} by exact key";
    EXPECT_TRUE(refs.contains("a"));
    EXPECT_TRUE(refs.contains("b")) << "'b' is the ref the omitted transaction published";
    EXPECT_TRUE(refs.contains("c"));
}

/// The same sentinel at the tail. A hidden tail key is still found by the bounded exact walk, not by a
/// stream enumeration.
TEST(CASRefRecoveryCasWalk, HiddenTailLogDoesNotAffectCheckpointRecovery)
{
    auto backend = std::make_shared<HidingListBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/hint_tail"};

    seedCkpt(*backend, layout, ns, lifeEpochCkpt(1, RefTxnId{1, 2}));
    seedTxn(*backend, layout, ns, RefTxnId{1, 1}, "a", /*birth=*/true);
    seedTxn(*backend, layout, ns, RefTxnId{1, 2}, "b", /*birth=*/false);
    backend->hidden_keys.insert(layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), RefTxnId{1, 2}));

    auto store = openWalkPool(backend);
    ASSERT_TRUE(store);
    backend->resetCounts();

    const auto refs = store->listRefs(ns);
    EXPECT_EQ(backend->listCount(layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns))), 0u);
    EXPECT_EQ(refs.size(), 2u) << "an omitted TAIL id is indistinguishable from the end of the stream to a "
                                  "listing; recovery never enumerates it and exact-reads the checkpoint range";
    EXPECT_TRUE(refs.contains("b"));
}

/// Hiding the checkpoint base snapshot from LIST cannot matter: the checkpoint names it, recovery
/// exact-reads its matching non-seal log first, then exact-reads the snapshot.
TEST(CASRefRecoveryCasWalk, CkptNamedBaseIsRecoveredWithoutStreamList)
{
    auto backend = std::make_shared<HidingListBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/hint_snap"};

    const RefTxnId base{1, 1};
    seedTxn(*backend, layout, ns, base, "a", /*birth=*/true);
    writeRefSnapshotRaw(*backend, layout,
        minimalLiveSnapshot(ns.string(), base, {committedRow("a", manifestRef(1, 1, 1))}));
    seedTxn(*backend, layout, ns, RefTxnId{1, 2}, "c", /*birth=*/false);
    seedCkpt(*backend, layout, ns, RefCkpt{.life_epoch = std::optional<uint64_t>{1},
                                           .committed_through = base,
                                           .checkpoint_snapshot_id = base,
                                           .last_epoch_seal = std::nullopt});
    backend->hidden_keys.insert(layout.refSnapshotKey(DB::Cas::tests::fixture::fixtureLife(ns), base));

    auto store = openWalkPool(backend);
    ASSERT_TRUE(store);
    backend->resetCounts();

    const auto refs = store->listRefs(ns);
    EXPECT_EQ(backend->listCount(layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns))), 0u);
    EXPECT_EQ(refs.size(), 2u) << "the checkpoint names the base; the listing's omission is irrelevant";
    EXPECT_TRUE(refs.contains("a")) << "'a' exists inside the checkpoint-named snapshot";
    EXPECT_TRUE(refs.contains("c"));
}

TEST(CASRefRecoveryCasWalk, MissingExactIdAtOrBelowCommittedFrontierIsCorruption)
{
    auto backend = std::make_shared<HidingListBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/missing_below_frontier"};
    const RefTxnId frontier{1, 2};

    DB::Cas::tests::fixture::admitLive(*backend, layout, ns);
    const NamespaceLifeId life = catalogLife(*backend, layout, ns);
    seedTxn(*backend, layout, ns, RefTxnId{1, 1}, "a", /*birth=*/true);
    ASSERT_EQ(backend->putIfAbsent(layout.refCkptKey(life), encodeRefCkpt(RefCkpt{
        .life_epoch = std::optional<uint64_t>{1},
        .committed_through = frontier,
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt})).outcome, PutOutcome::Done);
    const auto ckpt_before = readCkpt(*backend, layout, life);
    ASSERT_TRUE(ckpt_before);

    auto store = openWalkPool(backend);
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { (void)store->listRefs(ns); });

    const auto ckpt_after = readCkpt(*backend, layout, life);
    ASSERT_TRUE(ckpt_after);
    EXPECT_EQ(ckpt_after->token, ckpt_before->token)
        << "an unchanged checkpoint makes the missing committed id corruption, not a shorter stream";
}

TEST(CASRefRecoveryCasWalk, UncommittedSnapshotIsUnobservedWithoutStreamList)
{
    auto backend = std::make_shared<HidingListBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/hint_above_frontier"};
    const RefTxnId frontier{1, 1};
    const RefTxnId uncommitted_snapshot_id{1, 2};

    DB::Cas::tests::fixture::admitLive(*backend, layout, ns);
    const NamespaceLifeId life = catalogLife(*backend, layout, ns);
    seedTxn(*backend, layout, ns, frontier, "committed", /*birth=*/true);
    writeRefSnapshotRaw(*backend, layout,
        minimalLiveSnapshot(ns.string(), uncommitted_snapshot_id,
            {committedRow("laundered", manifestRef(1, 2, 1))}));
    ASSERT_EQ(backend->putIfAbsent(layout.refCkptKey(life), encodeRefCkpt(RefCkpt{
        .life_epoch = std::optional<uint64_t>{1},
        .committed_through = frontier,
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt})).outcome, PutOutcome::Done);

    auto store = openWalkPool(backend);
    backend->resetCounts();
    const auto refs = store->listRefs(ns);

    EXPECT_EQ(backend->listCount(layout.namespaceStreamPrefix(life)), 0u);
    EXPECT_TRUE(refs.contains("committed"));
    EXPECT_FALSE(refs.contains("laundered"))
        << "a physical snapshot not named by `_ckpt` cannot raise the recovered cut";
}

TEST(CASRefRecoveryCasWalk, ListingShapeDoesNotAffectCheckpointRecovery)
{
    const Layout layout("p");
    const RootNamespace ns{"srv1/list_equivalence"};
    const RefTxnId base{1, 1};
    const RefTxnId frontier{1, 2};
    auto seed = std::make_shared<HidingListBackend>();

    DB::Cas::tests::fixture::admitLive(*seed, layout, ns);
    seedTxn(*seed, layout, ns, base, "a", /*birth=*/true);
    writeRefSnapshotRaw(*seed, layout,
        minimalLiveSnapshot(ns.string(), base, {committedRow("a", manifestRef(1, 1, 1))}));
    seedTxn(*seed, layout, ns, frontier, "b", /*birth=*/false);
    seedCkpt(*seed, layout, ns, RefCkpt{
        .life_epoch = std::optional<uint64_t>{1},
        .committed_through = frontier,
        .checkpoint_snapshot_id = base,
        .last_epoch_seal = std::nullopt});
    const NamespaceLifeId life = catalogLife(*seed, layout, ns);

    const auto clone_seed = [&]() -> std::shared_ptr<HidingListBackend>
    {
        /// A clone starts empty: constructing the normal fixture would pre-seed independent pool-meta
        /// bytes before this loop could copy the source's identical durable image.
        auto backend = std::make_shared<HidingListBackend>(/*seed_pool_meta=*/false);
        String cursor;
        do
        {
            const ListPage page = seed->list("", cursor, 1000);
            for (const ListedKey & listed : page.keys)
            {
                const auto object = seed->get(listed.key);
                if (!object)
                    throw std::runtime_error("seed LIST returned a key that exact GET could not read");
                const auto existing = backend->get(listed.key);
                if (existing)
                {
                    if (existing->bytes != object->bytes || existing->attributes != object->attributes)
                        throw std::runtime_error("clone backend constructor disagreed with seeded object");
                }
                else if (backend->putIfAbsent(listed.key, object->bytes, object->attributes).outcome != PutOutcome::Done)
                    throw std::runtime_error("clone backend failed to copy seeded object");
            }
            cursor = page.next_cursor;
        } while (!cursor.empty());
        return backend;
    };
    const auto recover = [&](const std::shared_ptr<HidingListBackend> & backend)
    {
        auto store = openWalkPool(backend);
        backend->resetCounts();
        const auto refs = store->listRefs(ns);
        EXPECT_EQ(backend->listCount(layout.namespaceStreamPrefix(life)), 0u);
        return refs;
    };

    const auto full = recover(clone_seed());
    auto empty_backend = clone_seed();
    empty_backend->hidden_keys.insert(layout.refSnapshotKey(life, base));
    empty_backend->hidden_keys.insert(layout.refLogKey(life, frontier));
    const auto empty = recover(empty_backend);
    ASSERT_EQ(full.size(), empty.size());
    for (const auto & [name, resolved] : full)
    {
        ASSERT_TRUE(empty.contains(name));
        EXPECT_EQ(resolved.manifest_id, empty.at(name).manifest_id);
        EXPECT_EQ(resolved.manifest_size, empty.at(name).manifest_size);
        EXPECT_EQ(resolved.published_at_ms, empty.at(name).published_at_ms);
    }
    EXPECT_TRUE(empty.contains("a"));
    EXPECT_TRUE(empty.contains("b"));
}

TEST(CASRefRecoveryCasWalk, PhantomListedSnapshotIsUnobserved)
{
    auto backend = std::make_shared<HidingListBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/stale_snapshot_hint"};
    const RefTxnId checkpoint_base{1, 1};
    const RefTxnId frontier{1, 2};

    seedTxn(*backend, layout, ns, checkpoint_base, "a", /*birth=*/true);
    writeRefSnapshotRaw(*backend, layout,
        minimalLiveSnapshot(ns.string(), checkpoint_base, {committedRow("a", manifestRef(1, 1, 1))}));
    seedTxn(*backend, layout, ns, frontier, "b", /*birth=*/false);
    seedCkpt(*backend, layout, ns, RefCkpt{
        .life_epoch = std::optional<uint64_t>{1},
        .committed_through = frontier,
        .checkpoint_snapshot_id = checkpoint_base,
        .last_epoch_seal = std::nullopt});
    const NamespaceLifeId life = catalogLife(*backend, layout, ns);
    backend->phantom_list_keys.insert(layout.refSnapshotKey(life, frontier));

    auto store = openWalkPool(backend);
    backend->resetCounts();
    const auto refs = store->listRefs(ns);

    EXPECT_EQ(backend->listCount(layout.namespaceStreamPrefix(life)), 0u);
    EXPECT_TRUE(refs.contains("a"));
    EXPECT_TRUE(refs.contains("b"));
}

TEST(CASRefRecoveryCasWalk, ListedFPlusTwoWithoutFPlusOneIsInertUncommittedDebris)
{
    auto backend = std::make_shared<HidingListBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/listed_uncommitted_debris"};
    const RefTxnId frontier{1, 1};

    seedTxn(*backend, layout, ns, frontier, "a", /*birth=*/true);
    seedTxn(*backend, layout, ns, RefTxnId{1, 3}, "debris", /*birth=*/false);
    seedCkpt(*backend, layout, ns, lifeEpochCkpt(1, frontier));

    auto store = openWalkPool(backend);
    backend->resetCounts();
    const auto refs = store->listRefs(ns);

    EXPECT_EQ(backend->listCount(layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns))), 0u);
    EXPECT_TRUE(refs.contains("a"));
    EXPECT_FALSE(refs.contains("debris"));
}

TEST(CASRefRecoveryCasWalk, DuplicateCatalogLifeIsCorruptionBeforeColdRuntimeAdmission)
{
    auto backend = std::make_shared<HidingListBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/ambiguous_life_a"};
    auto store = openWalkPool(backend);
    const NamespaceLifeId life = strandOneUnfrontieredSuccessor(*backend, store, layout, ns);
    ASSERT_EQ(store->laneStateForTest(ns), RefLaneState::NeedsRecovery);

    const CasRefCatalog::Snapshot sampled = CasRefCatalog::read(*backend, layout);
    ASSERT_EQ(sampled.catalog.entries.size(), 1u);
    RefCatalog ambiguous = sampled.catalog;
    ambiguous.entries.push_back(CatalogEntry{
        .ns = RootNamespace{"srv1/ambiguous_life_b"},
        .state = NsState::Live,
        .incarnation = life.incarnation});
    std::sort(ambiguous.entries.begin(), ambiguous.entries.end(),
        [](const CatalogEntry & lhs, const CatalogEntry & rhs) { return lhs.ns.string() < rhs.ns.string(); });
    ASSERT_EQ(backend->casPut(layout.refCatalogKey(), encodeRefCatalog(ambiguous), sampled.token).outcome,
              CasOutcome::Committed);

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&]
    {
        auto cold_store = openWalkPool(backend);
        (void)cold_store->listRefs(ns);
    });
}

TEST(CASRefRecoveryCasWalk, CheckpointAdvanceAfterLastLogProbeRestartsBeforeInstall)
{
    auto backend = std::make_shared<AfterGetHookBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/final_authority_validation"};
    const RefTxnId initial_frontier{1, 1};
    const RefTxnId concurrent_frontier{1, 2};

    seedTxn(*backend, layout, ns, initial_frontier, "a", /*birth=*/true);
    seedCkpt(*backend, layout, ns, lifeEpochCkpt(1, initial_frontier));
    const NamespaceLifeId life = catalogLife(*backend, layout, ns);
    backend->watched_key = layout.refLogKey(life, concurrent_frontier);
    backend->after_get = [&]
    {
        seedTxn(*backend, layout, ns, concurrent_frontier, "b", /*birth=*/false);
        const auto sampled = readCkpt(*backend, layout, life);
        ASSERT_TRUE(sampled);
        RefCkpt advanced = sampled->ckpt;
        advanced.committed_through = concurrent_frontier;
        ASSERT_EQ(backend->casPut(layout.refCkptKey(life), encodeRefCkpt(advanced), sampled->token).outcome,
                  CasOutcome::Committed);
    };

    auto store = openWalkPool(backend);
    const uint64_t restarts_before = store->refRecoveryRestartsForTest(ns);
    const auto refs = store->listRefs(ns);

    EXPECT_TRUE(refs.contains("a"));
    EXPECT_TRUE(refs.contains("b"))
        << "the old private cut must be discarded when final exact authority moved after its last probe";
    EXPECT_GT(store->refRecoveryRestartsForTest(ns), restarts_before)
        << "the final authority observation is recovery's linearization point";
}

TEST(CASRefRecoveryCasWalk, LiveCatalogLifeWithoutReadableCheckpointIsCorruption)
{
    auto backend = std::make_shared<HidingListBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/live_without_ckpt"};

    DB::Cas::tests::fixture::admitLive(*backend, layout, ns);
    const NamespaceLifeId life = catalogLife(*backend, layout, ns);
    seedTxn(*backend, layout, ns, RefTxnId{7, 1}, "hint-must-not-be-genesis", /*birth=*/true);
    ASSERT_FALSE(readCkpt(*backend, layout, life));

    auto store = openWalkPool(backend);
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { (void)store->listRefs(ns); });
}

/// A 404 BELOW the exact committed frontier is not the end of the stream -- it is a HOLE, and a hole
/// in a dense stream is corruption. Recovery exact-reads the checkpoint token once to distinguish a
/// concurrently moved cut from durable-data loss, then FAILS CLOSED while that token is unchanged. It
/// must never fold what it has: that is precisely how an acknowledged transaction disappears.
TEST(CASRefRecoveryCasWalk, AbsentIdBelowADurableHigherIdFailsClosed)
{
    auto backend = std::make_shared<HidingListBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/hole"};

    seedCkpt(*backend, layout, ns, lifeEpochCkpt(1, RefTxnId{1, 3}));
    seedTxn(*backend, layout, ns, RefTxnId{1, 1}, "a", /*birth=*/true);
    /// {1,2} is MISSING while {1,3} is durable and listed: the listing itself witnesses the hole.
    seedTxn(*backend, layout, ns, RefTxnId{1, 3}, "c", /*birth=*/false);

    auto store = openWalkPool(backend);
    ASSERT_TRUE(store);
    store->setCasRetrySleepForTest([](uint64_t) {});

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { store->listRefs(ns); });
}

/// ---------------------------------------------------------------------------------------------
/// The CAS-walk: closing dead epochs in-band
/// ---------------------------------------------------------------------------------------------

/// The ordinary case: one dead epoch, closed by OUR seal at `{E, T+1}` -- the exact key a dying
/// predecessor's in-flight PUT would have taken, which is what makes the store's conditional create the
/// fence (INV-2) rather than a detector after the fact.
TEST(CASRefRecoveryCasWalk, DeadEpochIsClosedByOurOwnSealAtTPlusOne)
{
    auto backend = std::make_shared<HidingListBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/seal_created"};

    burnEpochsUpTo(*backend, layout, /*target_live_epoch=*/2);
    seedCkpt(*backend, layout, ns, lifeEpochCkpt(1, RefTxnId{1, 1}));
    seedTxn(*backend, layout, ns, RefTxnId{1, 1}, "a", /*birth=*/true);

    auto store = openWalkPool(backend);
    ASSERT_TRUE(store);
    ASSERT_EQ(store->liveWriterEpoch(), 2u);

    const uint64_t sealed_before = counterOf(ProfileEvents::CASRefRecoveryEpochSealed);
    ASSERT_EQ(store->listRefs(ns).size(), 1u);
    EXPECT_EQ(counterOf(ProfileEvents::CASRefRecoveryEpochSealed), sealed_before + 1);

    const RefTxnId seal_id{1, 2};
    const auto seal = readLogTxn(*backend, layout, ns, seal_id);
    ASSERT_TRUE(seal.has_value()) << "epoch 1 is dead and must be closed at {1,2}";
    EXPECT_TRUE(refLogTxnIsEpochSeal(*seal));
    EXPECT_EQ(seal->prev_epoch_seal, std::nullopt) << "sequence 2 never carries a chain link";
    EXPECT_EQ(store->lastEpochSealForTest(ns), std::optional<RefTxnId>(seal_id))
        << "the chain link the next epoch's sequence-1 transaction must name";
}

/// A concurrent recoverer got there first. Its seal is already at `{E, T+1}`, so our conditional create
/// loses -- and the right reaction is to ADOPT it, not to treat a peer's correct write as interference.
/// The adopted seal is the same chain link ours would have been.
TEST(CASRefRecoveryCasWalk, ConcurrentRecoverersSealIsAdoptedNotContested)
{
    auto backend = std::make_shared<LateMaterializeBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/seal_adopt"};

    burnEpochsUpTo(*backend, layout, /*target_live_epoch=*/2);
    seedCkpt(*backend, layout, ns, lifeEpochCkpt(1, RefTxnId{1, 1}));
    seedTxn(*backend, layout, ns, RefTxnId{1, 1}, "a", /*birth=*/true);
    /// The peer's seal lands between our read of {1,2} and our create of it, so we meet it as an
    /// OCCUPANT rather than as a tail entry.
    backend->late_key = layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), RefTxnId{1, 2});
    backend->late_bytes = sealObject(FormatId::RefLog, encodeRefLogTxn(makeSealTxn(ns, RefTxnId{1, 2})));

    auto store = openWalkPool(backend);
    ASSERT_TRUE(store);

    const uint64_t adopted_before = counterOf(ProfileEvents::CASRefRecoveryEpochSealAdopted);
    ASSERT_EQ(store->listRefs(ns).size(), 1u);
    EXPECT_GT(counterOf(ProfileEvents::CASRefRecoveryEpochSealAdopted), adopted_before);
    EXPECT_EQ(store->lastEpochSealForTest(ns), std::optional<RefTxnId>(RefTxnId{1, 2}))
        << "an adopted seal is this namespace's chain link exactly as a minted one is";
}

/// A STRAGGLER: an ordinary transaction of the dead epoch landed at `{E, T+1}` after our read of the
/// tail and before our seal. The rule is state-derived ids (INV-2): adopt the transaction, advance `T`
/// by ONE, and re-seal at the NEW `T+1`. Never mint `T+2` around it -- that writes a hole into the
/// durable stream that no later reader can tell from a lost object.
TEST(CASRefRecoveryCasWalk, StragglerAtTPlusOneIsAdoptedAndResealedAtTheNewTPlusOne)
{
    auto backend = std::make_shared<LateMaterializeBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/straggler"};

    burnEpochsUpTo(*backend, layout, /*target_live_epoch=*/2);
    seedCkpt(*backend, layout, ns, lifeEpochCkpt(1, RefTxnId{1, 1}));
    seedTxn(*backend, layout, ns, RefTxnId{1, 1}, "a", /*birth=*/true);
    /// The dying epoch's last append materializes between our read of {1,2} and our create of it -- the
    /// straggler, arriving exactly where the every-attempt rule says it can.
    backend->late_key = layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), RefTxnId{1, 2});
    backend->late_bytes = sealObject(FormatId::RefLog,
        encodeRefLogTxn(makeOrdinaryTxn(ns, RefTxnId{1, 2}, "late", /*birth=*/false)));

    auto store = openWalkPool(backend);
    ASSERT_TRUE(store);

    const uint64_t straggler_before = counterOf(ProfileEvents::CASRefRecoveryStragglerAdopted);
    const auto refs = store->listRefs(ns);
    EXPECT_EQ(refs.size(), 2u) << "the straggler's transaction is durable and must be applied, not skipped";
    EXPECT_TRUE(refs.contains("late"));
    EXPECT_GT(counterOf(ProfileEvents::CASRefRecoveryStragglerAdopted), straggler_before);

    const auto seal = readLogTxn(*backend, layout, ns, RefTxnId{1, 3});
    ASSERT_TRUE(seal.has_value()) << "the epoch must be re-sealed at the NEW T+1 = {1,3}, never at a blindly minted T+2";
    EXPECT_TRUE(refLogTxnIsEpochSeal(*seal));
    EXPECT_EQ(store->lastEpochSealForTest(ns), std::optional<RefTxnId>(RefTxnId{1, 3}));
}

TEST(CASRefRecoveryCasWalk, RecoveryPublishesEveryOccupiedObjectBeforeAdvancingPastIt)
{
    struct Case
    {
        String suffix;
        uint64_t live_epoch;
        RefTxnId occupant;
        RefTxnId forbidden_successor;
        bool occupant_is_seal;
    };
    const std::vector<Case> cases{
        {"seal", 3, {1, 2}, {2, 1}, true},
        {"straggler", 2, {1, 2}, {1, 3}, false},
    };

    for (const Case & test_case : cases)
    {
        SCOPED_TRACE(test_case.suffix);
        auto backend = std::make_shared<LateMaterializeBackend>();
        const Layout layout("p");
        const RootNamespace ns{"srv1/occupied_frontier_" + test_case.suffix};
        const RefTxnId initial_frontier{1, 1};

        burnEpochsUpTo(*backend, layout, test_case.live_epoch);
        seedTxn(*backend, layout, ns, initial_frontier, "a", /*birth=*/true);
        seedCkpt(*backend, layout, ns, lifeEpochCkpt(1, initial_frontier));
        const NamespaceLifeId life = catalogLife(*backend, layout, ns);
        backend->late_key = layout.refLogKey(life, test_case.occupant);
        const RefLogTxn occupant = test_case.occupant_is_seal
            ? makeSealTxn(ns, test_case.occupant)
            : makeOrdinaryTxn(ns, test_case.occupant, "late", /*birth=*/false);
        backend->late_bytes = sealObject(FormatId::RefLog, encodeRefLogTxn(occupant));

        uint64_t fake_now = 1'000'000;
        PoolConfig config = walkTestConfig();
        config.boot_ms_fn = [&fake_now] { return fake_now; };
        config.cas_request_budget.recovery_retry_budget_ms = 1;
        config.cas_request_budget.recovery_retry_initial_backoff_ms = 1;
        config.cas_request_budget.recovery_retry_max_backoff_ms = 1;
        auto store = openWalkPool(backend, config);
        store->setCasRetrySleepForTest([&fake_now](uint64_t ms) { fake_now += ms; });

        backend->ambiguous_cas_substr = layout.refCkptKey(life);
        backend->ambiguous_cas_count = 100'000;
        expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { (void)store->listRefs(ns); });

        EXPECT_TRUE(backend->get(layout.refLogKey(life, test_case.occupant)));
        EXPECT_FALSE(backend->get(layout.refLogKey(life, test_case.forbidden_successor)))
            << "recovery advanced before exact _ckpt certified the occupied object";
        EXPECT_EQ(readCkpt(*backend, layout, life)->ckpt.committed_through, initial_frontier);
        EXPECT_FALSE(store->refTableRecoveredForTest(ns));
    }
}

/// Two BURNED epochs -- mounted, never written to, and abandoned. `CasPool`'s epoch allocator mints and
/// never reclaims, so this is the normal shape of a pool that has restarted a few times, not an
/// anomaly. Each empty epoch is closed by its own sequence-1 seal, and each carries the previous seal as
/// its `prev_epoch_seal`: the chain is what makes a MISSING epoch detectable, which arithmetic within an
/// epoch cannot do.
TEST(CASRefRecoveryCasWalk, TwoBurnedEmptyEpochsProduceTwoChainedSequenceOneSeals)
{
    auto backend = std::make_shared<HidingListBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/burned"};

    burnEpochsUpTo(*backend, layout, /*target_live_epoch=*/4);
    seedCkpt(*backend, layout, ns, lifeEpochCkpt(1, RefTxnId{1, 1}));
    seedTxn(*backend, layout, ns, RefTxnId{1, 1}, "a", /*birth=*/true);

    auto store = openWalkPool(backend);
    ASSERT_TRUE(store);
    ASSERT_EQ(store->liveWriterEpoch(), 4u);

    ASSERT_EQ(store->listRefs(ns).size(), 1u);

    const auto seal1 = readLogTxn(*backend, layout, ns, RefTxnId{1, 2});
    ASSERT_TRUE(seal1.has_value()) << "epoch 1 closes at {1,2}";
    EXPECT_EQ(seal1->prev_epoch_seal, std::nullopt);

    const auto seal2 = readLogTxn(*backend, layout, ns, RefTxnId{2, 1});
    ASSERT_TRUE(seal2.has_value()) << "empty epoch 2 still closes -- at its sequence 1";
    EXPECT_TRUE(refLogTxnIsEpochSeal(*seal2));
    EXPECT_EQ(seal2->prev_epoch_seal, std::optional<RefTxnId>(RefTxnId{1, 2}))
        << "a sequence-1 seal MUST name the seal that closed the previous epoch";

    const auto seal3 = readLogTxn(*backend, layout, ns, RefTxnId{3, 1});
    ASSERT_TRUE(seal3.has_value()) << "empty epoch 3 closes too";
    EXPECT_EQ(seal3->prev_epoch_seal, std::optional<RefTxnId>(RefTxnId{2, 1}));

    EXPECT_FALSE(readLogTxn(*backend, layout, ns, RefTxnId{4, 1}).has_value())
        << "epoch 4 is LIVE -- sealing it would close the epoch this mount writes in";
    EXPECT_EQ(store->lastEpochSealForTest(ns), std::optional<RefTxnId>(RefTxnId{3, 1}));
}

TEST(CASRefRecoveryCasWalk, RecoveryPublishesEachCreatedSealBeforeCreatingTheNextEpochSeal)
{
    auto backend = std::make_shared<HidingListBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/seal_frontier_before_next"};
    const RefTxnId initial_frontier{1, 1};
    const RefTxnId first_seal{1, 2};
    const RefTxnId second_seal{2, 1};
    const RefTxnId cold_remount_frontier{3, 1};

    burnEpochsUpTo(*backend, layout, /*target_live_epoch=*/3);
    seedTxn(*backend, layout, ns, initial_frontier, "a", /*birth=*/true);
    seedCkpt(*backend, layout, ns, lifeEpochCkpt(1, initial_frontier));
    const NamespaceLifeId life = catalogLife(*backend, layout, ns);

    uint64_t fake_now = 1'000'000;
    PoolConfig config = walkTestConfig();
    config.boot_ms_fn = [&fake_now] { return fake_now; };
    config.cas_request_budget.recovery_retry_budget_ms = 1;
    config.cas_request_budget.recovery_retry_initial_backoff_ms = 1;
    config.cas_request_budget.recovery_retry_max_backoff_ms = 1;
    auto store = openWalkPool(backend, config);
    ASSERT_EQ(store->liveWriterEpoch(), 3u);
    store->setCasRetrySleepForTest([&fake_now](uint64_t ms) { fake_now += ms; });

    backend->ambiguous_cas_substr = layout.refCkptKey(life);
    backend->ambiguous_cas_count = 100'000;
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { (void)store->listRefs(ns); });

    EXPECT_TRUE(backend->get(layout.refLogKey(life, first_seal)))
        << "the first recovery seal became durable before its frontier attempt";
    EXPECT_FALSE(backend->get(layout.refLogKey(life, second_seal)))
        << "recovery may not create a second object while the first is still above exact _ckpt";
    EXPECT_EQ(readCkpt(*backend, layout, life)->ckpt.committed_through, initial_frontier);
    EXPECT_FALSE(store->refTableRecoveredForTest(ns));

    /// Restart cold, without the failed mount's `NeedsRecovery` attempt. The first seal is durable but
    /// still outside `_ckpt`; the remount must recover and certify it before it may create `{2,1}`.
    backend->ambiguous_cas_count = 0;
    store.reset();
    auto cold_store = openWalkPool(backend);
    ASSERT_EQ(cold_store->liveWriterEpoch(), 4u);
    ASSERT_EQ(cold_store->listRefs(ns).size(), 1u);
    EXPECT_TRUE(backend->get(layout.refLogKey(life, second_seal)));
    EXPECT_TRUE(backend->get(layout.refLogKey(life, cold_remount_frontier)));
    EXPECT_EQ(readCkpt(*backend, layout, life)->ckpt.committed_through, cold_remount_frontier);
}

/// A straggler is not an exception to the recovered-successor rule. When it materializes in the seal
/// slot, recovery adopts it as the one object above its accepted checkpoint and must certify that exact
/// frontier before it can create the following seal at the new `T+1`.
TEST(CASRefRecoveryCasWalk, RecoveryPublishesAnAdoptedStragglerBeforeCreatingItsFollowingSeal)
{
    auto backend = std::make_shared<LateMaterializeBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/straggler_frontier_before_seal"};
    const RefTxnId initial_frontier{1, 1};
    const RefTxnId straggler{1, 2};
    const RefTxnId following_seal{1, 3};

    burnEpochsUpTo(*backend, layout, /*target_live_epoch=*/2);
    seedTxn(*backend, layout, ns, initial_frontier, "a", /*birth=*/true);
    seedCkpt(*backend, layout, ns, lifeEpochCkpt(1, initial_frontier));
    const NamespaceLifeId life = catalogLife(*backend, layout, ns);
    backend->late_key = layout.refLogKey(life, straggler);
    backend->late_bytes = sealObject(FormatId::RefLog,
        encodeRefLogTxn(makeOrdinaryTxn(ns, straggler, "late", /*birth=*/false)));

    uint64_t fake_now = 1'000'000;
    PoolConfig config = walkTestConfig();
    config.boot_ms_fn = [&fake_now] { return fake_now; };
    config.cas_request_budget.recovery_retry_budget_ms = 1;
    config.cas_request_budget.recovery_retry_initial_backoff_ms = 1;
    config.cas_request_budget.recovery_retry_max_backoff_ms = 1;
    auto store = openWalkPool(backend, config);
    store->setCasRetrySleepForTest([&fake_now](uint64_t ms) { fake_now += ms; });

    backend->ambiguous_cas_substr = layout.refCkptKey(life);
    backend->ambiguous_cas_count = 100'000;
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { (void)store->listRefs(ns); });

    EXPECT_TRUE(backend->get(layout.refLogKey(life, straggler)))
        << "the straggler occupied the recovery seal slot";
    EXPECT_FALSE(backend->get(layout.refLogKey(life, following_seal)))
        << "recovery may not create a seal after an adopted straggler above exact _ckpt";
    EXPECT_EQ(readCkpt(*backend, layout, life)->ckpt.committed_through, initial_frontier);
    EXPECT_FALSE(store->refTableRecoveredForTest(ns));

    backend->ambiguous_cas_count = 0;
    ASSERT_EQ(store->listRefs(ns).size(), 2u);
    EXPECT_TRUE(backend->get(layout.refLogKey(life, following_seal)));
    EXPECT_EQ(readCkpt(*backend, layout, life)->ckpt.committed_through, following_seal);
}

/// GENESIS. A namespace born at epoch 5 has no epochs 1-4 of its own: they are not "empty epochs it
/// failed to close", they are epochs before it existed. The walk starts at the namespace's `life_epoch`
/// and writes no phantom seals below it, and with no transition ever having happened it installs NO
/// chain link -- `nullopt` means genesis and must mean it exactly, or the table's first transaction
/// would be required to name a seal that never existed.
TEST(CASRefRecoveryCasWalk, GenesisAtEpochFiveWritesNoPhantomSealsBelowLifeEpoch)
{
    auto backend = std::make_shared<HidingListBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/genesis5"};

    burnEpochsUpTo(*backend, layout, /*target_live_epoch=*/5);
    seedCkpt(*backend, layout, ns, lifeEpochCkpt(5, RefTxnId{5, 1}));
    seedTxn(*backend, layout, ns, RefTxnId{5, 1}, "a", /*birth=*/true);

    auto store = openWalkPool(backend);
    ASSERT_TRUE(store);
    ASSERT_EQ(store->liveWriterEpoch(), 5u);

    ASSERT_EQ(store->listRefs(ns).size(), 1u);

    for (uint64_t e = 1; e <= 4; ++e)
        EXPECT_FALSE(readLogTxn(*backend, layout, ns, RefTxnId{e, 1}).has_value())
            << "no seal may be written for epoch " << e << ", which predates this namespace";
    EXPECT_EQ(store->lastEpochSealForTest(ns), std::nullopt)
        << "no transition ever happened for this namespace: nullopt means GENESIS and must mean it exactly";
}

/// ---------------------------------------------------------------------------------------------
/// The trio: ONE captured generation, three checks
/// ---------------------------------------------------------------------------------------------

/// The GENERIC mid-walk bump: the fence moves while recovery is doing I/O, so the incarnation that
/// admitted this work is gone. Nothing may be installed -- the recovered view belongs to a mount that no
/// longer owns the namespace. The table stays unrecovered, and a retry under the CURRENT generation
/// succeeds, which is what makes this a refusal rather than a wedge.
TEST(CASRefRecoveryCasWalk, FenceBumpedMidWalkRefusesTheInstallAndTheRetrySucceeds)
{
    auto backend = std::make_shared<GetSeamBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/bump_midwalk"};

    seedCkpt(*backend, layout, ns, lifeEpochCkpt(1, RefTxnId{1, 1}));
    seedTxn(*backend, layout, ns, RefTxnId{1, 1}, "a", /*birth=*/true);

    auto store = openWalkPool(backend);
    ASSERT_TRUE(store);

    std::atomic<bool> bumped{false};
    backend->watched_substr = "_log/";
    backend->on_key = [&](const String &)
    {
        if (!bumped.exchange(true))
            rearmMountFenceAfterAnomalyForTest(store);
    };

    EXPECT_ANY_THROW(store->listRefs(ns)) << "a recovery whose I/O window straddled a fence bump must install nothing";

    backend->on_key = nullptr;
    fenceOutMountForRemount(*backend, layout.mountKey("test"));
    ASSERT_TRUE(store->tryRemountOnce())
        << "a generation bump cannot rebind the captured runtime; the production remount must publish "
           "a distinct runtime at the accepted generation";
    EXPECT_EQ(store->listRefs(ns).size(), 1u) << "the retry through the remounted runtime succeeds";
}

TEST(CASRefRecoveryCasWalk, RetiredLifePausedInRealRecoveryIoWritesAndInstallsNothing)
{
    auto backend = std::make_shared<GetSeamBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/recovery-retired-mid-io"};

    burnEpochsUpTo(*backend, layout, /*target_live_epoch=*/2);
    seedCkpt(*backend, layout, ns, lifeEpochCkpt(1, RefTxnId{1, 1}));
    seedTxn(*backend, layout, ns, RefTxnId{1, 1}, "predecessor", /*birth=*/true);
    const CatalogEntry predecessor = CasRefCatalog::read(*backend, layout).catalog.entries.front();
    const NamespaceLifeId predecessor_life
        = NamespaceLifeId::fromCatalogEntry(predecessor.ns, predecessor.incarnation);
    const auto predecessor_ckpt_before = backend->get(layout.refCkptKey(predecessor_life));
    ASSERT_TRUE(predecessor_ckpt_before);

    auto store = openWalkPool(backend);
    ASSERT_EQ(store->liveWriterEpoch(), 2u);
    const uint64_t recovery_installs_before = store->recoveryInstallCountForTest();

    std::mutex mutex;
    std::condition_variable cv;
    bool paused = false;
    bool resume = false;
    backend->watched_substr = "_log/";
    backend->on_key = [&](const String &)
    {
        std::unique_lock lock(mutex);
        paused = true;
        cv.notify_all();
        cv.wait(lock, [&] { return resume; });
    };

    std::exception_ptr recovery_error;
    std::thread recovery([&]
    {
        try
        {
            (void)store->listRefs(ns);
        }
        catch (...)
        {
            recovery_error = std::current_exception();
        }
    });
    {
        std::unique_lock lock(mutex);
        cv.wait(lock, [&] { return paused; });
    }

    const CatalogEntry successor = replaceCatalogLifeForTest(*backend, layout, predecessor, UInt128{0x5152});
    const NamespaceLifeId successor_life
        = NamespaceLifeId::fromCatalogEntry(successor.ns, successor.incarnation);
    ASSERT_EQ(backend->putIfAbsent(layout.refCkptKey(successor_life), encodeRefCkpt(lifeEpochCkpt(2))).outcome,
        PutOutcome::Done);
    const auto successor_ckpt_before = backend->get(layout.refCkptKey(successor_life));
    ASSERT_TRUE(successor_ckpt_before);
    store->invalidateRemovedCatalogLife(predecessor_life);
    backend->resetCounts();

    {
        std::lock_guard lock(mutex);
        resume = true;
    }
    cv.notify_all();
    recovery.join();

    EXPECT_TRUE(recovery_error) << "the predecessor recovery must be refused, not exposed";
    EXPECT_EQ(backend->putCount(layout.refLogKey(predecessor_life, RefTxnId{1, 2})), 0u)
        << "no predecessor seal retry may be sent after exact retirement";
    EXPECT_EQ(backend->casPutCount(layout.refCkptKey(predecessor_life)), 0u)
        << "no predecessor checkpoint CAS may be sent after exact retirement";
    const auto predecessor_ckpt_after = backend->get(layout.refCkptKey(predecessor_life));
    ASSERT_TRUE(predecessor_ckpt_after);
    EXPECT_EQ(predecessor_ckpt_after->token, predecessor_ckpt_before->token);
    EXPECT_FALSE(store->refTableRecoveredForTest(ns)) << "the detached predecessor result was installed";
    EXPECT_EQ(store->recoveryInstallCountForTest(), recovery_installs_before)
        << "the detached predecessor reached the recovery publication point";
    const String successor_prefix = layout.namespaceStreamPrefix(successor_life);
    for (const String & key : backend->touchedKeys())
        EXPECT_EQ(key.find(successor_prefix), String::npos)
            << "predecessor recovery retargeted storage I/O into successor key " << key;
    const auto successor_ckpt_after = backend->get(layout.refCkptKey(successor_life));
    ASSERT_TRUE(successor_ckpt_after);
    EXPECT_EQ(successor_ckpt_after->token, successor_ckpt_before->token);
    EXPECT_EQ(successor_ckpt_after->bytes, successor_ckpt_before->bytes);
}

/// Bump point 1 of the trio's two interior seams: AFTER the slot-occupy landed, BEFORE the `_ckpt` CAS.
/// The seal is durable (it was written under a generation that was still valid), but the checkpoint must
/// NOT advance and nothing may be installed. This is the seam a single "check the fence at entry" would
/// miss entirely.
TEST(CASRefRecoveryCasWalk, FenceBumpedAfterSlotOccupyBeforeCkptCasAdvancesNoCheckpoint)
{
    auto backend = std::make_shared<PutHookBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/bump_after_seal"};

    burnEpochsUpTo(*backend, layout, /*target_live_epoch=*/2);
    seedCkpt(*backend, layout, ns, lifeEpochCkpt(1, RefTxnId{1, 1}));
    seedTxn(*backend, layout, ns, RefTxnId{1, 1}, "a", /*birth=*/true);

    auto store = openWalkPool(backend);
    ASSERT_TRUE(store);

    const auto ckpt_before = readCkpt(*backend, layout, DB::Cas::tests::fixture::fixtureLife(ns));
    ASSERT_TRUE(ckpt_before.has_value());

    backend->watched_substr = "_log/";
    backend->on_key = [&] { rearmMountFenceAfterAnomalyForTest(store); };

    EXPECT_ANY_THROW(store->listRefs(ns));

    const auto ckpt_after = readCkpt(*backend, layout, DB::Cas::tests::fixture::fixtureLife(ns));
    ASSERT_TRUE(ckpt_after.has_value());
    EXPECT_EQ(ckpt_after->ckpt.last_epoch_seal, std::nullopt)
        << "the seal is durable but the checkpoint must not record it under a generation that moved";
    EXPECT_EQ(ckpt_after->token, ckpt_before->token) << "no CAS was sent at all";
}

/// Bump point 2: AFTER the `_ckpt` CAS, BEFORE the install. The checkpoint advance is harmless (the
/// merge is a semantic maximum, so the retry re-derives the same or a greater value), but the STATE must
/// not be published: this runtime's view belongs to a dead incarnation. Today there is no such recheck
/// at all -- that gap is the whole reason this test exists.
TEST(CASRefRecoveryCasWalk, FenceBumpedAfterCkptCasBeforeInstallPublishesNoState)
{
    auto backend = std::make_shared<PutHookBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/bump_after_ckpt"};

    burnEpochsUpTo(*backend, layout, /*target_live_epoch=*/2);
    seedCkpt(*backend, layout, ns, lifeEpochCkpt(1, RefTxnId{1, 1}));
    seedTxn(*backend, layout, ns, RefTxnId{1, 1}, "a", /*birth=*/true);

    auto store = openWalkPool(backend);
    ASSERT_TRUE(store);

    /// The `_ckpt` CAS is the LAST write recovery performs, so hooking it fires strictly between the
    /// checkpoint advance and the install recheck.
    backend->watched_substr = "/_ckpt";
    backend->on_key = [&] { rearmMountFenceAfterAnomalyForTest(store); };

    EXPECT_ANY_THROW(store->listRefs(ns)) << "the install recheck must refuse a result from a moved generation";

    const auto ckpt_after = readCkpt(*backend, layout, DB::Cas::tests::fixture::fixtureLife(ns));
    ASSERT_TRUE(ckpt_after.has_value());
    EXPECT_EQ(ckpt_after->ckpt.last_epoch_seal, std::optional<RefTxnId>(RefTxnId{1, 2}))
        << "the checkpoint advance already landed and is harmless -- the merge is a semantic maximum";

    backend->on_key = nullptr;
    fenceOutMountForRemount(*backend, layout.mountKey("test"));
    ASSERT_TRUE(store->tryRemountOnce())
        << "a generation bump cannot rebind the captured runtime; the production remount must publish "
           "a distinct runtime at the accepted generation";
    EXPECT_EQ(store->listRefs(ns).size(), 1u) << "the retry through the remounted runtime installs normally";
}

/// ---------------------------------------------------------------------------------------------
/// The self-remount barrier
/// ---------------------------------------------------------------------------------------------

/// Spec §3: "self-remount cancels or waits out recovery before rearming." The install recheck alone is
/// not that rule -- it protects the install, not the WINDOW. A recovery paused in its I/O while the
/// fence is re-armed would still be holding an admitted generation that is about to be superseded, and
/// the barrier is what guarantees no `_ckpt` CAS and no install can follow the re-arm.
///
/// Driven at a real I/O seam: recovery blocks inside a `get`, the remount barrier is invoked from
/// another thread and must BLOCK, the recovery is released, acknowledges the cancellation, and only then
/// does the barrier return.
TEST(CASRefRecoveryCasWalk, RemountBarrierBlocksUntilAPausedRecoveryAcknowledgesCancellation)
{
    auto backend = std::make_shared<GetSeamBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/remount_barrier"};

    burnEpochsUpTo(*backend, layout, /*target_live_epoch=*/2);
    seedCkpt(*backend, layout, ns, lifeEpochCkpt(1, RefTxnId{1, 1}));
    seedTxn(*backend, layout, ns, RefTxnId{1, 1}, "a", /*birth=*/true);

    auto store = openWalkPool(backend);
    ASSERT_TRUE(store);

    std::mutex m;
    std::condition_variable cv;
    bool recovery_parked = false;
    bool release_recovery = false;

    backend->watched_substr = "_log/";
    /// `GetSeamBackend` moves the hook out before calling it, so this parks exactly once without the
    /// hook having to clear itself -- see its `get` for why self-clearing is a use-after-free.
    backend->on_key = [&](const String &)
    {
        std::unique_lock lock(m);
        recovery_parked = true;
        cv.notify_all();
        cv.wait(lock, [&] { return release_recovery; });
    };

    const uint64_t ckpt_before = counterOf(ProfileEvents::CASRefCheckpointPublished);
    const uint64_t cancelled_before = counterOf(ProfileEvents::CASRefRecoveryCancelled);

    std::thread recovery([&] { try { store->listRefs(ns); } catch (...) {} }); // NOLINT(bugprone-empty-catch): the outcome is asserted below via the ProfileEvents counters, not this thread's exception

    {
        std::unique_lock lock(m);
        cv.wait(lock, [&] { return recovery_parked; });
    }

    std::atomic<bool> barrier_returned{false};
    std::thread barrier([&]
    {
        store->cancelRefRecoveriesAndAwaitQuiescence();
        barrier_returned.store(true);
    });

    /// Wait for the barrier's REQUEST to be visible before touching anything else. Releasing the parked
    /// recovery any earlier would race it past a flag set a moment too late, and the test would observe
    /// an ordinary completion and call it a missing cancellation.
    while (!store->refRecoveryCancelRequestedForTest(ns))
        std::this_thread::yield();

    /// The request is published and the recovery is still parked, so the barrier is now provably inside
    /// its wait. It must not have returned: fence re-arm may not proceed while a recovery is in flight.
    EXPECT_FALSE(barrier_returned.load());

    {
        std::lock_guard lock(m);
        release_recovery = true;
    }
    cv.notify_all();

    barrier.join();
    recovery.join();
    EXPECT_TRUE(barrier_returned.load());

    EXPECT_GT(counterOf(ProfileEvents::CASRefRecoveryCancelled), cancelled_before)
        << "the released recovery must observe the cancellation rather than run to completion";
    EXPECT_EQ(counterOf(ProfileEvents::CASRefCheckpointPublished), ckpt_before)
        << "a cancelled recovery performs ZERO _ckpt CASes";
    EXPECT_FALSE(store->refTableRecoveredForTest(ns)) << "and ZERO installs";
}

/// A `NeedsRecovery` lane replays the known-durable transaction before returning to `Ready`.
TEST(CASRefRecoveryCasWalk, NeedsRecoveryReplaysTheStrandedTxn)
{
    auto backend = std::make_shared<CountingBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/poisoned"};

    auto store = openWalkPool(backend);
    ASSERT_TRUE(store);

    /// Publish one ref through the real lane, then fail the next commit's install region: the
    /// transaction is durable and the install that would have recorded it throws.
    ASSERT_NO_THROW(store->appendRefOps(ns, MutationScope::ref("a"),
        [](const RefTableState & state)
        {
            std::vector<RefOp> ops;
            if (state.getLifecycle() != RefLifecycle::Live)
                ops.push_back(namespaceBirthOp());
            for (const RefOp & op : publishCommittedOps("a", manifestRef(1, 1, 1)))
                ops.push_back(op);
            return ops;
        },
        RootMutationOrigin::Writer, RootMutationKind::Publish));

    /// Built outside the region. One-shot, and re-allowing allocations for
    /// the duration of the throw: `std::rethrow_exception` allocates through libc++'s
    /// `__cxa_rethrow_primary_exception`, which the debug build's `DENY_ALLOCATIONS_IN_SCOPE` aborts on.
    /// (Found by the debug gate -- the first cut of this probe took the whole binary down there.) Same
    /// shape as `gtest_cas_ref_install_safety.cpp`'s `armOneShotInstallFailure`.
    auto planned_failure = std::make_exception_ptr(DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "install probe"));
    auto fired = std::make_shared<std::atomic<bool>>(false);
    store->setInstallRegionProbeForTest([planned_failure, fired]
    {
        if (fired->exchange(true))
            return;
        ALLOW_ALLOCATIONS_IN_SCOPE;
        std::rethrow_exception(planned_failure);
    });
    EXPECT_ANY_THROW(store->appendRefOps(ns, MutationScope::ref("b"),
        [](const RefTableState &) { return publishCommittedOps("b", manifestRef(1, 2, 1)); },
        RootMutationOrigin::Writer, RootMutationKind::Publish));
    store->setInstallRegionProbeForTest(nullptr);

    ASSERT_EQ(store->laneStateForTest(ns), RefLaneState::NeedsRecovery);
    /// "Durable but not applied here", stated as the two facts it is made of: the object IS in the
    /// store, and this runtime's floor is what keeps the allocator off its id. `ns` was born through
    /// the REAL production lane (`appendRefOps`), not the raw `seedTxn`/`casAdmitEntry` fixtures this
    /// file's OTHER tests use -- so its ref-layer objects sit at a REAL, catalog-minted incarnation,
    /// not the Stage-A sentinel `readLogTxn` assumes. Resolved here rather than through `readLogTxn`.
    {
        const CasRefCatalog::Snapshot snap = CasRefCatalog::read(*backend, layout);
        const CatalogEntry * entry = nullptr;
        for (const CatalogEntry & e : snap.catalog.entries)
            if (e.ns.string() == ns.string())
                entry = &e;
        ASSERT_NE(entry, nullptr) << "the birth above must have minted a catalog entry for " << ns.string();
        const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(entry->ns, entry->incarnation);
        ASSERT_TRUE(backend->get(layout.refLogKey(life, RefTxnId{1, 2})).has_value())
            << "the stranded transaction must be durable -- otherwise recovery is not owed";
    }

    /// The next touch drives recovery again -- this is the structural closure Task 3 deferred here.
    const auto refs = store->listRefs(ns);
    EXPECT_EQ(refs.size(), 2u);
    EXPECT_TRUE(refs.contains("b")) << "the walk re-derived the stranded transaction from the durable log";
    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Ready)
        << "only a completed recovery install returns the lane to Ready";
}

TEST(CASRefRecoveryCasWalk, WriterRecoveryAdoptsOneExactUnfrontieredSuccessorAndPublishesItsFrontier)
{
    auto backend = std::make_shared<HidingListBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/recovery_one_successor"};
    auto store = openWalkPool(backend);

    ASSERT_NO_THROW(store->appendRefOps(ns, MutationScope::ref("a"),
        [](const RefTableState & state)
        {
            std::vector<RefOp> ops;
            if (state.getLifecycle() != RefLifecycle::Live)
                ops.push_back(namespaceBirthOp());
            for (const RefOp & op : publishCommittedOps("a", manifestRef(1, 1, 1)))
                ops.push_back(op);
            return ops;
        }, RootMutationOrigin::Writer, RootMutationKind::Publish));

    const NamespaceLifeId life = catalogLife(*backend, layout, ns);
    const String ckpt_key = layout.refCkptKey(life);
    ASSERT_EQ(readCkpt(*backend, layout, life)->ckpt.committed_through, (RefTxnId{1, 1}));
    backend->ambiguous_cas_substr = ckpt_key;
    backend->ambiguous_cas_count = 200;

    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&]
    {
        store->appendRefOps(ns, MutationScope::ref("b"),
            [](const RefTableState &) { return publishCommittedOps("b", manifestRef(1, 2, 1)); },
            RootMutationOrigin::Writer, RootMutationKind::Publish);
    });
    ASSERT_EQ(store->laneStateForTest(ns), RefLaneState::NeedsRecovery);
    ASSERT_TRUE(backend->get(layout.refLogKey(life, RefTxnId{1, 2})))
        << "the sole deterministic successor must be durable before recovery";
    ASSERT_EQ(readCkpt(*backend, layout, life)->ckpt.committed_through, (RefTxnId{1, 1}));

    backend->ambiguous_cas_count = 0;
    const auto refs = store->listRefs(ns);

    EXPECT_TRUE(refs.contains("b"));
    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Ready);
    EXPECT_EQ(readCkpt(*backend, layout, life)->ckpt.committed_through, (RefTxnId{1, 2}))
        << "the successor is not installable until the current admitted fence publishes its frontier";
}

TEST(CASRefRecoveryCasWalk, ColdWriterRecoveryPublishesOneExactUnfrontieredSuccessorBeforeSealing)
{
    auto backend = std::make_shared<HidingListBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/recovery_cold_successor"};
    auto store = openWalkPool(backend);
    const NamespaceLifeId life = strandOneUnfrontieredSuccessor(*backend, store, layout, ns);
    ASSERT_EQ(store->laneStateForTest(ns), RefLaneState::NeedsRecovery);
    store.reset();
    std::vector<RefCkpt> checkpoint_cas_bodies;
    backend->before_cas_put = [&](const String & key, const String & bytes, const std::optional<Token> &)
    {
        if (key == layout.refCkptKey(life))
            checkpoint_cas_bodies.push_back(decodeRefCkpt(bytes));
    };

    /// A remount/process restart has no in-memory `RefAppendAttempt`; the writer recovery entry point
    /// still owns its one exact F+1 adoption duty from the durable checkpoint and log alone.
    auto cold_store = openWalkPool(backend);
    const auto refs = cold_store->listRefs(ns);

    EXPECT_TRUE(refs.contains("b"));
    EXPECT_EQ(cold_store->laneStateForTest(ns), RefLaneState::Ready);
    EXPECT_TRUE(std::any_of(checkpoint_cas_bodies.begin(), checkpoint_cas_bodies.end(),
        [](const RefCkpt & ckpt) { return ckpt.committed_through == std::make_optional(RefTxnId{1, 2}); }))
        << "the exact F+1 frontier must publish before the remount seals its dead epoch";
    EXPECT_EQ(readCkpt(*backend, layout, life)->ckpt.committed_through, (RefTxnId{1, 3}));
}

TEST(CASRefRecoveryCasWalk, WriterRecoveryAdoptsFirstCommittedTxnAboveLifeEpochOnlyCheckpoint)
{
    auto backend = std::make_shared<HidingListBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/recovery_first_unfrontiered"};
    auto store = openWalkPool(backend);

    /// Create the catalog life explicitly, then retain exactly the checkpoint fragment published by
    /// production birth before its first log. This makes `{1,1}` the first durable transaction above a
    /// readable checkpoint whose `committed_through` is absent.
    DB::Cas::tests::fixture::admitLive(*backend, layout, ns);
    const NamespaceLifeId life = catalogLife(*backend, layout, ns);
    ASSERT_EQ(backend->putIfAbsent(layout.refCkptKey(life), encodeRefCkpt(lifeEpochCkpt(1))).outcome,
              PutOutcome::Done);
    ASSERT_TRUE(readCkpt(*backend, layout, life)->ckpt.life_epoch);
    ASSERT_EQ(readCkpt(*backend, layout, life)->ckpt.committed_through, std::nullopt);

    backend->ambiguous_cas_substr = layout.refCkptKey(life);
    backend->ambiguous_cas_count = 200;
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&]
    {
        store->appendRefOps(ns, MutationScope::ref("a"),
            [](const RefTableState & state)
            {
                std::vector<RefOp> ops;
                if (state.getLifecycle() != RefLifecycle::Live)
                    ops.push_back(namespaceBirthOp());
                for (const RefOp & op : publishCommittedOps("a", manifestRef(1, 1, 1)))
                    ops.push_back(op);
                return ops;
            }, RootMutationOrigin::Writer, RootMutationKind::Publish);
    });
    ASSERT_EQ(store->laneStateForTest(ns), RefLaneState::NeedsRecovery);
    ASSERT_TRUE(backend->get(layout.refLogKey(life, RefTxnId{1, 1})));
    ASSERT_EQ(readCkpt(*backend, layout, life)->ckpt.committed_through, std::nullopt);
    ASSERT_FALSE(backend->get(layout.refSnapshotKey(life, RefTxnId{1, 1})))
        << "the grounding test must exercise the exact log successor, not a hinted snapshot";

    backend->ambiguous_cas_count = 0;
    const auto refs = store->listRefs(ns);

    EXPECT_TRUE(refs.contains("a"));
    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Ready);
    EXPECT_EQ(readCkpt(*backend, layout, life)->ckpt.committed_through, (RefTxnId{1, 1}));
}

TEST(CASRefRecoveryCasWalk, WriterRecoveryRestartsWhenCheckpointAdvancesPastPrivateCandidate)
{
    auto backend = std::make_shared<HidingListBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/recovery_checkpoint_moves"};
    auto store = openWalkPool(backend);
    const NamespaceLifeId life = strandOneUnfrontieredSuccessor(*backend, store, layout, ns);
    const String ckpt_key = layout.refCkptKey(life);
    const RefLogTxn later = makeOrdinaryTxn(ns, RefTxnId{1, 3}, "c", /*birth=*/false);
    bool injected = false;

    /// Recovery has fetched `{1,2}` and proved `{1,3}` absent. Another admitted writer can then append
    /// `{1,3}` and publish its frontier before recovery's own checkpoint CAS. The stale private
    /// candidate contains only `b`; it must restart and replay `c`, not accept an `IdenticalSkip` and
    /// install below the exact checkpoint it just observed.
    backend->before_cas_put = [&](const String & key, const String &, const std::optional<Token> & expected)
    {
        if (injected || key != ckpt_key)
            return;
        injected = true;
        ASSERT_TRUE(expected);
        ASSERT_EQ(backend->putIfAbsent(layout.refLogKey(life, later.txn_id),
                                      sealObject(FormatId::RefLog, encodeRefLogTxn(later))).outcome,
                  PutOutcome::Done);
        const auto current = backend->get(key);
        ASSERT_TRUE(current);
        ASSERT_EQ(current->token, *expected);
        const RefCkpt advanced = mergeCkpt(
            decodeRefCkpt(current->bytes),
            RefCkpt{.life_epoch = std::nullopt,
                    .committed_through = later.txn_id,
                    .checkpoint_snapshot_id = std::nullopt,
                    .last_epoch_seal = std::nullopt});
        ASSERT_EQ(backend->putOverwrite(key, encodeRefCkpt(advanced), current->token).outcome, PutOutcome::Done);
    };

    const auto refs = store->listRefs(ns);

    EXPECT_TRUE(injected);
    EXPECT_TRUE(refs.contains("b"));
    EXPECT_TRUE(refs.contains("c")) << "recovery must restart from the newer exact frontier";
    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Ready);
    EXPECT_EQ(readCkpt(*backend, layout, life)->ckpt.committed_through, later.txn_id);
}

TEST(CASRefRecoveryCasWalk, WriterRecoveryRejectsTwoUnfrontieredSuccessorsAfterExactCheckpointReread)
{
    auto backend = std::make_shared<HidingListBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/recovery_two_successors"};
    auto store = openWalkPool(backend);

    ASSERT_NO_THROW(store->appendRefOps(ns, MutationScope::ref("a"),
        [](const RefTableState & state)
        {
            std::vector<RefOp> ops;
            if (state.getLifecycle() != RefLifecycle::Live)
                ops.push_back(namespaceBirthOp());
            for (const RefOp & op : publishCommittedOps("a", manifestRef(1, 1, 1)))
                ops.push_back(op);
            return ops;
        }, RootMutationOrigin::Writer, RootMutationKind::Publish));

    const NamespaceLifeId life = catalogLife(*backend, layout, ns);
    const String ckpt_key = layout.refCkptKey(life);
    backend->ambiguous_cas_substr = ckpt_key;
    backend->ambiguous_cas_count = 200;
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&]
    {
        store->appendRefOps(ns, MutationScope::ref("b"),
            [](const RefTableState &) { return publishCommittedOps("b", manifestRef(1, 2, 1)); },
            RootMutationOrigin::Writer, RootMutationKind::Publish);
    });
    ASSERT_EQ(store->laneStateForTest(ns), RefLaneState::NeedsRecovery);

    const RefLogTxn second_successor = makeOrdinaryTxn(ns, RefTxnId{1, 3}, "c", /*birth=*/false);
    ASSERT_EQ(backend->putIfAbsent(layout.refLogKey(life, second_successor.txn_id),
                                  sealObject(FormatId::RefLog, encodeRefLogTxn(second_successor))).outcome,
              PutOutcome::Done);
    backend->ambiguous_cas_count = 0;

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { (void)store->listRefs(ns); });
    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::NeedsRecovery);
    EXPECT_EQ(readCkpt(*backend, layout, life)->ckpt.committed_through, (RefTxnId{1, 1}))
        << "corruption must not launder either successor into the frontier";
}

TEST(CASRefRecoveryCasWalk, WriterRecoveryRejectsDifferentOrdinaryBytesAtTheRetainedSuccessorSlot)
{
    auto backend = std::make_shared<HidingListBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/recovery_different_successor"};
    auto store = openWalkPool(backend);
    const NamespaceLifeId life = strandOneUnfrontieredSuccessor(*backend, store, layout, ns);
    ASSERT_EQ(store->laneStateForTest(ns), RefLaneState::NeedsRecovery);

    const String successor_key = layout.refLogKey(life, RefTxnId{1, 2});
    const auto original = backend->get(successor_key);
    ASSERT_TRUE(original);
    const RefLogTxn different = makeOrdinaryTxn(ns, RefTxnId{1, 2}, "different", /*birth=*/false);
    ASSERT_EQ(backend->putOverwrite(successor_key,
                                   sealObject(FormatId::RefLog, encodeRefLogTxn(different)),
                                   original->token).outcome,
              PutOutcome::Done);

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { (void)store->listRefs(ns); });
    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::NeedsRecovery);
    EXPECT_EQ(readCkpt(*backend, layout, life)->ckpt.committed_through, (RefTxnId{1, 1}));
}

TEST(CASRefRecoveryCasWalk, RetainedOldWriterAttemptLosesConclusiveToASuccessorSeal)
{
    auto backend = std::make_shared<HidingListBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/recovery_successor_seal"};
    auto store = openWalkPool(backend);
    const NamespaceLifeId life = strandOneUnfrontieredSuccessor(*backend, store, layout, ns);
    ASSERT_EQ(store->laneStateForTest(ns), RefLaneState::NeedsRecovery);

    const RefTxnId successor_id{1, 2};
    const String successor_key = layout.refLogKey(life, successor_id);
    const auto original = backend->get(successor_key);
    ASSERT_TRUE(original);
    const RefLogTxn successor_seal = makeSealTxn(ns, successor_id);
    ASSERT_EQ(backend->putOverwrite(successor_key,
                                   sealObject(FormatId::RefLog, encodeRefLogTxn(successor_seal)),
                                   original->token).outcome,
              PutOutcome::Done);

    const auto refs = store->listRefs(ns);

    EXPECT_FALSE(refs.contains("b")) << "the old writer's retained ordinary bytes lost at the sealed slot";
    EXPECT_EQ(store->lastEpochSealForTest(ns), std::make_optional(successor_id));
    EXPECT_EQ(readCkpt(*backend, layout, life)->ckpt.committed_through, successor_id);
    EXPECT_EQ(readCkpt(*backend, layout, life)->ckpt.last_epoch_seal, successor_id);
    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Ready);
}

/// ---------------------------------------------------------------------------------------------
/// Fail-closed on an unresolved slot
/// ---------------------------------------------------------------------------------------------

/// `Unresolved` from the slot-occupy means the store will not say whether our seal landed. That is not a
/// state to guess about: recovery takes the transient-retry path and, once its budget is spent, fails
/// closed with the table left unrecovered. Exposing a table whose dead epoch may or may not be closed is
/// the one outcome that must be impossible.
TEST(CASRefRecoveryCasWalk, UnresolvedSealSlotFailsClosedWithoutInstalling)
{
    auto backend = std::make_shared<HidingListBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/unresolved"};

    burnEpochsUpTo(*backend, layout, /*target_live_epoch=*/2);
    seedCkpt(*backend, layout, ns, lifeEpochCkpt(1, RefTxnId{1, 1}));
    seedTxn(*backend, layout, ns, RefTxnId{1, 1}, "a", /*birth=*/true);

    /// The backoff sleep ADVANCES the same fake clock the budget is measured against, so the retry
    /// envelope is spent in a handful of iterations instead of spinning against a frozen clock. Not
    /// cosmetic: with a frozen clock this test burns ~700k retries and the same number of log lines,
    /// which is how a real regression in this arm would become invisible in the noise.
    uint64_t fake_now = 1'000'000;
    PoolConfig config = walkTestConfig();
    config.boot_ms_fn = [&fake_now] { return fake_now; };
    auto store = openWalkPool(backend, config);
    ASSERT_TRUE(store);

    store->setCasRetrySleepForTest([&fake_now](uint64_t ms) { fake_now += ms; });
    backend->ambiguous_put_substr = "/_log/";

    EXPECT_ANY_THROW(store->listRefs(ns));
    EXPECT_FALSE(store->refTableRecoveredForTest(ns))
        << "a table whose dead epoch may or may not be closed must never be exposed as recovered";
}

/// ---------------------------------------------------------------------------------------------
/// Carried forward from the retired `RefWriterRecoverySeal` suite
/// ---------------------------------------------------------------------------------------------

/// THE property the whole in-band design exists for, and the one the retired suite could only
/// approximate with a detector: the Late Predecessor PUT is REFUSED, by the store, at the key it wanted.
///
/// A dying writer of epoch 1 has an append in flight for `{1,2}`. Recovery closes epoch 1 by occupying
/// exactly that slot. When the ghost's conditional create finally reaches the store there is nothing for
/// it to do -- the key is write-once and taken. The old sentinel seal was a SNAPSHOT at a synthetic id,
/// which left `{1,2}` free: the ghost landed, and all anyone could do was notice afterwards.
TEST(CASRefRecoveryCasWalk, ALatePredecessorPutAtTheSealedSlotIsRefusedByTheStore)
{
    auto backend = std::make_shared<HidingListBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/ghost"};

    burnEpochsUpTo(*backend, layout, /*target_live_epoch=*/2);
    seedCkpt(*backend, layout, ns, lifeEpochCkpt(1, RefTxnId{1, 1}));
    seedTxn(*backend, layout, ns, RefTxnId{1, 1}, "a", /*birth=*/true);

    auto store = openWalkPool(backend);
    ASSERT_TRUE(store);
    ASSERT_EQ(store->listRefs(ns).size(), 1u);

    /// The ghost: the exact append the dead epoch's writer had in flight, arriving late.
    const RefTxnId ghost_id{1, 2};
    const String ghost_bytes = sealObject(FormatId::RefLog,
        encodeRefLogTxn(makeOrdinaryTxn(ns, ghost_id, "ghost", /*birth=*/false)));
    const PutResult put = backend->putIfAbsent(layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), ghost_id), ghost_bytes);
    EXPECT_EQ(put.outcome, PutOutcome::PreconditionFailed)
        << "the seal occupies the ghost's own key, so the store itself is the fence";

    /// And the object at that key is still the seal, byte for byte -- nothing adopted the ghost.
    const auto occupant = readLogTxn(*backend, layout, ns, ghost_id);
    ASSERT_TRUE(occupant.has_value());
    EXPECT_TRUE(refLogTxnIsEpochSeal(*occupant));
}

/// An occupant at the seal slot that this build cannot decode is NOT a straggler to adopt and NOT a
/// peer's seal to defer to: it is an object at a key this namespace exclusively owns whose meaning is
/// unknown. Recovery fails closed on it -- and, just as importantly, stays RESTARTABLE: the throw must
/// leave `recovery_in_progress` cleared, or the table would be unrecoverable for the mount's life and
/// every later toucher would park forever on a condition variable nobody will signal.
TEST(CASRefRecoveryCasWalk, UndecodableOccupantAtTheSealSlotFailsClosedAndLeavesRecoveryRestartable)
{
    auto backend = std::make_shared<LateMaterializeBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/foreign_slot"};

    burnEpochsUpTo(*backend, layout, /*target_live_epoch=*/2);
    seedCkpt(*backend, layout, ns, lifeEpochCkpt(1, RefTxnId{1, 1}));
    seedTxn(*backend, layout, ns, RefTxnId{1, 1}, "a", /*birth=*/true);
    backend->late_key = layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), RefTxnId{1, 2});
    backend->late_bytes = "not a ref-log object at all";

    auto store = openWalkPool(backend);
    ASSERT_TRUE(store);

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { store->listRefs(ns); });
    EXPECT_FALSE(store->refTableRecoveredForTest(ns));

    /// Restartable: a second touch runs a WHOLE new attempt (it fails the same way, which is the point --
    /// it reaches the failure again rather than hanging on a stuck `recovery_in_progress`).
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { store->listRefs(ns); });
}

/// A second caller that arrives while a recovery is mid-walk WAITS for it rather than racing an
/// independent walk of its own. Two concurrent walks would both try to occupy the same seal slot, and
/// while the loser adopts correctly, they would also both replay the whole tail and one would install a
/// state the other's install immediately replaces -- work and I/O for nothing, on the path that is
/// already the most expensive one in the system.
TEST(CASRefRecoveryCasWalk, ASecondCallerWaitsForTheWalkInsteadOfRacingIt)
{
    auto backend = std::make_shared<GetSeamBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/serialized"};

    burnEpochsUpTo(*backend, layout, /*target_live_epoch=*/2);
    seedCkpt(*backend, layout, ns, lifeEpochCkpt(1, RefTxnId{1, 1}));
    seedTxn(*backend, layout, ns, RefTxnId{1, 1}, "a", /*birth=*/true);

    auto store = openWalkPool(backend);
    ASSERT_TRUE(store);

    std::mutex m;
    std::condition_variable cv;
    bool parked = false;
    bool release = false;

    backend->watched_substr = "_log/";
    backend->on_key = [&](const String &)
    {
        std::unique_lock lock(m);
        parked = true;
        cv.notify_all();
        cv.wait(lock, [&] { return release; });
    };

    const uint64_t adopted_before = counterOf(ProfileEvents::CASRefRecoveryEpochSealAdopted);
    std::thread first([&] { store->listRefs(ns); });
    {
        std::unique_lock lock(m);
        cv.wait(lock, [&] { return parked; });
    }

    /// The second caller blocks on `recovery_in_progress`. Its own recovery would have to LIST, and the
    /// walk holds no lock while parked, so nothing but the serialization flag can be keeping it out.
    std::atomic<bool> second_done{false};
    std::thread second([&] { store->listRefs(ns); second_done.store(true); });
    for (int i = 0; i < 50 && !second_done.load(); ++i)
        std::this_thread::yield();
    EXPECT_FALSE(second_done.load()) << "a second caller must wait out the in-flight walk, not race it";

    {
        std::lock_guard lock(m);
        release = true;
    }
    cv.notify_all();
    first.join();
    second.join();

    EXPECT_TRUE(store->refTableRecoveredForTest(ns));
    /// Exactly ONE walk minted the seal, and no second walk ever met it as an occupant. Adopting is the
    /// CORRECT outcome for a concurrent recoverer -- it is just work this serialization exists to avoid
    /// paying inside one process, so observing zero adoptions is what proves the second caller waited.
    EXPECT_EQ(counterOf(ProfileEvents::CASRefRecoveryEpochSealAdopted), adopted_before);
    const auto seal = readLogTxn(*backend, layout, ns, RefTxnId{1, 2});
    ASSERT_TRUE(seal.has_value());
    EXPECT_TRUE(refLogTxnIsEpochSeal(*seal));
}

/// Checkpoint-grounded recovery starts at the recreated life's own genesis and does not replay or
/// extend the predecessor life's stream.
///
/// The old same-stream fixture claimed that recovery walked through the epoch-1 removal into epoch 2.
/// With authoritative `_ckpt.life_epoch=2`, epoch 2 is instead the current life's genesis and the walk
/// begins at `{2,1}`. Epoch-1 objects are inert predecessor-life debris: they neither supply state nor
/// receive a recovery seal.
TEST(CASRefRecoveryCasWalk, RecoveryStartsAtRecreatedLifeGenesisAndLeavesPredecessorStreamUntouched)
{
    auto backend = std::make_shared<HidingListBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/removed_then_reborn"};

    burnEpochsUpTo(*backend, layout, /*target_live_epoch=*/3);
    seedCkpt(*backend, layout, ns, lifeEpochCkpt(2, RefTxnId{2, 1}));
    seedTxn(*backend, layout, ns, RefTxnId{1, 1}, "a", /*birth=*/true);

    /// Epoch 1 ends with the terminal record: the ref is removed, then the namespace.
    RefLogTxn removal;
    removal.ns = ns.string();
    removal.txn_id = RefTxnId{1, 2};
    removal.ops = {DB::Cas::tests::ownerTransitionOp(
                       RefOwnerBinding{RefOwnerKind::Committed, "a", manifestRef(1, 1, 1u)}, std::nullopt),
                   removeNamespaceOp()};
    DB::Cas::tests::fixture::writeRefLogRaw(*backend, layout, removal);

    /// The current life starts in epoch 2. Its birth is sequence 1 of its own genesis epoch, so it
    /// carries no chain link to the predecessor life.
    seedTxn(*backend, layout, ns, RefTxnId{2, 1}, "reborn", /*birth=*/true);

    auto store = openWalkPool(backend);
    ASSERT_TRUE(store);
    ASSERT_EQ(store->liveWriterEpoch(), 3u);

    const auto refs = store->listRefs(ns);
    EXPECT_EQ(refs.size(), 1u);
    EXPECT_TRUE(refs.contains("reborn")) << "recovery must begin at the recreated life's genesis";

    EXPECT_FALSE(readLogTxn(*backend, layout, ns, RefTxnId{1, 3}).has_value())
        << "recovery of life epoch 2 must not extend the predecessor-life stream";
    const auto seal2 = readLogTxn(*backend, layout, ns, RefTxnId{2, 2});
    ASSERT_TRUE(seal2.has_value()) << "epoch 2 IS live again by the time it dies, so it closes normally";
    EXPECT_TRUE(refLogTxnIsEpochSeal(*seal2));
    EXPECT_EQ(seal2->prev_epoch_seal, std::nullopt) << "sequence 2 carries no chain link";
}

/// `PutHookBackend::casPut` must route through its immediate parent `HidingListBackend::casPut`, not
/// past it to `CountingBackend`, so that a test arming BOTH layers on one `PutHookBackend` instance
/// gets both behaviors composed rather than one silently disabled by the other.
TEST(CASRefRecoveryCasWalk, PutHookBackendComposesHidingListBackendCasPutFaultInjection)
{
    auto backend = std::make_shared<PutHookBackend>();

    bool before_cas_put_fired = false;
    backend->before_cas_put = [&](const String &, const String &, const std::optional<Token> &)
    {
        before_cas_put_fired = true;
    };

    backend->watched_substr = "probe";
    bool on_key_fired = false;
    backend->on_key = [&] { on_key_fired = true; };

    ASSERT_EQ(backend->casPut("p/probe", "x", std::nullopt).outcome, CasOutcome::Committed);

    EXPECT_TRUE(before_cas_put_fired)
        << "HidingListBackend's before_cas_put hook must still fire for a PutHookBackend instance";
    EXPECT_TRUE(on_key_fired) << "PutHookBackend's own on_key hook must still fire on top of it";
}
