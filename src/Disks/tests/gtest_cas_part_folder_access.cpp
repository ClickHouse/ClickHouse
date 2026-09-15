#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Parts/PartFolderAccess.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEvent.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/ProfileEvents.h>
#include <Poco/Exception.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <latch>
#include <thread>
#include <type_traits>
#include <vector>

namespace DB::ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int ABORTED;
    extern const int BAD_ARGUMENTS;
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int NETWORK_ERROR;
}

namespace ProfileEvents
{
extern const Event CASRefRollbackBestEffortDropFailed;
}

using namespace DB;
using namespace DB::Cas::tests;

namespace
{

Cas::ManifestEntry inlineEntry(const String & path, const String & bytes)
{
    Cas::ManifestEntry e;
    e.path = path;
    e.placement = Cas::EntryPlacement::Inline;
    e.ref = DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(u128Of(bytes))};

    e.blob_size = bytes.size();
    e.inline_bytes = bytes;
    return e;
}

/// Publish `entries` as committed ref `ns/ref` through the real writer protocol.
Cas::ManifestId publishPart(const Cas::PoolPtr & store, const Cas::RootNamespace & ns,
                            const String & ref, std::vector<Cas::ManifestEntry> entries)
{
    auto build = store->beginPartWrite(Cas::PartWriteInfo{.intended_ref = ns.string() + "/" + ref,
                                                  .intended_namespace = ns, .op = Cas::ProvenanceOp::Insert});
    const Cas::ManifestId id = build->stageManifest(entries);
    build->precommitAdd(ns, ref, id);
    build->promote(ns, ref, build->buildId(), id);
    return id;
}

Cas::CachedPartFolderAccess::CacheParams cacheOn()
{
    return {.cache_bytes = 64ULL << 20, .max_entries = 10000, .max_entry_bytes = 16ULL << 20,
            .explain_enabled = true};
}

/// Every mutating backend op throws once armed — models a correlated backend outage during the
/// transaction's compensating rollback (dropRef must append a removal, which mutates the backend).
/// While armed, the store is unreachable for every mutation: a transport-class failure, so the request
/// engine settles it by a read (which fails too) and reissues until the call's own retry window closes.
/// A test arming it therefore drives the engine's clock, or pays that window in real time.
class RollbackFaultBackend final : public Cas::InMemoryBackend
{
public:
    std::atomic<bool> armed{false};

    std::expected<String, RawConflict> write(const String & key, const String & bytes,
                                             const std::optional<String> & expected_value,
                                             Cas::TransportAccess & access) override
    {
        failIfArmed();
        return InMemoryBackend::write(key, bytes, expected_value, access);
    }

    RawRemoval remove(const String & key, const String & expected_value, Cas::TransportAccess & access) override
    {
        failIfArmed();
        return InMemoryBackend::remove(key, expected_value, access);
    }

private:
    void failIfArmed()
    {
        if (armed.load())
            throw Exception(ErrorCodes::ABORTED, "injected backend outage");
    }
};

/// Task 7 (`publishEntries` abandons its build on exception): forces publishEntries's PROMOTE step
/// specifically -- not the earlier stageManifest/precommitAdd writes -- to observe a proven ref-log
/// conflict. `skip` lets the FIRST matching '_log/' PUT (precommitAdd's OwnerTransition-to-Precommit)
/// land normally; the fault then fires on the SECOND (promote's atomic precommit->committed move).
/// Mirrors `RefWriterTestBackend::corrupt_key_substr` (gtest_cas_ref_writer.cpp, reproduced locally
/// because that class lives in a different translation unit): landing a DIFFERENT object at the
/// intended key makes `putIfAbsentControlled`'s resolve-before-reissue observe a proven conflict
/// (CORRUPTED_DATA) rather than the ambiguous-timeout shape, which would instead wedge the whole
/// table's append lane.
class PromoteConflictOnceBackend final : public Cas::InMemoryBackend
{
public:
    String fault_key_substr;
    int skip = 0;
    int fault_count = 0;
    /// Every create ATTEMPTED at a matching key, faulted or not. It is how a test observes that a
    /// cleanup path ran its ref-log append at all, on a table where that append can no longer succeed.
    int matching_put_attempts = 0;

    /// Sabotages the sole write primitive, so the fault fires whichever verb (`create`/`replace`) issued it.
    std::expected<String, Cas::Backend::RawConflict> write(const String & key, const String & bytes,
                                                            const std::optional<String> & expected_value,
                                                            Cas::TransportAccess & access) override
    {
        if (!expected_value && !fault_key_substr.empty() && key.find(fault_key_substr) != String::npos)
        {
            ++matching_put_attempts;
            if (skip > 0)
                --skip;
            else if (fault_count > 0)
            {
                --fault_count;
                /// The qualified call bypasses virtual dispatch entirely (unlike a re-entrant call
                /// through the vtable), landing a foreign object at the key before the response is lost.
                InMemoryBackend::write(key, bytes + String("\x01_FOREIGN_DIFFERENT"), expected_value, access);
                throw Poco::TimeoutException("PromoteConflictOnceBackend: a foreign different object landed; response lost");
            }
        }
        return InMemoryBackend::write(key, bytes, expected_value, access);
    }
};

/// The same shape as `PromoteConflictOnceBackend`, except its fault is a whitelisted SYNCHRONOUS
/// REJECTION -- an S3-classified malformed request, which `classifyConditionalWriteResult` proves was
/// never applied. That distinction is the whole reason this second backend exists: a proven DIFFERENT
/// OBJECT is a breach of mount write-exclusivity and fences the whole mount closed, so every cleanup
/// append after it is refused at the gate and becomes unobservable. A definite rejection is an ordinary
/// failed write -- nothing is fenced, nothing is wedged, the table stays usable -- so the cleanup
/// appends that follow DO reach the store and can be counted.
class PromoteDefiniteFailureBackend final : public Cas::InMemoryBackend
{
public:
    String fault_key_substr;
    int skip = 0;
    int fault_count = 0;
    int matching_put_attempts = 0;

    /// Sabotages the sole write primitive, so the fault fires whichever verb (`create`/`replace`) issued it.
    std::expected<String, Cas::Backend::RawConflict> write(const String & key, const String & bytes,
                                                            const std::optional<String> & expected_value,
                                                            Cas::TransportAccess & access) override
    {
        if (!expected_value && !fault_key_substr.empty() && key.find(fault_key_substr) != String::npos)
        {
            ++matching_put_attempts;
            if (skip > 0)
                --skip;
            else if (fault_count > 0)
            {
                --fault_count;
                throw DB::S3Exception("PromoteDefiniteFailureBackend: simulated malformed request",
                                      Aws::S3::S3Errors::UNKNOWN, "MalformedXML");
            }
        }
        return InMemoryBackend::write(key, bytes, expected_value, access);
    }
};

}

TEST(CASPartFolderAccess, RetainedHitCostsNoRequest)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend);
    const Cas::Layout layout("p");
    const Cas::RootNamespace ns{"srv/t1"};
    Cas::CachedPartFolderAccess access(store, cacheOn());
    const auto id = publishPart(store, ns, "part_1", {inlineEntry("checksums.txt", "cs")});
    const Cas::PartRefKey key{ns, "part_1"};
    const String manifest_key = layout.manifestKey(id);

    /// Cold build: warms the retained view and the decode cache. Excluded from the counts below so
    /// they measure only the warm hits that follow.
    ASSERT_NE(access.getView(key, Cas::Freshness::CachedForLoad), nullptr);

    backend->resetCounts();
    for (int i = 0; i < 4; ++i)
        ASSERT_NE(access.getView(key, Cas::Freshness::CachedForLoad), nullptr);

    /// A retained hit costs no request at all -- not merely no manifest GET on this key, but no
    /// backend traffic of ANY kind (GET, HEAD, LIST, streamed GET) against ANY key.
    EXPECT_EQ(backend->getCount(manifest_key), 0u);
    EXPECT_EQ(backend->getTotal(), 0u);
    EXPECT_EQ(backend->headTotal(), 0u);
    EXPECT_EQ(backend->listTotal(), 0u);
    EXPECT_EQ(backend->getStreamTotal(), 0u);
    EXPECT_TRUE(access.explain(key).retained);
    EXPECT_EQ(access.explain(key).last_decision,
              Cas::CachedPartFolderAccess::LastDecision::Hit);
}

TEST(CASPartFolderAccess, HitPathJournalEmptyAndCheapWhenExplainDisabled)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend);
    const Cas::Layout layout("p");
    const Cas::RootNamespace ns{"srv/t1"};
    /// Retention ON, explain journal OFF (the production default): the hit path must take neither the
    /// per-disk explain mutex nor write a journal entry (B2).
    Cas::CachedPartFolderAccess access(store,
        {.cache_bytes = 64ULL << 20, .max_entries = 10000, .max_entry_bytes = 16ULL << 20,
         .explain_enabled = false});
    const auto id = publishPart(store, ns, "part_1", {inlineEntry("checksums.txt", "cs")});
    const Cas::PartRefKey key{ns, "part_1"};
    const String manifest_key = layout.manifestKey(id);

    backend->resetCounts();
    for (int i = 0; i < 5; ++i)
        ASSERT_NE(access.getView(key, Cas::Freshness::CachedForLoad), nullptr);

    /// Same request oracle as RetainedHitCostsNoRequest — one cold build, then retained hits.
    EXPECT_EQ(backend->getCount(manifest_key), 1u);
    /// The journal is never written when disabled.
    EXPECT_EQ(access.explainJournalSizeForTest(), 0u);
    /// explain() still reports live retention truthfully, but the decision defaults to Miss (unwritten).
    EXPECT_TRUE(access.explain(key).retained);
    EXPECT_EQ(access.explain(key).last_decision,
              Cas::CachedPartFolderAccess::LastDecision::Miss);
}

TEST(CASPartFolderAccess, GetViewServesCommittedFolder)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend);
    const Cas::RootNamespace ns{"srv/t1"};
    publishPart(store, ns, "part_1",
                {inlineEntry("checksums.txt", "cs"), inlineEntry("count.txt", "1"), inlineEntry("txn_version.txt", "v1")});

    Cas::CachedPartFolderAccess access(store);
    const Cas::PartRefKey key{ns, "part_1"};

    auto view = access.getView(key, Cas::Freshness::CachedForLoad);
    ASSERT_NE(view, nullptr);
    EXPECT_NE(view->findFile("checksums.txt"), nullptr);
    EXPECT_EQ(view->inlineBytes("txn_version.txt"), std::optional<String>("v1"));

    /// Absent ref => nullptr, never an exception, never retained (nothing to retain in Phase 2).
    EXPECT_EQ(access.getView({ns, "absent"}, Cas::Freshness::CachedForLoad), nullptr);
    EXPECT_TRUE(access.existsRef(key, Cas::Freshness::CachedForLoad));
    EXPECT_FALSE(access.existsRef({ns, "absent"}, Cas::Freshness::ForceFresh));
    ASSERT_TRUE(access.resolve(key, Cas::Freshness::ForceFresh).has_value());
}

TEST(CASPartFolderAccess, GetViewFailsClosedOnMissingBody)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend);
    const Cas::Layout layout("p");
    const Cas::RootNamespace ns{"srv/t1"};
    const auto id = publishPart(store, ns, "part_1", {inlineEntry("checksums.txt", "cs")});

    /// Physically delete the live manifest body (a protocol violation). Retention is off and the
    /// decode cache is cold (promote reads the body through the backend, not the reader), so every
    /// getView mode reaches the reader's miss path: one GET, no HEAD, FILE_DOESNT_EXIST.
    deleteManifestBody(*backend, layout, id);
    backend->resetCounts();

    Cas::CachedPartFolderAccess access(store);
    const Cas::PartRefKey key{ns, "part_1"};
    for (auto freshness : {Cas::Freshness::CachedForLoad,
                           Cas::Freshness::ForceFresh,
                           Cas::Freshness::StrictValidate})
        expectThrowsCode(ErrorCodes::FILE_DOESNT_EXIST, [&] { access.getView(key, freshness); });
    EXPECT_EQ(backend->headCount(layout.manifestKey(id)), 0u);
}

TEST(CASPartFolderAccess, WritePrimitivesRoundTrip)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend);
    const Cas::RootNamespace ns{"srv/t1"};
    Cas::CachedPartFolderAccess access(store);
    const Cas::PartRefKey key{ns, "part_1"};

    /// promoteBuild: the transaction's terminal publish step, through the facade.
    auto build = store->beginPartWrite(Cas::PartWriteInfo{.intended_ref = ns.string() + "/part_1",
                                                  .intended_namespace = ns, .op = Cas::ProvenanceOp::Insert});
    const Cas::ManifestId id = build->stageManifest({inlineEntry("checksums.txt", "cs")});
    build->precommitAdd(ns, "part_1", id);
    access.promoteBuild(*build, key, build->buildId(), id);
    ASSERT_TRUE(access.existsRef(key, Cas::Freshness::ForceFresh));

    /// dropRefIfPresent: replay-safe (absent ref is success, not failure).
    access.dropRefIfPresent(key);
    EXPECT_FALSE(access.existsRef(key, Cas::Freshness::ForceFresh));
    access.dropRefIfPresent(key);                              /// second drop: no-op, no throw
    access.dropRefBestEffort(key);                             /// noexcept even when absent

    /// dropNamespace clears the whole namespace.
    publishPart(store, ns, "part_2", {inlineEntry("checksums.txt", "cs")});
    access.dropNamespace(ns);
    EXPECT_FALSE(access.existsRef({ns, "part_2"}, Cas::Freshness::ForceFresh));
}

TEST(CASPartFolderAccess, RepublishRefMovesCommittedRef)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend);
    const Cas::RootNamespace ns{"srv/t1"};
    Cas::CachedPartFolderAccess access(store);
    publishPart(store, ns, "src_part", {inlineEntry("checksums.txt", "cs"), inlineEntry("txn_version.txt", "v1")});

    EXPECT_FALSE(access.republishRef({ns, "absent"}, {ns, "dst"}));   /// absent source: nothing written

    ASSERT_TRUE(access.republishRef({ns, "src_part"}, {ns, "dst_part"}));
    EXPECT_FALSE(access.existsRef({ns, "src_part"}, Cas::Freshness::ForceFresh));
    auto view = access.getView({ns, "dst_part"}, Cas::Freshness::ForceFresh);
    ASSERT_NE(view, nullptr);
    EXPECT_NE(view->findFile("checksums.txt"), nullptr);
    EXPECT_EQ(view->inlineBytes("txn_version.txt"), std::optional<String>("v1"));   /// carried over
}

TEST(CASPartFolderAccess, RepublishRefIdempotentRedriveAndConflict)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend);
    const Cas::RootNamespace ns{"srv/t1"};
    Cas::CachedPartFolderAccess access(store);

    /// Re-drive: dst already committed with the SAME content (a prior attempt's promote landed,
    /// only dropRef(src) was interrupted) -- idempotent-skip: drop src, dst's manifest is untouched
    /// (all-tree-part-files Task 9: there is no separate mutable payload left to drift/re-sync --
    /// identical `entries` is the whole idempotency contract now).
    publishPart(store, ns, "src", {inlineEntry("f", "same")});
    publishPart(store, ns, "dst", {inlineEntry("f", "same")});
    const auto dst_id_before = access.resolve({ns, "dst"}, Cas::Freshness::ForceFresh)->manifest_id;
    ASSERT_TRUE(access.republishRef({ns, "src"}, {ns, "dst"}));
    EXPECT_FALSE(access.existsRef({ns, "src"}, Cas::Freshness::ForceFresh));
    auto resolved = access.resolve({ns, "dst"}, Cas::Freshness::ForceFresh);
    EXPECT_EQ(resolved->manifest_id, dst_id_before) << "idempotent re-drive must not mint a fresh manifest";

    /// Conflict: dst committed with DIFFERENT content — fail closed, src untouched.
    publishPart(store, ns, "src2", {inlineEntry("f", "one")});
    publishPart(store, ns, "dst2", {inlineEntry("f", "two")});
    expectThrowsCode(ErrorCodes::ABORTED, [&] { access.republishRef({ns, "src2"}, {ns, "dst2"}); });
    EXPECT_TRUE(access.existsRef({ns, "src2"}, Cas::Freshness::ForceFresh));
}

/// Task 7: `publishEntries`'s `catch (...) { build->abandon(); throw; }` must leave no live-epoch
/// precommit binding behind when its promote fails -- only `abandon()` removes it (the build
/// destructor merely retires the build seq; GC never touches a live precommit). Drives the failure
/// through `republishRef` -> `publishEntries`, with the fault isolated to promote's own ref-log
/// append (precommitAdd's own append is let through first via `skip`).
TEST(CASPartFolderAccess, PublishEntriesAbandonsBuildOnPromoteFailure)
{
    auto backend = std::make_shared<PromoteConflictOnceBackend>();
    auto store = Cas::Pool::open(backend, Cas::PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    const Cas::RootNamespace ns{"srv/t1"};
    DB::Cas::tests::casAdmitRecoverableEntry(*backend, store->layout(), ns, store->liveWriterEpoch());
    Cas::CachedPartFolderAccess access(store);

    publishPart(store, ns, "src", {inlineEntry("f", "same")});

    backend->fault_key_substr = store->layout().namespaceStreamPrefix(fixture::fixtureLife(ns)) + "_log/";
    backend->skip = 1;         /// let precommitAdd's own ref-log append land normally
    backend->fault_count = 1;  /// fault exactly promote's ref-log append
    const int attempts_before = backend->matching_put_attempts;

    /// republishRef(src, dst) drives publishEntries(dst, ...): precommitAdd succeeds, promote's
    /// appendRefOps observes a proven conflict and throws CORRUPTED_DATA -- publishEntries's catch must
    /// abandon() the build before rethrowing.
    expectThrowsCode(ErrorCodes::CORRUPTED_DATA, [&] { access.republishRef({ns, "src"}, {ns, "dst"}); });
    /// The anomaly fenced this runtime, so a post-fence `ForceFresh` read must refuse rather than
    /// authorizing its stale generation. The backend assertions below prove directly that `dst` never
    /// committed and that no append skipped around the damaged slot.
    expectThrowsCode(ErrorCodes::NETWORK_ERROR,
        [&] { (void)access.existsRef({ns, "dst"}, Cas::Freshness::ForceFresh); });

    /// EXACTLY two ref-log create attempts reach the store: precommitAdd's own append and the promote's
    /// faulted one. Both of the cleanup appends that follow -- `promote`'s catch-abandon and the handle
    /// destructor's backstop -- are refused at the mount-fence gate before they reach the store, because
    /// proving a different object at our own key now fences the mount closed and schedules a remount
    /// (review I5: the append site self-heals like the wedge-resolve site instead of leaving this table
    /// blocked until a manual remount).
    ///
    /// COVERAGE NOTE, deliberately explicit: this count no longer DISCRIMINATES whether the catch-abandon
    /// ran. It used to (four attempts with it, three without), and that only worked because the
    /// catch-abandon could still reach the store and fail there, making the destructor retry. With the
    /// fence closed both cleanups are refused identically and unobservably, so the assertion below is a
    /// shape check, not the regression guard it was. The guard cannot be restored in THIS scenario --
    /// nothing the cleanup does is observable once the mount is fenced -- and it is not silently
    /// dropped: the property it protected is stated here, and reclaiming the binding is now the
    /// scheduled remount's job (a fresh incarnation re-derives the table and the stale-precommit sweep
    /// reclaims), not this best-effort abandon's.
    EXPECT_EQ(backend->matching_put_attempts, attempts_before + 2)
        << "only precommitAdd's append and the promote's faulted one may reach the store; every cleanup "
           "append after the anomaly is refused at the fence";
    EXPECT_FALSE(store->mayMutate()) << "the proven conflict must fence this mount closed";
    EXPECT_EQ(store->scheduleRemountCallCountForTest(), 1u)
        << "and must schedule exactly one remount -- the self-heal that replaces the manual one";

    /// And nothing was written ABOVE the damage: the occupant is the GREATEST log id in the namespace
    /// (keys render the id in fixed-width hex, so lexical order is id order). An append that carved a
    /// fresh id to get past the foreign object would sort above it.
    String greatest_key;
    size_t foreign_objects = 0;
    DB::Cas::tests::OperationForTest scan(*backend);
    for (String cursor;;)
    {
        const Cas::ListPage page = (*scan).list(backend->fault_key_substr, cursor, 1000, Cas::Retry::standard());
        for (const auto & listed : page.keys)
        {
            if (listed.key > greatest_key)
                greatest_key = listed.key;
            const auto body = (*scan).read(listed.key, Cas::Retry::standard());
            if (body && body->bytes.find("_FOREIGN_DIFFERENT") != String::npos)
                ++foreign_objects;
        }
        if (page.next_cursor.empty())
            break;
        cursor = page.next_cursor;
    }
    EXPECT_EQ(foreign_objects, 1u) << "the foreign object must still own the key it took";
    ASSERT_FALSE(greatest_key.empty());
    const auto greatest_body = (*scan).read(greatest_key, Cas::Retry::standard());
    ASSERT_TRUE(greatest_body.has_value());
    EXPECT_NE(greatest_body->bytes.find("_FOREIGN_DIFFERENT"), String::npos)
        << "the foreign occupant must still be the highest id in this table's stream: a log object above "
           "it would mean an append carved a fresh id around the damage instead of failing closed";
}

/// The DISCRIMINATING guard for the same duty, on the path where it can still be observed: a promote
/// failure that is an ordinary failed write rather than a breach of mount write-exclusivity. Nothing is
/// fenced and nothing is wedged, so both cleanup appends reach the store and the two worlds separate.
///
/// The fault covers TWO appends, and that is the whole construction:
///   with `promote`'s catch-abandon -- precommitAdd lands (skipped), promote's append is refused,
///   the catch-abandon's append is refused too, and the handle DESTRUCTOR's backstop retries and lands:
///   FOUR attempts, and no binding is left behind;
///   without it -- precommitAdd lands, promote's append is refused, and the destructor's backstop takes
///   the second fault and is refused: THREE attempts, and the precommit binding LEAKS.
/// So the count and the end state disagree between the two worlds, which is what makes this a guard
/// rather than a shape check. `livePrecommitsForTest` is the direct statement of the property --
/// `publishEntries` must not walk away from a live precommit binding -- and the count is what pins
/// WHERE the cleanup came from.
TEST(CASPartFolderAccess, PublishEntriesAbandonsBuildOnARetryablePromoteFailure)
{
    auto backend = std::make_shared<PromoteDefiniteFailureBackend>();
    auto store = Cas::Pool::open(backend, Cas::PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    const Cas::RootNamespace ns{"srv/t1"};
    DB::Cas::tests::casAdmitRecoverableEntry(*backend, store->layout(), ns, store->liveWriterEpoch());
    Cas::CachedPartFolderAccess access(store);

    publishPart(store, ns, "src", {inlineEntry("f", "same")});

    backend->fault_key_substr = store->layout().namespaceStreamPrefix(fixture::fixtureLife(ns)) + "_log/";
    backend->skip = 1;         /// let precommitAdd's own ref-log append land normally
    backend->fault_count = 2;  /// fault promote's append AND the cleanup append that follows it
    const int attempts_before = backend->matching_put_attempts;

    /// A definite rejection is reported to the caller as a retry-later failure, not as corruption.
    expectThrowsCode(ErrorCodes::NETWORK_ERROR, [&] { access.republishRef({ns, "src"}, {ns, "dst"}); });
    EXPECT_FALSE(access.existsRef({ns, "dst"}, Cas::Freshness::ForceFresh)) << "the failed promote never committed dst";

    EXPECT_TRUE(store->mayMutate()) << "an ordinary failed write must not fence the mount";
    EXPECT_EQ(store->scheduleRemountCallCountForTest(), 0u) << "and must not schedule a remount";
    EXPECT_FALSE(store->refLaneWedgedForTest(ns)) << "a definite rejection is proven non-durable: no wedge";

    EXPECT_EQ(backend->matching_put_attempts, attempts_before + 4)
        << "three attempts means only the destructor backstop ran -- publishEntries stopped abandoning "
           "the build at the promote site";
    EXPECT_TRUE(store->livePrecommitsForTest(ns).empty())
        << "publishEntries must not walk away from a live precommit binding";
}


/// ==== Task 12: the prepared-part-write handle (spec §relink-handle) ====
/// `prepareEntries` stops after `precommitAdd`, so the durable-but-unpromoted state -- the window the
/// relink confirm round-trip has to sit inside -- becomes an OWNED object instead of an interval inside
/// one call. Every test below pins one half of that ownership contract.

/// Prepare-then-promote must be indistinguishable from today's atomic `publishEntries`, and the state
/// BETWEEN the two halves must be exactly one live precommit and no committed ref.
TEST(CASPartFolderAccess, PrepareThenPromoteMatchesPublishEntries)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend);
    const Cas::RootNamespace ns{"srv/t1"};
    Cas::CachedPartFolderAccess access(store, cacheOn());

    const std::vector<Cas::ManifestEntry> entries{inlineEntry("f", "one"), inlineEntry("g", "two")};
    const Cas::CommitOutcome published = access.publishEntries({ns, "via_publish"}, entries, Cas::ProvenanceOp::Insert);

    auto prepared = access.prepareEntries({ns, "via_prepare"}, entries, Cas::ProvenanceOp::Insert);

    /// The interposition point: the manifest is durable and owned by a LIVE precommit, but nothing is
    /// committed yet. This is precisely the state the confirm round-trip runs in.
    EXPECT_TRUE(store->livePrecommitsForTest(ns).contains({"via_prepare", prepared.manifestId().ref}))
        << "prepareEntries must leave the precommit binding live -- it is the durable `+1`";
    EXPECT_FALSE(access.existsRef({ns, "via_prepare"}, Cas::Freshness::ForceFresh))
        << "prepareEntries must not commit the ref";

    const Cas::CommitOutcome promoted = prepared.promote();
    EXPECT_EQ(promoted.ns.string(), ns.string());
    EXPECT_EQ(promoted.ref, "via_prepare");
    EXPECT_EQ(promoted.manifest_ref, prepared.manifestId().ref);
    EXPECT_TRUE(promoted.created);
    EXPECT_EQ(promoted.created, published.created) << "the split must reproduce publishEntries's outcome shape";
    EXPECT_TRUE(store->livePrecommitsForTest(ns).empty()) << "promote moves the binding out of the precommit view";

    auto view = access.getView({ns, "via_prepare"}, Cas::Freshness::ForceFresh);
    ASSERT_NE(view, nullptr);
    EXPECT_EQ(view->inlineBytes("f"), std::optional<String>("one"));
    EXPECT_EQ(view->inlineBytes("g"), std::optional<String>("two"));
}

/// Abort is not "drop the handle": it must APPEND the exact precommit removal. An abandoned precommit
/// that keeps its `+1` is the retention-leak class (`BACKLOG {#unmatched-minus-one-retention-leak}`),
/// and the stale-precommit sweep is prior-epoch-scoped, so a same-epoch leak is never reclaimed.
/// Asserted through the ledger's own precommit view rather than inferred from a later `precommitAdd`.
TEST(CASPartFolderAccess, PrepareThenAbortAppendsThePrecommitRemoval)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend);
    const Cas::RootNamespace ns{"srv/t1"};
    Cas::CachedPartFolderAccess access(store, cacheOn());

    auto prepared = access.prepareEntries({ns, "part_1"}, {inlineEntry("f", "one")}, Cas::ProvenanceOp::Insert);
    const Cas::ManifestId id = prepared.manifestId();
    ASSERT_TRUE(store->livePrecommitsForTest(ns).contains({"part_1", id.ref}));

    prepared.abort();

    EXPECT_FALSE(access.existsRef({ns, "part_1"}, Cas::Freshness::ForceFresh)) << "an aborted prepare commits nothing";
    EXPECT_FALSE(store->livePrecommitsForTest(ns).contains({"part_1", id.ref}))
        << "abort must append the EXACT precommit removal; a same-epoch precommit left behind retains its "
           "blobs forever (the prior-epoch-scoped stale sweep never reclaims it)";
    EXPECT_TRUE(store->livePrecommitsForTest(ns).empty());
    /// The precommit BODY survives (delete-after-sealed-decrements) -- the removal queues GC's `-1`,
    /// it does not writer-delete the manifest. Mirrors
    /// `CASPartWriteTxn.AbandonAppendsPrecommitRemovalAndKeepsLivePrecommitBody`.
    {
        DB::Cas::tests::OperationForTest op(*backend);
        EXPECT_TRUE((*op).head(store->layout().manifestKey(id), Cas::Retry::standard()).has_value());
    }
}

/// A forgotten terminal must be impossible, not merely discouraged: `~PartWriteTxn` only retires the
/// build sequence, so the handle's own destructor is the last-resort owner of the precommit removal.
TEST(CASPartFolderAccess, DestroyingAnUnfinishedPreparedPartWriteAborts)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend);
    const Cas::RootNamespace ns{"srv/t1"};
    Cas::CachedPartFolderAccess access(store, cacheOn());

    std::optional<Cas::ManifestId> id;
    {
        auto prepared = access.prepareEntries({ns, "part_1"}, {inlineEntry("f", "one")}, Cas::ProvenanceOp::Insert);
        id = prepared.manifestId();
        ASSERT_TRUE(store->livePrecommitsForTest(ns).contains({"part_1", id->ref}));
    }   /// neither promoted nor aborted

    EXPECT_TRUE(store->livePrecommitsForTest(ns).empty())
        << "destruction without a terminal must still append the precommit removal";
    EXPECT_FALSE(access.existsRef({ns, "part_1"}, Cas::Freshness::ForceFresh));
}

/// The terminal flag is explicit and one-shot: a second `promote`/`abort` is a caller bug, not an
/// idempotent no-op, and must never re-drive the (already dead) transaction.
///
/// The rejection throws LOGICAL_ERROR, which aborts the whole process in debug/sanitizer builds
/// (Exception.cpp's handle_error_code) instead of behaving like a catchable exception -- so the
/// expectThrowsCode form only makes sense in a plain release build, and the DeathTest variant below
/// proves the SAME rejections positively abort under debug/sanitizer builds instead (same pattern as
/// CASWiringOpsDeathTest in gtest_ca_wiring.cpp).
#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(CASPartFolderAccess, PreparedPartWriteRejectsASecondTerminal)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend);
    const Cas::RootNamespace ns{"srv/t1"};
    Cas::CachedPartFolderAccess access(store, cacheOn());

    auto promoted = access.prepareEntries({ns, "promoted"}, {inlineEntry("f", "one")}, Cas::ProvenanceOp::Insert);
    EXPECT_FALSE(promoted.isTerminal());
    promoted.promote();
    EXPECT_TRUE(promoted.isTerminal());
    expectThrowsCode(ErrorCodes::LOGICAL_ERROR, [&] { promoted.promote(); });
    expectThrowsCode(ErrorCodes::LOGICAL_ERROR, [&] { promoted.abort(); });

    auto aborted = access.prepareEntries({ns, "aborted"}, {inlineEntry("f", "one")}, Cas::ProvenanceOp::Insert);
    aborted.abort();
    EXPECT_TRUE(aborted.isTerminal());
    expectThrowsCode(ErrorCodes::LOGICAL_ERROR, [&] { aborted.abort(); });
    expectThrowsCode(ErrorCodes::LOGICAL_ERROR, [&] { aborted.promote(); });

    EXPECT_TRUE(access.existsRef({ns, "promoted"}, Cas::Freshness::ForceFresh));
    EXPECT_FALSE(access.existsRef({ns, "aborted"}, Cas::Freshness::ForceFresh));
    EXPECT_TRUE(store->livePrecommitsForTest(ns).empty());
}
#else
TEST(CASPartFolderAccessDeathTest, PreparedPartWriteRejectsASecondTerminalAborts)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend);
    const Cas::RootNamespace ns{"srv/t1"};
    Cas::CachedPartFolderAccess access(store, cacheOn());

    auto promoted = access.prepareEntries({ns, "promoted"}, {inlineEntry("f", "one")}, Cas::ProvenanceOp::Insert);
    promoted.promote();
    EXPECT_TRUE(promoted.isTerminal());
    EXPECT_DEATH(promoted.promote(), "owes exactly one terminal operation");
    EXPECT_DEATH(promoted.abort(), "owes exactly one terminal operation");

    auto aborted = access.prepareEntries({ns, "aborted"}, {inlineEntry("f", "one")}, Cas::ProvenanceOp::Insert);
    aborted.abort();
    EXPECT_TRUE(aborted.isTerminal());
    EXPECT_DEATH(aborted.abort(), "owes exactly one terminal operation");
    EXPECT_DEATH(aborted.promote(), "owes exactly one terminal operation");

    EXPECT_TRUE(access.existsRef({ns, "promoted"}, Cas::Freshness::ForceFresh));
    EXPECT_FALSE(access.existsRef({ns, "aborted"}, Cas::Freshness::ForceFresh));
    EXPECT_TRUE(store->livePrecommitsForTest(ns).empty());
}
#endif

/// Move-only, and the move transfers the terminal duty in full: the moved-from handle is already
/// terminal (its destructor must not re-abort a transaction the destination now owns), while the
/// destination still owes exactly one terminal.
TEST(CASPartFolderAccess, PreparedPartWriteMoveTransfersTheTerminalDuty)
{
    static_assert(!std::is_copy_constructible_v<Cas::PreparedPartWrite>);
    static_assert(!std::is_copy_assignable_v<Cas::PreparedPartWrite>);

    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend);
    const Cas::RootNamespace ns{"srv/t1"};
    Cas::CachedPartFolderAccess access(store, cacheOn());

    auto source = access.prepareEntries({ns, "part_1"}, {inlineEntry("f", "one")}, Cas::ProvenanceOp::Insert);
    const Cas::ManifestId id = source.manifestId();
    {
        Cas::PreparedPartWrite moved = std::move(source);
        /// NOLINTNEXTLINE(bugprone-use-after-move,clang-analyzer-cplusplus.Move,hicpp-invalid-access-moved)
        EXPECT_TRUE(source.isTerminal()) << "a moved-from handle owes nothing";
#ifndef DEBUG_OR_SANITIZER_BUILD
        expectThrowsCode(ErrorCodes::LOGICAL_ERROR, [&] { source.abort(); });
#else
        /// LOGICAL_ERROR aborts the process in debug/sanitizer builds; EXPECT_DEATH forks, so the
        /// parent's state (and the rest of this test) is unaffected.
        EXPECT_DEATH(source.abort(), "owes exactly one terminal operation");
#endif
        EXPECT_TRUE(store->livePrecommitsForTest(ns).contains({"part_1", id.ref}))
            << "the moved-from handle must not have aborted the transaction it handed over";
        EXPECT_EQ(moved.manifestId().ref, id.ref);
        moved.promote();
    }   /// the moved-from handle's destructor also runs here: it must be a no-op, not a second abort

    EXPECT_TRUE(access.existsRef({ns, "part_1"}, Cas::Freshness::ForceFresh));
    EXPECT_TRUE(store->livePrecommitsForTest(ns).empty());
}

TEST(CASPartFolderAccess, ExplainRecordsDecisions)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend);
    const Cas::RootNamespace ns{"srv/t1"};
    Cas::CachedPartFolderAccess access(store, {.explain_enabled = true});
    publishPart(store, ns, "part_1", {inlineEntry("checksums.txt", "cs")});
    const Cas::PartRefKey key{ns, "part_1"};

    access.getView(key, Cas::Freshness::CachedForLoad);
    EXPECT_EQ(access.explain(key).last_decision,
              Cas::CachedPartFolderAccess::LastDecision::Miss);       /// cold build
    EXPECT_FALSE(access.explain(key).retained);                                    /// Phase 3: never

    access.getView(key, Cas::Freshness::ForceFresh);
    EXPECT_EQ(access.explain(key).last_decision,
              Cas::CachedPartFolderAccess::LastDecision::ForceFreshRead);

    access.getView(key, Cas::Freshness::StrictValidate);
    EXPECT_EQ(access.explain(key).last_decision,
              Cas::CachedPartFolderAccess::LastDecision::StrictBypass);

    access.dropRef(key);
    EXPECT_EQ(access.explain(key).last_decision,
              Cas::CachedPartFolderAccess::LastDecision::Invalidated);
    EXPECT_GT(access.explain(key).estimated_bytes, 0u);
}

TEST(CASPartFolderAccess, BaselineRequestCountsWithoutRetention)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend);
    const Cas::Layout layout("p");
    const Cas::RootNamespace ns{"srv/t1"};
    Cas::CachedPartFolderAccess access(store);
    const auto id = publishPart(store, ns, "part_1", {inlineEntry("checksums.txt", "cs")});
    const Cas::PartRefKey key{ns, "part_1"};
    const String manifest_key = layout.manifestKey(id);

    backend->resetCounts();
    constexpr int n = 5;
    for (int i = 0; i < n; ++i)
        ASSERT_NE(access.getView(key, Cas::Freshness::CachedForLoad), nullptr);

    /// Retention off: one manifest-body GET (the decode cache absorbs the rest) and no manifest
    /// HEAD at all — a cached decode is served without a request.
    EXPECT_EQ(backend->getCount(manifest_key), 1u);
    EXPECT_EQ(backend->headCount(manifest_key), 0u);
}

/// ==== Phase 4 (retention) semantics battery: spec §Testing acceptance criteria ====

/// REMOVED (all-tree-part-files Task 9):
/// `MutableRefreshWithoutManifestRead` and `WriteThroughEraseThenRebuild` proved the cache facade's
/// `LastDecision::MutableRefresh` fast path -- a cheap re-check that could serve a retained view whose
/// manifest was unchanged but whose separate mutable payload had drifted, without a manifest re-read.
/// That whole two-tier freshness model is gone: every per-part file is an ordinary manifest entry now,
/// so ANY content change is a manifest change (`repointRef`) and the existing manifest-id staleness
/// check (`getView`'s `cached->manifestId() == resolved->manifest_id` compare) is the only freshness
/// check left -- there is no cheaper "payload-only" path to test separately. Coverage that remains
/// valid: `MismatchRebuildAfterRepublish` below proves the cache correctly rebuilds when the manifest
/// id changes under a retained view (the one case the deleted tests' "erase => cold rebuild" half also
/// exercised); `gtest_cas_repoint.cpp` (Task 3) proves `repointRef` erases the affected view on success.
TEST(CASPartFolderAccess, MismatchRebuildAfterRepublish)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend);
    const Cas::Layout layout("p");
    const Cas::RootNamespace ns{"srv/t1"};
    Cas::CachedPartFolderAccess access(store, cacheOn());
    publishPart(store, ns, "part_1", {inlineEntry("f", "orig")});
    const Cas::PartRefKey key{ns, "part_1"};

    ASSERT_NE(access.getView(key, Cas::Freshness::CachedForLoad), nullptr);   /// retained

    /// Drop + republish the SAME ref name with DIFFERENT content through the raw Core protocol (no
    /// facade => no write-through erase): the retained entry survives with a manifest_id that no
    /// longer resolves — the next CachedForLoad hits the manifest-changed compare (step 2c).
    store->dropRef(ns, "part_1");
    const auto id2 = publishPart(store, ns, "part_1", {inlineEntry("f", "DIFFERENT")});
    const String manifest_key2 = layout.manifestKey(id2);
    backend->resetCounts();

    auto view = access.getView(key, Cas::Freshness::CachedForLoad);
    ASSERT_NE(view, nullptr);
    EXPECT_NE(view->findFile("f"), nullptr);
    EXPECT_EQ(view->findFile("f")->inline_bytes, "DIFFERENT");    /// never the stale view
    EXPECT_EQ(backend->getCount(manifest_key2), 1u);              /// one new manifest GET
    EXPECT_EQ(access.explain(key).last_decision,
              Cas::CachedPartFolderAccess::LastDecision::Miss);   /// rebuilt, now retained
    EXPECT_TRUE(access.explain(key).retained);
}

/// The decode cache is keyed by id and an id names one content forever, so a warm reader serves
/// `ForceFresh` from the immutable decode with no manifest request even after the body object is
/// gone. A retained-view hit is not what is being tested here: `ForceFresh` bypasses the view cache
/// and rebuilds the view from the pool's manifest cache.
TEST(CASPartFolderAccess, ForceFreshServesImmutableDecodeWithoutManifestRequestsAfterBodyDeletion)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend);
    const Cas::Layout layout("p");
    const Cas::RootNamespace ns{"srv/t1"};
    Cas::CachedPartFolderAccess access(store, cacheOn());
    const auto id = publishPart(store, ns, "part_1", {inlineEntry("f", "x")});
    const Cas::PartRefKey key{ns, "part_1"};
    const String manifest_key = layout.manifestKey(id);

    ASSERT_NE(access.getView(key, Cas::Freshness::ForceFresh), nullptr);   /// warms the decode cache
    deleteManifestBody(*backend, layout, id);   /// protocol violation: live body vanishes
    backend->resetCounts();

    auto view = access.getView(key, Cas::Freshness::ForceFresh);
    ASSERT_NE(view, nullptr);
    EXPECT_NE(view->findFile("f"), nullptr);
    EXPECT_EQ(backend->getCount(manifest_key), 0u);
    EXPECT_EQ(backend->headCount(manifest_key), 0u);
    EXPECT_EQ(access.explain(key).last_decision, Cas::CachedPartFolderAccess::LastDecision::ForceFreshRead);

    /// `StrictValidate` serves the same immutable decode: an id names one content, so once the id is
    /// resolved there is nothing stricter left to prove about the body. It bypasses retention, so its
    /// recorded decision differs from the `ForceFresh` one above.
    auto strict_view = access.getView(key, Cas::Freshness::StrictValidate);
    ASSERT_NE(strict_view, nullptr);
    EXPECT_EQ(strict_view->manifest().get(), view->manifest().get());
    EXPECT_EQ(backend->getCount(manifest_key), 0u);
    EXPECT_EQ(backend->headCount(manifest_key), 0u);
    EXPECT_EQ(access.explain(key).last_decision, Cas::CachedPartFolderAccess::LastDecision::StrictBypass);
}

/// With the decode cache disabled a prior read leaves nothing behind, so the deleted body surfaces
/// as FILE_DOESNT_EXIST in every mode: the miss path is the same fail-closed path a cold reader takes.
TEST(CASPartFolderAccess, DeletedBodyFailsClosedInEveryModeWhenDecodeCacheDisabled)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = DB::Cas::Pool::open(backend,
        DB::Cas::PoolConfig{.pool_prefix = "p", .server_root_id = "test", .manifest_decode_cache_bytes = 0});
    const Cas::Layout layout("p");
    const Cas::RootNamespace ns{"srv/t1"};
    Cas::CachedPartFolderAccess access(store);   /// retention off: every mode reaches the reader
    const auto id = publishPart(store, ns, "part_1", {inlineEntry("f", "x")});
    const Cas::PartRefKey key{ns, "part_1"};

    ASSERT_NE(access.getView(key, Cas::Freshness::ForceFresh), nullptr);
    deleteManifestBody(*backend, layout, id);
    backend->resetCounts();

    for (auto freshness : {Cas::Freshness::CachedForLoad,
                           Cas::Freshness::ForceFresh,
                           Cas::Freshness::StrictValidate})
        expectThrowsCode(ErrorCodes::FILE_DOESNT_EXIST, [&] { access.getView(key, freshness); });
    EXPECT_EQ(backend->headCount(layout.manifestKey(id)), 0u);
    EXPECT_EQ(backend->getCount(layout.manifestKey(id)), 3u);   /// one GET per attempt, nothing cached
}

TEST(CASPartFolderAccess, AbsenceIsNeverRetained)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend);
    const Cas::RootNamespace ns{"srv/t1"};
    Cas::CachedPartFolderAccess access(store, cacheOn());
    publishPart(store, ns, "part_1", {inlineEntry("f", "x")});
    const Cas::PartRefKey key{ns, "part_1"};

    ASSERT_NE(access.getView(key, Cas::Freshness::CachedForLoad), nullptr);   /// retained
    access.dropRef(key);
    EXPECT_EQ(access.getView(key, Cas::Freshness::CachedForLoad), nullptr);   /// absent: nullptr, never retained

    /// Re-publish under the SAME ref name: immediately visible, no stale absence remembered.
    publishPart(store, ns, "part_1", {inlineEntry("f", "y")});
    auto view = access.getView(key, Cas::Freshness::CachedForLoad);
    ASSERT_NE(view, nullptr);
    EXPECT_EQ(view->inlineBytes("f"), std::optional<String>("y"));
}

/// Task 23 (URF plan phase 7): `getView` emits a `RefResolve` audit event only when the access does
/// real resolve work -- a warm `CachedForLoad` hit whose retained view already matches the fresh
/// resolve serves the call with no new information, so it must add no row. `resolveRef` itself defers
/// the emit on this call path (`ResolveAudit::Deferred`, `CachedPartFolderAccess::resolve`), and
/// `getView` re-emits the identical event on every OTHER path -- cold builds and `ForceFresh`.
TEST(CASPartFolderAccess, GetViewEmitsRefResolveOnlyOnRealResolveWork)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend);
    const Cas::RootNamespace ns{"srv/t1"};
    publishPart(store, ns, "part_1", {inlineEntry("checksums.txt", "cs")});
    const Cas::PartRefKey key{ns, "part_1"};

    /// Heap-owned, not a plain local: the Pool can outlive this stack frame (a background publish holds
    /// `shared_from_this()`), so a by-reference capture of a local would dangle.
    auto seen = std::make_shared<SharedEventLog>();
    store->setEventSink([seen](const Cas::CasEvent & e)
    {
        seen->push(e);
    });
    Cas::CachedPartFolderAccess access(store, cacheOn());   /// retention on

    const auto refResolveCount = [&]
    {
        const std::vector<Cas::CasEvent> observed = seen->snapshot();
        return std::count_if(observed.begin(), observed.end(),
            [](const Cas::CasEvent & e) { return e.type == Cas::CasEventType::RefResolve; });
    };

    /// Cold CachedForLoad build: real resolve work -> exactly one RefResolve.
    ASSERT_NE(access.getView(key, Cas::Freshness::CachedForLoad), nullptr);
    EXPECT_EQ(refResolveCount(), 1);

    /// Warm hit: the retained view still matches the fresh resolve, so this call serves the SAME
    /// manifest with no new information -- before this fix it would emit a SECOND RefResolve
    /// (resolveRef emitted unconditionally); after the fix it must add none.
    ASSERT_NE(access.getView(key, Cas::Freshness::CachedForLoad), nullptr);
    EXPECT_EQ(refResolveCount(), 1) << "a warm view-cache hit must not add a RefResolve row";

    /// ForceFresh always bypasses the retained view, so this is real resolve work again -> +1.
    ASSERT_NE(access.getView(key, Cas::Freshness::ForceFresh), nullptr);
    EXPECT_EQ(refResolveCount(), 2);

    store->setEventSink(nullptr);
}

TEST(CASPartFolderAccess, OversizedViewServedNotRetained)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend);
    const Cas::Layout layout("p");
    const Cas::RootNamespace ns{"srv/t1"};
    /// max_entry_bytes = 1: every real view (>= the 256-byte fixed overhead alone) is oversized.
    Cas::CachedPartFolderAccess access(store,
        Cas::CachedPartFolderAccess::CacheParams{
            .cache_bytes = 64ULL << 20, .max_entries = 10000, .max_entry_bytes = 1,
            .explain_enabled = true});
    const auto id = publishPart(store, ns, "part_1", {inlineEntry("f", "x")});
    const Cas::PartRefKey key{ns, "part_1"};
    const String manifest_key = layout.manifestKey(id);

    auto view1 = access.getView(key, Cas::Freshness::CachedForLoad);
    ASSERT_NE(view1, nullptr);
    EXPECT_FALSE(access.explain(key).retained);
    EXPECT_EQ(access.explain(key).last_decision,
              Cas::CachedPartFolderAccess::LastDecision::OversizedBypass);

    backend->resetCounts();
    auto view2 = access.getView(key, Cas::Freshness::CachedForLoad);
    ASSERT_NE(view2, nullptr);
    /// Not retained: every call rebuilds the view (a new view object over the SAME shared decode) and
    /// records the bypass again; the rebuild costs no manifest request because the decode is cached.
    EXPECT_NE(view1.get(), view2.get());
    EXPECT_EQ(view1->manifest().get(), view2->manifest().get());
    EXPECT_EQ(access.explain(key).last_decision,
              Cas::CachedPartFolderAccess::LastDecision::OversizedBypass);
    EXPECT_EQ(backend->getCount(manifest_key), 0u);
    EXPECT_EQ(backend->headCount(manifest_key), 0u);
    EXPECT_FALSE(access.explain(key).retained);
}

TEST(CASPartFolderAccess, DisabledModeKeepsBaseline)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend);
    const Cas::Layout layout("p");
    const Cas::RootNamespace ns{"srv/t1"};
    /// CacheParams{} (cache_bytes == 0): the explicit disable switch, same as the single-arg ctor.
    Cas::CachedPartFolderAccess access(store, Cas::CachedPartFolderAccess::CacheParams{});
    const auto id = publishPart(store, ns, "part_1", {inlineEntry("checksums.txt", "cs")});
    const Cas::PartRefKey key{ns, "part_1"};
    const String manifest_key = layout.manifestKey(id);

    backend->resetCounts();
    constexpr int n = 5;
    for (int i = 0; i < n; ++i)
        ASSERT_NE(access.getView(key, Cas::Freshness::CachedForLoad), nullptr);

    /// bytes=0 restores the no-retention call graph: one body GET, then the decode cache serves every
    /// rebuild with no manifest request.
    EXPECT_EQ(backend->getCount(manifest_key), 1u);
    EXPECT_EQ(backend->headCount(manifest_key), 0u);
    EXPECT_FALSE(access.explain(key).retained);
}

TEST(CASPartFolderAccess, SingleFlightColdBuild)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend);
    const Cas::Layout layout("p");
    const Cas::RootNamespace ns{"srv/t1"};
    Cas::CachedPartFolderAccess access(store, cacheOn());
    const auto id = publishPart(store, ns, "part_1", {inlineEntry("f", "x")});
    const Cas::PartRefKey key{ns, "part_1"};
    const String manifest_key = layout.manifestKey(id);

    backend->resetCounts();
    constexpr int k = 8;
    std::latch start_gate(k);
    std::vector<std::thread> threads;
    std::vector<std::shared_ptr<const Cas::PartFolderView>> results(k);
    for (int i = 0; i < k; ++i)
        threads.emplace_back([&, i]
        {
            start_gate.arrive_and_wait();
            results[i] = access.getView(key, Cas::Freshness::CachedForLoad);
        });
    for (auto & t : threads)
        t.join();

    for (const auto & r : results)
        EXPECT_NE(r, nullptr);
    EXPECT_EQ(backend->getCount(manifest_key), 1u);   /// single-flight: ONE body GET for the burst
}

TEST(CASPartFolderAccess, DropNamespaceErasesAllViews)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend);
    const Cas::RootNamespace ns{"srv/t1"};
    /// Review C2: deliberately NOT pinned -- `ns` gets a REAL, random catalog incarnation from
    /// `publishPart` below, which is what this test drives production's namespace-drop/recreate
    /// terminal snapshot or retirement checkpoint at. Pinning
    /// it to the sentinel would make production's real-incarnation path untested by the one test that
    /// exercises it end-to-end (the exact gap C2 named).
    Cas::CachedPartFolderAccess access(store, cacheOn());
    publishPart(store, ns, "part_1", {inlineEntry("f", "x")});
    publishPart(store, ns, "part_2", {inlineEntry("f", "y")});
    const Cas::PartRefKey key1{ns, "part_1"};
    const Cas::PartRefKey key2{ns, "part_2"};

    ASSERT_NE(access.getView(key1, Cas::Freshness::CachedForLoad), nullptr);   /// retained
    ASSERT_NE(access.getView(key2, Cas::Freshness::CachedForLoad), nullptr);   /// retained
    EXPECT_TRUE(access.explain(key1).retained);
    EXPECT_TRUE(access.explain(key2).retained);

    access.dropNamespace(ns);

    /// dropNamespace removes the namespace via the ref-log `remove_namespace` transaction AND erases every
    /// cached view: the dropped entries must not masquerade as "retained", and no stale key1/key2 view may
    /// be served.
    EXPECT_FALSE(access.explain(key1).retained);
    EXPECT_FALSE(access.explain(key2).retained);   /// dropped too, even though never re-touched

    /// A fresh getView on the removed namespace is a COLD MISS (nullptr) -- never a stale hit on the
    /// dropped manifest. A residual retained entry would instead be served here without ever going through
    /// validate-on-hit, exactly the masquerade this guards against.
    EXPECT_EQ(access.getView(key1, Cas::Freshness::CachedForLoad), nullptr);
    EXPECT_EQ(access.getView(key2, Cas::Freshness::CachedForLoad), nullptr);

}

TEST(CASPartFolderAccess, BestEffortRollbackDropCountsAndSurvivesABackendOutage)
{
    auto backend = std::make_shared<RollbackFaultBackend>();
    auto store = openPoolForTest(backend);
    /// Both drops below give up only when their own retry window closes, so the engine's inter-attempt
    /// sleeps are paid in virtual time rather than by sleeping out the operation deadline for real.
    auto clock = Cas::tests::VirtualRetryClock::installOn(store);
    Cas::CachedPartFolderAccess access(store, cacheOn());

    const Cas::RootNamespace ns_a{"srv/ta"};
    const Cas::RootNamespace ns_b{"srv/tb"};
    publishPart(store, ns_a, "part_a", {inlineEntry("checksums.txt", "cs")});
    publishPart(store, ns_b, "part_b", {inlineEntry("checksums.txt", "cs")});

    backend->armed = true;
    /// Sanity: with the backend armed, a real dropRef propagates (so the fault reaches the catch).
    EXPECT_ANY_THROW(store->dropRef(ns_a, "part_a"));

    using ProfileEvents::global_counters;
    const auto before = global_counters[ProfileEvents::CASRefRollbackBestEffortDropFailed].load();
    /// The compensating-rollback path must NOT throw (noexcept) and MUST record the swallowed failure.
    access.dropRefBestEffort(Cas::PartRefKey{ns_b, "part_b"});
    const auto after = global_counters[ProfileEvents::CASRefRollbackBestEffortDropFailed].load();
    EXPECT_EQ(after, before + 1);
    EXPECT_GT(clock->pauseCount(), 0u)
        << "the give-up must be the call's own retry window, reached through the injected sleep";

    backend->armed = false;   /// let store teardown release its lease cleanly
}

/// Part B review, MAJOR 3a: a promote whose ref-log append did not resolve MUST NOT be reported as
/// "nothing was committed".
///
/// `PreparedRelinkOverPartWrite::promote` maps a `NETWORK_ERROR` to `MechanismFallbackAllowed`, which
/// tells the interserver receiver to fetch the part's bytes from the same sender instead. That is sound
/// only when the promote is PROVEN not to have committed. It is not proven here: the promotion object
/// landed and only its acknowledgement was lost, so the ref below IS committed while `promote` reports
/// failure -- and a byte fetch on top of it is a sequential double publication of one logical fetch.
///
/// The transaction therefore records the distinction where it is knowable (around its own append)
/// rather than leaving it to be guessed from an error code, which cannot carry it: the SAME
/// `NETWORK_ERROR` is raised by a promote rejected before the append (proof of the negative) and by one
/// whose append never resolved.
TEST(CASPartFolderAccess, AnUnresolvedPromoteIsNotReportedAsDefinitelyNotCommitted)
{
    auto backend = std::make_shared<Cas::tests::LatchedChunkFaultBackend>();
    auto store = openPoolForTest(backend);
    auto clock = Cas::tests::VirtualRetryClock::installOn(store);
    const Cas::RootNamespace ns{"srv/t1"};
    DB::Cas::tests::casAdmitRecoverableEntry(*backend, store->layout(), ns, store->liveWriterEpoch());
    Cas::CachedPartFolderAccess access(store, cacheOn());
    const Cas::PartRefKey key{ns, "part_1"};

    auto prepared = access.prepareEntries(key, {inlineEntry("f", "one")}, Cas::ProvenanceOp::Insert);
    ASSERT_FALSE(prepared.commitIsUnresolved()) << "no promote has been attempted yet";

    /// The promotion's own ref-log object lands; only the acknowledgement, and the controller's
    /// verifying read, are lost. Scoped to this namespace's ref log so nothing else consumes the fault.
    /// A COUNTED fault cannot produce an unresolved outcome: the request engine settles the ambiguity
    /// by an exact read that would find the landed object and report `Committed` inside the very same
    /// call. Both legs therefore stay LATCHED for the whole call, and `VirtualRetryClock` pays the
    /// retry window in virtual time instead of real wall-clock.
    backend->fault_substr = store->layout().namespaceStreamPrefix(fixture::fixtureLife(ns)) + "_log/";
    backend->mode = Cas::tests::ChunkFaultBackend::Mode::LandedThenLost;
    backend->latched = true;
    expectThrowsCode(ErrorCodes::NETWORK_ERROR, [&] { prepared.promote(); });
    ASSERT_GT(clock->pauseCount(), 1u)
        << "one attempt cannot exhaust the retry window: the fault must have outlasted every reissue";

    EXPECT_TRUE(prepared.commitIsUnresolved())
        << "a promote whose append may have landed must not be classified as a mechanism failure -- the "
           "receiver would fetch the bytes and publish the same part a second time";

    /// The hazard itself, stated as an assertion: the promote DID commit. Any further append into this
    /// table resolves the wedge first, which is what makes the committed row visible. Disarmed
    /// COMPLETELY, because that flush must reach the store normally: a still-armed lost read would
    /// fault the wedge's own settling read, and nothing would resolve.
    backend->latched = false;
    backend->mode = Cas::tests::ChunkFaultBackend::Mode::None;
    backend->fault_count = 0;
    backend->fail_read_once_key.clear();
    access.prepareEntries({ns, "flush_driver"}, {inlineEntry("f", "two")}, Cas::ProvenanceOp::Insert).abort();
    EXPECT_TRUE(access.existsRef(key, Cas::Freshness::ForceFresh))
        << "the promotion object landed, so 'the promote failed' says nothing about the ref";
}

/// Part B review, MAJOR 3b: nothing after a durable commit may throw before the handle records it.
///
/// `promoteBuild` used to assemble its `CommitOutcome` -- two `String` copies -- and invalidate the
/// cached view AFTER the durable append and BEFORE `PreparedPartWrite::promote` set `terminal`. An
/// allocation failure in that window therefore entered the failed-promote catch with the ref already
/// committed, where the handle abandons its build and reports the promote as failed. The outcome's
/// strings are now copied BEFORE the append and the commit is recorded in an allocation-free region
/// immediately after it, so the window is empty by construction; the probe below fires just past it.
TEST(CASPartFolderAccess, APostCommitFailureLeavesTheHandleTerminal)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend);
    const Cas::RootNamespace ns{"srv/t1"};
    Cas::CachedPartFolderAccess access(store, cacheOn());
    const Cas::PartRefKey key{ns, "part_1"};

    auto prepared = access.prepareEntries(key, {inlineEntry("f", "one")}, Cas::ProvenanceOp::Insert);

    /// Heap-owned, not a plain local: `setEventSink(nullptr)` below only stops FUTURE sink installs
    /// from using this closure -- it does not guarantee an already-in-flight background call is not
    /// still executing the old one -- and the Pool can outlive this stack frame regardless (a
    /// background publish holds `shared_from_this()`).
    auto seen = std::make_shared<SharedEventLog>();
    store->setEventSink([seen](const Cas::CasEvent & e)
    {
        seen->push(e);
    });

    /// `MEMORY_LIMIT_EXCEEDED` -- what a tracked allocation failure actually raises -- and deliberately
    /// not `LOGICAL_ERROR`, which aborts at construction in debug/sanitizer builds.
    access.setPostCommitProbeForTest([]
    {
        throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED,
            "simulated allocation failure in the post-commit work of promoteBuild");
    });
    expectThrowsCode(ErrorCodes::MEMORY_LIMIT_EXCEEDED, [&] { prepared.promote(); });
    access.setPostCommitProbeForTest(nullptr);
    store->setEventSink(nullptr);

    EXPECT_TRUE(prepared.isTerminal())
        << "the commit is durable, so the handle owes nothing";
    EXPECT_TRUE(access.existsRef(key, Cas::Freshness::ForceFresh)) << "the promote really did commit";
    EXPECT_TRUE(store->livePrecommitsForTest(ns).empty());

    /// The discriminating assertion. `isTerminal` alone is not one: the old code reached the catch,
    /// abandoned an ALREADY PROMOTED build -- which succeeds, because a promoted build no longer owes a
    /// precommit removal -- and so ended up terminal too, by accident. What the abandon leaves behind is
    /// the audit trail of a publish that is reported as thrown away while its ref is committed.
    const std::vector<Cas::CasEvent> observed = seen->snapshot();
    const auto build_aborts = std::count_if(observed.begin(), observed.end(),
        [](const Cas::CasEvent & e) { return e.type == Cas::CasEventType::BuildAbort; });
    EXPECT_EQ(build_aborts, 0)
        << "a build whose promote is DURABLE was abandoned by the failed-promote catch: the handle had "
           "not yet recorded the commit when the post-commit work threw";
    EXPECT_EQ(std::count_if(observed.begin(), observed.end(),
        [](const Cas::CasEvent & e) { return e.type == Cas::CasEventType::BuildPublish; }), 1);
}

/// Part B review, MAJOR 4: move ASSIGNMENT is deleted rather than implemented.
///
/// It cannot be implemented correctly. Overwriting a handle that still owes a terminal must first
/// discharge that duty, and `abandon` appends through the ref lane, so it can FAIL -- which a move
/// assignment has no way to report. The old implementation overwrote the destination's build even when
/// `abandonBuildBestEffort` returned false, permanently dropping a cleanup owner: a live-epoch precommit
/// that no sweep and no GC ever reclaims. Nothing needs the operator (the interserver relink's handle is
/// move CONSTRUCTED into place), and a contract that cannot be relied on is worse than none.
TEST(CASPartFolderAccess, PreparedPartWriteIsNotMoveAssignable)
{
    EXPECT_FALSE(std::is_move_assignable_v<Cas::PreparedPartWrite>)
        << "a move assignment cannot discharge a terminal duty that may fail to be discharged";
    EXPECT_TRUE(std::is_move_constructible_v<Cas::PreparedPartWrite>);
}
