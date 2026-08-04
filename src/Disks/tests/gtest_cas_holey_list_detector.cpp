#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefLogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefProtocol.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/ProfileEvents.h>

#include <algorithm>
#include <mutex>
#include <set>
#include <string>
#include <vector>

/// SKIPPED-TRANSACTION suite. The defect class: a GC round's fold cursor advances past a ref
/// transaction the round never applied. Once the cursor is sealed above a record, that record can
/// never be folded again, so BOTH directions of the damage are permanent:
///
///   - RETENTION: a skipped `-1` leaves a residual `+1`, so the blob is never reclaimed (a leak);
///   - DELETION:  a skipped `+1` hides a live owner, so GC deletes a blob a committed manifest still
///     references (data loss).
///
/// The suspected MECHANISM (a `LIST` page that omits a durable key) is UNCONFIRMED — a holey page was
/// never directly observed, it survives by elimination, and `CaRelinkConfirmCore.tla` `_sab_holeylist`
/// proves the mechanism is SUFFICIENT, not that it is what happened. These tests therefore use the
/// holey listing only as the cheapest way to make the EFFECT executable; nothing here may key on how
/// the hole was produced.

using namespace DB::Cas;
using DB::Cas::tests::idOf;
using DB::Cas::tests::u128Of;

namespace
{

/// A backend that drops ONE chosen key from ONE chosen `list` call, while leaving exact `get`/`head`
/// of that key working. This is the minimal realisation of "the store returned an incomplete answer":
/// the record is durable and readable, it is simply absent from one enumeration. The mechanism is
/// deliberately NOT modelled (no page split, no cursor games) — the arithmetic intake under test must
/// not depend on how the hole was produced.
///
/// WHICH call is explicit and load-bearing. A GC round enumerates the ref prefix ONCE, in
/// `Gc::listRefPrefix`, and the fold regroups that same enumeration -- so `nth = 0` is the walk whose
/// hole the fold would have to survive, and it is the one every test here arms. `nth` counts, from the
/// moment `omitFromNthListCall` is called, only those `list` calls that WOULD have returned the key — so
/// unrelated prefix enumerations do not shift it.
/// Arm the sabotage AFTER every seeding write: the writer's own sequence allocation lists the
/// namespace prefix and would otherwise consume a qualifying call.
///
/// Erasing a key from the page never disturbs pagination: `ListPage::next_cursor` is the LAST key the
/// underlying backend returned and is computed before the erase, so the next page still resumes
/// strictly after it.
class HoleyListBackend : public InMemoryBackend
{
public:
    /// Omit `key` from the `nth` (0-based) subsequent qualifying `list` call. Resets the counter.
    void omitFromNthListCall(const String & key, size_t nth)
    {
        std::lock_guard lock(m);
        omitted = key;
        target_call = nth;
        seen_calls = 0;
        served = false;
    }

    /// Whether the hole was actually served. Every test asserts this, so a mis-typed key or a
    /// miscounted `nth` cannot let a test pass vacuously.
    bool holeServed() const
    {
        std::lock_guard lock(m);
        return served;
    }

    ListPage list(const String & prefix, const String & cursor, size_t limit) override
    {
        ListPage page = InMemoryBackend::list(prefix, cursor, limit);
        std::lock_guard lock(m);
        if (omitted.empty())
            return page;
        auto it = std::find_if(page.keys.begin(), page.keys.end(),
                               [&](const ListedKey & k) { return k.key == omitted; });
        if (it == page.keys.end())
            return page;              /// not a qualifying call — do not count it
        if (seen_calls++ != target_call)
            return page;
        page.keys.erase(it);
        served = true;
        omitted.clear();              /// one hole only
        return page;
    }

private:
    mutable std::mutex m;
    String omitted;
    size_t target_call = 0;
    size_t seen_calls = 0;
    bool served = false;
};

PoolPtr openHoleyPool(std::shared_ptr<HoleyListBackend> & out_backend)
{
    out_backend = std::make_shared<HoleyListBackend>();
    return Pool::open(out_backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
}

/// A `ManifestEntry` for a Blob leaf at `path` referencing `payload`'s content hash.
ManifestEntry blobEntry(const String & path, const String & payload)
{
    ManifestEntry e;
    e.path = path;
    e.placement = EntryPlacement::Blob;
    e.ref = BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(u128Of(payload))};
    e.blob_size = payload.size();
    return e;
}

/// Publish one single-blob part through the REAL writer sequence and return its `ManifestId`.
ManifestId publishOneBlobPart(const PoolPtr & s, const RootNamespace & ns, const String & ref,
                              const String & payload)
{
    PartWriteInfo info;
    info.intended_ref = ns.string() + "/" + ref;
    auto build = s->beginPartWrite(info);
    const ManifestId id = build->stageManifest({blobEntry("data.bin", payload)});
    build->precommitAdd(ns, ref, id);
    build->putBlob(idOf(payload), BlobSource::fromString(payload));
    build->promote(ns, ref, build->buildId(), id);
    return id;
}

bool blobPresent(const std::shared_ptr<HoleyListBackend> & b, const Layout & layout, const String & payload)
{
    return b->head(layout.blobKey(BlobRef{BlobHashAlgo::CityHash128,
                                          BlobDigest::fromU128(u128Of(payload))})).exists;
}

/// Every ref object key of one namespace. Used to identify WHICH objects a publish appended, rather
/// than guessing a sequence number.
std::set<String> listRefKeys(Backend & b, const Layout & layout, const RootNamespace & ns)
{
    /// Stage B (Task 4-C): `ns` is born through the REAL append lane here, so its objects sit at a
    /// real catalog-minted incarnation, not the Stage-A sentinel.
    const NamespaceLifeId life = CasRefCatalog::lifeIfCataloged(b, layout, ns).value();
    std::set<String> keys;
    forEachListedKey(b, layout.namespaceStreamPrefix(life), [&](const ListedKey & k) { keys.insert(k.key); });
    return keys;
}

/// The keys present in `after` and not in `before`.
std::vector<String> addedKeys(const std::set<String> & before, const std::set<String> & after)
{
    std::vector<String> added;
    std::set_difference(after.begin(), after.end(), before.begin(), before.end(),
                        std::back_inserter(added));
    return added;
}

/// Among `candidates`, the ONE ref-log key whose transaction emits an edge of sign `change` naming
/// `manifest_id`.
///
/// Selecting the key by DECODING is load-bearing. One logical publish appends SEVERAL ref-log
/// transactions (`precommitAdd`, then `promote`), and it is the `precommitAdd` that carries the `+1`
/// activation — a promote is an owner move at the same `manifest_ref` and emits no edge at all. Picking
/// "the greatest new key" would therefore omit the wrong object and the sabotage would be a no-op that
/// still let the test pass.
String refLogKeyEmittingEdge(Backend & b, const Layout & layout, const RootNamespace & ns,
                             const std::vector<String> & candidates, const ManifestId & manifest_id,
                             int change)
{
    std::vector<String> hits;
    for (const String & key : candidates)
    {
        const auto parsed = layout.parseRefObjectKey(key);
        if (!parsed || parsed->kind != RefObjectKind::Log)
            continue;
        const auto got = b.get(key);
        if (!got)
            continue;
        const RefLogTxn txn =
            decodeRefLogTxn(openObject(FormatId::RefLog, got->bytes), ns.string(), parsed->txn_id);
        for (const RefManifestEdge & e : manifestEdgesOfTxn(txn))
            if (e.change == change && e.manifest_id == manifest_id)
            {
                hits.push_back(key);
                break;
            }
    }
    EXPECT_EQ(hits.size(), 1u) << "expected exactly one ref log emitting a " << change
                               << " edge for the manifest, found " << hits.size();
    return hits.empty() ? String{} : hits.front();
}

void runRounds(const PoolPtr & s, Gc & gc, int rounds)
{
    for (int i = 0; i < rounds; ++i)
    {
        DB::Cas::tests::runRegularRoundReclaiming(gc);
        s->renewWatermarkOnce();
    }
}

}

/// RETENTION DIRECTION (the RCA's primary reproduction). A ref-log record omitted from a listing used to
/// sort at or below the cursor forever, so restoring the listing could not recover it: the blob's `-1`
/// never folded and the blob was retained permanently. Under arithmetic intake the omitted record is
/// reached by exact key on the very round that was lied to, so the removal folds and the blob dies on
/// the normal schedule — no abort, and no waiting for the store to become honest again.
TEST(CASHoleyListDetector, OmittedRemoveRecordIsSkippedForever)
{
    std::shared_ptr<HoleyListBackend> b;
    auto s = openHoleyPool(b);
    const Layout & layout = s->layout();
    const RootNamespace ns{"test/tbl"};
    const String payload = "holey-payload";

    /// A: publish the part (its `+1` edges). Folded by the rounds below.
    const ManifestId part = publishOneBlobPart(s, ns, "part_a", payload);
    Gc gc(s, hexToU128("00000000000000000000000000000001"));
    runRounds(s, gc, 2);
    ASSERT_TRUE(blobPresent(b, layout, payload));

    /// R: drop the ref (the `-1`). Through the REAL writer API, never the raw ref-log helper: the raw
    /// helper allocates a sequence by listing, which collides with the ledger's own in-memory sequence
    /// as soon as the same namespace is written through the writer again (`part_h` below).
    const std::set<String> before_drop = listRefKeys(*b, layout, ns);
    s->dropRef(ns, "part_a");
    const std::set<String> after_drop = listRefKeys(*b, layout, ns);
    const String remove_key =
        refLogKeyEmittingEdge(*b, layout, ns, addedKeys(before_drop, after_drop), part, -1);
    ASSERT_FALSE(remove_key.empty());

    /// H: a later, unrelated record so the cursor has a reason to advance past R even when R is not
    /// returned.
    publishOneBlobPart(s, ns, "part_h", "harmless-payload");
    s->renewWatermarkOnce();   /// advance the floor so the dropped closure is not spared as in-flight

    /// nth = 0: the round's own enumeration of the ref prefix — the one the fold regroups, and the only
    /// walk whose hole the intake has to survive. Armed LAST, after every seeding write, so no
    /// writer-side namespace listing consumes a qualifying call.
    b->omitFromNthListCall(remove_key, /*nth=*/0);

    runRounds(s, gc, 1);
    ASSERT_TRUE(b->holeServed()) << "the sabotage never fired — the omitted key was never listed";

    /// Drive to the reclaim. The point is that the FIRST of these rounds — the one served the hole —
    /// already folded the removal; the rest are the condemn/graduate/delete pacing.
    runRounds(s, gc, 12);

    EXPECT_FALSE(blobPresent(b, layout, payload))
        << "the removal was hidden from one enumeration and never folded — the cursor advanced past a "
           "record the round never applied, which is the skipped-transaction defect itself";
}

/// DELETION DIRECTION (the mirror safety test from the RCA). Two owners share ONE deduplicated blob.
/// The SECOND owner's `+1` is omitted from one listing while the FIRST owner's `-1` folds normally,
/// so GC sees zero edges for a blob a live manifest still references. THIS MUST NEVER DELETE THE BLOB.
TEST(CASHoleyListDetector, OmittedActivationNeverPermitsDeletingALiveBlob)
{
    std::shared_ptr<HoleyListBackend> b;
    auto s = openHoleyPool(b);
    const Layout & layout = s->layout();
    const RootNamespace ns{"test/tbl"};
    const String payload = "shared-payload";

    /// M1 owns the token. Fold it so its `+1` is durable in the in-degree generation.
    const ManifestId m1 = publishOneBlobPart(s, ns, "part_1", payload);
    Gc gc(s, hexToU128("00000000000000000000000000000001"));
    runRounds(s, gc, 2);
    ASSERT_TRUE(blobPresent(b, layout, payload));
    ASSERT_TRUE(b->head(layout.manifestKey(m1)).exists)
        << "M1's body must still be present so its `-1` edges are readable at removal-fold";

    /// M2 adopts the SAME deduplicated blob (`putBlob` of an identical payload dedups). Learn WHICH
    /// ref-log object carries M2's ACTIVATION by diffing the namespace's ref prefix around the publish
    /// and decoding the new objects — do NOT guess a sequence number and do not append a probe
    /// transaction (that would perturb the very stream under test).
    const std::set<String> before = listRefKeys(*b, layout, ns);
    const ManifestId m2 = publishOneBlobPart(s, ns, "part_2", payload);
    const std::set<String> after = listRefKeys(*b, layout, ns);
    const String m2_key = refLogKeyEmittingEdge(*b, layout, ns, addedKeys(before, after), m2, +1);
    ASSERT_FALSE(m2_key.empty());

    /// M1's removal folds normally. Through the REAL writer API (see the retention test's note).
    s->dropRef(ns, "part_1");
    s->renewWatermarkOnce();   /// advance the floor so the removed closure is not spared as in-flight

    /// nth = 0: the round's own walk (see the note in the retention test). Armed LAST so the writer's
    /// own namespace listings cannot shift the count.
    b->omitFromNthListCall(m2_key, /*nth=*/0);

    runRounds(s, gc, 12);   /// condemn -> graduate -> delete needs several rounds
    /// The anti-vacuity check, and it is the RIGHT one now: a run that merely happened not to delete the
    /// blob must not pass for the wrong reason, and what makes this run non-trivial is that the hole was
    /// actually SERVED to the enumeration the fold works from. (It used to be "and the detector fired",
    /// which was only ever a proxy for that — and is now a property of a different, sampled mechanism,
    /// pinned in `CASRetirementSweep`.)
    ASSERT_TRUE(b->holeServed()) << "the sabotage never fired — the omitted key was never listed";

    EXPECT_TRUE(blobPresent(b, layout, payload))
        << "GC deleted a blob that manifest " << manifestRefDebugString(m2.ref)
        << " still references — the skipped-transaction DATA-LOSS class, reproduced";
}
