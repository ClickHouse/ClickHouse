#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Parts/PartFolderAccess.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/ProfileEvents.h>

/// Task 3 (all-tree-part-files plan, 2026-07-15): `CachedPartFolderAccess::repointRef` -- the audited
/// primitive a standalone write/remove on an already-COMMITTED part must go through once the mutable
/// per-part file set is empty. It republishes
/// the whole manifest with the new entry set, riding `PartWriteTxn::promote`'s `allow_repoint` mode (Task 2).

namespace ProfileEvents
{
extern const Event CASRefRepoint;
}

using namespace DB::Cas;

namespace
{

ManifestEntry inlineEntry(const String & path, const String & bytes)
{
    ManifestEntry e;
    e.path = path;
    e.placement = EntryPlacement::Inline;
    e.ref = BlobRef{BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(DB::Cas::tests::u128Of(bytes))};

    e.blob_size = bytes.size();
    e.inline_bytes = bytes;
    return e;
}

/// Publish `entries` as committed ref `ns/ref` through the real writer protocol.
ManifestId publishPart(const PoolPtr & store, const RootNamespace & ns, const String & ref,
                       std::vector<ManifestEntry> entries)
{
    auto build = store->beginPartWrite(PartWriteInfo{.intended_ref = ns.string() + "/" + ref,
                                             .intended_namespace = ns, .op = ProvenanceOp::Insert});
    const ManifestId id = build->stageManifest(entries);
    build->precommitAdd(ns, ref, id);
    build->promote(ns, ref, build->buildId(), id);
    return id;
}

}

/// Byte-equal candidate: the exact same entries republished onto an already-committed ref must be a
/// ZERO-mutation no-op -- no fresh manifest staged, no ref-log record appended, no `RefRepoint` event.
/// `stageManifest` mints a non-content-derived `ManifestRef` AND durably PUTs the body on every call
/// (CasPartWriteTxn.cpp), so this can only hold if the no-op check compares candidate `entries` directly
/// against the currently-committed manifest's DECODED entries -- never by staging first (the same
/// structural comparison `republishRef`'s BUG 1c fix uses).
TEST(CASRepoint, ByteEqualIsNoOp)
{
    auto backend = std::make_shared<DB::Cas::tests::CountingBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);
    const RootNamespace ns{"srv/t1"};
    DB::Cas::CachedPartFolderAccess access(store);
    const auto id = publishPart(store, ns, "part_1", {inlineEntry("checksums.txt", "cs")});
    const DB::Cas::PartRefKey key{ns, "part_1"};

    backend->resetCounts();
    const uint64_t repoints_before = ProfileEvents::global_counters[ProfileEvents::CASRefRepoint].load();
    const DB::Cas::CommitOutcome oc = access.repointRef(key, {inlineEntry("checksums.txt", "cs")}, ProvenanceOp::Other);
    EXPECT_FALSE(oc.created);
    EXPECT_EQ(oc.manifest_ref, id.ref) << "the byte-equal outcome must name the manifest ALREADY committed, unchanged";

    EXPECT_EQ(backend->putTotal(), 0u) << "byte-equal repoint must perform ZERO pool mutations";
    EXPECT_EQ(store->resolveRef(ns, "part_1")->manifest_id, id)
        << "the committed manifest identity must be untouched";
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASRefRepoint].load(), repoints_before);
}

/// A genuinely different entry set on an already-committed ref republishes the manifest: the returned
/// `CommitOutcome` names a FRESH manifest (`created` still false -- the ref was already committed),
/// the new content resolves, and the repoint is loud (ProfileEvent + the ref's cached view erased so a
/// subsequent read serves the new manifest, not a stale retained one).
TEST(CASRepoint, AddFileRepoints)
{
    auto backend = std::make_shared<DB::Cas::tests::CountingBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);
    const RootNamespace ns{"srv/t1"};
    DB::Cas::CachedPartFolderAccess access(
        store, {.cache_bytes = 64ULL << 20, .max_entries = 10000, .max_entry_bytes = 16ULL << 20,
                .explain_enabled = false});
    const auto id_before = publishPart(store, ns, "part_1", {inlineEntry("checksums.txt", "cs")});
    const DB::Cas::PartRefKey key{ns, "part_1"};
    /// Warm the retained view so the erase-on-success cache discipline is actually exercised.
    ASSERT_NE(access.getView(key, DB::Cas::Freshness::CachedForLoad), nullptr);

    const uint64_t repoints_before = ProfileEvents::global_counters[ProfileEvents::CASRefRepoint].load();
    const std::vector<ManifestEntry> new_entries{inlineEntry("checksums.txt", "cs"), inlineEntry("metadata_version.txt", "7")};
    const DB::Cas::CommitOutcome oc = access.repointRef(key, new_entries, ProvenanceOp::Other);
    EXPECT_FALSE(oc.created);
    EXPECT_NE(oc.manifest_ref, id_before.ref);

    const auto resolved = store->resolveRef(ns, "part_1");
    ASSERT_TRUE(resolved.has_value());
    EXPECT_NE(resolved->manifest_id, id_before) << "a genuine content change must mint a fresh manifest";
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASRefRepoint].load(), repoints_before + 1);

    /// The view a caller reads next must reflect the new file, not a stale retained one.
    auto view = access.getView(key, DB::Cas::Freshness::CachedForLoad);
    ASSERT_NE(view, nullptr);
    EXPECT_TRUE(view->hasFile("metadata_version.txt"));
}
