#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Parts/PartPathParser.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasBlobInDegree.h>
#include <Disks/DiskLocal.h>
#include <Common/ErrorCodes.h>
#include <filesystem>

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

/// M-W wiring tier (design 2026-06-11 section 7 tier 3): the ClickHouse-facing translation layer
/// tested through its own seams. Task 1: PartPathParser — the path-classification rows plus the
/// shadow/detached/mutable rows the later tasks route on.

using namespace DB::Cas;

TEST(CASPartPathParser, ParsePartFilePathAtomic)
{
    auto file = parsePartFilePath("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/columns.txt");
    ASSERT_TRUE(file.has_value());
    EXPECT_EQ(file->table_uuid, "a11a11a1-1111-4111-8111-111111111111");
    EXPECT_EQ(file->part_name, "all_1_1_0");
    EXPECT_EQ(file->file, "columns.txt");
    EXPECT_TRUE(file->backup_name.empty());
    EXPECT_TRUE(file->shadow_table_dir.empty());

    auto part_dir = parsePartFilePath("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/"); // trailing slash, no file
    ASSERT_TRUE(part_dir.has_value());
    EXPECT_EQ(part_dir->part_name, "all_1_1_0");
    EXPECT_TRUE(part_dir->file.empty());

    EXPECT_FALSE(parsePartFilePath("a11/a11a11a1-1111-4111-8111-111111111111").has_value()); // table dir, not a part
    EXPECT_FALSE(parsePartFilePath("123").has_value());        // shallower

    // The real-server shape carries a leading store/; the uuid-pair anchor makes it equivalent.
    auto atomic = parsePartFilePath("store/a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin");
    ASSERT_TRUE(atomic.has_value());
    EXPECT_EQ(atomic->table_uuid, "a11a11a1-1111-4111-8111-111111111111");
    EXPECT_EQ(atomic->part_name, "all_1_1_0");
    EXPECT_EQ(atomic->file, "data.bin");
}

TEST(CASPartPathParser, ThreeCharDatabaseSharingTablePrefixDoesNotFalseAnchorAsAtomic)
{
    // T12: a non-Atomic 3-char database directory whose table directory happens to start with the
    // SAME 3 characters (db "abc", table "abcxyz") used to satisfy the old loose Atomic-anchor shape
    // check (`prefix.size() == 3 && uuid.compare(0, 3, prefix) == 0`), false-anchoring "abc" as a
    // UUID hash-prefix and "abcxyz" as the table UUID -- even though neither looks anything like a
    // real UUID. The anchor now additionally requires the prefix to be lowercase-hex and the
    // candidate to have the exact 36-char dashed UUID shape, so this path falls through to the
    // non-Atomic fallback split instead (folding the whole leading path into table_uuid, exactly like
    // ParsePartFilePathNonAtomic's "data/memory_01069/mt" case).
    auto d = parsePartFilePath("data/abc/abcxyz/1_1_1_0/x.bin");
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->table_uuid, "data/abc/abcxyz");
    EXPECT_EQ(d->part_name, "1_1_1_0");
    EXPECT_EQ(d->file, "x.bin");
}

TEST(CASPartPathParser, RealHexPrefixUuidPairStillAnchorsAsAtomic)
{
    // Positive control for the tightened anchor: a REAL Atomic on-disk shape --
    // store/<uuid[:3]>/<uuid> with the UUID correctly 36-char dashed and genuinely sharing its first
    // 3 characters with the prefix -- still anchors exactly as before.
    auto a = parsePartFilePath("store/abc/abc12345-1234-5678-9abc-def012345678/all_1_1_0/x.bin");
    ASSERT_TRUE(a.has_value());
    EXPECT_EQ(a->table_uuid, "abc12345-1234-5678-9abc-def012345678");
    EXPECT_EQ(a->part_name, "all_1_1_0");
    EXPECT_EQ(a->file, "x.bin");
}

TEST(CASPartPathParser, ParsePartFilePathProjectionSubPath)
{
    // A projection file keeps its FULL in-part relative path as the file (the tree entry name).
    auto proj = parsePartFilePath("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/p.proj/data.bin");
    ASSERT_TRUE(proj.has_value());
    EXPECT_EQ(proj->part_name, "all_1_1_0");
    EXPECT_EQ(proj->file, "p.proj/data.bin");
}

TEST(CASPartPathParser, ParsePartFilePathNonAtomic)
{
    // Non-Atomic (Ordinary/Memory/Lazy) layout: data/<db>/<table>/<part>/<file> — no uuid anchor;
    // the part dir is recognized by its block-range suffix (B40).
    auto file = parsePartFilePath("data/memory_01069/mt/all_1_1_0/data.cmrk4");
    ASSERT_TRUE(file.has_value());
    EXPECT_EQ(file->table_uuid, "data/memory_01069/mt");
    EXPECT_EQ(file->part_name, "all_1_1_0");
    EXPECT_EQ(file->file, "data.cmrk4");

    // Temporary/operation prefixes keep the suffix and stay part dirs.
    auto tmp = parsePartFilePath("data/memory_01069/mt/tmp_insert_all_1_1_0/data.cmrk4");
    ASSERT_TRUE(tmp.has_value());
    EXPECT_EQ(tmp->part_name, "tmp_insert_all_1_1_0");

    // Mutation-level form <partition>_<min>_<max>_<level>_<mutation>.
    auto mut = parsePartFilePath("data/db/tbl/20200101_1_1_0_5/data.bin");
    ASSERT_TRUE(mut.has_value());
    EXPECT_EQ(mut->part_name, "20200101_1_1_0_5");

    // A non-Atomic table-level file is NOT a part file.
    EXPECT_FALSE(isPartFilePath("data/memory_01069/mt/format_version.txt"));
    auto tf = parseTableFilePath("data/memory_01069/mt/format_version.txt");
    ASSERT_TRUE(tf.has_value());
    EXPECT_EQ(tf->table_uuid, "data/memory_01069/mt");
    EXPECT_EQ(tf->tail, "format_version.txt");

    EXPECT_EQ(parseTableUuid("data/memory_01069/mt"), std::optional<std::string>("data/memory_01069/mt"));

    // Generic disk-root files classify as nothing (verbatim passthrough).
    EXPECT_FALSE(isPartFilePath("clickhouse_access_check_xyz"));
    EXPECT_FALSE(parseTableFilePath("clickhouse_access_check_xyz").has_value());
    EXPECT_FALSE(parseTableUuid("clickhouse_access_check_xyz").has_value());
}

TEST(CASPartPathParser, ParseTableUuid)
{
    EXPECT_EQ(parseTableUuid("a11/a11a11a1-1111-4111-8111-111111111111/"), std::optional<std::string>("a11a11a1-1111-4111-8111-111111111111"));
    EXPECT_EQ(parseTableUuid("a11/a11a11a1-1111-4111-8111-111111111111"), std::optional<std::string>("a11a11a1-1111-4111-8111-111111111111"));
    EXPECT_FALSE(parseTableUuid("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0").has_value()); // part dir, not table dir

    EXPECT_TRUE(endsWithTableUuidPair("store/a11/a11a11a1-1111-4111-8111-111111111111"));
    EXPECT_FALSE(endsWithTableUuidPair("store/a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0"));
    EXPECT_FALSE(endsWithTableUuidPair("shadow/bk1/store"));
}

TEST(CASPartPathParser, ParseTableFilePathNested)
{
    // The reserved deduplication_logs/ subdir is a table-level namespace, never a part dir.
    EXPECT_FALSE(isPartFilePath("a11/a11a11a1-1111-4111-8111-111111111111/deduplication_logs/deduplication_log_1.txt"));
    auto tf = parseTableFilePath("a11/a11a11a1-1111-4111-8111-111111111111/deduplication_logs/deduplication_log_1.txt");
    ASSERT_TRUE(tf.has_value());
    EXPECT_EQ(tf->table_uuid, "a11a11a1-1111-4111-8111-111111111111");
    EXPECT_EQ(tf->tail, "deduplication_logs/deduplication_log_1.txt");

    auto flat = parseTableFilePath("a11/a11a11a1-1111-4111-8111-111111111111/format_version.txt");
    ASSERT_TRUE(flat.has_value());
    EXPECT_EQ(flat->tail, "format_version.txt");

    EXPECT_FALSE(parseTableFilePath("a11/a11a11a1-1111-4111-8111-111111111111").has_value());
    EXPECT_FALSE(parseTableFilePath("a11/a11a11a1-1111-4111-8111-111111111111/").has_value());

    EXPECT_TRUE(isPartFilePath("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin"));
}

TEST(CASPartPathParser, ShadowFreezePaths)
{
    EXPECT_TRUE(isShadowPath("shadow/bk1/store/a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin"));
    EXPECT_TRUE(isShadowPath("/shadow/bk1"));
    EXPECT_FALSE(isShadowPath("store/a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin"));
    EXPECT_FALSE(isShadowPath("shadowy/bk1"));

    auto s = parsePartFilePath("shadow/bk1/store/a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin");
    ASSERT_TRUE(s.has_value());
    EXPECT_EQ(s->table_uuid, "a11a11a1-1111-4111-8111-111111111111");
    EXPECT_EQ(s->part_name, "all_1_1_0");
    EXPECT_EQ(s->file, "data.bin");
    EXPECT_EQ(s->backup_name, "bk1");
    EXPECT_EQ(s->shadow_table_dir, "shadow/bk1/store/a11/a11a11a1-1111-4111-8111-111111111111");
}

TEST(CASPartPathParser, DetachedPathsReportTheSharedDetachedComponent)
{
    // The PoC contract (B36): "detached" parses as the part_name; the real detached part dir is
    // the first component of `file`. The transaction/read routing re-splits on this shape.
    auto d = parsePartFilePath("a11/a11a11a1-1111-4111-8111-111111111111/detached/attaching_all_0_0_0/metadata_version.txt");
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->table_uuid, "a11a11a1-1111-4111-8111-111111111111");
    EXPECT_EQ(d->part_name, std::string(kDetachedDirName));
    EXPECT_EQ(d->file, "attaching_all_0_0_0/metadata_version.txt");
}

TEST(CASPartPathParser, MovingPathsReportTheSharedMovingComponent)
{
    // Atomic layout: "moving" lands on part_idx for free (it is the component right after the
    // table <uuid>, same mechanism as "detached" -- no parser change needed here, only route()).
    auto d = parsePartFilePath("a11/a11a11a1-1111-4111-8111-111111111111/moving/all_1_1_0/data.bin");
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->table_uuid, "a11a11a1-1111-4111-8111-111111111111");
    EXPECT_EQ(d->part_name, std::string(kMovingDirName));
    EXPECT_EQ(d->file, "all_1_1_0/data.bin");
}

TEST(CASPartPathParser, MovingPathsNonAtomicFoldIntoTheTableNamespace)
{
    // Mirrors DetachedPathsNonAtomicFoldIntoTheTableNamespace (U#6): without an explicit anchor
    // the right-to-left part-dir scan would anchor on the INNER real part dir and fold "moving"
    // into a spurious table_uuid ("data/<db>/<table>/moving"), diverging from the table's real
    // namespace -- the identical bug class the detached anchor was added to prevent.
    auto d = parsePartFilePath("data/db/tbl/moving/all_1_1_0/data.bin");
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->table_uuid, "data/db/tbl");
    EXPECT_EQ(d->part_name, std::string(kMovingDirName));
    EXPECT_EQ(d->file, "all_1_1_0/data.bin");

    // The bare non-Atomic moving CONTAINER dir folds to part_name == "moving" with an empty
    // file, exactly like the Atomic container.
    auto c = parsePartFilePath("data/db/tbl/moving");
    ASSERT_TRUE(c.has_value());
    EXPECT_EQ(c->table_uuid, "data/db/tbl");
    EXPECT_EQ(c->part_name, std::string(kMovingDirName));
    EXPECT_TRUE(c->file.empty());
}

TEST(CASPartPathParser, DetachedPathsNonAtomicFoldIntoTheTableNamespace)
{
    // U#6: the Ordinary/non-Atomic detached form data/<db>/<table>/detached/<part>/<file> must fold
    // into the table's OWN namespace with part_name == "detached" (mirroring the Atomic form), so
    // route() keys the detached/<part> ref off it. The right-to-left part-dir scan would otherwise
    // anchor on the INNER part dir and fold `detached` into a spurious table_uuid
    // ("data/<db>/<table>/detached") that DROP TABLE never cleans — a permanently orphaned live ref.
    auto d = parsePartFilePath("data/db/tbl/detached/attaching_all_0_0_0/metadata_version.txt");
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->table_uuid, "data/db/tbl");
    EXPECT_EQ(d->part_name, std::string(kDetachedDirName));
    EXPECT_EQ(d->file, "attaching_all_0_0_0/metadata_version.txt");

    // The bare non-Atomic detached CONTAINER dir folds to part_name == "detached" with an empty file,
    // exactly like the Atomic container, so route()'s empty-ref branch is reached for both layouts.
    auto c = parsePartFilePath("data/db/tbl/detached");
    ASSERT_TRUE(c.has_value());
    EXPECT_EQ(c->table_uuid, "data/db/tbl");
    EXPECT_EQ(c->part_name, std::string(kDetachedDirName));
    EXPECT_TRUE(c->file.empty());
}

TEST(CASPartPathParser, DetachedNamedTableIsKnownAmbiguityFoldedAsReservedDir)
{
    // ACCEPTED LIMITATION (see the anchor-site comment in findPartDirComponent): a non-Atomic
    // database or TABLE literally named "detached" is structurally indistinguishable, from the path
    // string alone, from the reserved detached subdir of a table one level up — so it gets folded
    // as the reserved dir, not as a table name. This test PINS that known, deliberately-accepted
    // behavior (backlogged by the stabilization campaign) so any future change to it is a conscious
    // one, not an accidental regression.
    auto d = parsePartFilePath("data/db/detached/all_1_1_0/data.bin");
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->table_uuid, "data/db");
    EXPECT_EQ(d->part_name, std::string(kDetachedDirName));
    EXPECT_EQ(d->file, "all_1_1_0/data.bin");

    // Consequently the table dir itself is unrecognized: it looks like a detached container instead.
    EXPECT_FALSE(parseTableUuid("data/db/detached").has_value());
}

TEST(CASPartPathParser, RawPathSplitMemoizedAcrossClassifiers)
{
    // The CA read path runs isPartFilePath then parsePartFilePath on the SAME raw path several times
    // per logical file-open (existsFile -> getFileSize -> getStorageObjects). The split is a pure
    // function of the path, so all of those must split the path exactly ONCE (B1).
    resetSplitCacheForTest();
    const std::string path = "store/a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/columns.txt";
    EXPECT_TRUE(isPartFilePath(path));
    ASSERT_TRUE(parsePartFilePath(path).has_value());
    ASSERT_TRUE(parsePartFilePath(path).has_value());
    EXPECT_EQ(splitCacheMissesForTest(), 1u) << "the same raw path must be split only once";

    // A distinct raw path is a fresh split (miss #2); repeats of it reuse the memo.
    const std::string other = "store/a22/a22a22a2-2222-4222-8222-222222222222/all_1_1_0/data.bin";
    EXPECT_TRUE(isPartFilePath(other));
    EXPECT_TRUE(isPartFilePath(other));
    EXPECT_EQ(splitCacheMissesForTest(), 2u);

    // Correctness is unchanged: the memoized parse yields the same fields the direct parse would.
    const auto parsed = parsePartFilePath(path);
    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(parsed->table_uuid, "a11a11a1-1111-4111-8111-111111111111");
    EXPECT_EQ(parsed->part_name, "all_1_1_0");
    EXPECT_EQ(parsed->file, "columns.txt");
}

TEST(CASPartPathParser, SplitCacheEvictionStaysCorrect)
{
    // The split cache is a small fixed-capacity FIFO ring, NOT an LRU/MRU: a hit never promotes its
    // slot, so a path seen recently can still be evicted by unrelated churn through the same thread.
    // That is only ever a cache-EFFECTIVENESS tradeoff, never a correctness one: pin that once enough
    // distinct paths evict the first path's cached split, re-parsing it still yields the exact right
    // result (a forced re-split / cache miss on the re-parse is expected and fine here — the
    // assertion is correctness under eviction, not hit rate).
    resetSplitCacheForTest();
    const std::string first = "store/a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/columns.txt";
    ASSERT_TRUE(parsePartFilePath(first).has_value());

    // 8 more distinct paths churn through the ring (capacity 8), evicting `first`'s slot.
    const std::vector<std::string> table_dirs = {
        "",
        "a11/a11a11a1-1111-4111-8111-111111111111",
        "a22/a22a22a2-2222-4222-8222-222222222222",
        "a33/a33a33a3-3333-4333-8333-333333333333",
        "a44/a44a44a4-4444-4444-8444-444444444444",
        "a55/a55a55a5-5555-4555-8555-555555555555",
        "a66/a66a66a6-6666-4666-8666-666666666666",
        "a77/a77a77a7-7777-4777-8777-777777777777",
        "a88/a88a88a8-8888-4888-8888-888888888888",
        "a99/a99a99a9-9999-4999-8999-999999999999",
    };
    for (int i = 2; i <= 9; ++i)
    {
        const std::string path = "store/" + table_dirs[i] + "/all_1_1_0/columns.txt";
        ASSERT_TRUE(parsePartFilePath(path).has_value());
    }

    const size_t misses_before_reparse = splitCacheMissesForTest();
    const auto reparsed = parsePartFilePath(first);
    ASSERT_TRUE(reparsed.has_value());
    EXPECT_EQ(reparsed->table_uuid, "a11a11a1-1111-4111-8111-111111111111");
    EXPECT_EQ(reparsed->part_name, "all_1_1_0");
    EXPECT_EQ(reparsed->file, "columns.txt");
    // Confirms the re-parse really was a forced re-split (the slot was evicted), not a lucky hit.
    EXPECT_EQ(splitCacheMissesForTest(), misses_before_reparse + 1);
}

/// ==== M-W Task 2: the read side over Cas::Pool ====
/// Fixture: publish parts through the CORE API, then read through the IMetadataStorage surface of
/// the rewritten ContentAddressedMetadataStorage (real ctor over a Local object storage; the
/// backend self-selects EmulatedSingleProcess token semantics).

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedExchange.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Plain/MetadataStorageFromPlainObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasFsck.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFormat.h>
#include <Disks/tests/cas_test_helpers.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadPipeline.h>
#include <Poco/AutoPtr.h>
#include <Poco/Util/MapConfiguration.h>

using DB::Cas::tests::idOf;
using DB::Cas::tests::u128Of;

namespace DB::ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int BAD_ARGUMENTS;
}

namespace
{

DB::Cas::ManifestEntry wiringBlobEntry(const String & path, const String & payload)
{
    DB::Cas::ManifestEntry e;
    e.path = path;
    e.placement = DB::Cas::EntryPlacement::Blob;
    e.ref = DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(u128Of(payload))};

    e.blob_size = payload.size();
    return e;
}

/// All-tree-part-files Task 6/9: the small per-part files (uuid.txt, metadata_version.txt, ...) are
/// ordinary Inline-placement manifest entries now — this is the low-level PartWriteTxn-API equivalent of
/// what `ContentAddressedTransaction::writeFile`'s inline candidate path stages in production.
DB::Cas::ManifestEntry wiringInlineEntry(const String & path, const String & bytes)
{
    DB::Cas::ManifestEntry e;
    e.path = path;
    e.placement = DB::Cas::EntryPlacement::Inline;
    e.ref = DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(u128Of(bytes))};

    e.blob_size = bytes.size();
    e.inline_bytes = bytes;
    return e;
}

/// The one table identity these tests use, as a namespace LIFE: namespace files are life-keyed
/// (directive §2), resolved from the CATALOG exactly as the disk's own write path resolves it. Naming
/// the Stage-A sentinel here instead would put the fixture's files under a prefix the disk no longer
/// reads (Task 4b), so `existsFile`/`listDirectory` below would report them absent -- the fixture and
/// the code under test must agree on the life, and the only way to guarantee that is to ask the same
/// resolver.
DB::Cas::NamespaceLifeId wiringLife(DB::ContentAddressedMetadataStorage & storage)
{
    return storage.store()->namespaceLife(
        storage.liveNamespace("a11a11a1-1111-4111-8111-111111111111"));
}

std::shared_ptr<DB::ContentAddressedMetadataStorage> openWiringStorage()
{
    auto settings = DB::Cas::tests::makeSettingsForTest(
        "test", std::filesystem::temp_directory_path() / "ca_wiring_scratch");
    auto storage = std::make_shared<DB::ContentAddressedMetadataStorage>(
        DB::Cas::tests::makeLocalObjectStorageForTest(), "pool", "srv1", "", nullptr, settings);
    storage->startup();
    return storage;
}

/// One part with a content blob, a projection file, and the small per-part files (uuid.txt,
/// metadata_version.txt — ordinary Inline entries now, all-tree-part-files Task 6/9), published
/// through the real PartWriteTxn into `ns` under `ref`.
void publishWiredPart(
    DB::ContentAddressedMetadataStorage & storage, const DB::Cas::RootNamespace & ns, const String & ref)
{
    /// Port off the removed PartWriteTxn::putTree/publish API onto the part-manifest write flow
    /// (beginPartWrite → stageManifest → precommitAdd → putBlob → promote). The wiring sets the owning
    /// namespace EXPLICITLY (intended_namespace) — faithful to ContentAddressedTransaction — so a
    /// `detached/<part>` ref (which itself contains '/') is staged in the TABLE namespace, not in a
    /// spurious `<ns>/detached` namespace. intended_ref stays as "ns/ref" diagnostic forensics.
    DB::Cas::PartWriteInfo info;
    info.intended_ref = ns.string() + "/" + ref;
    info.intended_namespace = ns;
    auto build = storage.store()->beginPartWrite(info);

    /// Strictly ascending canonical path order (PartFolderView's binary-search precondition):
    /// data.bin < metadata_version.txt < p.proj/data.bin < uuid.txt.
    const auto id = build->stageManifest(
        {wiringBlobEntry("data.bin", "payload-A"), wiringInlineEntry("metadata_version.txt", "5"),
         wiringBlobEntry("p.proj/data.bin", "payload-B"), wiringInlineEntry("uuid.txt", "u-123")});
    build->precommitAdd(ns, ref, id);
    build->putBlob(idOf("payload-A"), DB::Cas::BlobSource::fromString("payload-A"));
    build->putBlob(idOf("payload-B"), DB::Cas::BlobSource::fromString("payload-B"));
    build->promote(ns, ref, build->buildId(), id);

    /// promote stamps published_at_ms with nowMs(); the read assertions want a FIXED stamp, so pin it
    /// through the set_published_at path (no journal record for anything but the stamp itself).
    storage.store()->updateRefPublishedAt(ns, ref,
        [](DB::Cas::RefPublishedAtUpdate & r) { r.published_at_ms = 1700000000ULL * 1000; });   /// epoch ms; getLastModified /1000
}

}

/// `supportsAtomicFileWrites` (all-tree task 5): the CA metadata storage publishes a file write in
/// one shot, so `VersionMetadataOnDisk::storeInfoToDataPartStorage` can skip the tmp+replace dance.
/// A plain (non-content-addressed) metadata storage keeps the base-class default of `false`.
TEST(CASWiringCapability, SupportsAtomicFileWrites)
{
    auto ca_storage = openWiringStorage();
    EXPECT_TRUE(ca_storage->supportsAtomicFileWrites());

    auto plain_storage = std::make_shared<DB::MetadataStorageFromPlainObjectStorage>(
        DB::Cas::tests::makeLocalObjectStorageForTest(), "", /*object_metadata_cache_size=*/0);
    EXPECT_FALSE(plain_storage->supportsAtomicFileWrites());
}
TEST(CASWiringRead, ResolvesPublishedPart)
{
    auto storage = openWiringStorage();
    publishWiredPart(*storage, storage->liveNamespace("a11a11a1-1111-4111-8111-111111111111"), "all_1_1_0");

    EXPECT_TRUE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin"));
    EXPECT_FALSE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/missing.bin"));
    EXPECT_EQ(storage->getFileSize("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin"), 9u);

    EXPECT_TRUE(storage->existsDirectory("a11/a11a11a1-1111-4111-8111-111111111111"));
    EXPECT_TRUE(storage->existsDirectory("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0"));
    EXPECT_FALSE(storage->existsDirectory("a11/a11a11a1-1111-4111-8111-111111111111/all_9_9_9"));
    EXPECT_FALSE(storage->existsDirectory("a22/a22a22a2-2222-4222-8222-222222222222"));

    /// Part dir listing: nested keys collapse to their first component; the publish stamp
    /// (published_at_ms typed field) never surfaces as a dir entry — every staged file does.
    auto names = storage->listDirectory("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0");
    std::sort(names.begin(), names.end());
    EXPECT_EQ(names, (std::vector<std::string>{"data.bin", "metadata_version.txt", "p.proj", "uuid.txt"}));

    auto parts = storage->listDirectory("a11/a11a11a1-1111-4111-8111-111111111111");
    EXPECT_EQ(parts, (std::vector<std::string>{"all_1_1_0"}));

    /// The part dir reports EMPTY (virtual files; B45) so removeDirectory goes straight to the
    /// ref-unlink; the table dir keeps listing-based emptiness.
    EXPECT_TRUE(storage->isDirectoryEmpty("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0"));
    EXPECT_FALSE(storage->isDirectoryEmpty("a11/a11a11a1-1111-4111-8111-111111111111"));

    /// Blob-backed file: a real key, PAYLOAD-sized (the envelope header is a read-path concern).
    auto objects = storage->getStorageObjects("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin");
    ASSERT_EQ(objects.size(), 1u);
    EXPECT_FALSE(objects[0].remote_path.empty());
    EXPECT_EQ(objects[0].bytes_size, 9u);

    /// Small Inline entry: bytes live in the shard manifest, not as their own object.
    EXPECT_TRUE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/uuid.txt"));
    EXPECT_EQ(storage->getFileSize("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/uuid.txt"), 5u);
    EXPECT_EQ(storage->tryGetInManifestBytes("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/uuid.txt"), std::optional<String>("u-123"));
    auto mobj = storage->getStorageObjects("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/uuid.txt");
    ASSERT_EQ(mobj.size(), 1u);
    EXPECT_TRUE(mobj[0].remote_path.empty());   /// sized placeholder; bytes ride prepareInManifestRead

    /// The typed publish stamp (published_at_ms epoch ms) backs getLastModified for the part dir and its files.
    EXPECT_EQ(storage->getLastModified("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0").epochTime(), 1700000000);
    EXPECT_EQ(storage->getLastModified("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin").epochTime(), 1700000000);
}

TEST(CASWiringRead, BlobViewPlanRidesTheStandardPipeline)
{
    /// The committed read path (B116): an in-manifest file is served from memory via
    /// prepareInManifestRead; a blob-backed file translates to its physical blob object +
    /// payload window (getBlobViewPlan) and rides the STANDARD object-storage pipeline,
    /// bounded by the FileView stage — composed here the way DiskObjectStorage::prepareRead
    /// composes it.
    auto object_storage = DB::Cas::tests::makeLocalObjectStorageForTest();
    auto settings = DB::Cas::tests::makeSettingsForTest(
        "test", std::filesystem::temp_directory_path() / "ca_wiring_scratch");
    auto storage = std::make_shared<DB::ContentAddressedMetadataStorage>(
        object_storage, "pool", "srv1", "", nullptr, settings);
    storage->startup();
    publishWiredPart(*storage, storage->liveNamespace("a11a11a1-1111-4111-8111-111111111111"), "all_1_1_0");

    /// In-manifest file: memory source, no blob plan.
    DB::ReadPipeline manifest_pipeline;
    ASSERT_TRUE(storage->prepareInManifestRead("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/uuid.txt", DB::ReadSettings{}, manifest_pipeline));
    String manifest_bytes;
    {
        auto buf = manifest_pipeline.build();
        DB::readStringUntilEOF(manifest_bytes, *buf);
    }
    EXPECT_EQ(manifest_bytes, "u-123");
    /// Not a `getBlobViewPlan` call on the in-manifest path here (all-tree Task 6/9: uuid.txt is now
    /// a real Inline manifest entry): `getBlobViewPlan`'s only production caller
    /// (`DiskObjectStorage::prepareRead`) never reaches it once `prepareInManifestRead` returns true
    /// above — `getBlobViewPlan`'s precondition is "confirmed not in-manifest-servable," which calling
    /// it directly on an Inline path violates. Pre-Task-9 this assertion passed only by coincidence
    /// (uuid.txt was not a manifest entry at all, so `findFile` returned not-found, not because
    /// `getBlobViewPlan` gracefully handles an Inline entry it does find).

    /// Blob-backed file: a real physical key and a payload-sized window whose extent equals
    /// the object's readable size (a right-bounded read never overshoots the window).
    const std::string path = "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin";
    auto plan = storage->getBlobViewPlan(path);
    ASSERT_TRUE(plan.has_value());
    EXPECT_FALSE(plan->object.remote_path.empty());
    EXPECT_EQ(plan->object.local_path, path);
    EXPECT_EQ(plan->payload_end - plan->payload_offset, 9u);
    EXPECT_EQ(plan->object.bytes_size, plan->payload_end);
    EXPECT_FALSE(storage->prepareInManifestRead(path, DB::ReadSettings{}, manifest_pipeline = {}));

    auto make_pipeline = [&]
    {
        DB::ReadPipeline pipeline;
        pipeline.setSource(object_storage, {plan->object}, DB::ReadSettings{});
        pipeline.needGather();
        pipeline.needFileView(path, plan->payload_offset, plan->payload_end);
        return pipeline;
    };
    EXPECT_EQ(make_pipeline().describe(), "Source(ObjectStorage) -> Gather -> FileView");

    {
        auto buf = make_pipeline().build();
        EXPECT_EQ(buf->getFileName(), path);
        EXPECT_EQ(buf->tryGetFileSize(), std::optional<size_t>(9));
        String bytes;
        DB::readStringUntilEOF(bytes, *buf);
        EXPECT_EQ(bytes, "payload-A");
    }

    /// Right-bounded read through the view (the MergeTreeReaderStream::adjustRightMark shape):
    /// the bound is window-relative and forwarded down the chain.
    {
        auto buf = make_pipeline().build();
        buf->setReadUntilPosition(7);
        String head(7, '\0');
        buf->readStrict(head.data(), 7);
        EXPECT_EQ(head, "payload");
        EXPECT_TRUE(buf->eof());
        buf->setReadUntilEnd();
        String tail;
        DB::readStringUntilEOF(tail, *buf);
        EXPECT_EQ(tail, "-A");
    }

    /// Seek inside the window.
    {
        auto buf = make_pipeline().build();
        buf->seek(8, SEEK_SET);
        String last;
        DB::readStringUntilEOF(last, *buf);
        EXPECT_EQ(last, "A");
    }
}

TEST(CASWiringRead, ProjectionDirectory)
{
    auto storage = openWiringStorage();
    publishWiredPart(*storage, storage->liveNamespace("a11a11a1-1111-4111-8111-111111111111"), "all_1_1_0");

    EXPECT_TRUE(storage->existsDirectory("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/p.proj"));
    EXPECT_FALSE(storage->existsDirectory("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/q.proj"));
    EXPECT_EQ(storage->listDirectory("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/p.proj"), (std::vector<std::string>{"data.bin"}));
    EXPECT_TRUE(storage->isDirectoryEmpty("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/p.proj"));   /// B60
    EXPECT_TRUE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/p.proj/data.bin"));
    EXPECT_EQ(storage->getFileSize("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/p.proj/data.bin"), 9u);
}

TEST(CASWiringRead, DetachedFoldedIntoTableNamespace)
{
    auto storage = openWiringStorage();
    /// B181: a detached part is a `detached/`-prefixed ref INSIDE the table's own archive namespace,
    /// not a separate sibling namespace. Publish it that way through the core, and ALSO a live part
    /// that shares the same base name to prove the live↔detached collision is impossible (the ref
    /// names `all_1_1_0` and `detached/all_1_1_0` differ — one namespace, no re-split needed).
    publishWiredPart(*storage, storage->liveNamespace("a11a11a1-1111-4111-8111-111111111111"), "detached/broken_all_1_1_0");
    publishWiredPart(*storage, storage->liveNamespace("a11a11a1-1111-4111-8111-111111111111"), "broken_all_1_1_0");

    /// The TABLE dir collapses the `detached/<part>` refs to the single `detached` subdir entry
    /// alongside the live part name.
    auto top = storage->listDirectory("a11/a11a11a1-1111-4111-8111-111111111111");
    std::sort(top.begin(), top.end());
    EXPECT_EQ(top, (std::vector<std::string>{"broken_all_1_1_0", "detached"}));

    /// The detached CONTAINER lists the detached part DIRECTORY names (B36's intent), prefix-stripped
    /// — and NOT the live part of the same base name.
    EXPECT_TRUE(storage->existsDirectory("a11/a11a11a1-1111-4111-8111-111111111111/detached"));
    EXPECT_EQ(storage->listDirectory("a11/a11a11a1-1111-4111-8111-111111111111/detached"), (std::vector<std::string>{"broken_all_1_1_0"}));
    /// A single detached part dir + its files (the detached part is its own `detached/`-prefixed ref).
    EXPECT_TRUE(storage->existsDirectory("a11/a11a11a1-1111-4111-8111-111111111111/detached/broken_all_1_1_0"));
    auto names = storage->listDirectory("a11/a11a11a1-1111-4111-8111-111111111111/detached/broken_all_1_1_0");
    std::sort(names.begin(), names.end());
    EXPECT_EQ(names, (std::vector<std::string>{"data.bin", "metadata_version.txt", "p.proj", "uuid.txt"}));
    /// The B62 shape: a detached part's mutable file resolves through the `detached/`-prefixed ref.
    EXPECT_TRUE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/detached/broken_all_1_1_0/metadata_version.txt"));
    EXPECT_EQ(storage->tryGetInManifestBytes("a11/a11a11a1-1111-4111-8111-111111111111/detached/broken_all_1_1_0/metadata_version.txt"),
              std::optional<String>("5"));
    EXPECT_TRUE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/detached/broken_all_1_1_0/data.bin"));
}

TEST(CASWiringRoute, DetachedFoldsIntoTableNamespaceWithPrefixedRef)
{
    /// B181: a detached part file routes to the table's OWN archive namespace under a
    /// `detached/`-prefixed ref — NOT a separate sibling namespace.
    auto storage = openWiringStorage();
    auto p = parsePartFilePath("store/a11/a11a11a1-1111-4111-8111-111111111111/detached/broken_all_1_1_0/data.bin");
    ASSERT_TRUE(p.has_value());
    auto r = storage->route(*p);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->ns.string(), storage->liveNamespace("a11a11a1-1111-4111-8111-111111111111").string());
    EXPECT_EQ(r->ref, "detached/broken_all_1_1_0");
    EXPECT_EQ(r->file, "data.bin");

    /// The detached CONTAINER dir routes to the table ns with an empty ref (filtered listing).
    auto pc = parsePartFilePath("store/a11/a11a11a1-1111-4111-8111-111111111111/detached/broken_all_1_1_0");
    ASSERT_TRUE(pc.has_value());
    auto rc = storage->route(*pc);
    ASSERT_TRUE(rc.has_value());
    EXPECT_EQ(rc->ns.string(), storage->liveNamespace("a11a11a1-1111-4111-8111-111111111111").string());
    EXPECT_EQ(rc->ref, "detached/broken_all_1_1_0");
    EXPECT_TRUE(rc->file.empty());
}

TEST(CASWiringRoute, MovingFoldsOntoAPrefixedStagingRef)
{
    /// L1 (MOVE-to-CA fix): the mover clones a part under TABLE/moving/<part>/ before the
    /// atomic rename into place. Mirroring `detached`, a moved part resolves onto a
    /// `moving/`-PREFIXED staging ref -- NOT the part's final live ref directly. Publishing under
    /// the final ref before the mover's swap would break move crash-atomicity (a crash between the
    /// clone commit and the swap would leave a committed live ref that never went through the
    /// swap). The staging ref keeps the pre-swap clone un-live; the mover's rename does a real ref
    /// repoint moving/<part> -> <part>.
    auto storage = openWiringStorage();
    auto p = parsePartFilePath("store/a11/a11a11a1-1111-4111-8111-111111111111/moving/all_1_1_0/data.bin");
    ASSERT_TRUE(p.has_value());
    EXPECT_EQ(p->part_name, std::string(kMovingDirName));
    EXPECT_EQ(p->file, "all_1_1_0/data.bin");

    auto r = storage->route(*p);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->ns.string(), storage->liveNamespace("a11a11a1-1111-4111-8111-111111111111").string());
    EXPECT_EQ(r->ref, "moving/all_1_1_0");
    EXPECT_EQ(r->file, "data.bin");

    /// The bare moving CONTAINER dir TABLE/moving routes to the table ns with an empty ref.
    auto pc = parsePartFilePath("store/a11/a11a11a1-1111-4111-8111-111111111111/moving");
    ASSERT_TRUE(pc.has_value());
    auto rc = storage->route(*pc);
    ASSERT_TRUE(rc.has_value());
    EXPECT_EQ(rc->ns.string(), storage->liveNamespace("a11a11a1-1111-4111-8111-111111111111").string());
    EXPECT_TRUE(rc->ref.empty());
    EXPECT_TRUE(rc->file.empty());
}

TEST(CASWiringRead, ShadowFreezeTree)
{
    auto storage = openWiringStorage();
    publishWiredPart(*storage, DB::ContentAddressedMetadataStorage::shadowNamespace("shadow/bk1/store/a11/a11a11a1-1111-4111-8111-111111111111"), "all_1_1_0");

    /// Intermediate dirs derive from the registered shadow namespaces.
    EXPECT_TRUE(storage->existsDirectory("shadow/bk1"));
    EXPECT_TRUE(storage->existsDirectory("shadow/bk1/store"));
    EXPECT_FALSE(storage->existsDirectory("shadow/bk2"));
    EXPECT_EQ(storage->listDirectory("shadow"), (std::vector<std::string>{"bk1"}));
    EXPECT_EQ(storage->listDirectory("shadow/bk1"), (std::vector<std::string>{"store"}));
    EXPECT_EQ(storage->listDirectory("shadow/bk1/store"), (std::vector<std::string>{"a11"}));
    EXPECT_EQ(storage->listDirectory("shadow/bk1/store/a11"), (std::vector<std::string>{"a11a11a1-1111-4111-8111-111111111111"}));
    /// Shadow TABLE dir (strict uuid-pair anchor) and PART dir.
    EXPECT_TRUE(storage->existsDirectory("shadow/bk1/store/a11/a11a11a1-1111-4111-8111-111111111111"));
    EXPECT_EQ(storage->listDirectory("shadow/bk1/store/a11/a11a11a1-1111-4111-8111-111111111111"), (std::vector<std::string>{"all_1_1_0"}));
    EXPECT_TRUE(storage->existsDirectory("shadow/bk1/store/a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0"));
    auto names = storage->listDirectory("shadow/bk1/store/a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0");
    std::sort(names.begin(), names.end());
    EXPECT_EQ(names, (std::vector<std::string>{"data.bin", "metadata_version.txt", "p.proj", "uuid.txt"}));
    EXPECT_TRUE(storage->existsFile("shadow/bk1/store/a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin"));
    EXPECT_EQ(storage->getFileSize("shadow/bk1/store/a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin"), 9u);
}

TEST(CASWiringRead, VerbatimNamespaceFiles)
{
    auto storage = openWiringStorage();
    EXPECT_TRUE(storage->liveNamespace("a11a11a1-1111-4111-8111-111111111111").string().starts_with("test/"))
        << storage->liveNamespace("a11a11a1-1111-4111-8111-111111111111").string();
    EXPECT_NE(storage->liveNamespace("a11a11a1-1111-4111-8111-111111111111").string().find("/store/a11/a11a11a1-1111-4111-8111-111111111111@cas@"), std::string::npos)
        << storage->liveNamespace("a11a11a1-1111-4111-8111-111111111111").string();

    publishWiredPart(*storage, storage->liveNamespace("a11a11a1-1111-4111-8111-111111111111"), "all_1_1_0");
    storage->store()->putNamespaceFile(wiringLife(*storage), "format_version.txt", "1\n");
    storage->store()->putNamespaceFile(
        wiringLife(*storage), "deduplication_logs/deduplication_log_1.txt", "log-bytes");
    /// Loose disk-root files are plain mountpoint objects (design §5.2), not namespace files.
    storage->store()->putMountpointObject(storage->serverRootId() + "/" + "clickhouse_access_check_xyz", "ok");

    EXPECT_TRUE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/format_version.txt"));
    EXPECT_EQ(storage->getFileSize("a11/a11a11a1-1111-4111-8111-111111111111/format_version.txt"), 2u);
    EXPECT_EQ(storage->tryGetInManifestBytes("a11/a11a11a1-1111-4111-8111-111111111111/format_version.txt"), std::optional<String>("1\n"));

    /// Table dir listing merges part names + verbatim file first components.
    auto names = storage->listDirectory("a11/a11a11a1-1111-4111-8111-111111111111");
    std::sort(names.begin(), names.end());
    EXPECT_EQ(names, (std::vector<std::string>{"all_1_1_0", "deduplication_logs", "format_version.txt"}));

    /// The reserved table-level subdir.
    EXPECT_TRUE(storage->existsDirectory("a11/a11a11a1-1111-4111-8111-111111111111/deduplication_logs"));
    EXPECT_EQ(storage->listDirectory("a11/a11a11a1-1111-4111-8111-111111111111/deduplication_logs"),
              (std::vector<std::string>{"deduplication_log_1.txt"}));
    EXPECT_TRUE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/deduplication_logs/deduplication_log_1.txt"));

    /// Loose disk-root files are plain objects — existsFile checks the mountpoint object, not a namespace file.
    EXPECT_TRUE(storage->existsFile("clickhouse_access_check_xyz"));
    /// Loose files are real objects — tryGetInManifestBytes returns nullopt (not in-manifest bytes).
    EXPECT_EQ(storage->tryGetInManifestBytes("clickhouse_access_check_xyz"), std::nullopt);
    EXPECT_EQ(storage->getFileSize("clickhouse_access_check_xyz"), 2u);
    EXPECT_FALSE(storage->existsFile("clickhouse_access_check_other"));
}

/// `DirShape::TableDir`'s `existsDirectory` used to answer "has at least one committed part", so an
/// Atomic table that only ever wrote its namespace-level `format_version.txt` (no part published yet)
/// reported its own root as absent. `existsDirectory` is the precheck `MergeTreeData::dropAllData`
/// uses to decide whether `removeRecursive`/`dropNamespace` needs to run at all -- a false negative
/// here means `DROP TABLE` on such a table never admits removal, leaking a `Live` catalog row forever.
TEST(CASWiringRead, TableRootExistsWithNamespaceFilesButNoCommittedRef)
{
    auto storage = openWiringStorage();
    storage->store()->putNamespaceFile(wiringLife(*storage), "format_version.txt", "1\n");
    EXPECT_TRUE(storage->existsDirectory("a11/a11a11a1-1111-4111-8111-111111111111"));
}

/// Same defect, non-Atomic fallback shape (`parseTableUuid` folds the whole leading path into the
/// "uuid"): a files-only table under `data/<db>/<table>` must be present too.
TEST(CASWiringRead, TableRootExistsWithNamespaceFilesButNoCommittedRefNonAtomic)
{
    auto storage = openWiringStorage();
    const auto ns = storage->liveNamespace("data/memory_01069/mt");
    storage->store()->putNamespaceFile(storage->store()->namespaceLife(ns), "format_version.txt", "1\n");
    EXPECT_TRUE(storage->existsDirectory("data/memory_01069/mt"));
}

/// A cataloged `Live` life with ZERO refs and ZERO namespace files -- not just zero refs -- must still
/// report present. `namespaceLife` is the write-side resolution that mints a `Live` catalog row on
/// first touch; calling it alone (no ref, no namespace file written afterward) is the minimal way to
/// reach this state, and it prevents a future regression from "catalog OR files" back to "files only".
TEST(CASWiringRead, EmptyCatalogedLiveTableRootExists)
{
    auto storage = openWiringStorage();
    (void)storage->store()->namespaceLife(storage->liveNamespace("a55a55a5-5555-4555-8555-555555555555"));
    EXPECT_TRUE(storage->existsDirectory("a55/a55a55a5-5555-4555-8555-555555555555"));
}

/// C4: the fixed dispatch order is the invariant. Pins the two ambiguous early guards that make the
/// order load-bearing: store/<u3> (AtomicShard, ambiguous with the non-Atomic table fallback) and a
/// shadow table dir (which also satisfies parseTableUuid). existsDirectory/listDirectory must agree.
TEST(CASWiringRoute, DirShapeDispatchOrderIsStable)
{
    auto storage = openWiringStorage();
    publishWiredPart(*storage, storage->liveNamespace("a11a11a1-1111-4111-8111-111111111111"), "all_1_1_0");
    publishWiredPart(*storage, DB::ContentAddressedMetadataStorage::shadowNamespace("shadow/bk1/store/a11/a11a11a1-1111-4111-8111-111111111111"), "all_1_1_0");

    using DS = DB::ContentAddressedMetadataStorage::DirShape;
    EXPECT_EQ(storage->classifyDirectoryForTest("store/uui").shape,             DS::AtomicShard);
    EXPECT_EQ(storage->classifyDirectoryForTest("a11/a11a11a1-1111-4111-8111-111111111111").shape,            DS::TableDir);
    EXPECT_EQ(storage->classifyDirectoryForTest("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0").shape,  DS::PartDir);
    EXPECT_EQ(storage->classifyDirectoryForTest("a11/a11a11a1-1111-4111-8111-111111111111/detached").shape,   DS::DetachedContainer);
    EXPECT_EQ(storage->classifyDirectoryForTest("a11/a11a11a1-1111-4111-8111-111111111111/moving").shape,    DS::MovingContainer);
    EXPECT_EQ(storage->classifyDirectoryForTest("shadow/bk1/store/a11/a11a11a1-1111-4111-8111-111111111111").shape, DS::ShadowTable);
    EXPECT_EQ(storage->classifyDirectoryForTest("shadow/bk1").shape,            DS::ShadowIntermediate);
    EXPECT_EQ(storage->classifyDirectoryForTest("a11/a11a11a1-1111-4111-8111-111111111111/deduplication_logs").shape, DS::TableSubdir);
    EXPECT_EQ(storage->classifyDirectoryForTest("store").shape,                 DS::GenericIntermediate);
}

/// ==== M-W Task 3: the write path through IMetadataTransaction ====


namespace
{

void writeThroughTransaction(DB::IMetadataTransaction & tx, const String & path, const String & bytes)
{
    auto & ca_tx = dynamic_cast<DB::ContentAddressedTransaction &>(tx);
    auto buf = ca_tx.writeFile(path, 65536, DB::WriteMode::Rewrite, {});
    buf->write(bytes.data(), bytes.size());
    buf->finalize();
}

}

TEST(CASWiringWrite, ContentRoundTripThroughTransaction)
{
    auto storage = openWiringStorage();

    auto tx = storage->createTransaction();
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin", "content-A");
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/checksums.txt", "sums");
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/uuid.txt", "u-42");
    /// Nothing visible before commit.
    EXPECT_FALSE(storage->existsDirectory("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0"));
    tx->commit(DB::NoCommitOptions{});

    EXPECT_TRUE(storage->existsDirectory("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0"));
    EXPECT_TRUE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin"));
    EXPECT_EQ(storage->getFileSize("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin"), 9u);
    EXPECT_EQ(storage->tryGetInManifestBytes("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/uuid.txt"), std::optional<String>("u-42"));
    auto names = storage->listDirectory("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0");
    std::sort(names.begin(), names.end());
    EXPECT_EQ(names, (std::vector<std::string>{"checksums.txt", "data.bin", "uuid.txt"}));
    /// The publish stamp was added automatically and is filtered from listings.
    EXPECT_GT(storage->getLastModified("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0").epochTime(), 1700000000);
}

TEST(CASWiringWrite, InlineOnlyPartPublishesWithoutBuildCrash)
{
    /// Regression (CRASH-CA-S3 "staged entries without a PartWriteTxn"): a part whose files are ALL inline
    /// — no `partFileMustStayBlob` file (`.bin`/`.mrk*`/`primary.idx`), e.g. an EMPTY merge output that
    /// writes only `checksums.txt`/`count.txt` and no `data.bin` — staged manifest entries via the
    /// inline write path, which did NOT establish a PartWriteTxn (only the blob path did, via `buildFor`). So
    /// `publishStaging` reached its `st.build != nullptr` invariant with entries but no PartWriteTxn and threw
    /// LOGICAL_ERROR — a SERVER CRASH under `abort_on_logical_error`. Writing only inline metadata files
    /// to a fresh part and committing must SUCCEED and publish the part. (Bug pre-existed the inline-files
    /// feature; fix: the inline path now calls `buildFor` like the blob path.)
    auto storage = openWiringStorage();
    auto tx = storage->createTransaction();
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/checksums.txt", "sums");   // inline (no blob)
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/count.txt", "0");          // inline (no blob)
    EXPECT_NO_THROW(tx->commit(DB::NoCommitOptions{}));

    EXPECT_TRUE(storage->existsDirectory("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0"));
    EXPECT_EQ(storage->tryGetInManifestBytes("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/checksums.txt"), std::optional<String>("sums"));
    EXPECT_EQ(storage->tryGetInManifestBytes("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/count.txt"), std::optional<String>("0"));
    auto names = storage->listDirectory("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0");
    std::sort(names.begin(), names.end());
    EXPECT_EQ(names, (std::vector<std::string>{"checksums.txt", "count.txt"}));
}

TEST(CASWiringWrite, IdenticalContentDedupsToOneBlob)
{
    auto storage = openWiringStorage();

    auto tx = storage->createTransaction();
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin", "same-bytes");
    tx->commit(DB::NoCommitOptions{});
    auto tx2 = storage->createTransaction();
    writeThroughTransaction(*tx2, "a11/a11a11a1-1111-4111-8111-111111111111/all_2_2_0/data.bin", "same-bytes");
    tx2->commit(DB::NoCommitOptions{});

    /// Identical content => the SAME blob object (the key is the content hash).
    auto a = storage->getStorageObjects("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin");
    auto b = storage->getStorageObjects("a11/a11a11a1-1111-4111-8111-111111111111/all_2_2_0/data.bin");
    ASSERT_EQ(a.size(), 1u);
    ASSERT_EQ(b.size(), 1u);
    EXPECT_EQ(a[0].remote_path, b[0].remote_path);
}

TEST(CASWiringWrite, UncommittedTransactionPublishesNothing)
{
    auto storage = openWiringStorage();
    {
        auto tx = storage->createTransaction();
        writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin", "doomed");
        /// destroyed without commit => PartWriteTxn abandoned (uploads are heartbeat-gated debris)
    }
    EXPECT_FALSE(storage->existsDirectory("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0"));
    EXPECT_FALSE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin"));
}

TEST(CASWiringWrite, MutableOnlyUpdateOnCommittedPart)
{
    auto storage = openWiringStorage();
    auto tx = storage->createTransaction();
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin", "content-A");
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/txn_version.txt", "v1");
    tx->commit(DB::NoCommitOptions{});

    /// The MVCC autocommit one-shot shape: a fresh transaction rewriting ONLY a mutable file of a
    /// COMMITTED part goes through updateRefPublishedAt (no tree rebuild, no journal record).
    auto tx2 = storage->createTransaction();
    writeThroughTransaction(*tx2, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/txn_version.txt", "v2");
    tx2->commit(DB::NoCommitOptions{});

    EXPECT_EQ(storage->tryGetInManifestBytes("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/txn_version.txt"), std::optional<String>("v2"));
    EXPECT_TRUE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin"));   /// the tree is untouched
}

TEST(CASWiringWrite, VerbatimFilesDurableOnFinalizeAndAppendable)
{
    auto storage = openWiringStorage();
    auto tx = storage->createTransaction();
    /// Verbatim files are durable on FINALIZE, with no commit (the disk layer's autocommit
    /// contract for table-level files).
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/mutation_5.txt", "commands\n");
    EXPECT_TRUE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/mutation_5.txt"));

    /// Append = read-modify-rewrite (the MVCC mutation-entry CSN append).
    {
        auto & ca_tx = dynamic_cast<DB::ContentAddressedTransaction &>(*tx);
        auto buf = ca_tx.writeFile("a11/a11a11a1-1111-4111-8111-111111111111/mutation_5.txt", 65536, DB::WriteMode::Append, {});
        buf->write("csn 42\n", 7);
        buf->finalize();
    }
    EXPECT_EQ(storage->tryGetInManifestBytes("a11/a11a11a1-1111-4111-8111-111111111111/mutation_5.txt"),
              std::optional<String>("commands\ncsn 42\n"));
}

/// ==== M-W Tasks 5-7: carry-forward, renames, removals, detached/ATTACH/FREEZE ====

TEST(CASWiringOps, HardLinkCarriesForwardWithoutReupload)
{
    auto storage = openWiringStorage();
    auto tx = storage->createTransaction();
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin", "shared-payload");
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/uuid.txt", "u-1");
    tx->commit(DB::NoCommitOptions{});

    /// A mutation/merge carries unchanged files into the new part by hardlink.
    auto tx2 = storage->createTransaction();
    tx2->createHardLink("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin", "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_5/data.bin");
    tx2->createHardLink("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/uuid.txt", "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_5/uuid.txt");
    tx2->commit(DB::NoCommitOptions{});

    EXPECT_TRUE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_5/data.bin"));
    EXPECT_EQ(storage->getStorageObjects("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin")[0].remote_path,
              storage->getStorageObjects("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_5/data.bin")[0].remote_path);
    EXPECT_EQ(storage->tryGetInManifestBytes("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_5/uuid.txt"), std::optional<String>("u-1"));
}

TEST(CASWiringOps, TmpToFinalRenamePublishesUnderFinalName)
{
    auto storage = openWiringStorage();
    auto tx = storage->createTransaction();
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/tmp_insert_all_1_1_0/data.bin", "fresh");
    tx->moveDirectory("a11/a11a11a1-1111-4111-8111-111111111111/tmp_insert_all_1_1_0", "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0");
    tx->commit(DB::NoCommitOptions{});

    EXPECT_TRUE(storage->existsDirectory("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0"));
    EXPECT_FALSE(storage->existsDirectory("a11/a11a11a1-1111-4111-8111-111111111111/tmp_insert_all_1_1_0"));
    EXPECT_TRUE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin"));
}

TEST(CASWiringOps, CommittedPartRenameMovesTheRef)
{
    auto storage = openWiringStorage();
    auto tx = storage->createTransaction();
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin", "bytes");
    tx->commit(DB::NoCommitOptions{});

    /// MergeTree renames a part to delete_tmp_<part> before removing it.
    auto tx2 = storage->createTransaction();
    tx2->moveDirectory("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0", "a11/a11a11a1-1111-4111-8111-111111111111/delete_tmp_all_1_1_0");
    tx2->commit(DB::NoCommitOptions{});

    EXPECT_FALSE(storage->existsDirectory("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0"));
    EXPECT_TRUE(storage->existsDirectory("a11/a11a11a1-1111-4111-8111-111111111111/delete_tmp_all_1_1_0"));
    EXPECT_TRUE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/delete_tmp_all_1_1_0/data.bin"));
}

TEST(CASWiringOps, ProjectionTmpRenameRekeysStagedEntries)
{
    auto storage = openWiringStorage();
    auto tx = storage->createTransaction();
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin", "main");
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/p_1.tmp_proj/data.bin", "proj");
    auto & ca_tx = dynamic_cast<DB::ContentAddressedTransaction &>(*tx);
    ca_tx.moveDirectory("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/p_1.tmp_proj", "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/p.proj");
    tx->commit(DB::NoCommitOptions{});

    EXPECT_TRUE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/p.proj/data.bin"));
    EXPECT_FALSE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/p_1.tmp_proj/data.bin"));
    EXPECT_EQ(storage->listDirectory("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/p.proj"), (std::vector<std::string>{"data.bin"}));
}

TEST(CASWiringOps, DetachAttachRoundTrip)
{
    auto storage = openWiringStorage();
    auto tx = storage->createTransaction();
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin", "detachable");
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/metadata_version.txt", "3");
    tx->commit(DB::NoCommitOptions{});

    /// DETACH: a committed part moves into the detached namespace - pure ref ops.
    auto tx2 = storage->createTransaction();
    tx2->moveDirectory("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0", "a11/a11a11a1-1111-4111-8111-111111111111/detached/all_1_1_0");
    tx2->commit(DB::NoCommitOptions{});
    EXPECT_FALSE(storage->existsDirectory("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0"));
    EXPECT_TRUE(storage->existsDirectory("a11/a11a11a1-1111-4111-8111-111111111111/detached/all_1_1_0"));
    EXPECT_EQ(storage->tryGetInManifestBytes("a11/a11a11a1-1111-4111-8111-111111111111/detached/all_1_1_0/metadata_version.txt"),
              std::optional<String>("3"));

    /// ATTACH: stage-rename within detached, then publish back into the live namespace.
    auto tx3 = storage->createTransaction();
    tx3->moveDirectory("a11/a11a11a1-1111-4111-8111-111111111111/detached/all_1_1_0", "a11/a11a11a1-1111-4111-8111-111111111111/detached/attaching_all_1_1_0");
    tx3->commit(DB::NoCommitOptions{});
    auto tx4 = storage->createTransaction();
    tx4->moveDirectory("a11/a11a11a1-1111-4111-8111-111111111111/detached/attaching_all_1_1_0", "a11/a11a11a1-1111-4111-8111-111111111111/all_2_2_0");
    tx4->commit(DB::NoCommitOptions{});

    EXPECT_TRUE(storage->existsDirectory("a11/a11a11a1-1111-4111-8111-111111111111/all_2_2_0"));
    EXPECT_TRUE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/all_2_2_0/data.bin"));
    EXPECT_FALSE(storage->existsDirectory("a11/a11a11a1-1111-4111-8111-111111111111/detached/attaching_all_1_1_0"));
    EXPECT_EQ(storage->listDirectory("a11/a11a11a1-1111-4111-8111-111111111111/detached"), (std::vector<std::string>{}));
}

TEST(CASWiringOps, RemovalsDropRefsAndNamespaces)
{
    auto storage = openWiringStorage();
    auto tx = storage->createTransaction();
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin", "gone-soon");
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_2_2_0/data.bin", "stays");
    tx->commit(DB::NoCommitOptions{});
    storage->store()->putNamespaceFile(wiringLife(*storage), "format_version.txt", "1\n");

    /// The fast-removal path (all-tree Task 8, B123 evolution): per-file unlinks stage removal marks
    /// (`content_removed`) but nothing durable changes until commit; removeDirectory(<part>) drops the
    /// ref and supersedes any marks staged for it in the SAME transaction — still exactly one ref-drop,
    /// zero repoints. `existsFile` below stays true because this whole sequence is one uncommitted
    /// transaction (`tx2`), not because the unlink was a no-op.
    auto tx2 = storage->createTransaction();
    tx2->unlinkFile("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin", false, false);
    EXPECT_TRUE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin"));   /// still committed
    tx2->removeDirectory("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0");
    EXPECT_FALSE(storage->existsDirectory("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0"));
    EXPECT_TRUE(storage->existsDirectory("a11/a11a11a1-1111-4111-8111-111111111111/all_2_2_0"));

    /// DROP TABLE: removeRecursive on the table dir drops the live + detached namespaces.
    tx2->removeRecursive("a11/a11a11a1-1111-4111-8111-111111111111", {});
    EXPECT_FALSE(storage->existsDirectory("a11/a11a11a1-1111-4111-8111-111111111111"));
    EXPECT_FALSE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/format_version.txt"));
}

/// The negative test that forbids hooking last-part removal as table-drop admission: removing a
/// table's ONLY part is indistinguishable, from that call alone, from a merge, a TTL cleanup, or a
/// `TRUNCATE` that leaves the table usable. The root must stay present, and a fresh part must still be
/// publishable into it.
TEST(CASWiringOps, LastRefRemovalIsNotNamespaceRemoval)
{
    auto storage = openWiringStorage();
    auto tx = storage->createTransaction();
    writeThroughTransaction(*tx, "a66/a66a66a6-6666-4666-8666-666666666666/all_1_1_0/data.bin", "only-part");
    tx->commit(DB::NoCommitOptions{});
    storage->store()->putNamespaceFile(
        storage->store()->namespaceLife(storage->liveNamespace("a66a66a6-6666-4666-8666-666666666666")),
        "format_version.txt", "1\n");

    auto tx2 = storage->createTransaction();
    tx2->removeDirectory("a66/a66a66a6-6666-4666-8666-666666666666/all_1_1_0");
    EXPECT_FALSE(storage->existsDirectory("a66/a66a66a6-6666-4666-8666-666666666666/all_1_1_0"));
    EXPECT_TRUE(storage->existsDirectory("a66/a66a66a6-6666-4666-8666-666666666666"))
        << "removing the table's last part must not be treated as DROP TABLE admission";

    auto tx3 = storage->createTransaction();
    writeThroughTransaction(*tx3, "a66/a66a66a6-6666-4666-8666-666666666666/all_2_2_0/data.bin", "new-part");
    tx3->commit(DB::NoCommitOptions{});
    EXPECT_TRUE(storage->existsDirectory("a66/a66a66a6-6666-4666-8666-666666666666/all_2_2_0"))
        << "the namespace never transitioned to Removing, so a fresh part publishes normally";
}

/// A files-only table root (no part ever published) becomes logically absent IMMEDIATELY once
/// `removeRecursive` durably completes the removal -- no GC round required. This is the same-call
/// synchronous half of the fix: `DROP TABLE ... SYNC` must not depend on GC latency to observe removal.
TEST(CASWiringOps, FilesOnlyTableRootRemovalIsImmediatelyAbsentWithoutGc)
{
    auto storage = openWiringStorage();
    const auto ns = storage->liveNamespace("a77a77a7-7777-4777-8777-777777777777");
    storage->store()->putNamespaceFile(storage->store()->namespaceLife(ns), "format_version.txt", "1\n");
    EXPECT_TRUE(storage->existsDirectory("a77/a77a77a7-7777-4777-8777-777777777777"));

    auto tx = storage->createTransaction();
    tx->removeRecursive("a77/a77a77a7-7777-4777-8777-777777777777", {});
    EXPECT_FALSE(storage->existsDirectory("a77/a77a77a7-7777-4777-8777-777777777777"))
        << "the terminal remove_namespace transaction is durable synchronously";
    EXPECT_FALSE(storage->existsFile("a77/a77a77a7-7777-4777-8777-777777777777/format_version.txt"));
}

/// REMOVED (all-tree-part-files Task 6):
/// `MutableTmpMoveOnCommittedPart` exercised `VersionMetadataOnDisk`'s OLD atomic-write dance —
/// autocommit `txn_version.txt.tmp`, then a standalone one-shot `moveFile(.tmp -> txn_version.txt)`
/// — via `ContentAddressedTransaction::moveFile` directly. That dance no longer exists in production:
/// Task 5's `supportsAtomicFileWrites` short-circuit makes `VersionMetadataOnDisk::storeInfoToData-
/// PartStorage` write `txn_version.txt` directly in one shot on a CA disk, with no `.tmp` file and no
/// rename ever produced. Task 9 completed the cleanup this comment used to defer: `moveFile`'s legacy
/// "rename FROM a committed mutable-per-part-file, source not staged in this transaction" branch is
/// now DELETED (it had been provably unreachable since Task 5, and rebuilding it against `entries`
/// would only add unused surface for a dead path). Coverage that remains valid: Task 5's own
/// capability test proves no `.tmp` file is ever created; `CASTransactionAllTree.CommittedTxnVersion-
/// StoreRepoints` (`gtest_ca_transaction.cpp`) proves the real, live path — a standalone write of
/// `txn_version.txt` directly onto an already-committed part — repoints correctly.

TEST(CASWiringOps, VerbatimMoveAndUnlink)
{
    auto storage = openWiringStorage();
    auto tx = storage->createTransaction();
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/tmp_mutation_5.txt", "cmds");
    auto & ca_tx = dynamic_cast<DB::ContentAddressedTransaction &>(*tx);
    ca_tx.moveFile("a11/a11a11a1-1111-4111-8111-111111111111/tmp_mutation_5.txt", "a11/a11a11a1-1111-4111-8111-111111111111/mutation_5.txt");
    EXPECT_TRUE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/mutation_5.txt"));
    EXPECT_FALSE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/tmp_mutation_5.txt"));
    ca_tx.unlinkFile("a11/a11a11a1-1111-4111-8111-111111111111/mutation_5.txt", false, false);
    EXPECT_FALSE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/mutation_5.txt"));
}

TEST(CASWiringOps, UnlinkHonorsIfExistsForPartFiles)
{
    auto storage = openWiringStorage();
    const String path = "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin";

    auto missing_tx = storage->createTransaction();
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::FILE_DOESNT_EXIST,
        [&] { missing_tx->unlinkFile(path, /*if_exists=*/false, /*should_remove_objects=*/true); });

    auto ignored_tx = storage->createTransaction();
    EXPECT_NO_THROW(ignored_tx->unlinkFile(path, /*if_exists=*/true, /*should_remove_objects=*/true));
    EXPECT_NO_THROW(ignored_tx->commit(DB::NoCommitOptions{}));
    EXPECT_FALSE(storage->existsFile(path));

    auto create_tx = storage->createTransaction();
    writeThroughTransaction(*create_tx, path, "payload");
    create_tx->commit(DB::NoCommitOptions{});
    ASSERT_TRUE(storage->existsFile(path));

    auto existing_tx = storage->createTransaction();
    EXPECT_NO_THROW(existing_tx->unlinkFile(path, /*if_exists=*/false, /*should_remove_objects=*/true));
    EXPECT_NO_THROW(existing_tx->commit(DB::NoCommitOptions{}));
    EXPECT_FALSE(storage->existsFile(path));
}

TEST(CASWiringOps, TableRenameMovesRefsFilesAndDetached)
{
    auto storage = openWiringStorage();
    auto tx = storage->createTransaction();
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin", "live");
    tx->commit(DB::NoCommitOptions{});
    auto tx2 = storage->createTransaction();
    tx2->moveDirectory("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0", "a11/a11a11a1-1111-4111-8111-111111111111/detached/all_1_1_0");   /// one detached part
    tx2->commit(DB::NoCommitOptions{});
    auto tx3 = storage->createTransaction();
    writeThroughTransaction(*tx3, "a11/a11a11a1-1111-4111-8111-111111111111/all_2_2_0/data.bin", "live2");
    tx3->commit(DB::NoCommitOptions{});
    storage->store()->putNamespaceFile(wiringLife(*storage), "format_version.txt", "1\n");

    auto tx4 = storage->createTransaction();
    tx4->moveDirectory("a11/a11a11a1-1111-4111-8111-111111111111", "a22/a22a22a2-2222-4222-8222-222222222222");
    tx4->commit(DB::NoCommitOptions{});

    EXPECT_FALSE(storage->existsDirectory("a11/a11a11a1-1111-4111-8111-111111111111"));
    EXPECT_TRUE(storage->existsDirectory("a22/a22a22a2-2222-4222-8222-222222222222"));
    EXPECT_TRUE(storage->existsFile("a22/a22a22a2-2222-4222-8222-222222222222/all_2_2_0/data.bin"));
    EXPECT_TRUE(storage->existsFile("a22/a22a22a2-2222-4222-8222-222222222222/format_version.txt"));
    EXPECT_TRUE(storage->existsDirectory("a22/a22a22a2-2222-4222-8222-222222222222/detached/all_1_1_0"));
}

/// B126: RENAME TABLE move_namespace is idempotent — re-driving the SAME rename after it completed is a
/// clean no-op (the source namespace is already gone), so a partial-failure re-drive is safe.
TEST(CASWiringOps, TableRenameIsIdempotentOnRedrive)
{
    auto storage = openWiringStorage();
    auto tx = storage->createTransaction();
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin", "live");
    tx->commit(DB::NoCommitOptions{});
    storage->store()->putNamespaceFile(wiringLife(*storage), "format_version.txt", "1\n");

    auto tx2 = storage->createTransaction();
    tx2->moveDirectory("a11/a11a11a1-1111-4111-8111-111111111111", "a22/a22a22a2-2222-4222-8222-222222222222");
    tx2->commit(DB::NoCommitOptions{});

    /// Re-drive the identical rename: a11a11a1-1111-4111-8111-111111111111 is empty/gone, so every step no-ops; must not throw.
    auto tx3 = storage->createTransaction();
    EXPECT_NO_THROW(tx3->moveDirectory("a11/a11a11a1-1111-4111-8111-111111111111", "a22/a22a22a2-2222-4222-8222-222222222222"));
    tx3->commit(DB::NoCommitOptions{});

    EXPECT_TRUE(storage->existsFile("a22/a22a22a2-2222-4222-8222-222222222222/all_1_1_0/data.bin"));
    EXPECT_TRUE(storage->existsFile("a22/a22a22a2-2222-4222-8222-222222222222/format_version.txt"));
    EXPECT_FALSE(storage->existsDirectory("a11/a11a11a1-1111-4111-8111-111111111111"));
}

/// B123: a verbatim-file move (get->put->remove, no native rename) is idempotent on re-drive — once the
/// source is gone but the destination is present, a re-driven move is a no-op, not a FILE_DOESNT_EXIST.
TEST(CASWiringOps, VerbatimMoveIsIdempotentOnRedrive)
{
    auto storage = openWiringStorage();
    auto tx = storage->createTransaction();
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/tmp_mutation_7.txt", "cmds");
    auto & ca_tx = dynamic_cast<DB::ContentAddressedTransaction &>(*tx);
    ca_tx.moveFile("a11/a11a11a1-1111-4111-8111-111111111111/tmp_mutation_7.txt", "a11/a11a11a1-1111-4111-8111-111111111111/mutation_7.txt");
    EXPECT_TRUE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/mutation_7.txt"));
    /// Re-drive: source gone, destination present → no-op (no throw).
    EXPECT_NO_THROW(ca_tx.moveFile("a11/a11a11a1-1111-4111-8111-111111111111/tmp_mutation_7.txt", "a11/a11a11a1-1111-4111-8111-111111111111/mutation_7.txt"));
    EXPECT_TRUE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/mutation_7.txt"));
    /// Both source and destination absent → genuine missing source still throws.
    EXPECT_ANY_THROW(ca_tx.moveFile("a11/a11a11a1-1111-4111-8111-111111111111/tmp_mutation_8.txt", "a11/a11a11a1-1111-4111-8111-111111111111/mutation_8.txt"));
}

/// B124: moveDirectory's staged-merge is source-wins, and a genuine collision (the same mutable file
/// staged under BOTH the source and destination part keys with DIFFERING bytes) fails loud instead of
/// silently dropping a just-written file. Identical bytes are a benign idempotent re-key.
///
/// The "fails loud" collision throws LOGICAL_ERROR, which aborts the whole process in debug/sanitizer
/// builds (Exception.cpp's handle_error_code) instead of behaving like a catchable exception -- so
/// EXPECT_ANY_THROW only makes sense in a plain release build. CASWiringOpsDeathTest below proves the
/// SAME collision positively aborts under debug/sanitizer builds instead (same pattern as the existing
/// CASBlobDigestDeathTest precedent).
TEST(CASWiringOps, MoveDirectoryMutableCollisionPolicy)
{
#ifndef DEBUG_OR_SANITIZER_BUILD
    /// Differing bytes → fail loud.
    {
        auto storage = openWiringStorage();
        auto tx = storage->createTransaction();
        writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/tmp_x/txn_version.txt", "A");
        writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_9_9_9/txn_version.txt", "B");
        auto & ca_tx = dynamic_cast<DB::ContentAddressedTransaction &>(*tx);
        EXPECT_ANY_THROW(ca_tx.moveDirectory("a11/a11a11a1-1111-4111-8111-111111111111/tmp_x", "a11/a11a11a1-1111-4111-8111-111111111111/all_9_9_9"));
    }
#endif
    /// Identical bytes → benign, no throw (source-wins, idempotent). Both parts carry real content so
    /// the eager publish-at-rename builds a proper ref (a mutable-only staging would instead hit
    /// updateRefPublishedAt on a not-yet-committed ref — unrelated to the collision policy under test).
    /// data.bin must ALSO match now: all-tree Task 9 generalized the differing-bytes collision check
    /// from the legacy mutable-file names to every entry, so a differing data.bin would (correctly)
    /// throw too and defeat this block's "benign" premise.
    {
        auto storage = openWiringStorage();
        auto tx = storage->createTransaction();
        writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/tmp_y/data.bin", "d1");
        writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/tmp_y/txn_version.txt", "SAME");
        writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_8_8_8/data.bin", "d1");
        writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_8_8_8/txn_version.txt", "SAME");
        auto & ca_tx = dynamic_cast<DB::ContentAddressedTransaction &>(*tx);
        EXPECT_NO_THROW(ca_tx.moveDirectory("a11/a11a11a1-1111-4111-8111-111111111111/tmp_y", "a11/a11a11a1-1111-4111-8111-111111111111/all_8_8_8"));
    }
}

#if defined(DEBUG_OR_SANITIZER_BUILD)
/// Debug/sanitizer-build counterpart to MoveDirectoryMutableCollisionPolicy's "differing bytes → fail
/// loud" case: LOGICAL_ERROR aborts the process here instead of throwing a catchable exception, so the
/// check must be a death test (same pattern as CASBlobDigestDeathTest in gtest_cas_blob_digest.cpp).
TEST(CASWiringOpsDeathTest, MoveDirectoryMutableCollisionPolicyAborts)
{
    auto storage = openWiringStorage();
    auto tx = storage->createTransaction();
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/tmp_x/txn_version.txt", "A");
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_9_9_9/txn_version.txt", "B");
    auto & ca_tx = dynamic_cast<DB::ContentAddressedTransaction &>(*tx);
    EXPECT_DEATH({ ca_tx.moveDirectory("a11/a11a11a1-1111-4111-8111-111111111111/tmp_x", "a11/a11a11a1-1111-4111-8111-111111111111/all_9_9_9"); }, "");
}
#endif

/// D3 review pin: moveDirectory's staged-merge collision code has four (src build?, dst build?)
/// combinations. This one — destination already holds a staged PartWriteTxn, source has none — proved
/// confusable when the plan's author sketched a fix: a naive rewrite of the four-way branch can fall
/// through to `src_st.build->abandon()` on a null build. The merge must be a pure no-op on the
/// destination's build in this combination — no abandon, no adopt — while everything else (any
/// removal marks carried from the source) still merges in and the destination's own content
/// publishes exactly as staged.
///
/// T9-review fix (all-tree-part-files): the ORIGINAL construction staged the source via a
/// `txn_version.txt` WRITE, relying on the pre-Task-6 "the mutable-file write path never calls
/// buildFor" fact to keep `src_st.build` null. Since Task 6/9, `writeFile`'s inline-candidate path
/// (which `txn_version.txt` now takes — it is an ordinary tree entry, not a mutable sidecar file)
/// unconditionally calls `buildFor` for ANY inline entry, so the source silently acquired a REAL
/// PartWriteTxn and this test drifted onto the *other* merge branch (`else if (src_st.build)`) without
/// failing — both branches produce the same externally-visible result (assertions passed either
/// way), so the drift was invisible. Fixed by staging the source via `unlinkFile` instead of a
/// write: Task 8's removal-mark staging (`content_removed`) is the one remaining staging shape that
/// genuinely never calls `buildFor` (`publishStaging`'s own `!st.build && ...` guard depends on
/// this), so `parts[src_key]` exists but `src_st.build` stays null again, restoring the test's
/// documented precondition.
///
/// Made RED-able (the review's ask): `PartWriteTxn::abandon()` unconditionally emits a `BuildAbort`
/// `CasEvent` (`CasPartWriteTxn.cpp`) — this only happens if the buggy `else if (src_st.build)` branch
/// runs `src_st.build->abandon()`. Registering an event sink (`Cas::Pool::setEventSink`, the same
/// public test hook `gtest_cas_event_log.cpp` uses) and asserting no `BuildAbort` event fires is a
/// genuine behavioral discriminator between the two merge branches — not just "assertions pass
/// either way" — so a future regression that gives the source a PartWriteTxn again fails this test loudly.
TEST(CASWiringOps, MoveDirectoryOntoExistingDestinationBuildSurvives)
{
    std::vector<DB::Cas::CasEvent> events;   /// declared BEFORE the Pool so it outlives the background syncer's emits (ASan 2026-07-09)
    auto storage = openWiringStorage();
    storage->store()->setEventSink([&](const DB::Cas::CasEvent & e) { events.push_back(e); });

    /// unlinkFile now honors if_exists=false (triage #24, 8fc0c964a5b): the target must be real. Commit
    /// it in its own transaction first so the removal below targets a genuinely-committed file, not a
    /// never-existed path.
    auto setup_tx = storage->createTransaction();
    writeThroughTransaction(*setup_tx, "a11/a11a11a1-1111-4111-8111-111111111111/tmp_z/txn_version.txt", "creation_tid: (7,7,00000000-0000-0000-0000-000000000000)");
    setup_tx->commit(DB::NoCommitOptions{});

    auto tx = storage->createTransaction();
    /// Destination already has a real blob upload staged -> a live PartWriteTxn.
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_7_7_7/data.bin", "dst-content");
    /// Source is staged with ONLY a removal mark (Task 8's content_removed staging) -> parts[src_key]
    /// exists, but src_st.build stays null (unlinkFile never calls buildFor).
    tx->unlinkFile("a11/a11a11a1-1111-4111-8111-111111111111/tmp_z/txn_version.txt", /*if_exists=*/false, /*should_remove_objects=*/true);

    auto & ca_tx = dynamic_cast<DB::ContentAddressedTransaction &>(*tx);
    EXPECT_NO_THROW(ca_tx.moveDirectory("a11/a11a11a1-1111-4111-8111-111111111111/tmp_z", "a11/a11a11a1-1111-4111-8111-111111111111/all_7_7_7"));

    /// The discriminator: no BuildAbort event means src_st.build->abandon() was never called during
    /// the re-key merge, i.e. the intended neither-branch no-op merge ran, not the two-builds
    /// merge-and-abandon branch. Checked right after the re-key so it stays scoped to the merge.
    EXPECT_FALSE(std::any_of(events.begin(), events.end(),
        [](const DB::Cas::CasEvent & e) { return e.type == DB::Cas::CasEventType::BuildAbort; }))
        << "src_st.build->abandon() fired — the source unexpectedly has a real PartWriteTxn again";

    /// [TXN-ONE-PIPELINE] the re-key does not publish; the destination's build is materialized only at
    /// commit(). The destination's own build then publishes its own content untouched; the source's
    /// removal mark names a path never committed anywhere, so it is a harmless no-op once merged into
    /// the destination's (first-time-published) staging.
    tx->commit(DB::NoCommitOptions{});
    EXPECT_TRUE(storage->existsDirectory("a11/a11a11a1-1111-4111-8111-111111111111/all_7_7_7"));
    EXPECT_EQ(storage->getFileSize("a11/a11a11a1-1111-4111-8111-111111111111/all_7_7_7/data.bin"), 11u);   /// "dst-content"
    EXPECT_FALSE(storage->tryGetInManifestBytes("a11/a11a11a1-1111-4111-8111-111111111111/all_7_7_7/txn_version.txt").has_value());

    storage->store()->setEventSink(nullptr);
}

TEST(CASWiringOps, FreezeViaHardLinksIntoShadow)
{
    auto storage = openWiringStorage();
    auto tx = storage->createTransaction();
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin", "frozen-bytes");
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/metadata_version.txt", "7");
    tx->commit(DB::NoCommitOptions{});

    /// FREEZE clones a committed part file-by-file into the shadow tree via hardlinks; the staged
    /// shadow part publishes at commit (pool-global - any replica reads the backup).
    auto tx2 = storage->createTransaction();
    tx2->createHardLink("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin", "shadow/bk1/store/a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin");
    tx2->createHardLink("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/metadata_version.txt",
                        "shadow/bk1/store/a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/metadata_version.txt");
    tx2->commit(DB::NoCommitOptions{});

    EXPECT_TRUE(storage->existsDirectory("shadow/bk1/store/a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0"));
    EXPECT_TRUE(storage->existsFile("shadow/bk1/store/a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin"));
    EXPECT_EQ(storage->tryGetInManifestBytes("shadow/bk1/store/a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/metadata_version.txt"),
              std::optional<String>("7"));

    /// UNFREEZE: removeRecursive of the backup root drops every shadow namespace under it.
    auto tx3 = storage->createTransaction();
    tx3->removeRecursive("shadow/bk1", {});
    EXPECT_FALSE(storage->existsDirectory("shadow/bk1/store/a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0"));
    EXPECT_FALSE(storage->existsDirectory("shadow/bk1"));
}

/// ==== M-W Task 8: in-flight read-your-writes (B59) ====

TEST(CASWiringInFlight, StagedFilesVisibleBeforeCommit)
{
    auto storage = openWiringStorage();
    auto tx = storage->createTransaction();
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/tmp_mut_all_1_1_0/p.proj/data.bin", "proj-bytes");
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/tmp_mut_all_1_1_0/uuid.txt", "u-9");

    /// B188 precommit-first: content blobs are PENDING (staged locally, not yet uploaded). So
    /// tryGetInFlightStorageObjects returns {} — the pool object does not exist yet. The caller
    /// (DataPartStorageOnDiskFull::prepareRead) falls back to tryGetInFlightFileSize to get the size
    /// and then serves the content via tryReadFileInFlight (local temp file). File sizes and directory
    /// overlay still work because they are driven by the staged tree entry, not the pool.
    auto objects = tx->tryGetInFlightStorageObjects("a11/a11a11a1-1111-4111-8111-111111111111/tmp_mut_all_1_1_0/p.proj/data.bin");
    EXPECT_FALSE(objects.has_value());
    EXPECT_EQ(tx->tryGetInFlightFileSize("a11/a11a11a1-1111-4111-8111-111111111111/tmp_mut_all_1_1_0/p.proj/data.bin"), std::optional<uint64_t>(10));
    EXPECT_EQ(tx->tryGetInFlightFileSize("a11/a11a11a1-1111-4111-8111-111111111111/tmp_mut_all_1_1_0/uuid.txt"), std::optional<uint64_t>(3));
    EXPECT_FALSE(tx->tryGetInFlightFileSize("a11/a11a11a1-1111-4111-8111-111111111111/tmp_mut_all_1_1_0/missing.bin").has_value());

    /// Bytes read back: a pending blob from the local temp file (B188); staged mutable bytes from memory.
    {
        auto buf = tx->tryReadFileInFlight("a11/a11a11a1-1111-4111-8111-111111111111/tmp_mut_all_1_1_0/p.proj/data.bin", {}, std::nullopt);
        ASSERT_TRUE(buf);
        String read;
        readStringUntilEOF(read, *buf);
        EXPECT_EQ(read, "proj-bytes");   /// B188: served from local temp file (pending upload)
    }
    {
        auto buf = tx->tryReadFileInFlight("a11/a11a11a1-1111-4111-8111-111111111111/tmp_mut_all_1_1_0/uuid.txt", {}, std::nullopt);
        ASSERT_TRUE(buf);
        String read;
        readStringUntilEOF(read, *buf);
        EXPECT_EQ(read, "u-9");
    }

    /// The directory overlay answers for INNER dirs only (the PoC contract): the part dir itself
    /// is FALSE so a rejected temporary part's removeIfNeeded takes the clean early-return path.
    EXPECT_FALSE(tx->hasInFlightDirectory("a11/a11a11a1-1111-4111-8111-111111111111/tmp_mut_all_1_1_0"));
    EXPECT_TRUE(tx->hasInFlightDirectory("a11/a11a11a1-1111-4111-8111-111111111111/tmp_mut_all_1_1_0/p.proj"));
    EXPECT_FALSE(tx->hasInFlightDirectory("a11/a11a11a1-1111-4111-8111-111111111111/tmp_mut_all_1_1_0/q.proj"));
    auto top = tx->listInFlightDirectory("a11/a11a11a1-1111-4111-8111-111111111111/tmp_mut_all_1_1_0");
    EXPECT_EQ(top, (std::vector<std::string>{"p.proj", "uuid.txt"}));
    EXPECT_EQ(tx->listInFlightDirectory("a11/a11a11a1-1111-4111-8111-111111111111/tmp_mut_all_1_1_0/p.proj"),
              (std::vector<std::string>{"data.bin"}));
}

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

/// ==== M-W Task 10: the GC scheduler end-to-end through the wiring ====

TEST(CASWiringGc, DroppedPartIsReclaimedByRounds)
{
    auto storage = openWiringStorage();
    auto tx = storage->createTransaction();
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin", "reclaim-me");
    tx->commit(DB::NoCommitOptions{});

    auto * exchange = dynamic_cast<DB::IContentAddressedExchange *>(storage.get());
    ASSERT_NE(exchange, nullptr);
    const auto blob_key = storage->getStorageObjects("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin")[0].remote_path;

    auto tx2 = storage->createTransaction();
    tx2->removeDirectory("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0");   /// dropRef - the part is unreachable now

    /// Round 1 folds the drop and retires+deletes the part MANIFEST; the freed blob is retired+deleted
    /// by a FOLLOWING round (next-round reclamation, M-C3). The steal needs one extra observation
    /// window between rounds (the pacing scheduler is stable across these calls - each call after the
    /// first re-acquires via renewal).
    storage->runOneGcRoundForTest();
    storage->runOneGcRoundForTest();
    storage->runOneGcRoundForTest();

    EXPECT_FALSE(storage->existsDirectory("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0"));
    /// The relink offer (B7 part_manifest_v2): a reclaimed part is no longer a committed CA part here,
    /// so getRelinkOffer offers NOTHING and the sender streams bytes — the documented fallback.
    EXPECT_FALSE(exchange->getRelinkOffer("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0").has_value());

    /// A fresh identical write re-CREATES the content at the same key and reads back fine.
    auto tx3 = storage->createTransaction();
    writeThroughTransaction(*tx3, "a11/a11a11a1-1111-4111-8111-111111111111/all_9_9_0/data.bin", "reclaim-me");
    tx3->commit(DB::NoCommitOptions{});
    EXPECT_EQ(storage->getStorageObjects("a11/a11a11a1-1111-4111-8111-111111111111/all_9_9_0/data.bin")[0].remote_path, blob_key);
    EXPECT_TRUE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/all_9_9_0/data.bin"));
}

/// B199 (real-path displacement reclamation, ported off the tree model to part manifests): re-writing
/// the SAME part path with DISTINCT content publishes a NEW part ManifestId over the ref (a true-removal
/// of the old owner manifest + an activation of the new one in the single ordered journal — no shared
/// content-addressed identity between the two parts). GC must reclaim the displaced (manifestA) unique
/// blobs while never losing the live (manifestB) closure.
///
/// NOTE (port): the original repro pre-deleted treeA's TREE OBJECT before the fold to exercise the
/// tree-era inline-closure 404 path (the precommit `Add` carried treeA's closure INLINE so the fold
/// recorded edges without a `readTree`). That mechanism is gone: a part manifest carries its OWN blob
/// edges and the fold reads the ONE removal-target body to release them (a missing removal body clamps
/// + records an anomaly, never guesses). So this port drives the genuine manifest displacement WITHOUT
/// the out-of-band pre-delete twist — the reclamation contract (no leak / no loss) is what survives.
///
/// PORT (rev. 15 displacement shape): a part is a single-owner ManifestId and `promote` is a PURE OWNER
/// MOVE (precommit→committed). Re-publishing over a LIVE committed ref does NOT emit a removal of the
/// displaced owner (the displaced manifest is not named in any event), so its blobs would never get a
/// -1 — there is no in-place "republish-over-committed". The genuine displacement that DOES journal a
/// true-removal is the real MergeTree pattern: DROP the old part (dropRef appends old→none, leaving the
/// old body present for the fold to read the -1 edges), THEN publish the new part. GC folds manifestA's
/// removal, retires its now-zero-in-degree blobs, and the recheck cleanup deletes the owner-removed
/// body. We do NOT pre-delete manifestA's body — only GC deletes an owner-removed body, after sealing
/// its decrements.
TEST(CASWiringGc, DisplacedTreeBlobsReclaimedThroughRealPath)
{
    auto storage = openWiringStorage();

    /// Commit manifestA with unique content (data-A / mark-A), through the real precommit-first transaction.
    {
        auto tx = storage->createTransaction();
        writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_0_0_0/data.bin", "data-A");
        writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_0_0_0/data.cmrk3", "mark-A");
        tx->commit(DB::NoCommitOptions{});
    }
    const auto resolved_a = storage->store()->resolveRef(storage->liveNamespace("a11a11a1-1111-4111-8111-111111111111"), "all_0_0_0");
    ASSERT_TRUE(resolved_a.has_value());
    const DB::Cas::ManifestId manifest_a = resolved_a->manifest_id;

    /// DISPLACE (true-removal repoint): drop the old part so dropRef journals manifestA's removal
    /// (old=committed(manifestA)→new=none) — this leaves manifestA's body PRESENT for the fold to read
    /// its -1 edges. Then re-write the SAME part path with DISTINCT content (data-B / mark-B), which
    /// publishes a NEW part ManifestId over the (now free) ref. Confirm the displacement is real: the
    /// ref resolves to a DIFFERENT manifest.
    {
        auto tx = storage->createTransaction();
        tx->removeDirectory("a11/a11a11a1-1111-4111-8111-111111111111/all_0_0_0");
        tx->commit(DB::NoCommitOptions{});
    }
    {
        auto tx = storage->createTransaction();
        writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_0_0_0/data.bin", "data-B");
        writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_0_0_0/data.cmrk3", "mark-B");
        tx->commit(DB::NoCommitOptions{});
    }
    const auto resolved_b = storage->store()->resolveRef(storage->liveNamespace("a11a11a1-1111-4111-8111-111111111111"), "all_0_0_0");
    ASSERT_TRUE(resolved_b.has_value());
    ASSERT_FALSE(manifest_a == resolved_b->manifest_id)
        << "the second write must displace the ref to a distinct part manifest (last-op-wins)";

    /// Drive GC to a fixpoint. Displacement reclamation needs the next-round cascade (manifestA's
    /// removal folds, its blobs hit zero in-degree, a following round retires+deletes them); give a
    /// generous bound so the displaced closure fully drains.
    for (int i = 0; i < 8; ++i)
        storage->runOneGcRoundForTest();

    const DB::Cas::FsckReport after = DB::Cas::runFsck(*storage->store(), /*detail=*/false);
    EXPECT_EQ(after.dangling, 0u) << "displacement must never lose a reachable object (manifestB stays live)";
    EXPECT_GT(after.reachable, 0u) << "the live ref points at manifestB; manifestB's closure is reachable";
    /// The REAL path: `runOneGcRoundForTest` drives the production scheduler, so the displaced closure
    /// is not merely recognized as unreachable but actually reclaimed -- recognition alone would leave
    /// every displacement leaking one part's unique blobs forever.
    EXPECT_EQ(after.unreachable, 0u)
        << "manifestA's unique blobs (data-A / mark-A) must be RECLAIMED once the displacement folds, "
        << "not just recognized as unreachable; unreachable=" << after.unreachable;
}

/// ==== M-W Task 11 / B7: the DataPartsExchange facade (manifest relink, part_manifest_v2) ====

/// Publish-then-confirm (Task 14) split the receiver's adoption into `prepare` + `promote`, with the
/// interserver confirm interposed between them. The confirm belongs to `Fetcher`, not to the storage, so
/// the tests below that only care about the ADOPTION drive both halves back to back through this helper
/// -- which is exactly what `publishEntries` does for the atomic callers. `false` is the
/// `MechanismFallbackAllowed` outcome of either half: nothing published, the caller byte-fetches.
namespace
{

/// The RECEIVER's disk-relative staging path for every relink test below: the tmp-fetch dir of the
/// receiving table (a22...), which is a DIFFERENT table from the sender's (a11...) -- that is what makes
/// the "the sender's namespace id is ignored" assertions meaningful. `prepareAdoptFromManifest` is
/// addressed by path, exactly like `getRelinkOffer`, so the ref name is the router's business.
constexpr auto kReceiverTmpFetchPath = "a22/a22a22a2-2222-4222-8222-222222222222/tmp-fetch_all_1_1_0";

bool adoptPartFromManifestAndPromote(DB::IContentAddressedExchange & exchange, const String & part_path,
                                     const String & manifest_bytes)
{
    std::unique_ptr<DB::ICaPreparedRelink> prepared;
    if (exchange.prepareAdoptFromManifest(part_path, manifest_bytes, prepared)
        == DB::CaRelinkPrepare::MechanismFallbackAllowed)
        return false;
    EXPECT_NE(prepared, nullptr) << "a Prepared outcome must carry the handle that owes the terminal operation";
    return prepared->promote() == DB::CaRelinkPromote::Committed;
}

}

/// B7 sender side: getRelinkOffer returns the COMMITTED part's encoded PartManifest body — the
/// opaque payload the receiver decodes. The bytes must decode to the same entries the part was
/// published with; an absent part offers nothing (the sender streams bytes — the documented fallback).
/// Task 13 adds the second half of the offer: the confirm token, which must name the SAME manifest the
/// body carries. That equality is the offer's whole safety property — a token naming anything else
/// would have the receiver confirm a manifest whose entries it never adopted.
TEST(CASWiringExchange, GetRelinkOfferReturnsBodyAndTokenForCommittedPart)
{
    auto storage = openWiringStorage();
    /// Publish a real committed part (data.bin + a projection blob + mutable per-part files).
    publishWiredPart(*storage, storage->liveNamespace("a11a11a1-1111-4111-8111-111111111111"), "all_1_1_0");

    auto * exchange = dynamic_cast<DB::IContentAddressedExchange *>(storage.get());
    ASSERT_NE(exchange, nullptr);
    EXPECT_FALSE(exchange->getPoolUUID().empty());

    auto offer = exchange->getRelinkOffer("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0");
    ASSERT_TRUE(offer.has_value());
    EXPECT_FALSE(offer->manifest_bytes.empty());

    /// The transferred body decodes to the SAME entries the part names — the blob entries AND the
    /// per-part files (uuid.txt/metadata_version.txt are ordinary tree entries now, all-tree Task 6/9).
    /// The sender's ManifestRef/namespace/digest are present but non-authoritative downstream.
    const DB::Cas::PartManifest decoded = DB::Cas::decodePartManifest(offer->manifest_bytes);
    ASSERT_EQ(decoded.entries.size(), 4u);
    EXPECT_EQ(decoded.entries[0].path, "data.bin");
    EXPECT_EQ(decoded.entries[0].ref.digest.toU128(), u128Of("payload-A"));
    EXPECT_EQ(decoded.entries[2].path, "p.proj/data.bin");
    EXPECT_EQ(decoded.entries[2].ref.digest.toU128(), u128Of("payload-B"));

    /// The token: it decodes, it names this mount and this pool, and it names the manifest that the
    /// body just decoded to.
    const auto token = DB::decodeCasRelinkSourceToken(offer->confirm_token);
    ASSERT_TRUE(token.has_value()) << "the sender minted a token its own decoder rejects: " << offer->confirm_token;
    EXPECT_EQ(token->pool_uuid, exchange->getPoolUUID());
    EXPECT_EQ(token->root_namespace, storage->liveNamespace("a11a11a1-1111-4111-8111-111111111111").string());
    EXPECT_EQ(token->ref_name, "all_1_1_0");
    EXPECT_EQ(token->part_name, "all_1_1_0");
    EXPECT_EQ(token->manifest_ref_text, DB::Cas::manifestRefDebugString(decoded.ref));
    EXPECT_TRUE(exchange->ownsNamespace(token->server_root_id, token->root_namespace))
        << "the minted token must route back to the mount that minted it";

    /// An absent part is not a committed CA part here -> no offer.
    EXPECT_FALSE(exchange->getRelinkOffer("a11/a11a11a1-1111-4111-8111-111111111111/all_9_9_9").has_value());
}

/// B7 receiver side (the core): take a COMMITTED part's transferred manifest bytes and adopt them into
/// a DIFFERENT table namespace WITHOUT moving any blob body (blobs are shared by hash in the pool).
/// The receiver stages its OWN fresh local manifest, precommitAdd + promote it, and reports success.
/// Asserts: success; the adopted ref is live + loadable; the receiver's ManifestId differs from the
/// sender's (no shared identity); the ref lives in the RECEIVER namespace (no cross-namespace adoption);
/// and NO blob body was uploaded by the receiver (the put-counter stays flat across adopt).
TEST(CASWiringExchange, AdoptPartFromManifestPublishesFreshLocalManifest)
{
    auto storage = openWiringStorage();
    const auto sender_ns = storage->liveNamespace("a11a11a1-1111-4111-8111-111111111111");
    publishWiredPart(*storage, sender_ns, "all_1_1_0");

    auto * exchange = dynamic_cast<DB::IContentAddressedExchange *>(storage.get());
    ASSERT_NE(exchange, nullptr);

    auto offer = exchange->getRelinkOffer("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0");
    ASSERT_TRUE(offer.has_value());
    const String & bytes = offer->manifest_bytes;
    const DB::Cas::ManifestId sender_id =
        storage->store()->resolveRef(sender_ns, "all_1_1_0")->manifest_id;

    /// Count blob PUTs over the adopt: a manifest relink must NOT upload any blob body (the blobs are
    /// already in the shared pool, adopted by hash). We assert via the blob keys' presence/incarnation:
    /// the receiver never overwrites or re-creates the blobs — their head tokens are unchanged.
    const auto data_key = storage->store()->layout().blobKey(idOf("payload-A"));
    const auto proj_key = storage->store()->layout().blobKey(idOf("payload-B"));
    const auto data_tok_before = storage->store()->backend().head(data_key).token;
    const auto proj_tok_before = storage->store()->backend().head(proj_key).token;

    /// Adopt into a DIFFERENT table (a22a22a2-2222-4222-8222-222222222222). The transferred body's root_namespace_id is the sender's
    /// (a11a11a1-1111-4111-8111-111111111111) — the receiver must IGNORE it and use a22a22a2-2222-4222-8222-222222222222.
    const bool ok = adoptPartFromManifestAndPromote(*exchange, kReceiverTmpFetchPath, bytes);
    EXPECT_TRUE(ok);

    /// The adopted ref is live in the RECEIVER namespace and loadable.
    const auto receiver_ns = storage->liveNamespace("a22a22a2-2222-4222-8222-222222222222");
    auto receiver_resolved = storage->store()->resolveRef(receiver_ns, "tmp-fetch_all_1_1_0");
    ASSERT_TRUE(receiver_resolved.has_value());
    const DB::Cas::PartManifest receiver_manifest =
        storage->store()->readManifest(receiver_resolved->manifest_id);
    ASSERT_EQ(receiver_manifest.entries.size(), 4u);
    EXPECT_EQ(receiver_manifest.entries[0].ref.digest.toU128(), u128Of("payload-A"));

    /// FRESH receiver-local identity: a DIFFERENT ManifestId from the sender's, in the RECEIVER namespace.
    EXPECT_FALSE(sender_id == receiver_resolved->manifest_id)
        << "the receiver must mint its OWN manifest id, not share the sender's";
    EXPECT_EQ(receiver_resolved->manifest_id.root_namespace.string(), receiver_ns.string())
        << "the adopted manifest must live in the receiver namespace (derived from table_uuid), not the sender's";
    EXPECT_FALSE(receiver_ns.string() == sender_ns.string());

    /// NO blob body was uploaded: the shared blobs' incarnations are untouched by the adopt.
    EXPECT_EQ(storage->store()->backend().head(data_key).token, data_tok_before)
        << "adopt-from-manifest must not re-upload a blob already in the shared pool";
    EXPECT_EQ(storage->store()->backend().head(proj_key).token, proj_tok_before);
}

/// B7 fail-closed: if a referenced blob is absent/condemned in the pool, adoptPartFromManifest must
/// promote-abort and return FALSE (NOT throw) so the caller byte-fetches — exactly where the old pin
/// protocol fell back. Nothing is published (no dangling ref).
TEST(CASWiringExchange, AdoptFailsClosedAndFallsBackOnCondemnedBlob)
{
    /// §4 manifest-trust (test name is legacy — adopt no longer fails closed on a raced pool blob):
    /// adoptPartFromManifest runs the receiver's local promote, which TRUSTS the committed-source adopted
    /// leaves via the durable manifest edge — no per-file HEAD/loadMeta probe on the pool blobs. So even if
    /// a pool blob raced to absent, adopt SUCCEEDS and publishes the receiver ref. This matches ordinary
    /// ReplicatedMergeTree interserver trust: the sender served the manifest from a LIVE part whose refs pin
    /// the blobs at in-degree >= 1, so this scenario cannot arise on the real fetch path; a genuinely-absent
    /// adopted blob is an fsck finding, not an adopt-time abort.
    auto storage = openWiringStorage();
    const auto sender_ns = storage->liveNamespace("a11a11a1-1111-4111-8111-111111111111");
    publishWiredPart(*storage, sender_ns, "all_1_1_0");

    auto * exchange = dynamic_cast<DB::IContentAddressedExchange *>(storage.get());
    ASSERT_NE(exchange, nullptr);
    auto offer = exchange->getRelinkOffer("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0");
    ASSERT_TRUE(offer.has_value());
    const String & bytes = offer->manifest_bytes;

    /// Artificially delete a referenced pool blob — the live-sender invariant excludes this on the real
    /// path; §4 promote does not re-probe it, so adopt trusts the manifest edge and publishes.
    const auto data_key = storage->store()->layout().blobKey(idOf("payload-A"));
    const auto h = storage->store()->backend().head(data_key);
    ASSERT_TRUE(h.exists);
    ASSERT_EQ(storage->store()->backend().deleteExact(data_key, h.token).kind,
              DB::Cas::DeleteOutcome::Kind::Deleted);

    /// §4: promote trusts the adopted leaves — no re-probe — so adopt SUCCEEDS (returns true) and publishes.
    const bool ok = adoptPartFromManifestAndPromote(*exchange, kReceiverTmpFetchPath, bytes);
    EXPECT_TRUE(ok) << "§4: adopt trusts the manifest edge; a raced pool blob is not re-probed at promote";

    /// The receiver ref publishes (the D4 trade-off), and the deleted pool blob surfaces via fsck's
    /// reachable-but-absent scan (the backstop — INV-NO-DANGLE-via-fsck).
    EXPECT_TRUE(storage->store()->resolveRef(storage->liveNamespace("a22a22a2-2222-4222-8222-222222222222"), "tmp-fetch_all_1_1_0").has_value());
    const DB::Cas::FsckReport rep = DB::Cas::runFsck(*storage->store(), /*detail=*/true);
    EXPECT_GE(rep.dangling, 1u) << "§4 D4 backstop: the deleted pool blob must surface as an fsck dangling "
                                   "finding (dangling=" << rep.dangling << ")";
}

/// All-tree task 7/9: relink self-containment. Task 6 routes uuid.txt/metadata_version.txt through
/// the content path, so a committed part's manifest ENTRIES already carry these files — the receiver
/// no longer needs a mutable_files sidecar to reconstruct them. Task 9 completed the cleanup:
/// `adoptPartFromManifest` no longer even HAS a sidecar parameter (Fetcher::relinkPartToDisk's call
/// site simply dropped the argument). This publishes a part whose per-part files are ordinary
/// manifest entries and adopts it, mirroring the post-task-9 call site exactly.
TEST(CASWiringExchange, AdoptPartFromManifestSelfContainedWithoutMutableFilesSidecar)
{
    auto storage = openWiringStorage();
    const auto sender_ns = storage->liveNamespace("a11a11a1-1111-4111-8111-111111111111");

    DB::Cas::PartWriteInfo info;
    info.intended_ref = sender_ns.string() + "/all_1_1_0";
    info.intended_namespace = sender_ns;
    auto build = storage->store()->beginPartWrite(info);
    const auto id = build->stageManifest(
        {wiringBlobEntry("data.bin", "payload-A"),
         wiringBlobEntry("uuid.txt", "payload-uuid"),
         wiringBlobEntry("metadata_version.txt", "payload-mv")});
    build->precommitAdd(sender_ns, "all_1_1_0", id);
    build->putBlob(idOf("payload-A"), DB::Cas::BlobSource::fromString("payload-A"));
    build->putBlob(idOf("payload-uuid"), DB::Cas::BlobSource::fromString("payload-uuid"));
    build->putBlob(idOf("payload-mv"), DB::Cas::BlobSource::fromString("payload-mv"));
    build->promote(sender_ns, "all_1_1_0", build->buildId(), id);

    auto * exchange = dynamic_cast<DB::IContentAddressedExchange *>(storage.get());
    ASSERT_NE(exchange, nullptr);
    auto offer = exchange->getRelinkOffer("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0");
    ASSERT_TRUE(offer.has_value());
    const String & bytes = offer->manifest_bytes;
    const DB::Cas::PartManifest decoded = DB::Cas::decodePartManifest(bytes);
    ASSERT_EQ(decoded.entries.size(), 3u) << "uuid.txt/metadata_version.txt travel as ordinary entries";

    /// No sidecar parameter to pass anymore — exactly what Fetcher::relinkPartToDisk's call looks like
    /// now that the manifest is self-contained (no reconstruction from a wire-transferred header).
    const bool ok = adoptPartFromManifestAndPromote(*exchange, kReceiverTmpFetchPath, bytes);
    EXPECT_TRUE(ok);

    const auto receiver_ns = storage->liveNamespace("a22a22a2-2222-4222-8222-222222222222");
    auto resolved = storage->store()->resolveRef(receiver_ns, "tmp-fetch_all_1_1_0");
    ASSERT_TRUE(resolved.has_value());

    const DB::Cas::PartManifest receiver_manifest = storage->store()->readManifest(resolved->manifest_id);
    ASSERT_EQ(receiver_manifest.entries.size(), 3u);
    bool has_uuid_entry = false;
    bool has_metadata_version_entry = false;
    for (const auto & entry : receiver_manifest.entries)
    {
        if (entry.path == "uuid.txt")
            has_uuid_entry = true;
        if (entry.path == "metadata_version.txt")
            has_metadata_version_entry = true;
    }
    EXPECT_TRUE(has_uuid_entry) << "uuid.txt must read back as an ordinary content entry, not mutable_files";
    EXPECT_TRUE(has_metadata_version_entry)
        << "metadata_version.txt must read back as an ordinary content entry, not mutable_files";
}

/// Publish-then-confirm, receiver half (Task 14): `prepare` must make the receiver's `+1` DURABLE while
/// publishing NOTHING. That combination is the protocol -- the durable `+1` is what a later `yes` is
/// worth anything against, and the absent committed ref is what makes an unproven source cost nothing.
TEST(CASWiringExchange, PrepareAdoptIsDurableButPublishesNothingUntilPromote)
{
    auto storage = openWiringStorage();
    const auto sender_ns = storage->liveNamespace("a11a11a1-1111-4111-8111-111111111111");
    publishWiredPart(*storage, sender_ns, "all_1_1_0");

    auto * exchange = dynamic_cast<DB::IContentAddressedExchange *>(storage.get());
    ASSERT_NE(exchange, nullptr);
    auto offer = exchange->getRelinkOffer("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0");
    ASSERT_TRUE(offer.has_value());

    const auto receiver_ns = storage->liveNamespace("a22a22a2-2222-4222-8222-222222222222");
    std::unique_ptr<DB::ICaPreparedRelink> prepared;
    ASSERT_EQ(exchange->prepareAdoptFromManifest(kReceiverTmpFetchPath, offer->manifest_bytes, prepared),
              DB::CaRelinkPrepare::Prepared);
    ASSERT_NE(prepared, nullptr);

    EXPECT_FALSE(storage->store()->resolveRef(receiver_ns, "tmp-fetch_all_1_1_0").has_value())
        << "prepare must not commit the ref -- the source has not been asked anything yet";
    EXPECT_EQ(storage->store()->livePrecommitsForTest(receiver_ns).size(), 1u)
        << "prepare must leave the receiver's +1 durable, or a later confirm proves nothing";

    EXPECT_EQ(prepared->promote(), DB::CaRelinkPromote::Committed);
    EXPECT_TRUE(storage->store()->resolveRef(receiver_ns, "tmp-fetch_all_1_1_0").has_value());
    EXPECT_TRUE(storage->store()->livePrecommitsForTest(receiver_ns).empty())
        << "promote moves the binding out of the precommit view";
}

/// The unproven-source branch of the taxonomy (row 3), at the storage seam: `abort` releases the durable
/// `+1` and publishes nothing. A leaked same-epoch precommit is reclaimed by nothing -- not the
/// prior-epoch stale sweep, not GC -- so this removal is the ONLY thing standing between an unproven
/// confirm and permanently retained blobs.
TEST(CASWiringExchange, AbortedPrepareReleasesThePrecommitAndPublishesNothing)
{
    auto storage = openWiringStorage();
    const auto sender_ns = storage->liveNamespace("a11a11a1-1111-4111-8111-111111111111");
    publishWiredPart(*storage, sender_ns, "all_1_1_0");

    auto * exchange = dynamic_cast<DB::IContentAddressedExchange *>(storage.get());
    ASSERT_NE(exchange, nullptr);
    auto offer = exchange->getRelinkOffer("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0");
    ASSERT_TRUE(offer.has_value());

    const auto receiver_ns = storage->liveNamespace("a22a22a2-2222-4222-8222-222222222222");
    std::unique_ptr<DB::ICaPreparedRelink> prepared;
    ASSERT_EQ(exchange->prepareAdoptFromManifest(kReceiverTmpFetchPath, offer->manifest_bytes, prepared),
              DB::CaRelinkPrepare::Prepared);
    ASSERT_NE(prepared, nullptr);
    ASSERT_EQ(storage->store()->livePrecommitsForTest(receiver_ns).size(), 1u);

    prepared->abort();
    EXPECT_TRUE(storage->store()->livePrecommitsForTest(receiver_ns).empty())
        << "abort must append the exact precommit removal, not merely drop the transaction";
    EXPECT_FALSE(storage->store()->resolveRef(receiver_ns, "tmp-fetch_all_1_1_0").has_value())
        << "an aborted relink must leave no committed ref behind";

    /// A second `abort` -- what the scope guard does after an explicit one -- must be a silent no-op
    /// rather than an error, and destruction of an aborted handle must not re-drive anything.
    prepared->abort();
    prepared.reset();
    EXPECT_TRUE(storage->store()->livePrecommitsForTest(receiver_ns).empty());
}

/// The `MechanismFallbackAllowed` branch (taxonomy row 2): an undecodable manifest is a mechanism
/// failure, not a source failure -- the sender still has the part, so the receiver byte-fetches. Nothing
/// may be staged, because there is no handle to abort it with.
TEST(CASWiringExchange, PrepareAdoptOfAnUndecodableManifestAllowsTheByteFallback)
{
    auto storage = openWiringStorage();
    auto * exchange = dynamic_cast<DB::IContentAddressedExchange *>(storage.get());
    ASSERT_NE(exchange, nullptr);

    const auto receiver_ns = storage->liveNamespace("a22a22a2-2222-4222-8222-222222222222");
    std::unique_ptr<DB::ICaPreparedRelink> prepared;
    EXPECT_EQ(exchange->prepareAdoptFromManifest(kReceiverTmpFetchPath, "not a manifest at all", prepared),
              DB::CaRelinkPrepare::MechanismFallbackAllowed);
    EXPECT_EQ(prepared, nullptr) << "no handle may be returned when nothing was staged";
    EXPECT_TRUE(storage->store()->livePrecommitsForTest(receiver_ns).empty());
    EXPECT_FALSE(storage->store()->resolveRef(receiver_ns, "tmp-fetch_all_1_1_0").has_value());
}

/// B66b: a relink whose TARGET is a DETACHED part dir -- what `FETCH PARTITION ... TO detached` now
/// does instead of streaming bytes. Nothing about the detached case is special-cased on the receiver:
/// `Fetcher::relinkPartToDisk` hands over the staging path under the `detached/` parent and the router
/// folds it onto a `detached/`-prefixed ref in the table's OWN namespace, exactly as every other read
/// and write of a detached part is routed.
///
/// The load-bearing assertion is the NEGATIVE one. A detached fetch must publish a detached ref and
/// nothing else: a live ref of the same name would make an un-attached part visible to the table, which
/// is the one way a detached target could differ from the active one in a way that matters.
TEST(CASWiringExchange, AdoptIntoADetachedTargetPublishesADetachedRefAndNoLiveRef)
{
    auto storage = openWiringStorage();
    const auto sender_ns = storage->liveNamespace("a11a11a1-1111-4111-8111-111111111111");
    publishWiredPart(*storage, sender_ns, "all_1_1_0");

    auto * exchange = dynamic_cast<DB::IContentAddressedExchange *>(storage.get());
    ASSERT_NE(exchange, nullptr);
    auto offer = exchange->getRelinkOffer("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0");
    ASSERT_TRUE(offer.has_value());

    /// The receiver's staging path under the detached parent -- the path `relinkPartToDisk` composes
    /// with `to_detached`, and the same one `downloadPartToDisk` would have written bytes into.
    const String detached_tmp_path
        = "a22/a22a22a2-2222-4222-8222-222222222222/detached/tmp-fetch_all_1_1_0";
    EXPECT_TRUE(adoptPartFromManifestAndPromote(*exchange, detached_tmp_path, offer->manifest_bytes));

    const auto receiver_ns = storage->liveNamespace("a22a22a2-2222-4222-8222-222222222222");
    EXPECT_TRUE(storage->store()->resolveRef(receiver_ns, "detached/tmp-fetch_all_1_1_0").has_value())
        << "the detached target must publish the `detached/`-prefixed ref in the table's own namespace";
    EXPECT_FALSE(storage->store()->resolveRef(receiver_ns, "tmp-fetch_all_1_1_0").has_value())
        << "a detached fetch must NOT publish a live ref of the same name";

    /// The adopted part reads back through the ordinary path surface, blobs and per-part files alike --
    /// no bytes were transferred for any of them.
    EXPECT_TRUE(storage->existsFile(detached_tmp_path + "/data.bin"));
    EXPECT_TRUE(storage->existsFile(detached_tmp_path + "/p.proj/data.bin"));
    EXPECT_TRUE(storage->existsFile(detached_tmp_path + "/uuid.txt"));

    /// Finalization, unchanged by this task: `IMergeTreeDataPart::renameTo(detached/<part>)` is a
    /// moveDirectory of the staged dir to its final detached name, which on a content-addressed disk is
    /// a ref repoint WITHIN the same namespace -- the same shape the active path's
    /// `renameTempPartAndReplace` uses, and the reason the relinked detached part needs no new
    /// finalization of its own.
    {
        auto tx = storage->createTransaction();
        tx->moveDirectory(detached_tmp_path, "a22/a22a22a2-2222-4222-8222-222222222222/detached/all_1_1_0");
        tx->commit(DB::NoCommitOptions{});
    }
    EXPECT_TRUE(storage->existsFile(
        "a22/a22a22a2-2222-4222-8222-222222222222/detached/all_1_1_0/data.bin"));
    EXPECT_FALSE(storage->store()->resolveRef(receiver_ns, "detached/tmp-fetch_all_1_1_0").has_value());
    EXPECT_EQ(storage->detachedRefNames(receiver_ns), (std::vector<std::string>{"detached/all_1_1_0"}));
    EXPECT_FALSE(storage->store()->resolveRef(receiver_ns, "all_1_1_0").has_value())
        << "the detached finalization must stay inside the `detached/` ref space";
}

/// A relink target that is not a part DIRECTORY is a caller bug, and it must be loud rather than
/// answered with `MechanismFallbackAllowed`: the byte fetch that a fallback invites would write to the
/// same wrong place. The table dir stands in for the whole class (a file inside a part, a FREEZE shadow
/// path, a bare `detached` container) -- all of them route to something that is not a part ref.
///
/// The refusal throws LOGICAL_ERROR, which aborts the whole process in debug/sanitizer builds
/// (Exception.cpp's handle_error_code) instead of behaving like a catchable exception -- so the
/// EXPECT_THROW form only makes sense in a plain release build, and CASWiringExchangeDeathTest below
/// proves the SAME refusals positively abort under debug/sanitizer builds instead (same pattern as
/// CASWiringOpsDeathTest above).
#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(CASWiringExchange, PrepareAdoptRefusesATargetThatIsNotAPartDirectory)
{
    auto storage = openWiringStorage();
    auto * exchange = dynamic_cast<DB::IContentAddressedExchange *>(storage.get());
    ASSERT_NE(exchange, nullptr);

    std::unique_ptr<DB::ICaPreparedRelink> prepared;
    EXPECT_THROW(exchange->prepareAdoptFromManifest(
                     "a22/a22a22a2-2222-4222-8222-222222222222", std::string{}, prepared),
                 DB::Exception);
    EXPECT_THROW(exchange->prepareAdoptFromManifest(
                     "a22/a22a22a2-2222-4222-8222-222222222222/tmp-fetch_all_1_1_0/data.bin",
                     std::string{}, prepared),
                 DB::Exception);
    EXPECT_THROW(exchange->prepareAdoptFromManifest(
                     "shadow/bk1/store/a22/a22a22a2-2222-4222-8222-222222222222/all_1_1_0",
                     std::string{}, prepared),
                 DB::Exception);
    EXPECT_EQ(prepared, nullptr);
}
#else
TEST(CASWiringExchangeDeathTest, PrepareAdoptRefusesATargetThatIsNotAPartDirectoryAborts)
{
    auto storage = openWiringStorage();
    auto * exchange = dynamic_cast<DB::IContentAddressedExchange *>(storage.get());
    ASSERT_NE(exchange, nullptr);

    std::unique_ptr<DB::ICaPreparedRelink> prepared;
    EXPECT_DEATH(exchange->prepareAdoptFromManifest(
                     "a22/a22a22a2-2222-4222-8222-222222222222", std::string{}, prepared),
                 "does not address a content-addressed part directory");
    EXPECT_DEATH(exchange->prepareAdoptFromManifest(
                     "a22/a22a22a2-2222-4222-8222-222222222222/tmp-fetch_all_1_1_0/data.bin",
                     std::string{}, prepared),
                 "does not address a content-addressed part directory");
    EXPECT_DEATH(exchange->prepareAdoptFromManifest(
                     "shadow/bk1/store/a22/a22a22a2-2222-4222-8222-222222222222/all_1_1_0",
                     std::string{}, prepared),
                 "does not address a content-addressed part directory");
    EXPECT_EQ(prepared, nullptr);
}
#endif

/// ==== Commit atomicity (B122): a publish failing mid-loop must not leave a PARTIAL commit ====

#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>
#include <Common/Exception.h>
#include <algorithm>
#include <set>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CORRUPTED_DATA;
    extern const int READONLY;
}

namespace
{

/// A LocalObjectStorage whose writeObject can be armed to throw — the single seam needed to drive a
/// backend write failure at a chosen point. The hook runs BEFORE the write is created; throwing from
/// it fails the put exactly as a real backend error would. Everything else delegates to the base.
class FaultyLocalObjectStorage : public DB::LocalObjectStorage
{
public:
    using DB::LocalObjectStorage::LocalObjectStorage;

    std::function<void(const std::string &)> on_write;

    std::unique_ptr<DB::WriteBufferFromFileBase> writeObject(
        const DB::StoredObject & object,
        DB::WriteMode mode,
        std::optional<DB::ObjectAttributes> attributes,
        size_t buf_size,
        const DB::WriteSettings & write_settings) override
    {
        if (on_write)
            on_write(object.remote_path);
        return DB::LocalObjectStorage::writeObject(object, mode, attributes, buf_size, write_settings);
    }
};

/// True for a per-part manifest BODY object (<...>/cas/manifests/<namespace...>/<epoch-seq>/<NNNNNN>.zst)
/// — the FIRST durable object `publishStaging` writes for a part (via `PartWriteTxn::stageManifest`). Since Task B
/// (chaos-tolerance-report) that write rides the CAS request controller: a transient fault is retried
/// (budgeted attempts + resolve-before-reissue), so an injected fault must be PERSISTENT to fail the
/// publish — the controller exhausts its budget and `stageManifest` throws ABORTED out of `publishStaging`.
/// Exactly one body per part (retries re-PUT the same per-part key), so counting FIRST attempts isolates
/// part publishes one-for-one. Ref-log txns (`cas/ns/stream/.../_log/...`), tree blobs (`blobs/`), GC state
/// (`gc/`) and verbatim files are excluded.
///
/// The suffix is taken from `storedSuffix(FormatId::PartManifest)` (the registered v3 stored suffix, now
/// `.zst`) rather than hard-coded: codecs-v3 phase-3 made the part manifest an Always-compressed text
/// object, changing the body key from the pre-v3 `.proto` to `.zst`. The old hard-coded
/// `.ends_with(".proto")` stopped matching after that cutover, so the fault never fired and this
/// (test-local) predicate silently no-op'd — the same failure mode this comment already recorded for the
/// earlier `RootShardManifest` removal (commit `318291fe5e5`, whose all-digits key stopped matching).
/// Sourcing the suffix from the format registry keeps the predicate correct across future
/// compression-policy changes.
bool isPartManifestBodyPath(const std::string & path)
{
    return path.find("/cas/manifests/") != std::string::npos
        && path.ends_with(DB::Cas::storedSuffix(DB::Cas::FormatId::PartManifest));
}

std::shared_ptr<FaultyLocalObjectStorage> makeFaultyStorageForTest()
{
    static std::atomic<uint64_t> counter{0};
    const auto unique = std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1));
    const auto root = (std::filesystem::temp_directory_path() / ("ca_b122_" + unique)).string();
    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    std::filesystem::create_directories(root, ec);
    return std::make_shared<FaultyLocalObjectStorage>(DB::LocalObjectStorageSettings("test", root, /*read_only_=*/false));
}

}

TEST(CASWiringWrite, PartialCommitRollsBackPublishedParts)
{
    auto faulty = makeFaultyStorageForTest();
    auto settings = DB::Cas::tests::makeSettingsForTest(
        "test", std::filesystem::temp_directory_path() / "ca_b122_scratch");
    auto storage = std::make_shared<DB::ContentAddressedMetadataStorage>(
        faulty, "pool", "srv1", "", nullptr, settings);
    storage->startup();
    /// The manifest-body PUT rides the CAS request controller, whose inter-attempt backoff would
    /// otherwise serve the REAL capped-exponential sleeps (~56s at the default budget) while the
    /// persistent injected fault exhausts the whole attempt budget. Neutralize only the sleeps — the
    /// retry/exhaustion/rollback semantics under test are unchanged.
    storage->store()->setCasRetrySleepForTest([](uint64_t) {});

    /// Two parts in ONE transaction, published sequentially at commit (the staging map orders all_1_1_0
    /// before all_2_2_0). writeThroughTransaction only STAGES to local temp files here — the pool writes
    /// (manifest bodies, blob uploads, ref-log promotes) all happen later, inside commit's publishStaging.
    auto tx = storage->createTransaction();
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin", "content-A");
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_2_2_0/data.bin", "content-B");

    /// Fail the SECOND part's manifest-body write (all_2_2_0's stageManifest) — by then all_1_1_0 has
    /// fully published (its manifest body + blob + promoted ref). A pre-B122 commit() would leave
    /// all_1_1_0 durably visible: a partial commit. PERSISTENT (`>= 2`, not one-shot): the manifest
    /// body PUT rides the CAS request controller (Task B), which absorbs a transient fault by design —
    /// only a fault that outlasts the whole attempt budget fails the publish (as ABORTED).
    /// CORRUPTED_DATA (not LOGICAL_ERROR): `handle_error_code` (Exception.cpp) aborts the whole
    /// process for LOGICAL_ERROR under debug/sanitizer builds, since that code means "an internal
    /// invariant broke" there -- but this is a simulated BACKEND write failure, not an invariant
    /// violation, so it must stay a catchable exception. CORRUPTED_DATA keeps the exact same
    /// `isDeterministicLocalFailure` classification LOGICAL_ERROR had (CasRequestControl.cpp), so the
    /// controller's retry/exhaustion behavior under test is unchanged.
    int manifest_writes = 0;
    faulty->on_write = [&](const std::string & path)
    {
        if (isPartManifestBodyPath(path) && ++manifest_writes >= 2)
            throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "injected publish failure (B122)");
    };

    EXPECT_THROW(tx->commit(DB::NoCommitOptions{}), DB::Exception);

    /// All-or-nothing: the part that DID publish must have been rolled back (commit's compensating
    /// `dropRefIfMatches`, keyed on the exact `CommitOutcome` `all_1_1_0`'s own publish produced).
    /// Disarm first so the read-back assertions run clean — the rollback itself only writes ref-log
    /// ops, never a manifest body, so it does not re-trip the count-2 fault.
    faulty->on_write = nullptr;
    EXPECT_FALSE(storage->existsDirectory("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0"));
    EXPECT_FALSE(storage->existsDirectory("a11/a11a11a1-1111-4111-8111-111111111111/all_2_2_0"));
}

TEST(CASWiringReadOnly, ObserveOnlyOpenReadsButRejectsWrites)
{
    /// 1. Writable storage publishes a part into a fixed root.
    const auto root = (std::filesystem::temp_directory_path()
                       / ("ca_ro_" + std::to_string(::getpid()))).string();
    std::error_code ec; std::filesystem::remove_all(root, ec); std::filesystem::create_directories(root, ec);
    auto writable_os = std::make_shared<DB::LocalObjectStorage>(
        DB::LocalObjectStorageSettings("test", root, /*read_only_=*/false));
    {
        auto w_settings = DB::Cas::tests::makeSettingsForTest(
            "test", std::filesystem::temp_directory_path() / "ca_ro_scratch");
        auto w = std::make_shared<DB::ContentAddressedMetadataStorage>(
            writable_os, "pool", "srv1", "", nullptr, w_settings);
        w->startup();
        auto tx = w->createTransaction();
        writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin", "ro-bytes");
        tx->commit(DB::NoCommitOptions{});
    }

    /// 2. Read-only object storage over the SAME root => observe-only metadata storage.
    auto ro_os = std::make_shared<DB::LocalObjectStorage>(
        DB::LocalObjectStorageSettings("test", root, /*read_only_=*/true));
    /// Same `server_root_id` as the writer: live namespaces are rooted by configured layout identity, so an
    /// observe-only mount reads the same server-root's data — the WORM scenario.
    auto ro_settings = DB::Cas::tests::makeSettingsForTest(
        "test", std::filesystem::temp_directory_path() / "ca_ro_scratch2");
    auto ro = std::make_shared<DB::ContentAddressedMetadataStorage>(
        ro_os, "pool", "srv1", "", nullptr, ro_settings);
    ro->startup();   /// must NOT throw (probe skipped — a probe write would fail on a read-only os)

    EXPECT_TRUE(ro->isReadOnly());
    /// Reads work:
    EXPECT_TRUE(ro->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin"));
    EXPECT_EQ(ro->getFileSize("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin"), 8u);
    /// Writes fail closed:
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::READONLY,
        [&] { ro->createTransaction(); });
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::READONLY,
        [&]
        {
            std::unique_ptr<DB::ICaPreparedRelink> prepared;
            ro->prepareAdoptFromManifest("a11/a11a11a1-1111-4111-8111-111111111111/tmp-fetch", std::string{}, prepared);
        });
}

TEST(CASWiringRead, UnsetPublishedAtMsReturnsEpoch)
{
    /// A ref published without a stamp (published_at_ms == 0, the default) must return the epoch
    /// (Poco::Timestamp(0)) rather than throwing: stamps only feed cleanup TTLs and system tables,
    /// so a missing stamp is harmless.
    auto storage = openWiringStorage();
    auto tx = storage->createTransaction();
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin", "x");
    tx->commit(DB::NoCommitOptions{});

    /// Ensure published_at_ms is unset (the default is 0).
    storage->store()->updateRefPublishedAt(storage->liveNamespace("a11a11a1-1111-4111-8111-111111111111"), "all_1_1_0",
        [](DB::Cas::RefPublishedAtUpdate & r) { r.published_at_ms = 0; });

    EXPECT_EQ(storage->getLastModified("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0").epochTime(), 0);
}

/// ==== B188 precommit-first order invariant (Task 6) ====
///
/// A RecordingLocalObjectStorage records the four IObjectStorage methods the CA emulated-mode backend
/// uses on the commit path — writeObject (PUT), exists + getObjectMetadata (the HEAD), and readObject
/// (the GET) — as (op_name, logical_key). "Logical" means the bare pool key (without the emu_root
/// prefix) — the same string the Layout functions produce, so the `/blobs/`, `/trees/`, and opaque
/// ref-stream (`/cas/ns/stream/<life_id>/`) substring tests are unambiguous.
///
/// After commit the test asserts: the FIRST write that appends the create-precommit owner event (the
/// first durable CAS to the target ROOT SHARD's key — owner_kind == Precommit; the converged rev. 15
/// model has NO `_precommits` namespace, the precommit binding lives in the target shard's journal)
/// happened before ALL ops (read OR write) on keys containing "/blobs/" or "/trees/". The precommit
/// owner record is what pins the in-flight build-root closure so GC cannot reclaim the not-yet-uploaded
/// content objects; therefore every pool op touching a content blob or the manifest tree must be AFTER
/// the precommit owner record is durably written. The READ gating is the heart of the B188 fix: the
/// original bug was an EAGER HEAD on a content blob during staging, before any precommit protection
/// existed — a write-only assertion would not catch its reintroduction.

namespace
{

/// Records the four IObjectStorage methods the CA emulated-mode backend uses on the commit path
/// (writeObject/exists/getObjectMetadata/readObject). listObjects/copyObject are deliberately NOT
/// overridden — they are not on the commit path the order invariant gates.
class RecordingLocalObjectStorage final : public DB::LocalObjectStorage
{
public:
    using DB::LocalObjectStorage::LocalObjectStorage;

    struct Record
    {
        std::string op;    /// "writeObject" | "exists" | "getObjectMetadata" | "readObject"
        std::string key;   /// logical (emu_root stripped)
    };

    /// Append-only; mutable so the const read methods (exists/readObject/tryGetObjectMetadata) can
    /// record. No mutex — these tests are single-threaded.
    mutable std::vector<Record> ops;

    /// Strip the common-key-prefix (emu_root) to recover the logical key. The emu_root is returned by
    /// getCommonKeyPrefix() and always ends with a path separator in LocalObjectStorage.
    std::string toLogical(const std::string & physical) const
    {
        const std::string root = getCommonKeyPrefix();
        std::string logical;
        if (!root.empty() && physical.starts_with(root))
            logical = physical.substr(root.size());
        else
            logical = physical;
        /// Strip any leading slash left after prefix removal.
        if (!logical.empty() && logical.front() == '/')
            logical = logical.substr(1);
        return logical;
    }

    std::unique_ptr<DB::WriteBufferFromFileBase> writeObject(
        const DB::StoredObject & object,
        DB::WriteMode mode,
        std::optional<DB::ObjectAttributes> attributes,
        size_t buf_size,
        const DB::WriteSettings & write_settings) override
    {
        ops.push_back({"writeObject", toLogical(object.remote_path)});
        return DB::LocalObjectStorage::writeObject(object, mode, attributes, buf_size, write_settings);
    }

    /// Backs the CA backend's `head` (emuExists) and gates its `get` (emuExists before emuRead).
    bool exists(const DB::StoredObject & object) const override
    {
        ops.push_back({"exists", toLogical(object.remote_path)});
        return DB::LocalObjectStorage::exists(object);
    }

    /// Backs the CA backend's `head` size/attributes lookup (emuPath stat).
    std::optional<DB::ObjectMetadata> tryGetObjectMetadata(const std::string & path, bool with_tags) const override
    {
        ops.push_back({"getObjectMetadata", toLogical(path)});
        return DB::LocalObjectStorage::tryGetObjectMetadata(path, with_tags);
    }

    /// Backs the CA backend's `get` body read (readObjectRanged).
    std::unique_ptr<DB::ReadBufferFromFileBase> readObject(
        const DB::StoredObject & object,
        const DB::ReadSettings & read_settings,
        std::optional<size_t> read_hint,
        bool use_external_buffer,
        bool restrict_seek) const override
    {
        ops.push_back({"readObject", toLogical(object.remote_path)});
        return DB::LocalObjectStorage::readObject(object, read_settings, read_hint, use_external_buffer, restrict_seek);
    }
};

std::shared_ptr<RecordingLocalObjectStorage> makeRecordingStorageForTest(const std::string & tag)
{
    static std::atomic<uint64_t> counter{0};
    const auto unique = std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1));
    const auto root = (std::filesystem::temp_directory_path() / ("ca_b188_" + tag + "_" + unique)).string();
    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    std::filesystem::create_directories(root, ec);
    return std::make_shared<RecordingLocalObjectStorage>(
        DB::LocalObjectStorageSettings("test", root, /*read_only_=*/false));
}

/// True for a durable ref-object write key under `/cas/ns/stream/`. In the snapshot+log ref model the
/// writer's first durable ref write on the precommit path is an immutable transaction-log object
/// (`<...>/cas/ns/stream/<life_id>/_log/<txn-id>.zst`); a published table snapshot is
/// `<...>/_snap/<txn-id>.zst`. The predicate anchors on whichever durable ref write comes first. It
/// excludes blobs (`/blobs/`), part-manifests (`/cas/manifests/...`), GC state (`/gc/`), and verbatim
/// files (`/_files/...`).
bool isRefWriteKey(const std::string & key)
{
    if (key.find("/cas/ns/stream/") == std::string::npos)
        return false;
    return key.find("/_log/") != std::string::npos || key.find("/_snap/") != std::string::npos;
}

/// Index of the first writeObject that durably appends the create-precommit ref transaction — i.e. the
/// first durable write (writeObject) of a ref-object key (a `_log/<txn-id>` object in the snapshot+log
/// model). Anchors on the WRITE, not on any op: recovery READS the ref prefix before the durable write,
/// so an any-op scan would anchor on that READ rather than the durable write. Returns -1 if no ref write
/// was recorded.
int firstPrecommitWriteIdx(const std::vector<RecordingLocalObjectStorage::Record> & log)
{
    for (int i = 0; i < static_cast<int>(log.size()); ++i)
        if (log[i].op == "writeObject" && isRefWriteKey(log[i].key))
            return i;
    return -1;
}

}

/// B188: every pool op (read OR write) on /blobs/ or /trees/ must come AFTER the first write that
/// appends the create-precommit owner event (the first root-shard CAS) — including HEAD
/// (exists/getObjectMetadata) and GET (readObject), since the
/// exact bug was an eager HEAD on a content blob during staging. The transaction writes a fresh
/// content file (pending blob) AND adopts an existing committed blob via hardlink — both paths must
/// satisfy the invariant.
TEST(CASWiringPrecommitOrder, NoContentPoolOpBeforePrecommit)
{
    auto recording = makeRecordingStorageForTest("order");
    auto settings = DB::Cas::tests::makeSettingsForTest(
        "test", std::filesystem::temp_directory_path() / "ca_b188_order_scratch");
    auto storage = std::make_shared<DB::ContentAddressedMetadataStorage>(
        recording, "pool", "srv1", "", nullptr, settings);
    storage->startup();

    /// Phase 1: publish a committed source part — this gives us a committed blob to adopt in Phase 2.
    {
        auto tx = storage->createTransaction();
        writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_0_0_0/data.bin", "source-blob");
        tx->commit(DB::NoCommitOptions{});
    }

    /// Phase 2: a new transaction that BOTH writes a fresh content blob (all_1_1_0/data.bin, pending)
    /// AND carries forward that PENDING blob via hardlink into a second fresh part (all_2_2_0/extra.bin,
    /// the cross-part pending-source adopt path). We clear the op log after Phase 1 so only Phase 2's
    /// ops are analysed.
    recording->ops.clear();

    auto tx = storage->createTransaction();
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin", "fresh-content");
    /// Adopt by hardlinking a PENDING blob (the file just written above) into a SECOND fresh part
    /// (all_2_2_0). This is the B188-relevant adopt: the cross-part pending-source branch copies the
    /// PendingBlob into the dst build (NO eager pool op — the blob is not durable yet, so a HEAD/GET on
    /// it before precommit would be the exact bug). We deliberately do NOT adopt from the committed
    /// source part here: adoptFromTree(committed source) legitimately READS that source's
    /// already-durable, ref-pinned tree during staging — a foreign-tree read that is NOT a B188
    /// violation (the invariant is about THIS build's own not-yet-uploaded content, never a committed
    /// object owned by a live part). Gating it would be a false positive; see the committed-source
    /// adopt coverage in CASWiringOps.HardLinkCarriesForwardWithoutReupload.
    tx->createHardLink("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin", "a11/a11a11a1-1111-4111-8111-111111111111/all_2_2_0/extra.bin");
    tx->commit(DB::NoCommitOptions{});

    const auto & log = recording->ops;

    /// The content objects THIS transaction publishes are exactly the BLOB keys it WRITES under
    /// /blobs/ (the fresh/pending content blobs). The B188 invariant is that the build must not touch
    /// ITS OWN not-yet-protected content before precommit. NOTE (rev. 15 manifest model): the staged
    /// part-manifest body (`/_manifests/...`) is the precommit's EVIDENCE and is therefore written
    /// BEFORE precommitAdd by design (stageManifest → precommitAdd → putBlob → promote) — it is NOT a
    /// gated content object. Only the content BLOBS must wait for the precommit. Reads of foreign
    /// committed objects (another part's blob) are legitimate and must not be gated — so we restrict
    /// the gate to the set of /blobs/ keys this transaction itself wrote.
    std::set<std::string> own_content_keys;
    for (const auto & r : log)
        if (r.op == "writeObject" && r.key.find("/blobs/") != std::string::npos)
            own_content_keys.insert(r.key);

    /// Anchor on the first precommit WRITE (the durable casPut), not on any precommit-key op.
    const int first_precommit_idx = firstPrecommitWriteIdx(log);
    ASSERT_GE(first_precommit_idx, 0)
        << "No create-precommit owner write (root-shard CAS) was recorded — precommit step did not fire";

    /// Every op (read OR write) on one of THIS build's own content blobs must have an index AFTER
    /// first_precommit_idx. This gates HEAD (exists/getObjectMetadata) and GET (readObject), not just
    /// PUT (writeObject) — an eager HEAD/GET on the build's own pending blob before precommit is the
    /// exact B188 regression this guards against.
    for (int i = 0; i < static_cast<int>(log.size()); ++i)
    {
        if (!own_content_keys.contains(log[i].key))
            continue;
        EXPECT_GT(i, first_precommit_idx)
            << "Own-content pool op '" << log[i].op << "' on '" << log[i].key << "' at index " << i
            << " came BEFORE the first precommit write at index " << first_precommit_idx
            << " — violates B188 precommit-first invariant (no HEAD/GET/PUT on this build's content before precommit)";
    }

    /// Sanity: both parts are readable after commit, with the SAME underlying blob (content identity).
    EXPECT_TRUE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin"));
    EXPECT_TRUE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/all_2_2_0/extra.bin"));
    EXPECT_EQ(storage->getFileSize("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin"), 13u);   /// "fresh-content"
    EXPECT_EQ(storage->getStorageObjects("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin")[0].remote_path,
              storage->getStorageObjects("a11/a11a11a1-1111-4111-8111-111111111111/all_2_2_0/extra.bin")[0].remote_path);

    /// Confirm at least one blob WRITE and one staged-manifest WRITE were recorded (both the upload
    /// path and the manifest-evidence path were exercised), so the gate above actually had content
    /// keys to check and the precommit anchored on a real build.
    const bool has_blob_write = std::any_of(log.begin(), log.end(),
        [](const RecordingLocalObjectStorage::Record & r)
        { return r.op == "writeObject" && r.key.find("/blobs/") != std::string::npos; });
    const bool has_tree_write = std::any_of(log.begin(), log.end(),
        [](const RecordingLocalObjectStorage::Record & r)
        { return r.op == "writeObject" && r.key.find("/cas/manifests/") != std::string::npos; });
    EXPECT_TRUE(has_blob_write) << "No /blobs/ write recorded — fresh blob path not exercised";
    EXPECT_TRUE(has_tree_write) << "No /cas/manifests/ write recorded — manifest staging path not exercised";
    EXPECT_FALSE(own_content_keys.empty()) << "No own content keys collected — gate would be vacuous";
}

/// B188 committed-source adopt (the LITERAL bug path): when createHardLink carries forward a blob
/// from a COMMITTED source part (the source is NOT staged in this transaction), it takes the
/// adoptFromTree -> adoptEvidence branch — a TOKENLESS W-EVIDENCE dep with NO eager HEAD on the
/// adopted blob. The regression this guards is reverting adoptEvidence to a reuseBlob(false) (or any
/// observeAndAdmit) that HEADs the adopted blob during staging, before any precommit protection
/// exists. The own-content gate in NoContentPoolOpBeforePrecommit CANNOT catch this: the adopted blob
/// is FOREIGN (owned by the live source part, never written by this transaction), so it is absent from
/// own_content_keys. This test asserts a TARGETED invariant on that exact foreign blob key: no
/// exists/getObjectMetadata/readObject/writeObject on it before first_precommit_idx.
///
/// adoptFromTree legitimately READS the source TREE during staging (to find the entry) — that is fine
/// and is NOT asserted here; the assertion is scoped to the adopted BLOB key alone.
TEST(CASWiringPrecommitOrder, CommittedSourceAdoptNoHeadBeforePrecommit)
{
    auto recording = makeRecordingStorageForTest("committed_adopt");
    auto settings = DB::Cas::tests::makeSettingsForTest(
        "test", std::filesystem::temp_directory_path() / "ca_b188_committed_adopt_scratch");
    auto storage = std::make_shared<DB::ContentAddressedMetadataStorage>(
        recording, "pool", "srv1", "", nullptr, settings);
    storage->startup();

    /// Phase 1: commit a source part with a content blob. Capture the source blob's logical key from
    /// the recorded /blobs/ write (the SAME key derivation the recorder uses, so the substring/index
    /// comparisons in Phase 2 line up exactly).
    recording->ops.clear();
    {
        auto tx = storage->createTransaction();
        writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_0_0_0/data.bin", "committed-source-blob");
        tx->commit(DB::NoCommitOptions{});
    }
    std::string source_blob_key;
    for (const auto & r : recording->ops)
    {
        if (r.op == "writeObject" && r.key.find("/blobs/") != std::string::npos)
        {
            source_blob_key = r.key;
            break;
        }
    }
    ASSERT_FALSE(source_blob_key.empty())
        << "Phase 1 recorded no /blobs/ write — could not capture the committed-source blob key";

    /// Phase 2: a FRESH transaction that hardlinks the COMMITTED source blob into a NEW part. The
    /// source part (all_0_0_0) is not staged here, so createHardLink takes the committed-source branch
    /// (adoptFromTree -> adoptEvidence). Clear the log so only Phase 2's ops are analysed.
    recording->ops.clear();
    {
        auto tx = storage->createTransaction();
        tx->createHardLink("a11/a11a11a1-1111-4111-8111-111111111111/all_0_0_0/data.bin", "a11/a11a11a1-1111-4111-8111-111111111111/all_5_5_0/data.bin");
        tx->commit(DB::NoCommitOptions{});
    }

    const auto & log = recording->ops;

    /// Anchor on the first precommit WRITE (the durable casPut), not on any precommit-key op.
    const int first_precommit_idx = firstPrecommitWriteIdx(log);
    ASSERT_GE(first_precommit_idx, 0)
        << "No create-precommit owner write (root-shard CAS) was recorded — precommit step did not fire";

    /// TARGETED assertion: the adopted (foreign, committed) blob key must NOT be touched by ANY op
    /// (HEAD via exists/getObjectMetadata, GET via readObject, or PUT via writeObject) before the
    /// precommit write. With the bug reintroduced, adoptEvidence -> reuseBlob -> observeAndAdmit would
    /// HEAD this exact key during staging at an index < first_precommit_idx, failing here.
    bool adopted_blob_touched_before_precommit = false;
    for (int i = 0; i < first_precommit_idx; ++i)
    {
        if (log[i].key == source_blob_key)
        {
            adopted_blob_touched_before_precommit = true;
            ADD_FAILURE()
                << "Adopted committed-source blob op '" << log[i].op << "' on '" << log[i].key
                << "' at index " << i << " came BEFORE the first precommit write at index "
                << first_precommit_idx << " — violates B188 (committed-source adopt must not HEAD/GET/"
                << "PUT the adopted blob before precommit; expected a tokenless adoptEvidence dep)";
        }
    }
    EXPECT_FALSE(adopted_blob_touched_before_precommit);

    /// The committed-source adopt also must NOT re-upload the blob at all (content carried forward by
    /// reference): no writeObject on the source blob key in Phase 2.
    const bool reuploaded = std::any_of(log.begin(), log.end(),
        [&](const RecordingLocalObjectStorage::Record & r)
        { return r.op == "writeObject" && r.key == source_blob_key; });
    EXPECT_FALSE(reuploaded) << "Committed-source adopt re-uploaded the blob — should carry by reference";

    /// Sanity: the new part reads back and shares the source blob object.
    EXPECT_TRUE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/all_5_5_0/data.bin"));
    EXPECT_EQ(storage->getStorageObjects("a11/a11a11a1-1111-4111-8111-111111111111/all_0_0_0/data.bin")[0].remote_path,
              storage->getStorageObjects("a11/a11a11a1-1111-4111-8111-111111111111/all_5_5_0/data.bin")[0].remote_path);
}

/// B188 pending-blob hardlink (Task 6 Test 2): within a SINGLE transaction, write a content file
/// into part X (pending blob, not yet uploaded), then createHardLink that SAME file into part Y
/// (the cross-part pending-source branch: `&dst_st != src_st`, copies the PendingBlob record so
/// publishStaging uploads it for the dst part too). After commit both parts must read back the
/// identical content.
TEST(CASWiringPending, HardlinkOfPendingBlobCommitsAndReadsBack)
{
    auto storage = openWiringStorage();

    auto tx = storage->createTransaction();

    /// Write fresh content into part X — the blob is PENDING (not uploaded yet, temp-file only).
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_10_10_0/data.bin", "pending-payload");

    /// Before commit, hardlink part X's file into part Y. At this point:
    ///   - src_st = staging for all_10_10_0 (exists: contains the pending blob)
    ///   - dst_st = staging for all_11_11_0 (created fresh here)
    ///   - &dst_st != src_st => PendingBlob is COPIED into dst_st.pending_blobs
    ///   - Both builds get recordPendingBlobDep (tokenless dep — no pool op until post-precommit)
    tx->createHardLink("a11/a11a11a1-1111-4111-8111-111111111111/all_10_10_0/data.bin", "a11/a11a11a1-1111-4111-8111-111111111111/all_11_11_0/data.bin");

    /// Nothing visible yet (B188: no uploads before precommit).
    EXPECT_FALSE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/all_10_10_0/data.bin"));
    EXPECT_FALSE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/all_11_11_0/data.bin"));

    tx->commit(DB::NoCommitOptions{});

    /// Both parts must be visible and carry the same content.
    ASSERT_TRUE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/all_10_10_0/data.bin"));
    ASSERT_TRUE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/all_11_11_0/data.bin"));

    EXPECT_EQ(storage->getFileSize("a11/a11a11a1-1111-4111-8111-111111111111/all_10_10_0/data.bin"), 15u);   /// "pending-payload"
    EXPECT_EQ(storage->getFileSize("a11/a11a11a1-1111-4111-8111-111111111111/all_11_11_0/data.bin"), 15u);

    /// Both parts must point to the SAME underlying blob object (content-addressed identity).
    auto objs_x = storage->getStorageObjects("a11/a11a11a1-1111-4111-8111-111111111111/all_10_10_0/data.bin");
    auto objs_y = storage->getStorageObjects("a11/a11a11a1-1111-4111-8111-111111111111/all_11_11_0/data.bin");
    ASSERT_EQ(objs_x.size(), 1u);
    ASSERT_EQ(objs_y.size(), 1u);
    EXPECT_EQ(objs_x[0].remote_path, objs_y[0].remote_path)
        << "Hardlinked pending blob must map to the SAME pool object in both parts";
}

/// ==== B190 Task 4: precommit-first for republishRef and committed-source createHardLink ====
///
/// B190-A: republishRef (called by moveDirectory for a COMMITTED part rename — RENAME TABLE, DETACH,
/// ATTACH, delete_tmp_ rename) must carry the source part's BLOBS forward by TOKENLESS W-EVIDENCE
/// (adoptEvidence), NOT by HEAD/GET/PUT on the source blob before precommit. In the rev. 15 manifest
/// model republishRef legitimately READS the FOREIGN source MANIFEST body (to copy its entries into a
/// fresh dst manifest) during staging — that is the manifest-era analog of the old adoptFromTree
/// source-tree read and is NOT a violation (see CommittedSourceAdoptNoHeadBeforePrecommit). The
/// invariant that survives: the source BLOB key must not be touched before the first precommit write.
TEST(CASWiringPrecommitOrder, RepublishRefNoTreeHeadBeforePrecommit)
{
    auto recording = makeRecordingStorageForTest("republish");
    auto settings = DB::Cas::tests::makeSettingsForTest(
        "test", std::filesystem::temp_directory_path() / "ca_b190_republish_scratch");
    auto storage = std::make_shared<DB::ContentAddressedMetadataStorage>(
        recording, "pool", "srv1", "", nullptr, settings);
    storage->startup();

    /// Phase 1: commit a source part. Capture its BLOB key from the /blobs/ write (republishRef must
    /// carry this blob by reference, never touching it before precommit).
    recording->ops.clear();
    {
        auto tx = storage->createTransaction();
        writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin", "republish-source");
        tx->commit(DB::NoCommitOptions{});
    }
    std::string source_blob_key;
    for (const auto & r : recording->ops)
    {
        if (r.op == "writeObject" && r.key.find("/blobs/") != std::string::npos)
        {
            source_blob_key = r.key;
            break;
        }
    }
    ASSERT_FALSE(source_blob_key.empty())
        << "Phase 1 recorded no /blobs/ write — could not capture the source blob key";

    /// Phase 2: a COMMITTED rename (delete_tmp_ pattern) that triggers republishRef. Clear the log
    /// so only Phase 2's ops are analysed.
    recording->ops.clear();
    {
        auto tx = storage->createTransaction();
        tx->moveDirectory("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0", "a11/a11a11a1-1111-4111-8111-111111111111/delete_tmp_all_1_1_0");
        tx->commit(DB::NoCommitOptions{});
    }

    const auto & log = recording->ops;

    const int first_precommit_idx = firstPrecommitWriteIdx(log);
    ASSERT_GE(first_precommit_idx, 0)
        << "No create-precommit owner write (root-shard CAS) was recorded — precommit step did not fire";

    /// The source BLOB key must NOT be accessed (HEAD via exists/getObjectMetadata, GET via readObject,
    /// or PUT via writeObject) before the precommit write. With an eager adopt-by-HEAD on the source
    /// blob (the regression), observeAndAdmit HEADs the blob key at an index < first_precommit_idx,
    /// failing here. A tokenless adoptEvidence dep touches nothing.
    bool blob_touched_before_precommit = false;
    for (int i = 0; i < first_precommit_idx; ++i)
    {
        if (log[i].key == source_blob_key)
        {
            blob_touched_before_precommit = true;
            ADD_FAILURE()
                << "republishRef blob op '" << log[i].op << "' on '" << log[i].key
                << "' at index " << i << " came BEFORE the first precommit write at index "
                << first_precommit_idx << " — violates B190 precommit-first: republishRef must not "
                << "HEAD/GET/PUT the source blob before precommit (use tokenless adoptEvidence)";
        }
    }
    EXPECT_FALSE(blob_touched_before_precommit);

    /// Sanity: the renamed part is visible under the new name and NOT under the old name.
    EXPECT_FALSE(storage->existsDirectory("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0"));
    EXPECT_TRUE(storage->existsDirectory("a11/a11a11a1-1111-4111-8111-111111111111/delete_tmp_all_1_1_0"));
    EXPECT_TRUE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/delete_tmp_all_1_1_0/data.bin"));
}

/// B190-B: the adoptStagedBlob helper unifies the 6 inline pending/uploaded adopt blocks from
/// createHardLink / moveFile / moveDirectory. The observable invariant: after refactoring, ALL
/// six sites still produce the same result as before — pending blobs are copied (hardlink) or
/// moved (moveFile/moveDirectory), and uploaded blobs are adopted by tokenless evidence. This test
/// exercises the non-trivial CROSS-PART pending path (createHardLink copies; moveFile moves) and
/// verifies both a copy and a move of the SAME pending source produce the correct committed state.
TEST(CASWiringPrecommitOrder, AdoptStagedBlobHelperUnifiesSixSites)
{
    /// Use a recording storage so we can verify no pre-precommit pool ops on own content.
    auto recording = makeRecordingStorageForTest("adopt_helper");
    auto settings = DB::Cas::tests::makeSettingsForTest(
        "test", std::filesystem::temp_directory_path() / "ca_b190_adopt_scratch");
    auto storage = std::make_shared<DB::ContentAddressedMetadataStorage>(
        recording, "pool", "srv1", "", nullptr, settings);
    storage->startup();

    recording->ops.clear();

    /// One transaction: write a pending blob into part A, hardlink (COPY pending) into part B,
    /// and moveFile (MOVE pending) of a DIFFERENT pending blob from part A into part C.
    auto tx = storage->createTransaction();
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_A_A_0/data.bin", "blob-for-copy");
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_A_A_0/extra.bin", "blob-for-move");

    /// createHardLink = COPY semantics: both src and dst should see the blob after commit.
    tx->createHardLink("a11/a11a11a1-1111-4111-8111-111111111111/all_A_A_0/data.bin", "a11/a11a11a1-1111-4111-8111-111111111111/all_B_B_0/data.bin");

    /// moveFile cross-part = MOVE semantics: src loses the blob, dst gains it.
    {
        auto & ca_tx = dynamic_cast<DB::ContentAddressedTransaction &>(*tx);
        ca_tx.moveFile("a11/a11a11a1-1111-4111-8111-111111111111/all_A_A_0/extra.bin", "a11/a11a11a1-1111-4111-8111-111111111111/all_C_C_0/extra.bin");
    }

    tx->commit(DB::NoCommitOptions{});

    const auto & log = recording->ops;
    const int first_precommit_idx = firstPrecommitWriteIdx(log);
    ASSERT_GE(first_precommit_idx, 0)
        << "No create-precommit owner write (root-shard CAS) was recorded — precommit step did not fire";

    /// Collect own content keys (the content BLOBS this transaction wrote). The staged part-manifest
    /// body (`/_manifests/...`) is the precommit's evidence and is written before precommit by design,
    /// so it is NOT gated content — only /blobs/ are.
    std::set<std::string> own_content_keys;
    for (const auto & r : log)
        if (r.op == "writeObject" && r.key.find("/blobs/") != std::string::npos)
            own_content_keys.insert(r.key);

    /// No own-content pool op before precommit (B188 invariant extends to all adopt sites).
    for (int i = 0; i < static_cast<int>(log.size()); ++i)
    {
        if (!own_content_keys.contains(log[i].key))
            continue;
        EXPECT_GT(i, first_precommit_idx)
            << "Own-content op '" << log[i].op << "' on '" << log[i].key << "' at index " << i
            << " before precommit at " << first_precommit_idx;
    }

    /// COPY semantics: both A and B see the copied blob.
    EXPECT_TRUE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/all_A_A_0/data.bin"));
    EXPECT_TRUE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/all_B_B_0/data.bin"));
    EXPECT_EQ(storage->getFileSize("a11/a11a11a1-1111-4111-8111-111111111111/all_A_A_0/data.bin"), 13u);   /// "blob-for-copy" (13 bytes)
    EXPECT_EQ(storage->getFileSize("a11/a11a11a1-1111-4111-8111-111111111111/all_B_B_0/data.bin"), 13u);
    EXPECT_EQ(storage->getStorageObjects("a11/a11a11a1-1111-4111-8111-111111111111/all_A_A_0/data.bin")[0].remote_path,
              storage->getStorageObjects("a11/a11a11a1-1111-4111-8111-111111111111/all_B_B_0/data.bin")[0].remote_path)
        << "COPY (hardlink): both parts must share the same blob object";

    /// MOVE semantics: A loses extra.bin, C gains it.
    EXPECT_FALSE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/all_A_A_0/extra.bin"));
    EXPECT_TRUE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/all_C_C_0/extra.bin"));
    EXPECT_EQ(storage->getFileSize("a11/a11a11a1-1111-4111-8111-111111111111/all_C_C_0/extra.bin"), 13u);   /// "blob-for-move" (13 bytes)
}

/// ==== B189: orphaned pending blob must NOT be uploaded after unlinkFile / replaceFile ====
///
/// When a file is written (pending blob X) and then unlinked (or replaced) within the same
/// transaction, X's tree entry is removed — so X is NOT referenced by the staged tree. Before the
/// B189 fix, publishStaging iterated pending_blobs unconditionally and uploaded X anyway (a wasted
/// PUT of an unreferenced blob). After the fix, publishStaging builds the set of blob hashes
/// referenced by the staged tree entries and uploads ONLY those — orphaned blobs are skipped.
///
/// The test uses RecordingLocalObjectStorage to capture every writeObject call. After commit it
/// checks that the orphaned blob's pool key received NO writeObject, while a kept blob (written and
/// NOT removed in the same transaction) IS uploaded.
TEST(CASWiringOps, OrphanedPendingBlobNotUploadedAfterUnlink)
{
    auto recording = makeRecordingStorageForTest("b189_unlink");
    auto settings = DB::Cas::tests::makeSettingsForTest(
        "test", std::filesystem::temp_directory_path() / "ca_b189_unlink_scratch");
    auto storage = std::make_shared<DB::ContentAddressedMetadataStorage>(
        recording, "pool", "srv1", "", nullptr, settings);
    storage->startup();

    recording->ops.clear();

    auto tx = storage->createTransaction();

    /// Write blob X — this will be unlinked (orphaned) before commit.
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/orphan.bin", "orphan-bytes");

    /// Write blob Y — this is kept (its tree entry survives to the staged tree).
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/kept.bin", "kept-bytes");

    /// Unlink blob X — removes its tree entry; the pending_blobs record remains but is now orphaned.
    tx->unlinkFile("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/orphan.bin", false, false);

    /// Sanity: the unlinked file is no longer staged (in-flight should not report it).
    EXPECT_FALSE(tx->tryGetInFlightFileSize("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/orphan.bin").has_value());
    EXPECT_EQ(tx->tryGetInFlightFileSize("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/kept.bin"), std::optional<uint64_t>(10));

    tx->commit(DB::NoCommitOptions{});

    const auto & log = recording->ops;

    /// Collect blob BODY keys written by this transaction (only /blobs/ writeObjects). Exclude the
    /// per-hash `.meta` freshness descriptor sibling (`blobMetaKey` = body key + `.meta`, spec
    /// §meta-protocols v3): it lives under the same /blobs/ prefix but is NOT a blob upload, so it must
    /// not inflate the body-upload count. `putBlob` writes exactly one such `.meta` per body.
    std::vector<std::string> blob_writes;
    for (const auto & r : log)
        if (r.op == "writeObject" && r.key.find("/blobs/") != std::string::npos && !r.key.ends_with(".meta"))
            blob_writes.push_back(r.key);

    /// Exactly ONE blob must have been uploaded (the kept one). The orphaned blob's pool key must
    /// NOT appear in any writeObject — B189: orphan is filtered out of the publish upload.
    EXPECT_EQ(blob_writes.size(), 1u)
        << "Expected exactly 1 blob upload (the kept blob); got " << blob_writes.size()
        << ". If 2, the orphaned pending blob was uploaded — B189 regression.";

    /// The kept file is visible after commit; the orphaned file is not.
    EXPECT_TRUE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/kept.bin"));
    EXPECT_EQ(storage->getFileSize("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/kept.bin"), 10u);   /// "kept-bytes"
    EXPECT_FALSE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/orphan.bin"));
}

/// B189 companion: the same orphan-filter applies when the tree entry is removed by replaceFile
/// (the destination entry erased before the move). Write blob X to dst, then replaceFile src->dst
/// (erases X's entry, moves src's entry to dst). The orphaned X must not be uploaded.
TEST(CASWiringOps, OrphanedPendingBlobNotUploadedAfterReplace)
{
    auto recording = makeRecordingStorageForTest("b189_replace");
    auto settings = DB::Cas::tests::makeSettingsForTest(
        "test", std::filesystem::temp_directory_path() / "ca_b189_replace_scratch");
    auto storage = std::make_shared<DB::ContentAddressedMetadataStorage>(
        recording, "pool", "srv1", "", nullptr, settings);
    storage->startup();

    recording->ops.clear();

    auto tx = storage->createTransaction();

    /// Write blob X into the destination slot — it will be erased by replaceFile.
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin", "original-bytes");

    /// Write blob Y into the source slot — it will replace the destination.
    writeThroughTransaction(*tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/new.bin", "replacement-bytes");

    /// replaceFile: erases the dst entry (X orphaned), then moves src->dst.
    {
        auto & ca_tx = dynamic_cast<DB::ContentAddressedTransaction &>(*tx);
        ca_tx.replaceFile("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/new.bin", "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin");
    }

    tx->commit(DB::NoCommitOptions{});

    const auto & log = recording->ops;

    /// Exactly ONE blob must have been uploaded (the replacement blob Y). Exclude the per-hash `.meta`
    /// freshness descriptor sibling (see the AfterUnlink test) — it is not a blob body upload.
    std::vector<std::string> blob_writes;
    for (const auto & r : log)
        if (r.op == "writeObject" && r.key.find("/blobs/") != std::string::npos && !r.key.ends_with(".meta"))
            blob_writes.push_back(r.key);

    EXPECT_EQ(blob_writes.size(), 1u)
        << "Expected exactly 1 blob upload (the replacement blob); got " << blob_writes.size()
        << ". If 2, the orphaned original blob was uploaded — B189 regression.";

    /// After commit the destination slot carries the replacement content.
    EXPECT_TRUE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin"));
    EXPECT_EQ(storage->getFileSize("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin"), 17u);   /// "replacement-bytes"
    EXPECT_FALSE(storage->existsFile("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/new.bin"));
}

/// ==== Promote tokened-leaf edge-protection (spec 2026-07-09-cas-writer-gc-simplification, Phase A) ====
///
/// A fast GC can PREMATURELY condemn a blob a writer just putBlob'd, in the tiny putBlob->promote window
/// (the precommit->blob edge is not yet folded, so GC reads in-degree 0). Under EDGE-BEFORE-OBSERVE the
/// precommit closure named the blob BEFORE putBlob observed it, so the condemnation cannot graduate to a
/// delete (the next fold sees the edge, d >= 1, spared) — it is doomed, not the blob. promote therefore
/// does NOT re-validate or resurrect a TOKENED leaf; it commits with the blob's token UNCHANGED. The only
/// blob-side abort promote still performs is the owner-liveness check (a reclaimed precommit) — which runs
/// BEFORE any blob work and touches nothing.
///
/// These tests drive the REAL writer sequence (stageManifest -> precommitAdd -> putBlob -> promote) against
/// a raw in-memory Pool (no background GC → deterministic), and condemn the blob's CURRENT token by seeding
/// gc/state + the per-hash freshness meta the way a real GC condemn does (see `seedCondemnBlobToken` below).

namespace DB::ErrorCodes
{
    extern const int ABORTED;
    extern const int NETWORK_ERROR;
}

namespace
{

DB::Cas::PoolPtr openResurrectStore(std::shared_ptr<DB::Cas::InMemoryBackend> & out_backend)
{
    out_backend = std::make_shared<DB::Cas::InMemoryBackend>();
    return DB::Cas::Pool::open(
        out_backend, DB::Cas::PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
}

/// Condemn (kind=Blob, hash, token) by seeding gc/state + a per-shard retired set (the durable GC ledger
/// shape — RetiredEntry, exact-token delete, unchanged by this task) AND condemning the per-hash freshness
/// meta, which is what the writer's condemned decision ACTUALLY point-reads (spec §meta-protocols v3).
/// Bumps the round so the retirement is a fresh one; leaves the object itself in place (condemn, NOT delete).
void seedCondemnBlobToken(DB::Cas::Pool & store, const DB::UInt128 & hash,
                          [[maybe_unused]] const DB::Cas::Token & token, [[maybe_unused]] uint64_t size)
{
    using namespace DB::Cas;
    Backend & b = store.backend();
    const Layout & layout = store.layout();

    GcState state;
    const HeadResult head = b.head(layout.gcStateKey());
    if (head.exists)
    {
        const auto got = b.get(layout.gcStateKey());
        state = decodeGcState(got->bytes);
    }
    state.round += 1;

    /// Retired-in-snapshot: there is no separate retired-list object to seed — condemned state rides the
    /// GC snapshot runs, which this writer-side edge-protection test does not exercise. The writer's
    /// condemned decision point-reads the per-hash freshness meta (condemned below), so bumping the round
    /// and condemning the meta is enough.
    if (head.exists)
        b.putOverwrite(layout.gcStateKey(), encodeGcState(state), head.token);
    else
        b.putIfAbsent(layout.gcStateKey(), encodeGcState(state));

    /// The writer's fresh upload (putBlob) already wrote a Clean meta for `hash` (Task 3), so this is a
    /// plain Clean -> Condemned CAS — exactly what GC's real condemn path does.
    DB::Cas::tests::condemnMeta(b, layout, hash, state.round);
}

}

/// A blob condemned in the putBlob->promote window is EDGE-PROTECTED (spec
/// 2026-07-09-cas-writer-gc-simplification, Phase A): the precommit closure naming the blob was durable
/// BEFORE putBlob observed it, so a condemnation in this window cannot graduate to a delete (the next fold
/// sees the edge, d >= 1, spared). promote therefore does NOT re-check or resurrect a TOKENED leaf — it
/// commits leaving the blob's token UNCHANGED (no resurrect PUT). The premature condemn is doomed on its own.
TEST(CASWiringResurrect, PromoteIgnoresCondemnedTokenedBlobEdgeProtected)
{
    using namespace DB::Cas;
    std::shared_ptr<InMemoryBackend> backend;
    auto store = openResurrectStore(backend);
    const RootNamespace ns{"test/tbl"};
    const String ref = "all_1_1_0";
    const String P = "resurrect-me";

    PartWriteInfo info;
    info.intended_ref = ns.string() + "/" + ref;
    auto build = store->beginPartWrite(info);

    const ManifestId id = build->stageManifest({wiringBlobEntry("data.bin", P)});
    build->precommitAdd(ns, ref, id);
    build->putBlob(idOf(P), BlobSource::fromString(P));

    /// Condemn the freshly-uploaded blob's CURRENT token (GC condemning the not-yet-folded fresh incarnation).
    const String blob_key = store->layout().blobKey(idOf(P));
    const HeadResult h1 = store->backend().head(blob_key);
    ASSERT_TRUE(h1.exists);
    const Token t0 = h1.token;
    seedCondemnBlobToken(*store, u128Of(P), t0, h1.size);
    {
        const auto lm = DB::Cas::tests::loadMetaForTest(store->backend(), store->layout(), u128Of(P));
        ASSERT_TRUE(lm.has_value() && lm->meta.state == MetaState::Condemned)
            << "precondition: the putBlob'd token must be condemned before promote";
    }

    /// promote must NOT abort AND must NOT touch the tokened leaf — it is edge-protected.
    EXPECT_NO_THROW(build->promote(ns, ref, build->buildId(), id));

    /// The ref is committed and the blob's token is UNCHANGED — no resurrect PUT ran (tokened leaves are
    /// not re-validated: EDGE-BEFORE-OBSERVE guarantees the condemnation is doomed, not the blob).
    EXPECT_TRUE(store->resolveRef(ns, ref).has_value()) << "the ref must resolve after promote";
    const HeadResult h2 = store->backend().head(blob_key);
    ASSERT_TRUE(h2.exists);
    EXPECT_EQ(h2.token, t0)
        << "tokened leaf is edge-protected: promote must not re-upload it (token unchanged)";
}

/// promote is a PURE owner MOVE (Δ=0 blob delta) — sound ONLY while this build's precommit is STILL the
/// live owner of the ref (`WPromote owner==bld` / INV_NO_DANGLE): a Δ=0 move over a ref with no live
/// precommit edge would republish a committed manifest onto to-be-deleted blobs. So when the precommit
/// binding is absent from the ref-table state, promote MUST fail closed with ABORTED — at the owner-liveness
/// check in the append closure, which runs BEFORE any blob revalidation, so NO consequential PUT / resurrect
/// happens (a condemned leaf is left untouched, exactly as on the success path).
///
/// This drives that guard the DETERMINISTIC way: a promote whose precommit was NEVER added (so the binding
/// is simply absent). The original "precommit added, then REMOVED out from under a still-live build" shape is
/// NOT reachable by any deterministic single-threaded in-runtime actor: `PartWriteTxn::abandon` marks the build
/// not-alive (`requireAlive` → LOGICAL_ERROR) and `Pool::dropNamespace` cancels the build (`requireAlive` →
/// ABORTED) — BOTH trip `requireAlive` at promote's first line, before this closure ever runs. Only a narrow
/// promote-vs-dropNamespace RACE (dropNamespace clears the binding in the window between promote's
/// `requireAlive` and its append closure) reaches the closure guard, which is therefore a defensive backstop
/// (a candidate for a later dead-code review — out of scope here). The previous version of this test faked
/// the removal with an out-of-band `appendOwnerEvent` the single-leader runtime never observes — an
/// unreachable state that surfaced as a CORRUPTED_DATA ref-log collision, not the intended ABORTED.
TEST(CASWiringResurrect, PromoteWithoutLivePrecommitAbortsWithoutResurrect)
{
    using namespace DB::Cas;
    std::shared_ptr<InMemoryBackend> backend;
    auto store = openResurrectStore(backend);
    const RootNamespace ns{"test/tbl"};
    const String ref = "all_2_2_0";
    const String P = "abandoned-me";

    PartWriteInfo info;
    info.intended_ref = ns.string() + "/" + ref;
    auto build = store->beginPartWrite(info);

    /// Stage the manifest and upload the (fresh) blob, but DO NOT precommitAdd — so the precommit owner
    /// binding is absent from the ref-table state when promote runs. A fresh putBlob needs no precommit
    /// (only the ADOPT path requires the durable edge); it records the tokened leaf t0.
    const ManifestId id = build->stageManifest({wiringBlobEntry("data.bin", P)});
    build->putBlob(idOf(P), BlobSource::fromString(P));

    const String blob_key = store->layout().blobKey(idOf(P));
    const HeadResult h1 = store->backend().head(blob_key);
    ASSERT_TRUE(h1.exists);
    /// Condemn the leaf so that, WERE the blob gate reached, promote would resurrect it — proving the abort
    /// happens strictly BEFORE any blob work.
    seedCondemnBlobToken(*store, u128Of(P), h1.token, h1.size);
    {
        const auto lm = DB::Cas::tests::loadMetaForTest(store->backend(), store->layout(), u128Of(P));
        ASSERT_TRUE(lm.has_value() && lm->meta.state == MetaState::Condemned);
    }

    /// promote aborts at the owner-liveness check (NETWORK_ERROR, fix #37 phase 2), before the blob gate.
    try
    {
        build->promote(ns, ref, build->buildId(), id);
        FAIL() << "expected promote to abort: the precommit is not the live owner of the ref";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::NETWORK_ERROR);
    }

    /// No blob work ran before the abort: the leaf's token is UNCHANGED (still the condemned one) and its
    /// meta is still Condemned — the owner check aborts before any PUT / resurrect.
    const HeadResult h2 = store->backend().head(blob_key);
    ASSERT_TRUE(h2.exists);
    EXPECT_EQ(h2.token, h1.token)
        << "the aborting path must perform no PUT — the tokened leaf is untouched";
    const auto lm_after = DB::Cas::tests::loadMetaForTest(store->backend(), store->layout(), u128Of(P));
    EXPECT_TRUE(lm_after.has_value() && lm_after->meta.state == MetaState::Condemned)
        << "no re-upload/resurrect before the owner check — the token is still the condemned one";
}

/// tryFromDisk must be exception-free for a plain local disk: it runs on every
/// asynchronous-metrics tick for every configured disk, and probing via
/// `getMetadataStorage`'s NOT_IMPLEMENTED throw pollutes `system.errors` (the Exception
/// constructor counts the error even when the throw is caught) — a steady +N/s stream on a
/// pure-local server, caught as a stray-error failure by strict-error tests
/// (`test_cancel_backup`'s NoTrashChecker, Altinity PR#2073).
TEST(CASWiring, TryFromDiskOnLocalDiskIsExceptionFreeAndCountsNoError)
{
    auto tmp = std::filesystem::temp_directory_path() / "ca_wiring_tryfromdisk_test";
    std::filesystem::create_directories(tmp);
    const DB::DiskPtr local = std::make_shared<DB::DiskLocal>("tryfromdisk_local", tmp.string());

    const auto before = DB::ErrorCodes::values[DB::ErrorCodes::NOT_IMPLEMENTED].get().local.count;
    auto * ca = DB::ContentAddressedMetadataStorage::tryFromDisk(local);
    const auto after = DB::ErrorCodes::values[DB::ErrorCodes::NOT_IMPLEMENTED].get().local.count;

    EXPECT_EQ(ca, nullptr);
    EXPECT_EQ(after, before)
        << "tryFromDisk on a non-content-addressed disk must not construct (and thereby count) "
           "a NOT_IMPLEMENTED exception — it runs per disk on every asynchronous-metrics tick";
    std::filesystem::remove_all(tmp);
}
