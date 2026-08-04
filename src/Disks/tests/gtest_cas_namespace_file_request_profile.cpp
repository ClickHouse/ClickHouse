#include <gtest/gtest.h>
#include "cas_test_helpers.h"

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <IO/WriteHelpers.h>
#include <Core/Defines.h>

/// Per-TU declaration of the one setting this file overrides, following the pattern `cas_test_helpers.h`
/// documents for `server_root_id`/`scratch_path`: defined once in `ContentAddressedSettings.cpp`, declared
/// by each consumer for what it actually references.
namespace DB::ContentAddressedSetting
{
    extern const ContentAddressedSettingsBool gc_enabled;
}

#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

/// The namespace-file REQUEST PROFILE gate (directive §dedup-performance-constraint) -- for the
/// `Pool` namespace-file surface, which is the layer that must be read carefully below.
///
/// `MergeTreeDeduplicationLog` rotates namespace files on the insert path because the CA disk cannot
/// append, so every namespace-file operation's request profile is insert latency. The directive's
/// constraint has five clauses: no catalog request per file operation, no ref-log append, no blob
/// upload, no folder-manifest rewrite, and unchanged direct-object backend request counts.
///
/// WHAT THIS FILE PINS: the last clause, per key. The counts below were READ OFF this tree before any
/// key change and pasted as literals, which is the whole point of the file -- expectations re-derived
/// after a change measure the change against itself. Incarnation qualification changes the KEY a
/// namespace file is stored under, so the keys are derived from `Layout` rather than spelled out; what
/// must not move is the count per key and the set of keys touched.
///
/// TWO KINDS OF GATE LIVE IN THIS FILE, and mistaking one for the other would misread what it proves.
/// The per-operation COUNTS above are baseline-anchored: they were read off the tree BEFORE the key change
/// and pasted as literals, so they detect DRIFT from a measured past. The four negatives below are a
/// FORWARD ALARM: a zero has no baseline to drift from, and the disk-layer cases could not have existed
/// before the life resolution they measure was put on that path. They are no weaker for it -- they fail
/// the moment a catalog, ref-log, blob or manifest request appears where none belongs -- but they are not
/// evidence that anything was "unchanged".
///
/// WHERE THE OTHER FOUR CLAUSES ARE FENCED, and why they could not be fenced by the cases above. Every
/// pool-layer case drives `Pool::putNamespaceFile`/`getNamespaceFile`/`removeNamespaceFile`/
/// `listNamespaceFiles`, which reach `CasPlainObjects` and have no catalog, ref-log, blob or manifest
/// path to take -- so at THAT layer the four negatives hold by construction of the call and measuring
/// them proves nothing. The layer where they can be violated is the DISK operation above, where
/// `ContentAddressedTransaction::writeFile` resolves the namespace's life; that is exactly where Task 4b
/// put a life resolution, so that is where a per-operation catalog GET would appear. The two
/// `CASNamespaceFileDiskProfile` cases at the bottom of this file fence it there, through a recording
/// `IObjectStorage` (the metadata storage builds its own `Backend` from an `ObjectStoragePtr`, so the
/// object storage is the injectable seam -- no production surface is widened for the test's benefit).

using namespace DB::Cas;
using namespace DB::Cas::tests;

namespace
{

const String kNsString = "test/req_profile@cas@";
const String kFile = "format_version.txt";
/// A NESTED relative name, which is what the dedup log actually stores (its segments live in a
/// table-level subdirectory), so the profile is captured on the shape the constraint is about.
const String kSegment1 = "deduplication_logs/deduplication_log_1.txt";
const String kSegment2 = "deduplication_logs/deduplication_log_2.txt";

/// The identity every case below operates under. `fixture::fixtureLife` is the transitional mint Task 6
/// deletes; what matters to this file is only that ONE life is used throughout, so a count is not
/// split across two prefixes.
NamespaceLifeId testLife()
{
    return fixture::fixtureLife(RootNamespace{kNsString});
}

/// A pool over `CountingBackend`, with the counts reset AFTER open: `Pool::open` runs its own
/// capability probe and mount claim, and those requests belong to no file operation.
PoolPtr openCountedPool(std::shared_ptr<CountingBackend> & out_backend)
{
    out_backend = std::make_shared<CountingBackend>();
    PoolPtr store = openPoolForTest(out_backend);
    out_backend->resetCounts();
    return store;
}

}

/// CREATE (the key is absent) and REWRITE (the key is present) are different request shapes on the
/// same call, and the profile pins both: one HEAD to learn the token, then the create-if-absent or the
/// token-conditioned replacement that HEAD selected.
TEST(CASNamespaceFileRequestProfile, CreateThenRewrite)
{
    std::shared_ptr<CountingBackend> backend;
    PoolPtr store = openCountedPool(backend);
    const NamespaceLifeId life = testLife();
    const String key = store->layout().namespaceFileKey(life, kFile);

    store->putNamespaceFile(life, kFile, "1\n");

    EXPECT_EQ(backend->headCount(key), 1u);
    EXPECT_EQ(backend->putCount(key), 1u);            /// putIfAbsent -- the key was absent
    EXPECT_EQ(backend->putOverwriteCount(key), 0u);
    EXPECT_EQ(backend->getCount(key), 0u);
    EXPECT_EQ(backend->deleteCount(key), 0u);
    EXPECT_EQ(backend->listTotal(), 0u);
    EXPECT_EQ(backend->casPutTotal(), 0u);
    EXPECT_EQ(backend->touchedKeys(), std::vector<String>{key});

    backend->resetCounts();
    store->putNamespaceFile(life, kFile, "2\n");

    EXPECT_EQ(backend->headCount(key), 1u);
    EXPECT_EQ(backend->putOverwriteCount(key), 1u);   /// token-conditioned replacement -- it existed
    EXPECT_EQ(backend->putCount(key), 0u);
    EXPECT_EQ(backend->getCount(key), 0u);
    EXPECT_EQ(backend->deleteCount(key), 0u);
    EXPECT_EQ(backend->listTotal(), 0u);
    EXPECT_EQ(backend->casPutTotal(), 0u);
    EXPECT_EQ(backend->touchedKeys(), std::vector<String>{key});
}

/// A plain read is one whole-object GET and nothing else.
TEST(CASNamespaceFileRequestProfile, Read)
{
    std::shared_ptr<CountingBackend> backend;
    PoolPtr store = openCountedPool(backend);
    const NamespaceLifeId life = testLife();
    const String key = store->layout().namespaceFileKey(life, kFile);

    store->putNamespaceFile(life, kFile, "1\n");
    backend->resetCounts();

    EXPECT_EQ(store->getNamespaceFile(life, kFile), String("1\n"));

    EXPECT_EQ(backend->getCount(key), 1u);
    EXPECT_EQ(backend->wholeGetCount(key), 1u);
    EXPECT_EQ(backend->headCount(key), 0u);
    EXPECT_EQ(backend->putCount(key), 0u);
    EXPECT_EQ(backend->putOverwriteCount(key), 0u);
    EXPECT_EQ(backend->touchedKeys(), std::vector<String>{key});
}

/// APPEND on a CA disk is serviced by read-modify-rewrite, and its request shape is the composition of
/// the two calls that implement it: a GET of the current body, then a whole-body PUT of base+delta.
/// Driven here as that composition against the same key, which is the shape whose count must not move.
TEST(CASNamespaceFileRequestProfile, ReadModifyRewriteAppend)
{
    std::shared_ptr<CountingBackend> backend;
    PoolPtr store = openCountedPool(backend);
    const NamespaceLifeId life = testLife();
    const String key = store->layout().namespaceFileKey(life, kSegment1);

    store->putNamespaceFile(life, kSegment1, "base");
    backend->resetCounts();

    const std::optional<String> carried = store->getNamespaceFile(life, kSegment1);
    ASSERT_TRUE(carried.has_value());
    store->putNamespaceFile(life, kSegment1, *carried + "-delta");

    EXPECT_EQ(backend->getCount(key), 1u);
    EXPECT_EQ(backend->headCount(key), 1u);
    EXPECT_EQ(backend->putOverwriteCount(key), 1u);
    EXPECT_EQ(backend->putCount(key), 0u);
    EXPECT_EQ(backend->deleteCount(key), 0u);
    EXPECT_EQ(backend->listTotal(), 0u);
    EXPECT_EQ(backend->touchedKeys(), std::vector<String>{key});
    EXPECT_EQ(store->getNamespaceFile(life, kSegment1), String("base-delta"));
}

/// REMOVE is exact-token deletion, so it is one HEAD for the token plus one delete against it.
TEST(CASNamespaceFileRequestProfile, Remove)
{
    std::shared_ptr<CountingBackend> backend;
    PoolPtr store = openCountedPool(backend);
    const NamespaceLifeId life = testLife();
    const String key = store->layout().namespaceFileKey(life, kFile);

    store->putNamespaceFile(life, kFile, "1\n");
    backend->resetCounts();

    store->removeNamespaceFile(life, kFile);

    EXPECT_EQ(backend->headCount(key), 1u);
    EXPECT_EQ(backend->deleteCount(key), 1u);
    EXPECT_EQ(backend->getCount(key), 0u);
    EXPECT_EQ(backend->putCount(key), 0u);
    EXPECT_EQ(backend->putOverwriteCount(key), 0u);
    EXPECT_EQ(backend->listTotal(), 0u);
    EXPECT_EQ(backend->touchedKeys(), std::vector<String>{key});
    EXPECT_FALSE(store->getNamespaceFile(life, kFile).has_value());
}

/// ROTATION is the sequence the constraint names: the retiring segment is enumerated, the new segment
/// is created, and the retired one is removed. One LIST of the files prefix serves the enumeration (a
/// single page here), and each segment carries its own create or remove shape.
TEST(CASNamespaceFileRequestProfile, DedupLogRotation)
{
    std::shared_ptr<CountingBackend> backend;
    PoolPtr store = openCountedPool(backend);
    const NamespaceLifeId life = testLife();
    const String prefix = store->layout().namespaceFilesPrefix(life);
    const String old_key = store->layout().namespaceFileKey(life, kSegment1);
    const String new_key = store->layout().namespaceFileKey(life, kSegment2);

    store->putNamespaceFile(life, kSegment1, "segment-1-records");
    backend->resetCounts();

    const std::vector<String> before = store->listNamespaceFiles(life);
    ASSERT_EQ(before, std::vector<String>{kSegment1});
    store->putNamespaceFile(life, kSegment2, "segment-2-records");
    store->removeNamespaceFile(life, kSegment1);

    EXPECT_EQ(backend->listCount(prefix), 1u);
    EXPECT_EQ(backend->listTotal(), 1u);
    EXPECT_EQ(backend->headCount(new_key), 1u);
    EXPECT_EQ(backend->putCount(new_key), 1u);
    EXPECT_EQ(backend->putOverwriteCount(new_key), 0u);
    EXPECT_EQ(backend->headCount(old_key), 1u);
    EXPECT_EQ(backend->deleteCount(old_key), 1u);
    EXPECT_EQ(backend->getTotal(), 0u);              /// rotation reads no body
    EXPECT_EQ(backend->casPutTotal(), 0u);
    /// Sorted, and the files prefix is a proper prefix of both segment keys, so it comes first.
    EXPECT_EQ(backend->touchedKeys(), (std::vector<String>{prefix, old_key, new_key}));

    EXPECT_EQ(store->listNamespaceFiles(life), std::vector<String>{kSegment2});
}


/// ===================== THE FOUR NEGATIVES, AT THE DISK LAYER =====================
///
/// Constraint 16's other four clauses: a namespace-file operation performs no catalog request, no
/// ref-log append, no blob upload and no folder-manifest rewrite. They are asserted here rather than
/// above because only here is there a life resolution to get wrong.
///
/// WHAT MAKES THE CLAIM NON-TRIVIAL. Task 4b's read and write paths resolve a catalog-minted life. That
/// resolution is per TABLE-OPEN -- `CasRefLedger` caches it on the table's runtime -- so the steady-state
/// operation pays nothing for it. If it ever became per-operation, `format_version.txt` and every
/// dedup-log rotation on the insert path would carry a catalog round trip, and nothing else in the suite
/// would notice. `SteadyStateFileOperationsTouchNoCatalogRefBlobOrManifestKey` is that alarm, and
/// `TheLifeResolutionIsPaidOncePerTableOpen` is the other half: it shows the birth cost EXISTS and is
/// paid exactly once, so the steady-state zeros are a real property rather than an artifact of a fixture
/// that never triggered a resolution at all.

namespace
{

/// A `LocalObjectStorage` that records every key it is asked about, per operation family. Used to ask
/// "was any key under these four families touched at all", which is a question about WHICH keys an
/// operation reaches -- not about counts -- so recording the key set is the whole instrument.
///
/// It overrides every method `CasObjectStorageBackend` reaches: a family left un-overridden would be an
/// unrecorded path, and an assertion of "nothing touched it" would then be silently satisfied by the
/// gap rather than by the behaviour.
class RecordingObjectStorage : public DB::LocalObjectStorage
{
public:
    using DB::LocalObjectStorage::LocalObjectStorage;

    bool exists(const DB::StoredObject & object) const override
    {
        record(object.remote_path, /*is_write*/ false);
        return DB::LocalObjectStorage::exists(object);
    }

    std::unique_ptr<DB::ReadBufferFromFileBase> readObject(
        const DB::StoredObject & object, const DB::ReadSettings & read_settings,
        std::optional<size_t> read_hint, bool use_external_buffer,
        bool restrict_seek) const override
    {
        record(object.remote_path, /*is_write*/ false);
        return DB::LocalObjectStorage::readObject(object, read_settings, read_hint, use_external_buffer, restrict_seek);
    }

    std::unique_ptr<DB::WriteBufferFromFileBase> writeObject(
        const DB::StoredObject & object, DB::WriteMode mode,
        std::optional<DB::ObjectAttributes> attributes,
        size_t buf_size,
        const DB::WriteSettings & write_settings) override
    {
        record(object.remote_path, /*is_write*/ true);
        return DB::LocalObjectStorage::writeObject(object, mode, attributes, buf_size, write_settings);
    }

    void removeObjectIfExists(const DB::StoredObject & object) override
    {
        record(object.remote_path, /*is_write*/ true);
        DB::LocalObjectStorage::removeObjectIfExists(object);
    }

    void removeObjectsIfExist(const DB::StoredObjects & objects) override
    {
        for (const DB::StoredObject & object : objects)
            record(object.remote_path, /*is_write*/ true);
        DB::LocalObjectStorage::removeObjectsIfExist(objects);
    }

    DB::ObjectMetadata getObjectMetadata(const std::string & path, bool with_tags) const override
    {
        record(path, /*is_write*/ false);
        return DB::LocalObjectStorage::getObjectMetadata(path, with_tags);
    }

    std::optional<DB::ObjectMetadata> tryGetObjectMetadata(const std::string & path, bool with_tags) const override
    {
        record(path, /*is_write*/ false);
        return DB::LocalObjectStorage::tryGetObjectMetadata(path, with_tags);
    }

    void listObjects(const std::string & path, DB::RelativePathsWithMetadata & children, size_t max_keys) const override
    {
        record(path, /*is_write*/ false);
        DB::LocalObjectStorage::listObjects(path, children, max_keys);
    }

    bool existsOrHasAnyChild(const std::string & path) const override
    {
        record(path, /*is_write*/ false);
        return DB::LocalObjectStorage::existsOrHasAnyChild(path);
    }

    void copyObject(
        const DB::StoredObject & object_from, const DB::StoredObject & object_to,
        const DB::ReadSettings & read_settings, const DB::WriteSettings & write_settings,
        std::optional<DB::ObjectAttributes> object_to_attributes) override
    {
        record(object_from.remote_path, /*is_write*/ false);
        record(object_to.remote_path, /*is_write*/ true);
        DB::LocalObjectStorage::copyObject(object_from, object_to, read_settings, write_settings, object_to_attributes);
    }

    /// Every recorded key containing `needle`, in first-touch order, so a failure names the offender.
    std::vector<String> touchedContaining(std::string_view needle) const
    {
        std::lock_guard lock(mutex);
        std::vector<String> out;
        for (const String & key : touched)
            if (key.find(needle) != String::npos)
                out.push_back(key);
        return out;
    }

    std::vector<String> writtenContaining(std::string_view needle) const
    {
        std::lock_guard lock(mutex);
        std::vector<String> out;
        for (const String & key : written)
            if (key.find(needle) != String::npos)
                out.push_back(key);
        return out;
    }

    void resetRecords()
    {
        std::lock_guard lock(mutex);
        touched.clear();
        written.clear();
    }

private:
    void record(const std::string & key, bool is_write) const
    {
        std::lock_guard lock(mutex);
        touched.push_back(key);
        if (is_write)
            written.push_back(key);
    }

    mutable std::mutex mutex;
    mutable std::vector<String> touched;
    mutable std::vector<String> written;
};

const std::string kTableUuid = "a11a11a1-1111-4111-8111-111111111111";
const std::string kTablePath = "a11/a11a11a1-1111-4111-8111-111111111111";

/// The four families the constraint forbids a file operation from touching, as key substrings. Taken
/// from `Layout` where a helper exists rather than spelled out, so a layout change breaks this by
/// failing to compile or by moving the substring, not by silently matching nothing.
struct ForbiddenFamily
{
    String needle;
    String clause;
};

std::vector<ForbiddenFamily> forbiddenFamilies(const DB::Cas::Layout & layout)
{
    return {
        {layout.refCatalogKey(), "no catalog request"},
        {layout.casRefsPrefix(), "no ref-log append"},
        {layout.blobsPrefix(), "no blob upload"},
        {layout.casManifestsPrefix(), "no folder-manifest rewrite"},
    };
}

std::shared_ptr<DB::ContentAddressedMetadataStorage> openRecordingStorage(
    std::shared_ptr<RecordingObjectStorage> & out_object_storage)
{
    static std::atomic<uint64_t> counter{0};
    const String unique = std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1));
    const auto root = (std::filesystem::temp_directory_path() / ("cas_ns_file_profile_" + unique)).string();
    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    std::filesystem::create_directories(root, ec);

    out_object_storage = std::make_shared<RecordingObjectStorage>(
        DB::LocalObjectStorageSettings("test", root, /*read_only_=*/false));

    auto settings = makeSettingsForTest(
        "test", std::filesystem::temp_directory_path() / ("cas_ns_file_profile_scratch_" + unique));
    /// A GC round touches `cas/ref_catalog`, `cas/ns/stream/` and `cas/manifests/`, so with the
    /// background scheduler enabled these zeros would hold only because the first tick (60s) outlives the
    /// test. A timer is not a fence.
    settings[DB::ContentAddressedSetting::gc_enabled] = false;
    auto storage = std::make_shared<DB::ContentAddressedMetadataStorage>(
        out_object_storage, "pool", "srv1", "", nullptr, settings);
    storage->startup();
    return storage;
}

/// One verbatim namespace file written through the REAL disk write path (the buffer whose finalize
/// callback reaches `putNamespaceFile`), not through the pool surface.
void writeVerbatimThroughDisk(   /// ASSERT_* inside -> must return void
    DB::ContentAddressedMetadataStorage & storage, const std::string & path, const String & bytes,
    DB::WriteMode mode = DB::WriteMode::Rewrite)
{
    /// `tryCreateWriteBuffer` is the interface entry the disk itself uses, so this drives the same
    /// buffer construction (and the same autocommit-on-finalize contract for verbatim files) that a real
    /// write does. `owner` is null here: only a part-blob buffer's deferred finalize needs the pin, and
    /// a verbatim file finalizes inline, inside this call's scope.
    auto tx = storage.createTransaction();
    auto buf = tx->tryCreateWriteBuffer(
        /*owner*/ nullptr, path, DB::DBMS_DEFAULT_BUFFER_SIZE, mode, {}, /*autocommit*/ true);
    ASSERT_TRUE(buf != nullptr);
    DB::writeString(bytes, *buf);
    buf->finalize();
}

}

/// The steady state: with the table open and its life already resolved, no namespace-file operation --
/// rewrite, append, read, rotation, remove -- touches a catalog, ref, blob or manifest key.
TEST(CASNamespaceFileDiskProfile, SteadyStateFileOperationsTouchNoCatalogRefBlobOrManifestKey)
{
    std::shared_ptr<RecordingObjectStorage> object_storage;
    auto storage = openRecordingStorage(object_storage);
    const DB::Cas::Layout & layout = storage->store()->layout();

    /// Open the table by doing the first file operation, which is what resolves (and here mints) the
    /// life. Everything measured below happens after it.
    writeVerbatimThroughDisk(*storage, kTablePath + "/format_version.txt", "1\n");
    ASSERT_TRUE(storage->existsFile(kTablePath + "/format_version.txt"));

    object_storage->resetRecords();

    /// A whole-file rewrite, the read-modify-rewrite append, a read, and a dedup-log rotation
    /// (create the new segment, enumerate, drop the retired one) -- the four shapes the constraint names.
    writeVerbatimThroughDisk(*storage, kTablePath + "/format_version.txt", "2\n");
    writeVerbatimThroughDisk(*storage, kTablePath + "/deduplication_logs/deduplication_log_1.txt", "a");
    writeVerbatimThroughDisk(
        *storage, kTablePath + "/deduplication_logs/deduplication_log_1.txt", "b", DB::WriteMode::Append);
    EXPECT_EQ(storage->tryGetInManifestBytes(kTablePath + "/deduplication_logs/deduplication_log_1.txt"),
              std::optional<String>("ab"));
    writeVerbatimThroughDisk(*storage, kTablePath + "/deduplication_logs/deduplication_log_2.txt", "c");
    storage->createTransaction()->unlinkFile(
        kTablePath + "/deduplication_logs/deduplication_log_1.txt", /*if_exists*/ false, /*remove_metadata_only*/ false);

    for (const ForbiddenFamily & family : forbiddenFamilies(layout))
        EXPECT_EQ(object_storage->touchedContaining(family.needle), std::vector<String>{})
            << "Constraint 16, '" << family.clause << "': a namespace-file operation reached " << family.needle;

    /// A positive control on the instrument itself: the operations above DID reach the store, so the
    /// four empty answers are the absence of those families and not a recorder that recorded nothing.
    EXPECT_FALSE(object_storage->writtenContaining("/_files/").empty())
        << "the recorder must have seen the file writes themselves";
}

/// The other half: the life resolution is real and is paid ONCE per table-open. Without this, the zeros
/// above could be produced by a fixture in which no resolution ever happened.
TEST(CASNamespaceFileDiskProfile, TheLifeResolutionIsPaidOncePerTableOpen)
{
    std::shared_ptr<RecordingObjectStorage> object_storage;
    auto storage = openRecordingStorage(object_storage);
    const DB::Cas::Layout & layout = storage->store()->layout();

    /// The FIRST namespace-file operation on a never-opened table resolves the life from the catalog,
    /// minting the namespace when it names none -- so it DOES reach the catalog. That is the per-open
    /// cost, and the reason the steady-state case above resets its records after this point.
    writeVerbatimThroughDisk(*storage, kTablePath + "/format_version.txt", "1\n");
    EXPECT_FALSE(object_storage->touchedContaining(layout.refCatalogKey()).empty())
        << "the first file operation must resolve a life, which reaches the catalog";

    object_storage->resetRecords();

    /// The second operation on the SAME open table resolves nothing: the life is cached on the table's
    /// runtime. This is the assertion that says "per table-open", and it is the one that would fail if a
    /// future change moved the resolution onto the operation.
    writeVerbatimThroughDisk(*storage, kTablePath + "/format_version.txt", "2\n");
    EXPECT_EQ(object_storage->touchedContaining(layout.refCatalogKey()), std::vector<String>{})
        << "a second file operation must not re-resolve the life";
}


/// THE REMOVAL PATHS MUST NOT CREATE A NAMESPACE — the case that regressed silently in this task's first
/// round, so it is pinned on the catalog rather than on the file outcome.
///
/// Why the file outcome cannot pin it: `unlinkFile`/`removeRecursive` against a never-opened table
/// answer "absent" both before and after the defect, because a freshly minted namespace has no files
/// either. The only observable difference is the catalog write, so that is what is asserted. And it
/// matters twice over: `unlinkFile(..., if_exists = true)` is called from cleanup paths whose contract is
/// to be a no-op, and the catalog is ONE pool-wide object under a capacity-admission predicate — a
/// removal that admits an entry per never-created table grows it without bound.
TEST(CASNamespaceFileDiskProfile, RemovalOnANeverOpenedTableLeavesTheCatalogUntouched)
{
    std::shared_ptr<RecordingObjectStorage> object_storage;
    auto storage = openRecordingStorage(object_storage);
    const DB::Cas::Layout & layout = storage->store()->layout();

    /// A valid pool already owns its explicit empty mandatory catalog. Nothing has opened this table:
    /// no namespace file written, no part published, and no ref operation has changed that object.
    const auto catalog_before = storage->store()->backend().get(layout.refCatalogKey());
    ASSERT_TRUE(catalog_before);
    EXPECT_TRUE(decodeRefCatalog(catalog_before->bytes).entries.empty());
    object_storage->resetRecords();

    /// Three removal shapes, all against paths under a table that does not exist.
    storage->createTransaction()->unlinkFile(
        kTablePath + "/format_version.txt", /*if_exists*/ true, /*remove_metadata_only*/ false);
    storage->createTransaction()->removeRecursive(
        kTablePath + "/deduplication_logs", DB::IMetadataTransaction::ShouldRemoveObjectsPredicate{});
    /// The table directory ITSELF, which is a different arm from the subdirectory above: it is the one
    /// that reaches the ref layer's namespace drop and its ref enumeration, rather than only the
    /// namespace-file resolver.
    storage->createTransaction()->removeRecursive(
        kTablePath, DB::IMetadataTransaction::ShouldRemoveObjectsPredicate{});
    EXPECT_FALSE(storage->existsFile(kTablePath + "/format_version.txt"));

    EXPECT_EQ(object_storage->writtenContaining(layout.refCatalogKey()), std::vector<String>{})
        << "a removal must not write the catalog: it must not birth the namespace it is removing from";
    const auto catalog_after_removal = storage->store()->backend().get(layout.refCatalogKey());
    ASSERT_TRUE(catalog_after_removal);
    EXPECT_EQ(catalog_after_removal->bytes, catalog_before->bytes);
    EXPECT_EQ(catalog_after_removal->token, catalog_before->token)
        << "the mandatory catalog must remain byte-for-byte and token-for-token unchanged";

    /// Not vacuous: the SAME operations on the same table after a write do reach the file, so the zeros
    /// above are the absence of a birth and not the absence of any work.
    writeVerbatimThroughDisk(*storage, kTablePath + "/format_version.txt", "1\n");
    ASSERT_TRUE(storage->existsFile(kTablePath + "/format_version.txt"));
    storage->createTransaction()->unlinkFile(
        kTablePath + "/format_version.txt", /*if_exists*/ false, /*remove_metadata_only*/ false);
    EXPECT_FALSE(storage->existsFile(kTablePath + "/format_version.txt"));
    /// Positive control: the write really did birth the namespace and mutate the same catalog object
    /// whose stability the removal assertions pin above.
    const auto catalog_after_birth = storage->store()->backend().get(layout.refCatalogKey());
    ASSERT_TRUE(catalog_after_birth);
    EXPECT_NE(catalog_after_birth->bytes, catalog_after_removal->bytes);
    EXPECT_NE(catalog_after_birth->token, catalog_after_removal->token);
}
