#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCatalogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCatalog.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Core/Defines.h>
#include <IO/WriteHelpers.h>

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <memory>
#include <optional>
#include <unistd.h>

namespace DB::ContentAddressedSetting
{
    extern const ContentAddressedSettingsBool gc_enabled;
}

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NETWORK_ERROR;
}

using namespace DB::Cas;
using namespace DB::Cas::tests;

namespace
{

const String kTableUuid = "a11a11a1-1111-4111-8111-111111111111";
const String kTablePath = "a11/a11a11a1-1111-4111-8111-111111111111";
const String kFile = "format_version.txt";
const String kFilePath = kTablePath + "/" + kFile;
const UInt128 kLife2Id = hexToU128("22222222222222222222222222222222");

struct DiskFixture
{
    DB::ObjectStoragePtr object_storage;
    std::shared_ptr<DB::ContentAddressedMetadataStorage> storage;
};

DiskFixture openDiskFixture()
{
    static std::atomic<uint64_t> counter{0};
    const String unique = std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1));
    auto settings = makeSettingsForTest(
        "srv1", std::filesystem::temp_directory_path() / ("cas_ns_file_contract_scratch_" + unique));
    settings[DB::ContentAddressedSetting::gc_enabled] = false;

    DiskFixture fixture;
    fixture.object_storage = makeLocalObjectStorageForTest();
    fixture.storage = std::make_shared<DB::ContentAddressedMetadataStorage>(
        fixture.object_storage, "pool", "srv1", "", nullptr, settings);
    fixture.storage->startup();
    return fixture;
}

void writeVerbatimThroughDisk(
    DB::ContentAddressedMetadataStorage & storage, const String & path, const String & bytes)
{
    auto transaction = storage.createTransaction();
    auto & ca_transaction = dynamic_cast<DB::ContentAddressedTransaction &>(*transaction);
    auto buffer = ca_transaction.writeFile(path, DB::DBMS_DEFAULT_BUFFER_SIZE, DB::WriteMode::Rewrite, {});
    ASSERT_NE(dynamic_cast<CaInlineWriteBuffer *>(buffer.get()), nullptr);
    DB::writeString(bytes, *buffer);
    buffer->finalize();
}

/// Delete the current catalog life through the production exact-removal authority while retaining all
/// old physical bytes and the original process's already-resident runtime.
void deleteCatalogLife(
    DB::ContentAddressedMetadataStorage & storage, const NamespaceLifeId & life1)
{
    Backend & backend = storage.store()->backend();
    const Layout & layout = storage.store()->layout();
    CasRefCatalog::casUpdate(backend, layout, [&](const RefCatalog & current)
    {
        RefCatalog next = current;
        const auto it = std::find_if(next.entries.begin(), next.entries.end(), [&](const CatalogEntry & entry)
        {
            return entry.ns == life1.ns && entry.incarnation == life1.incarnation;
        });
        if (it == next.entries.end())
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR, "Missing fixture catalog life '{}'", life1.ns.string());
        it->state = NsState::Removing;
        it->removal_started_round = 1;
        return next;
    });

    const CasRefCatalog::Snapshot snapshot = CasRefCatalog::read(backend, layout);
    const auto it = std::find_if(snapshot.catalog.entries.begin(), snapshot.catalog.entries.end(), [&](const CatalogEntry & entry)
    {
        return entry.ns == life1.ns && entry.incarnation == life1.incarnation;
    });
    if (it == snapshot.catalog.entries.end())
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR, "Missing Removing fixture catalog life '{}'", life1.ns.string());

    CasFoldSeal parent;
    parent.ref_lives.emplace(life1.incarnation, RefLifeFoldState{
        .coverage = RefCoverage{.classification = 2, .last_folded_ref_id = RefTxnId{1, 1}},
        .cleanup_evidence = RefCleanupEvidence{.remove_txn_id = RefTxnId{1, 1}}});
    if (CasRefCatalog::deleteCompletedRemoving(
            backend, layout, *it, parent, 1,
            [](uint64_t) { return CasRefCatalog::LeaderFenceStatus::Held; })
        != CasRefCatalog::CompletedRemovingDeleteOutcome::Deleted)
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR, "Failed to delete fixture catalog life '{}'", life1.ns.string());

}

NamespaceLifeId admitReplacementLife(
    DB::ContentAddressedMetadataStorage & storage, const NamespaceLifeId & life1)
{
    if (life1.incarnation == kLife2Id)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Fixture life ids unexpectedly collide");
    const NamespaceLifeId life2 = NamespaceLifeId::fromCatalogEntry(life1.ns, kLife2Id);
    CasRefCatalog::casAdmitEntry(
        storage.store()->backend(), storage.store()->layout(), storage.store()->poolConfig().gc_shards, CatalogEntry{
        .ns = life2.ns, .state = NsState::Live, .incarnation = life2.incarnation});
    return life2;
}

NamespaceLifeId replaceCatalogLife(
    DB::ContentAddressedMetadataStorage & storage, const NamespaceLifeId & life1)
{
    deleteCatalogLife(storage, life1);
    return admitReplacementLife(storage, life1);
}

NamespaceLifeId currentLife(DB::ContentAddressedMetadataStorage & storage)
{
    const RootNamespace ns = storage.liveNamespace(kTableUuid);
    const auto life = storage.store()->namespaceFilesLifeIfReadable(ns);
    if (!life)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Fixture namespace '{}' has no readable life", ns.string());
    return *life;
}

}

/// This storage already holds life 1. Reusing its warm runtime after same-name rebirth is a retained
/// life-handle operation, not a fresh logical-name admission: it may still see predecessor bytes (or
/// answer absent), but the opaque physical life id makes successor bytes structurally unreachable.
TEST(CASNamespaceFileReadContract, HeldLifeAfterSameNameRebirthNeverSeesSuccessorBytes)
{
    DiskFixture fixture = openDiskFixture();
    writeVerbatimThroughDisk(*fixture.storage, kFilePath, "life-1\n");
    const NamespaceLifeId life1 = currentLife(*fixture.storage);
    const NamespaceLifeId life2 = replaceCatalogLife(*fixture.storage, life1);
    fixture.storage->store()->putNamespaceFile(life2, kFile, "life-2\n");

    const std::optional<String> held_read = fixture.storage->tryGetInManifestBytes(kFilePath);
    EXPECT_NE(held_read, std::optional<String>{"life-2\n"});
    EXPECT_TRUE(!held_read || held_read == std::optional<String>{"life-1\n"});
    EXPECT_EQ(fixture.storage->store()->getNamespaceFile(life1, kFile), std::optional<String>{"life-1\n"});
}

/// Mutation caught: capturing only the namespace name and resolving it when the buffer finalizes would
/// overwrite life 2. The real buffer must retain the exact life admitted when it was opened.
TEST(CASNamespaceFileReadContract, DelayedInlineFinalizeCannotChangeSuccessorTokenOrBytes)
{
    DiskFixture fixture = openDiskFixture();
    writeVerbatimThroughDisk(*fixture.storage, kFilePath, "life-1-before\n");
    const NamespaceLifeId life1 = currentLife(*fixture.storage);

    auto delayed_transaction = fixture.storage->createTransaction();
    auto & ca_transaction = dynamic_cast<DB::ContentAddressedTransaction &>(*delayed_transaction);
    auto delayed_buffer = ca_transaction.writeFile(
        kFilePath, DB::DBMS_DEFAULT_BUFFER_SIZE, DB::WriteMode::Rewrite, {});
    ASSERT_NE(dynamic_cast<CaInlineWriteBuffer *>(delayed_buffer.get()), nullptr);
    DB::writeString("life-1-delayed\n", *delayed_buffer);

    const NamespaceLifeId life2 = replaceCatalogLife(*fixture.storage, life1);
    fixture.storage->store()->putNamespaceFile(life2, kFile, "life-2-stable\n");
    Backend & backend = fixture.storage->store()->backend();
    const Layout & layout = fixture.storage->store()->layout();
    const String life1_key = layout.namespaceFileKey(life1, kFile);
    const String life2_key = layout.namespaceFileKey(life2, kFile);
    ASSERT_TRUE(std::filesystem::exists(nativeKeyUnder(fixture.object_storage, life2_key)));
    const HeadResult life2_before = backend.head(life2_key);
    ASSERT_TRUE(life2_before.exists);
    const auto life2_body_before = backend.get(life2_key);
    ASSERT_TRUE(life2_body_before.has_value());
    ASSERT_EQ(life2_body_before->bytes, "life-2-stable\n");

    bool stale_failure = false;
    try
    {
        delayed_buffer->finalize();
    }
    catch (const DB::Exception & e)
    {
        stale_failure = true;
        EXPECT_EQ(e.code(), DB::ErrorCodes::NETWORK_ERROR);
        EXPECT_NE(e.message().find("retrying later"), String::npos);
    }

    const HeadResult life2_after = backend.head(life2_key);
    ASSERT_TRUE(life2_after.exists);
    EXPECT_EQ(life2_after.token, life2_before.token);
    const auto life2_body_after = backend.get(life2_key);
    ASSERT_TRUE(life2_body_after.has_value());
    EXPECT_EQ(life2_body_after->bytes, "life-2-stable\n");

    if (!stale_failure)
    {
        ASSERT_TRUE(std::filesystem::exists(nativeKeyUnder(fixture.object_storage, life1_key)));
        const auto life1_body = backend.get(life1_key);
        ASSERT_TRUE(life1_body.has_value());
        EXPECT_EQ(life1_body->bytes, "life-1-delayed\n");
    }
}

/// `listNamespaceFiles` derives its LIST prefix from `layout.namespaceFilesPrefix(life)` -- a physical
/// life-scoped stream, not the catalog. Listing under a held life must cost exactly one LIST of the
/// files prefix and nothing else.
TEST(CASNamespaceFileReadContract, ListThroughHeldLifeIssuesZeroCatalogRequests)
{
    auto backend = std::make_shared<CountingBackend>();
    PoolPtr store = openPoolForTest(backend);
    const NamespaceLifeId life = fixture::fixtureLife(RootNamespace{"00/ns_file_list_contract@cas@"});
    const String prefix = store->layout().namespaceFilesPrefix(life);

    store->putNamespaceFile(life, "a.txt", "a\n");
    store->putNamespaceFile(life, "b.txt", "b\n");
    backend->resetCounts();

    const std::vector<String> names = store->listNamespaceFiles(life);

    std::vector<String> sorted_names = names;
    std::sort(sorted_names.begin(), sorted_names.end());
    EXPECT_EQ(sorted_names, (std::vector<String>{"a.txt", "b.txt"}));

    /// Positive control: the journal recorded exactly one LIST against the namespace-file stream
    /// prefix and nothing else, so the touched-set assertion below names an absence, not a
    /// recorder that never saw anything.
    EXPECT_EQ(backend->listCount(prefix), 1u);
    EXPECT_EQ(backend->listTotal(), 1u);
    EXPECT_EQ(backend->touchedKeys(), std::vector<String>{prefix});
}
