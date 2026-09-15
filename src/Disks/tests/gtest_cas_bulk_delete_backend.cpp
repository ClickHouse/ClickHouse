#include <gtest/gtest.h>

#include "config.h"

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInstrumentedBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasObjectStorageBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasThrottlingBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/ProfileEvents.h>

/// `removeManyWriteOnce` deletes up to 1000 write-once keys in one request with no precondition:
/// an absent key is success, a present one is gone afterwards, and every backend honours the same
/// fault knobs the single-key delete has.

namespace ProfileEvents
{
    extern const Event CASBulkDeleteRequests;
    extern const Event CASManifestDelete;
    extern const Event CASRootDelete;
}

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

using namespace DB::Cas;
using DB::Cas::tests::expectThrowsCode;
using DB::Cas::tests::openRequestsForTest;

namespace
{

const Layout kLayout{"p"};
const RootNamespace kNs{"test/aa@cas@"};

ManifestId manifest(uint32_t ordinal)
{
    return ManifestId{kNs, ManifestRef{.writer_epoch = 1, .build_sequence = 1, .manifest_ordinal = ordinal}};
}

/// Three manifest keys with bodies and one that was never written.
struct Keys
{
    std::vector<WriteOnceKey> present;
    WriteOnceKey absent;
};

Keys seed(CasOperation & op)
{
    Keys keys{.present = {}, .absent = kLayout.writeOnceManifestKey(manifest(4))};
    for (uint32_t ordinal = 1; ordinal <= 3; ++ordinal)
    {
        const WriteOnceKey key = kLayout.writeOnceManifestKey(manifest(ordinal));
        EXPECT_TRUE(std::holds_alternative<Committed>(op.create(key.str(), "body-" + std::to_string(ordinal), Retry::once())));
        keys.present.push_back(key);
    }
    return keys;
}

}

TEST(CASBulkDeleteBackend, InMemoryDeletesPresentKeysAndTreatsAbsentAsSuccess)
{
    auto backend = std::make_shared<InMemoryBackend>();
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    Keys keys = seed(op);
    std::vector<WriteOnceKey> batch = keys.present;
    batch.push_back(keys.absent);

    op.removeManyWriteOnce(batch, Retry::once());

    for (const WriteOnceKey & key : batch)
        EXPECT_FALSE(op.head(key.str(), Retry::once()).has_value()) << key.str();
    EXPECT_EQ(backend->bulkRemoveCalls(), 1u);
}

TEST(CASBulkDeleteBackend, InMemoryHeldDeletesLandLater)
{
    auto backend = std::make_shared<InMemoryBackend>();
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    Keys keys = seed(op);

    backend->setHoldDeletes(true);
    op.removeManyWriteOnce(keys.present, Retry::once());
    for (const WriteOnceKey & key : keys.present)
        EXPECT_TRUE(op.head(key.str(), Retry::once()).has_value()) << "held, not landed: " << key.str();
    while (backend->pendingDeletes() > 0)
        backend->landPendingDelete(0);
    for (const WriteOnceKey & key : keys.present)
        EXPECT_FALSE(op.head(key.str(), Retry::once()).has_value()) << key.str();
}

TEST(CASBulkDeleteBackend, InMemoryArmedFailureFiresOnceAndTheHookRunsBeforeTheDeletes)
{
    auto backend = std::make_shared<InMemoryBackend>();
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    Keys keys = seed(op);

    size_t hook_runs = 0;
    backend->onBeforeBulkRemove([&] { ++hook_runs; });
    backend->failNextBulkRemoveWith(std::make_exception_ptr(Poco::TimeoutException("injected")));

    op.removeManyWriteOnce(keys.present, Retry::standard());   /// the engine reissues the chunk
    EXPECT_EQ(backend->bulkRemoveCalls(), 2u);
    EXPECT_EQ(hook_runs, 1u) << "the hook runs on the attempt that deletes, not on the refused one";
    for (const WriteOnceKey & key : keys.present)
        EXPECT_FALSE(op.head(key.str(), Retry::once()).has_value()) << key.str();
}

TEST(CASBulkDeleteBackend, InstrumentedCountsOneRequestAndOneDeletePerKeyClass)
{
    auto inner = std::make_shared<InMemoryBackend>();
    auto backend = std::make_shared<InstrumentedBackend>(inner);
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    Keys keys = seed(op);
    const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(kNs, DB::UInt128(0x55));
    const WriteOnceKey log = kLayout.writeOnceRefLogKey(life, RefTxnId{1, 1});
    ASSERT_TRUE(std::holds_alternative<Committed>(op.create(log.str(), "log", Retry::once())));

    const auto requests_before = ProfileEvents::global_counters[ProfileEvents::CASBulkDeleteRequests].load();
    const auto manifest_before = ProfileEvents::global_counters[ProfileEvents::CASManifestDelete].load();
    const auto root_before = ProfileEvents::global_counters[ProfileEvents::CASRootDelete].load();

    std::vector<WriteOnceKey> batch = keys.present;
    batch.push_back(log);
    op.removeManyWriteOnce(batch, Retry::once());

    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASBulkDeleteRequests].load() - requests_before, 1u);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASManifestDelete].load() - manifest_before, 3u);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASRootDelete].load() - root_before, 1u);
}

#if USE_AWS_S3
TEST(CASBulkDeleteBackend, ThrottlingRefusesTheChunkOnceAndTheEngineReissuesIt)
{
    auto inner = std::make_shared<InMemoryBackend>();
    auto backend = std::make_shared<ThrottlingBackend>(inner, ThrottlingBackend::Mode::FirstPerKey, 1, 429);
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    /// Seeded under `Retry::standard()`: the FirstPerKey refusal on each key's own create is an
    /// AMBIGUOUS attempt the engine must reissue to land at all, which only a reissuable policy grants.
    std::vector<WriteOnceKey> present;
    for (uint32_t ordinal = 1; ordinal <= 3; ++ordinal)
    {
        const WriteOnceKey key = kLayout.writeOnceManifestKey(manifest(ordinal));
        ASSERT_TRUE(std::holds_alternative<Committed>(op.create(key.str(), "body-" + std::to_string(ordinal), Retry::standard())));
        present.push_back(key);
    }
    /// the FirstPerKey refusal is spent on these keys' writes above; the bulk delete's own request
    /// each key names is the SECOND request naming it and passes unrefused.
    op.removeManyWriteOnce(present, Retry::standard());
    for (const WriteOnceKey & key : present)
        EXPECT_FALSE(op.head(key.str(), Retry::once()).has_value()) << key.str();
    EXPECT_GE(backend->refusals(present.front().str()), 1u);
}
#endif

#if USE_AWS_S3
TEST(CASBulkDeleteBackend, EmulatedModeDeletesUnderTheEmulationLockAndForgetsTheTokens)
{
    auto storage = DB::Cas::tests::makeLocalObjectStorageForTest();
    auto backend = std::make_shared<ObjectStorageBackend>(storage, ObjectStorageBackend::Mode::EmulatedSingleProcess);
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    Keys keys = seed(op);
    std::vector<WriteOnceKey> batch = keys.present;
    batch.push_back(keys.absent);

    op.removeManyWriteOnce(batch, Retry::once());

    for (const WriteOnceKey & key : batch)
        EXPECT_FALSE(op.head(key.str(), Retry::once()).has_value()) << key.str();
    /// A recreate at a deleted key must mint a fresh incarnation, which is what the token bookkeeping
    /// after a delete exists for.
    EXPECT_TRUE(std::holds_alternative<Committed>(op.create(keys.present.front().str(), "again", Retry::once())));
}

TEST(CASBulkDeleteBackend, LocalObjectStorageRefusesTheProfileOverload)
{
    auto storage = DB::Cas::tests::makeLocalObjectStorageForTest();
    DB::StoredObjects objects{DB::StoredObject("p/anything")};
    expectThrowsCode(DB::ErrorCodes::NOT_IMPLEMENTED, [&]
    {
        storage->removeObjectsIfExistUnderProfile(objects, DB::ObjectStorageControlRequest{
            .profile = DB::ObjectStorageRetryProfile::SingleAttempt, .attempt_timeout_ms = 1000});
    });
}
#endif
