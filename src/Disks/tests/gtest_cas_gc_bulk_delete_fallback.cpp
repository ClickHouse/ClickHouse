#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>

#include <variant>
#include <vector>

/// `removeChunkWriteOnceOrOneByOne` (CasGc.h) is what both GC bulk-delete call sites (manifest_deletes'
/// flush() and cleanupRefObjects' chunk loop) use to survive a backend without `DeleteObjects`. Tested
/// here in isolation, directly against the engine, rather than only through the much larger machinery of
/// a full GC round.

namespace DB::ErrorCodes
{
extern const int CORRUPTED_DATA;
extern const int NETWORK_ERROR;
extern const int NOT_IMPLEMENTED;
}

using namespace DB::Cas;
using DB::Cas::tests::expectThrowsCode;

namespace
{

const Layout kLayout{"p"};
const RootNamespace kNs{"test/aa@cas@"};

WriteOnceKey manifestKey(uint32_t ordinal)
{
    return kLayout.writeOnceManifestKey(
        ManifestId{kNs, ManifestRef{.writer_epoch = 1, .build_sequence = 1, .manifest_ordinal = ordinal}});
}

std::vector<WriteOnceKey> manifestKeys(uint32_t count)
{
    std::vector<WriteOnceKey> keys;
    for (uint32_t ordinal = 1; ordinal <= count; ++ordinal)
        keys.push_back(manifestKey(ordinal));
    return keys;
}

PoolPtr openPlainPool(const std::shared_ptr<InMemoryBackend> & backend)
{
    PoolConfig config;
    config.pool_prefix = "p";
    config.server_root_id = "test";
    return Pool::open(backend, config);
}

}

TEST(CASGCBulkDeleteFallback, HappyPathIsOneRequest)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPlainPool(backend);
    CasOperation op = store->openRequests().admit();
    const std::vector<WriteOnceKey> keys = manifestKeys(3);
    for (const WriteOnceKey & key : keys)
        ASSERT_TRUE(std::holds_alternative<Committed>(op.create(key.str(), "b", Retry::once())));

    const uint64_t requests_issued = removeChunkWriteOnceOrOneByOne(op, keys, Retry::once());

    EXPECT_EQ(requests_issued, 1u);
    EXPECT_EQ(backend->bulkRemoveCalls(), 1u);
    for (const WriteOnceKey & key : keys)
        EXPECT_FALSE(op.head(key.str(), Retry::once()).has_value()) << key.str();
}

TEST(CASGCBulkDeleteFallback, NotImplementedFallsBackToOneRequestPerKeyEachDeleted)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPlainPool(backend);
    CasOperation op = store->openRequests().admit();
    const std::vector<WriteOnceKey> keys = manifestKeys(3);
    for (const WriteOnceKey & key : keys)
        ASSERT_TRUE(std::holds_alternative<Committed>(op.create(key.str(), "b", Retry::once())));

    backend->failNextBulkRemoveWith(std::make_exception_ptr(DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "no batch delete")));

    const uint64_t requests_issued = removeChunkWriteOnceOrOneByOne(op, keys, Retry::once());

    EXPECT_EQ(requests_issued, 4u) << "the failed bulk attempt is itself a call, counted alongside the 3 that followed it";
    EXPECT_EQ(backend->bulkRemoveCalls(), 4u) << "1 failed bulk attempt + 3 single-key fallback requests";
    for (const WriteOnceKey & key : keys)
        EXPECT_FALSE(op.head(key.str(), Retry::once()).has_value()) << key.str();
}

/// A teardown begun WHILE the fallback is mid-loop stops the remainder at admission, exactly as any
/// other CAS request would be: `removeChunkWriteOnceOrOneByOne`'s per-key loop is not a special path
/// around the engine's own fence, it is ordinary calls through it.
TEST(CASGCBulkDeleteFallback, TeardownBegunBetweenTwoFallbackKeysStopsTheRemainderAtAdmission)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPlainPool(backend);
    CasOperation op = store->openRequests().admit();
    const std::vector<WriteOnceKey> keys = manifestKeys(4);
    for (const WriteOnceKey & key : keys)
        ASSERT_TRUE(std::holds_alternative<Committed>(op.create(key.str(), "b", Retry::once())));

    backend->failNextBulkRemoveWith(std::make_exception_ptr(DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "no batch delete")));

    /// The hook does not run on the armed (failing) bulk attempt (it is rethrown before the hook would
    /// fire), so this counts only the fallback's own per-key calls that actually reached the backend.
    /// Teardown is armed once the SECOND such call has been served, so it is the THIRD key's own
    /// admission -- checked at the start of its own `removeManyWriteOnce`, before this hook could run
    /// again -- that is refused; the fourth key is never attempted at all.
    size_t backend_calls_served = 0;
    backend->onBeforeBulkRemove([&]
    {
        if (++backend_calls_served == 2)
            store->beginTeardown();
    });

    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { (void)removeChunkWriteOnceOrOneByOne(op, keys, Retry::standard()); });

    /// `store->beginTeardown()` is irreversible here (this test never re-opens the pool), so `op` itself
    /// -- the open plane -- refuses every further request, verification reads included. Read through the
    /// mount plane instead: a different fence over the SAME backend, unaffected by open-plane teardown
    /// (see `CASGCTeardownStop.OpenPlaneRefusesAfterTeardownBeganAndTheMountPlaneDoesNot`).
    CasOperation verify = store->mountRequests().admit();
    EXPECT_FALSE(verify.head(keys[0].str(), Retry::once()).has_value()) << "deleted before teardown began";
    EXPECT_FALSE(verify.head(keys[1].str(), Retry::once()).has_value()) << "deleted before teardown began";
    EXPECT_TRUE(verify.head(keys[2].str(), Retry::once()).has_value()) << "refused at admission, never reached the backend";
    EXPECT_TRUE(verify.head(keys[3].str(), Retry::once()).has_value()) << "never attempted";
    EXPECT_EQ(backend->bulkRemoveCalls(), 3u) << "1 failed bulk attempt + 2 single-key fallback requests that landed";
}

/// A REAL error on one of the fallback's per-key deletes (not "batch not supported", so not caught and
/// retried again) stops the loop exactly where it happened: the keys before it are deleted, the ones
/// from it on are never attempted, and the error itself propagates out of the helper.
TEST(CASGCBulkDeleteFallback, ARealErrorOnAFallbackKeyStopsTheRemainderAndPropagates)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPlainPool(backend);
    CasOperation op = store->openRequests().admit();
    const std::vector<WriteOnceKey> keys = manifestKeys(4);
    for (const WriteOnceKey & key : keys)
        ASSERT_TRUE(std::holds_alternative<Committed>(op.create(key.str(), "b", Retry::once())));

    backend->failNextBulkRemoveWith(std::make_exception_ptr(DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "no batch delete")));

    /// The hook does not run on an armed (failing) call, so this fires only on the fallback's own
    /// per-key calls that actually reached the backend -- the FIRST of which (key[0]'s own delete) arms
    /// a real, non-capability failure for the call right after it, i.e. key[1]'s.
    backend->onBeforeBulkRemove([&]
    {
        backend->failNextBulkRemoveWith(std::make_exception_ptr(DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "not a capability problem")));
    });

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { (void)removeChunkWriteOnceOrOneByOne(op, keys, Retry::once()); });

    EXPECT_FALSE(op.head(keys[0].str(), Retry::once()).has_value()) << "deleted before the real error";
    EXPECT_TRUE(op.head(keys[1].str(), Retry::once()).has_value()) << "this delete is the one that failed";
    EXPECT_TRUE(op.head(keys[2].str(), Retry::once()).has_value()) << "never attempted";
    EXPECT_TRUE(op.head(keys[3].str(), Retry::once()).has_value()) << "never attempted";
    EXPECT_EQ(backend->bulkRemoveCalls(), 3u) << "1 failed bulk attempt + key[0]'s delete + key[1]'s failed attempt";
}

/// A failure outside the "batch delete not supported" class must propagate as-is, with no fallback:
/// the helper does not treat every `removeManyWriteOnce` failure as "try one key at a time".
TEST(CASGCBulkDeleteFallback, OtherFailureClassPropagatesWithNoFallback)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPlainPool(backend);
    CasOperation op = store->openRequests().admit();
    const std::vector<WriteOnceKey> keys = manifestKeys(3);
    for (const WriteOnceKey & key : keys)
        ASSERT_TRUE(std::holds_alternative<Committed>(op.create(key.str(), "b", Retry::once())));

    backend->failNextBulkRemoveWith(std::make_exception_ptr(DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "not a capability problem")));

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { (void)removeChunkWriteOnceOrOneByOne(op, keys, Retry::once()); });

    EXPECT_EQ(backend->bulkRemoveCalls(), 1u) << "no per-key fallback for a non-capability failure";
    for (const WriteOnceKey & key : keys)
        EXPECT_TRUE(op.head(key.str(), Retry::once()).has_value()) << "nothing was deleted";
}
