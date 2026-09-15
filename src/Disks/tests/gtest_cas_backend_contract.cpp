#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasObjectStorageBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/tests/cas_test_helpers.h>
#include <functional>
#include <memory>
#include <variant>

using namespace DB::Cas;

using DB::Cas::tests::expectBytes;
using DB::Cas::tests::openRequestsForTest;

/// Parameterized contract suite: every case creates a fresh backend from the factory, then exercises
/// the seam generically through `CasRequests`/`CasOperation` over an open fence (no InMemoryBackend-
/// specific calls). Fault-injection-only features are excluded -- those are InMemory-specific tests.
class CASBackendContract : public ::testing::TestWithParam<std::function<BackendPtr()>>
{
};

TEST_P(CASBackendContract, PutIfAbsentAndGet)
{
    auto b = GetParam()();
    auto requests = openRequestsForTest(b);
    auto op = requests.admit();
    const auto put = op.create("k", "v1", Retry::once());
    ASSERT_TRUE(std::holds_alternative<Committed>(put));
    const Etag t1 = std::get<Committed>(put).etag;
    EXPECT_TRUE(std::holds_alternative<Conflict>(op.create("k", "clobber", Retry::once())));
    auto g = op.read("k", Retry::once());
    ASSERT_TRUE(g.has_value());
    EXPECT_EQ(g->bytes, "v1");
    EXPECT_EQ(g->etag, t1);
    EXPECT_FALSE(op.read("absent", Retry::once()).has_value());
}

/// A wrong-but-REAL precondition, since `Etag` has no public constructor any more: overwriting the key
/// once legitimately mints a second incarnation, which makes the FIRST one genuinely stale for this
/// same key -- a value the engine accepts as a precondition (unlike a fabricated one) but refuses as
/// the wrong one, because the object has already moved past it.
TEST_P(CASBackendContract, OverwriteIsTokenExactAndMintsFreshToken)
{
    auto b = GetParam()();
    auto requests = openRequestsForTest(b);
    auto op = requests.admit();
    const auto created = op.create("k", "v1", Retry::once());
    ASSERT_TRUE(std::holds_alternative<Committed>(created));
    const Etag t1 = std::get<Committed>(created).etag;
    const auto warmup = op.replace("k", "v1b", t1, Retry::once());   // mints a second incarnation, so t1 goes stale
    ASSERT_TRUE(std::holds_alternative<Committed>(warmup));
    const Etag t2 = std::get<Committed>(warmup).etag;

    EXPECT_TRUE(std::holds_alternative<Conflict>(op.replace("k", "v2", t1, Retry::once())));
    expectBytes(b, "k", "v1b");                                 // untouched on mismatch

    const auto overwrite = op.replace("k", "v2", t2, Retry::once());
    ASSERT_TRUE(std::holds_alternative<Committed>(overwrite));
    EXPECT_NE(std::get<Committed>(overwrite).etag, t2);  // etags never repeat
    expectBytes(b, "k", "v2");
}

TEST_P(CASBackendContract, CasPutCreateAndSwap)
{
    auto b = GetParam()();
    auto requests = openRequestsForTest(b);
    auto op = requests.admit();
    const auto create = op.create("m", "s1", Retry::once());
    ASSERT_TRUE(std::holds_alternative<Committed>(create));                     // create-if-absent
    const Etag t1 = std::get<Committed>(create).etag;
    EXPECT_TRUE(std::holds_alternative<Conflict>(op.create("m", "s1x", Retry::once())));   // exists now

    /// Mint a second real incarnation so `t1` becomes a genuinely stale (never fabricated) wrong swap.
    const auto warmup = op.replace("m", "s1y", t1, Retry::once());
    ASSERT_TRUE(std::holds_alternative<Committed>(warmup));
    const Etag t2 = std::get<Committed>(warmup).etag;
    EXPECT_TRUE(std::holds_alternative<Conflict>(op.replace("m", "s2", t1, Retry::once())));
    expectBytes(b, "m", "s1y");

    EXPECT_TRUE(std::holds_alternative<Committed>(op.replace("m", "s2", t2, Retry::once())));
    expectBytes(b, "m", "s2");
}

TEST_P(CASBackendContract, DeleteExactnessAndSurvival)
{
    auto b = GetParam()();
    auto requests = openRequestsForTest(b);
    auto op = requests.admit();
    const auto created = op.create("k", "v1", Retry::once());
    ASSERT_TRUE(std::holds_alternative<Committed>(created));
    const Etag t1 = std::get<Committed>(created).etag;

    /// A real but stale incarnation, minted by a legitimate overwrite (see the comment on
    /// `OverwriteIsTokenExactAndMintsFreshToken`).
    const auto warmup = op.replace("k", "v1b", t1, Retry::once());
    ASSERT_TRUE(std::holds_alternative<Committed>(warmup));
    const Etag t2 = std::get<Committed>(warmup).etag;

    EXPECT_EQ(op.remove("k", t1, Retry::once()), Removal::Mismatch);
    EXPECT_TRUE(op.read("k", Retry::once()).has_value());       // SURVIVES wrong-incarnation delete
    EXPECT_EQ(op.remove("k", t2, Retry::once()), Removal::Removed);
    EXPECT_FALSE(op.read("k", Retry::once()).has_value());
}

TEST_P(CASBackendContract, DeleteNotFound)
{
    auto b = GetParam()();
    auto requests = openRequestsForTest(b);
    auto op = requests.admit();
    const auto created = op.create("k", "v1", Retry::once());
    ASSERT_TRUE(std::holds_alternative<Committed>(created));
    const Etag t1 = std::get<Committed>(created).etag;
    EXPECT_EQ(op.remove("k", t1, Retry::once()), Removal::Removed);
    EXPECT_EQ(op.remove("k", t1, Retry::once()), Removal::Gone);
}

/// `Range` had no primitive read counterpart even before this migration -- `op.read` takes no range
/// argument at all, so a non-whole window is refused by the TYPE, not by a runtime NOT_IMPLEMENTED
/// throw. The property (the whole read still serves) is what `ReadAfterWrite` below already pins.

TEST_P(CASBackendContract, Head)
{
    auto b = GetParam()();
    auto requests = openRequestsForTest(b);
    auto op = requests.admit();
    ASSERT_TRUE(std::holds_alternative<Committed>(op.create("k", "hello", Retry::once())));
    auto h = op.head("k", Retry::once());
    ASSERT_TRUE(h.has_value());
    EXPECT_EQ(h->size, 5u);
    auto h2 = op.head("missing", Retry::once());
    EXPECT_FALSE(h2.has_value());
}

TEST_P(CASBackendContract, ListPagination)
{
    auto b = GetParam()();
    auto requests = openRequestsForTest(b);
    auto op = requests.admit();
    ASSERT_TRUE(std::holds_alternative<Committed>(op.create("p/a", "0123456789", Retry::once())));
    ASSERT_TRUE(std::holds_alternative<Committed>(op.create("p/b", "xy", Retry::once())));
    ASSERT_TRUE(std::holds_alternative<Committed>(op.create("q/c", "z", Retry::once())));
    auto page = op.list("p/", "", 10, Retry::once());
    ASSERT_EQ(page.keys.size(), 2u);                          // sorted, prefix-scoped
    EXPECT_EQ(page.keys[0].key, "p/a");
    EXPECT_EQ(page.keys[1].key, "p/b");
    EXPECT_TRUE(page.next_cursor.empty());
    auto page1 = op.list("p/", "", 1, Retry::once());         // pagination
    EXPECT_EQ(page1.keys.size(), 1u);
    EXPECT_EQ(page1.keys[0].key, "p/a");
    EXPECT_EQ(page1.next_cursor, "p/a");
    EXPECT_FALSE(page1.next_cursor.empty());
    auto page2 = op.list("p/", page1.next_cursor, 1, Retry::once());
    EXPECT_EQ(page2.keys[0].key, "p/b");
}

TEST_P(CASBackendContract, ReadAfterWrite)
{
    auto b = GetParam()();
    auto requests = openRequestsForTest(b);
    auto op = requests.admit();
    const auto created = op.create("rw", "payload", Retry::once());
    ASSERT_TRUE(std::holds_alternative<Committed>(created));
    const Etag t1 = std::get<Committed>(created).etag;
    auto g = op.read("rw", Retry::once());
    ASSERT_TRUE(g.has_value());
    EXPECT_EQ(g->bytes, "payload");
    EXPECT_EQ(g->etag, t1);
    auto h = op.head("rw", Retry::once());
    ASSERT_TRUE(h.has_value());
    EXPECT_EQ(h->etag, t1);
}

/// After an object is created then deleted (key absent again), a conditional update against the
/// incarnation it held while alive must be rejected with the object still absent -- an
/// incarnation-conditional update can never recreate a missing key. For the Native S3 adapter this
/// pins the 404-on-If-Match -> Conflict mapping; for every backend it pins that absence is not a write
/// opportunity for a since-deleted incarnation. The legacy `putOverwrite` and `casPut(expected)`
/// verbs this test used to drive separately both reach this SAME primitive (`replace`) now.
TEST_P(CASBackendContract, OverwriteAndCasOnMissingKey)
{
    auto b = GetParam()();
    auto requests = openRequestsForTest(b);
    auto op = requests.admit();
    const auto created = op.create("k", "v1", Retry::once());
    ASSERT_TRUE(std::holds_alternative<Committed>(created));
    const Etag t1 = std::get<Committed>(created).etag;
    EXPECT_EQ(op.remove("k", t1, Retry::once()), Removal::Removed);
    ASSERT_FALSE(op.read("k", Retry::once()).has_value());     // key is absent

    EXPECT_TRUE(std::holds_alternative<Conflict>(op.replace("k", "v2", t1, Retry::once())));
    EXPECT_FALSE(op.read("k", Retry::once()).has_value());     // still absent
}

INSTANTIATE_TEST_SUITE_P(CASInMemory, CASBackendContract,
    ::testing::Values(+[]() -> BackendPtr { return std::make_shared<InMemoryBackend>(); }));

INSTANTIATE_TEST_SUITE_P(CASLocal, CASBackendContract,
    ::testing::Values(+[]() -> BackendPtr
    {
        return std::make_shared<ObjectStorageBackend>(
            DB::Cas::tests::makeLocalObjectStorageForTest(), ObjectStorageBackend::Mode::EmulatedSingleProcess);
    }));
