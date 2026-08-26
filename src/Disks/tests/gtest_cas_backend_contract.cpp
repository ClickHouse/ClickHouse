#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasObjectStorageBackend.h>
#include <Disks/tests/cas_test_helpers.h>
#include <functional>
#include <memory>

using namespace DB::Cas;

/// Parameterized contract suite: every case creates a fresh backend from the factory,
/// then exercises the Backend seam generically (no InMemoryBackend-specific calls).
/// Fault-injection-only features are excluded — those are InMemory-specific tests.
class CASBackendContract : public ::testing::TestWithParam<std::function<BackendPtr()>>
{
};

TEST_P(CASBackendContract, PutIfAbsentAndGet)
{
    auto b = GetParam()();
    const auto put = b->putIfAbsent("k", "v1");
    const Token t1 = put.token;
    EXPECT_EQ(put.outcome, PutOutcome::Done);
    EXPECT_FALSE(t1.empty());
    EXPECT_EQ(b->putIfAbsent("k", "clobber").outcome, PutOutcome::PreconditionFailed);
    auto g = b->get("k");
    ASSERT_TRUE(g.has_value());
    EXPECT_EQ(g->bytes, "v1");
    EXPECT_EQ(g->token, t1);
    EXPECT_FALSE(b->get("absent").has_value());
}

TEST_P(CASBackendContract, OverwriteIsTokenExactAndMintsFreshToken)
{
    auto b = GetParam()();
    const Token t1 = b->putIfAbsent("k", "v1").token;
    EXPECT_EQ(b->putOverwrite("k", "v2", Token{"wrong", TokenType::Emulated}).outcome, PutOutcome::PreconditionFailed);
    EXPECT_EQ(b->get("k")->bytes, "v1");                       // untouched on mismatch
    const auto overwrite = b->putOverwrite("k", "v2", t1);
    EXPECT_EQ(overwrite.outcome, PutOutcome::Done);
    EXPECT_NE(overwrite.token, t1);                            // tokens never repeat
    EXPECT_EQ(b->get("k")->bytes, "v2");
}

TEST_P(CASBackendContract, CasPutCreateAndSwap)
{
    auto b = GetParam()();
    const auto create = b->casPut("m", "s1", std::nullopt);
    const Token t1 = create.token;
    EXPECT_EQ(create.outcome, CasOutcome::Committed);                              // create-if-absent
    EXPECT_EQ(b->casPut("m", "s1x", std::nullopt).outcome, CasOutcome::Conflict);  // exists now
    EXPECT_EQ(b->casPut("m", "s2", Token{"stale", TokenType::Emulated}).outcome, CasOutcome::Conflict);
    EXPECT_EQ(b->get("m")->bytes, "s1");
    EXPECT_EQ(b->casPut("m", "s2", t1).outcome, CasOutcome::Committed);
    EXPECT_EQ(b->get("m")->bytes, "s2");
}

TEST_P(CASBackendContract, DeleteExactnessAndSurvival)
{
    auto b = GetParam()();
    const Token t1 = b->putIfAbsent("k", "v1").token;
    auto d1 = b->deleteExact("k", Token{"wrong", TokenType::Emulated});
    EXPECT_EQ(d1.kind, DeleteOutcome::Kind::TokenMismatch);
    EXPECT_TRUE(b->get("k").has_value());                      // SURVIVES wrong-token delete
    auto d2 = b->deleteExact("k", t1);
    EXPECT_EQ(d2.kind, DeleteOutcome::Kind::Deleted);
    EXPECT_FALSE(d2.created_delete_marker);
    EXPECT_FALSE(b->get("k").has_value());
}

TEST_P(CASBackendContract, DeleteNotFound)
{
    auto b = GetParam()();
    const Token t1 = b->putIfAbsent("k", "v1").token;
    b->deleteExact("k", t1);
    EXPECT_EQ(b->deleteExact("k", t1).kind, DeleteOutcome::Kind::NotFound);
}

TEST_P(CASBackendContract, RangeGet)
{
    auto b = GetParam()();
    b->putIfAbsent("k", "0123456789");
    Range r;
    r.offset = 2;
    r.length = 3u;
    EXPECT_EQ(b->get("k", r)->bytes, "234");
}

TEST_P(CASBackendContract, Head)
{
    auto b = GetParam()();
    b->putIfAbsent("k", "hello");
    auto h = b->head("k");
    EXPECT_TRUE(h.exists);
    EXPECT_EQ(h.size, 5u);
    EXPECT_FALSE(h.token.empty());
    auto h2 = b->head("missing");
    EXPECT_FALSE(h2.exists);
}

TEST_P(CASBackendContract, ListPagination)
{
    auto b = GetParam()();
    b->putIfAbsent("p/a", "0123456789");
    b->putIfAbsent("p/b", "xy");
    b->putIfAbsent("q/c", "z");
    auto page = b->list("p/", "", 10);
    ASSERT_EQ(page.keys.size(), 2u);                          // sorted, prefix-scoped
    EXPECT_EQ(page.keys[0].key, "p/a");
    EXPECT_EQ(page.keys[1].key, "p/b");
    EXPECT_TRUE(page.next_cursor.empty());
    auto page1 = b->list("p/", "", 1);                        // pagination
    EXPECT_EQ(page1.keys.size(), 1u);
    EXPECT_EQ(page1.keys[0].key, "p/a");
    EXPECT_EQ(page1.next_cursor, "p/a");
    EXPECT_FALSE(page1.next_cursor.empty());
    auto page2 = b->list("p/", page1.next_cursor, 1);
    EXPECT_EQ(page2.keys[0].key, "p/b");
}

TEST_P(CASBackendContract, ReadAfterWrite)
{
    auto b = GetParam()();
    const Token t1 = b->putIfAbsent("rw", "payload").token;
    auto g = b->get("rw");
    ASSERT_TRUE(g.has_value());
    EXPECT_EQ(g->bytes, "payload");
    EXPECT_EQ(g->token, t1);
    auto h = b->head("rw");
    EXPECT_TRUE(h.exists);
    EXPECT_EQ(h.token, t1);
}

/// After an object is created then deleted (key absent again), BOTH conditional updates against a stale
/// token must be rejected with the object still absent — a token-conditional update can never recreate a
/// missing key. For the Native S3 adapter this pins the 404-on-If-Match -> PreconditionFailed/Conflict
/// mapping; for every backend it pins that absence is not a write opportunity for a stale token.
TEST_P(CASBackendContract, OverwriteAndCasOnMissingKey)
{
    auto b = GetParam()();
    const Token t1 = b->putIfAbsent("k", "v1").token;
    EXPECT_EQ(b->deleteExact("k", t1).kind, DeleteOutcome::Kind::Deleted);
    ASSERT_FALSE(b->get("k").has_value());                     // key is absent

    EXPECT_EQ(b->putOverwrite("k", "v2", t1).outcome, PutOutcome::PreconditionFailed);
    EXPECT_FALSE(b->get("k").has_value());                     // still absent

    EXPECT_EQ(b->casPut("k", "v2", t1).outcome, CasOutcome::Conflict);
    EXPECT_FALSE(b->get("k").has_value());                     // still absent
}

INSTANTIATE_TEST_SUITE_P(CASInMemory, CASBackendContract,
    ::testing::Values(+[]() -> BackendPtr { return std::make_shared<InMemoryBackend>(); }));

INSTANTIATE_TEST_SUITE_P(CASLocal, CASBackendContract,
    ::testing::Values(+[]() -> BackendPtr
    {
        return std::make_shared<ObjectStorageBackend>(
            DB::Cas::tests::makeLocalObjectStorageForTest(), ObjectStorageBackend::Mode::EmulatedSingleProcess);
    }));
