#include <gtest/gtest.h>
#include <IO/ReadBufferFromString.h>
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
/// token must be rejected with the object still absent — a token-conditional update can never resurrect a
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

TEST_P(CASBackendContract, StreamPutRoundTrip)
{
    auto b = GetParam()();
    auto sink = b->putIfAbsentStream("k/stream1");
    sink->buffer().write("hello ", 6);
    sink->buffer().write("world", 5);
    const auto res = sink->finalize();
    const Token tok = res.token;
    ASSERT_EQ(res.outcome, PutOutcome::Done);
    ASSERT_FALSE(tok.empty());
    auto got = b->get("k/stream1");
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->bytes, "hello world");
    EXPECT_EQ(got->token, tok);
}

TEST_P(CASBackendContract, StreamPutPreconditionAtFinalize)
{
    auto b = GetParam()();
    const auto first_put = b->putIfAbsent("k/stream2", "original");
    const Token first = first_put.token;
    ASSERT_EQ(first_put.outcome, PutOutcome::Done);
    auto sink = b->putIfAbsentStream("k/stream2");
    sink->buffer().write("loser", 5);
    ASSERT_EQ(sink->finalize().outcome, PutOutcome::PreconditionFailed);
    auto got = b->get("k/stream2");
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->bytes, "original");    /// the failed conditional write left the object unmodified
    EXPECT_EQ(got->token, first);
}

TEST_P(CASBackendContract, StreamPutCancelLeavesNothing)
{
    auto b = GetParam()();
    {
        auto sink = b->putIfAbsentStream("k/stream3");
        sink->buffer().write("partial", 7);
        sink->cancel();
    }
    EXPECT_FALSE(b->head("k/stream3").exists);
}

TEST_P(CASBackendContract, StreamPutDestructionWithoutFinalizeLeavesNothing)
{
    auto b = GetParam()();
    {
        auto sink = b->putIfAbsentStream("k/stream4");
        sink->buffer().write("partial", 7);
        /// no finalize, no cancel — destructor must behave as cancel (never publish)
    }
    EXPECT_FALSE(b->head("k/stream4").exists);
}

TEST_P(CASBackendContract, StreamPutEmptyBody)
{
    auto b = GetParam()();
    auto sink = b->putIfAbsentStream("k/stream_empty");
    const auto res = sink->finalize();
    const Token tok = res.token;
    ASSERT_EQ(res.outcome, PutOutcome::Done);
    ASSERT_FALSE(tok.empty());
    auto got = b->get("k/stream_empty");
    ASSERT_TRUE(got.has_value());
    EXPECT_TRUE(got->bytes.empty());
    EXPECT_EQ(got->token, tok);
    auto h = b->head("k/stream_empty");
    EXPECT_TRUE(h.exists);
    EXPECT_EQ(h.size, 0u);
}

/// ~1 MB written in chunks: exercises buffer growth in the memory-buffered sinks and, for the
/// future Native sink, the real streaming path.
TEST_P(CASBackendContract, StreamPutLargeBody)
{
    auto b = GetParam()();
    String chunk(4096, '\0');
    for (size_t i = 0; i < chunk.size(); ++i)
        chunk[i] = static_cast<char>('a' + i % 26);

    String expected;
    auto sink = b->putIfAbsentStream("k/stream_large");
    for (size_t written = 0; written < (1 << 20); written += chunk.size())
    {
        sink->buffer().write(chunk.data(), chunk.size());
        expected += chunk;
    }
    const auto res = sink->finalize();
    const Token tok = res.token;
    ASSERT_EQ(res.outcome, PutOutcome::Done);

    auto got = b->get("k/stream_large");
    ASSERT_TRUE(got.has_value());
    ASSERT_EQ(got->bytes.size(), expected.size());
    EXPECT_EQ(got->bytes, expected);
    EXPECT_EQ(got->token, tok);
}

INSTANTIATE_TEST_SUITE_P(CASInMemory, CASBackendContract,
    ::testing::Values(+[]() -> BackendPtr { return std::make_shared<InMemoryBackend>(); }));

INSTANTIATE_TEST_SUITE_P(CASLocal, CASBackendContract,
    ::testing::Values(+[]() -> BackendPtr
    {
        return std::make_shared<ObjectStorageBackend>(
            DB::Cas::tests::makeLocalObjectStorageForTest(), ObjectStorageBackend::Mode::EmulatedSingleProcess);
    }));

/// The unconditional resurrect counts while streaming and aborts WITHOUT publishing when the reader
/// yields a different byte count than declared. With no precondition on the write, this is the last
/// line of defence against a source truncated after hashing: a post-write check would fire only after
/// the short body had displaced the condemned incarnation.
TEST_P(CASBackendContract, ResurrectWrongSizePublishesNothing)
{
    auto b = GetParam()();
    const auto created = b->putIfAbsent("k/res_short", "condemned-body");
    ASSERT_EQ(created.outcome, PutOutcome::Done);

    DB::ReadBufferFromOwnString in{String("short")};
    EXPECT_THROW(b->resurrect(in, /*payload_size=*/1000, "k/res_short", String("HDR")), DB::Exception);

    const auto got = b->get("k/res_short");
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->bytes, "condemned-body") << "a size-mismatched resurrect must publish nothing";
    EXPECT_EQ(got->token, created.token);
}

/// The resurrect works on EVERY mode of every backend -- the emulated (local object storage) mode
/// included. The former conditional putOverwrite supported local disks, and losing that would make a
/// condemned blob unrepairable on a local content-addressed disk.
TEST_P(CASBackendContract, ResurrectReplacesBodyAndMintsFreshToken)
{
    auto b = GetParam()();
    const auto created = b->putIfAbsent("k/res_ok", "condemned-body");
    ASSERT_EQ(created.outcome, PutOutcome::Done);

    const String payload = "resurrected-payload";
    DB::ReadBufferFromOwnString in{payload};
    const Token fresh = b->resurrect(in, payload.size(), "k/res_ok", String("HDR"));
    EXPECT_FALSE(fresh.empty());
    EXPECT_NE(fresh, created.token);

    const auto got = b->get("k/res_ok");
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->bytes, "HDR" + payload);

    /// INV-NO-RETURN: the queued exact-token delete of the condemned incarnation misses the fresh one.
    EXPECT_EQ(b->deleteExact("k/res_ok", created.token).kind, DeleteOutcome::Kind::TokenMismatch);
    EXPECT_TRUE(b->head("k/res_ok").exists);
}
