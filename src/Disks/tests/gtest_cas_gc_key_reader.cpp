#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcKeyReader.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcReadAhead.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasKeyReader.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadPool.h>

/// A reader hands a sequential walk its next object and lets the walk say which keys it will want
/// (hint) and which hinted keys it will never take (discard). The inline reader ignores hints; the
/// read-ahead reader turns them into worker requests and counts a discarded one as wasted at once.

namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

namespace ProfileEvents
{
    extern const Event CASGCReadAheadWasted;
    extern const Event CASGCReadAheadHit;
    extern const Event CASGCReadAheadMiss;
}

using namespace DB::Cas;
using DB::Cas::tests::CountingBackend;
using DB::Cas::tests::openRequestsForTest;

namespace
{

struct Rig
{
    std::shared_ptr<CountingBackend> backend = std::make_shared<CountingBackend>();
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    ThreadPool pool{CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive,
                    CurrentMetrics::LocalThreadScheduled, /*max_threads*/ 4, /*max_free_threads*/ 4, /*queue_size*/ 0};

    void put(const String & key, const String & bytes)
    {
        ASSERT_TRUE(std::holds_alternative<Committed>(op.create(key, bytes, Retry::once()))) << key;
    }
};

uint64_t wasted()
{
    return ProfileEvents::global_counters[ProfileEvents::CASGCReadAheadWasted].load();
}

}

TEST(CASGCKeyReader, DiscardCountsWastedAtOnceAndALaterTakeReadsInline)
{
    Rig rig;
    rig.put("k1", "one");
    GcReadAhead reads(rig.op, rig.requests, rig.pool, 4);
    ReadAheadKeyReader reader(reads);
    rig.backend->resetCounts();

    const uint64_t wasted_before = wasted();
    reader.hint("k1");
    EXPECT_EQ(reader.pending(), 1u);
    reader.discard("k1");
    EXPECT_EQ(reader.pending(), 0u);
    EXPECT_EQ(wasted() - wasted_before, 1u);

    const auto got = reader.take("k1");
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->bytes, "one");
    EXPECT_EQ(rig.backend->getCount("k1"), 2u) << "the discarded request and the inline one";
}

TEST(CASGCKeyReader, DiscardOfAnUnhintedKeyIsANoOp)
{
    Rig rig;
    GcReadAhead reads(rig.op, rig.requests, rig.pool, 4);
    ReadAheadKeyReader reader(reads);
    const uint64_t wasted_before = wasted();
    reader.discard("never-hinted");
    EXPECT_EQ(wasted() - wasted_before, 0u);
    EXPECT_EQ(reader.pending(), 0u);
}

TEST(CASGCKeyReader, DiscardSwallowsAWorkerFailureThatATakeWouldRethrow)
{
    Rig rig;
    rig.put("k1", "one");
    GcReadAhead reads(rig.op, rig.requests, rig.pool, 4);
    ReadAheadKeyReader reader(reads);

    rig.backend->failNextReadWith("k1", std::make_exception_ptr(std::runtime_error("injected worker fault")));
    reader.hint("k1");
    EXPECT_NO_THROW(reader.discard("k1"));

    rig.backend->failNextReadWith("k1", std::make_exception_ptr(std::runtime_error("injected worker fault")));
    reader.hint("k1");
    EXPECT_THROW(static_cast<void>(reader.take("k1")), std::runtime_error);
}

TEST(CASGCKeyReader, InlineReaderHintsNothingAndReadsOnTake)
{
    Rig rig;
    rig.put("k1", "one");
    InlineKeyReader reader(rig.op);
    rig.backend->resetCounts();
    EXPECT_EQ(reader.window(), 0u);
    reader.hint("k1");
    EXPECT_EQ(rig.backend->getCount("k1"), 0u);
    EXPECT_EQ(reader.pending(), 0u);
    const auto got = reader.take("k1");
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->bytes, "one");
    EXPECT_EQ(rig.backend->getCount("k1"), 1u);
    reader.discard("k1");
}

TEST(CASGCKeyReader, ReadAheadReaderWindowAndPendingAreTheReadAheads)
{
    Rig rig;
    GcReadAhead reads(rig.op, rig.requests, rig.pool, 8);
    ReadAheadKeyReader reader(reads);
    EXPECT_EQ(reader.window(), reads.window());
    EXPECT_EQ(reader.window(), 32u);
    rig.put("a", "1");
    reader.hint("a");
    EXPECT_EQ(reader.pending(), reads.pending());
    static_cast<void>(reader.take("a"));
}
