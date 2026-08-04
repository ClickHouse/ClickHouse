#include "cas_format_test_battery.h"
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasGcStateFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>

using namespace DB::Cas;

namespace DB::ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
}

TEST(CASFormatBattery, GcState)
{
    GcState s;
    s.round = 4;
    s.gc_shards = 1;
    s.snap_generation = 9;
    s.snap_pruned_through = 7;
    s.snap_attempt = 3;
    s.manifest_sweep_cursor = "";
    s.lease = GcLease{UInt128(1), 12};
    runFormatBattery({FormatId::GcState,
        [&] { return sealObject(FormatId::GcState, encodeGcState(s)); },
        [](std::string_view d) { decodeGcState(std::string(openObject(FormatId::GcState, d))); },
        currentFormatHeader("cas_gc_state") +
        "{\"rnd\":\"4\",\"gcs\":1,\"sg\":\"9\",\"spt\":\"7\",\"sa\":\"3\",\"msc\":\"\","
        "\"lo\":\"00000000000000000000000000000001\",\"ls\":\"12\"}\n"});
}

TEST(CASFormatBattery, GcHeartbeat)
{
    GcHeartbeat hb{UInt128(1), 1741};
    runFormatBattery({FormatId::GcHeartbeat,
        [&] { return sealObject(FormatId::GcHeartbeat, encodeGcHeartbeat(hb)); },
        [](std::string_view d) { decodeGcHeartbeat(std::string(openObject(FormatId::GcHeartbeat, d))); },
        currentFormatHeader("cas_gc_hb") +
        "{\"by\":\"00000000000000000000000000000001\",\"seq\":\"1741\"}\n"});
}

/// ---------- field round-trips (migrated from gtest_cas_gc_formats.cpp, re-pointed at the text codec) ----------

TEST(CASGCStateFormat, RoundTripsCoreFields)
{
    GcState s;
    s.round = 7;
    s.gc_shards = 1;
    s.snap_generation = 12;
    s.lease.owner = hexToU128("00000000000000000000000000000005");
    s.lease.seq = 5;
    auto d = decodeGcState(encodeGcState(s));
    EXPECT_EQ(d.round, 7u);
    EXPECT_EQ(d.gc_shards, 1u);
    EXPECT_EQ(d.snap_generation, 12u);
    EXPECT_EQ(d.lease.owner, hexToU128("00000000000000000000000000000005"));
    EXPECT_EQ(d.lease.seq, 5u);
}

TEST(CASGCStateFormat, SnapPrunedThroughAndAttemptAndCursorRoundTrip)
{
    GcState s;
    s.gc_shards = 2;
    s.snap_generation = 42;
    s.snap_pruned_through = 38;
    s.snap_attempt = 7;
    s.manifest_sweep_cursor = "p/cas/manifests/server/store/abc/table@cas@/writer/42/aa/id";
    auto d = decodeGcState(encodeGcState(s));
    EXPECT_EQ(d.snap_pruned_through, 38u);
    EXPECT_EQ(d.snap_attempt, 7u);
    EXPECT_EQ(d.manifest_sweep_cursor, s.manifest_sweep_cursor);
}

TEST(CASGCStateFormat, DefaultsRoundTrip)
{
    GcState s;   /// gc_shards defaults to 1
    EXPECT_EQ(s.gc_shards, 1u);
    auto d = decodeGcState(encodeGcState(s));
    EXPECT_EQ(d.round, 0u);
    EXPECT_EQ(d.snap_attempt, 0u);
    EXPECT_TRUE(d.manifest_sweep_cursor.empty());
    EXPECT_EQ(d.lease.owner, UInt128{});
}

TEST(CASGCStateFormat, RejectsZeroGcShards)
{
    /// `v:3` is deliberate and must NOT follow a future `G_BUILD` bump: any version <= G_BUILD passes
    /// the header gate, which is the point — the BODY is what has to fail here.
    const String bad = "{\"type\":\"cas_gc_state\",\"v\":3}\n"
                       "{\"rnd\":\"0\",\"gcs\":0,\"sg\":\"0\",\"spt\":\"0\",\"sa\":\"0\",\"msc\":\"\","
                       "\"lo\":\"00000000000000000000000000000000\",\"ls\":\"0\"}\n";
    EXPECT_THROW(decodeGcState(bad), DB::Exception);
}

#ifndef DEBUG_OR_SANITIZER_BUILD
/// encodeGcState(gc_shards=0) throws LOGICAL_ERROR, which aborts the whole process in debug/sanitizer
/// builds instead of behaving like a catchable exception -- CASGCStateFormatDeathTest below proves the
/// abort positively in those builds instead.
TEST(CASGCStateFormat, RejectsZeroGcShardsOnEncode)
{
    GcState state;
    state.gc_shards = 0;

    try
    {
        encodeGcState(state);
        FAIL() << "expected exception code " << DB::ErrorCodes::LOGICAL_ERROR;
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::LOGICAL_ERROR);
    }
}
#endif

#if defined(DEBUG_OR_SANITIZER_BUILD)
TEST(CASGCStateFormatDeathTest, RejectsZeroGcShardsOnEncodeAborts)
{
    GcState state;
    state.gc_shards = 0;
    EXPECT_DEATH({ (void)encodeGcState(state); }, "");
}
#endif

TEST(CASGCStateFormat, RejectsAbsentGcShards)
{
    /// An absent gcs key must fail closed (the writer always emits it) rather than silently defaulting
    /// to the struct's gc_shards = 1 — a missing shard count means a corrupt object, not "use the floor".
    /// `v:3` is deliberate and must NOT follow a future `G_BUILD` bump: any version <= G_BUILD passes
    /// the header gate, which is the point — the BODY is what has to fail here.
    const String bad = "{\"type\":\"cas_gc_state\",\"v\":3}\n"
                       "{\"rnd\":\"0\",\"sg\":\"0\",\"spt\":\"0\",\"sa\":\"0\",\"msc\":\"\","
                       "\"lo\":\"00000000000000000000000000000000\",\"ls\":\"0\"}\n";
    EXPECT_THROW(decodeGcState(bad), DB::Exception);
}

TEST(CASGCStateFormat, GarbageFailsClosed)
{
    EXPECT_THROW(decodeGcState(String("")), DB::Exception);
    EXPECT_THROW(decodeGcState(String("not a cas object\n")), DB::Exception);
}

TEST(CASGCHeartbeatFormat, RoundTripAndBoundaries)
{
    GcHeartbeat hb;
    hb.owner = hexToU128("0123456789abcdeffedcba9876543210");
    hb.hb_seq = 12345;
    GcHeartbeat d = decodeGcHeartbeat(encodeGcHeartbeat(hb));
    EXPECT_EQ(d.owner, hb.owner);
    EXPECT_EQ(d.hb_seq, 12345u);

    GcHeartbeat z;
    z.owner = hexToU128("ffffffffffffffffffffffffffffffff");
    z.hb_seq = 0;
    EXPECT_EQ(decodeGcHeartbeat(encodeGcHeartbeat(z)).owner, z.owner);
    EXPECT_THROW(decodeGcHeartbeat(String("short")), DB::Exception);
}

TEST(CASGCHeartbeatFormat, RejectsMissingIdentityFields)
{
    /// `v:3` is deliberate and must NOT follow a future `G_BUILD` bump: any version <= G_BUILD passes
    /// the header gate, which is the point — the BODY is what has to fail here.
    const String header = "{\"type\":\"cas_gc_hb\",\"v\":3}\n";

    const auto expectCorrupted = [](const String & data)
    {
        try
        {
            decodeGcHeartbeat(data);
            FAIL() << "expected CORRUPTED_DATA";
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), DB::ErrorCodes::CORRUPTED_DATA);
        }
    };

    expectCorrupted(header + "{\"seq\":\"1741\"}\n");
    expectCorrupted(header + "{\"by\":\"00000000000000000000000000000001\"}\n");
}
