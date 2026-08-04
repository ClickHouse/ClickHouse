#include "cas_format_test_battery.h"
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasServerRootFormats.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Common/Exception.h>
#include <limits>

using namespace DB::Cas;

namespace DB::ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

TEST(CASFormatBattery, Owner)
{
    OwnerObject o;
    o.server_uuid = hexToU128("0123456789abcdeffedcba9876543210");
    const String golden = currentFormatHeader("cas_owner") +
        "{\"su\":\"0123456789abcdeffedcba9876543210\"}\n";
    EXPECT_EQ(encodeOwner(o), golden);
    EXPECT_FALSE(decodeOwner(golden).retired_at_ms.has_value());
    runFormatBattery({FormatId::Owner,
        [&] { return sealObject(FormatId::Owner, encodeOwner(o)); },
        [](std::string_view s) { decodeOwner(std::string(openObject(FormatId::Owner, s))); },
        golden});
}

TEST(CASOwnerFormat, RetiredAtRoundTrip)
{
    OwnerObject o;
    o.server_uuid = hexToU128("0123456789abcdeffedcba9876543210");
    o.retired_at_ms = 1752537600000ULL;

    const OwnerObject back = decodeOwner(encodeOwner(o));
    EXPECT_EQ(back.server_uuid, o.server_uuid);
    EXPECT_EQ(back.retired_at_ms, o.retired_at_ms);
}

TEST(CASFormatBattery, ServerEpoch)
{
    ServerEpoch e;
    e.next_writer_epoch = 7;
    runFormatBattery({FormatId::ServerEpoch,
        [&] { return sealObject(FormatId::ServerEpoch, encodeServerEpoch(e)); },
        [](std::string_view s) { decodeServerEpoch(std::string(openObject(FormatId::ServerEpoch, s))); },
        currentFormatHeader("cas_epoch") + "{\"nwe\":\"7\"}\n"});
}

TEST(CASFormatBattery, MountLease)
{
    MountLease m{hexToU128("0123456789abcdeffedcba9876543210"), 7, "host-1", 4242,
                 1752537600000ULL, 5, 1752537630000ULL, 9, false};
    runFormatBattery({FormatId::MountLease,
        [&] { return sealObject(FormatId::MountLease, encodeMountLease(m)); },
        [](std::string_view s) { decodeMountLease(std::string(openObject(FormatId::MountLease, s))); },
        currentFormatHeader("cas_mount_lease") +
        "{\"su\":\"0123456789abcdeffedcba9876543210\",\"we\":\"7\",\"hn\":\"host-1\",\"pid\":4242,"
        "\"sat\":1752537600000,\"seq\":\"5\",\"eat\":1752537630000,\"ma\":\"9\",\"fen\":false}\n"});
}

TEST(CASMountLeaseFormat, FarewellSentinelAndFencedSurvive)
{
    MountLease m{hexToU128("0123456789abcdeffedcba9876543210"), 7, "h", 1,
                 1, 5, 2, std::numeric_limits<uint64_t>::max(), true};
    const MountLease back = decodeMountLease(encodeMountLease(m));
    EXPECT_EQ(back.min_active, std::numeric_limits<uint64_t>::max());
    EXPECT_TRUE(back.gc_fenced);
    EXPECT_EQ(back.hostname, "h");
    EXPECT_EQ(back.writer_epoch, 7u);
    EXPECT_EQ(back.seq, 5u);
}

TEST(CASMountLeaseFormat, RejectsMissingIdentityFields)
{
    const String header = "{\"type\":\"cas_mount_lease\",\"v\":3}\n";
    const String fields = "\"hn\":\"host-1\",\"pid\":4242,\"sat\":1752537600000,"
                          "\"seq\":\"5\",\"eat\":1752537630000,\"ma\":\"9\",\"fen\":false}";

    const auto expectCorrupted = [](const String & data)
    {
        try
        {
            decodeMountLease(data);
            FAIL() << "expected CORRUPTED_DATA";
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), DB::ErrorCodes::CORRUPTED_DATA);
        }
    };

    expectCorrupted(header + R"({"we":"7",)" + fields + "\n");
    expectCorrupted(header + R"({"su":"0123456789abcdeffedcba9876543210",)" + fields + "\n");
}
