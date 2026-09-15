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

CAS_BATTERY_COVERS(Owner);

TEST(CASFormatBattery, Owner)
{
    OwnerObject o;
    o.server_uuid = hexToU128("0123456789abcdeffedcba9876543210");
    const String golden = currentFormatHeader("cas_owner") +
        "{\"server_uuid\":\"0123456789abcdeffedcba9876543210\"}\n";
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

    EXPECT_EQ(encodeOwner(o), currentFormatHeader("cas_owner")
        + "{\"server_uuid\":\"0123456789abcdeffedcba9876543210\",\"retired_at_ms\":1752537600000}\n");
    const OwnerObject back = decodeOwner(encodeOwner(o));
    EXPECT_EQ(back.server_uuid, o.server_uuid);
    EXPECT_EQ(back.retired_at_ms, o.retired_at_ms);
}

CAS_BATTERY_COVERS(ServerEpoch);

TEST(CASFormatBattery, ServerEpoch)
{
    ServerEpoch e;
    e.next_writer_epoch = 7;
    runFormatBattery({FormatId::ServerEpoch,
        [&] { return sealObject(FormatId::ServerEpoch, encodeServerEpoch(e)); },
        [](std::string_view s) { decodeServerEpoch(std::string(openObject(FormatId::ServerEpoch, s))); },
        currentFormatHeader("cas_epoch") + "{\"next_writer_epoch\":\"7\"}\n"});
}

CAS_BATTERY_COVERS(MountLease);

TEST(CASFormatBattery, MountLease)
{
    MountLease m{hexToU128("0123456789abcdeffedcba9876543210"), 7, "host-1", 4242,
                 1752537600000ULL, 5, 1752537630000ULL, 9, false,
                 hexToU128("00112233445566778899aabbccddeeff")};
    runFormatBattery({FormatId::MountLease,
        [&] { return sealObject(FormatId::MountLease, encodeMountLease(m)); },
        [](std::string_view s) { decodeMountLease(std::string(openObject(FormatId::MountLease, s))); },
        currentFormatHeader("cas_mount_lease") +
        "{\"server_uuid\":\"0123456789abcdeffedcba9876543210\",\"writer_epoch\":\"7\",\"hostname\":\"host-1\",\"pid\":4242,"
        "\"started_at_ms\":1752537600000,\"seq\":\"5\",\"expires_at_ms\":1752537630000,\"min_active_build_sequence\":\"9\",\"gc_fenced\":false,"
        "\"write_attempt_id\":\"00112233445566778899aabbccddeeff\"}\n"});
}

TEST(CASMountLeaseFormat, FarewellSentinelAndFencedSurvive)
{
    MountLease m{hexToU128("0123456789abcdeffedcba9876543210"), 7, "h", 1,
                 1, 5, 2, std::numeric_limits<uint64_t>::max(), true,
                 hexToU128("00112233445566778899aabbccddeeff")};
    const MountLease back = decodeMountLease(encodeMountLease(m));
    EXPECT_EQ(back.min_active_build_sequence, std::numeric_limits<uint64_t>::max());
    EXPECT_TRUE(back.gc_fenced);
    EXPECT_EQ(back.hostname, "h");
    EXPECT_EQ(back.writer_epoch, 7u);
    EXPECT_EQ(back.seq, 5u);
}

TEST(CASMountLeaseFormat, WriteAttemptIdIsRequiredAndCanonical)
{
    MountLease m;
    m.server_uuid = hexToU128("0123456789abcdeffedcba9876543210");
    m.writer_epoch = 7;
    m.write_attempt_id = hexToU128("00112233445566778899aabbccddeeff");

    const String encoded = encodeMountLease(m);
    EXPECT_NE(encoded.find("\"write_attempt_id\":\"00112233445566778899aabbccddeeff\""), String::npos);
    EXPECT_EQ(decodeMountLease(encoded).write_attempt_id, m.write_attempt_id);

    const String without_attempt_id = currentFormatHeader("cas_mount_lease") +
        "{\"server_uuid\":\"0123456789abcdeffedcba9876543210\",\"writer_epoch\":\"7\",\"hostname\":\"\",\"pid\":0,"
        "\"started_at_ms\":0,\"seq\":\"0\",\"expires_at_ms\":0,\"min_active_build_sequence\":\"0\",\"gc_fenced\":false}\n";
    try
    {
        decodeMountLease(without_attempt_id);
        FAIL() << "expected CORRUPTED_DATA";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::CORRUPTED_DATA);
    }
}

TEST(CASMountLeaseFormat, ZeroWriteAttemptIdIsRejected)
{
    const String data = currentFormatHeader("cas_mount_lease") +
        "{\"server_uuid\":\"0123456789abcdeffedcba9876543210\",\"writer_epoch\":\"7\",\"hostname\":\"\",\"pid\":0,"
        "\"started_at_ms\":0,\"seq\":\"0\",\"expires_at_ms\":0,\"min_active_build_sequence\":\"0\",\"gc_fenced\":false,"
        "\"write_attempt_id\":\"00000000000000000000000000000000\"}\n";
    try
    {
        decodeMountLease(data);
        FAIL() << "expected CORRUPTED_DATA";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::CORRUPTED_DATA);
    }
}

TEST(CASMountLeaseFormat, UnknownFieldsRemainTolerated)
{
    MountLease m;
    m.server_uuid = hexToU128("0123456789abcdeffedcba9876543210");
    m.writer_epoch = 7;
    m.write_attempt_id = hexToU128("00112233445566778899aabbccddeeff");
    String encoded = encodeMountLease(m);
    const size_t end = encoded.find("}\n");
    ASSERT_NE(end, String::npos);
    encoded.insert(end, ",\"future_mount_field\":true");
    EXPECT_EQ(decodeMountLease(encoded).write_attempt_id, m.write_attempt_id);
}

TEST(CASMountLeaseFormat, RejectsMissingIdentityFields)
{
    /// Each arm drops exactly ONE identity and keeps the other two, and each asserts the message that
    /// names the dropped one. A body missing two of them would satisfy whichever clause runs first, so
    /// a shared fixture and a shared message together would let two of the three checks be deleted
    /// with this test still green.
    const String header = "{\"type\":\"cas_mount_lease\",\"v\":1}\n";
    const String uuid = R"("server_uuid":"0123456789abcdeffedcba9876543210",)";
    const String epoch = R"("writer_epoch":"7",)";
    const String attempt = R"("write_attempt_id":"00112233445566778899aabbccddeeff",)";
    const String rest = "\"hostname\":\"host-1\",\"pid\":4242,\"started_at_ms\":1752537600000,"
                        "\"seq\":\"5\",\"expires_at_ms\":1752537630000,\"min_active_build_sequence\":\"9\",\"gc_fenced\":false}";

    const auto expectMessage = [](const String & data, std::string_view expected)
    {
        try
        {
            decodeMountLease(data);
            FAIL() << "expected CORRUPTED_DATA";
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), DB::ErrorCodes::CORRUPTED_DATA);
            EXPECT_EQ(e.message(), expected);
        }
    };

    expectMessage(header + "{" + epoch + attempt + rest + "\n", "CAS mount-lease: missing server_uuid");
    expectMessage(header + "{" + uuid + attempt + rest + "\n", "CAS mount-lease: missing writer_epoch");
    expectMessage(header + "{" + uuid + epoch + rest + "\n", "CAS mount-lease: missing or zero write_attempt_id");
}
