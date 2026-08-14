#include <gtest/gtest.h>

#include <Core/ProtocolDefines.h>
#include <Core/Settings.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ClientInfo.h>
#include <Storages/Distributed/Defines.h>
#include <Storages/Distributed/DistributedAsyncInsertHeader.h>
#include <Common/config_version.h>
#include <Common/logger_useful.h>

#include <city.h>

#include <filesystem>
#include <unistd.h>

using namespace DB;

namespace
{

/// A queue file written by a much older server: just the size of the query, followed by the query text.
/// No settings, no client info at all.
void writeOldestLayout(WriteBuffer & out, const String & query)
{
    writeVarUInt(query.size(), out);
    out.write(query.data(), query.size());
}

/// A queue file written by an older server: the `DBMS_DISTRIBUTED_SIGNATURE_HEADER_OLD_FORMAT` signature,
/// the insert settings in the binary format, and the query text. Still no client info.
void writeOldFormatLayout(WriteBuffer & out, const String & query)
{
    writeVarUInt(DBMS_DISTRIBUTED_SIGNATURE_HEADER_OLD_FORMAT, out);
    Settings settings;
    settings.write(out, SettingsWriteFormat::BINARY);
    writeStringBinary(query, out);
}

/// A queue file in the current layout, carrying an embedded `ClientInfo` with the given version.
void writeCurrentLayout(WriteBuffer & out, const String & query, UInt64 version_major, UInt64 version_minor, UInt64 version_patch)
{
    ClientInfo client_info;
    client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
    client_info.interface = ClientInfo::Interface::TCP;
    client_info.initial_address = Poco::Net::SocketAddress("127.0.0.1:9000");
    client_info.client_version_major = version_major;
    client_info.client_version_minor = version_minor;
    client_info.client_version_patch = version_patch;
    client_info.client_tcp_protocol_version = version_major == 0 ? 0 : DBMS_TCP_PROTOCOL_VERSION;

    WriteBufferFromOwnString header_buf;
    writeVarUInt(DBMS_TCP_PROTOCOL_VERSION, header_buf);
    writeStringBinary(query, header_buf);
    Settings settings;
    settings.write(header_buf);
    client_info.write(header_buf, DBMS_TCP_PROTOCOL_VERSION, /*with_trailing_fields=*/ false);
    header_buf.finalize();

    const std::string_view header = header_buf.stringView();
    writeVarUInt(DBMS_DISTRIBUTED_SIGNATURE_HEADER, out);
    writeStringBinary(header, out);
    writePODBinary(CityHash_v1_0_2::CityHash128(header.data(), header.size()), out);
}

/// Serializes a queue file with `write_layout`, then reads it back through `DistributedAsyncInsertHeader::read`.
template <typename WriteLayout>
DistributedAsyncInsertHeader roundTrip(const String & name, WriteLayout && write_layout)
{
    const auto path = std::filesystem::temp_directory_path()
        / fmt::format("gtest_distributed_async_insert_header.{}.{}.bin", getpid(), name);

    {
        WriteBufferFromFile out(path);
        write_layout(out);
        out.finalize();
    }

    ReadBufferFromFile in(path);
    DistributedAsyncInsertHeader header = DistributedAsyncInsertHeader::read(in, getLogger("gtest_distributed_async_insert_header"));
    std::filesystem::remove(path);
    return header;
}

}

/// A legacy queue file carries no client info at all, so it is left default-constructed - which means the
/// `TCP` interface with a zero version, exactly what `ClientInfo::write` serializes on replay. The zero
/// version must be replaced with this server's own, or the receiving shard would treat the initiator as a
/// pre-23.3 server and apply legacy compatibility downgrades.
TEST(DistributedAsyncInsertHeader, FillsZeroVersionForOldestLayout)
{
    const auto header = roundTrip("oldest", [](WriteBuffer & out) { writeOldestLayout(out, "INSERT INTO t VALUES"); });

    EXPECT_EQ(header.insert_query, "INSERT INTO t VALUES");
    EXPECT_EQ(header.client_info.client_version_major, VERSION_MAJOR);
    EXPECT_EQ(header.client_info.client_version_minor, VERSION_MINOR);
    EXPECT_EQ(header.client_info.client_version_patch, VERSION_PATCH);
    EXPECT_EQ(header.client_info.client_tcp_protocol_version, DBMS_TCP_PROTOCOL_VERSION);
}

TEST(DistributedAsyncInsertHeader, FillsZeroVersionForOldFormatLayout)
{
    const auto header = roundTrip("old_format", [](WriteBuffer & out) { writeOldFormatLayout(out, "INSERT INTO t VALUES"); });

    EXPECT_EQ(header.insert_query, "INSERT INTO t VALUES");
    EXPECT_EQ(header.client_info.client_version_major, VERSION_MAJOR);
    EXPECT_EQ(header.client_info.client_version_minor, VERSION_MINOR);
    EXPECT_EQ(header.client_info.client_version_patch, VERSION_PATCH);
    EXPECT_EQ(header.client_info.client_tcp_protocol_version, DBMS_TCP_PROTOCOL_VERSION);
}

/// The current layout embeds a `ClientInfo`, but a server-initiated query context of an older server (a
/// `Buffer` flush, a streaming consumer, an asynchronous insert flush) had a zero version there as well.
TEST(DistributedAsyncInsertHeader, FillsZeroVersionForCurrentLayout)
{
    const auto header = roundTrip("current_zero", [](WriteBuffer & out) { writeCurrentLayout(out, "INSERT INTO t VALUES", 0, 0, 0); });

    EXPECT_EQ(header.insert_query, "INSERT INTO t VALUES");
    EXPECT_EQ(header.client_info.client_version_major, VERSION_MAJOR);
    EXPECT_EQ(header.client_info.client_version_minor, VERSION_MINOR);
    EXPECT_EQ(header.client_info.client_version_patch, VERSION_PATCH);
    EXPECT_EQ(header.client_info.client_tcp_protocol_version, DBMS_TCP_PROTOCOL_VERSION);
}

/// A non-zero version identifies the real initiator and must be preserved as is.
TEST(DistributedAsyncInsertHeader, KeepsNonZeroVersion)
{
    const auto header = roundTrip("current_non_zero", [](WriteBuffer & out) { writeCurrentLayout(out, "INSERT INTO t VALUES", 23, 3, 1); });

    EXPECT_EQ(header.client_info.client_version_major, 23u);
    EXPECT_EQ(header.client_info.client_version_minor, 3u);
    EXPECT_EQ(header.client_info.client_version_patch, 1u);
    EXPECT_EQ(header.client_info.client_tcp_protocol_version, DBMS_TCP_PROTOCOL_VERSION);
}
