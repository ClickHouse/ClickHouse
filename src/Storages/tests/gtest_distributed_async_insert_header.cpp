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
#include <Common/logger_useful.h>

#include <Poco/Net/SocketAddress.h>

#include <city.h>

#include <filesystem>
#include <unistd.h>

using namespace DB;

namespace
{

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

/// The embedded `ClientInfo` of the queue-file header is frozen: the fields this branch added for wire
/// compatibility (`client_agent`, `is_internal`) must not be serialized into it, or a reader - this
/// branch, an older binary after a downgrade, or a newer one after an upgrade, all of which read that
/// `ClientInfo` with `with_trailing_fields = false` - would misinterpret the fields that follow it,
/// starting with `rows` and `bytes`.
TEST(DistributedAsyncInsertHeader, EmbeddedClientInfoLayoutIsFrozen)
{
    const auto header = roundTrip("frozen_embedded_layout", [](WriteBuffer & out)
    {
        ClientInfo client_info;
        client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
        client_info.interface = ClientInfo::Interface::TCP;
        client_info.initial_address = std::make_shared<Poco::Net::SocketAddress>("127.0.0.1:9000");
        /// Non-default values, so that serializing them into the embedded layout would be visible.
        client_info.client_agent = "some-agent";
        client_info.is_internal = true;

        WriteBufferFromOwnString header_buf;
        writeVarUInt(DBMS_TCP_PROTOCOL_VERSION, header_buf);
        writeStringBinary("INSERT INTO t VALUES", header_buf);
        Settings settings;
        settings.write(header_buf);
        client_info.write(header_buf, DBMS_TCP_PROTOCOL_VERSION, /*with_trailing_fields=*/ false);
        writeVarUInt(123, header_buf);
        writeVarUInt(456, header_buf);
        writeStringBinary("", header_buf);
        header_buf.finalize();

        const std::string_view header_data = header_buf.stringView();
        writeVarUInt(DBMS_DISTRIBUTED_SIGNATURE_HEADER, out);
        writeStringBinary(header_data, out);
        writePODBinary(CityHash_v1_0_2::CityHash128(header_data.data(), header_data.size()), out);
    });

    /// `rows` and `bytes` immediately follow the embedded `ClientInfo`; they parse correctly only if the
    /// two fields were suppressed from it.
    EXPECT_EQ(header.rows, 123u);
    EXPECT_EQ(header.bytes, 456u);
    EXPECT_TRUE(header.client_info.client_agent.empty());
    EXPECT_FALSE(header.client_info.is_internal);
}
