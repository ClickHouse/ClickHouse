#include <gtest/gtest.h>

#include <Core/ProtocolDefines.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/TablesStatus.h>
#include <Common/Exception.h>

#include <optional>
#include <string>

namespace DB::ErrorCodes
{
    extern const int TOO_LARGE_ARRAY_SIZE;
    extern const int TOO_LARGE_STRING_SIZE;
}

using namespace DB;

namespace
{

/// A `TablesStatusRequest` body as a hostile peer could put it on the wire. Built by hand rather
/// than with `TablesStatusRequest::write`, so that a table name can declare a length that is never
/// followed by its bytes - which is the case the name bound exists for.
std::string requestBody(size_t table_count, std::optional<size_t> declared_name_size = {})
{
    WriteBufferFromOwnString out;
    writeVarUInt(table_count, out);
    for (size_t i = 0; i < table_count; ++i)
    {
        writeStringBinary(std::string("default"), out);
        if (declared_name_size.has_value())
            writeVarUInt(*declared_name_size, out);
        else
            writeStringBinary("t" + std::to_string(i), out);
    }
    out.finalize();
    return out.str();
}

TablesStatusRequest readBody(const std::string & body, const TablesStatusRequestLimits & limits)
{
    ReadBufferFromString in(body);
    TablesStatusRequest request;
    request.read(in, DBMS_MIN_REVISION_WITH_TABLES_STATUS, limits);
    return request;
}

constexpr TablesStatusRequestLimits GENERIC_LIMITS{DEFAULT_MAX_STRING_SIZE, DEFAULT_MAX_STRING_SIZE};

}

/// An interserver request is deserialized before the peer has proven knowledge of the cluster
/// secret, so `INTERSERVER_TABLES_STATUS_REQUEST_LIMITS` bounds it. At the cap it still parses - the
/// bound must not reject what a legitimate peer could send.
TEST(TablesStatusRequestRead, AcceptsTheInterserverTableCap)
{
    const auto & limits = INTERSERVER_TABLES_STATUS_REQUEST_LIMITS;
    auto request = readBody(requestBody(limits.max_tables), limits);
    EXPECT_EQ(request.tables.size(), limits.max_tables);
}

TEST(TablesStatusRequestRead, RejectsOneTableOverTheInterserverCap)
{
    const auto & limits = INTERSERVER_TABLES_STATUS_REQUEST_LIMITS;
    /// The count is checked before any name is read, so the body does not have to be complete.
    try
    {
        readBody(requestBody(limits.max_tables + 1), limits);
        FAIL() << "a request over the interserver table cap was accepted";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::TOO_LARGE_ARRAY_SIZE);
    }
}

/// The table count alone does not bound the request: `readStringBinary` reserves the declared size
/// of a name before reading its bytes. Here the declared length is followed by no bytes at all, so
/// a server honouring the cap refuses on the length, while one that does not would reserve it and
/// only then fail on the truncated buffer.
TEST(TablesStatusRequestRead, RejectsANameOverTheInterserverCap)
{
    const auto & limits = INTERSERVER_TABLES_STATUS_REQUEST_LIMITS;
    try
    {
        readBody(requestBody(1, limits.max_name_size + 1), limits);
        FAIL() << "a table name declared over the interserver cap was accepted";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::TOO_LARGE_STRING_SIZE);
    }
}

/// The cap applies to the interserver path only. An ordinary authenticated client keeps the generic
/// limits, so the very request the cap rejects must still parse for it.
TEST(TablesStatusRequestRead, OrdinaryClientIsNotBoundedByTheInterserverCap)
{
    const size_t over_cap = INTERSERVER_TABLES_STATUS_REQUEST_LIMITS.max_tables + 1;
    auto request = readBody(requestBody(over_cap), GENERIC_LIMITS);
    EXPECT_EQ(request.tables.size(), over_cap);

    const size_t over_name_cap = INTERSERVER_TABLES_STATUS_REQUEST_LIMITS.max_name_size + 1;
    WriteBufferFromOwnString out;
    writeVarUInt(static_cast<size_t>(1), out);
    writeStringBinary(std::string("default"), out);
    writeStringBinary(std::string(over_name_cap, 'x'), out);
    out.finalize();
    auto long_name = readBody(out.str(), GENERIC_LIMITS);
    ASSERT_EQ(long_name.tables.size(), 1u);
    EXPECT_EQ(long_name.tables.begin()->table.size(), over_name_cap);
}
