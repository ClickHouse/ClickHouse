#include <gtest/gtest.h>

#include <Core/ProtocolDefines.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/TablesStatus.h>
#include <Common/Exception.h>

#include <string>

namespace DB::ErrorCodes
{
    extern const int TOO_LARGE_ARRAY_SIZE;
}

using namespace DB;

namespace
{

/// A `TablesStatusRequest` body, built by hand rather than with `TablesStatusRequest::write` so that
/// the declared table count can exceed what a legitimate peer would send.
std::string requestBody(size_t table_count, const std::string & table_name = "t")
{
    WriteBufferFromOwnString out;
    writeVarUInt(table_count, out);
    for (size_t i = 0; i < table_count; ++i)
    {
        writeStringBinary(std::string("default"), out);
        writeStringBinary(table_name + std::to_string(i), out);
    }
    out.finalize();
    return out.str();
}

TablesStatusRequest readBody(const std::string & body, TablesStatusRequestSource source)
{
    ReadBufferFromString in(body);
    TablesStatusRequest request;
    request.read(in, DBMS_MIN_REVISION_WITH_TABLES_STATUS, source);
    return request;
}

}

/// An interserver request is deserialized before the peer has proven knowledge of the cluster
/// secret, so `read` bounds how many tables it may ask about. At the bound it still parses - the
/// bound must not reject what a legitimate peer could send.
TEST(TablesStatusRequestRead, AcceptsTheInterserverTableBound)
{
    auto request = readBody(
        requestBody(MAX_TABLES_IN_INTERSERVER_STATUS_REQUEST), TablesStatusRequestSource::InterserverPeer);
    EXPECT_EQ(request.tables.size(), MAX_TABLES_IN_INTERSERVER_STATUS_REQUEST);
}

TEST(TablesStatusRequestRead, RejectsOneTableOverTheInterserverBound)
{
    /// The count is checked before any name is read, so the body does not have to be complete.
    try
    {
        readBody(
            requestBody(MAX_TABLES_IN_INTERSERVER_STATUS_REQUEST + 1), TablesStatusRequestSource::InterserverPeer);
        FAIL() << "a request over the interserver table bound was accepted";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::TOO_LARGE_ARRAY_SIZE);
    }
}

/// The bound is on the table count only; the names keep the generic string bound, read growing so a
/// declared size is not an allocation by itself. There is deliberately no ceiling on how long a name
/// may be, because ClickHouse does not have one to borrow - `IDatabase::checkTableNameLength` is a
/// no-op unless the database is a `DatabaseOnDisk` - and the name comes from the `Distributed`
/// engine arguments, so a ceiling here could reject a legitimate request. This pins that: a name far
/// longer than any filesystem-backed database would allow must still parse.
TEST(TablesStatusRequestRead, DoesNotCapHowLongATableNameMayBe)
{
    const std::string long_name(1024 * 1024, 'x');
    auto request = readBody(requestBody(1, long_name), TablesStatusRequestSource::InterserverPeer);
    ASSERT_EQ(request.tables.size(), 1u);
    EXPECT_EQ(request.tables.begin()->table.size(), long_name.size() + 1);
}

/// The bound follows from the source, so that no call site can hand an interserver connection the
/// generic bound. This pins the other half of that mapping: as `Client`, the very request the
/// interserver bound rejects must still parse.
TEST(TablesStatusRequestRead, ClientIsNotBoundedByTheInterserverBound)
{
    const size_t over_bound = MAX_TABLES_IN_INTERSERVER_STATUS_REQUEST + 1;
    auto request = readBody(requestBody(over_bound), TablesStatusRequestSource::Client);
    EXPECT_EQ(request.tables.size(), over_bound);
}
