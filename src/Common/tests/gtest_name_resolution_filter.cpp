#include <Common/DNSResolver.h>
#include <Common/Exception.h>
#include <Common/NameResolutionFilter.h>

#include <Poco/AutoPtr.h>
#include <Poco/DOM/DOMParser.h>
#include <Poco/Util/XMLConfiguration.h>

#include <gtest/gtest.h>

#include <fmt/format.h>

#include <netdb.h>
#include <sys/socket.h>

namespace DB::ErrorCodes
{
extern const int UNACCEPTABLE_URL;
}

namespace
{

Poco::AutoPtr<Poco::Util::XMLConfiguration> parseConfig(const std::string & xml)
{
    Poco::XML::DOMParser dom_parser;
    Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
    return new Poco::Util::XMLConfiguration(document);
}

void configure(bool check_name_resolution)
{
    auto config = parseConfig(fmt::format(R"CONFIG(<clickhouse>
    <remote_url_allow_hosts>
        <host>127.0.0.1:9000</host>
        <host_regexp>^allowed\.test$</host_regexp>
        <check_name_resolution>{}</check_name_resolution>
    </remote_url_allow_hosts>
</clickhouse>)CONFIG", check_name_resolution));
    DB::NameResolutionFilter::setValuesFromConfig(*config);
}

/// Resolves a host as a client library does: through `getaddrinfo` of the C library. Only literal addresses are
/// used, so nothing goes to DNS.
int resolveWithCLibrary(const char * host)
{
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_NUMERICHOST;

    addrinfo * result = nullptr;
    const int error = getaddrinfo(host, "9000", &hints, &result);
    if (result)
        freeaddrinfo(result);
    return error;
}

class NameResolutionFilterTest : public ::testing::Test
{
protected:
    void TearDown() override
    {
        DB::NameResolutionFilter::setValuesFromConfig(*parseConfig("<clickhouse/>"));
    }
};

}

TEST_F(NameResolutionFilterTest, DisabledByDefault)
{
    configure(false);
    EXPECT_FALSE(DB::NameResolutionFilter::isEnabled());
    EXPECT_TRUE(DB::NameResolutionFilter::isAllowed("127.0.0.2"));
    EXPECT_NO_THROW(DB::DNSResolver::instance().resolveHost("127.0.0.2"));
#if defined(OS_LINUX)
    EXPECT_EQ(resolveWithCLibrary("127.0.0.2"), 0);
#endif
}

TEST_F(NameResolutionFilterTest, Check)
{
    configure(true);
    EXPECT_TRUE(DB::NameResolutionFilter::isEnabled());

    EXPECT_TRUE(DB::NameResolutionFilter::isAllowed("127.0.0.1"));
    EXPECT_TRUE(DB::NameResolutionFilter::isAllowed("allowed.test"));
    EXPECT_TRUE(DB::NameResolutionFilter::isAllowed("ALLOWED.test"));
    EXPECT_FALSE(DB::NameResolutionFilter::isAllowed("127.0.0.2"));
    EXPECT_FALSE(DB::NameResolutionFilter::isAllowed("allowed.test.evil"));

    /// Literal addresses do not reach `getaddrinfo` in `DNSResolver`, and its cache is not trusted either.
    EXPECT_NO_THROW(DB::DNSResolver::instance().resolveHost("127.0.0.1"));
    try
    {
        DB::DNSResolver::instance().resolveHost("127.0.0.2");
        FAIL() << "Expected UNACCEPTABLE_URL";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::UNACCEPTABLE_URL);
    }
    EXPECT_THROW(DB::DNSResolver::instance().resolveAddress("[::1]", 9000), DB::Exception);
}

#if defined(OS_LINUX)
TEST_F(NameResolutionFilterTest, CLibrary)
{
    configure(true);
    EXPECT_EQ(resolveWithCLibrary("127.0.0.1"), 0);
    EXPECT_EQ(resolveWithCLibrary("127.0.0.2"), EAI_NONAME);
    EXPECT_EQ(resolveWithCLibrary("::1"), EAI_NONAME);
}
#endif

TEST_F(NameResolutionFilterTest, InternalScope)
{
    configure(true);
    {
        DB::NameResolutionFilter::InternalScope internal_scope;
        EXPECT_TRUE(DB::NameResolutionFilter::isAllowed("127.0.0.2"));
        EXPECT_NO_THROW(DB::DNSResolver::instance().resolveHost("127.0.0.2"));
#if defined(OS_LINUX)
        EXPECT_EQ(resolveWithCLibrary("127.0.0.2"), 0);
#endif
    }
    /// The address resolved inside the scope is in the cache of `DNSResolver` now, which must not let it through.
    EXPECT_FALSE(DB::NameResolutionFilter::isAllowed("127.0.0.2"));
    EXPECT_THROW(DB::DNSResolver::instance().resolveHost("127.0.0.2"), DB::Exception);
#if defined(OS_LINUX)
    EXPECT_EQ(resolveWithCLibrary("127.0.0.2"), EAI_NONAME);
#endif
}

TEST_F(NameResolutionFilterTest, InternalHost)
{
    configure(true);
    EXPECT_FALSE(DB::NameResolutionFilter::isAllowed("127.0.0.3"));
    DB::NameResolutionFilter::addInternalHost("127.0.0.3");
    EXPECT_TRUE(DB::NameResolutionFilter::isAllowed("127.0.0.3"));
    EXPECT_TRUE(DB::NameResolutionFilter::isAllowed("[127.0.0.3]"));
#if defined(OS_LINUX)
    EXPECT_EQ(resolveWithCLibrary("127.0.0.3"), 0);
#endif
}
