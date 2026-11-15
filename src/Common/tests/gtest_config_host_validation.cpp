#include <Poco/AutoPtr.h>
#include <Poco/DOM/DOMParser.h>
#include <Poco/Util/XMLConfiguration.h>

#include <gtest/gtest.h>

TEST(Common, ConfigHostValidation)
{
    std::string xml(R"CONFIG(<clickhouse>
    <IPv4_1>0.0.0.0</IPv4_1>
    <IPv4_2>192.168.0.1</IPv4_2>
    <IPv4_3>127.0.0.1</IPv4_3>
    <IPv4_4>255.255.255.255</IPv4_4>
    <IPv6_1>2001:0db8:85a3:0000:0000:8a2e:0370:7334</IPv6_1>
    <IPv6_2>2001:DB8::8a2e:370:7334</IPv6_2>
    <IPv6_3>::1</IPv6_3>
    <IPv6_4>::</IPv6_4>
    <Domain_1>www.example.com.</Domain_1>
    <Domain_2>a.co</Domain_2>
    <Domain_3>localhost</Domain_3>
    <Domain_4>xn--fiqs8s.xn--fiqz9s</Domain_4>
    <IPv4_Invalid_1>192.168.1.256</IPv4_Invalid_1>
    <IPv4_Invalid_2>192.168.1.1.1</IPv4_Invalid_2>
    <IPv4_Invalid_3>192.168.1.99999999999999999999</IPv4_Invalid_3>
    <IPv4_Invalid_4>192.168.1.a</IPv4_Invalid_4>
    <IPv6_Invalid_1>2001:0db8:85a3:::8a2e:0370:7334</IPv6_Invalid_1>
    <IPv6_Invalid_2>1200::AB00:1234::2552:7777:1313</IPv6_Invalid_2>
    <IPv6_Invalid_3>1200::AB00:1234:Q000:2552:7777:1313</IPv6_Invalid_3>
    <IPv6_Invalid_4>1200:AB00:1234:2552:7777:1313:FFFF</IPv6_Invalid_4>
    <Domain_Invalid_1>example.com..</Domain_Invalid_1>
    <Domain_Invalid_2>5example.com</Domain_Invalid_2>
    <Domain_Invalid_3>example.com-</Domain_Invalid_3>
    <Domain_Invalid_4>exa_mple.com</Domain_Invalid_4>
</clickhouse>)CONFIG");

    Poco::XML::DOMParser dom_parser;
    Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(document);

    EXPECT_NO_THROW(config->getHost("IPv4_1"));
    EXPECT_NO_THROW(config->getHost("IPv4_2"));
    EXPECT_NO_THROW(config->getHost("IPv4_3"));
    EXPECT_NO_THROW(config->getHost("IPv4_4"));

    EXPECT_NO_THROW(config->getHost("IPv6_1"));
    EXPECT_NO_THROW(config->getHost("IPv6_2"));
    EXPECT_NO_THROW(config->getHost("IPv6_3"));
    EXPECT_NO_THROW(config->getHost("IPv6_4"));

    EXPECT_NO_THROW(config->getHost("Domain_1"));
    EXPECT_NO_THROW(config->getHost("Domain_2"));
    EXPECT_NO_THROW(config->getHost("Domain_3"));
    EXPECT_NO_THROW(config->getHost("Domain_4"));

    EXPECT_THROW(config->getHost("IPv4_Invalid_1"), Poco::SyntaxException);
    EXPECT_THROW(config->getHost("IPv4_Invalid_2"), Poco::SyntaxException);
    EXPECT_THROW(config->getHost("IPv4_Invalid_3"), Poco::SyntaxException);
    EXPECT_THROW(config->getHost("IPv4_Invalid_4"), Poco::SyntaxException);

    EXPECT_THROW(config->getHost("IPv6_Invalid_1"), Poco::SyntaxException);
    EXPECT_THROW(config->getHost("IPv6_Invalid_2"), Poco::SyntaxException);
    EXPECT_THROW(config->getHost("IPv6_Invalid_3"), Poco::SyntaxException);
    EXPECT_THROW(config->getHost("IPv6_Invalid_4"), Poco::SyntaxException);

    EXPECT_THROW(config->getHost("Domain_Invalid_1"), Poco::SyntaxException);
    EXPECT_THROW(config->getHost("Domain_Invalid_2"), Poco::SyntaxException);
    EXPECT_THROW(config->getHost("Domain_Invalid_3"), Poco::SyntaxException);
    EXPECT_THROW(config->getHost("Domain_Invalid_4"), Poco::SyntaxException);
}
