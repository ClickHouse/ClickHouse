#include <Common/RemoteHostFilter.h>
#include <Common/Exception.h>

#include <Poco/AutoPtr.h>
#include <Poco/DOM/DOMParser.h>
#include <Poco/URI.h>
#include <Poco/Util/XMLConfiguration.h>

#include <gtest/gtest.h>

#include <fmt/format.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
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

}

TEST(RemoteHostFilter, MalformedReloadKeepsPreviousValues)
{
    DB::RemoteHostFilter filter;

    auto good_config = parseConfig(R"CONFIG(<clickhouse>
    <remote_url_allow_hosts>
        <host>allowed.com</host>
        <host_regexp>^.*\.allowed-regexp\.com$</host_regexp>
        <s3_bucket>s3.example.com/my-bucket</s3_bucket>
    </remote_url_allow_hosts>
</clickhouse>)CONFIG");
    filter.setValuesFromConfig(*good_config);

    auto check_previous_values = [&]
    {
        EXPECT_NO_THROW(filter.checkURL(Poco::URI("http://allowed.com/path")));
        EXPECT_NO_THROW(filter.checkURL(Poco::URI("http://sub.allowed-regexp.com/path")));
        EXPECT_TRUE(filter.isBucketAllowed("s3.example.com", 443, "my-bucket"));
        EXPECT_FALSE(filter.isBucketAllowed("s3.example.com", 443, "other-bucket"));
        EXPECT_THROW(filter.checkURL(Poco::URI("http://new.com/path")), DB::Exception);
    };
    check_previous_values();

    auto bad_config = parseConfig(R"CONFIG(<clickhouse>
    <remote_url_allow_hosts>
        <host>new.com</host>
        <s3_bucket>no-bucket</s3_bucket>
        <host>allowed.com</host>
        <host_regexp>^.*\.allowed-regexp\.com$</host_regexp>
        <s3_bucket>s3.example.com/my-bucket</s3_bucket>
    </remote_url_allow_hosts>
</clickhouse>)CONFIG");
    EXPECT_THROW(filter.setValuesFromConfig(*bad_config), DB::Exception);
    check_previous_values();

    auto empty_config = parseConfig("<clickhouse></clickhouse>");
    filter.setValuesFromConfig(*empty_config);
    EXPECT_NO_THROW(filter.checkURL(Poco::URI("http://new.com/path")));
    EXPECT_TRUE(filter.isBucketAllowed("s3.example.com", 443, "other-bucket"));
}

TEST(RemoteHostFilter, S3BucketEntryFormat)
{
    const auto config_with_entry = [](const std::string & entry)
    {
        return parseConfig(fmt::format(
            "<clickhouse><remote_url_allow_hosts><s3_bucket>{}</s3_bucket></remote_url_allow_hosts></clickhouse>", entry));
    };

    for (const std::string & bad : {"no-bucket", "/bucket", "s3.example.com/", "s3.example.com/bucket/key", "https://s3.example.com/bucket"})
    {
        DB::RemoteHostFilter filter;
        try
        {
            filter.setValuesFromConfig(*config_with_entry(bad));
            FAIL() << "accepted malformed <s3_bucket> " << bad;
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), DB::ErrorCodes::BAD_ARGUMENTS) << bad;
        }
    }

    for (const std::string & good : {"s3.example.com/bucket", "s3.example.com:443/bucket", "minio1:9001/root", "127.0.0.1:9001/root"})
    {
        DB::RemoteHostFilter filter;
        EXPECT_NO_THROW(filter.setValuesFromConfig(*config_with_entry(good))) << good;
    }
}

TEST(RemoteHostFilter, S3BucketEntryPort)
{
    DB::RemoteHostFilter filter;
    filter.setValuesFromConfig(*parseConfig(R"CONFIG(<clickhouse>
    <remote_url_allow_hosts>
        <s3_bucket>s3.example.com/any-port</s3_bucket>
        <s3_bucket>minio1:9001/one-port</s3_bucket>
    </remote_url_allow_hosts>
</clickhouse>)CONFIG"));

    /// An entry without a port matches the bucket on every port, like a <host> entry without a port.
    EXPECT_TRUE(filter.isBucketAllowed("s3.example.com", 443, "any-port"));
    EXPECT_TRUE(filter.isBucketAllowed("s3.example.com", 80, "any-port"));
    EXPECT_TRUE(filter.isBucketAllowed("s3.example.com", 9001, "any-port"));

    /// An entry with a port matches that port only.
    EXPECT_TRUE(filter.isBucketAllowed("minio1", 9001, "one-port"));
    EXPECT_FALSE(filter.isBucketAllowed("minio1", 9002, "one-port"));
    EXPECT_FALSE(filter.isBucketAllowed("minio1", 80, "one-port"));

    /// The host and the bucket are compared exactly: no prefix of the bucket, no other host.
    EXPECT_FALSE(filter.isBucketAllowed("s3.example.com", 443, "any"));
    EXPECT_FALSE(filter.isBucketAllowed("s3.example.com", 443, "any-port-2"));
    EXPECT_FALSE(filter.isBucketAllowed("other.example.com", 443, "any-port"));

    /// A bucket entry does not allow the host itself, so a non-S3 URL to that host is still rejected.
    try
    {
        filter.checkURL(Poco::URI("https://s3.example.com/any-port/key"));
        FAIL() << "a bucket entry allowed the host for a plain URL";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::UNACCEPTABLE_URL);
    }
}

TEST(RemoteHostFilter, AbsentSectionAllowsAllAndEmptySectionRejectsAll)
{
    DB::RemoteHostFilter filter;

    /// No <remote_url_allow_hosts> at all: the filter is off.
    filter.setValuesFromConfig(*parseConfig("<clickhouse></clickhouse>"));
    EXPECT_NO_THROW(filter.checkURL(Poco::URI("https://anything.example.com/")));
    EXPECT_TRUE(filter.isBucketAllowed("s3.example.com", 443, "any"));

    /// A present but empty section: the filter is on and the list is empty, so everything is rejected.
    filter.setValuesFromConfig(*parseConfig("<clickhouse><remote_url_allow_hosts></remote_url_allow_hosts></clickhouse>"));
    EXPECT_THROW(filter.checkURL(Poco::URI("https://anything.example.com/")), DB::Exception);
    EXPECT_FALSE(filter.isBucketAllowed("s3.example.com", 443, "any"));
}
