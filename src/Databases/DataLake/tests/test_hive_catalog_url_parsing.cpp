#include <Databases/DataLake/HiveCatalog.h>
#include <gtest/gtest.h>
#include <Common/Exception.h>
#include <base/types.h>

namespace DB::ErrorCodes
{
extern const int DATALAKE_DATABASE_ERROR;
}

namespace DB::DataLake::Test
{

class HiveCatalogUrlParsingTest : public ::testing::Test
{
protected:
    void SetUp() override {}
    void TearDown() override {}
};

std::pair<String, Int32> testParseHostPort(const String & url)
{
    auto protocol_sep = url.find("://");
    if (protocol_sep == String::npos)
        throw DB::Exception(DB::ErrorCodes::DATALAKE_DATABASE_ERROR, "Invalid URL format: missing protocol separator '://'");

    size_t start = protocol_sep + 3;

    auto colon_pos = url.find(':', start);
    if (colon_pos == String::npos)
        throw DB::Exception(DB::ErrorCodes::DATALAKE_DATABASE_ERROR, "Invalid URL format: missing port number");

    auto slash_pos = url.find('/', start);

    String host = url.substr(start, colon_pos - start);

    size_t port_end = (slash_pos != String::npos) ? slash_pos : url.size();
    String port_str = url.substr(colon_pos + 1, port_end - colon_pos - 1);

    if (port_str.empty() || !std::all_of(port_str.begin(), port_str.end(), ::isdigit))
        throw DB::Exception(DB::ErrorCodes::DATALAKE_DATABASE_ERROR, "Invalid port number: '{}'", port_str);

    try
    {
        Int32 port = std::stoi(port_str);
        if (port <= 0 || port > 65535)
            throw DB::Exception(DB::ErrorCodes::DATALAKE_DATABASE_ERROR, "Port number out of valid range (1-65535): {}", port);
        return {host, port};
    }
    catch (const std::out_of_range&)
    {
        throw DB::Exception(DB::ErrorCodes::DATALAKE_DATABASE_ERROR, "Invalid port number format: {}", port_str);
    }
    catch (const std::invalid_argument&)
    {
        throw DB::Exception(DB::ErrorCodes::DATALAKE_DATABASE_ERROR, "Invalid port number: '{}'", port_str);
    }
}

TEST_F(HiveCatalogUrlParsingTest, ValidUrls)
{
    auto result = testParseHostPort("thrift://hive-metastore:9083");
    EXPECT_EQ(result.first, "hive-metastore");
    EXPECT_EQ(result.second, 9083);

    result = testParseHostPort("thrift://hive-metastore:9083/metastore");
    EXPECT_EQ(result.first, "hive-metastore");
    EXPECT_EQ(result.second, 9083);

    result = testParseHostPort("thrift://hive-metastore:9083/metastore/db/table");
    EXPECT_EQ(result.first, "hive-metastore");
    EXPECT_EQ(result.second, 9083);

    result = testParseHostPort("thrift://hive-metastore:9083/metastore?param=value");
    EXPECT_EQ(result.first, "hive-metastore");
    EXPECT_EQ(result.second, 9083);
}

TEST_F(HiveCatalogUrlParsingTest, MissingProtocolSeparator)
{
    EXPECT_THROW({
        testParseHostPort("thrift:hive-metastore:9083");
    }, DB::Exception);

    EXPECT_THROW({
        testParseHostPort("hive-metastore:9083");
    }, DB::Exception);
}

TEST_F(HiveCatalogUrlParsingTest, MissingPort)
{
    EXPECT_THROW({
        testParseHostPort("thrift://hive-metastore");
    }, DB::Exception);

    EXPECT_THROW({
        testParseHostPort("thrift://hive-metastore/");
    }, DB::Exception);
}

TEST_F(HiveCatalogUrlParsingTest, InvalidPortFormats)
{
    EXPECT_THROW({
        testParseHostPort("thrift://hive-metastore:abc");
    }, DB::Exception);

    EXPECT_THROW({
        testParseHostPort("thrift://hive-metastore:");
    }, DB::Exception);

    EXPECT_THROW({
        testParseHostPort("thrift://hive-metastore:123abc");
    }, DB::Exception);
}

TEST_F(HiveCatalogUrlParsingTest, PortRangeValidation)
{
    EXPECT_THROW({
        testParseHostPort("thrift://hive-metastore:0");
    }, DB::Exception);

    auto result = testParseHostPort("thrift://hive-metastore:1");
    EXPECT_EQ(result.first, "hive-metastore");
    EXPECT_EQ(result.second, 1);

    result = testParseHostPort("thrift://hive-metastore:65535");
    EXPECT_EQ(result.first, "hive-metastore");
    EXPECT_EQ(result.second, 65535);

    EXPECT_THROW({
        testParseHostPort("thrift://hive-metastore:65536");
    }, DB::Exception);

    EXPECT_THROW({
        testParseHostPort("thrift://hive-metastore:999999");
    }, DB::Exception);
}

TEST_F(HiveCatalogUrlParsingTest, EdgeCases)
{
    EXPECT_THROW({
        testParseHostPort("");
    }, DB::Exception);

    EXPECT_THROW({
        testParseHostPort("thrift://");
    }, DB::Exception);

    EXPECT_THROW({
        testParseHostPort("thrift://:");
    }, DB::Exception);
}

}

int main(int argc, char ** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
