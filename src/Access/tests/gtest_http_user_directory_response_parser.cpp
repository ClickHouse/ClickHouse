#include <gtest/gtest.h>

#include <sstream>

#include <Access/HTTPUserDirectoryResponseParser.h>
#include <Common/Exception.h>
#include <Poco/Net/HTTPResponse.h>

using namespace DB;

namespace
{

HTTPUserDirectoryResponseParser::Result parseBody(Poco::Net::HTTPResponse::HTTPStatus status, const std::string & body)
{
    Poco::Net::HTTPResponse response;
    response.setStatus(status);
    std::istringstream body_stream(body);
    return HTTPUserDirectoryResponseParser{}.parse(response, &body_stream);
}

}

TEST(HTTPUserDirectoryResponseParser, FullValidResponse)
{
    auto result = parseBody(Poco::Net::HTTPResponse::HTTP_OK,
        R"({"settings": {"max_threads": "4"}, "roles": ["reader", "analyst"], "valid_until": 4102444800})");
    EXPECT_EQ(result.status, HTTPUserDirectoryResponseParser::Result::Status::Ok);
    ASSERT_EQ(result.role_names.size(), 2u);
    EXPECT_EQ(result.role_names[0], "reader");
    EXPECT_EQ(result.role_names[1], "analyst");
    ASSERT_EQ(result.settings.size(), 1u);
    EXPECT_EQ(result.settings[0].name, "max_threads");
    EXPECT_EQ(result.valid_until, 4102444800);
}

TEST(HTTPUserDirectoryResponseParser, EmptyBodyIsOk)
{
    auto result = parseBody(Poco::Net::HTTPResponse::HTTP_OK, "");
    EXPECT_EQ(result.status, HTTPUserDirectoryResponseParser::Result::Status::Ok);
    EXPECT_TRUE(result.role_names.empty());
    EXPECT_TRUE(result.settings.empty());
    EXPECT_EQ(result.valid_until, 0);
}

TEST(HTTPUserDirectoryResponseParser, WhitespaceBodyIsOk)
{
    auto result = parseBody(Poco::Net::HTTPResponse::HTTP_OK, "  \n\t ");
    EXPECT_EQ(result.status, HTTPUserDirectoryResponseParser::Result::Status::Ok);
}

TEST(HTTPUserDirectoryResponseParser, EmptyObjectIsOk)
{
    auto result = parseBody(Poco::Net::HTTPResponse::HTTP_OK, "{}");
    EXPECT_EQ(result.status, HTTPUserDirectoryResponseParser::Result::Status::Ok);
    EXPECT_TRUE(result.role_names.empty());
}

TEST(HTTPUserDirectoryResponseParser, EmptyRolesArrayIsOk)
{
    auto result = parseBody(Poco::Net::HTTPResponse::HTTP_OK, R"({"roles": []})");
    EXPECT_EQ(result.status, HTTPUserDirectoryResponseParser::Result::Status::Ok);
    EXPECT_TRUE(result.role_names.empty());
}

TEST(HTTPUserDirectoryResponseParser, ValidUntilZeroMeansNoExpiry)
{
    auto result = parseBody(Poco::Net::HTTPResponse::HTTP_OK, R"({"valid_until": 0})");
    EXPECT_EQ(result.valid_until, 0);
}

TEST(HTTPUserDirectoryResponseParser, UnknownTopLevelFieldsIgnored)
{
    auto result = parseBody(Poco::Net::HTTPResponse::HTTP_OK, R"({"roles": ["r"], "future_field": {"x": 1}})");
    EXPECT_EQ(result.status, HTTPUserDirectoryResponseParser::Result::Status::Ok);
    ASSERT_EQ(result.role_names.size(), 1u);
}

TEST(HTTPUserDirectoryResponseParser, NotFoundStatus)
{
    auto result = parseBody(Poco::Net::HTTPResponse::HTTP_NOT_FOUND, "");
    EXPECT_EQ(result.status, HTTPUserDirectoryResponseParser::Result::Status::UserNotFound);
}

TEST(HTTPUserDirectoryResponseParser, RejectedAndErrorStatusesThrow)
{
    EXPECT_THROW(parseBody(Poco::Net::HTTPResponse::HTTP_UNAUTHORIZED, ""), Exception);
    EXPECT_THROW(parseBody(Poco::Net::HTTPResponse::HTTP_FORBIDDEN, ""), Exception);
    EXPECT_THROW(parseBody(Poco::Net::HTTPResponse::HTTP_TOO_MANY_REQUESTS, ""), Exception);
    EXPECT_THROW(parseBody(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR, ""), Exception);
    EXPECT_THROW(parseBody(Poco::Net::HTTPResponse::HTTP_FOUND, ""), Exception);
}

TEST(HTTPUserDirectoryResponseParser, OversizedBodyThrows)
{
    /// The parser caps the response body at 1 MiB (compromised-helper hardening).
    std::string big_body = R"({"roles": [")" + std::string(2 * 1024 * 1024, 'x') + R"("]})";
    EXPECT_THROW(parseBody(Poco::Net::HTTPResponse::HTTP_OK, big_body), Exception);
}

TEST(HTTPUserDirectoryResponseParser, MalformedMetadataThrows)
{
    /// Malformed JSON.
    EXPECT_THROW(parseBody(Poco::Net::HTTPResponse::HTTP_OK, "{not json"), Exception);
    /// Top level is not an object.
    EXPECT_THROW(parseBody(Poco::Net::HTTPResponse::HTTP_OK, R"(["reader"])"), Exception);
    /// roles is not an array.
    EXPECT_THROW(parseBody(Poco::Net::HTTPResponse::HTTP_OK, R"({"roles": "reader"})"), Exception);
    /// roles element is not a string.
    EXPECT_THROW(parseBody(Poco::Net::HTTPResponse::HTTP_OK, R"({"roles": [1]})"), Exception);
    /// settings is not an object.
    EXPECT_THROW(parseBody(Poco::Net::HTTPResponse::HTTP_OK, R"({"settings": ["max_threads"]})"), Exception);
    /// Unknown setting name must fail (strict), unlike the legacy parser.
    EXPECT_THROW(parseBody(Poco::Net::HTTPResponse::HTTP_OK, R"({"settings": {"no_such_setting_xyz": "1"}})"), Exception);
    /// valid_until: negative, fractional, string, boolean.
    EXPECT_THROW(parseBody(Poco::Net::HTTPResponse::HTTP_OK, R"({"valid_until": -5})"), Exception);
    EXPECT_THROW(parseBody(Poco::Net::HTTPResponse::HTTP_OK, R"({"valid_until": 123.5})"), Exception);
    EXPECT_THROW(parseBody(Poco::Net::HTTPResponse::HTTP_OK, R"({"valid_until": "123"})"), Exception);
    /// Poco::Dynamic::Var::isInteger() reports true for a boolean value, so `isInteger` alone is not
    /// enough to reject `valid_until: true` - must also check `isBoolean()`.
    EXPECT_THROW(parseBody(Poco::Net::HTTPResponse::HTTP_OK, R"({"valid_until": true})"), Exception);
}
