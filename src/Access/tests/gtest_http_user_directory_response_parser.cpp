#include <gtest/gtest.h>

#include <map>
#include <sstream>
#include <stdexcept>
#include <streambuf>

#include <Access/HTTPUserDirectoryResponseParser.h>
#include <Common/Exception.h>
#include <Poco/Net/HTTPResponse.h>

using namespace DB;

namespace
{

/// A length-delimited response, as every real helper sends: Content-Length matches the body.
HTTPUserDirectoryResponseParser::Result parseBody(Poco::Net::HTTPResponse::HTTPStatus status, const std::string & body)
{
    Poco::Net::HTTPResponse response;
    response.setStatus(status);
    response.setContentLength(static_cast<std::streamsize>(body.size()));
    std::istringstream body_stream(body);
    return HTTPUserDirectoryResponseParser{}.parse(response, &body_stream);
}

/// A stream buffer whose every read fails, standing in for a stream implementation that reports a
/// broken transfer through `badbit` instead of an exception.
class FailingStreamBuf : public std::streambuf
{
protected:
    int_type underflow() override { throw std::runtime_error("broken transfer"); }
};

}

TEST(HTTPUserDirectoryResponseParser, OkResponseMustBeLengthDelimited)
{
    /// Neither Content-Length nor chunked: a truncation would be undetectable, so the 200 is rejected
    /// whether the body is empty or valid.
    for (const std::string & body : {std::string{}, std::string{"{}"}})
    {
        Poco::Net::HTTPResponse response;
        response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
        std::istringstream body_stream(body);
        EXPECT_THROW(HTTPUserDirectoryResponseParser{}.parse(response, &body_stream), Exception) << body;
    }
    /// Content-Length: 0 keeps the "empty 200 means {}" contract.
    EXPECT_EQ(parseBody(Poco::Net::HTTPResponse::HTTP_OK, "").status, HTTPUserDirectoryResponseParser::Result::Status::Ok);
    /// Chunked framing is accepted as well.
    {
        Poco::Net::HTTPResponse response;
        response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
        response.setChunkedTransferEncoding(true);
        std::istringstream body_stream(R"({"roles": ["reader"]})");
        auto result = HTTPUserDirectoryResponseParser{}.parse(response, &body_stream);
        EXPECT_EQ(result.status, HTTPUserDirectoryResponseParser::Result::Status::Ok);
        ASSERT_EQ(result.role_names.size(), 1u);
    }
    /// A 404 does not need verifiable framing: the status is the result.
    {
        Poco::Net::HTTPResponse response;
        response.setStatus(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
        std::istringstream body_stream("");
        EXPECT_EQ(HTTPUserDirectoryResponseParser{}.parse(response, &body_stream).status,
            HTTPUserDirectoryResponseParser::Result::Status::UserNotFound);
    }
}

TEST(HTTPUserDirectoryResponseParser, BrokenBodyStreamFailsClosed)
{
    /// With the default exception mask, `istream::read` turns a streambuf exception into `badbit` and
    /// returns nothing; the parser must not mistake that for an empty (valid) body.
    for (auto status : {Poco::Net::HTTPResponse::HTTP_OK, Poco::Net::HTTPResponse::HTTP_NOT_FOUND})
    {
        Poco::Net::HTTPResponse response;
        response.setStatus(status);
        response.setContentLength(100);
        FailingStreamBuf failing_buf;
        std::istream body_stream(&failing_buf);
        EXPECT_THROW(HTTPUserDirectoryResponseParser{}.parse(response, &body_stream), Exception) << static_cast<int>(status);
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

TEST(HTTPUserDirectoryResponseParser, NotFoundBodyIsDrained)
{
    /// A 404 body is consumed to the end so the pooled connection stays reusable.
    Poco::Net::HTTPResponse response;
    response.setStatus(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
    std::istringstream body_stream(R"({"error": "no such user"})");
    auto result = HTTPUserDirectoryResponseParser{}.parse(response, &body_stream);
    EXPECT_EQ(result.status, HTTPUserDirectoryResponseParser::Result::Status::UserNotFound);
    EXPECT_TRUE(body_stream.eof());
}

TEST(HTTPUserDirectoryResponseParser, OversizedNotFoundBodyThrows)
{
    EXPECT_THROW(parseBody(Poco::Net::HTTPResponse::HTTP_NOT_FOUND, std::string(2 * 1024 * 1024, 'x')), Exception);
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
    /// A setting value must be a JSON scalar.
    EXPECT_THROW(parseBody(Poco::Net::HTTPResponse::HTTP_OK, R"({"settings": {"max_threads": [4]}})"), Exception);
    EXPECT_THROW(parseBody(Poco::Net::HTTPResponse::HTTP_OK, R"({"settings": {"max_threads": {"v": 4}}})"), Exception);
    EXPECT_THROW(parseBody(Poco::Net::HTTPResponse::HTTP_OK, R"({"settings": {"max_threads": null}})"), Exception);
    EXPECT_THROW(parseBody(Poco::Net::HTTPResponse::HTTP_OK, R"({"settings": {"": "1"}})"), Exception);
    /// valid_until: negative, fractional, string, boolean.
    EXPECT_THROW(parseBody(Poco::Net::HTTPResponse::HTTP_OK, R"({"valid_until": -5})"), Exception);
    EXPECT_THROW(parseBody(Poco::Net::HTTPResponse::HTTP_OK, R"({"valid_until": 123.5})"), Exception);
    EXPECT_THROW(parseBody(Poco::Net::HTTPResponse::HTTP_OK, R"({"valid_until": "123"})"), Exception);
    /// Poco::Dynamic::Var::isInteger reports true for a boolean value, so `isInteger` alone is not
    /// enough to reject `valid_until: true` - must also check `isBoolean`.
    EXPECT_THROW(parseBody(Poco::Net::HTTPResponse::HTTP_OK, R"({"valid_until": true})"), Exception);
}

TEST(HTTPUserDirectoryResponseParser, SettingValuesKeepJsonScalarType)
{
    /// The parser does not know which names are settings: it returns every entry with a `Field` that
    /// preserves the JSON scalar type. Name policy and built-in casting are the storage's job.
    /// Entries are looked up by name: a JSON object has no defined member order.
    auto result = parseBody(Poco::Net::HTTPResponse::HTTP_OK,
        R"({"settings": {"SQL_tenant": "acme", "SQL_region_id": 42, "SQL_offset": -7, "SQL_ratio": 0.5, "SQL_enabled": true, "max_threads": "4"}})");
    EXPECT_EQ(result.status, HTTPUserDirectoryResponseParser::Result::Status::Ok);
    ASSERT_EQ(result.settings.size(), 6u);
    std::map<String, Field> by_name;
    for (const auto & change : result.settings)
        by_name.emplace(change.name, change.value);
    ASSERT_EQ(by_name.size(), 6u);
    EXPECT_EQ(by_name.at("SQL_tenant").getType(), Field::Types::String);
    EXPECT_EQ(by_name.at("SQL_tenant").safeGet<String>(), "acme");
    EXPECT_EQ(by_name.at("SQL_region_id").getType(), Field::Types::UInt64);
    EXPECT_EQ(by_name.at("SQL_region_id").safeGet<UInt64>(), 42u);
    EXPECT_EQ(by_name.at("SQL_offset").getType(), Field::Types::Int64);
    EXPECT_EQ(by_name.at("SQL_offset").safeGet<Int64>(), -7);
    EXPECT_EQ(by_name.at("SQL_ratio").getType(), Field::Types::Float64);
    EXPECT_EQ(by_name.at("SQL_enabled").getType(), Field::Types::Bool);
    EXPECT_EQ(by_name.at("SQL_enabled").safeGet<bool>(), true);
    EXPECT_EQ(by_name.at("max_threads").getType(), Field::Types::String);
    EXPECT_EQ(by_name.at("max_threads").safeGet<String>(), "4");
}
