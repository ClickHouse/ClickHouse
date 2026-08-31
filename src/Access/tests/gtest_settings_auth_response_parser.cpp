#include <gtest/gtest.h>

#include <Access/SettingsAuthResponseParser.h>

#include <Poco/Net/HTTPResponse.h>

#include <fmt/format.h>

#include <sstream>

using namespace DB;

namespace
{

SettingsAuthResponseParser::Result parseResponse(
    const String & body,
    Poco::Net::HTTPResponse::HTTPStatus status = Poco::Net::HTTPResponse::HTTP_OK)
{
    Poco::Net::HTTPResponse response;
    response.setStatus(status);
    std::istringstream body_stream(body); // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    return SettingsAuthResponseParser{}.parse(response, &body_stream);
}

}

TEST(SettingsAuthResponseParser, ParsesIndependentMetadataFields)
{
    const auto result = parseResponse(R"({
        "settings": {"auth_num": "UInt64_15"},
        "roles": ["reader", "analyst"],
        "valid_until": 1788192000
    })");

    ASSERT_TRUE(result.is_ok);
    ASSERT_EQ(result.settings.size(), 1u);
    EXPECT_EQ(result.settings[0].name, "auth_num");
    EXPECT_EQ(result.settings[0].value.safeGet<UInt64>(), 15u);
    EXPECT_EQ(result.roles, Strings({"reader", "analyst"}));
    EXPECT_EQ(result.roles_status, SettingsAuthResponseParser::MetadataStatus::Valid);
    ASSERT_TRUE(result.valid_until.has_value());
    EXPECT_EQ(*result.valid_until, static_cast<time_t>(1788192000));
    EXPECT_EQ(result.valid_until_status, SettingsAuthResponseParser::MetadataStatus::Valid);
}

TEST(SettingsAuthResponseParser, NonOKStatusRejectsWithoutParsingMetadata)
{
    const auto result = parseResponse(
        R"({"settings":{"auth_num":"UInt64_15"},"roles":["reader"],"valid_until":1788192000})",
        Poco::Net::HTTPResponse::HTTP_UNAUTHORIZED);

    EXPECT_FALSE(result.is_ok);
    EXPECT_TRUE(result.settings.empty());
    EXPECT_TRUE(result.roles.empty());
    EXPECT_EQ(result.roles_status, SettingsAuthResponseParser::MetadataStatus::Absent);
    EXPECT_FALSE(result.valid_until.has_value());
    EXPECT_EQ(result.valid_until_status, SettingsAuthResponseParser::MetadataStatus::Absent);
}

TEST(SettingsAuthResponseParser, InvalidRootKeepsSuccessfulHTTPStatusAndIgnoresMetadata)
{
    const auto result = parseResponse("not JSON");

    EXPECT_TRUE(result.is_ok);
    EXPECT_TRUE(result.settings.empty());
    EXPECT_TRUE(result.roles.empty());
    EXPECT_EQ(result.roles_status, SettingsAuthResponseParser::MetadataStatus::Absent);
    EXPECT_FALSE(result.valid_until.has_value());
    EXPECT_EQ(result.valid_until_status, SettingsAuthResponseParser::MetadataStatus::Absent);
}

TEST(SettingsAuthResponseParser, PreservesSettingsParsedBeforeConversionFailure)
{
    const auto result = parseResponse(R"({
        "settings": {
            "auth_a_valid": "UInt64_15",
            "auth_z_invalid": "UInt64_not-a-number"
        },
        "roles": ["reader"],
        "valid_until": 1788192000
    })");

    ASSERT_TRUE(result.is_ok);
    ASSERT_EQ(result.settings.size(), 1u);
    EXPECT_EQ(result.settings[0].name, "auth_a_valid");
    EXPECT_EQ(result.settings[0].value.safeGet<UInt64>(), 15u);
    EXPECT_EQ(result.roles, Strings({"reader"}));
    EXPECT_EQ(result.roles_status, SettingsAuthResponseParser::MetadataStatus::Valid);
    EXPECT_EQ(result.valid_until, static_cast<time_t>(1788192000));
    EXPECT_EQ(result.valid_until_status, SettingsAuthResponseParser::MetadataStatus::Valid);
}

TEST(SettingsAuthResponseParser, MalformedRolesAreInvalidAtomically)
{
    for (const auto & roles : {R"("reader")", R"(["reader", 123])", R"(["reader", true])"})
    {
        const auto result = parseResponse(fmt::format(
            R"({{"settings":{{"auth_num":"UInt64_15"}},"roles":{},"valid_until":1788192000}})", roles));

        ASSERT_TRUE(result.is_ok);
        ASSERT_EQ(result.settings.size(), 1u);
        EXPECT_TRUE(result.roles.empty());
        EXPECT_EQ(result.roles_status, SettingsAuthResponseParser::MetadataStatus::Invalid);
        EXPECT_EQ(result.valid_until, static_cast<time_t>(1788192000));
        EXPECT_EQ(result.valid_until_status, SettingsAuthResponseParser::MetadataStatus::Valid);
    }
}

TEST(SettingsAuthResponseParser, MalformedValidUntilIsInvalidIndependently)
{
    for (const auto & valid_until : {R"("not-a-timestamp")", "true", "1.5", "9223372036854775808"})
    {
        const auto result = parseResponse(fmt::format(
            R"({{"settings":{{"auth_num":"UInt64_15"}},"roles":["reader"],"valid_until":{}}})", valid_until));

        ASSERT_TRUE(result.is_ok);
        ASSERT_EQ(result.settings.size(), 1u);
        EXPECT_EQ(result.roles, Strings({"reader"}));
        EXPECT_EQ(result.roles_status, SettingsAuthResponseParser::MetadataStatus::Valid);
        EXPECT_FALSE(result.valid_until.has_value());
        EXPECT_EQ(result.valid_until_status, SettingsAuthResponseParser::MetadataStatus::Invalid);
    }
}

TEST(SettingsAuthResponseParser, ExplicitZeroValidUntilIsPreserved)
{
    const auto result = parseResponse(R"({"valid_until":0})");

    ASSERT_TRUE(result.is_ok);
    EXPECT_EQ(result.roles_status, SettingsAuthResponseParser::MetadataStatus::Absent);
    ASSERT_TRUE(result.valid_until.has_value());
    EXPECT_EQ(*result.valid_until, 0);
    EXPECT_EQ(result.valid_until_status, SettingsAuthResponseParser::MetadataStatus::Valid);
}
