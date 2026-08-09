#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Core/BaseSettings.h>
#include <Core/Settings.h>
#include <Core/SettingsFields.h>
#include <Core/SettingsEnums.h>
#include <Core/Field.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <limits>

namespace
{
using namespace DB;
using SettingMySQLDataTypesSupport = SettingFieldMultiEnum<MySQLDataTypesSupport, SettingFieldMySQLDataTypesSupportTraits>;
}

namespace DB
{

template <typename Enum, typename Traits>
bool operator== (const SettingFieldMultiEnum<Enum, Traits> & setting, const Field & f)
{
    return Field(setting) == f;
}

template <typename Enum, typename Traits>
bool operator== (const Field & f, const SettingFieldMultiEnum<Enum, Traits> & setting)
{
    return f == Field(setting);
}

}

GTEST_TEST(SettingMySQLDataTypesSupport, WithDefault)
{
    // Setting can be default-initialized and that means all values are unset.
    const SettingMySQLDataTypesSupport setting;
    ASSERT_EQ(std::vector<MySQLDataTypesSupport>{}, setting.value);
    ASSERT_EQ("", setting.toString());
    ASSERT_EQ(setting, Field(""));

    ASSERT_FALSE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DECIMAL));
    ASSERT_FALSE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DATETIME64));
}

GTEST_TEST(SettingMySQLDataTypesSupport, WithDECIMAL)
{
    // Setting can be initialized with MySQLDataTypesSupport::DECIMAL
    // and this value can be obtained in varios forms with getters.
    const SettingMySQLDataTypesSupport setting(MySQLDataTypesSupport::DECIMAL);
    ASSERT_EQ(std::vector<MySQLDataTypesSupport>{MySQLDataTypesSupport::DECIMAL}, setting.value);

    ASSERT_TRUE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DECIMAL));
    ASSERT_FALSE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DATETIME64));

    ASSERT_EQ("decimal", setting.toString());
    ASSERT_EQ(Field("decimal"), setting);
}

GTEST_TEST(SettingMySQLDataTypesSupport, WithDATE)
{
    SettingMySQLDataTypesSupport setting;
    setting = String("date2Date32");
    ASSERT_EQ(std::vector<MySQLDataTypesSupport>{MySQLDataTypesSupport::DATE2DATE32}, setting.value);

    ASSERT_TRUE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DATE2DATE32));
    ASSERT_FALSE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DECIMAL));
    ASSERT_FALSE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DATETIME64));

    ASSERT_EQ("date2Date32", setting.toString());
    ASSERT_EQ(Field("date2Date32"), setting);

    setting = String("date2String");
    ASSERT_EQ(std::vector<MySQLDataTypesSupport>{MySQLDataTypesSupport::DATE2STRING}, setting.value);

    ASSERT_TRUE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DATE2STRING));
    ASSERT_FALSE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DATE2DATE32));

    ASSERT_EQ("date2String", setting.toString());
    ASSERT_EQ(Field("date2String"), setting);
}

GTEST_TEST(SettingMySQLDataTypesSupport, SetString)
{
    SettingMySQLDataTypesSupport setting;
    setting = String("decimal");
    ASSERT_TRUE(setting.changed);

    ASSERT_TRUE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DECIMAL));
    ASSERT_FALSE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DATETIME64));
    ASSERT_EQ("decimal", setting.toString());
    ASSERT_EQ(Field("decimal"), setting);

    setting = "datetime64,decimal";
    ASSERT_TRUE(setting.changed);
    ASSERT_TRUE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DECIMAL));
    ASSERT_TRUE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DATETIME64));
    ASSERT_EQ("datetime64,decimal", setting.toString());
    ASSERT_EQ(Field("datetime64,decimal"), setting);

    // comma with spaces
    setting = " datetime64 ,    decimal "; /// bad punctuation is ok here
    ASSERT_TRUE(setting.changed);
    ASSERT_TRUE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DECIMAL));
    ASSERT_TRUE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DATETIME64));
    ASSERT_EQ("datetime64,decimal", setting.toString());
    ASSERT_EQ(Field("datetime64,decimal"), setting);

    setting = String(",,,,,,,, ,decimal");
    ASSERT_TRUE(setting.changed);
    ASSERT_TRUE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DECIMAL));
    ASSERT_FALSE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DATETIME64));
    ASSERT_EQ("decimal", setting.toString());
    ASSERT_EQ(Field("decimal"), setting);

    setting = String(",decimal,decimal,decimal,decimal,decimal,decimal,decimal,decimal,decimal,");
    ASSERT_TRUE(setting.changed); //since previous value was DECIMAL
    ASSERT_TRUE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DECIMAL));
    ASSERT_FALSE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DATETIME64));
    ASSERT_EQ("decimal", setting.toString());
    ASSERT_EQ(Field("decimal"), setting);

    setting = String("");
    ASSERT_TRUE(setting.changed);
    ASSERT_FALSE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DECIMAL));
    ASSERT_FALSE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DATETIME64));
    ASSERT_EQ("", setting.toString());
    ASSERT_EQ(Field(""), setting);
}

GTEST_TEST(SettingMySQLDataTypesSupport, SetInvalidString)
{
    // Setting can be initialized with int value corresponding to (DECIMAL | DATETIME64)
    SettingMySQLDataTypesSupport setting;
    EXPECT_THROW(setting = String("FOOBAR"), Exception);
    ASSERT_FALSE(setting.changed);
    ASSERT_EQ(std::vector<MySQLDataTypesSupport>{}, setting.value);

    EXPECT_THROW(setting = String("decimal,datetime64,123"), Exception);
    ASSERT_FALSE(setting.changed);
    ASSERT_EQ(std::vector<MySQLDataTypesSupport>{}, setting.value);

    EXPECT_NO_THROW(setting = String(", "));
    ASSERT_TRUE(setting.changed);
    ASSERT_EQ(std::vector<MySQLDataTypesSupport>{}, setting.value);
}

GTEST_TEST(QueryParameters, RoundTrip)
{
    NameToNameMap parameters{{"max_threads", "0"}, {"name", "John's \\ Doe"}, {"empty", ""}};

    WriteBufferFromOwnString out;
    writeQueryParameters(parameters, out);

    ReadBufferFromString in(out.str());
    ASSERT_EQ(readQueryParameters(in), parameters);
}

GTEST_TEST(QueryParameters, DuplicateNameOnTheWireLastOccurrenceWins)
{
    /// A driver that appends an override without deduplicating first relies on the last
    /// occurrence of a repeated Parameter[] key winning, matching the pre-existing behavior
    /// of routing parameters through a Settings object (BaseSettings::read overwrites the
    /// same custom setting entry on each occurrence of its name).
    WriteBufferFromOwnString out;
    BaseSettingsHelpers::writeString("x", out);
    BaseSettingsHelpers::writeFlags(BaseSettingsHelpers::Flags::CUSTOM, out);
    BaseSettingsHelpers::writeString(SettingFieldCustom(Field(String("1"))).toString(), out);
    BaseSettingsHelpers::writeString("x", out);
    BaseSettingsHelpers::writeFlags(BaseSettingsHelpers::Flags::CUSTOM, out);
    BaseSettingsHelpers::writeString(SettingFieldCustom(Field(String("2"))).toString(), out);
    BaseSettingsHelpers::writeString(std::string_view{}, out);

    ReadBufferFromString in(out.str());
    NameToNameMap parameters = readQueryParameters(in);
    ASSERT_EQ(parameters.at("x"), "2");
}

GTEST_TEST(SettingFieldTimespan, ValueAlwaysFitsInt64Microseconds)
{
    constexpr Int64 max_ms = std::numeric_limits<Int64>::max() / 1000;
    constexpr Int64 max_s = std::numeric_limits<Int64>::max() / 1000000;

    /// Values whose microseconds fit Int64 are stored exactly.
    ASSERT_EQ(SettingFieldMilliseconds(UInt64(0)).totalMicroseconds(), 0);
    ASSERT_EQ(SettingFieldMilliseconds(UInt64(5000)).totalMicroseconds(), 5000000);
    ASSERT_EQ(SettingFieldSeconds(UInt64(300)).totalMicroseconds(), 300000000);
    ASSERT_EQ(SettingFieldMilliseconds(UInt64(max_ms)).totalMilliseconds(), max_ms);
    ASSERT_EQ(SettingFieldSeconds(UInt64(max_s)).totalSeconds(), max_s);

    /// Larger values are rejected instead of wrapping mod 2^64. Before the check, UInt64 max
    /// wrapped to -1 ms, 2^61 to exactly 0 ms and 2^61 + 1 to exactly 1 ms.
    ASSERT_THROW(SettingFieldMilliseconds(UInt64(max_ms) + 1), DB::Exception);
    ASSERT_THROW(SettingFieldMilliseconds{std::numeric_limits<UInt64>::max()}, DB::Exception);
    ASSERT_THROW(SettingFieldMilliseconds(UInt64(1) << 61), DB::Exception);
    ASSERT_THROW(SettingFieldMilliseconds((UInt64(1) << 61) + 1), DB::Exception);
    ASSERT_THROW(SettingFieldSeconds{std::numeric_limits<UInt64>::max()}, DB::Exception);

    /// Every integer producer funnels into the same check: Field (SET and profiles) and the
    /// native-protocol binary form.
    SettingFieldMilliseconds assigned;
    ASSERT_THROW(assigned = std::numeric_limits<UInt64>::max(), DB::Exception);
    ASSERT_THROW(SettingFieldMilliseconds(Field(UInt64(1) << 61)), DB::Exception);

    /// The largest accepted value survives a string round-trip exactly.
    SettingFieldMilliseconds largest{UInt64(max_ms)};
    SettingFieldMilliseconds reparsed;
    reparsed.parseFromString(largest.toString());
    ASSERT_EQ(reparsed.totalMicroseconds(), largest.totalMicroseconds());
}

GTEST_TEST(SettingFieldTimespan, SecondsParseFromStringChecksTheRange)
{
    SettingFieldSeconds seconds;
    seconds.parseFromString("300");
    ASSERT_EQ(seconds.totalSeconds(), 300);

    /// A value that does not fit Int64 microseconds is rejected, the same as through Field.
    ASSERT_THROW(seconds.parseFromString("1e30"), DB::Exception);
}
