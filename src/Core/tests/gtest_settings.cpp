#include <gtest/gtest.h>

#include <limits>
#include <string>

#include <Common/Exception.h>
#include <Core/SettingsFields.h>
#include <Core/SettingsEnums.h>
#include <Core/Field.h>

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

GTEST_TEST(SettingFieldMilliseconds, HugeValueSaturatesInsteadOfWrapping)
{
    // A millisecond value above INT64_MAX / 1000 used to overflow the `x * 1000` conversion to
    // microseconds into a negative Int64, so the stored timespan went negative and totalMilliseconds()
    // returned garbage. The value is now saturated, so it must stay non-negative and not exceed the
    // largest representable millisecond count.
    constexpr Int64 max_ms = std::numeric_limits<Int64>::max() / 1000;

    for (UInt64 huge : {UInt64(9223372036854776ULL), UInt64(std::numeric_limits<UInt64>::max())})
    {
        SettingFieldMilliseconds from_ctor(huge);
        ASSERT_GE(from_ctor.totalMicroseconds(), 0);
        ASSERT_GE(from_ctor.totalMilliseconds(), 0);
        ASSERT_LE(from_ctor.totalMilliseconds(), max_ms);
        ASSERT_LE(static_cast<UInt64>(from_ctor), static_cast<UInt64>(max_ms));

        SettingFieldMilliseconds from_assign;
        from_assign = huge;
        ASSERT_GE(from_assign.totalMicroseconds(), 0);
        ASSERT_LE(from_assign.totalMilliseconds(), max_ms);

        SettingFieldMilliseconds from_string;
        from_string.parseFromString(std::to_string(huge));
        ASSERT_GE(from_string.totalMicroseconds(), 0);
        ASSERT_LE(from_string.totalMilliseconds(), max_ms);
    }

    // Values that fit are stored exactly.
    SettingFieldMilliseconds ok(9223372036854775ULL);
    ASSERT_EQ(ok.totalMilliseconds(), 9223372036854775LL);
    SettingFieldMilliseconds small(5000ULL);
    ASSERT_EQ(small.totalMilliseconds(), 5000LL);
    ASSERT_EQ(small.totalMicroseconds(), 5000000LL);
}

GTEST_TEST(SettingFieldSeconds, HugeStringValueDoesNotWrap)
{
    // parseFromString (native-protocol string path) must reject an out-of-range value, like the
    // Field path, rather than wrapping its microseconds via an undefined float-to-Int64 cast.
    for (const char * huge : {"100000000000000", "9223372036854775807", "1e19"})
    {
        SettingFieldSeconds from_string;
        ASSERT_ANY_THROW(from_string.parseFromString(huge));
    }

    // Values within range parse exactly, including fractional seconds.
    SettingFieldSeconds ok;
    ok.parseFromString("120");
    ASSERT_EQ(ok.totalSeconds(), 120LL);
    ASSERT_EQ(ok.totalMicroseconds(), 120000000LL);

    SettingFieldSeconds fractional;
    fractional.parseFromString("1.5");
    ASSERT_EQ(fractional.totalMicroseconds(), 1500000LL);

    // A large-but-representable value stays non-negative (no wrap).
    SettingFieldSeconds big;
    big.parseFromString("100000000000");
    ASSERT_GE(big.totalMicroseconds(), 0);
    ASSERT_EQ(big.totalSeconds(), 100000000000LL);
}
