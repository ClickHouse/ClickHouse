#include <gtest/gtest.h>

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
