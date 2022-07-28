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

GTEST_TEST(MySQLDataTypesSupport, WithDefault)
{
    // Setting can be default-initialized and that means all values are unset.
    const SettingMySQLDataTypesSupport setting;
    ASSERT_EQ(0, setting.value.getValue());
    ASSERT_EQ("", setting.toString());
    ASSERT_EQ(setting, Field(""));

    ASSERT_FALSE(setting.value.isSet(MySQLDataTypesSupport::DECIMAL));
    ASSERT_FALSE(setting.value.isSet(MySQLDataTypesSupport::DATETIME64));
}

GTEST_TEST(SettingMySQLDataTypesSupport, WithDECIMAL)
{
    // Setting can be initialized with MySQLDataTypesSupport::DECIMAL
    // and this value can be obtained in varios forms with getters.
    const SettingMySQLDataTypesSupport setting(MySQLDataTypesSupport::DECIMAL);
    ASSERT_EQ(1, setting.value.getValue());

    ASSERT_TRUE(setting.value.isSet(MySQLDataTypesSupport::DECIMAL));
    ASSERT_FALSE(setting.value.isSet(MySQLDataTypesSupport::DATETIME64));

    ASSERT_EQ("decimal", setting.toString());
    ASSERT_EQ(Field("decimal"), setting);
}

GTEST_TEST(SettingMySQLDataTypesSupport, With1)
{
    // Setting can be initialized with int value corresponding to DECIMAL
    // and rest of the test is the same as for that value.
    const SettingMySQLDataTypesSupport setting(1u);
    ASSERT_EQ(1, setting.value.getValue());

    ASSERT_TRUE(setting.value.isSet(MySQLDataTypesSupport::DECIMAL));
    ASSERT_FALSE(setting.value.isSet(MySQLDataTypesSupport::DATETIME64));

    ASSERT_EQ("decimal", setting.toString());
    ASSERT_EQ(Field("decimal"), setting);
}

GTEST_TEST(SettingMySQLDataTypesSupport, WithMultipleValues)
{
    // Setting can be initialized with int value corresponding to (DECIMAL | DATETIME64)
    const SettingMySQLDataTypesSupport setting(3u);
    ASSERT_EQ(3, setting.value.getValue());

    ASSERT_TRUE(setting.value.isSet(MySQLDataTypesSupport::DECIMAL));
    ASSERT_TRUE(setting.value.isSet(MySQLDataTypesSupport::DATETIME64));

    ASSERT_EQ("decimal,datetime64", setting.toString());
    ASSERT_EQ(Field("decimal,datetime64"), setting);
}

GTEST_TEST(SettingMySQLDataTypesSupport, SetString)
{
    SettingMySQLDataTypesSupport setting;
    setting = String("decimal");
    ASSERT_TRUE(setting.changed);
    ASSERT_TRUE(setting.value.isSet(MySQLDataTypesSupport::DECIMAL));
    ASSERT_FALSE(setting.value.isSet(MySQLDataTypesSupport::DATETIME64));
    ASSERT_EQ("decimal", setting.toString());
    ASSERT_EQ(Field("decimal"), setting);

    setting = "datetime64,decimal";
    ASSERT_TRUE(setting.changed);
    ASSERT_TRUE(setting.value.isSet(MySQLDataTypesSupport::DECIMAL));
    ASSERT_TRUE(setting.value.isSet(MySQLDataTypesSupport::DATETIME64));
    ASSERT_EQ("decimal,datetime64", setting.toString());
    ASSERT_EQ(Field("decimal,datetime64"), setting);

    // comma with spaces
    setting = " datetime64 ,    decimal ";
    ASSERT_FALSE(setting.changed); // false since value is the same as previous one.
    ASSERT_TRUE(setting.value.isSet(MySQLDataTypesSupport::DECIMAL));
    ASSERT_TRUE(setting.value.isSet(MySQLDataTypesSupport::DATETIME64));
    ASSERT_EQ("decimal,datetime64", setting.toString());
    ASSERT_EQ(Field("decimal,datetime64"), setting);

    setting = String(",,,,,,,, ,decimal");
    ASSERT_TRUE(setting.changed);
    ASSERT_TRUE(setting.value.isSet(MySQLDataTypesSupport::DECIMAL));
    ASSERT_FALSE(setting.value.isSet(MySQLDataTypesSupport::DATETIME64));
    ASSERT_EQ("decimal", setting.toString());
    ASSERT_EQ(Field("decimal"), setting);

    setting = String(",decimal,decimal,decimal,decimal,decimal,decimal,decimal,decimal,decimal,");
    ASSERT_FALSE(setting.changed); //since previous value was DECIMAL
    ASSERT_TRUE(setting.value.isSet(MySQLDataTypesSupport::DECIMAL));
    ASSERT_FALSE(setting.value.isSet(MySQLDataTypesSupport::DATETIME64));
    ASSERT_EQ("decimal", setting.toString());
    ASSERT_EQ(Field("decimal"), setting);

    setting = String("");
    ASSERT_TRUE(setting.changed);
    ASSERT_FALSE(setting.value.isSet(MySQLDataTypesSupport::DECIMAL));
    ASSERT_FALSE(setting.value.isSet(MySQLDataTypesSupport::DATETIME64));
    ASSERT_EQ("", setting.toString());
    ASSERT_EQ(Field(""), setting);
}

GTEST_TEST(SettingMySQLDataTypesSupport, SetInvalidString)
{
    // Setting can be initialized with int value corresponding to (DECIMAL | DATETIME64)
    SettingMySQLDataTypesSupport setting;
    EXPECT_THROW(setting = String("FOOBAR"), Exception);
    ASSERT_FALSE(setting.changed);
    ASSERT_EQ(0, setting.value.getValue());

    EXPECT_THROW(setting = String("decimal,datetime64,123"), Exception);
    ASSERT_FALSE(setting.changed);
    ASSERT_EQ(0, setting.value.getValue());

    EXPECT_NO_THROW(setting = String(", "));
    ASSERT_FALSE(setting.changed);
    ASSERT_EQ(0, setting.value.getValue());
}

