#include <gtest/gtest.h>

#include <Core/Field.h>
#include <Core/Settings.h>

/// These `Field` shapes reach a custom setting only over the native wire format, so no stateless test can build a carrier.

using namespace DB;

namespace
{

const String PRESIGNED = "https://bucket/k?X-Amz-Signature=SIG_CANARY&list-type=2";
const String PRESIGNED_MASKED = "https://bucket/k?X-Amz-Signature=[HIDDEN]&list-type=2";
const String USERINFO = "http://user:PW_CANARY@host/p";
const String USERINFO_MASKED = "http://user:[HIDDEN]@host/p";

String mapped(const Field & value)
{
    Settings settings;
    settings.setCustom("SQL_gtest_mask", value);
    return settings.changedToMap().at("SQL_gtest_mask");
}

}

TEST(SettingsMapSecretMask, MasksTheLeavesOfAnArray)
{
    EXPECT_EQ(
        mapped(Array{PRESIGNED, USERINFO, String{"keep-me"}}),
        Field(Array{PRESIGNED_MASKED, USERINFO_MASKED, String{"keep-me"}}).dump());
}

TEST(SettingsMapSecretMask, MasksTheLeavesOfAnObject)
{
    EXPECT_EQ(
        mapped(Object{{"endpoint", PRESIGNED}, {"region", "eu-west-1"}}),
        Field(Object{{"endpoint", PRESIGNED_MASKED}, {"region", "eu-west-1"}}).dump());
}

TEST(SettingsMapSecretMask, MasksBothMembersOfAnAggregateFunctionState)
{
    EXPECT_EQ(
        mapped(AggregateFunctionStateData{PRESIGNED, USERINFO}),
        Field(AggregateFunctionStateData{PRESIGNED_MASKED, USERINFO_MASKED}).dump());
}

TEST(SettingsMapSecretMask, LeavesAValueWithoutACredentialUnchanged)
{
    Field plain = Array{String{"https://bucket/k?list-type=2"}, String{"no credential here"}};
    EXPECT_EQ(mapped(plain), plain.dump());
}
