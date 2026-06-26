#include <Backups/BackupInfo.h>

#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <gtest/gtest.h>

#include <cstdlib>
#include <iostream>


using namespace DB;

namespace
{
    void requireContains(const String & str, const String & expected)
    {
        if (str.find(expected) == String::npos)
        {
            std::cerr << "Expected to find " << expected << " in " << str << '\n';
            std::_Exit(1);
        }
    }

    void requireNotContains(const String & str, const String & unexpected)
    {
        if (str.find(unexpected) != String::npos)
        {
            std::cerr << "Did not expect to find " << unexpected << " in " << str << '\n';
            std::_Exit(1);
        }
    }

    [[noreturn]] void checkURLOverrideExpressionWithContext()
    {
        tryRegisterFunctions();
        const auto & context = getContext().context;
        auto info = BackupInfo::fromString("S3(collection, url = concat('https://user:URLPASSWORD@', 's3.example.com/bucket/backup'))");

        String str = info.withoutS3Credentials(context).toString();
        requireContains(str, "'https://s3.example.com/bucket/backup'");
        requireNotContains(str, "URLPASSWORD");
        requireNotContains(str, "concat");
        std::_Exit(0);
    }

    [[noreturn]] void checkExpressionCredentialKeyWithContext()
    {
        tryRegisterFunctions();
        const auto & context = getContext().context;
        auto info = BackupInfo::fromString("S3(collection, concat('secret_', 'access_key') = 'KEYSECRET')");

        String str = info.withoutS3Credentials(context).toString();
        requireNotContains(str, "KEYSECRET");
        requireNotContains(str, "concat");
        std::_Exit(0);
    }

    [[noreturn]] void checkExpressionURLKeyAndValueWithContext()
    {
        tryRegisterFunctions();
        const auto & context = getContext().context;
        auto info = BackupInfo::fromString("S3(collection, concat('u', 'rl') = concat('https://user:URLPASSWORD@', 'host/bucket/backup'))");

        String str = info.withoutS3Credentials(context).toString();
        requireContains(str, "host/bucket/backup");
        requireNotContains(str, "URLPASSWORD");
        std::_Exit(0);
    }
}


TEST(BackupInfoDeathTest, WithoutS3CredentialsEvaluatesURLOverrideExpression)
{
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    EXPECT_EXIT(checkURLOverrideExpressionWithContext(), ::testing::ExitedWithCode(0), ".*");
}


TEST(BackupInfoDeathTest, WithoutS3CredentialsStripsExpressionCredentialKey)
{
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    EXPECT_EXIT(checkExpressionCredentialKeyWithContext(), ::testing::ExitedWithCode(0), ".*");
}


TEST(BackupInfoDeathTest, WithoutS3CredentialsRedactsExpressionURLKeyAndValue)
{
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    EXPECT_EXIT(checkExpressionURLKeyAndValueWithContext(), ::testing::ExitedWithCode(0), ".*");
}


TEST(BackupInfo, WithoutS3CredentialsStripsPositionalArguments)
{
    auto info = BackupInfo::fromString("S3('https://s3.example.com/bucket/backup', 'KEYID', 'KEYSECRET')");

    EXPECT_EQ(info.withoutS3Credentials().toString(), "S3('https://s3.example.com/bucket/backup')");
}

TEST(BackupInfo, WithoutS3CredentialsStripsAuthKeyValueArguments)
{
    auto info = BackupInfo::fromString(
        "S3(collection, filename = 'backup', access_key_id = 'KEYID', secret_access_key = 'KEYSECRET', session_token = 'TOKEN', "
        "role_arn = 'ROLEARN', role_session_name = 'ROLESESSION', external_id = 'EXTERNALID')");

    String str = info.withoutS3Credentials().toString();
    EXPECT_NE(str.find("collection"), String::npos);
    EXPECT_NE(str.find("filename"), String::npos);
    for (const auto * credential : {"KEYID", "KEYSECRET", "TOKEN", "ROLEARN", "ROLESESSION", "EXTERNALID"})
        EXPECT_EQ(str.find(credential), String::npos) << str;
}

TEST(BackupInfo, WithoutS3CredentialsStripsExtraCredentials)
{
    auto info = BackupInfo::fromString(
        "S3('https://s3.example.com/bucket/backup', extra_credentials(role_arn = 'ROLEARN', role_session_name = 'ROLESESSION'))");

    EXPECT_EQ(info.withoutS3Credentials().toString(), "S3('https://s3.example.com/bucket/backup')");
}

TEST(BackupInfo, WithoutS3CredentialsRedactsURLUserInfo)
{
    auto info = BackupInfo::fromString("S3('https://user:URLPASSWORD@s3.example.com/bucket/backup', 'KEYID', 'KEYSECRET')");

    EXPECT_EQ(info.withoutS3Credentials().toString(), "S3('https://s3.example.com/bucket/backup')");
}

TEST(BackupInfo, WithoutS3CredentialsRedactsPresignedURLParameters)
{
    auto info = BackupInfo::fromString(
        "S3('https://s3.example.com/bucket/backup?versionId=v1&X-Amz-Signature=URLSIGNATURE&Expires=12345')");

    EXPECT_EQ(info.withoutS3Credentials().toString(), "S3('https://s3.example.com/bucket/backup?versionId=v1')");
}

TEST(BackupInfo, WithoutS3CredentialsRedactsURLOverride)
{
    auto info = BackupInfo::fromString("S3(collection, url = 'https://s3.example.com/bucket/backup?X-Amz-Signature=URLSIGNATURE')");

    String str = info.withoutS3Credentials().toString();
    EXPECT_NE(str.find("bucket/backup"), String::npos) << str;
    EXPECT_EQ(str.find("URLSIGNATURE"), String::npos) << str;
}

TEST(BackupInfo, WithoutS3CredentialsRejectsExpressionCredentialKeyWithoutContext)
{
    auto info = BackupInfo::fromString("S3(collection, concat('secret_', 'access_key') = 'KEYSECRET')");

    EXPECT_THROW((void)info.withoutS3Credentials(), Exception);
}

TEST(BackupInfo, WithoutS3CredentialsRejectsURLOverrideExpressionWithoutContext)
{
    auto info = BackupInfo::fromString("S3(collection, url = concat('https://host/', 'bucket'))");

    EXPECT_THROW((void)info.withoutS3Credentials(), Exception);
}

TEST(BackupInfo, WithoutS3CredentialsKeepsPlainQuery)
{
    auto info = BackupInfo::fromString("S3('https://s3.example.com/bucket/backup?foo=bar')");

    EXPECT_EQ(info.withoutS3Credentials().toString(), "S3('https://s3.example.com/bucket/backup?foo=bar')");
}

TEST(BackupInfo, WithoutS3CredentialsKeepsOtherEngines)
{
    for (const auto * backup_name : {"Disk('backups', 'path')", "File('path')"})
    {
        auto info = BackupInfo::fromString(backup_name);
        EXPECT_EQ(info.withoutS3Credentials().toString(), info.toString());
    }
}
