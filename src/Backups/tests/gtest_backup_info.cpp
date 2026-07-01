#include <Backups/BackupInfo.h>

#include "config.h"

#include <Common/Exception.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <Poco/Util/MapConfiguration.h>

#include <gtest/gtest.h>

#include <cstdlib>
#include <iostream>


using namespace DB;

namespace
{
    void checkCanCopyS3CredentialsInvariant(const String & source_str, const String & dest_str)
    {
        auto source = BackupInfo::fromString(source_str);
        auto dest = BackupInfo::fromString(dest_str);
        auto dest_for_copy = dest;

        bool copy_succeeded = true;
        try
        {
            source.copyS3CredentialsTo(dest_for_copy);
        }
        catch (const Exception &)
        {
            copy_succeeded = false;
        }

        EXPECT_EQ(source.canCopyS3CredentialsTo(dest), copy_succeeded) << source_str << " -> " << dest_str;
    }

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

    NamedCollectionPtr makeNamedCollection(std::initializer_list<std::pair<String, String>> values)
    {
        Poco::AutoPtr<Poco::Util::MapConfiguration> config(new Poco::Util::MapConfiguration);
        NamedCollection::Keys keys;
        for (const auto & [key, value] : values)
        {
            config->setString("collection." + key, value);
            keys.insert(key);
        }

        return NamedCollectionFromConfig::create(*config, "collection", "collection", keys);
    }

    ContextMutablePtr makeContextWithBackupAllowedPaths()
    {
        auto context = Context::createCopy(getContext().context);

        Poco::AutoPtr<Poco::Util::MapConfiguration> config(new Poco::Util::MapConfiguration);
        config->setString("backups.allowed_path", "/allowed");
        config->setString("backups.allowed_path[1]", "/also_allowed");
        context->setConfig(config);

        return context;
    }

    ContextMutablePtr makeContextWithBackupAllowedDisks()
    {
        auto context = Context::createCopy(getContext().context);

        Poco::AutoPtr<Poco::Util::MapConfiguration> config(new Poco::Util::MapConfiguration);
        config->setString("backups.allowed_disk", "other_disk");
        context->setConfig(config);

        return context;
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

TEST(BackupInfo, WithoutS3CredentialsRedactsCombinedURLCredentials)
{
    auto info = BackupInfo::fromString(
        "S3('https://user:URLPASSWORD@s3.example.com/bucket/backup?versionId=v1&X-Amz-Signature=URLSIGNATURE&Expires=12345', "
        "'KEYID', 'KEYSECRET')");

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

TEST(BackupInfo, WithoutS3CredentialsIsIdempotent)
{
    auto info = BackupInfo::fromString(
        "S3(collection, url = 'https://user:URLPASSWORD@s3.example.com/bucket/backup?versionId=v1&X-Amz-Signature=URLSIGNATURE', "
        "access_key_id = 'KEYID', secret_access_key = 'KEYSECRET', extra_credentials(external_id = 'SECRET_EXTERNAL_ID'))");

    auto once = info.withoutS3Credentials();
    EXPECT_EQ(once.withoutS3Credentials().toString(), once.toString());
}

TEST(BackupInfo, WithoutS3CredentialsKeepsOtherEngines)
{
    for (const auto * backup_name : {"Disk('backups', 'path')", "File('path')"})
    {
        auto info = BackupInfo::fromString(backup_name);
        EXPECT_EQ(info.withoutS3Credentials().toString(), info.toString());
    }
}

TEST(BackupInfo, CanCopyS3CredentialsToMatchesCopyS3CredentialsTo)
{
    checkCanCopyS3CredentialsInvariant("S3('https://s3.example.com/backup', 'KEYID', 'KEYSECRET')", "S3('https://s3.example.com/base')");
    checkCanCopyS3CredentialsInvariant("S3(collection)", "S3('https://s3.example.com/base')");
    checkCanCopyS3CredentialsInvariant("S3('https://s3.example.com/backup', 'KEYID', 'KEYSECRET')", "S3(collection)");
    checkCanCopyS3CredentialsInvariant("Disk('backups', 'path')", "S3('https://s3.example.com/base')");
    checkCanCopyS3CredentialsInvariant("S3('https://s3.example.com/backup', 'KEYID', 'KEYSECRET')", "Disk('backups', 'path')");
    checkCanCopyS3CredentialsInvariant("S3('https://s3.example.com/backup')", "S3('https://s3.example.com/base')");
}

TEST(BackupInfo, NormalizedStringIgnoresS3Credentials)
{
    auto first = BackupInfo::fromString("S3('s3://bucket/backup/', 'key1', 'secret1')");
    auto second = BackupInfo::fromString("S3('s3://bucket/backup', 'key2', 'secret2')");

    EXPECT_EQ(first.toNormalizedString(), second.toNormalizedString());
}

TEST(BackupInfo, NormalizedStringRedactsS3UrlCredentials)
{
    auto first = BackupInfo::fromString(
        "S3('https://user1:password1@s3.example.com/bucket/backup?X-Amz-Signature=signature1', 'key1', 'secret1')");
    auto second = BackupInfo::fromString(
        "S3('https://user2:password2@s3.example.com/bucket/backup?X-Amz-Signature=signature2', 'key2', 'secret2')");

    EXPECT_EQ(first.toNormalizedString(), second.toNormalizedString());
    EXPECT_EQ(first.toNormalizedString().find("password1"), String::npos);
    EXPECT_EQ(first.toNormalizedString().find("signature1"), String::npos);
    EXPECT_EQ(second.toNormalizedString().find("password2"), String::npos);
    EXPECT_EQ(second.toNormalizedString().find("signature2"), String::npos);
}

#if USE_AWS_S3
TEST(BackupInfo, NormalizedStringPreservesS3VersionIdOnly)
{
    auto first = BackupInfo::fromString("S3('s3://bucket/backup?part=1&versionId=v1')");
    auto second = BackupInfo::fromString("S3('s3://bucket/backup?part=2&versionId=v1')");
    auto third = BackupInfo::fromString("S3('s3://bucket/backup?part=1&versionId=v2')");

    EXPECT_EQ(first.toNormalizedString(), second.toNormalizedString());
    EXPECT_NE(first.toNormalizedString(), third.toNormalizedString());
    EXPECT_EQ(first.toNormalizedString().find("part=1"), String::npos);
    EXPECT_EQ(second.toNormalizedString().find("part=2"), String::npos);
}

TEST(BackupInfo, NormalizedStringIgnoresS3UrlQueryExceptVersionId)
{
    auto first = BackupInfo::fromString("S3('s3://bucket/backup?secret=one&versionId=v1')");
    auto second = BackupInfo::fromString("S3('s3://bucket/backup?secret=two&versionId=v1')");
    auto third = BackupInfo::fromString("S3('s3://bucket/backup?secret=one&versionId=v2')");

    EXPECT_EQ(first.toNormalizedString(), second.toNormalizedString());
    EXPECT_NE(first.toNormalizedString(), third.toNormalizedString());
    EXPECT_EQ(first.toNormalizedString().find("secret"), String::npos);
    EXPECT_EQ(second.toNormalizedString().find("two"), String::npos);
}

TEST(BackupInfo, NormalizedStringIgnoresS3PathQueryExceptVersionId)
{
    auto first = BackupInfo::fromString("S3(collection, 'backup?secret=one&versionId=v1')");
    auto second = BackupInfo::fromString("S3(collection, 'backup?secret=two&versionId=v1')");
    auto third = BackupInfo::fromString("S3(collection, 'backup?secret=one&versionId=v2')");

    EXPECT_EQ(first.toNormalizedString(), second.toNormalizedString());
    EXPECT_NE(first.toNormalizedString(), third.toNormalizedString());
    EXPECT_EQ(first.toNormalizedString().find("secret"), String::npos);
    EXPECT_EQ(second.toNormalizedString().find("two"), String::npos);
}

TEST(BackupInfo, NormalizedStringCanonicalizesEquivalentS3Urls)
{
    auto s3 = BackupInfo::fromString("S3('s3://bucket/backup')");
    auto virtual_hosted = BackupInfo::fromString("S3('https://bucket.s3.amazonaws.com/backup')");
    auto path_style = BackupInfo::fromString("S3('https://s3.amazonaws.com/bucket/backup')");

    EXPECT_EQ(s3.toNormalizedString(), virtual_hosted.toNormalizedString());
    EXPECT_EQ(s3.toNormalizedString(), path_style.toNormalizedString());
}
#endif

TEST(BackupInfo, NormalizedStringUsesFrozenS3NamedCollection)
{
    auto context = getContext().context;
    auto first = BackupInfo::fromString("S3(collection, 'backup')");
    auto second = BackupInfo::fromString("S3(collection, 'backup/')");
    auto third = BackupInfo::fromString("S3(collection, 'other')");

    first.frozen_named_collection = makeNamedCollection({{"url", "s3://bucket/base"}});
    second.frozen_named_collection = makeNamedCollection({{"url", "s3://bucket/base"}});
    third.frozen_named_collection = makeNamedCollection({{"url", "s3://bucket/base"}});

    EXPECT_EQ(first.toNormalizedString(context), second.toNormalizedString(context));
    EXPECT_NE(first.toNormalizedString(context), third.toNormalizedString(context));
}

TEST(BackupInfo, NormalizedStringRejectsUnresolvedNamedCollectionWithoutContext)
{
    auto info = BackupInfo::fromString("S3(collection)");

    EXPECT_THROW((void)info.toNormalizedString(ContextPtr{}), Exception);
}

TEST(BackupInfo, NormalizedStringCanonicalizesDiskPath)
{
    auto first = BackupInfo::fromString("Disk('backups', 'dir/../backup/')");
    auto second = BackupInfo::fromString("Disk('backups', 'backup')");

    EXPECT_EQ(first.toNormalizedString(), second.toNormalizedString());
}

TEST(BackupInfo, NormalizedStringRejectsDisallowedDiskWithContext)
{
    auto context = makeContextWithBackupAllowedDisks();
    auto info = BackupInfo::fromString("Disk('default', 'backup')");

    EXPECT_THROW((void)info.toNormalizedString(context), Exception);
}

TEST(BackupInfo, NormalizedStringCanonicalizesFilePath)
{
    auto first = BackupInfo::fromString("File('dir/../backup/')");
    auto second = BackupInfo::fromString("File('backup')");

    EXPECT_EQ(first.toNormalizedString(), second.toNormalizedString());
}

TEST(BackupInfo, NormalizedStringPreservesArchiveDirectoryPath)
{
    auto archive_file = BackupInfo::fromString("File('backup.zip')");
    auto archive_directory = BackupInfo::fromString("File('backup.zip/')");

    EXPECT_NE(archive_file.toNormalizedString(), archive_directory.toNormalizedString());
}

TEST(BackupInfo, NormalizedStringRejectsNonStringPath)
{
    auto info = BackupInfo::fromString("Disk('backups', 1)");

    EXPECT_THROW((void)info.toNormalizedString(), Exception);
}

TEST(BackupInfo, NormalizedStringValidatesFilePathWithContext)
{
    auto context = makeContextWithBackupAllowedPaths();
    auto disallowed = BackupInfo::fromString("File('/not_allowed/backup')");
    auto allowed = BackupInfo::fromString("File('/also_allowed/backup')");

    EXPECT_THROW((void)disallowed.toNormalizedString(context), Exception);
    EXPECT_NE(allowed.toNormalizedString(context).find("/also_allowed/backup"), String::npos);
}

TEST(BackupInfo, NormalizedStringCanonicalizesKeyValueArgNames)
{
    auto first = BackupInfo::fromString("S3(collection, url='s3://bucket/backup')");
    auto second = BackupInfo::fromString("S3(collection, URL='s3://bucket/backup')");

    EXPECT_EQ(first.toNormalizedString(), second.toNormalizedString());
}

TEST(BackupInfo, NormalizedStringRejectsNonStringKeyValueArg)
{
    auto info = BackupInfo::fromString("S3(collection, url=concat('s3://bucket/', 'backup'))");

    EXPECT_THROW((void)info.toNormalizedString(), Exception);
}

TEST(BackupInfo, NormalizedStringIgnoresAzureCredentials)
{
    auto first = BackupInfo::fromString(
        "AzureBlobStorage('https://account.blob.core.windows.net', 'container', 'backup/', 'account', 'key1')");
    auto second = BackupInfo::fromString(
        "AzureBlobStorage('https://account.blob.core.windows.net', 'container', 'backup', 'account', 'key2')");

    EXPECT_EQ(first.toNormalizedString(), second.toNormalizedString());
}

TEST(BackupInfo, NormalizedStringRedactsAzureConnectionStringCredentials)
{
    auto first = BackupInfo::fromString(
        "AzureBlobStorage('DefaultEndpointsProtocol=https;AccountName=account;"
        "AccountKey=key1;EndpointSuffix=core.windows.net', 'container', 'backup')");
    auto second = BackupInfo::fromString(
        "AzureBlobStorage('EndpointSuffix=core.windows.net;AccountKey=key2;"
        "AccountName=account;DefaultEndpointsProtocol=https', 'container', 'backup')");

    EXPECT_EQ(first.toNormalizedString(), second.toNormalizedString());
    EXPECT_EQ(first.toNormalizedString().find("key1"), String::npos);
    EXPECT_EQ(second.toNormalizedString().find("key2"), String::npos);
}

TEST(BackupInfo, NormalizedStringUsesFrozenAzureNamedCollection)
{
    auto context = getContext().context;
    auto first = BackupInfo::fromString("AzureBlobStorage(collection, 'backup')");
    auto second = BackupInfo::fromString("AzureBlobStorage(collection, 'backup/')");
    auto third = BackupInfo::fromString("AzureBlobStorage(collection, 'other')");

    auto collection = makeNamedCollection(
        {
            {"storage_account_url", "https://account.blob.core.windows.net"},
            {"container", "container"},
            {"blob_path", "base"},
        });
    first.frozen_named_collection = collection;
    second.frozen_named_collection = collection;
    third.frozen_named_collection = collection;

    EXPECT_EQ(first.toNormalizedString(context), second.toNormalizedString(context));
    EXPECT_NE(first.toNormalizedString(context), third.toNormalizedString(context));
}
