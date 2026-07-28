#include <Backups/BackupFactory.h>
#include <Backups/BackupInfo.h>

#include "config.h"

#include <Common/Exception.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Parsers/ASTCreateNamedCollectionQuery.h>
#include <Parsers/ASTDropNamedCollectionQuery.h>
#include <base/scope_guard.h>

#include <Poco/Util/MapConfiguration.h>

#include <gtest/gtest.h>

#include <cstdlib>
#include <iostream>


using namespace DB;

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
}

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

    ContextMutablePtr makeContextWithBackupLocations()
    {
        auto context = Context::createCopy(getContext().context);

        Poco::AutoPtr<Poco::Util::MapConfiguration> config(new Poco::Util::MapConfiguration);
        config->setString("backups.allowed_path", "/allowed");
        config->setString("backups.allowed_disk", "default");
        context->setConfig(config);
        return context;
    }

    String getDestinationIdentity(const String & backup_name, ContextPtr context)
    {
        return BackupFactory::instance().getDestinationIdentity(BackupInfo::fromString(backup_name), context);
    }

    template <typename F>
    void expectExceptionCode(F && function, int expected_code)
    {
        try
        {
            function();
            FAIL() << "Expected an exception";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), expected_code);
        }
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


TEST(BackupInfo, DestinationIdentityRequiresContextAndFrozenCollection)
{
    auto context = getContext().context;
    auto info = BackupInfo::fromString("S3(collection)");

    expectExceptionCode(
        [&] { (void)BackupFactory::instance().getDestinationIdentity(info, {}); },
        ErrorCodes::BAD_ARGUMENTS);
    expectExceptionCode(
        [&] { (void)BackupFactory::instance().getDestinationIdentity(info, context); },
        ErrorCodes::BAD_ARGUMENTS);
}


TEST(BackupInfo, DestinationIdentityCanonicalizesLocalLocations)
{
    auto context = makeContextWithBackupLocations();

    EXPECT_EQ(
        getDestinationIdentity("File('dir/../backup/')", context),
        getDestinationIdentity("File('/allowed/backup')", context));
    EXPECT_NE(
        getDestinationIdentity("File('/allowed/backup.zip')", context),
        getDestinationIdentity("File('/allowed/backup.zip/')", context));
    EXPECT_EQ(
        getDestinationIdentity("Disk('default', '')", context),
        getDestinationIdentity("Disk('default', '.')", context));
    EXPECT_EQ(
        getDestinationIdentity("File('/allowed/backup')", context),
        "backup-destination-v1:4:File:20:path=/allowed/backup:8:archive=");
}


TEST(BackupInfo, DestinationIdentityRejectsNonPersistentEngines)
{
    auto context = getContext().context;

    expectExceptionCode(
        [&] { (void)getDestinationIdentity("Memory('backup')", context); },
        ErrorCodes::SUPPORT_IS_DISABLED);
    expectExceptionCode(
        [&] { (void)getDestinationIdentity("Null()", context); },
        ErrorCodes::SUPPORT_IS_DISABLED);
}


#if USE_AWS_S3
TEST(BackupInfo, DestinationIdentityIgnoresS3Credentials)
{
    auto context = getContext().context;
    const String first = getDestinationIdentity("S3('s3://bucket/backup/', 'key1', 'secret1')", context);
    const String second = getDestinationIdentity("S3('https://bucket.s3.amazonaws.com/backup', 'key2', 'secret2')", context);

    EXPECT_EQ(first, second);
    EXPECT_EQ(first.find("key1"), String::npos);
    EXPECT_EQ(first.find("secret1"), String::npos);
    EXPECT_NE(first, getDestinationIdentity("S3('s3://bucket/other')", context));
    EXPECT_NE(
        getDestinationIdentity("S3('s3://bucket/backup.zip')", context),
        getDestinationIdentity("S3('s3://bucket/backup.zip/')", context));
    EXPECT_THROW((void)getDestinationIdentity("S3('s3://bucket/backup', 1, 2)", context), Exception);

    const String with_url_credentials = getDestinationIdentity(
        "S3('https://user:URLPASSWORD@bucket.s3.amazonaws.com/backup')",
        context);
    EXPECT_EQ(with_url_credentials, second);
    EXPECT_EQ(with_url_credentials.find("URLPASSWORD"), String::npos);
}


TEST(BackupInfo, DestinationIdentityHidesS3CredentialsInParseErrors)
{
    auto context = getContext().context;
    auto info = BackupInfo::fromString("S3('https://s3.region.amazonaws.com/bucket//?X-Amz-Signature=TOPSECRET')");

    try
    {
        (void)BackupFactory::instance().getDestinationIdentity(info, context);
        FAIL() << "Expected invalid S3 destination";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.message().find("TOPSECRET"), String::npos);
    }
}


TEST(BackupInfo, FreezeNamedCollectionPreservesDestinationSnapshot)
{
    const String collection_name = "backup_destination_identity_frozen_snapshot";
    auto create_query = make_intrusive<ASTCreateNamedCollectionQuery>();
    create_query->collection_name = collection_name;
    create_query->changes.emplace_back("url", Field("s3://bucket/base"));
    create_query->overridability.emplace("url", true);
    NamedCollectionFactory::instance().createFromSQL(*create_query);

    auto drop_collection = [&]
    {
        auto drop_query = make_intrusive<ASTDropNamedCollectionQuery>();
        drop_query->collection_name = collection_name;
        drop_query->if_exists = true;
        NamedCollectionFactory::instance().removeFromSQL(*drop_query);
    };
    SCOPE_EXIT({ drop_collection(); });

    auto context = getContext().context;
    auto info = BackupInfo::fromString("S3(" + collection_name + ", url='https://user:URLPASSWORD@bucket.s3.amazonaws.com/overridden')");
    auto frozen = info.freezeNamedCollection(context);
    const String identity = BackupFactory::instance().getDestinationIdentity(frozen, context);
    auto redacted = frozen.withoutS3Credentials(context);

    EXPECT_TRUE(frozen.frozen_named_collection->isQueryOverridden("url"));
    EXPECT_NE(frozen.getNamedCollection(context)->get<String>("url").find("URLPASSWORD"), String::npos);
    EXPECT_FALSE(redacted.frozen_named_collection);
    EXPECT_EQ(redacted.toString().find("URLPASSWORD"), String::npos);
    drop_collection();
    EXPECT_THROW((void)redacted.getNamedCollection(context), Exception);
    EXPECT_EQ(BackupFactory::instance().getDestinationIdentity(frozen, context), identity);
}
#endif


#if USE_AZURE_BLOB_STORAGE
TEST(BackupInfo, DestinationIdentityIgnoresAzureCredentials)
{
    auto context = getContext().context;
    const String first = getDestinationIdentity(
        "AzureBlobStorage('https://account.blob.core.windows.net', 'container', 'backup/', 'account', 'key1')",
        context);
    const String second = getDestinationIdentity(
        "AzureBlobStorage('https://account.blob.core.windows.net', 'container', 'backup', 'account', 'key2')",
        context);

    EXPECT_EQ(first, second);
    EXPECT_EQ(first.find("key1"), String::npos);
    EXPECT_EQ(first.find("key2"), String::npos);
    EXPECT_NE(
        first,
        getDestinationIdentity(
            "AzureBlobStorage('https://account.blob.core.windows.net', 'container', 'backup//', 'account', 'key2')",
            context));
    EXPECT_NE(
        getDestinationIdentity(
            "AzureBlobStorage('https://account.blob.core.windows.net', 'container', '', 'account', 'key2')",
            context),
        getDestinationIdentity(
            "AzureBlobStorage('https://account.blob.core.windows.net', 'container', '/', 'account', 'key2')",
            context));
    EXPECT_NE(
        getDestinationIdentity(
            "AzureBlobStorage('https://account.blob.core.windows.net', 'container', '/', 'account', 'key2')",
            context),
        getDestinationIdentity(
            "AzureBlobStorage('https://account.blob.core.windows.net', 'container', '//', 'account', 'key2')",
            context));
    EXPECT_NE(
        getDestinationIdentity(
            "AzureBlobStorage('https://account.blob.core.windows.net', 'container', 'backup.zip', 'account', 'key2')",
            context),
        getDestinationIdentity(
            "AzureBlobStorage('https://account.blob.core.windows.net', 'container', '/backup.zip', 'account', 'key2')",
            context));
}


TEST(BackupInfo, DestinationIdentityRejectsCredentialBearingAzureEndpoint)
{
    auto context = getContext().context;
    auto check_rejected = [&](const String & backup_name, const String & secret)
    {
        try
        {
            (void)getDestinationIdentity(backup_name, context);
            FAIL() << "Expected invalid Azure destination";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.message().find(secret), String::npos);
        }
    };

    check_rejected(
        "AzureBlobStorage('DefaultEndpointsProtocol=https;AccountName=account;AccountKey=TOPSECRET', "
        "'container', 'backup', 'account', 'key')",
        "TOPSECRET");
    check_rejected(
        "AzureBlobStorage('https://user:URLPASSWORD@account.blob.core.windows.net', 'container', 'backup')",
        "URLPASSWORD");
}


TEST(BackupInfo, DestinationIdentityRedactsAzureConnectionStringCredentials)
{
    auto context = getContext().context;
    const String first = getDestinationIdentity(
        "AzureBlobStorage('DefaultEndpointsProtocol=https;AccountName=account;AccountKey=key1;"
        "EndpointSuffix=core.windows.net', 'container', 'backup')",
        context);
    const String second = getDestinationIdentity(
        "AzureBlobStorage('EndpointSuffix=core.windows.net;AccountKey=key2;AccountName=account;"
        "DefaultEndpointsProtocol=https', 'container', 'backup')",
        context);

    EXPECT_EQ(first, second);
    EXPECT_EQ(first.find("key1"), String::npos);
    EXPECT_EQ(second.find("key2"), String::npos);
}
#endif
