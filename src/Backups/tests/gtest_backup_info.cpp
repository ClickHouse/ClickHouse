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
    void checkCopyCredentials(const String & source_str, const String & destination_str, const char * expected)
    {
        auto source = BackupInfo::fromString(source_str);
        auto destination = BackupInfo::fromString(destination_str);
        EXPECT_EQ(BackupFactory::instance().copyCredentials(source, destination, getContext().context), expected != nullptr);
        if (expected)
            EXPECT_EQ(destination.toString(), expected);
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

        String str = BackupFactory::instance().withoutCredentials(info, context).toString();
        requireContains(str, "'https://s3.example.com/bucket/backup'");
        requireNotContains(str, "URLPASSWORD");
        requireNotContains(str, "concat");
        std::_Exit(0);
    }

    [[noreturn]] void checkExpressionCredentialKeyWithContext()
    {
        tryRegisterFunctions();
        const auto & context = getContext().context;
        auto info = BackupInfo::fromString("S3(collection, concat('secret_', 'access_key') = throwIf(1, 'KEYSECRET'))");

        String str = BackupFactory::instance().withoutCredentials(info, context).toString();
        requireNotContains(str, "KEYSECRET");
        requireNotContains(str, "concat");
        std::_Exit(0);
    }

    [[noreturn]] void checkExpressionURLKeyAndValueWithContext()
    {
        tryRegisterFunctions();
        const auto & context = getContext().context;
        auto info = BackupInfo::fromString("S3(collection, concat('u', 'rl') = concat('https://user:URLPASSWORD@', 'host/bucket/backup'))");

        String str = BackupFactory::instance().withoutCredentials(info, context).toString();
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

    BackupInfo withoutCredentials(const BackupInfo & backup_info)
    {
        return BackupFactory::instance().withoutCredentials(backup_info, getContext().context);
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

    EXPECT_EQ(withoutCredentials(info).toString(), "S3('https://s3.example.com/bucket/backup')");
}

TEST(BackupInfo, WithoutS3CredentialsStripsAuthKeyValueArguments)
{
    auto info = BackupInfo::fromString(
        "S3(collection, filename = 'backup', access_key_id = 'KEYID', secret_access_key = 'KEYSECRET', session_token = 'TOKEN', "
        "role_arn = 'ROLEARN', role_session_name = 'ROLESESSION', external_id = 'EXTERNALID')");

    String str = withoutCredentials(info).toString();
    EXPECT_NE(str.find("collection"), String::npos);
    EXPECT_NE(str.find("filename"), String::npos);
    for (const auto * credential : {"KEYID", "KEYSECRET", "TOKEN", "ROLEARN", "ROLESESSION", "EXTERNALID"})
        EXPECT_EQ(str.find(credential), String::npos) << str;
}

TEST(BackupInfo, WithoutS3CredentialsStripsExtraCredentials)
{
    auto info = BackupInfo::fromString(
        "S3('https://s3.example.com/bucket/backup', extra_credentials(role_arn = 'ROLEARN', role_session_name = 'ROLESESSION'))");

    EXPECT_EQ(withoutCredentials(info).toString(), "S3('https://s3.example.com/bucket/backup')");
}

TEST(BackupInfo, WithoutS3CredentialsRedactsURLUserInfo)
{
    auto info = BackupInfo::fromString("S3('https://user:URLPASSWORD@s3.example.com/bucket/backup', 'KEYID', 'KEYSECRET')");

    EXPECT_EQ(withoutCredentials(info).toString(), "S3('https://s3.example.com/bucket/backup')");
}

TEST(BackupInfo, WithoutS3CredentialsRedactsPresignedURLParameters)
{
    auto info = BackupInfo::fromString(
        "S3('https://s3.example.com/bucket/backup?versionId=v1&X-Amz-Signature=URLSIGNATURE&Expires=12345')");

    EXPECT_EQ(withoutCredentials(info).toString(), "S3('https://s3.example.com/bucket/backup?versionId=v1')");
}

TEST(BackupInfo, WithoutS3CredentialsRedactsCombinedURLCredentials)
{
    auto info = BackupInfo::fromString(
        "S3('https://user:URLPASSWORD@s3.example.com/bucket/backup?versionId=v1&X-Amz-Signature=URLSIGNATURE&Expires=12345', "
        "'KEYID', 'KEYSECRET')");

    EXPECT_EQ(withoutCredentials(info).toString(), "S3('https://s3.example.com/bucket/backup?versionId=v1')");
}

TEST(BackupInfo, WithoutS3CredentialsRedactsURLOverride)
{
    auto info = BackupInfo::fromString("S3(collection, url = 'https://s3.example.com/bucket/backup?X-Amz-Signature=URLSIGNATURE')");

    String str = withoutCredentials(info).toString();
    EXPECT_NE(str.find("bucket/backup"), String::npos) << str;
    EXPECT_EQ(str.find("URLSIGNATURE"), String::npos) << str;
}

TEST(BackupInfo, WithoutCredentialsRejectsMalformedKeyValueArgument)
{
    EXPECT_THROW(withoutCredentials(BackupInfo::fromString("S3(collection, equals())")), Exception);

    auto info = BackupInfo::fromString("S3(collection, tuple('key', 'value'))");
    try
    {
        (void)BackupInfo::evaluateKeyValueArgument(info.function_arg, 0, getContext().context);
        FAIL() << "Expected an invalid key-value argument";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.message(), "Invalid backup locator key-value argument");
    }
}

TEST(BackupInfo, WithoutS3CredentialsKeepsPlainQuery)
{
    auto info = BackupInfo::fromString("S3('https://s3.example.com/bucket/backup?foo&encoded=a+b%2B')");

    EXPECT_EQ(withoutCredentials(info).toString(), "S3('https://s3.example.com/bucket/backup?foo&encoded=a+b%2B')");
}

TEST(BackupInfo, WithoutS3CredentialsIsIdempotent)
{
    auto info = BackupInfo::fromString(
        "S3(collection, url = 'https://user:URLPASSWORD@s3.example.com/bucket/backup?versionId=v1&X-Amz-Signature=URLSIGNATURE', "
        "access_key_id = 'KEYID', secret_access_key = 'KEYSECRET', extra_credentials(external_id = 'SECRET_EXTERNAL_ID'))");

    auto once = withoutCredentials(info);
    EXPECT_EQ(withoutCredentials(once).toString(), once.toString());
}

TEST(BackupInfo, WithoutS3CredentialsKeepsOtherEngines)
{
    for (const auto * backup_name : {"Disk('backups', 'path')", "File('path')"})
    {
        auto info = BackupInfo::fromString(backup_name);
        EXPECT_EQ(withoutCredentials(info).toString(), info.toString());
    }
}

TEST(BackupInfo, CopyCredentialsSupportsS3)
{
    checkCopyCredentials(
        "S3('https://s3.example.com/backup', 'KEYID', 'KEYSECRET')",
        "S3('https://s3.example.com/base')",
        "S3('https://s3.example.com/base', 'KEYID', 'KEYSECRET')");
    checkCopyCredentials(
        "S3('https://s3.example.com/backup', 'KEYID', 'KEYSECRET')",
        "S3('https://s3.example.com/base', 'OLDKEY', 'OLDSECRET', 'extra')",
        "S3('https://s3.example.com/base', 'KEYID', 'KEYSECRET')");
    checkCopyCredentials("S3(collection)", "S3('https://s3.example.com/base')", nullptr);
    checkCopyCredentials("S3('https://s3.example.com/backup', 'KEYID', 'KEYSECRET')", "S3(collection)", nullptr);
    checkCopyCredentials("Disk('backups', 'path')", "S3('https://s3.example.com/base')", nullptr);
    checkCopyCredentials("S3('https://s3.example.com/backup')", "S3('https://s3.example.com/base')", nullptr);
}


TEST(BackupInfo, CopyCredentialsDoesNotModifyDestinationOnFailure)
{
    auto source = BackupInfo::fromString("S3('https://s3.example.com/backup', 'KEYID', 'KEYSECRET')");
    auto destination = BackupInfo::fromString("S3('https://s3.example.com/base')");
    const auto expected = BackupInfo::fromString("S3('https://s3.example.com/base', 'OTHERKEY', 'OTHERSECRET')");
    const String original_destination = destination.toString();

    EXPECT_FALSE(BackupFactory::instance().copyCredentials(source, destination, getContext().context, &expected));
    EXPECT_EQ(destination.toString(), original_destination);

    const auto malformed_expected
        = BackupInfo::fromString("S3('https://s3.example.com/base', 'KEYID', 'KEYSECRET', equals())");
    expectExceptionCode(
        [&] { (void)BackupFactory::instance().copyCredentials(source, destination, getContext().context, &malformed_expected); },
        ErrorCodes::BAD_ARGUMENTS);
    EXPECT_EQ(destination.toString(), original_destination);

    auto unsupported_destination = BackupInfo::fromString("S3(url = 'https://s3.example.com/base')");
    const String original_unsupported_destination = unsupported_destination.toString();
    EXPECT_FALSE(BackupFactory::instance().copyCredentials(source, unsupported_destination, getContext().context));
    EXPECT_EQ(unsupported_destination.toString(), original_unsupported_destination);
}


TEST(BackupInfo, CopyCredentialsRequiresContext)
{
    auto source = BackupInfo::fromString("S3('https://s3.example.com/backup', 'KEYID', 'KEYSECRET')");
    auto destination = BackupInfo::fromString("S3('https://s3.example.com/base')");
    expectExceptionCode(
        [&] { (void)BackupFactory::instance().copyCredentials(source, destination, {}); },
        ErrorCodes::BAD_ARGUMENTS);
}


TEST(BackupInfo, EquivalentLocatorsIgnoreKeyValueArgumentOrder)
{
    auto context = getContext().context;
    const auto first = BackupInfo::fromString("S3(collection, url = 's3://bucket/base', access_key_id = 'key')");
    const auto second = BackupInfo::fromString("S3(collection, access_key_id = 'key', url = 's3://bucket/base')");
    const auto different = BackupInfo::fromString("S3(collection, access_key_id = 'other', url = 's3://bucket/base')");

    EXPECT_TRUE(first.isEquivalentTo(second, context));
    EXPECT_FALSE(first.isEquivalentTo(different, context));
}


TEST(BackupInfo, EquivalentLocatorsSupportNonStringValues)
{
    auto context = getContext().context;
    const auto first = BackupInfo::fromString("S3(collection, use_environment_credentials = true, connect_timeout_ms = 1000)");
    const auto second = BackupInfo::fromString("S3(collection, connect_timeout_ms = 1000, use_environment_credentials = true)");
    const auto different = BackupInfo::fromString("S3(collection, connect_timeout_ms = 1001, use_environment_credentials = true)");

    EXPECT_TRUE(first.isEquivalentTo(second, context));
    EXPECT_FALSE(first.isEquivalentTo(different, context));
}


TEST(BackupInfo, EquivalentLocatorsHideInvalidValues)
{
    tryRegisterFunctions();
    auto context = getContext().context;
    const auto info = BackupInfo::fromString("S3(collection, access_key_id = throwIf(1, 'TOPSECRET'))");

    try
    {
        (void)info.isEquivalentTo(info, context);
        FAIL() << "Expected invalid backup locator override";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
        EXPECT_EQ(e.message().find("TOPSECRET"), String::npos);
    }
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
    auto redacted = BackupFactory::instance().withoutCredentials(frozen, context);

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
TEST(BackupInfo, CopyCredentialsSupportsAzure)
{
    const String collection_name = "backup_azure_inline_credentials";
    const String base_collection_name = "backup_azure_inline_base";
    auto create_query = make_intrusive<ASTCreateNamedCollectionQuery>();
    create_query->collection_name = collection_name;
    create_query->changes.emplace_back("container", Field("container"));
    NamedCollectionFactory::instance().createFromSQL(*create_query);

    auto create_base_query = make_intrusive<ASTCreateNamedCollectionQuery>();
    create_base_query->collection_name = base_collection_name;
    create_base_query->changes.emplace_back("container", Field("container"));
    NamedCollectionFactory::instance().createFromSQL(*create_base_query);

    auto drop_collection = [&]
    {
        for (const auto & name : {collection_name, base_collection_name})
        {
            auto drop_query = make_intrusive<ASTDropNamedCollectionQuery>();
            drop_query->collection_name = name;
            drop_query->if_exists = true;
            NamedCollectionFactory::instance().removeFromSQL(*drop_query);
        }
    };
    SCOPE_EXIT({ drop_collection(); });

    auto context = getContext().context;
    auto check_copy = [&](const String & source_str, const String & destination_str, const String & expected_str)
    {
        auto source = BackupInfo::fromString(source_str);
        auto destination = BackupInfo::fromString(destination_str);
        auto expected = BackupInfo::fromString(expected_str);
        const String redacted_destination = destination.toString();

        ASSERT_TRUE(BackupFactory::instance().copyCredentials(source, destination, context, &expected));
        EXPECT_EQ(destination.toString(), redacted_destination);
        EXPECT_TRUE(destination.credentials_source);
        EXPECT_EQ(
            BackupFactory::instance().getDestinationIdentity(destination, context),
            BackupFactory::instance().getDestinationIdentity(expected.freezeNamedCollection(context), context));
    };

    check_copy(
        "AzureBlobStorage('DefaultEndpointsProtocol=https;AccountName=account;AccountKey=SECRET', 'container', 'incremental')",
        "AzureBlobStorage('https://account.blob.core.windows.net', 'container', 'base')",
        "AzureBlobStorage('DefaultEndpointsProtocol=https;AccountName=account;AccountKey=SECRET', 'container', 'base')");
    check_copy(
        "AzureBlobStorage('AccountKey=SECRET;AccountName=account;DefaultEndpointsProtocol=https', 'container', 'incremental')",
        "AzureBlobStorage('https://account.blob.core.windows.net', 'container', 'base')",
        "AzureBlobStorage('DefaultEndpointsProtocol=https;AccountName=account;AccountKey=SECRET', 'container', 'base')");
    check_copy(
        "AzureBlobStorage('https://account.blob.core.windows.net?sig=SECRET', 'container', 'incremental')",
        "AzureBlobStorage('https://account.blob.core.windows.net', 'container', 'base')",
        "AzureBlobStorage('https://account.blob.core.windows.net?sig=SECRET', 'container', 'base')");
    check_copy(
        "AzureBlobStorage('https://account.blob.core.windows.net?sp=rw&sig=SECRET&se=2026-07-25', 'container', 'incremental')",
        "AzureBlobStorage('https://account.blob.core.windows.net', 'container', 'base')",
        "AzureBlobStorage('https://account.blob.core.windows.net?se=2026-07-25&sp=rw&sig=SECRET', 'container', 'base')");
    check_copy(
        "AzureBlobStorage('SharedAccessSignature=sp=rw&sig=SECRET;BlobEndpoint=https://account.blob.core.windows.net', "
        "'container', 'incremental')",
        "AzureBlobStorage('https://account.blob.core.windows.net', 'container', 'base')",
        "AzureBlobStorage('BlobEndpoint=https://account.blob.core.windows.net;SharedAccessSignature=sig=SECRET&sp=rw', "
        "'container', 'base')");
    check_copy(
        "AzureBlobStorage('https://account.blob.core.windows.net', 'container', 'incremental', 'account', 'SECRET')",
        "AzureBlobStorage('https://account.blob.core.windows.net', 'container', 'base')",
        "AzureBlobStorage('https://account.blob.core.windows.net', 'container', 'base', 'account', 'SECRET')");
    check_copy(
        "AzureBlobStorage(" + collection_name
            + ", storage_account_url = 'https://account.blob.core.windows.net', account_name = 'account', "
              "account_key = 'SECRET', blob_path = 'incremental')",
        "AzureBlobStorage(" + base_collection_name
            + ", storage_account_url = 'https://account.blob.core.windows.net', blob_path = 'base')",
        "AzureBlobStorage(" + base_collection_name
            + ", storage_account_url = 'https://account.blob.core.windows.net', account_name = 'account', "
              "account_key = 'SECRET', blob_path = 'base')");

    auto source = BackupInfo::fromString(
        "AzureBlobStorage('DefaultEndpointsProtocol=https;AccountName=account;AccountKey=SECRET', 'container', 'incremental')");
    auto destination = BackupInfo::fromString("AzureBlobStorage('https://account.blob.core.windows.net', 'container', 'base')");
    auto different_syntax = BackupInfo::fromString(
        "AzureBlobStorage('https://account.blob.core.windows.net', 'container', 'base', 'account', 'SECRET')");
    EXPECT_FALSE(BackupFactory::instance().copyCredentials(source, destination, context, &different_syntax));

    auto different_connection_string = BackupInfo::fromString(
        "AzureBlobStorage('DefaultEndpointsProtocol=https;AccountName=account;AccountKey=OTHER', 'container', 'base')");
    EXPECT_FALSE(BackupFactory::instance().copyCredentials(source, destination, context, &different_connection_string));

    auto sas_connection_source = BackupInfo::fromString(
        "AzureBlobStorage('BlobEndpoint=https://account.blob.core.windows.net;SharedAccessSignature=sp=rw&sig=SECRET', "
        "'container', 'incremental')");
    auto sas_connection_destination = BackupInfo::fromString(
        "AzureBlobStorage('https://account.blob.core.windows.net', 'container', 'base')");
    auto different_sas_connection = BackupInfo::fromString(
        "AzureBlobStorage('BlobEndpoint=https://account.blob.core.windows.net;SharedAccessSignature=sp=rw&sig=OTHER', "
        "'container', 'base')");
    EXPECT_FALSE(BackupFactory::instance().copyCredentials(
        sas_connection_source, sas_connection_destination, context, &different_sas_connection));

    auto raw_sas = BackupInfo::fromString(
        "AzureBlobStorage('https://account.blob.core.windows.net?sp=rw&sig=SECRET', 'container', 'base')");
    EXPECT_FALSE(BackupFactory::instance().copyCredentials(sas_connection_source, sas_connection_destination, context, &raw_sas));

    for (const auto * malformed_source_str : {
             "AzureBlobStorage('AccountKey=TOPSECRET', 'container', 'incremental')",
             "AzureBlobStorage('https://account.blob.core.windows.net:A1?sig=TOPSECRET', 'container', 'incremental')"})
    {
        auto malformed_source = BackupInfo::fromString(malformed_source_str);
        auto unchanged_destination = BackupInfo::fromString(
            "AzureBlobStorage('https://account.blob.core.windows.net', 'container', 'base')");
        const String original_destination = unchanged_destination.toString();
        EXPECT_FALSE(BackupFactory::instance().copyCredentials(malformed_source, unchanged_destination, context));
        EXPECT_EQ(unchanged_destination.toString(), original_destination);
        EXPECT_EQ(unchanged_destination.toString().find("TOPSECRET"), String::npos);
    }
}


TEST(BackupInfo, CopyCredentialsSupportsInheritedAzureNamedCollectionCredentials)
{
    const String collection_name = "backup_azure_inherited_credentials";
    const String base_collection_name = "backup_azure_base_without_credentials";
    const String conflicting_base_collection_name = "backup_azure_base_with_conflicting_endpoint";
    const String sas_collection_name = "backup_azure_inherited_sas";
    auto create_query = make_intrusive<ASTCreateNamedCollectionQuery>();
    create_query->collection_name = collection_name;
    create_query->changes.emplace_back("storage_account_url", Field("https://account.blob.core.windows.net"));
    create_query->changes.emplace_back("container", Field("container"));
    create_query->changes.emplace_back("account_name", Field("account"));
    create_query->changes.emplace_back("account_key", Field("SECRET"));
    NamedCollectionFactory::instance().createFromSQL(*create_query);

    auto create_base_query = make_intrusive<ASTCreateNamedCollectionQuery>();
    create_base_query->collection_name = base_collection_name;
    create_base_query->changes.emplace_back("container", Field("container"));
    NamedCollectionFactory::instance().createFromSQL(*create_base_query);

    auto create_conflicting_base_query = make_intrusive<ASTCreateNamedCollectionQuery>();
    create_conflicting_base_query->collection_name = conflicting_base_collection_name;
    create_conflicting_base_query->changes.emplace_back("connection_string", Field("https://other.blob.core.windows.net"));
    create_conflicting_base_query->changes.emplace_back("container", Field("other_container"));
    NamedCollectionFactory::instance().createFromSQL(*create_conflicting_base_query);

    auto create_sas_query = make_intrusive<ASTCreateNamedCollectionQuery>();
    create_sas_query->collection_name = sas_collection_name;
    create_sas_query->changes.emplace_back(
        "storage_account_url", Field("https://account.blob.core.windows.net?sig=SAS_SECRET"));
    create_sas_query->changes.emplace_back("container", Field("container"));
    NamedCollectionFactory::instance().createFromSQL(*create_sas_query);

    auto drop_collection = [&]
    {
        for (const auto & name : {collection_name, base_collection_name, conflicting_base_collection_name, sas_collection_name})
        {
            auto drop_query = make_intrusive<ASTDropNamedCollectionQuery>();
            drop_query->collection_name = name;
            drop_query->if_exists = true;
            NamedCollectionFactory::instance().removeFromSQL(*drop_query);
        }
    };
    SCOPE_EXIT({ drop_collection(); });

    auto context = getContext().context;
    auto source = BackupInfo::fromString("AzureBlobStorage(" + collection_name + ", 'incremental')");
    auto positional_destination = BackupInfo::fromString(
        "AzureBlobStorage('https://account.blob.core.windows.net', 'container', 'base')");
    auto positional_expected = BackupInfo::fromString(
        "AzureBlobStorage('https://account.blob.core.windows.net', 'container', 'base', 'account', 'SECRET')");
    ASSERT_TRUE(BackupFactory::instance().copyCredentials(source, positional_destination, context, &positional_expected));
    EXPECT_EQ(
        BackupFactory::instance().getDestinationIdentity(positional_destination, context),
        BackupFactory::instance().getDestinationIdentity(positional_expected, context));
    EXPECT_EQ(positional_destination.toString().find("SECRET"), String::npos);

    auto named_destination = BackupInfo::fromString(
        "AzureBlobStorage(" + base_collection_name
        + ", storage_account_url = 'https://account.blob.core.windows.net', blob_path = 'base')");
    auto named_expected = BackupInfo::fromString(
            "AzureBlobStorage(" + base_collection_name
            + ", blob_path = 'base', account_key = 'SECRET', account_name = 'account', "
              "storage_account_url = 'https://account.blob.core.windows.net')");
    ASSERT_TRUE(BackupFactory::instance().copyCredentials(source, named_destination, context, &named_expected));
    EXPECT_EQ(
        BackupFactory::instance().getDestinationIdentity(named_destination, context),
        BackupFactory::instance().getDestinationIdentity(named_expected.freezeNamedCollection(context), context));

    auto inherited_destination = BackupInfo::fromString(
        "AzureBlobStorage(" + conflicting_base_collection_name + ", 'base')");
    ASSERT_TRUE(BackupFactory::instance().copyCredentials(source, inherited_destination, context));
    EXPECT_EQ(
        BackupFactory::instance().getDestinationIdentity(inherited_destination, context),
        getDestinationIdentity("AzureBlobStorage('https://account.blob.core.windows.net', 'container', 'base')", context));

    auto sas_source = BackupInfo::fromString("AzureBlobStorage(" + sas_collection_name + ", 'incremental')");
    auto sas_destination = BackupInfo::fromString(
        "AzureBlobStorage('https://account.blob.core.windows.net', 'container', 'base')");
    auto sas_expected = BackupInfo::fromString(
        "AzureBlobStorage('https://account.blob.core.windows.net?sig=SAS_SECRET', 'container', 'base')");
    ASSERT_TRUE(BackupFactory::instance().copyCredentials(sas_source, sas_destination, context, &sas_expected));
    EXPECT_EQ(sas_destination.toString().find("SAS_SECRET"), String::npos);
    EXPECT_EQ(sas_destination.toStringForLogging().find("SAS_SECRET"), String::npos);
    EXPECT_EQ(
        BackupFactory::instance().getDestinationIdentity(sas_destination, context),
        BackupFactory::instance().getDestinationIdentity(sas_expected, context));

    auto different_sas = BackupInfo::fromString(
        "AzureBlobStorage('https://account.blob.core.windows.net?sig=OTHER', 'container', 'base')");
    EXPECT_FALSE(BackupFactory::instance().copyCredentials(sas_source, sas_destination, context, &different_sas));
}


TEST(BackupInfo, WithoutCredentialsRedactsAzureArguments)
{
    for (const auto * locator : {
             "AzureBlobStorage('DefaultEndpointsProtocol=https;AccountName=account;AccountKey=SECRET;EndpointSuffix=core.windows.net', "
             "'container', 'backup')",
             "AzureBlobStorage('https://account.blob.core.windows.net?sv=1&sig=SECRET', 'container', 'backup')",
             "AzureBlobStorage('https://account.blob.core.windows.net', 'container', 'backup', 'account', 'SECRET')"})
    {
        auto redacted = withoutCredentials(BackupInfo::fromString(locator));
        EXPECT_EQ(redacted.args.size(), 3);
        EXPECT_EQ(redacted.toString().find("SECRET"), String::npos);
        EXPECT_NE(redacted.toString().find("https://account.blob.core.windows.net"), String::npos);
    }
}


TEST(BackupInfo, WithoutCredentialsRedactsAzureNamedCollectionOverrides)
{
    auto info = BackupInfo::fromString(
        "AzureBlobStorage(collection, "
        "connection_string = 'DefaultEndpointsProtocol=https;AccountName=account;AccountKey=CONNECTION_SECRET;EndpointSuffix=core.windows.net', "
        "storage_account_url = 'https://account.blob.core.windows.net?sig=SAS_SECRET', "
        "account_name = 'account', account_key = 'ACCOUNT_SECRET', client_id = 'CLIENT_SECRET', tenant_id = 'TENANT_SECRET', "
        "blob_path = 'backup', extra_credentials(account_key = 'FUNCTION_SECRET'))");

    String str = withoutCredentials(info).toString();
    EXPECT_NE(str.find("connection_string"), String::npos);
    EXPECT_NE(str.find("storage_account_url"), String::npos);
    EXPECT_NE(str.find("blob_path"), String::npos);
    EXPECT_EQ(str.find("SECRET"), String::npos) << str;
}


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
