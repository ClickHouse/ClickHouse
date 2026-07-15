#include <Backups/BackupInfo.h>

#include "config.h"

#include <Common/Exception.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Parsers/ASTCreateNamedCollectionQuery.h>
#include <Parsers/ASTDropNamedCollectionQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <base/scope_guard.h>

#include <Poco/Util/MapConfiguration.h>

#include <gtest/gtest.h>

#include <cstdlib>
#include <iostream>


using namespace DB;

namespace DB::ErrorCodes
{
    extern const int NAMED_COLLECTION_DOESNT_EXIST;
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

    ContextMutablePtr makeContextWithBackupAllowedDisk(const String & disk_name)
    {
        auto context = Context::createCopy(getContext().context);

        Poco::AutoPtr<Poco::Util::MapConfiguration> config(new Poco::Util::MapConfiguration);
        config->setString("backups.allowed_disk", disk_name);
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

TEST(BackupInfo, NormalizedStringKeepsS3UrlPlainQueryAsKey)
{
    auto first = BackupInfo::fromString("S3('s3://bucket/backup?part=1')");
    auto second = BackupInfo::fromString("S3('s3://bucket/backup?part=2')");

    EXPECT_NE(first.toNormalizedString(), second.toNormalizedString());
    EXPECT_NE(first.toNormalizedString().find("part=1"), String::npos);
    EXPECT_NE(second.toNormalizedString().find("part=2"), String::npos);
}

TEST(BackupInfo, NormalizedStringKeepsS3PathPlainQueryAsKey)
{
    auto first = BackupInfo::fromString("S3(collection, 'backup?part=1')");
    auto second = BackupInfo::fromString("S3(collection, 'backup?part=2')");

    EXPECT_NE(first.toNormalizedString(), second.toNormalizedString());
    EXPECT_NE(first.toNormalizedString().find("part=1"), String::npos);
    EXPECT_NE(second.toNormalizedString().find("part=2"), String::npos);
}

TEST(BackupInfo, NormalizedStringDoesNotTreatS3QueryAtAsUserInfo)
{
    auto info = BackupInfo::fromString("S3('https://bucket.s3.amazonaws.com?versionId=@v1')");
    String normalized = info.toNormalizedString();

    EXPECT_NE(normalized.find("endpoint=https://s3.amazonaws.com"), String::npos);
    EXPECT_NE(normalized.find("bucket=bucket"), String::npos);
    EXPECT_NE(normalized.find("version_id=@v1"), String::npos);
}

TEST(BackupInfo, NormalizedStringCanonicalizesEquivalentS3Urls)
{
    auto s3 = BackupInfo::fromString("S3('s3://bucket/backup')");
    auto virtual_hosted = BackupInfo::fromString("S3('https://bucket.s3.amazonaws.com/backup')");
    auto path_style = BackupInfo::fromString("S3('https://s3.amazonaws.com/bucket/backup')");

    EXPECT_EQ(s3.toNormalizedString(), virtual_hosted.toNormalizedString());
    EXPECT_EQ(s3.toNormalizedString(), path_style.toNormalizedString());
}

TEST(BackupInfo, NormalizedStringDistinguishesS3ArchiveMode)
{
    auto archive = BackupInfo::fromString("S3('s3://bucket/backup.zip')");
    auto directory_with_fragment = BackupInfo::fromString("S3('s3://bucket/backup.zip#directory')");
    auto directory_with_slash = BackupInfo::fromString("S3('s3://bucket/backup.zip/')");

    EXPECT_NE(archive.toNormalizedString(), directory_with_fragment.toNormalizedString());
    EXPECT_EQ(directory_with_fragment.toNormalizedString(), directory_with_slash.toNormalizedString());
}

TEST(BackupInfo, NormalizedStringEncodesS3FieldsUnambiguously)
{
    auto key_contains_delimiter = BackupInfo::fromString("S3('s3://bucket/foo;version_id=bar?versionId=baz')");
    auto version_contains_delimiter = BackupInfo::fromString("S3('s3://bucket/foo?versionId=bar;version_id=baz')");

    EXPECT_NE(key_contains_delimiter.toNormalizedString(), version_contains_delimiter.toNormalizedString());
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

TEST(BackupInfo, NormalizedStringUsesFrozenS3NamedCollectionWithoutContext)
{
    auto first = BackupInfo::fromString("S3(collection, 'backup')");
    auto second = BackupInfo::fromString("S3(collection, 'backup/')");

    first.frozen_named_collection = makeNamedCollection({{"url", "s3://bucket/base"}});
    second.frozen_named_collection = makeNamedCollection({{"url", "s3://bucket/base"}});

    EXPECT_EQ(first.toNormalizedString(ContextPtr{}), second.toNormalizedString(ContextPtr{}));
    EXPECT_EQ(first.toNormalizedString(), second.toNormalizedString());
    EXPECT_EQ(first.toNormalizedString(), first.toNormalizedString(ContextPtr{}));
}

#if USE_AWS_S3
TEST(BackupInfo, NormalizedStringUsesS3BackupPathJoinSemantics)
{
    auto context = getContext().context;
    auto info = BackupInfo::fromString("S3(collection, '/absolute')");
    info.frozen_named_collection = makeNamedCollection({{"url", "s3://bucket/base"}});

    EXPECT_THROW((void)info.toNormalizedString(context), Exception);
}
#endif

TEST(BackupInfo, NormalizedStringRejectsUnresolvedNamedCollectionWithoutContext)
{
    auto info = BackupInfo::fromString("S3(collection)");

    EXPECT_THROW((void)info.toNormalizedString(ContextPtr{}), Exception);
}

TEST(BackupInfo, NormalizedStringRejectsInvalidBackupEngineShapes)
{
    auto context = getContext().context;

    EXPECT_THROW(
        (void)BackupInfo::fromString("S3('s3://bucket/backup', 'orphan')").toNormalizedString(),
        Exception);
    EXPECT_THROW(
        (void)BackupInfo::fromString("S3('s3://bucket/backup', 1, 2)").toNormalizedString(),
        Exception);

    auto s3_collection = BackupInfo::fromString("S3(collection, 'a', 'b')");
    s3_collection.frozen_named_collection = makeNamedCollection({{"url", "s3://bucket/base"}});
    EXPECT_THROW((void)s3_collection.toNormalizedString(context), Exception);

    EXPECT_THROW(
        (void)BackupInfo::fromString("AzureBlobStorage('url', 'container')").toNormalizedString(),
        Exception);
    EXPECT_THROW(
        (void)BackupInfo::fromString("AzureBlobStorage('url', 'container', 'path', 1, 2)").toNormalizedString(),
        Exception);
    EXPECT_THROW((void)BackupInfo::fromString("File(collection, 'path')").toNormalizedString(context), Exception);
    EXPECT_THROW((void)BackupInfo::fromString("Disk('backups')").toNormalizedString(), Exception);
    EXPECT_THROW((void)BackupInfo::fromString("Memory()").toNormalizedString(), Exception);
    EXPECT_THROW((void)BackupInfo::fromString("Memory(collection)").toNormalizedString(context), Exception);
    EXPECT_THROW((void)BackupInfo::fromString("Null('x')").toNormalizedString(), Exception);
    EXPECT_THROW((void)BackupInfo::fromString("Unknown('x')").toNormalizedString(), Exception);
}

TEST(BackupInfo, NormalizedStringSupportsNullAndRejectsMemoryEngine)
{
    auto ignored_null_arguments = BackupInfo::fromString(
        "Null(missing_collection, url=throwIf(1), extra_credentials(foo='bar'))");

    EXPECT_THROW((void)BackupInfo::fromString("Memory('backup')").toNormalizedString(), Exception);
    EXPECT_EQ(BackupInfo::fromString("Null()").toNormalizedString(), "Null()");
    EXPECT_EQ(BackupInfo::fromString("Null(collection)").toNormalizedString(), "Null()");
    EXPECT_EQ(BackupInfo::fromString("Null(collection)").toNormalizedString(ContextPtr{}), "Null()");
    EXPECT_EQ(ignored_null_arguments.toNormalizedString(ContextPtr{}), "Null()");
    EXPECT_THROW((void)BackupInfo::fromString("Null(collection, 'x')").toNormalizedString(), Exception);

    auto frozen = ignored_null_arguments.freezeNamedCollection(ContextPtr{});
    EXPECT_EQ(frozen.id_arg, ignored_null_arguments.id_arg);
    EXPECT_EQ(frozen.kv_args.size(), ignored_null_arguments.kv_args.size());
    EXPECT_EQ(frozen.function_arg, ignored_null_arguments.function_arg);
    EXPECT_FALSE(frozen.frozen_named_collection);
}

#if USE_AWS_S3
TEST(BackupInfo, NormalizedStringValidatesS3ExtraCredentials)
{
    auto context = getContext().context;
    auto valid = BackupInfo::fromString("S3('s3://bucket/backup', extra_credentials(role_arn = 'ROLEARN'))");
    auto invalid = BackupInfo::fromString("S3('s3://bucket/backup', extra_credentials(unknown = 'TOPSECRET'))");
    auto ignored_for_named_collection = BackupInfo::fromString("S3(collection, extra_credentials(unknown = 'UNKNOWN'))");
    ignored_for_named_collection.frozen_named_collection = makeNamedCollection({{"url", "s3://bucket/base"}});

    EXPECT_NO_THROW((void)valid.toNormalizedString(context));
    EXPECT_THROW((void)valid.toNormalizedString(), Exception);
    EXPECT_NO_THROW((void)ignored_for_named_collection.toNormalizedString(context));

    try
    {
        (void)invalid.toNormalizedString(context);
        FAIL() << "Expected invalid extra credentials exception";
    }
    catch (const Exception & e)
    {
        EXPECT_NE(e.message().find("extra_credentials"), String::npos);
        EXPECT_EQ(e.message().find("TOPSECRET"), String::npos);
    }

    try
    {
        (void)BackupInfo::fromString(
            "S3('s3://bucket/backup', extra_credentials(foo = 'TOPSECRET'), 'trailing')");
        FAIL() << "Expected misplaced extra credentials exception";
    }
    catch (const Exception & e)
    {
        EXPECT_NE(e.message().find("extra_credentials"), String::npos);
        EXPECT_EQ(e.message().find("TOPSECRET"), String::npos);
    }
}
#endif

TEST(BackupInfo, NormalizedStringCanonicalizesDiskPath)
{
    auto context = makeContextWithBackupAllowedDisk("default");
    auto first = BackupInfo::fromString("Disk('default', 'dir/../backup/')");
    auto second = BackupInfo::fromString("Disk('default', 'backup')");

    EXPECT_EQ(first.toNormalizedString(context), second.toNormalizedString(context));
}

TEST(BackupInfo, NormalizedStringRejectsDisallowedDiskWithContext)
{
    auto context = makeContextWithBackupAllowedDisk("other_disk");
    auto info = BackupInfo::fromString("Disk('default', 'backup')");

    EXPECT_THROW((void)info.toNormalizedString(context), Exception);
}

TEST(BackupInfo, NormalizedStringCanonicalizesDiskRootPath)
{
    auto context = makeContextWithBackupAllowedDisk("default");
    auto empty = BackupInfo::fromString("Disk('default', '')");
    auto dot = BackupInfo::fromString("Disk('default', '.')");

    EXPECT_EQ(empty.toNormalizedString(context), dot.toNormalizedString(context));
}

TEST(BackupInfo, NormalizedStringCanonicalizesFilePath)
{
    auto context = makeContextWithBackupAllowedPaths();
    auto first = BackupInfo::fromString("File('dir/../backup/')");
    auto second = BackupInfo::fromString("File('backup')");

    EXPECT_EQ(first.toNormalizedString(context), second.toNormalizedString(context));
}

TEST(BackupInfo, NormalizedStringPreservesArchiveDirectoryPath)
{
    auto context = makeContextWithBackupAllowedPaths();
    auto archive_file = BackupInfo::fromString("File('backup.zip')");
    auto archive_directory = BackupInfo::fromString("File('backup.zip/')");

    EXPECT_NE(archive_file.toNormalizedString(context), archive_directory.toNormalizedString(context));
}

TEST(BackupInfo, NormalizedStringRequiresContextForFileAndDisk)
{
    auto file = BackupInfo::fromString("File('backup')");
    auto disk = BackupInfo::fromString("Disk('backups', 'backup')");

    EXPECT_THROW((void)file.toNormalizedString(), Exception);
    EXPECT_THROW((void)file.toNormalizedString(ContextPtr{}), Exception);
    EXPECT_THROW((void)disk.toNormalizedString(), Exception);
    EXPECT_THROW((void)disk.toNormalizedString(ContextPtr{}), Exception);
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

TEST(BackupInfo, NormalizedStringIncludesOnlyEffectiveNamedCollectionOverrides)
{
    auto base = BackupInfo::fromString("S3(collection)");
    auto locator_override = BackupInfo::fromString("S3(collection, url='https://user:password@s3.example.com/bucket/backup')");
    auto ignored_case_mismatch = BackupInfo::fromString("S3(collection, URL='https://user:password@s3.example.com/bucket/backup')");
    auto ignored_non_locator = BackupInfo::fromString("S3(collection, storage_class_name='STANDARD_IA')");

    EXPECT_NE(base.toNormalizedString(), locator_override.toNormalizedString());
    EXPECT_EQ(base.toNormalizedString(), ignored_case_mismatch.toNormalizedString());
    EXPECT_EQ(base.toNormalizedString(), ignored_non_locator.toNormalizedString());
    EXPECT_EQ(locator_override.toNormalizedString().find("password"), String::npos);
    EXPECT_EQ(ignored_case_mismatch.toNormalizedString().find("password"), String::npos);
    EXPECT_EQ(ignored_non_locator.toNormalizedString().find("STANDARD_IA"), String::npos);
}

TEST(BackupInfo, NormalizedStringValidatesUnresolvedS3NamedCollectionFilename)
{
    auto base = BackupInfo::fromString("S3(collection)");
    auto empty_positional = BackupInfo::fromString("S3(collection, '')");
    auto empty_override = BackupInfo::fromString("S3(collection, filename='')");
    auto absolute_positional = BackupInfo::fromString("S3(collection, '/backup')");
    auto absolute_override = BackupInfo::fromString("S3(collection, filename='/backup')");

    EXPECT_NO_THROW((void)base.toNormalizedString());
    EXPECT_THROW((void)empty_positional.toNormalizedString(), Exception);
    EXPECT_THROW((void)empty_override.toNormalizedString(), Exception);
    EXPECT_THROW((void)absolute_positional.toNormalizedString(), Exception);
    EXPECT_THROW((void)absolute_override.toNormalizedString(), Exception);
}

TEST(BackupInfo, NormalizedStringResolvesEmptyS3NamedCollectionFilename)
{
    auto archive = BackupInfo::fromString("S3(collection)");
    auto archive_with_empty_filename = BackupInfo::fromString("S3(collection, '')");
    archive.frozen_named_collection = makeNamedCollection({{"url", "s3://bucket/backup.zip"}});
    archive_with_empty_filename.frozen_named_collection = makeNamedCollection({{"url", "s3://bucket/backup.zip"}});

    EXPECT_NE(archive.toNormalizedString(), archive_with_empty_filename.toNormalizedString());

    const String collection_name = "backup_info_empty_filename_override";
    auto create_query = make_intrusive<ASTCreateNamedCollectionQuery>();
    create_query->collection_name = collection_name;
    create_query->changes.emplace_back("url", Field("s3://bucket/base"));
    create_query->changes.emplace_back("filename", Field("stored"));
    create_query->overridability.emplace("filename", true);
    NamedCollectionFactory::instance().createFromSQL(*create_query);

    SCOPE_EXIT({
        auto drop_query = make_intrusive<ASTDropNamedCollectionQuery>();
        drop_query->collection_name = collection_name;
        drop_query->if_exists = true;
        NamedCollectionFactory::instance().removeFromSQL(*drop_query);
    });

    auto context = getContext().context;
    auto stored_filename = BackupInfo::fromString("S3(" + collection_name + ")");
    auto empty_override = BackupInfo::fromString("S3(" + collection_name + ", filename='')");

    EXPECT_NE(stored_filename.toNormalizedString(context), empty_override.toNormalizedString(context));
}

TEST(BackupInfo, NormalizedStringIgnoresCaseInsensitiveAndGoogleAdcCredentialOverrides)
{
    auto base = BackupInfo::fromString("S3(collection)");
    auto with_credentials = BackupInfo::fromString(
        "S3(collection, ACCESS_KEY_ID='KEYID', google_adc_client_id='CLIENTID', google_adc_client_secret='CLIENTSECRET', "
        "google_adc_refresh_token='REFRESHTOKEN')");

    EXPECT_EQ(base.toNormalizedString(), with_credentials.toNormalizedString());
    EXPECT_EQ(with_credentials.toNormalizedString().find("KEYID"), String::npos);
    EXPECT_EQ(with_credentials.toNormalizedString().find("CLIENTID"), String::npos);
    EXPECT_EQ(with_credentials.toNormalizedString().find("CLIENTSECRET"), String::npos);
    EXPECT_EQ(with_credentials.toNormalizedString().find("REFRESHTOKEN"), String::npos);
}

TEST(BackupInfo, NormalizedStringIgnoresAzureNonLocatorNamedCollectionOverrides)
{
    auto base = BackupInfo::fromString("AzureBlobStorage(collection)");
    auto locator_override = BackupInfo::fromString("AzureBlobStorage(collection, blob_path='backup')");
    auto ignored_non_locator = BackupInfo::fromString("AzureBlobStorage(collection, format='CSV')");

    EXPECT_NE(base.toNormalizedString(), locator_override.toNormalizedString());
    EXPECT_EQ(base.toNormalizedString(), ignored_non_locator.toNormalizedString());
    EXPECT_EQ(ignored_non_locator.toNormalizedString().find("CSV"), String::npos);
}

TEST(BackupInfo, NormalizedStringIgnoresShadowedAzureNamedCollectionOverrides)
{
    auto positional_path = BackupInfo::fromString("AzureBlobStorage(collection, 'backup')");
    auto positional_path_with_shadowed_override = BackupInfo::fromString(
        "AzureBlobStorage(collection, 'backup', blob_path='ignored')");
    auto connection_string = BackupInfo::fromString(
        "AzureBlobStorage(collection, connection_string='https://account.blob.core.windows.net')");
    auto connection_string_with_shadowed_url = BackupInfo::fromString(
        "AzureBlobStorage(collection, storage_account_url='https://ignored.blob.core.windows.net', "
        "connection_string='https://account.blob.core.windows.net')");

    EXPECT_EQ(positional_path.toNormalizedString(), positional_path_with_shadowed_override.toNormalizedString());
    EXPECT_THROW((void)connection_string.toNormalizedString(), Exception);
    EXPECT_THROW((void)connection_string_with_shadowed_url.toNormalizedString(), Exception);
}

TEST(BackupInfo, NormalizedStringResolvesAzureConnectionOverridePrecedence)
{
    const String collection_name = "backup_info_azure_connection_precedence";

    auto create_query = make_intrusive<ASTCreateNamedCollectionQuery>();
    create_query->collection_name = collection_name;
    create_query->changes.emplace_back("connection_string", Field("https://account.blob.core.windows.net"));
    create_query->changes.emplace_back("storage_account_url", Field("https://base-ignored.blob.core.windows.net"));
    create_query->changes.emplace_back("container", Field("container"));
    create_query->changes.emplace_back("blob_path", Field("backup"));
    create_query->overridability.emplace("storage_account_url", true);
    NamedCollectionFactory::instance().createFromSQL(*create_query);

    SCOPE_EXIT({
        auto drop_query = make_intrusive<ASTDropNamedCollectionQuery>();
        drop_query->collection_name = collection_name;
        drop_query->if_exists = true;
        NamedCollectionFactory::instance().removeFromSQL(*drop_query);
    });

    auto context = getContext().context;
    auto base = BackupInfo::fromString("AzureBlobStorage(" + collection_name + ")");
    auto shadowed_override = BackupInfo::fromString(
        "AzureBlobStorage(" + collection_name + ", storage_account_url='https://ignored.blob.core.windows.net')");

    EXPECT_EQ(base.toNormalizedString(context), shadowed_override.toNormalizedString(context));
}

TEST(BackupInfo, NormalizedStringRejectsNonOverridableNamedCollectionOverride)
{
    const String collection_name = "backup_info_non_overridable_url";

    auto create_query = make_intrusive<ASTCreateNamedCollectionQuery>();
    create_query->collection_name = collection_name;
    create_query->changes.emplace_back("url", Field("s3://bucket/base"));
    create_query->overridability.emplace("url", false);
    NamedCollectionFactory::instance().createFromSQL(*create_query);

    SCOPE_EXIT({
        auto drop_query = make_intrusive<ASTDropNamedCollectionQuery>();
        drop_query->collection_name = collection_name;
        drop_query->if_exists = true;
        NamedCollectionFactory::instance().removeFromSQL(*drop_query);
    });

    auto context = getContext().context;
    auto info = BackupInfo::fromString("S3(" + collection_name + ", url='s3://bucket/other')");

    EXPECT_THROW((void)info.toNormalizedString(context), Exception);
}

TEST(BackupInfo, NormalizedStringRejectsDuplicateNamedCollectionOverrides)
{
    const String collection_name = "backup_info_duplicate_override";

    auto create_query = make_intrusive<ASTCreateNamedCollectionQuery>();
    create_query->collection_name = collection_name;
    create_query->changes.emplace_back("url", Field("s3://bucket/base"));
    create_query->overridability.emplace("url", true);
    NamedCollectionFactory::instance().createFromSQL(*create_query);

    SCOPE_EXIT({
        auto drop_query = make_intrusive<ASTDropNamedCollectionQuery>();
        drop_query->collection_name = collection_name;
        drop_query->if_exists = true;
        NamedCollectionFactory::instance().removeFromSQL(*drop_query);
    });

    auto context = getContext().context;
    auto first = BackupInfo::fromString(
        "S3(" + collection_name + ", url='s3://bucket/a', url='s3://bucket/b')");
    auto second = BackupInfo::fromString(
        "S3(" + collection_name + ", url='s3://bucket/b', url='s3://bucket/a')");
    auto malformed = BackupInfo::fromString("S3(" + collection_name + ", equals(url))");
    auto malformed_secret = BackupInfo::fromString(
        "S3(" + collection_name + ", equals(secret_access_key, 'TOPSECRET', 'extra'))");

    ASTs default_args{make_intrusive<ASTIdentifier>(collection_name)};
    default_args.insert(default_args.end(), first.kv_args.begin(), first.kv_args.end());
    auto default_collection = tryGetNamedCollectionWithOverrides(default_args, context);

    EXPECT_EQ(default_collection->get<String>("url"), "s3://bucket/b");
    EXPECT_THROW((void)first.getNamedCollection(context), Exception);
    EXPECT_THROW((void)malformed.getNamedCollection(context), Exception);
    EXPECT_THROW((void)first.toNormalizedString(), Exception);
    EXPECT_THROW((void)second.toNormalizedString(context), Exception);
    EXPECT_THROW((void)malformed.toNormalizedString(context), Exception);

    try
    {
        (void)malformed_secret.getNamedCollection(context);
        FAIL() << "Expected malformed secret override exception";
    }
    catch (const Exception & e)
    {
        EXPECT_NE(e.message().find("equals"), String::npos);
        EXPECT_EQ(e.message().find("TOPSECRET"), String::npos);
    }

    try
    {
        (void)malformed_secret.toNormalizedString();
        FAIL() << "Expected malformed secret override normalization exception";
    }
    catch (const Exception & e)
    {
        EXPECT_NE(e.message().find("equals"), String::npos);
        EXPECT_EQ(e.message().find("TOPSECRET"), String::npos);
    }
}

TEST(BackupInfo, NormalizedStringChecksNamedCollectionBeforeOverrides)
{
    auto context = getContext().context;
    auto info = BackupInfo::fromString("S3(backup_info_missing_collection, url=throwIf(1))");

    try
    {
        (void)info.toNormalizedString(context);
        FAIL() << "Expected a missing named collection exception";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::NAMED_COLLECTION_DOESNT_EXIST);
    }
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
    auto uppercase_scheme = BackupInfo::fromString(
        "AzureBlobStorage('HTTPS://account.blob.core.windows.net', 'container', 'backup', 'account', 'key3')");
    auto connection_string = BackupInfo::fromString(
        "AzureBlobStorage('DefaultEndpointsProtocol=https;AccountName=account;AccountKey=key', 'container', 'backup', 'account', 'key')");
    auto sas_url = BackupInfo::fromString(
        "AzureBlobStorage('https://account.blob.core.windows.net?sig=secret', 'container', 'backup', 'account', 'key')");
    auto invalid_scheme = BackupInfo::fromString(
        "AzureBlobStorage('httpx://account.blob.core.windows.net', 'container', 'backup', 'account', 'key')");
    auto missing_host = BackupInfo::fromString(
        "AzureBlobStorage('https:///', 'container', 'backup', 'account', 'key')");
    auto leak_prone_value = BackupInfo::fromString(
        "AzureBlobStorage('httpAccountKey=SECRET', 'container', 'backup', 'account', 'key')");
    auto trailing_query_marker = BackupInfo::fromString(
        "AzureBlobStorage('https://account.blob.core.windows.net?', 'container', 'backup', 'account', 'key')");
    auto trailing_fragment_marker = BackupInfo::fromString(
        "AzureBlobStorage('https://account.blob.core.windows.net#', 'container', 'backup', 'account', 'key')");
    auto empty_userinfo = BackupInfo::fromString(
        "AzureBlobStorage('https://@account.blob.core.windows.net', 'container', 'backup', 'account', 'key')");

    EXPECT_EQ(first.toNormalizedString(), second.toNormalizedString());
    EXPECT_EQ(first.toNormalizedString(), uppercase_scheme.toNormalizedString());
    EXPECT_THROW((void)connection_string.toNormalizedString(), Exception);
    EXPECT_THROW((void)connection_string.toNormalizedString(getContext().context), Exception);
    EXPECT_THROW((void)sas_url.toNormalizedString(), Exception);
    EXPECT_THROW((void)invalid_scheme.toNormalizedString(), Exception);
    EXPECT_THROW((void)missing_host.toNormalizedString(), Exception);
    EXPECT_THROW((void)leak_prone_value.toNormalizedString(), Exception);
    EXPECT_THROW((void)trailing_query_marker.toNormalizedString(), Exception);
    EXPECT_THROW((void)trailing_fragment_marker.toNormalizedString(), Exception);
    EXPECT_THROW((void)empty_userinfo.toNormalizedString(), Exception);
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

TEST(BackupInfo, NormalizedStringRejectsAzureURLFragments)
{
    auto clean = BackupInfo::fromString(
        "AzureBlobStorage('https://account.blob.core.windows.net', 'container', 'backup')");
    auto fragment = BackupInfo::fromString(
        "AzureBlobStorage('https://account.blob.core.windows.net#fragment', 'container', 'backup')");
    auto uppercase_url = BackupInfo::fromString(
        "AzureBlobStorage('HTTPS://account.blob.core.windows.net', 'container', 'backup')");
    auto uppercase_fragment = BackupInfo::fromString(
        "AzureBlobStorage('HTTPS://account.blob.core.windows.net#fragment', 'container', 'backup')");
    auto sas_with_fragment_character = BackupInfo::fromString(
        "AzureBlobStorage('https://account.blob.core.windows.net?sig=secret#credential', 'container', 'backup')");
    auto connection_string_fragment = BackupInfo::fromString(
        "AzureBlobStorage('BlobEndpoint=https://account.blob.core.windows.net#fragment;AccountName=account;AccountKey=key', "
        "'container', 'backup')");
    auto uppercase_connection_string_fragment = BackupInfo::fromString(
        "AzureBlobStorage('BlobEndpoint=HTTPS://account.blob.core.windows.net#fragment;AccountName=account;AccountKey=key', "
        "'container', 'backup')");

    EXPECT_THROW((void)fragment.toNormalizedString(), Exception);
    EXPECT_THROW((void)uppercase_url.toNormalizedString(), Exception);
    EXPECT_THROW((void)uppercase_url.toNormalizedString(getContext().context), Exception);
    EXPECT_THROW((void)uppercase_fragment.toNormalizedString(), Exception);
    EXPECT_THROW((void)uppercase_fragment.toNormalizedString(getContext().context), Exception);
    EXPECT_THROW((void)connection_string_fragment.toNormalizedString(), Exception);
    EXPECT_THROW((void)connection_string_fragment.toNormalizedString(getContext().context), Exception);
    EXPECT_THROW((void)uppercase_connection_string_fragment.toNormalizedString(), Exception);
    EXPECT_EQ(clean.toNormalizedString(), sas_with_fragment_character.toNormalizedString());
}

#if USE_AZURE_BLOB_STORAGE
TEST(BackupInfo, NormalizedStringRedactsAzureURLCredentials)
{
    auto direct = BackupInfo::fromString(
        "AzureBlobStorage('https://user:password@account.blob.core.windows.net', 'container', 'backup')");
    auto connection_string = BackupInfo::fromString(
        "AzureBlobStorage('BlobEndpoint=https://account.blob.core.windows.net;SharedAccessSignature=sig=secret;AccountName=account', "
        "'container', 'backup')");

    EXPECT_EQ(direct.toNormalizedString().find("password"), String::npos);
    EXPECT_EQ(connection_string.toNormalizedString().find("secret"), String::npos);
}

TEST(BackupInfo, NormalizedStringUsesAzureConnectionStringParserSemantics)
{
    auto with_ignored_lowercase_keys = BackupInfo::fromString(
        "AzureBlobStorage('DefaultEndpointsProtocol=https;AccountName=accounta;AccountKey=key1;EndpointSuffix=core.windows.net;"
        "accountname=accountb;endpointsuffix=example.com', 'container', 'backup')");
    auto effective_destination = BackupInfo::fromString(
        "AzureBlobStorage('DefaultEndpointsProtocol=https;AccountName=accounta;AccountKey=key2;EndpointSuffix=core.windows.net', "
        "'container', 'backup')");
    auto ignored_destination = BackupInfo::fromString(
        "AzureBlobStorage('DefaultEndpointsProtocol=https;AccountName=accountb;AccountKey=key3;EndpointSuffix=example.com', "
        "'container', 'backup')");
    auto invalid_blob_endpoint = BackupInfo::fromString(
        "AzureBlobStorage('BlobEndpoint=https://user:password@account.blob.core.windows.net;AccountName=account;AccountKey=key', "
        "'container', 'backup')");

    EXPECT_EQ(with_ignored_lowercase_keys.toNormalizedString(), effective_destination.toNormalizedString());
    EXPECT_NE(with_ignored_lowercase_keys.toNormalizedString(), ignored_destination.toNormalizedString());
    EXPECT_THROW((void)invalid_blob_endpoint.toNormalizedString(), Exception);
}

TEST(BackupInfo, NormalizedStringCanonicalizesAzureEndpointTrailingSlash)
{
    auto no_slash = BackupInfo::fromString(
        "AzureBlobStorage('https://account.blob.core.windows.net', 'container', 'backup')");
    auto one_slash = BackupInfo::fromString(
        "AzureBlobStorage('https://account.blob.core.windows.net/', 'container', 'backup')");
    auto two_slashes = BackupInfo::fromString(
        "AzureBlobStorage('https://account.blob.core.windows.net//', 'container', 'backup')");
    auto archive_name = BackupInfo::fromString(
        "AzureBlobStorage('https://account.blob.core.windows.net/account.zip', 'container', 'backup')");
    auto archive_name_with_slash = BackupInfo::fromString(
        "AzureBlobStorage('https://account.blob.core.windows.net/account.zip/', 'container', 'backup')");
    auto prefix = BackupInfo::fromString(
        "AzureBlobStorage('https://account.blob.core.windows.net/prefix', 'container', 'backup')");
    auto prefix_with_slash = BackupInfo::fromString(
        "AzureBlobStorage('https://account.blob.core.windows.net/prefix/', 'container', 'backup')");
    auto prefix_with_two_slashes = BackupInfo::fromString(
        "AzureBlobStorage('https://account.blob.core.windows.net/prefix//', 'container', 'backup')");

    EXPECT_EQ(no_slash.toNormalizedString(), one_slash.toNormalizedString());
    EXPECT_EQ(no_slash.toNormalizedString(), two_slashes.toNormalizedString());
    EXPECT_EQ(archive_name.toNormalizedString(), archive_name_with_slash.toNormalizedString());
    EXPECT_EQ(prefix.toNormalizedString(), prefix_with_slash.toNormalizedString());
    EXPECT_NE(prefix.toNormalizedString(), prefix_with_two_slashes.toNormalizedString());
}
#endif

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

TEST(BackupInfo, NormalizedStringValidatesFrozenAzureExplicitCredentialsURL)
{
    auto context = getContext().context;
    auto info = BackupInfo::fromString("AzureBlobStorage(collection)");
    info.frozen_named_collection = makeNamedCollection(
        {
            {"connection_string", "DefaultEndpointsProtocol=https;AccountName=account;AccountKey=key"},
            {"container", "container"},
            {"blob_path", "backup"},
            {"account_name", "account"},
            {"account_key", "key"},
        });

    EXPECT_THROW((void)info.toNormalizedString(context), Exception);
}

TEST(BackupInfo, NormalizedStringRejectsFrozenAzureURLFragment)
{
    auto context = getContext().context;
    auto info = BackupInfo::fromString("AzureBlobStorage(collection)");
    info.frozen_named_collection = makeNamedCollection(
        {
            {"storage_account_url", "https://account.blob.core.windows.net#fragment"},
            {"container", "container"},
            {"blob_path", "backup"},
        });

    EXPECT_THROW((void)info.toNormalizedString(context), Exception);
}

TEST(BackupInfo, FreezeNamedCollectionPreservesResolvedSnapshot)
{
    const String collection_name = "backup_info_frozen_snapshot";

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
    auto info = BackupInfo::fromString("S3(" + collection_name + ", url='s3://bucket/overridden')");
    auto frozen = info.freezeNamedCollection(context);
    String identity = frozen.toNormalizedString();

    EXPECT_TRUE(frozen.frozen_named_collection->isQueryOverridden("url"));
    drop_collection();
    EXPECT_EQ(frozen.toNormalizedString(), identity);
}
