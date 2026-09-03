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
        if (!str.contains(expected))
        {
            std::cerr << "Expected to find " << expected << " in " << str << '\n';
            std::_Exit(1);
        }
    }

    void requireNotContains(const String & str, const String & unexpected)
    {
        if (str.contains(unexpected))
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

    [[noreturn]] void checkExpressionRoleArnValueWithContext()
    {
        tryRegisterFunctions();
        const auto & context = getContext().context;
        /// `collectCredentials` resolves the value when opening the locator, so one written as an
        /// expression names a role just as well and has to survive into the metadata.
        auto info = BackupInfo::fromString(
            "S3('https://s3.example.com/bucket/backup', extra_credentials(role_arn = concat('arn::', 'role')))");

        String str = info.withoutS3Credentials(context).toString();
        requireContains(str, "extra_credentials");
        requireContains(str, "concat('arn::', 'role')");
        std::_Exit(0);
    }

    [[noreturn]] void checkCopyS3CredentialsResolvesExtraCredentialsWithContext()
    {
        tryRegisterFunctions();
        const auto & context = getContext().context;

        /// `collectCredentials` resolves both sides of each assignment when the locator is opened, so what
        /// a clause authenticates as is decided by what it loads, not by how it is written. A resolved
        /// `role_arn` is lent whatever its spelling; a clause that resolves to no role is not.
        const auto dest = BackupInfo::fromString("S3('https://s3.example.com/base')");

        for (const auto * lendable :
             {"S3('https://s3.example.com/backup', extra_credentials(concat('role_', 'arn') = 'arn::role'))",
              "S3('https://s3.example.com/backup', extra_credentials(role_arn = concat('arn::', 'role')))"})
        {
            auto source = BackupInfo::fromString(lendable);
            if (!source.canCopyS3CredentialsTo(dest, context))
            {
                std::cerr << "Expected to be able to lend the credentials of " << lendable << '\n';
                std::_Exit(1);
            }
        }

        for (const auto * barren :
             {"S3('https://s3.example.com/backup', extra_credentials(concat('external_', 'id') = 'EXTERNALID'))",
              "S3('https://s3.example.com/backup', extra_credentials(role_arn = concat('', '')))"})
        {
            auto source = BackupInfo::fromString(barren);
            if (source.canCopyS3CredentialsTo(dest, context))
            {
                std::cerr << "Expected " << barren << " to name no role to assume\n";
                std::_Exit(1);
            }
        }

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


TEST(BackupInfoDeathTest, WithoutS3CredentialsKeepsExpressionRoleArnValue)
{
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    EXPECT_EXIT(checkExpressionRoleArnValueWithContext(), ::testing::ExitedWithCode(0), ".*");
}


TEST(BackupInfoDeathTest, CopyS3CredentialsToResolvesExtraCredentials)
{
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    EXPECT_EXIT(checkCopyS3CredentialsResolvesExtraCredentialsWithContext(), ::testing::ExitedWithCode(0), ".*");
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
    for (const auto * credential : {"KEYID", "KEYSECRET", "TOKEN", "EXTERNALID"})
        EXPECT_EQ(str.find(credential), String::npos) << str;
    /// The role identifiers are not secrets and are needed to reopen the base backup on restore.
    for (const auto * kept : {"ROLEARN", "ROLESESSION"})
        EXPECT_NE(str.find(kept), String::npos) << str;
}

TEST(BackupInfo, WithoutS3CredentialsKeepsNonSecretExtraCredentials)
{
    auto info = BackupInfo::fromString(
        "S3('https://s3.example.com/bucket/backup', extra_credentials(role_arn = 'ROLEARN', role_session_name = 'ROLESESSION'))");

    EXPECT_EQ(info.withoutS3Credentials().toString(), info.toString());
}

TEST(BackupInfo, WithoutS3CredentialsStripsExternalIdFromExtraCredentials)
{
    auto info = BackupInfo::fromString(
        "S3('https://s3.example.com/bucket/backup', extra_credentials(role_arn = 'ROLEARN', external_id = 'EXTERNALID'))");

    String str = info.withoutS3Credentials().toString();
    EXPECT_NE(str.find("ROLEARN"), String::npos) << str;
    EXPECT_EQ(str.find("EXTERNALID"), String::npos) << str;
}

TEST(BackupInfo, WithoutS3CredentialsStripsExtraCredentialsWithOnlySecrets)
{
    auto info
        = BackupInfo::fromString("S3('https://s3.example.com/bucket/backup', extra_credentials(external_id = 'EXTERNALID'))");

    EXPECT_EQ(info.withoutS3Credentials().toString(), "S3('https://s3.example.com/bucket/backup')");
}

TEST(BackupInfo, WithoutS3CredentialsRejectsNonLiteralExtraCredentialsValueWithoutContext)
{
    /// A value that is no literal has to be resolved to be classified, and a nested secret must not be
    /// persisted on the guess that it is one of the identifiers. Without a context it fails closed, the
    /// same way a key that is no literal does.
    auto info = BackupInfo::fromString(
        "S3('https://s3.example.com/bucket/backup', extra_credentials(role_arn = headers('Authorization' = 'SECRET')))");

    EXPECT_THROW(info.withoutS3Credentials(), Exception);
}

TEST(BackupInfo, WithoutS3CredentialsStripsUnrecognizedTrailingFunction)
{
    /// Only `extra_credentials` is consumed by the `S3` backup engine; anything else is dropped.
    auto info
        = BackupInfo::fromString("S3('https://s3.example.com/bucket/backup', headers('Authorization' = 'SECRET'))");

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
    checkCanCopyS3CredentialsInvariant(
        "S3('https://s3.example.com/backup', extra_credentials(role_arn = 'ROLEARN'))", "S3('https://s3.example.com/base')");
    checkCanCopyS3CredentialsInvariant(
        "S3('https://s3.example.com/backup', extra_credentials(role_arn = 'ROLEARN'))", "S3(collection)");
    checkCanCopyS3CredentialsInvariant(
        "S3('https://s3.example.com/backup', headers('Authorization' = 'SECRET'))", "S3('https://s3.example.com/base')");
    checkCanCopyS3CredentialsInvariant(
        "S3('https://s3.example.com/backup', extra_credentials(role_session_name = 'SESSION'))",
        "S3('https://s3.example.com/base')");
}

TEST(BackupInfo, CopyS3CredentialsToRejectsClauseNamingNoRole)
{
    /// Only a non-empty `role_arn` makes `getCredentialsProvider` assume a role, so a clause carrying
    /// just the session name or the external id lends no identity. Copying it would leave the base
    /// backup unauthenticated instead of failing where the credentials are asked for.
    for (const auto * source_str :
         {"S3('https://s3.example.com/backup', extra_credentials(role_session_name = 'SESSION'))",
          "S3('https://s3.example.com/backup', extra_credentials(external_id = 'EXTERNALID'))",
          "S3('https://s3.example.com/backup', extra_credentials(role_arn = ''))"})
    {
        auto source = BackupInfo::fromString(source_str);
        auto dest = BackupInfo::fromString("S3('https://s3.example.com/base')");

        EXPECT_FALSE(source.canCopyS3CredentialsTo(dest)) << source_str;
        expectExceptionCode([&] { source.copyS3CredentialsTo(dest); }, ErrorCodes::BAD_ARGUMENTS);
    }
}

TEST(BackupInfo, CopyS3CredentialsToKeepsDestinationCredentialsWhenSourceNamesNoRole)
{
    /// The rejection has to come before the destination is cleared: a source that cannot lend anything
    /// must not cost the destination the credentials it was given.
    auto source = BackupInfo::fromString("S3('https://s3.example.com/backup', extra_credentials(role_session_name = 'SESSION'))");
    auto dest = BackupInfo::fromString("S3('https://s3.example.com/base', 'OTHERKEYID', 'OTHERKEYSECRET')");
    const String dest_before = dest.toString();

    expectExceptionCode([&] { source.copyS3CredentialsTo(dest); }, ErrorCodes::BAD_ARGUMENTS);

    EXPECT_EQ(dest.toString(), dest_before);
}

TEST(BackupInfo, CopyS3CredentialsToCarriesExtraCredentials)
{
    auto source = BackupInfo::fromString(
        "S3('https://s3.example.com/backup', extra_credentials(role_arn = 'ROLEARN', external_id = 'EXTERNALID'))");
    auto dest = BackupInfo::fromString("S3('https://s3.example.com/base')");

    source.copyS3CredentialsTo(dest);

    EXPECT_EQ(
        dest.toString(),
        "S3('https://s3.example.com/base', extra_credentials(equals(role_arn, 'ROLEARN'), equals(external_id, 'EXTERNALID')))");
}

TEST(BackupInfo, CopyS3CredentialsToKeepsDestinationRoleBesideTheCopiedKeyPair)
{
    /// The setting repairs a base backup locator that cannot authenticate on its own, so a role the
    /// destination names has to survive: it is opened as the copied key pair assuming that role, the
    /// composition `getCredentialsProvider` builds, and what a key pair copied onto it always did.
    auto source = BackupInfo::fromString("S3('https://s3.example.com/backup', 'KEYID', 'KEYSECRET')");
    auto dest = BackupInfo::fromString("S3('https://s3.example.com/base', extra_credentials(role_arn = 'OTHERROLE'))");

    source.copyS3CredentialsTo(dest);

    EXPECT_EQ(
        dest.toString(),
        "S3('https://s3.example.com/base', 'KEYID', 'KEYSECRET', extra_credentials(equals(role_arn, 'OTHERROLE')))");
}

TEST(BackupInfo, CopyS3CredentialsToOverwritesTheDestinationRoleWithTheCopiedOne)
{
    /// Same kind, so the source wins: the destination cannot assume two roles.
    auto source = BackupInfo::fromString("S3('https://s3.example.com/backup', extra_credentials(role_arn = 'ROLEARN'))");
    auto dest = BackupInfo::fromString("S3('https://s3.example.com/base', extra_credentials(role_arn = 'OTHERROLE'))");

    source.copyS3CredentialsTo(dest);

    EXPECT_EQ(dest.toString(), "S3('https://s3.example.com/base', extra_credentials(equals(role_arn, 'ROLEARN')))");
}

TEST(BackupInfo, CopyS3CredentialsToDropsADestinationKeyPairItCannotReplay)
{
    /// A key pair the source has none of to lend cannot come back: `withoutS3Credentials` strips it from
    /// the metadata and re-copying restores only the role. Keeping it would open the base backup here and
    /// leave it unopenable on restore, so it goes and the locator stays reconstructable.
    auto source = BackupInfo::fromString("S3('https://s3.example.com/backup', extra_credentials(role_arn = 'ROLEARN'))");
    auto dest = BackupInfo::fromString("S3('https://s3.example.com/base', 'OTHERKEYID', 'OTHERKEYSECRET')");

    source.copyS3CredentialsTo(dest);

    EXPECT_EQ(dest.toString(), "S3('https://s3.example.com/base', extra_credentials(equals(role_arn, 'ROLEARN')))");
}

TEST(BackupInfo, CopyS3CredentialsToRoundTripsTheMergedLocatorThroughRedaction)
{
    /// What `BackupImpl::writeBackupMetadata` compares before it may replace the credentials with the
    /// `<base_backup_copy_s3_credentials_from_backup>` marker: strip the copied locator, copy onto the
    /// stripped form again, and the two must agree. A destination role survives redaction, so the merge
    /// this performs has to survive the round trip too.
    auto source = BackupInfo::fromString("S3('https://s3.example.com/backup', 'KEYID', 'KEYSECRET')");
    auto dest = BackupInfo::fromString("S3('https://s3.example.com/base', extra_credentials(role_arn = 'OTHERROLE'))");

    source.copyS3CredentialsTo(dest);

    auto stripped = dest.withoutS3Credentials();
    EXPECT_NE(stripped.toString(), dest.toString());

    auto replayed = stripped;
    source.copyS3CredentialsTo(replayed);
    EXPECT_EQ(replayed.toString(), dest.toString());
}

TEST(BackupInfo, CopyS3CredentialsToDropsADestinationClauseNamingNoRole)
{
    /// `external_id` without a `role_arn` assumes nothing, so it is not authentication to preserve.
    /// Keeping it would also leave the locator unreconstructable from the stripped form, costing the
    /// `<base_backup_copy_s3_credentials_from_backup>` marker for no gain.
    auto source = BackupInfo::fromString("S3('https://s3.example.com/backup', 'KEYID', 'KEYSECRET')");
    auto dest = BackupInfo::fromString("S3('https://s3.example.com/base', extra_credentials(external_id = 'EXTERNALID'))");

    source.copyS3CredentialsTo(dest);

    EXPECT_EQ(dest.toString(), "S3('https://s3.example.com/base', 'KEYID', 'KEYSECRET')");
}

TEST(BackupInfo, CopyS3CredentialsToLeavesAnEmptyDestinationCarryingOnlyTheCopiedCredentials)
{
    /// The shape every locator written with the `<base_backup_copy_s3_credentials_from_backup>` marker
    /// has, since stripping is what produced it. Nothing is there to keep, so copying reconstructs it
    /// exactly -- which is what `BackupImpl::writeBackupMetadata` compares before emitting the marker.
    auto source = BackupInfo::fromString("S3('https://s3.example.com/backup', 'KEYID', 'KEYSECRET')");
    auto dest = BackupInfo::fromString("S3('https://s3.example.com/base')");

    source.copyS3CredentialsTo(dest);

    EXPECT_EQ(dest.toString(), "S3('https://s3.example.com/base', 'KEYID', 'KEYSECRET')");
}

TEST(BackupInfo, CopyS3CredentialsToRejectsSourceWithoutCredentials)
{
    auto source = BackupInfo::fromString("S3('https://s3.example.com/backup')");
    auto dest = BackupInfo::fromString("S3('https://s3.example.com/base')");

    expectExceptionCode([&] { source.copyS3CredentialsTo(dest); }, ErrorCodes::BAD_ARGUMENTS);
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
