#include <Access/SettingsProfilesInfo.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>
#include <gtest/gtest.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace
{
using namespace DB;

constexpr auto password = "settings_context_error_password";

SettingChange malformedURIChange()
{
    return {"format_avro_schema_registry_url", String("http://user:") + password + "@["};
}

template <typename Apply>
void checkMaskedURIError(Apply && apply)
{
    try
    {
        apply();
        FAIL() << "Expected the malformed setting to be rejected";
    }
    catch (const Exception & exception)
    {
        const auto message = exception.message();
        EXPECT_EQ(exception.code(), ErrorCodes::BAD_ARGUMENTS);
        EXPECT_EQ(message.find(password), String::npos);
        EXPECT_NE(
            message.find("in attempt to set the value of setting 'format_avro_schema_registry_url' to 'http://[HIDDEN]@['"),
            String::npos);
    }
}
}

GTEST_TEST(SettingsContextErrors, CachedLoginProfileMasksCredential)
{
    auto global_context = getMutableContext().context;
    auto context = Context::createCopy(global_context);
    Settings inherited;
    ASSERT_TRUE(inherited.hasServerOwnedStorage());
    context->setSettings(inherited);

    /// SQL and configuration parsers normally validate this before login. Construct the resolved
    /// profile directly to exercise an exception from the cached login-resolution path itself.
    SettingsProfilesInfo profile(global_context->getAccessControl());
    profile.settings.push_back(malformedURIChange());
    checkMaskedURIError([&]
    {
        context->setCurrentProfiles(profile, /*check_constraints=*/ false);
    });
    EXPECT_TRUE(context->getSettingsRef().sharesSnapshotWith(inherited));
    EXPECT_FALSE(profile.tryGetCachedSettings(inherited, true));
    EXPECT_FALSE(profile.tryGetCachedSettings(inherited, false));
}

GTEST_TEST(SettingsContextErrors, SingleAndBatchChangesMaskCredential)
{
    auto context = Context::createCopy(getMutableContext().context);
    checkMaskedURIError([&]
    {
        context->applySettingChange(malformedURIChange());
    });
    checkMaskedURIError([&]
    {
        context->applySettingsChanges({malformedURIChange()});
    });
}

GTEST_TEST(SettingsContextErrors, NonSecretValueRemainsInError)
{
    auto context = Context::createCopy(getMutableContext().context);
    try
    {
        context->applySettingChange({"max_threads", String("not a number")});
        FAIL() << "Expected the malformed setting to be rejected";
    }
    catch (const Exception & exception)
    {
        EXPECT_NE(
            exception.message().find("in attempt to set the value of setting 'max_threads' to 'not a number'"),
            String::npos);
    }
}

GTEST_TEST(SettingsContextErrors, ShorthandAliasStillMasksURIPassword)
{
    auto context = Context::createCopy(getMutableContext().context);
    SettingChange change{"enable_analyzer", malformedURIChange().value};
    change.shorthand = true;
    try
    {
        context->applySettingChange(change);
        FAIL() << "Expected the shorthand with an explicit value to be rejected";
    }
    catch (const Exception & exception)
    {
        EXPECT_EQ(exception.message().find(password), String::npos);
        EXPECT_NE(
            exception.message().find("in attempt to set the value of setting 'enable_analyzer' to 'http://user:[HIDDEN]@['"),
            String::npos);
    }
}

GTEST_TEST(SettingsContextErrors, ShorthandSecretSettingMasksPresignedCredential)
{
    auto context = Context::createCopy(getMutableContext().context);
    SettingChange change{"format_avro_schema_registry_url", String("https://registry.invalid/?X-Amz-Signature=") + password};
    change.shorthand = true;
    try
    {
        context->applySettingChange(change);
        FAIL() << "Expected the shorthand for a non-Bool setting to be rejected";
    }
    catch (const Exception & exception)
    {
        EXPECT_EQ(exception.message().find(password), String::npos);
        EXPECT_NE(exception.message().find("to 'https://registry.invalid/?X-Amz-Signature=[HIDDEN]'"), String::npos);
    }
}
