#include <Access/SettingsProfilesInfo.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Common/tests/gtest_global_context.h>
#include <gtest/gtest.h>

namespace DB::Setting
{
extern const SettingsUInt64 max_query_size;
}

GTEST_TEST(SettingsProfileCustomCache, ProfileValuesAreAppliedWithoutPublishingQueryOwnedStorage)
{
    using namespace DB;
    auto global_context = getMutableContext().context;
    SettingsProfilesInfo profile(global_context->getAccessControl());
    profile.settings.emplace_back("max_query_size", UInt64(100000));
    profile.settings.emplace_back("custom_profile_payload", String(256, 'p'));

    Settings inherited;
    ASSERT_TRUE(inherited.hasServerOwnedStorage());
    for (size_t attempt = 0; attempt < 2; ++attempt)
    {
        auto context = Context::createCopy(global_context);
        context->setSettings(inherited);
        context->setCurrentProfiles(profile, /*check_constraints=*/ false);

        EXPECT_EQ(context->getSettingsRef()[Setting::max_query_size].value, 100000);
        EXPECT_EQ(context->getSettingsRef().get("custom_profile_payload"), Field(String(256, 'p')));
        EXPECT_FALSE(context->getSettingsRef().hasServerOwnedStorage());
        EXPECT_TRUE(inherited.hasServerOwnedStorage());
        EXPECT_FALSE(profile.tryGetCachedSettings(inherited, true));
        EXPECT_FALSE(profile.tryGetCachedSettings(inherited, false));
    }
}
