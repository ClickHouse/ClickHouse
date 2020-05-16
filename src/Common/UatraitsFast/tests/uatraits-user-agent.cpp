#include <Poco/Util/MapConfiguration.h>
#include <iostream>
#include <library/unittest/env.h>
#include <library/unittest/registar.h>
#include <library/unittest/tests_data.h>
#include <metrika/core/libs/uatraits-fast/UserAgent.h>
#include <util/system/compiler.h>

static std::string MakePath(const std::string & path)
{
    return "data/" + path;
}

struct UserAgentFixture : public NUnitTest::TBaseFixture
{
    UserAgentFixture()
    {
        Poco::AutoPtr<Poco::Util::MapConfiguration> config = new Poco::Util::MapConfiguration;
        config->setInt("reload_frequency_sec", 600);
        config->setString("browsers_path", MakePath("browser.xml"));
        config->setString("profiles_path", MakePath("profiles.xml"));
        config->setString("extra_path", MakePath("extra.xml"));

        components::UserAgent::instance().create(*config);
        components::UserAgent::instance().reload();
    }
};

void CheckUA(const std::string & userAgent,
    appmetrica::types::OperatingSystem os, unsigned int osVersion,
    appmetrica::types::Browser browser, unsigned int browserVersion)
{
    auto agent = components::UserAgent::instance().detect(userAgent);
    if (agent.getOperatingSystem().getName() != os)
        throw std::runtime_error("Operating Systems are not equal");
    UNIT_ASSERT_EQUAL(agent.getOperatingSystem().getVersion().v1, osVersion);
    Y_ASSERT(agent.getBrowser().getName().has_value());
    if (*agent.getBrowser().getName() != browser)
        throw std::runtime_error("Browsers are not equal");
    UNIT_ASSERT_EQUAL(agent.getBrowser().getVersion().v1, browserVersion);
}

Y_UNIT_TEST_SUITE_F(UATraitsUserAgent, UserAgentFixture)
{

Y_UNIT_TEST(AppleThirdPartyCookies)
{
    CheckUA("Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A5370a Safari/604.1",
        appmetrica::types::OperatingSystem::IOS, 11, appmetrica::types::Browser::MOBILE_SAFARI, 11);
    CheckUA("Mozilla/5.0 (iPhone; CPU iPhone OS 10_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/10.0 Mobile/15A5370a Safari/604.1",
        appmetrica::types::OperatingSystem::IOS, 10, appmetrica::types::Browser::MOBILE_SAFARI, 10);
    CheckUA("Mozilla/5.0 (iPhone; CPU iPhone OS 10_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A5370a Safari/604.1",
        appmetrica::types::OperatingSystem::IOS, 10, appmetrica::types::Browser::MOBILE_SAFARI, 11);
    CheckUA("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Safari/604.1.38",
        appmetrica::types::OperatingSystem::MACOS, 10, appmetrica::types::Browser::SAFARI, 11);
    CheckUA("'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_0) AppleWebKit/603.3.8 (KHTML, like Gecko) Version/10.1.2 Safari/603.3.8",
        appmetrica::types::OperatingSystem::MACOS, 10, appmetrica::types::Browser::SAFARI, 10);
    CheckUA("Mozilla/5.0 (iPhone; CPU iPhone OS 11_3 like Mac OS X) AppleWebKit/604.1.34 (KHTML, like Gecko) Version/11.0 YaBrowser/17.11.1.743.10 Mobile/15E216 Safari/604.1",
        appmetrica::types::OperatingSystem::IOS, 11, appmetrica::types::Browser::YANDEX_BROWSER, 17);
}

Y_UNIT_TEST(SameSiteNoneDetection)
{
    auto ua_chromium = components::UserAgent::instance().detect("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/76.0.3770.90 Chrome/76.0.3770.90 Safari/537.36");
    UNIT_ASSERT_UNEQUAL(ua_chromium.getBrowser().getName(), appmetrica::types::Browser::CHROME);
    UNIT_ASSERT_EQUAL(ua_chromium.getBrowser().getBase(), appmetrica::types::BrowserBase::CHROMIUM);
    UNIT_ASSERT_EQUAL(ua_chromium.getBrowser().getVersion().v1, 76);
    UNIT_ASSERT_EQUAL(ua_chromium.getBrowser().getBaseVersion().v1, 76);
    UNIT_ASSERT(ua_chromium.hasSameSiteSupport());

    auto ua_chrome = components::UserAgent::instance().detect("Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36");
    UNIT_ASSERT_EQUAL(ua_chrome.getBrowser().getName(), appmetrica::types::Browser::CHROME);
    UNIT_ASSERT_EQUAL(ua_chrome.getBrowser().getBase(), appmetrica::types::BrowserBase::CHROMIUM);
    UNIT_ASSERT_EQUAL(ua_chrome.getBrowser().getVersion().v1, 75);
    UNIT_ASSERT_EQUAL(ua_chrome.getBrowser().getBaseVersion().v1, 75);
    UNIT_ASSERT(!ua_chrome.hasSameSiteSupport());

    auto ua_yabrowser = components::UserAgent::instance().detect("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3729.169 YaBrowser/19.9.2.594 (beta) Yowser/2.5 Safari/537.36");
    UNIT_ASSERT_EQUAL(ua_yabrowser.getBrowser().getBase(), appmetrica::types::BrowserBase::CHROMIUM);
    UNIT_ASSERT_EQUAL(ua_yabrowser.getBrowser().getVersion().v1, 19);
    UNIT_ASSERT_EQUAL(ua_yabrowser.getBrowser().getBaseVersion().v1, 76);
    UNIT_ASSERT(!ua_yabrowser.hasSameSiteSupport()); // в YaBro отключен SameSite

    auto ua_yabrowser2 = components::UserAgent::instance().detect("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 YaBrowser/19.12.3.332 (beta) Yowser/2.5 Safari/537.36 Yptp/1.55");
    UNIT_ASSERT_EQUAL(ua_yabrowser2.getBrowser().getBase(), appmetrica::types::BrowserBase::CHROMIUM);
    UNIT_ASSERT_EQUAL(ua_yabrowser2.getBrowser().getVersion().v1, 19);
    UNIT_ASSERT_EQUAL(ua_yabrowser2.getBrowser().getBaseVersion().v1, 78);
    UNIT_ASSERT(!ua_yabrowser2.hasSameSiteSupport()); // в YaBro отключен SameSite

    auto ua_firefox = components::UserAgent::instance().detect("Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:68.0) Gecko/20100101 Firefox/68.0");
    Y_ASSERT(!ua_firefox.getBrowser().getBase().has_value());
    UNIT_ASSERT(!ua_firefox.hasSameSiteSupport());

    auto ua_safari = components::UserAgent::instance().detect("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/604.0.12 (KHTML, like Gecko) Version/10.1 Safari/600.1.13");
    Y_ASSERT(!ua_safari.getBrowser().getBase().has_value());
    UNIT_ASSERT(!ua_safari.hasSameSiteSupport());

    auto ua_chrome_mob = components::UserAgent::instance().detect("Mozilla/5.0 (Linux; Android 6.0; M5 Note) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3770.143 Mobile Safari/537.36");
    UNIT_ASSERT_EQUAL(ua_chrome_mob.getBrowser().getName(), appmetrica::types::Browser::CHROME_MOBILE);
    UNIT_ASSERT_EQUAL(ua_chrome_mob.getBrowser().getBase(), appmetrica::types::BrowserBase::CHROMIUM);
    UNIT_ASSERT_EQUAL(ua_chrome_mob.getBrowser().getVersion().v1, 77);
    UNIT_ASSERT_EQUAL(ua_chrome_mob.getBrowser().getBaseVersion().v1, 77);
    UNIT_ASSERT(ua_chrome_mob.hasSameSiteSupport());

    auto ua_yabrowser_mob = components::UserAgent::instance().detect("Mozilla/5.0 (Linux; Android 6.0; M5 Note) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 YaBrowser/19.6.4.349.00 Mobile Safari/537.36");
    UNIT_ASSERT_EQUAL(ua_yabrowser_mob.getBrowser().getBase(), appmetrica::types::BrowserBase::CHROMIUM);
    UNIT_ASSERT_EQUAL(ua_yabrowser_mob.getBrowser().getBaseVersion().v1, 74);
    UNIT_ASSERT(!ua_yabrowser_mob.hasSameSiteSupport());

    auto ua_firefox_mob = components::UserAgent::instance().detect("Mozilla/5.0 (Android 6.0; Mobile; rv:68.0) Gecko/68.0 Firefox/68.0");
    Y_ASSERT(!ua_firefox_mob.getBrowser().getBase().has_value());
    UNIT_ASSERT(!ua_firefox_mob.hasSameSiteSupport());

    auto ua_yabrowser_ios = components::UserAgent::instance().detect("Mozilla/5.0 (iPhone; CPU iPhone OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0 YaBrowser/19.9.2.561.10 Mobile/15E148 Safari/605.1");
    UNIT_ASSERT_EQUAL(ua_yabrowser_ios.getBrowser().getBase(), appmetrica::types::BrowserBase::SAFARI);
    UNIT_ASSERT(!ua_yabrowser_ios.hasSameSiteSupport());

    auto ua_chrome_ios = components::UserAgent::instance().detect("Mozilla/5.0 (iPhone; CPU iPhone OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/76.0.3729.121 Mobile/15E148 Safari/605.1");
    UNIT_ASSERT_EQUAL(ua_chrome_ios.getBrowser().getName(), appmetrica::types::Browser::CHROME_MOBILE);
    UNIT_ASSERT_EQUAL(ua_chrome_ios.getBrowser().getBase(), appmetrica::types::BrowserBase::SAFARI);
    UNIT_ASSERT(!ua_chrome_ios.hasSameSiteSupport());

    auto ua_yabrowser_ios2 = components::UserAgent::instance().detect("Mozilla/5.0 (iPhone; CPU iPhone OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0 YaBrowser/19.8.2.561.10 Mobile/15E148 Safari/605.1");
    UNIT_ASSERT_EQUAL(ua_yabrowser_ios2.getBrowser().getBase(), appmetrica::types::BrowserBase::SAFARI);
    UNIT_ASSERT(!ua_yabrowser_ios2.hasSameSiteSupport());

    auto ua_chrome_ios2 = components::UserAgent::instance().detect("Mozilla/5.0 (iPhone; CPU iPhone OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/75.0.3729.121 Mobile/15E148 Safari/605.1");
    UNIT_ASSERT_EQUAL(ua_chrome_ios2.getBrowser().getName(), appmetrica::types::Browser::CHROME_MOBILE);
    UNIT_ASSERT_EQUAL(ua_chrome_ios2.getBrowser().getBase(), appmetrica::types::BrowserBase::SAFARI);
    UNIT_ASSERT(!ua_chrome_ios2.hasSameSiteSupport());

    auto ua_opera_ios = components::UserAgent::instance().detect("Mozilla/5.0 (iPhone; CPU iPhone OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148");
    Y_ASSERT(!ua_opera_ios.getBrowser().getBase().has_value());
    UNIT_ASSERT(!ua_opera_ios.hasSameSiteSupport());

    auto ua_safari_ios = components::UserAgent::instance().detect("Mozilla/5.0 (iPhone; CPU iPhone OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.1 Mobile/15E148 Safari/604.1");
    UNIT_ASSERT_EQUAL(ua_safari_ios.getBrowser().getBase(), appmetrica::types::BrowserBase::SAFARI);
    UNIT_ASSERT(!ua_safari_ios.hasSameSiteSupport());
}

Y_UNIT_TEST(ItpDetection)
{
    const auto useragent_itp = components::UserAgent::instance().detect("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.1 Safari/605.1.15");
    UNIT_ASSERT(useragent_itp.isItpEnabled());

    const auto useragent_no_itp = components::UserAgent::instance().detect("Opera/9.80 (MAUI Runtime; Opera Mini/4.4.32206/73.132; U; ru) Presto/2.12.423 Version/12.16");
    UNIT_ASSERT(!useragent_no_itp.isItpEnabled());

    const auto edge_no_itp = components::UserAgent::instance().detect("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36 Edg/78.0.100.0");
    UNIT_ASSERT(!edge_no_itp.isItpEnabled());

    const auto win_edge_itp = components::UserAgent::instance().detect("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36 Edg/79.0.100.0");
    UNIT_ASSERT(win_edge_itp.isItpEnabled());

    const auto mac_edge_itp = components::UserAgent::instance().detect("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36 Edg/79.0.100.0");
    UNIT_ASSERT(mac_edge_itp.isItpEnabled());

    const auto test_edge_itp = components::UserAgent::instance().detect("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36 Edge/79.0.100.0");
    UNIT_ASSERT(test_edge_itp.isItpEnabled());

    /// EdgA нет отдельно в https://github.yandex-team.ru/InfraComponents/uatraits-data/blob/master/data/browser.xml, он считается хромом.
    const auto android_edge_itp = components::UserAgent::instance().detect("Mozilla/5.0 (Linux; Android 8.1.0; Pixel Build/OPM4.171019.021.D1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.109 Mobile Safari/537.36 EdgA/42.0.0.2057");
    UNIT_ASSERT_EQUAL(android_edge_itp.getBrowser().getName(), appmetrica::types::Browser::CHROME_MOBILE);
    UNIT_ASSERT(!android_edge_itp.isItpEnabled());

    const auto ios_edge_itp = components::UserAgent::instance().detect("Mozilla/5.0 (iPhone; CPU iPhone OS 12_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.1.1 EdgiOS/44.5.0.10 Mobile/15E148 Safari/604.1");
    UNIT_ASSERT(ios_edge_itp.isItpEnabled());
}

}
