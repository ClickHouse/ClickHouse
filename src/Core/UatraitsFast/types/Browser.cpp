#include "Browser.h"

namespace uatraits::types
{
namespace
{
static const std::string chrome_mobile = "ChromeMobile";
static const std::string firefox = "Firefox";
static const std::string mobile_firefox = "MobileFirefox";
static const std::string mobile_safari = "MobileSafari";
static const std::string safari = "Safari";
static const std::string samsung_internet = "Samsung Internet";
static const std::string yandex_browser = "YandexBrowser";
static const std::string yandex_browser_lite = "YandexBrowserLite";
static const std::string yandex_search = "YandexSearch";
static const std::string chrome = "Chrome";
static const std::string edge = "Edge";
} // anonymous namespace
std::optional<Browser> browserFromString(const std::string & browser)
{
    if (browser == chrome_mobile)
        return Browser::CHROME_MOBILE;
    else if (browser == firefox)
        return Browser::FIREFOX;
    else if (browser == mobile_firefox)
        return Browser::MOBILE_FIREFOX;
    else if (browser == mobile_safari)
        return Browser::MOBILE_SAFARI;
    else if (browser == safari)
        return Browser::SAFARI;
    else if (browser == samsung_internet)
        return Browser::SAMSUNG_INTERNET;
    else if (browser == yandex_browser)
        return Browser::YANDEX_BROWSER;
    else if (browser == yandex_browser_lite)
        return Browser::YANDEX_BROWSER_LITE;
    else if (browser == yandex_search)
        return Browser::YANDEX_SEARCH;
    else if (browser == chrome)
        return Browser::CHROME;
    else if (browser == edge)
        return Browser::EDGE;
    return {};
}
const std::string & toString(const Browser browser)
{
    switch (browser)
    {
        case Browser::CHROME_MOBILE: return chrome_mobile;
        case Browser::FIREFOX: return firefox;
        case Browser::MOBILE_FIREFOX: return mobile_firefox;
        case Browser::MOBILE_SAFARI: return mobile_safari;
        case Browser::SAFARI: return safari;
        case Browser::SAMSUNG_INTERNET: return samsung_internet;
        case Browser::YANDEX_BROWSER: return yandex_browser;
        case Browser::YANDEX_BROWSER_LITE: return yandex_browser_lite;
        case Browser::YANDEX_SEARCH: return yandex_search;
        case Browser::CHROME: return chrome;
        case Browser::EDGE: return edge;
    }
    throw std::logic_error("Unexpected browser");
}
const std::string & toString(const std::optional<Browser> & browser)
{
    if (browser.has_value())
        return toString(browser.value());
    static const std::string empty_string;
    return empty_string;
}
} // namespace uatraits::types
