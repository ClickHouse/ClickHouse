#pragma once
#include <optional>
#include <string>
namespace uatraits::types
{
enum class Browser
{
    CHROME_MOBILE       /* "ChromeMobile" */,
    FIREFOX             /* "Firefox" */,
    MOBILE_FIREFOX      /* "MobileFirefox" */,
    MOBILE_SAFARI       /* "MobileSafari" */,
    SAFARI              /* "Safari" */,
    SAMSUNG_INTERNET    /* "Samsung Internet" */,
    YANDEX_BROWSER      /* "YandexBrowser" */,
    YANDEX_BROWSER_LITE /* "YandexBrowserLite" */,
    YANDEX_SEARCH       /* "YandexSearch" */,
    CHROME              /* "Chrome" */,
    EDGE                /* "Edge" */,
};
std::optional<Browser> browserFromString(const std::string & browser);
const std::string & toString(const Browser browser);
const std::string & toString(const std::optional<Browser> & browser);
} // namespace uatraits::types
