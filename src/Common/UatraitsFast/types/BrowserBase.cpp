#include "BrowserBase.h"
namespace uatraits::types
{
namespace
{
static const std::string chromium = "Chromium";
static const std::string safari = "Safari";
} // anonymous namespace
std::optional<BrowserBase> browserBaseFromString(const std::string & browser_base)
{
    if (browser_base == chromium)
        return BrowserBase::CHROMIUM;
    else if (browser_base == safari)
        return BrowserBase::SAFARI;
    return {};
}
const std::string & toString(const BrowserBase browser_base)
{
    switch (browser_base)
    {
        case BrowserBase::CHROMIUM: return chromium;
        case BrowserBase::SAFARI: return safari;
    }
    throw std::logic_error("Unexpected browser base");
}
const std::string & toString(const std::optional<BrowserBase> & browser_base)
{
    if (browser_base.has_value())
        return toString(browser_base.value());
    static const std::string empty_string;
    return empty_string;
}
} // namespace uatraits::types
