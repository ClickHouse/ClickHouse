#pragma once
#include <optional>
#include <string>
namespace uatraits::types
{
enum class BrowserBase
{
    CHROMIUM,
    SAFARI
};
std::optional<BrowserBase> browserBaseFromString(const std::string & browser_base);
const std::string & toString(const BrowserBase browser_base);
const std::string & toString(const std::optional<BrowserBase> & browser_base);
} // namespace uatraits::types