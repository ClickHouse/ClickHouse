#pragma once
#include <cstdint>
#include <string>
namespace uatraits::types
{
enum class OperatingSystem : std::int8_t
{
    UNKNOWN = 0 /* "unknown" */,
    ANDROID = 1 /* "android" */,
    IOS = 2     /* "ios" */,
    WINDOWS = 3 /* "windows" */,
    MACOS = 4   /* "macos" */,
    LINUX = 5   /* "linux" */,
};
const std::string & toString(const OperatingSystem os);
std::string toAppPlatform(const OperatingSystem os, bool is_desktop);
OperatingSystem operatingSystemFromString(const std::string & os);
std::string toLogsApiString(const OperatingSystem os);
} // namespace uatraits::types
namespace std
{
template <>
struct hash<uatraits::types::OperatingSystem>
{
    std::size_t operator()(const uatraits::types::OperatingSystem & os) const noexcept
    {
        return static_cast<std::size_t>(os);
    }
};
} // namespace std

