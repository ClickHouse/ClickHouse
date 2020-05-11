#include "OperatingSystem.h"
#include <stdexcept>
namespace uatriats::types
{
namespace
{
const std::string os_unknown{"unknown"};
const std::string os_android{"android"};
const std::string os_ios{"ios"};
const std::string os_windows{"windows"};
const std::string os_macos{"macos"};
const std::string os_linux{"linux"};
} // anonymous namespace
const std::string & toString(const OperatingSystem os)
{
    switch (os)
    {
        case OperatingSystem::UNKNOWN:
            return os_unknown;
        case OperatingSystem::ANDROID:
            return os_android;
        case OperatingSystem::IOS:
            return os_ios;
        case OperatingSystem::WINDOWS:
            return os_windows;
        case OperatingSystem::MACOS:
            return os_macos;
        case OperatingSystem::LINUX:
            return os_linux;
    }
    throw std::runtime_error("Unexpected operating system");
}
std::string toAppPlatform(const OperatingSystem os, bool is_desktop)
{
    switch (os)
    {
        case OperatingSystem::MACOS: // desktop
        case OperatingSystem::LINUX: // desktop
        case OperatingSystem::UNKNOWN:
            return "";
        case OperatingSystem::ANDROID:
            return "android";
        case OperatingSystem::IOS:
            return "iOS";
        case OperatingSystem::WINDOWS:
            return is_desktop ? "" : "WindowsPhone";
    }
    throw std::runtime_error("Unexpected operating system");
}
std::string toLogsApiString(const OperatingSystem os)
{
    if (os == OperatingSystem::UNKNOWN)
        return "";
    return toString(os);
}
OperatingSystem operatingSystemFromString(const std::string & os)
{
    if (os == os_android)
        return OperatingSystem::ANDROID;
    if (os == os_ios)
        return OperatingSystem::IOS;
    if (os == os_windows)
        return OperatingSystem::WINDOWS;
    if (os == os_macos)
        return OperatingSystem::MACOS;
    if (os == os_linux)
        return OperatingSystem::LINUX;
    return OperatingSystem::UNKNOWN;
}
} // namespace uatraits::types
