#include <Common/getUserHomePath.h>

#include <cstdlib>
#include <cstring>

#if defined(OS_WINDOWS)
#include <filesystem>

#include <base/pathToString.h>
#endif

namespace DB
{

#if defined(OS_WINDOWS)

namespace
{

std::string getPathEnvUTF8(const wchar_t * name)
{
    const wchar_t * value = _wgetenv(name); // NOLINT(concurrency-mt-unsafe)
    if (!value || !*value)
        return {};
    return pathToString(std::filesystem::path(value));
}

}

std::string getPathFromEnvironment(const char * name)
{
    /// Environment variable names are ASCII, so widening them character by character is exact.
    std::wstring wide_name(name, name + strlen(name));
    return getPathEnvUTF8(wide_name.c_str());
}

std::string getUserHomePath()
{
    if (auto home = getPathEnvUTF8(L"HOME"); !home.empty())
        return home;

    if (auto profile = getPathEnvUTF8(L"USERPROFILE"); !profile.empty())
        return profile;

    const auto drive = getPathEnvUTF8(L"HOMEDRIVE");
    const auto dir = getPathEnvUTF8(L"HOMEPATH");
    if (!drive.empty() && !dir.empty())
        return drive + dir;

    return {};
}

#else

std::string getUserHomePath()
{
    const char * home = getenv("HOME"); // NOLINT(concurrency-mt-unsafe)
    return home ? home : "";
}

std::string getPathFromEnvironment(const char * name)
{
    const char * value = getenv(name); // NOLINT(concurrency-mt-unsafe)
    return value ? value : "";
}

#endif

}
