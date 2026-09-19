#include <base/getOSUserName.h>

#if defined(OS_WINDOWS)
#include <base/pathToString.h>

#include <Poco/UnWindows.h>
#include <lmcons.h> /// `UNLEN`
#else
#include <unistd.h>

#include <cstring>
#endif


std::string getOSUserName()
{
#if defined(OS_WINDOWS)
    /// `UNLEN` is the longest a user name can be; the call wants room for the terminator too.
    wchar_t buffer[UNLEN + 1];
    DWORD size = UNLEN + 1;
    if (!GetUserNameW(buffer, &size))
        return {};

    /// `size` counts the terminator on success.
    return pathToString(std::filesystem::path(std::wstring_view(buffer, size > 0 ? size - 1 : 0)));
#else
    std::string result;
    result.resize(256, '\0');
    if (0 != getlogin_r(result.data(), static_cast<int>(result.size() - 1)))
        return {};

    result.resize(strlen(result.c_str()));
    return result;
#endif
}
