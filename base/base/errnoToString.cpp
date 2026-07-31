#include "errnoToString.h"

#include <fmt/format.h>

#include <cstring>


std::string errnoToString(int the_errno)
{
    const size_t buf_size = 128;
    char buf[buf_size];

#if defined(_GNU_SOURCE) && !defined(OS_WINDOWS)
    /// The GNU flavour of `strerror_r` returns the message - which need not be `buf`, it may be a
    /// static string - and never fails.
    return fmt::format("errno: {}, strerror: {}", the_errno, strerror_r(the_errno, buf, sizeof(buf)));
#else
    /// The flavours that fill in `buf` and report a status: XSI `strerror_r`, and `strerror_s` on
    /// Windows, which is the same function with its arguments in the other order.
#if defined(OS_WINDOWS)
    int rc = strerror_s(buf, buf_size, the_errno);
#else
    int rc = strerror_r(the_errno, buf, buf_size);
#endif

#if defined(OS_DARWIN)
    if (rc != 0 && rc != EINVAL)
#else
    if (rc != 0)
#endif
    {
        std::string tmp = std::to_string(the_errno);
        const char * code_str = tmp.c_str();
        const char * unknown_message = "Unknown error ";
        strcpy(buf, unknown_message);
        strcpy(buf + strlen(unknown_message), code_str);
    }
    return fmt::format("errno: {}, strerror: {}", the_errno, buf);
#endif
}
