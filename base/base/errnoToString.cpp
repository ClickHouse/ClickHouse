#include "errnoToString.h"

#include <fmt/format.h>

#include <cerrno>
#include <cstdio>
#include <cstring>

namespace
{

[[maybe_unused]] const char * getErrorMessage(int rc, char * buf, size_t buf_size, int the_errno)
{
#ifdef OS_DARWIN
    if (rc != 0 && rc != EINVAL)
#else
    if (rc != 0)
#endif
        (void)snprintf(buf, buf_size, "Unknown error %d", the_errno);
    return buf;
}

[[maybe_unused]] const char * getErrorMessage(const char * message, char *, size_t, int)
{
    return message;
}

}

std::string errnoToString(int the_errno)
{
    const size_t buf_size = 128;
    char buf[buf_size];

    return fmt::format(
        "errno: {}, strerror: {}", the_errno, getErrorMessage(strerror_r(the_errno, buf, buf_size), buf, buf_size, the_errno));
}
