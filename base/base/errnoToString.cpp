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

#if defined(OS_WINDOWS)
    /// The Windows CRT spells the bounded form `strerror_s`, with the arguments in the other order.
    /// Like the XSI `strerror_r` it reports a status rather than the string, so it resolves to the
    /// same overload above.
    const auto result = strerror_s(buf, buf_size, the_errno);
#else
    const auto result = strerror_r(the_errno, buf, buf_size); /// NOLINT(readability-qualified-auto): a `char *` with glibc, an `int` with the XSI form
#endif

    return fmt::format("errno: {}, strerror: {}", the_errno, getErrorMessage(result, buf, buf_size, the_errno));
}
