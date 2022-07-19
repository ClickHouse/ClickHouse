#include "errnoToString.h"

#include <fmt/format.h>


std::string errnoToString(int code, int the_errno)
{
    const size_t buf_size = 128;
    char buf[buf_size];
#ifndef _GNU_SOURCE
    int rc = strerror_r(the_errno, buf, buf_size);
#ifdef OS_DARWIN
    if (rc != 0 && rc != EINVAL)
#else
    if (rc != 0)
#endif
    {
        std::string tmp = std::to_string(code);
        const char * code_str = tmp.c_str();
        const char * unknown_message = "Unknown error ";
        strcpy(buf, unknown_message);
        strcpy(buf + strlen(unknown_message), code_str);
    }
    return fmt::format("errno: {}, strerror: {}", the_errno, buf);
#else
    (void)code;
    return fmt::format("errno: {}, strerror: {}", the_errno, strerror_r(the_errno, buf, sizeof(buf)));
#endif
}
