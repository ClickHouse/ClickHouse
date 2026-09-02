#include <Common/IO.h>

#include <unistd.h>
#include <cerrno>
#include <cstring>

bool writeRetry(int fd, const char * data, size_t size)
{
    if (!size)
        size = strlen(data);

    while (size != 0)
    {
        ssize_t res = ::write(fd, data, size);

        if ((-1 == res || 0 == res) && errno != EINTR)
            return false;

        if (res > 0)
        {
            data += res;
            size -= res;
        }
    }

    return true;
}
