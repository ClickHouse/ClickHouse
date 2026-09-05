#include <Common/IO.h>

#include <unistd.h>
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <limits>

bool writeRetry(int fd, const char * data, size_t size)
{
    if (!size)
        size = strlen(data);

    while (size != 0)
    {
#if defined(OS_WINDOWS)
        /// The Windows CRT's `_write` takes the count as `unsigned int`, not `size_t`. Clamping
        /// is lossless here because this is a write loop: whatever is left goes out on the next
        /// iteration, exactly as it would after a short write on any platform.
        const auto chunk = static_cast<unsigned int>(std::min<size_t>(size, std::numeric_limits<int>::max()));
#else
        const auto chunk = size;
#endif
        ssize_t res = ::write(fd, data, chunk);

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
