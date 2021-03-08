#include <string.h>
#include <iostream>
#include <memcpy.h>

__attribute__((__noinline__)) void memcpy_noinline(void * __restrict dst, const void * __restrict src, size_t size)
{
    memcpy(dst, src, size);
}


int main(int, char **)
{
    constexpr size_t buf_size = 100;
    char buf[buf_size]{};
    memcpy_noinline(buf, "abc", 3);

    size_t bytes_to_copy = 3;
    while (bytes_to_copy * 2 < buf_size)
    {
        memcpy_noinline(&buf[bytes_to_copy], buf, bytes_to_copy);
        bytes_to_copy *= 2;
    }

    std::cerr << buf << "\n";
    return 0;
}
