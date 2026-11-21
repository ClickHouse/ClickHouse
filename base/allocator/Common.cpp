#include <allocator/Common.h>

#include <cstdlib>
#include <unistd.h>

namespace jemalloc
{

namespace
{

void writeString(const char * s)
{
    size_t length = std::strlen(s);
    while (length)
    {
        ssize_t written = ::write(STDERR_FILENO, s, length);
        if (written <= 0)
            return;
        s += written;
        length -= size_t(written);
    }
}

void writeNumber(unsigned long value)
{
    char buf[32];
    char * end = buf + sizeof(buf);
    char * p = end;
    *--p = '\0';
    do
    {
        *--p = static_cast<char>('0' + value % 10);
        value /= 10;
    } while (value);
    writeString(p);
}

}

void assertionFailed(const char * file, int line, const char * function, const char * expression)
{
    /// Does not use the formatting machinery: it must work even when that is broken.
    writeString("<jemalloc>: ");
    writeString(file);
    writeString(":");
    writeNumber(static_cast<unsigned long>(line));
    writeString(": ");
    writeString(function);
    writeString(": Failed assertion: \"");
    writeString(expression);
    writeString("\"\n");
    std::abort();
}

}
