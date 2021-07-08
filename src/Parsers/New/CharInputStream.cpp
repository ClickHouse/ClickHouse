#include <Parsers/New/CharInputStream.h>

#include <Exceptions.h>


namespace DB
{

using namespace antlr4;

CharInputStream::CharInputStream(const char * begin, const char * end)
{
    d = begin;
    s = end - begin;
}

size_t CharInputStream::LA(ssize_t i)
{
    if (i == 0) return 0;  // undefined

    ssize_t position = static_cast<ssize_t>(p);
    if (i < 0)
    {
        i++; // e.g., translate LA(-1) to use offset i=0; then data[p+0-1]
        if ((position + i - 1) < 0)
            return IntStream::EOF; // invalid; no char before first char
    }

    if ((position + i - 1) >= static_cast<ssize_t>(s))
        return IntStream::EOF;

    return d[static_cast<size_t>((position + i - 1))];
}

void CharInputStream::consume()
{
    if (p >= s)
    {
        assert(LA(1) == IntStream::EOF);
        throw IllegalStateException("cannot consume EOF");
    }

    if (p < s) ++p;
}

void CharInputStream::seek(size_t i)
{
    if (i <= p)
    {
        p = i; // just jump; don't update stream state (line, ...)
        return;
    }

    // seek forward, consume until p hits index or s (whichever comes first)
    i = std::min(i, s);
    while (p < i)
        consume();
}

std::string CharInputStream::getText(const antlr4::misc::Interval &interval)
{
    if (interval.a < 0 || interval.b < 0)
        return {};

    size_t start = static_cast<size_t>(interval.a);
    size_t stop = static_cast<size_t>(interval.b);


    if (stop >= s)
        stop = s - 1;

    size_t count = stop - start + 1;
    if (start >= s)
        return "";

    return {d + start, count};
}

}
