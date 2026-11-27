#include <Common/getMappedArea.h>
#include <Common/Exception.h>

#if defined(OS_LINUX)

#include <Common/StringUtils.h>
#include <base/hex.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace
{

uintptr_t readAddressHex(DB::ReadBuffer & in)
{
    uintptr_t res = 0;
    while (!in.eof())
    {
        if (isHexDigit(*in.position()))
        {
            res *= 16;
            res += unhex(*in.position());
            ++in.position();
        }
        else
            break;
    }
    return res;
}

}

std::pair<void *, size_t> getMappedArea(void * ptr)
{
    using namespace DB;

    uintptr_t uintptr = reinterpret_cast<uintptr_t>(ptr);
    ReadBufferFromFile in("/proc/self/maps");

    while (!in.eof())
    {
        uintptr_t begin = readAddressHex(in);
        assertChar('-', in);
        uintptr_t end = readAddressHex(in);
        skipToNextLineOrEOF(in);

        if (begin <= uintptr && uintptr < end)
            return {reinterpret_cast<void *>(begin), end - begin};
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find mapped area for pointer");
}

}

#else

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

std::pair<void *, size_t> getMappedArea(void *)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The function getMappedArea is implemented only for Linux");
}

}

#endif

