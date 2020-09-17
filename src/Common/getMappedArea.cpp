#include "getMappedArea.h"

#if defined(__linux__)

#include <Common/StringUtils/StringUtils.h>
#include <Common/hex.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
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

    throw Exception("Cannot find mapped area for pointer", ErrorCodes::LOGICAL_ERROR);
}

}

#else

namespace DB
{

std::pair<void *, size_t> getMappedArea(void * ptr)
{
    throw Exception("The function getMappedArea is implemented only for Linux", ErrorCodes::NOT_IMPLEMENTED);
}

}

#endif

