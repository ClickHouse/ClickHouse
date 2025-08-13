#include <Common/isValidASCII.h>

namespace DB::ASCII
{

UInt8 isValidASCII(const UInt8 * data, UInt64 len)
{
    for (UInt64 i = 0; i < len; ++i)
    {
        if (data[i] > 0x7F)
            return 0;
    }
    return 1;
}

}
