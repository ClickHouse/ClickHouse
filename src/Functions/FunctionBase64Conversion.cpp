#include "config.h"
#if USE_BASE64
#    include <span>
#    include <vector>
#    include <Core/TypeId.h>

namespace DB
{

std::vector<UInt8> preprocessBase64Url(const std::span<const UInt8> src)
{
    std::vector<UInt8> padded_src{};
    // insert padding to please aklomp library
    size_t padded_size = src.size();
    size_t remainder = padded_size % 4;
    switch (remainder)
    {
        case 0:
            break; // no padding needed
        case 1:
            padded_size += 3; // this case is impossible to occur, however, we'll insert padding anyway
            break;
        case 2:
            padded_size += 2; // two bytes padding
            break;
        default: // remainder == 3
            padded_size += 1; // one byte padding
            break;
    }
    padded_src.resize(padded_size);

    // Do symbol substitution as described in https://datatracker.ietf.org/doc/html/rfc4648#page-7
    size_t i = 0;
    for (; i < src.size(); ++i)
    {
        switch (src[i])
        {
        case '_':
            padded_src[i] = '/';
            break;
        case '-':
            padded_src[i] = '+';
            break;
        default:
            padded_src[i] = src[i];
            break;
        }
    }
    if (remainder == 1)
    {
        padded_src[i] = '=';
        ++i;
        padded_src[i] = '=';
        ++i;
        padded_src[i] = '=';
    }
    else if (remainder == 2)
    {
        padded_src[i] = '=';
        ++i;
        padded_src[i] = '=';
    }
    else if (remainder == 3)
        padded_src[i] = '=';

    return padded_src;
}

size_t postprocessBase64Url(UInt8 * dst, size_t out_len)
{
    // Do symbol substitution as described in https://datatracker.ietf.org/doc/html/rfc4648#page-7
    for (size_t i = 0; i < out_len; ++i)
    {
        switch (dst[i])
        {
        case '/':
            dst[i] = '_';
            break;
        case '+':
            dst[i] = '-';
            break;
        case '=': // stop when padding is detected
            return i;
        default:
            break;
        }
    }
    return out_len;
}

}

#endif
