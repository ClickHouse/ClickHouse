#include <Common/encodingURL.h>
#include <Common/memcpySmall.h>

size_t encodeURL(const char * __restrict src, size_t src_size, char * __restrict dst, bool space_as_plus)
{
    char * dst_pos = dst;
    for (size_t i = 0; i < src_size; ++i)
    {
        if ((src[i] >= '0' && src[i] <= '9') || (src[i] >= 'a' && src[i] <= 'z') || (src[i] >= 'A' && src[i] <= 'Z')
            || src[i] == '-' || src[i] == '_' || src[i] == '.' || src[i] == '~')
        {
            *dst_pos = src[i];
            ++dst_pos;
        }
        else if (src[i] == ' ' && space_as_plus)
        {
            *dst_pos = '+';
            ++dst_pos;
        }
        else
        {
            dst_pos[0] = '%';
            ++dst_pos;
            writeHexByteUppercase(src[i], dst_pos);
            dst_pos += 2;
        }
    }
    *dst_pos = 0;
    ++dst_pos;
    return dst_pos - dst;
}


size_t decodeURL(const char * __restrict src, size_t src_size, char * __restrict dst, bool plus_as_space)
{
    const char * src_prev_pos = src;
    const char * src_curr_pos = src;
    const char * src_end = src + src_size;
    char * dst_pos = dst;

    while (true)
    {
        src_curr_pos = find_first_symbols<'%', '+'>(src_curr_pos, src_end);

        if (src_curr_pos == src_end)
        {
            break;
        }
        if (*src_curr_pos == '+')
        {
            if (!plus_as_space)
            {
                ++src_curr_pos;
                continue;
            }
            size_t bytes_to_copy = src_curr_pos - src_prev_pos;
            memcpySmallAllowReadWriteOverflow15(dst_pos, src_prev_pos, bytes_to_copy);
            dst_pos += bytes_to_copy;

            ++src_curr_pos;
            src_prev_pos = src_curr_pos;
            *dst_pos = ' ';
            ++dst_pos;
        }
        else if (src_end - src_curr_pos < 3)
        {
            src_curr_pos = src_end;
            break;
        }
        else
        {
            unsigned char high = unhex(src_curr_pos[1]);
            unsigned char low = unhex(src_curr_pos[2]);

            if (high != 0xFF && low != 0xFF)
            {
                unsigned char octet = (high << 4) + low;

                size_t bytes_to_copy = src_curr_pos - src_prev_pos;
                memcpySmallAllowReadWriteOverflow15(dst_pos, src_prev_pos, bytes_to_copy);
                dst_pos += bytes_to_copy;

                *dst_pos = octet;
                ++dst_pos;

                src_prev_pos = src_curr_pos + 3;
            }

            src_curr_pos += 3;
        }
    }

    if (src_prev_pos < src_curr_pos)
    {
        size_t bytes_to_copy = src_curr_pos - src_prev_pos;
        memcpySmallAllowReadWriteOverflow15(dst_pos, src_prev_pos, bytes_to_copy);
        dst_pos += bytes_to_copy;
    }

    return dst_pos - dst;
}
