#include <zlib.h>

extern "C" uLong public_zlib_compress_bound(uLong size)
{
    return compressBound(size);
}
