/* Public fill entry; compiler-generated unwind data is retained. */
#include "kernels.h"
#include "aarch64_helpers.h"

void *str_memset(void *d, int c, size_t n)
{
    if (n <= 16)
    {
        u8 *bytes = d;
        uint64_t value = (u8)c * UINT64_C(0x0101010101010101);
        if (n >= 8)
        {
            kl_store64(bytes, value);
            kl_store64(bytes + n - 8, value);
        }
        else if (n >= 4)
        {
            kl_store32(bytes, (uint32_t)value);
            kl_store32(bytes + n - 4, (uint32_t)value);
        }
        else if (n)
        {
            bytes[0] = (u8)c;
            bytes[n / 2] = (u8)c;
            bytes[n - 1] = (u8)c;
        }
        return d;
    }
    return kl_memset_neon_bulk128(d, c, n);
}
