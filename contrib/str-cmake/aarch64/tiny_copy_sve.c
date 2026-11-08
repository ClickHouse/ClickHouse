#include <arm_sve.h>
#include <stddef.h>
#include <stdint.h>

/* The dispatcher guarantees `n <= 16`; inactive lanes never access memory. */
void *ch_str_sve_copy_tiny16(void *d, const void *s, size_t n);
void *ch_str_sve_copy_tiny16(void *d, const void *s, size_t n)
{
    svbool_t active = svwhilelt_b8((uint64_t)0, (uint64_t)n);
    svuint8_t bytes = svld1_u8(active, (const unsigned char *)s);
    svst1_u8(active, (unsigned char *)d, bytes);
    return d;
}
