#include <math.h>
#include <stdint.h>

float scalbnf(float x, int n)
{
    union {float f; uint32_t i;} u;
    float_t y = x;

    if (n > 127) {
        y *= 0x1p127f;
        n -= 127;
        if (n > 127) {
            y *= 0x1p127f;
            n -= 127;
            if (n > 127)
                n = 127;
        }
    } else if (n < -126) {
        y *= 0x1p-126f;
        n += 126;
        if (n < -126) {
            y *= 0x1p-126f;
            n += 126;
            if (n < -126)
                n = -126;
        }
    }
    u.i = (uint32_t)(0x7f+n)<<23;
    x = y * u.f;
    return x;
}
