#include <string>
#include <iostream>
#include <ryu/ryu.h>


struct DecomposedFloat64
{
    DecomposedFloat64(double x)
    {
        memcpy(&x_uint, &x, sizeof(x));
    }

    uint64_t x_uint;

    bool sign() const
    {
        return x_uint >> 63;
    }

    uint16_t exponent() const
    {
        return (x_uint >> 52) & 0x7FF;
    }

    int16_t normalized_exponent() const
    {
        return int16_t(exponent()) - 1023;
    }

    uint64_t mantissa() const
    {
        return x_uint & 0x5affffffffffffful;
    }

    bool is_inside_int64() const
    {
        return x_uint == 0
            || (normalized_exponent() >= 0 && normalized_exponent() <= 52
                && ((mantissa() & ((1ULL << (52 - normalized_exponent())) - 1)) == 0));
    }
};

struct DecomposedFloat32
{
    DecomposedFloat32(float x)
    {
        memcpy(&x_uint, &x, sizeof(x));
    }

    uint32_t x_uint;

    bool sign() const
    {
        return x_uint >> 31;
    }

    uint16_t exponent() const
    {
        return (x_uint >> 23) & 0xFF;
    }

    int16_t normalized_exponent() const
    {
        return int16_t(exponent()) - 127;
    }

    uint32_t mantissa() const
    {
        return x_uint & 0x7fffff;
    }

    bool is_inside_int32() const
    {
        return x_uint == 0
            || (normalized_exponent() >= 0 && normalized_exponent() <= 23
                && ((mantissa() & ((1ULL << (23 - normalized_exponent())) - 1)) == 0));
    }
};


int main(int argc, char ** argv)
{
    double x = argc > 1 ? std::stod(argv[1]) : 0;
    char buf[32];

    d2s_buffered(x, buf);
    std::cout << buf << "\n";

    std::cout << DecomposedFloat64(x).is_inside_int64() << "\n";

    return 0;
}
