// this is a test case for specific PR https://github.com/ClickHouse/ClickHouse/pull/89063
// should be removed after testing and not appear in master

#include <cstdint>
#include <cstddef>
#include <cstdlib>
#include <sys/types.h>

bool trap_it = false;
int odd_or_even = 0;

void trap_or_not(uint n)
{
    if (trap_it && n % 2 == odd_or_even)
        __builtin_trap();
}

extern "C"
int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    // Branch 1: "HI!" or "HI?" or "HOK" (first three bytes)
    if (size > 2 && data[0] == 'H')
    {
        if (data[1] == 'I')
        {
            if (data[2] == '!')
            {
                trap_or_not(0);
                return 0;
            }
            else if (data[2] == '?')
            {
                trap_or_not(1);
                return 0;
            }
        }
        else if (data[1] == 'O')
        {
            if (data[2] == 'K')
            {
                trap_or_not(2);
                return 0;
            }
        }
    }

    // Branch 2: "BYE"
    if (size > 2 && data[0] == 'B')
    {
        if (data[1] == 'Y' && data[2] == 'E')
        {
            trap_or_not(3);
            return 0;
        }
    }

    // Branch 3: first byte is 0xFF, second is 0x00, third is not zero
    if (size > 2 && data[0] == 0xFF)
    {
        if (data[1] == 0x00 && data[2] != 0x00)
        {
            trap_or_not(4);
            return 0;
        }
    }

    // Branch 4: size exactly 7 and all bytes are zero except last
    if (size == 7)
    {
        bool all_zero_except_last = true;
        for (size_t i = 0; i < 6; ++i)
            if (data[i] != 0)
                all_zero_except_last = false;
        if (all_zero_except_last && data[6] != 0)
        {
            trap_or_not(5);
            return 0;
        }
    }

    // Branch 5: if first byte is even and second is odd
    if (size > 1 && (data[0] % 2 == 0) && (data[1] % 2 == 1))
    {
        trap_or_not(6);
        return 0;
    }

    return 0;
}
