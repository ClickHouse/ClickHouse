#include <iostream>
#include <stdexcept>

#include <common/LocalDateTime.h>


void fillStackWithGarbage()
{
    volatile uint64_t a = 0xAABBCCDDEEFF0011ULL;
    volatile uint64_t b = 0x2233445566778899ULL;
    std::cout << a + b << '\n';
}

void checkComparison()
{
    LocalDateTime a("2018-07-18 01:02:03");
    LocalDateTime b("2018-07-18 01:02:03");

    if (a != b)
        throw std::runtime_error("Test failed");
}


int main(int, char **)
{
    fillStackWithGarbage();
    checkComparison();
    return 0;
}
