#include <iostream>
#include <stdexcept>
#include <gtest/gtest.h>

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

    EXPECT_EQ(a, b);
    EXPECT_FALSE(a != b);
}


TEST(LocalDateTime, Comparison)
{
    fillStackWithGarbage();
    checkComparison();
}
