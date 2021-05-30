#include <Common/SchedMetrics.h>
#include <iostream>

#include <gtest/gtest.h>

TEST(SchedMetrics, SimpleTest) 
{
    for (int i = 0; i < 10; i++) 
    {
        DB::SchedMetrics a;
        DB::SchedMetrics::Data x = a.get();
        std::cout << x.total_csw << "\n";
    }
}
