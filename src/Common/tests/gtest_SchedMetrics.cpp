#include <Common/SchedMetrics.h>
#include <iostream>

int main() 
{
    for (int i = 0; i < 100000; i++) 
    {
        DB::SchedMetrics a;
        DB::SchedMetrics::Data x = a.get();
        std::cout << x.total_csw << "\n";
    }
}
