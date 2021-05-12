#include <Common/OpenFDMetrics.h>
#include <iostream>

int main() 
{
    for (int i = 0; i < 10; i++) {
        DB::OpenFDMetrics a;
        DB::OpenFDMetrics::Data x = a.get();
        std::cout << x.cnt << "\n";
    }
}
