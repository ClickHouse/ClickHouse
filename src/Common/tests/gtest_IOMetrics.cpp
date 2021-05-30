#include <Common/IOMetrics.h>
#include <iostream>
#include <gtest/gtest.h>

TEST(IOMetrics, SimpleTest) 
{
    std::cout << "IOMetrics\n";
    for (int i = 0; i < 10; i++) 
    {
        DB::IOMetrics a;
        DB::IOMetrics::Data x = a.get();
        std::cout << "Read\n";
        std::cout << x.read_total << " " << x.read_avg << "\n";
        for (auto &it : x.dev_read) 
        {
            std::cout << it.first << " " << it.second << "\n"; 
        }
        std::cout << "Write\n";
        std::cout << x.write_total << " " << x.write_avg << "\n";
        for (auto &it : x.dev_write) 
        {
            std::cout << it.first << " " << it.second << "\n"; 
        }
        std::cout << "Queue Size\n";
        std::cout << x.queue_size_total << " " << x.queue_size_avg << "\n";
        for (auto &it : x.dev_queue_size) 
        {
            std::cout << it.first << " " << it.second << "\n"; 
        }
        std::cout << "Util\n";
        std::cout << x.util_total << " " << x.util_avg << "\n";
        for (auto &it : x.dev_util) 
        {
            std::cout << it.first << " " << it.second << "\n"; 
        }
        std::cout << "TPS\n";
        std::cout << x.tps_total << " " << x.tps_avg << "\n";
        for (auto &it : x.dev_tps) 
        {
            std::cout << it.first << " " << it.second << "\n"; 
        }        
    }
}
