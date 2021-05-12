#include <Common/IOMetrics.h>
#include <iostream>

int main() 
{
    for (int i = 0; i < 10; i++) 
    {
        DB::IOMetrics a;
        DB::IOMetrics::Data x = a.get();
        std::cout << "Read:\n";
        std::cout << x.read_total << " " << x.read_avg << "\n";
        for (auto &it : x.dev_read) 
        {
            std::cout << it << " "; 
        }
        std::cout << "\n";
        std::cout << "write:\n";
        std::cout << x.write_total << " " << x.write_avg << "\n";
        for (auto &it : x.dev_write) 
        {
            std::cout << it << " "; 
        }
        std::cout << "\n";
        std::cout << "Queue size:\n";
        std::cout << x.queue_size_total << " " << x.queue_size_avg << "\n";
        for (auto &it : x.dev_queue_size) 
        {
            std::cout << it << " "; 
        }
        std::cout << "\n";
        std::cout << "Util:\n";
        std::cout << x.util_total << " " << x.util_avg << "\n";
        for (auto &it : x.dev_util) 
        {
            std::cout << it << " "; 
        }
        std::cout << "Tcp:\n";
        std::cout << x.tcp_total << " " << x.tcp_avg << "\n";
        for (auto &it : x.dev_tcp) 
        {
            std::cout << it << " "; 
        }
        std::cout << "\n\n";
    }
}   
