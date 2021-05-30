#include <Common/NetworkMetrics.h>
#include <iostream>

#include <gtest/gtest.h>

TEST(NetworkMetrics, SimpleTest) 
{
    std::cout << "NetworkMetrics\n";
    for (int i = 0; i < 10; i++)
    {
        DB::NetworkMetrics a;
        DB::NetworkMetrics::Data x = a.get();
        std::cout << x.received_bytes << " "
                  << x.received_packets << " "
                  << x.transmitted_bytes << " "
                  << x.transmitted_packets << " "
                  << x.tcp_retransmit << " "
                  << x.tcp << " "
                  << x.udp << " "
                  << x.distinct_hosts << "\n";
    }
}
