#include <gtest/gtest.h>
#include <thread>
#include <cmath>
#include <Common/DNSPTRResolverProvider.h>
#include <Common/DNSResolver.h>
#include <Poco/Net/IPAddress.h>
#include <random>

namespace DB
{
TEST(Common, ReverseDNS)
{
    auto func = [&]()
    {
        // Good random seed, good engine
        auto rnd1 = std::mt19937(std::random_device{}());

        for (int i = 0; i < 10; ++i)
        {
            auto & dns_resolver_instance = DNSResolver::instance();
            dns_resolver_instance.setDisableCacheFlag();

            auto val1 = rnd1() % static_cast<uint32_t>((std::pow(2, 31) - 1));
            auto val2 = rnd1() % static_cast<uint32_t>((std::pow(2, 31) - 1));
            auto val3 = rnd1() % static_cast<uint32_t>((std::pow(2, 31) - 1));
            auto val4 = rnd1() % static_cast<uint32_t>((std::pow(2, 31) - 1));

            uint32_t ipv4_buffer[1] = {
                static_cast<uint32_t>(val1)
            };

            uint32_t ipv6_buffer[4] = {
                static_cast<uint32_t>(val1),
                static_cast<uint32_t>(val2),
                static_cast<uint32_t>(val3),
                static_cast<uint32_t>(val4)
            };

            dns_resolver_instance.reverseResolve(Poco::Net::IPAddress{ ipv4_buffer, sizeof(ipv4_buffer)});
            dns_resolver_instance.reverseResolve(Poco::Net::IPAddress{ ipv6_buffer, sizeof(ipv6_buffer)});
        }
    };

    auto number_of_threads = 200u;
    std::vector<std::thread> threads;
    threads.reserve(number_of_threads);

    for (auto i = 0u; i < number_of_threads; i++)
    {
        threads.emplace_back(func);
    }

    for (auto & thread : threads)
    {
        thread.join();
    }

}
}
