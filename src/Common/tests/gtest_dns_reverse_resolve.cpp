#include <gtest/gtest.h>
#include <thread>
#include <Common/DNSPTRResolverProvider.h>
#include <Common/DNSResolver.h>
#include <Poco/Net/IPAddress.h>
#include <random>

namespace DB
{
TEST(Common, ReverseDNS)
{
    auto addresses = std::vector<std::string>({
        "8.8.8.8", "2001:4860:4860::8888", // dns.google
        "142.250.219.35", // google.com
        "157.240.12.35", // facebook
        "208.84.244.116", "2600:1419:c400::214:c410", //www.terra.com.br,
        "127.0.0.1", "::1"
    });

    auto func = [&]()
    {
        // Good random seed, good engine
        auto rnd1 = std::mt19937(std::random_device{}());

        for (int i = 0; i < 50; ++i)
        {
            auto & dns_resolver_instance = DNSResolver::instance();
//            unfortunately, DNS cache can't be disabled because we might end up causing a DDoS attack
//            dns_resolver_instance.setDisableCacheFlag();

            auto addr_index = rnd1() % addresses.size();

            [[maybe_unused]] auto result = dns_resolver_instance.reverseResolve(Poco::Net::IPAddress{ addresses[addr_index] });

//            will not assert either because some of the IP addresses might change in the future and
//            this test will become flaky
//            ASSERT_TRUE(!result.empty());
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
