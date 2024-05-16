#include <IO/WriteBufferFromFile.h>
#include <base/sleep.h>
#include <Common/CurrentThread.h>
#include <Common/HostResolvePool.h>

#include <thread>
#include <gtest/gtest.h>

class ResolvePoolMock : public DB::HostResolver
{
public:
    using ResolveFunction = DB::HostResolver::ResolveFunction;

    ResolvePoolMock(String host_, Poco::Timespan history_, ResolveFunction && func)
    : DB::HostResolver(std::move(func), std::move(host_), history_)
    {
    }
};

class ResolvePoolTest : public testing::Test
{
protected:
    ResolvePoolTest()
    {
        DB::HostResolversPool::instance().dropCache();
    }

    void SetUp() override {
        DB::CurrentThread::getProfileEvents().reset();

        ASSERT_EQ(0, CurrentMetrics::get(metrics.active_count));

        addresses = std::set<String>{"127.0.0.1", "127.0.0.2", "127.0.0.3"};
        // Code here will be called immediately after the constructor (right
        // before each test).
    }

    void TearDown() override {
        // Code here will be called immediately after each test (right
        // before the destructor).
    }

    DB::HostResolver::Ptr make_resolver(size_t history_ms = 200)
    {
        auto resolve_func = [&] (const String &)
        {
            std::vector<Poco::Net::IPAddress> result;
            result.reserve(addresses.size());
            for (const auto & item : addresses)
            {
                result.push_back(Poco::Net::IPAddress(item));
            }
            return result;
        };


        return std::make_shared<ResolvePoolMock>("some_host", Poco::Timespan(history_ms * 1000), std::move(resolve_func));
    }

    DB::HostResolverMetrics metrics = DB::HostResolver::getMetrics();
    std::set<String> addresses;
};

TEST_F(ResolvePoolTest, CanResolve)
{
    auto resolver = make_resolver();
    auto address = resolver->resolve();

    ASSERT_TRUE(addresses.contains(*address));

    ASSERT_EQ(addresses.size(), DB::CurrentThread::getProfileEvents()[metrics.discovered]);
    ASSERT_EQ(addresses.size(), CurrentMetrics::get(metrics.active_count));
}

TEST_F(ResolvePoolTest, CanResolveAll)
{
    auto resolver = make_resolver();

    std::set<String> results;
    while (results.size() != addresses.size())
    {
        auto next_addr = resolver->resolve();
        results.insert(*next_addr);
    }

    ASSERT_EQ(addresses.size(), DB::CurrentThread::getProfileEvents()[metrics.discovered]);
}

size_t getSum(std::map<String, size_t> container)
{
    size_t sum = 0;
    for (auto & [_, val] : container)
    {
        sum += val;
    }
    return sum;
}

size_t getMin(std::map<String, size_t> container)
{
    if (container.empty())
        return 0;

    size_t min_val = container.begin()->second;
    for (auto & [_, val] : container)
    {
        min_val = std::min(min_val, val);
    }
    return min_val;
}

double getMean(std::map<String, size_t> container)
{
    return 1.0 * getSum(container) / container.size();
}

double getMaxDiff(std::map<String, size_t> container, double ref_val)
{
    double diff = 0.0;
    for (auto & [_, val] : container)
    {
        diff = std::max(std::fabs(val - ref_val), diff);
    }

    return diff;
}

TEST_F(ResolvePoolTest, CanResolveEvenly)
{
    auto resolver = make_resolver();

    std::map<String, size_t> results;

    for (size_t i = 0; i < 50000; ++i)
    {
        auto next_addr = resolver->resolve();
        if (results.contains(*next_addr))
        {
            results[*next_addr] += 1;
        }
        else
        {
            results[*next_addr] = 1;
        }
    }

    auto mean = getMean(results);
    auto diff = getMaxDiff(results, mean);

    ASSERT_GT(0.3 * mean, diff);
}

TEST_F(ResolvePoolTest, CanMerge)
{
    auto resolver = make_resolver(100000);
    auto address = resolver->resolve();

    ASSERT_TRUE(addresses.contains(*address));

    ASSERT_EQ(addresses.size(), DB::CurrentThread::getProfileEvents()[metrics.discovered]);

    auto old_addresses = addresses;
    addresses = std::set<String>{"127.0.0.4", "127.0.0.5"};


    resolver->update();
    ASSERT_EQ(addresses.size() + old_addresses.size(), DB::CurrentThread::getProfileEvents()[metrics.discovered]);
    ASSERT_EQ(addresses.size() + old_addresses.size(), CurrentMetrics::get(metrics.active_count));

    std::set<String> results;
    while (results.size() != addresses.size() + old_addresses.size())
    {
        auto next_addr = resolver->resolve();
        results.insert(*next_addr);
    }
}

TEST_F(ResolvePoolTest, CanGainEven)
{
    auto resolver = make_resolver();
    auto address = resolver->resolve();

    std::map<String, size_t> results;
    for (size_t i = 0; i < 40000; ++i)
    {
        auto next_addr = resolver->resolve();
        if (results.contains(*next_addr))
        {
            results[*next_addr] += 1;
        }
        else
        {
            results[*next_addr] = 1;
        }
    }

    ASSERT_GT(getMin(results), 10000);

    addresses.insert("127.0.0.4");
    addresses.insert("127.0.0.5");

    resolver->update();

    /// return mostly new addresses
    for (size_t i = 0; i < 3000; ++i)
    {
        auto next_addr = resolver->resolve();
        if (results.contains(*next_addr))
        {
            results[*next_addr] += 1;
        }
        else
        {
            results[*next_addr] = 1;
        }
    }

    ASSERT_EQ(results.size(), 5);

    ASSERT_GT(getMin(results), 1000);
}

TEST_F(ResolvePoolTest, CanFail)
{
    auto resolver = make_resolver(10000);

    auto failed_addr = resolver->resolve();
    failed_addr.setFail();

    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.failed]);
    ASSERT_EQ(addresses.size(), CurrentMetrics::get(metrics.active_count));
    ASSERT_EQ(addresses.size(), DB::CurrentThread::getProfileEvents()[metrics.discovered]);

    for (size_t i = 0; i < 1000; ++i)
    {
        auto next_addr = resolver->resolve();

        ASSERT_TRUE(addresses.contains(*next_addr));
        ASSERT_NE(*next_addr, *failed_addr);
    }
}

TEST_F(ResolvePoolTest, CanFailAndHeal)
{
    auto resolver = make_resolver();

    auto failed_addr = resolver->resolve();
    failed_addr.setFail();

    while (true)
    {
        auto next_addr = resolver->resolve();
        if (*failed_addr == *next_addr)
            break;
    }
}


TEST_F(ResolvePoolTest, CanExpire)
{
    auto resolver = make_resolver();

    auto expired_addr = resolver->resolve();
    ASSERT_TRUE(addresses.contains(*expired_addr));

    addresses.erase(*expired_addr);
    sleepForSeconds(1);

    for (size_t i = 0; i < 1000; ++i)
    {
        auto next_addr = resolver->resolve();

        ASSERT_TRUE(addresses.contains(*next_addr));
        ASSERT_NE(*next_addr, *expired_addr);
    }

    ASSERT_EQ(addresses.size() + 1, DB::CurrentThread::getProfileEvents()[metrics.discovered]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.expired]);
}
