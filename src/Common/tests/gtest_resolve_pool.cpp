#include <IO/WriteBufferFromFile.h>
#include <Common/CurrentThread.h>
#include <Common/HostResolvePool.h>
#include <base/defines.h>

#include <gtest/gtest.h>

#include <optional>
#include <chrono>
#include <thread>


using namespace std::literals::chrono_literals;


static auto now()
{
    return std::chrono::steady_clock::now();
}

void sleep_until(auto time_point)
{
    std::this_thread::sleep_until(time_point);
}

void sleep_for(auto duration)
{
    std::this_thread::sleep_for(duration);
}

size_t toMilliseconds(auto duration)
{
   return std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
}

const auto epsilon = 1ms;

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
        ASSERT_EQ(0, CurrentMetrics::get(metrics.banned_count));

        addresses = std::multiset<String>{"127.0.0.1", "127.0.0.2", "127.0.0.3"};
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
    std::multiset<String> addresses;
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

static size_t getSum(std::map<String, size_t> container)
{
    size_t sum = 0;
    for (auto & [_, val] : container)
    {
        sum += val;
    }
    return sum;
}

static size_t getMin(std::map<String, size_t> container)
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

static double getMean(std::map<String, size_t> container)
{
    return 1.0 * static_cast<double>(getSum(container)) / static_cast<double>(container.size());
}

static double getMaxDiff(std::map<String, size_t> container, double ref_val)
{
    double diff = 0.0;
    for (auto & [_, val] : container)
    {
        diff = std::max(std::fabs(static_cast<double>(val) - ref_val), diff);
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
    addresses = std::multiset<String>{"127.0.0.4", "127.0.0.5"};


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
    ASSERT_EQ(1, CurrentMetrics::get(metrics.banned_count));
    ASSERT_EQ(addresses.size(), DB::CurrentThread::getProfileEvents()[metrics.discovered]);

    for (size_t i = 0; i < 1000; ++i)
    {
        auto next_addr = resolver->resolve();

        ASSERT_TRUE(addresses.contains(*next_addr));
        ASSERT_NE(*next_addr, *failed_addr);
    }
}

TEST_F(ResolvePoolTest, WeightsConsistentWhenFailRefreshThrows)
{
    /// `setFail` records the per-address failure and then performs a DNS refresh via
    /// `update`, which can throw (e.g. NXDOMAIN or an empty result). The failed address
    /// must still be excluded from selection right away: its selection weight has to be
    /// recomputed under the lock before the throwing refresh, otherwise `selectBest`
    /// would keep handing out the just-failed address from a stale weight table.
    bool throw_on_resolve = false;
    std::multiset<String> local_addresses{"127.0.0.1", "127.0.0.2", "127.0.0.3"};

    auto resolve_func = [&] (const String &)
    {
        std::vector<Poco::Net::IPAddress> result;
        if (throw_on_resolve)
            return result; /// Empty result makes `HostResolver::update` throw `DNS_ERROR`.
        result.reserve(local_addresses.size());
        for (const auto & item : local_addresses)
            result.push_back(Poco::Net::IPAddress(item));
        return result;
    };

    /// Large history so the ban does not expire and no background refresh fires during the loop.
    auto resolver = std::make_shared<ResolvePoolMock>("some_host", Poco::Timespan(10 * 1000 * 1000), std::move(resolve_func));

    auto failed_addr = resolver->resolve();
    String failed = *failed_addr;

    /// Force the DNS refresh triggered inside `setFail` to throw.
    throw_on_resolve = true;
    EXPECT_ANY_THROW(failed_addr.setFail());
    throw_on_resolve = false;

    /// The just-failed address must not be selected anymore despite the failed refresh.
    for (size_t i = 0; i < 1000; ++i)
    {
        auto next_addr = resolver->resolve();
        ASSERT_NE(*next_addr, failed);
        next_addr.setUnused();
    }
}

TEST_F(ResolvePoolTest, SetUnusedHasNoSideEffects)
{
    auto resolver = make_resolver();

    /// `setUnused` is for the case where the caller selected an address via `resolve` but never
    /// actually attempted a connection (e.g. the address duplicated one tried earlier in the same
    /// scope). It must suppress the destructor's `setSuccess` callback without recording a
    /// failure to the pool, otherwise duplicate hits would inflate `HostResolverFailed` and
    /// trigger spurious DNS refreshes.
    {
        auto addr = resolver->resolve();
        addr.setUnused();
    }

    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.failed]);
    ASSERT_EQ(0, CurrentMetrics::get(metrics.banned_count));
    /// Only the initial resolve happened, no extra DNS refresh from `setUnused`.
    ASSERT_EQ(addresses.size(), DB::CurrentThread::getProfileEvents()[metrics.discovered]);
}

TEST_F(ResolvePoolTest, CanFailAndHeal)
{
    auto resolver = make_resolver();
    ASSERT_EQ(0, CurrentMetrics::get(metrics.banned_count));

    auto failed_addr = resolver->resolve();
    failed_addr.setFail();
    ASSERT_EQ(1, CurrentMetrics::get(metrics.banned_count));

    while (true)
    {
        auto next_addr = resolver->resolve();
        if (*failed_addr == *next_addr)
        {
            ASSERT_EQ(0, CurrentMetrics::get(metrics.banned_count));
            break;
        }
    }
}


TEST_F(ResolvePoolTest, CanExpire)
{
    auto history = 5ms;
    auto resolver = make_resolver(toMilliseconds(history));

    auto expired_addr = resolver->resolve();
    ASSERT_TRUE(addresses.contains(*expired_addr));
    addresses.erase(*expired_addr);

    sleep_for(history + epsilon);

    for (size_t i = 0; i < 1000; ++i)
    {
        auto next_addr = resolver->resolve();

        ASSERT_TRUE(addresses.contains(*next_addr));
        ASSERT_NE(*next_addr, *expired_addr);
    }

    ASSERT_EQ(addresses.size() + 1, DB::CurrentThread::getProfileEvents()[metrics.discovered]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.expired]);
}


TEST_F(ResolvePoolTest, DuplicatesInAddresses)
{
    auto resolver = make_resolver();

    size_t unuque_addresses = addresses.size();

    ASSERT_EQ(3, unuque_addresses);
    ASSERT_EQ(3, DB::CurrentThread::getProfileEvents()[metrics.discovered]);

    ASSERT_TRUE(!addresses.empty());
    addresses.insert(*addresses.begin());
    addresses.insert(*addresses.begin());

    size_t total_addresses = addresses.size();

    ASSERT_EQ(addresses.count(*addresses.begin()), 3);
    ASSERT_EQ(unuque_addresses + 2, total_addresses);

    resolver->update();
    ASSERT_EQ(3, DB::CurrentThread::getProfileEvents()[metrics.discovered]);
}

void check_no_failed_address(size_t iteration, auto & resolver, auto & addresses, auto & failed_addr, auto & metrics, auto deadline)
{
    ASSERT_EQ(iteration, DB::CurrentThread::getProfileEvents()[metrics.failed]);
    for (size_t i = 0; i < 100; ++i)
    {
        auto next_addr = resolver->resolve();

        if (now() > deadline)
        {
            ASSERT_NE(i, 0);
            break;
        }

        ASSERT_TRUE(addresses.contains(*next_addr));
        ASSERT_NE(*next_addr, *failed_addr);
    }
}

TEST_F(ResolvePoolTest, BannedForConsiquenceFail)
{
    auto history = 10ms;
    auto resolver = make_resolver(toMilliseconds(history));

    auto failed_addr = resolver->resolve();
    ASSERT_TRUE(addresses.contains(*failed_addr));


    failed_addr.setFail();
    auto start_at = now();

    ASSERT_EQ(3, CurrentMetrics::get(metrics.active_count));
    ASSERT_EQ(1, CurrentMetrics::get(metrics.banned_count));
    check_no_failed_address(1, resolver, addresses, failed_addr, metrics, start_at + history - epsilon);

    sleep_until(start_at + history + epsilon);

    resolver->update();
    ASSERT_EQ(3, CurrentMetrics::get(metrics.active_count));
    ASSERT_EQ(0, CurrentMetrics::get(metrics.banned_count));

    failed_addr.setFail();
    start_at = now();

    check_no_failed_address(2, resolver, addresses, failed_addr, metrics, start_at + history - epsilon);

    sleep_until(start_at + history + epsilon);

    resolver->update();

    // too much time has passed
    if (now() > start_at + 2*history - epsilon)
        return;

    ASSERT_EQ(3, CurrentMetrics::get(metrics.active_count));
    ASSERT_EQ(1, CurrentMetrics::get(metrics.banned_count));

    // ip still banned adter history_ms + update, because it was his second consiquent fail
    check_no_failed_address(2, resolver, addresses, failed_addr, metrics, start_at + 2*history - epsilon);
}

TEST_F(ResolvePoolTest, NoAditionalBannForConcurrentFail)
{
    auto history = 10ms;
    auto resolver = make_resolver(toMilliseconds(history));

    auto failed_addr = resolver->resolve();
    ASSERT_TRUE(addresses.contains(*failed_addr));

    failed_addr.setFail();
    failed_addr.setFail();
    failed_addr.setFail();

    auto start_at = now();

    ASSERT_EQ(3, CurrentMetrics::get(metrics.active_count));
    ASSERT_EQ(1, CurrentMetrics::get(metrics.banned_count));
    check_no_failed_address(3, resolver, addresses, failed_addr, metrics, start_at + history - epsilon);

    sleep_until(start_at + history + epsilon);

    resolver->update();

    // ip is cleared after just 1 history_ms interval.
    ASSERT_EQ(3, CurrentMetrics::get(metrics.active_count));
    ASSERT_EQ(0, CurrentMetrics::get(metrics.banned_count));
}

TEST_F(ResolvePoolTest, StillBannedAfterSuccess)
{
    auto history = 5ms;
    auto resolver = make_resolver(toMilliseconds(history));

    auto failed_addr = resolver->resolve();
    ASSERT_TRUE(addresses.contains(*failed_addr));

    std::optional<decltype(resolver->resolve())> again_addr;
    while (true)
    {
        auto addr = resolver->resolve();
        if (*addr == *failed_addr)
        {
            again_addr.emplace(std::move(addr));
            break;
        }
    }
    chassert(again_addr);

    auto start_at = now();
    failed_addr.setFail();

    ASSERT_EQ(3, CurrentMetrics::get(metrics.active_count));
    ASSERT_EQ(1, CurrentMetrics::get(metrics.banned_count));
    check_no_failed_address(1, resolver, addresses, failed_addr, metrics, start_at + history - epsilon);

    again_addr = std::nullopt; // success;

    ASSERT_EQ(3, CurrentMetrics::get(metrics.active_count));
    ASSERT_EQ(1, CurrentMetrics::get(metrics.banned_count));
}
