#include <Storages/MergeTree/ZooKeeperRetries.h>

#include <random>
#include <Common/randomSeed.h>
#include <pcg_random.hpp>

#include <gtest/gtest.h>

using namespace DB;

static pcg64 rng(randomSeed());

constexpr static size_t MAX_RETRIES = 5;

static ZooKeeperRetriesInfo getRetriesInfo()
{
    return ZooKeeperRetriesInfo(
        "ZkRetryControllTest",
        &Poco::Logger::get("ZkRetryControllTest"),
        /* max_retries_ */ MAX_RETRIES,
        /* initial_backoff_ms_ */ 10,
        /* max_backoff_ms_ */ 100);
}

TEST(ZkRetryControll, Simple)
{
    String data;

    {
        for (size_t i = 0; i < 100; ++i)
        {
            auto retries_info = getRetriesInfo();
            auto retries_ctl = ZooKeeperRetriesControl("executeDDLQueryOnCluster", retries_info);

            size_t retry_cnt = 0;
            retries_ctl.retryLoop([&]()
            {
                retry_cnt += 1;

                /// random number from 0 to MAX_RETRIES
                size_t max_retries = rng() % (MAX_RETRIES + 1);
                if (retry_cnt >= max_retries)
                {
                    data += "test";
                    return;
                }
                throw zkutil::KeeperException("test", Coordination::Error::ZSESSIONEXPIRED);
            });
        }
    }

    ASSERT_EQ(data.size(), std::string("test").size() * 100);
}
