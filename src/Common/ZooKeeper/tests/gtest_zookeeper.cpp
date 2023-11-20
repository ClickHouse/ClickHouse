#include <string>
#include <vector>
#include <gtest/gtest.h>

#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeper.h>

TEST(ZooKeeperTest, TestMatchPath)
{
    using namespace Coordination;

    ASSERT_EQ(matchPath("/path/file", "/path"), PathMatchResult::IS_CHILD);
    ASSERT_EQ(matchPath("/path/file", "/path/"), PathMatchResult::IS_CHILD);
    ASSERT_EQ(matchPath("/path/file", "/"), PathMatchResult::IS_CHILD);
    ASSERT_EQ(matchPath("/", "/"), PathMatchResult::EXACT);
    ASSERT_EQ(matchPath("/path", "/path/"), PathMatchResult::EXACT);
    ASSERT_EQ(matchPath("/path/", "/path"), PathMatchResult::EXACT);
}

TEST(ZooKeeperTest, AvailabilityZoneHelper)
{
    using namespace zkutil;
    auto& az_helper = ZooKeeperAvailabilityZoneMap::instance();
    az_helper.skip_dns_check_for_test = true;

    const std::string local_az = "us-west-2b";
    std::vector<std::string> hosts{"keeper1", "keeper2"};
    bool dns_error_occurred = false;
    az_helper.update("keeper1", "us-west-2a");
    auto actual = az_helper.shuffleHosts(&Poco::Logger::get("Application"), local_az, hosts, dns_error_occurred);

    // Keeper2 comes first because it's unknown, keeper1 is in different az.
    std::vector<ShuffleHost> expected{
        {"keeper2", 1, Priority{1} },
        {"keeper1", 0, Priority{2}},
    };
    ASSERT_EQ(actual.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i)
    {
        ASSERT_EQ(expected[i].host, actual[i].host);
        ASSERT_EQ(expected[i].original_index, actual[i].original_index);
        ASSERT_EQ(expected[i].priority, actual[i].priority);
    }

    az_helper.update("keeper3", "us-west-2b");
    expected = {
        {"keeper3", 2, Priority{0} },
        {"keeper2", 1, Priority{1} },
        {"keeper1", 0, Priority{2}},
    };
    hosts = {"keeper1", "keeper2", "keeper3"};
    actual = az_helper.shuffleHosts(&Poco::Logger::get("Application"), local_az, hosts, dns_error_occurred);
    ASSERT_EQ(actual.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i)
    {
        ASSERT_EQ(expected[i].host, actual[i].host);
        ASSERT_EQ(expected[i].original_index, actual[i].original_index);
        ASSERT_EQ(expected[i].priority, actual[i].priority);
    }
}
