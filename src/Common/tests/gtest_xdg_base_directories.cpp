#include <gtest/gtest.h>

#include <Common/XDGBaseDirectories.h>

#include <cstdlib>
#include <optional>
#include <string>
#include <utility>
#include <vector>


namespace
{

/// Saves and restores the environment variables that `XDGBaseDirectories` reads,
/// so that the tests do not leak their values into the rest of the test binary.
class XDGBaseDirectoriesTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        for (const char * name : names)
            saved.emplace_back(name, get(name));
    }

    void TearDown() override
    {
        for (const auto & [name, value] : saved)
        {
            if (value)
                set(name, *value);
            else
                unset(name);
        }
    }

    static std::optional<std::string> get(const char * name)
    {
        const char * value = getenv(name); // NOLINT(concurrency-mt-unsafe)
        if (!value)
            return {};
        return std::string(value);
    }

    static void set(const char * name, const std::string & value)
    {
        ASSERT_EQ(0, setenv(name, value.c_str(), 1)); // NOLINT(concurrency-mt-unsafe)
    }

    static void unset(const char * name)
    {
        ASSERT_EQ(0, unsetenv(name)); // NOLINT(concurrency-mt-unsafe)
    }

    static constexpr const char * names[]
        = {"HOME", "XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_STATE_HOME", "XDG_CACHE_HOME"};

    std::vector<std::pair<const char *, std::optional<std::string>>> saved;
};

}

/// Every getter must read its own environment variable, and only it.
/// The values are deliberately all different, so that a getter reading the wrong
/// variable (as `getCacheHome` did, reading `XDG_STATE_HOME`) fails the test.
TEST_F(XDGBaseDirectoriesTest, EnvironmentVariables)
{
    set("HOME", "/home/unused");
    set("XDG_CONFIG_HOME", "/xdg/config");
    set("XDG_DATA_HOME", "/xdg/data");
    set("XDG_STATE_HOME", "/xdg/state");
    set("XDG_CACHE_HOME", "/xdg/cache");

    EXPECT_EQ("/xdg/config/clickhouse", DB::XDGBaseDirectories::getConfigurationHome());
    EXPECT_EQ("/xdg/data/clickhouse", DB::XDGBaseDirectories::getDataHome());
    EXPECT_EQ("/xdg/state/clickhouse", DB::XDGBaseDirectories::getStateHome());
    EXPECT_EQ("/xdg/cache/clickhouse", DB::XDGBaseDirectories::getCacheHome());
}

/// Without the `XDG_*` variables, the paths are derived from `HOME`.
TEST_F(XDGBaseDirectoriesTest, HomeFallback)
{
    set("HOME", "/home/test");
    unset("XDG_CONFIG_HOME");
    unset("XDG_DATA_HOME");
    unset("XDG_STATE_HOME");
    unset("XDG_CACHE_HOME");

    EXPECT_EQ("/home/test/.config/clickhouse", DB::XDGBaseDirectories::getConfigurationHome());
    EXPECT_EQ("/home/test/.local/share/clickhouse", DB::XDGBaseDirectories::getDataHome());
    EXPECT_EQ("/home/test/.local/state/clickhouse", DB::XDGBaseDirectories::getStateHome());
    EXPECT_EQ("/home/test/.cache/clickhouse", DB::XDGBaseDirectories::getCacheHome());
}

/// With neither the `XDG_*` variable nor `HOME`, the path is empty.
TEST_F(XDGBaseDirectoriesTest, NoHome)
{
    unset("HOME");
    unset("XDG_CONFIG_HOME");
    unset("XDG_DATA_HOME");
    unset("XDG_STATE_HOME");
    unset("XDG_CACHE_HOME");

    EXPECT_EQ("", DB::XDGBaseDirectories::getConfigurationHome());
    EXPECT_EQ("", DB::XDGBaseDirectories::getDataHome());
    EXPECT_EQ("", DB::XDGBaseDirectories::getStateHome());
    EXPECT_EQ("", DB::XDGBaseDirectories::getCacheHome());
}
