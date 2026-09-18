#include <gtest/gtest.h>

#include <Coordination/KeeperAsynchronousMetrics.h>

#include <limits>
#include <optional>

using namespace DB;

/// `getCurrentProcessFDCount` returns `-1` and `getMaxFileDescriptorCount` returns `std::nullopt` when the count
/// cannot be determined. The sentinel must survive as `-1` instead of wrapping around to 2^64 - 1, which is
/// indistinguishable from an unlimited `RLIMIT_NOFILE`. This is the branch that a running Keeper does not take
/// on Linux and macOS, so it is checked here directly.
TEST(KeeperAsynchronousMetrics, UndeterminedFileDescriptorCounts)
{
    AsynchronousMetricValues values;
    setKeeperFileDescriptorMetrics(values, -1, std::nullopt);

    ASSERT_EQ(values.at("KeeperOpenFileDescriptorCount").value, -1.0);
    ASSERT_EQ(values.at("KeeperMaxFileDescriptorCount").value, -1.0);
}

TEST(KeeperAsynchronousMetrics, DeterminedFileDescriptorCounts)
{
    AsynchronousMetricValues values;
    setKeeperFileDescriptorMetrics(values, 42, 1024);

    ASSERT_EQ(values.at("KeeperOpenFileDescriptorCount").value, 42.0);
    ASSERT_EQ(values.at("KeeperMaxFileDescriptorCount").value, 1024.0);
}

/// An unlimited `RLIMIT_NOFILE` is reported verbatim and must not be confused with the `-1` sentinel.
TEST(KeeperAsynchronousMetrics, UnlimitedMaxFileDescriptorCount)
{
    AsynchronousMetricValues values;
    setKeeperFileDescriptorMetrics(values, 42, std::numeric_limits<size_t>::max());

    ASSERT_EQ(values.at("KeeperMaxFileDescriptorCount").value, static_cast<double>(std::numeric_limits<size_t>::max()));
}
