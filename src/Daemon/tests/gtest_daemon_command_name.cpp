#include <Daemon/BaseDaemon.h>

#include <gtest/gtest.h>

/// `BaseDaemon::initialize` passes the result of the virtual `getCommandName()` to `buildLoggers`, which is where
/// the default syslog program name comes from. `Server` and `Keeper` override it as
/// `disambiguateCommandName("clickhouse-server" / "clickhouse-keeper")`, which delegates to the function tested
/// here.
///
/// The two name arguments are different things and the distinction is the point of most of these cases:
/// `application.name` is the full file name of the executable, while `commandName()` is `application.baseName`,
/// which `Poco::Path::getBaseName` truncates at the last dot.
///
/// The mapping is tested through the free function rather than through a daemon instance on purpose: constructing
/// any `Poco::Util::Application` asserts that it is the only instance in the process and installs process-wide
/// signal handlers, neither of which is acceptable inside the shared unit test binary.

TEST(BaseDaemonCommandName, MultiPurposeBinaryGetsSubcommandName)
{
    /// `programs/main.cpp` erases the subcommand argument, so `clickhouse server` and `clickhouse keeper` both
    /// report "clickhouse" and would otherwise share a single syslog identifier.
    EXPECT_EQ(disambiguateClickHouseCommandName("clickhouse", "clickhouse", "clickhouse-server"), "clickhouse-server");
    EXPECT_EQ(disambiguateClickHouseCommandName("clickhouse", "clickhouse", "clickhouse-keeper"), "clickhouse-keeper");
}

TEST(BaseDaemonCommandName, DedicatedBinaryOrSymlinkKeepsItsOwnName)
{
    /// These already report an unambiguous name, so nothing is substituted.
    EXPECT_EQ(
        disambiguateClickHouseCommandName("clickhouse-server", "clickhouse-server", "clickhouse-server"),
        "clickhouse-server");
    EXPECT_EQ(
        disambiguateClickHouseCommandName("clickhouse-keeper", "clickhouse-keeper", "clickhouse-keeper"),
        "clickhouse-keeper");
}

TEST(BaseDaemonCommandName, RenamedBinaryKeepsTheNameItReportedBefore)
{
    /// Regression guard for the review finding on `Server.h`: substituting the subcommand name unconditionally
    /// would silently change the syslog identifier of deployments that run a renamed binary.
    EXPECT_EQ(disambiguateClickHouseCommandName("ch-prod", "ch-prod", "clickhouse-server"), "ch-prod");
    EXPECT_EQ(
        disambiguateClickHouseCommandName("clickhouse-server-v2", "clickhouse-server-v2", "clickhouse-server"),
        "clickhouse-server-v2");
    EXPECT_EQ(disambiguateClickHouseCommandName("ch-keeper", "ch-keeper", "clickhouse-keeper"), "ch-keeper");
}

TEST(BaseDaemonCommandName, DottedBinaryNameIsNotTreatedAsTheMultiPurposeBinary)
{
    /// Regression guard for the review finding on `BaseDaemon.cpp`. `Poco::Path::getBaseName` strips everything
    /// after the last dot, so `clickhouse.prod` arrives as `commandName() == "clickhouse"` despite not being the
    /// multi-purpose binary. Deciding on the base name would rewrite this deployment's identifier; deciding on the
    /// full file name keeps the `clickhouse` it reported before this change.
    EXPECT_EQ(disambiguateClickHouseCommandName("clickhouse.prod", "clickhouse", "clickhouse-server"), "clickhouse");
    EXPECT_EQ(disambiguateClickHouseCommandName("clickhouse.prod", "clickhouse", "clickhouse-keeper"), "clickhouse");
    EXPECT_EQ(disambiguateClickHouseCommandName("clickhouse.test.1", "clickhouse.test", "clickhouse-server"), "clickhouse.test");
}

TEST(BaseDaemonCommandName, SubstitutionIsExactMatchOnly)
{
    /// Only the exact multi-purpose binary name is ambiguous. A name that merely starts with "clickhouse" is a
    /// distinct binary and must be preserved.
    EXPECT_EQ(disambiguateClickHouseCommandName("clickhouse2", "clickhouse2", "clickhouse-server"), "clickhouse2");
    EXPECT_EQ(disambiguateClickHouseCommandName("Clickhouse", "Clickhouse", "clickhouse-server"), "Clickhouse");
    EXPECT_EQ(disambiguateClickHouseCommandName("clickhouse ", "clickhouse ", "clickhouse-server"), "clickhouse ");
}
