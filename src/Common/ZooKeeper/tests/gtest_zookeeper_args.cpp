#include <gtest/gtest.h>

#include <base/getFQDNOrHostName.h>
#include <Common/ZooKeeper/ZooKeeperArgs.h>
#include <Common/isLocalAddress.h>

#include <Poco/AutoPtr.h>
#include <Poco/DOM/DOMParser.h>
#include <Poco/Util/XMLConfiguration.h>

#include <fmt/format.h>


namespace
{
    Poco::AutoPtr<Poco::Util::XMLConfiguration> configFromXML(const std::string & xml)
    {
        Poco::XML::DOMParser dom_parser;
        Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
        return new Poco::Util::XMLConfiguration(document);
    }
}

/// The host name similarity vectors used by the hostname-based load balancing strategies must be
/// computed on the bare host name, with the optional `secure://` scheme and the `:port` suffix
/// stripped. If the scheme leaked into the comparison, `hostname_longest_common_prefix` would
/// score every candidate as 0 for TLS configurations (no local host name starts with `secure://`)
/// and selection would silently degenerate to random.
///
/// To keep the test independent of the machine it runs on, one configured node reuses the local
/// host name itself (obtained from `getFQDNOrHostName`, the same source `ZooKeeperArgs` uses):
/// its expected common prefix/suffix length is the full host name length, and its expected
/// distances are 0 — none of which can hold if `secure://` contaminates the comparison.
TEST(ZooKeeperArgs, SecureSchemeStrippedFromHostNameSimilarity)
{
    const std::string & local_hostname = getFQDNOrHostName();
    ASSERT_FALSE(local_hostname.empty());

    auto config = configFromXML(fmt::format(
        R"(<clickhouse>
    <zookeeper>
        <node>
            <host>{}</host>
            <port>2181</port>
            <secure>1</secure>
        </node>
        <node>
            <host>zoo2</host>
            <port>2181</port>
            <secure>1</secure>
        </node>
        <node>
            <host>zoo3</host>
            <port>2181</port>
        </node>
    </zookeeper>
</clickhouse>)",
        local_hostname));

    zkutil::ZooKeeperArgs args(*config, "zookeeper");

    /// Sanity check: the secure flag really produces `secure://host:port` entries.
    ASSERT_EQ(args.hosts.size(), 3u);
    ASSERT_EQ(args.hosts[0], "secure://" + local_hostname + ":2181");
    ASSERT_EQ(args.hosts[1], "secure://zoo2:2181");
    ASSERT_EQ(args.hosts[2], "zoo3:2181");

    /// The node that matches the local host name exactly: perfect similarity, zero distance.
    /// With an unstripped scheme the prefix/suffix lengths could not reach the full host name
    /// length and the distances could not be 0.
    EXPECT_EQ(args.get_priority_load_balancing.hostname_longest_common_prefix[0], local_hostname.size());
    EXPECT_EQ(args.get_priority_load_balancing.hostname_longest_common_suffix[0], local_hostname.size());
    EXPECT_EQ(args.get_priority_load_balancing.hostname_prefix_distance[0], 0u);
    EXPECT_EQ(args.get_priority_load_balancing.hostname_levenshtein_distance[0], 0u);

    /// Secure and plain nodes must be scored identically to a direct computation on the bare host name.
    const std::vector<std::string> bare_hosts{local_hostname, "zoo2", "zoo3"};
    for (size_t i = 0; i < bare_hosts.size(); ++i)
    {
        EXPECT_EQ(
            args.get_priority_load_balancing.hostname_longest_common_prefix[i],
            DB::getHostNameLongestCommonPrefix(local_hostname, bare_hosts[i]))
            << "host index " << i;
        EXPECT_EQ(
            args.get_priority_load_balancing.hostname_longest_common_suffix[i],
            DB::getHostNameLongestCommonSuffix(local_hostname, bare_hosts[i]))
            << "host index " << i;
        EXPECT_EQ(
            args.get_priority_load_balancing.hostname_prefix_distance[i],
            DB::getHostNamePrefixDistance(local_hostname, bare_hosts[i]))
            << "host index " << i;
        EXPECT_EQ(
            args.get_priority_load_balancing.hostname_levenshtein_distance[i],
            DB::getHostNameLevenshteinDistance(local_hostname, bare_hosts[i]))
            << "host index " << i;
    }
}

/// Same invariant for the `keeper_server` section, where `tcp_port_secure` makes every
/// raft host a `secure://host:port` entry.
TEST(ZooKeeperArgs, SecureSchemeStrippedForKeeperServerSection)
{
    const std::string & local_hostname = getFQDNOrHostName();
    ASSERT_FALSE(local_hostname.empty());

    auto config = configFromXML(fmt::format(
        R"(<clickhouse>
    <keeper_server>
        <tcp_port_secure>9281</tcp_port_secure>
        <raft_configuration>
            <server>
                <id>1</id>
                <hostname>{}</hostname>
            </server>
            <server>
                <id>2</id>
                <hostname>keeper2</hostname>
            </server>
        </raft_configuration>
    </keeper_server>
</clickhouse>)",
        local_hostname));

    zkutil::ZooKeeperArgs args(*config, "keeper_server");

    ASSERT_EQ(args.hosts.size(), 2u);
    ASSERT_EQ(args.hosts[0], "secure://" + local_hostname + ":9281");
    ASSERT_EQ(args.hosts[1], "secure://keeper2:9281");

    EXPECT_EQ(args.get_priority_load_balancing.hostname_longest_common_prefix[0], local_hostname.size());
    EXPECT_EQ(args.get_priority_load_balancing.hostname_longest_common_suffix[0], local_hostname.size());
    EXPECT_EQ(args.get_priority_load_balancing.hostname_prefix_distance[0], 0u);
    EXPECT_EQ(args.get_priority_load_balancing.hostname_levenshtein_distance[0], 0u);

    EXPECT_EQ(
        args.get_priority_load_balancing.hostname_longest_common_prefix[1],
        DB::getHostNameLongestCommonPrefix(local_hostname, "keeper2"));
    EXPECT_EQ(
        args.get_priority_load_balancing.hostname_longest_common_suffix[1],
        DB::getHostNameLongestCommonSuffix(local_hostname, "keeper2"));
}
