#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Interpreters/Cluster.h>

#include <Poco/AutoPtr.h>
#include <Poco/DOM/DOMParser.h>
#include <Poco/Util/XMLConfiguration.h>

#include <fmt/format.h>


namespace DB::ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
}

namespace
{
    DB::Cluster::Address addressWithHost(const std::string & host)
    {
        Poco::XML::DOMParser dom_parser;
        Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(fmt::format(
            "<clickhouse><remote_servers><c><shard><replica><host>{}</host><port>9000</port></replica></shard></c></remote_servers></clickhouse>",
            host));
        Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(document);
        return DB::Cluster::Address(*config, "remote_servers.c.shard.replica", "c", "", 1, 1, /* treat_local_port_as_remote = */ true);
    }
}

/// A replica host that cannot be a host fails when the cluster is loaded, so a server does not start with it (issue #46918).
TEST(ClusterAddress, RejectsInvalidHost)
{
    for (const std::string host : {" 127.0.0.1", "127.0.0.1 ", " localhost", "", "127.0.0.256"})
    {
        try
        {
            addressWithHost(host);
            ADD_FAILURE() << "host [" << host << "] was accepted";
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), DB::ErrorCodes::INVALID_CONFIG_PARAMETER) << e.message();
            EXPECT_NE(e.message().find("remote_servers.c.shard.replica.host"), std::string::npos) << e.message();
        }
    }
}

/// Unlike a ZooKeeper host, a replica host has its port apart, so a bare IPv6 address works.
TEST(ClusterAddress, AcceptsValidHosts)
{
    for (const std::string host : {"127.0.0.1", "localhost", "keeper_1", "::1", "[::1]"})
        EXPECT_EQ(addressWithHost(host).host_name, host);
}
