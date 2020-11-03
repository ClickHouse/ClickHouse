#include <Interpreters/DDLTask.h>
#include <Common/DNSResolver.h>
#include <Common/isLocalAddress.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <Poco/Net/NetException.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_FORMAT_VERSION;
}

HostID HostID::fromString(const String & host_port_str)
{
    HostID res;
    std::tie(res.host_name, res.port) = Cluster::Address::fromString(host_port_str);
    return res;
}

bool HostID::isLocalAddress(UInt16 clickhouse_port) const
{
    try
    {
        return DB::isLocalAddress(DNSResolver::instance().resolveAddress(host_name, port), clickhouse_port);
    }
    catch (const Poco::Net::NetException &)
    {
        /// Avoid "Host not found" exceptions
        return false;
    }
}


String DDLLogEntry::toString() const
{
    WriteBufferFromOwnString wb;

    Strings host_id_strings(hosts.size());
    std::transform(hosts.begin(), hosts.end(), host_id_strings.begin(), HostID::applyToString);

    auto version = CURRENT_VERSION;
    wb << "version: " << version << "\n";
    wb << "query: " << escape << query << "\n";
    wb << "hosts: " << host_id_strings << "\n";
    wb << "initiator: " << initiator << "\n";

    return wb.str();
}

void DDLLogEntry::parse(const String & data)
{
    ReadBufferFromString rb(data);

    int version;
    rb >> "version: " >> version >> "\n";

    if (version != CURRENT_VERSION)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unknown DDLLogEntry format version: {}", version);

    Strings host_id_strings;
    rb >> "query: " >> escape >> query >> "\n";
    rb >> "hosts: " >> host_id_strings >> "\n";

    if (!rb.eof())
        rb >> "initiator: " >> initiator >> "\n";
    else
        initiator.clear();

    assertEOF(rb);

    hosts.resize(host_id_strings.size());
    std::transform(host_id_strings.begin(), host_id_strings.end(), hosts.begin(), HostID::fromString);
}


}
