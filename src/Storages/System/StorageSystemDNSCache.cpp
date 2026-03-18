#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Common/DNSResolver.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include "StorageSystemDNSCache.h"

namespace DB
{

static DataTypePtr getIPFamilyEnumType()
{
    return std::make_shared<DataTypeEnum8>(
    DataTypeEnum8::Values
        {
            {"IPv4",           static_cast<Int8>(Poco::Net::AddressFamily::IPv4)},
            {"IPv6",           static_cast<Int8>(Poco::Net::AddressFamily::IPv6)},
            {"UNIX_LOCAL",     static_cast<Int8>(Poco::Net::AddressFamily::UNIX_LOCAL)},
        });
}

ColumnsDescription StorageSystemDNSCache::getColumnsDescription()
{
    return ColumnsDescription
        {
            {"hostname",           std::make_shared<DataTypeString>(), "Hostname."},
            {"ip_address",         std::make_shared<DataTypeString>(), "IP address."},
            {"ip_family",          getIPFamilyEnumType(), "IP address family."},
            {"cached_at",          std::make_shared<DataTypeDateTime>(), "Record cached timestamp."},
        };
}

void StorageSystemDNSCache::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    using HostIPPair = std::pair<std::string, std::string>;
    std::unordered_set<HostIPPair, boost::hash<std::pair<std::string, std::string>>> reported_elements;

    for (const auto & [hostname, entry] : DNSResolver::instance().cacheEntries())
    {
        for (const auto &address : entry.addresses)
        {
            std::string ip = address.toString();

            // Cache might report the same ip address multiple times. Report only one of them.
            if (reported_elements.contains(HostIPPair(hostname, ip)))
                continue;

            reported_elements.insert(HostIPPair(hostname, ip));

            size_t i = 0;
            res_columns[i++]->insert(hostname);
            res_columns[i++]->insert(ip);
            res_columns[i++]->insert(address.family());
            res_columns[i++]->insert(static_cast<UInt32>(std::chrono::system_clock::to_time_t(entry.cached_at)));
        }
    }
}

}
