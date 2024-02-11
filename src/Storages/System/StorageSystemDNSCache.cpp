#include <Access/ContextAccess.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Interpreters/Context.h>
#include <Storages/System/StorageSystemDNSCache.h>
#include <Common/DNSResolver.h>
#include "StorageSystemDatabases.h"

namespace DB
{

ColumnsDescription StorageSystemDNSCache::getColumnsDescription()
{
    return ColumnsDescription
        {
            {"hostname", std::make_shared<DataTypeString>(), "Hostname."},
            {"ip_address", std::make_shared<DataTypeString>(), "IP address."},
            {"ip_family", std::make_shared<DataTypeString>(), "IP address family."},
            {"cached_at", std::make_shared<DataTypeDateTime>(), "Record cached timestamp."},
        };
}

void StorageSystemDNSCache::fillData(MutableColumns & res_columns, ContextPtr, const SelectQueryInfo &) const
{
    using HostIPPair = std::pair<std::string, std::string>;
    std::set<HostIPPair> reported_elements;

    for (const auto & [hostname, entry] : DNSResolver::instance().cacheEntries())
    {
        for (const auto &address : entry.addresses)
        {
            std::string ip = address.toString();

            // Cache might report the same ip address multiple times. Report only one of them.
            if (reported_elements.contains(HostIPPair(hostname, ip)))
                continue;

            reported_elements.insert(HostIPPair(hostname, ip));

            std::string family_str;
            switch (address.family())
            {
                case Poco::Net::AddressFamily::IPv4:
                    family_str = "IPv4";
                break;
                case Poco::Net::AddressFamily::IPv6:
                    family_str = "IPv6";
                break;
                case Poco::Net::AddressFamily::UNIX_LOCAL:
                    family_str = "UNIX_LOCAL";
                break;
            }

            size_t i = 0;
            res_columns[i++]->insert(hostname);
            res_columns[i++]->insert(ip);
            res_columns[i++]->insert(family_str);
            res_columns[i++]->insert(static_cast<UInt32>(std::chrono::system_clock::to_time_t(entry.cached_at)));
        }
    }
}

}
