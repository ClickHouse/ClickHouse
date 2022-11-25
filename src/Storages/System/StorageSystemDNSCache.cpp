#include <Storages/System/StorageSystemDNSCache.h>
#include <Storages/MergeTree/ReplicatedFetchList.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Access/ContextAccess.h>
#include <Common/DNSResolver.h>

namespace DB
{

NamesAndTypesList StorageSystemDNSCache::getNamesAndTypes()
{
    return {
        {"hostname", std::make_shared<DataTypeString>()},
        {"ip_address", std::make_shared<DataTypeString>()},
        {"family", std::make_shared<DataTypeString>()}
    };
}

void StorageSystemDNSCache::fillData(MutableColumns & res_columns, ContextPtr, const SelectQueryInfo &) const
{
    using HostIPPair = std::pair<std::string, std::string>;
    auto &dns_resolver = DNSResolver::instance();
    auto cache_entries = dns_resolver.cacheEntries();
    std::set<HostIPPair> reported_elements;

    for (const auto & entry : dns_resolver.cacheEntries())
    {
        const std::string &hostname = entry.first;

        for (const auto & address : entry.second)
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
        }
    }
}

}
