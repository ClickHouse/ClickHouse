#include <Access/ContextAccess.h>
#include <Common/SystemTableDocumentation.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Interpreters/Context.h>
#include <Common/DNSResolver.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/StorageSystemDNSCache.h>

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

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemDNSCache) }

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "dns_cache",
    .description = R"DOCS_MD(
Contains information about cached DNS records.
)DOCS_MD",
    .examples = R"DOCS_MD(
```sql title="Query"
SELECT * FROM system.dns_cache;
```

| hostname | ip\_address | ip\_family | cached\_at |
| :--- | :--- | :--- | :--- |
| localhost | ::1 | IPv6 | 2024-02-11 17:04:40 |
| localhost | 127.0.0.1 | IPv4 | 2024-02-11 17:04:40 |
)DOCS_MD",
    .see_also = R"DOCS_MD(
- [disable_internal_dns_cache setting](/reference/settings/server-settings/settings/disable#disable_internal_dns_cache)
- [dns_cache_max_entries setting](/reference/settings/server-settings/settings/dns-cache#dns_cache_max_entries)
- [dns_cache_update_period setting](/reference/settings/server-settings/settings/dns-cache#dns_cache_update_period)
- [dns_max_consecutive_failures setting](/reference/settings/server-settings/settings/other#dns_max_consecutive_failures)
)DOCS_MD")

}
