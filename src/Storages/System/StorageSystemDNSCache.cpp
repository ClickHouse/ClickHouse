#include <Access/ContextAccess.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/ReplicatedFetchList.h>
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
        {"family", std::make_shared<DataTypeString>(), "IP address's family"},
        {"cached_at", std::make_shared<DataTypeDateTime>(), "Record's cached timestamp."},
        {"expires_at", std::make_shared<DataTypeDateTimea>(), "Record's expires timestamp."}
    };
}

void StorageSystemDNSCache::fillData(MutableColumns & res_columns, ContextPtr, const SelectQueryInfo &) const
{
    // TODO:: implement
}

}
