#pragma once

#include <Common/CurrentMetrics.h>
#include <Storages/IStorage_fwd.h>


namespace DB
{
    std::vector<CurrentMetrics::Metric> getAttachedCountersForStorage(const StoragePtr & storage);
}
