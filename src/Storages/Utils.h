#pragma once

#include <Common/CurrentMetrics.h>
#include <Storages/IStorage_fwd.h>


namespace DB
{
    CurrentMetrics::Metric getAttachedCounterForStorage(const StoragePtr & storage);
}
