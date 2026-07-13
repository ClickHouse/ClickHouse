#pragma once

#include <Disks/DiskType.h>

#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>

namespace DB
{

struct PlainRewritableMetrics
{
    const ProfileEvents::Event directory_created = ProfileEvents::end();
    const ProfileEvents::Event directory_removed = ProfileEvents::end();

    const CurrentMetrics::Metric directory_map_size = CurrentMetrics::end();
    const CurrentMetrics::Metric file_count = CurrentMetrics::end();
};

std::shared_ptr<PlainRewritableMetrics> createPlainRewritableMetrics(ObjectStorageType object_storage_type);

}
