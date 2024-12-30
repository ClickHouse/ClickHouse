#pragma once

#include <Disks/DiskType.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>

namespace DB
{

struct MetadataStorageMetrics
{
    const ProfileEvents::Event directory_created = ProfileEvents::end();
    const ProfileEvents::Event directory_removed = ProfileEvents::end();

    CurrentMetrics::Metric directory_map_size = CurrentMetrics::end();
    CurrentMetrics::Metric unique_filenames_count = CurrentMetrics::end();
    CurrentMetrics::Metric file_count = CurrentMetrics::end();

    template <typename ObjectStorage, MetadataStorageType metadata_type>
    static MetadataStorageMetrics create()
    {
        return MetadataStorageMetrics{};
    }
};

}
