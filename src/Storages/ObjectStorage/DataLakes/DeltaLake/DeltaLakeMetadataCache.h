#pragma once

#include "config.h"

#if USE_DELTA_KERNEL_RS

#include <Common/CacheBase.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelPointerWrapper.h>
#include "delta_kernel_ffi.hpp"

#include <fmt/format.h>
#include <optional>
#include <vector>

namespace ProfileEvents
{
    extern const Event DeltaLakeMetadataCacheHits;
    extern const Event DeltaLakeMetadataCacheMisses;
    extern const Event DeltaLakeMetadataCacheWeightLost;
}

namespace CurrentMetrics
{
    extern const Metric DeltaLakeMetadataCacheBytes;
    extern const Metric DeltaLakeMetadataCacheEntries;
}

namespace DeltaLake
{

struct ScannedDataFile
{
    using KernelDvInfo = KernelPointerWrapper<ffi::SharedDvInfo, ffi::free_kernel_dv_info>;
    using KernelExpression = KernelPointerWrapper<ffi::SharedExpression, ffi::free_kernel_expression>;

    std::string path;
    int64_t size = 0;
    std::optional<UInt64> num_records;
    KernelDvInfo dv_info;
    std::optional<KernelExpression> transform;
};

using ScannedDataFilePtr = std::shared_ptr<const ScannedDataFile>;

struct SnapshotDataFiles
{
    std::vector<ScannedDataFilePtr> files;
};

}

namespace DB
{

struct DeltaLakeMetadataCacheWeightFunction
{
    size_t operator()(const DeltaLake::SnapshotDataFiles & data_files) const
    {
        size_t weight = sizeof(DeltaLake::SnapshotDataFiles) + data_files.files.capacity() * sizeof(DeltaLake::ScannedDataFilePtr);
        for (const auto & file : data_files.files)
            weight += sizeof(DeltaLake::ScannedDataFile) + file->path.capacity();
        return weight;
    }
};

class DeltaLakeMetadataCache
    : public CacheBase<String, DeltaLake::SnapshotDataFiles, std::hash<String>, DeltaLakeMetadataCacheWeightFunction>
{
public:
    using Base = CacheBase<String, DeltaLake::SnapshotDataFiles, std::hash<String>, DeltaLakeMetadataCacheWeightFunction>;

    DeltaLakeMetadataCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio)
        : Base(cache_policy, CurrentMetrics::DeltaLakeMetadataCacheBytes, CurrentMetrics::DeltaLakeMetadataCacheEntries, max_size_in_bytes, max_count, size_ratio)
    {
    }

    static String makeKey(const String & storage_description, const String & table_location, const String & table_id, UInt64 version)
    {
        return fmt::format("{}\n{}\n{}\n{}", storage_description, table_location, table_id, version);
    }

    MappedPtr get(const Key & key)
    {
        auto result = Base::get(key);
        ProfileEvents::increment(result ? ProfileEvents::DeltaLakeMetadataCacheHits : ProfileEvents::DeltaLakeMetadataCacheMisses);
        return result;
    }

private:
    void onEntryRemoval(const size_t weight_loss, const MappedPtr &) override
    {
        ProfileEvents::increment(ProfileEvents::DeltaLakeMetadataCacheWeightLost, weight_loss);
    }
};

using DeltaLakeMetadataCachePtr = std::shared_ptr<DeltaLakeMetadataCache>;

}

#endif
