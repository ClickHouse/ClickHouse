#pragma once

#include "config.h"

#if USE_DELTA_KERNEL_RS

#include <Common/CacheBase.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/DataFileStatistics.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelPointerWrapper.h>
#include "delta_kernel_ffi.hpp"

#include <optional>
#include <vector>

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
    std::optional<DataFileStatistics> statistics;
};

using ScannedDataFilePtr = std::shared_ptr<const ScannedDataFile>;

struct SnapshotDataFiles
{
    std::vector<ScannedDataFilePtr> files;
    bool has_deletion_vectors = false;

    [[nodiscard]] static size_t getFileWeight(const ScannedDataFile & file);
};

}

namespace DB
{

struct DeltaLakeMetadataCacheWeightFunction
{
    size_t operator()(const DeltaLake::SnapshotDataFiles & data_files) const;
};

class DeltaLakeMetadataCache
    : public CacheBase<String, DeltaLake::SnapshotDataFiles, std::hash<String>, DeltaLakeMetadataCacheWeightFunction>
{
public:
    using Base = CacheBase<String, DeltaLake::SnapshotDataFiles, std::hash<String>, DeltaLakeMetadataCacheWeightFunction>;

    DeltaLakeMetadataCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio);

    static String makeKey(const String & storage_description, const String & table_location, const String & table_id, UInt64 version);

    MappedPtr get(const Key & key);

private:
    void onEntryRemoval(size_t weight_loss, const MappedPtr &) override;
};

using DeltaLakeMetadataCachePtr = std::shared_ptr<DeltaLakeMetadataCache>;

}

#endif
