#pragma once

#include "config.h"

#if USE_DELTA_KERNEL_RS

#include <Common/CacheBase.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelPointerWrapper.h>
#include "delta_kernel_ffi.hpp"

namespace DeltaLake
{

struct CachedScanMetadata
{
    using KernelCachedScanMetadata = KernelPointerWrapper<::SharedCachedScanMetadata, ::free_cached_scan_metadata>;

    explicit CachedScanMetadata(::SharedCachedScanMetadata * handle_);

    KernelCachedScanMetadata handle;
    size_t memory_size = 0;
};

using CachedScanMetadataPtr = std::shared_ptr<const CachedScanMetadata>;

}

namespace DB
{

struct DeltaLakeMetadataCacheWeightFunction
{
    size_t operator()(const DeltaLake::CachedScanMetadata & scan_metadata) const;
};

class DeltaLakeMetadataCache
    : public CacheBase<String, DeltaLake::CachedScanMetadata, std::hash<String>, DeltaLakeMetadataCacheWeightFunction>
{
public:
    using Base = CacheBase<String, DeltaLake::CachedScanMetadata, std::hash<String>, DeltaLakeMetadataCacheWeightFunction>;

    DeltaLakeMetadataCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio);

    static String makeKey(const String & storage_description, const String & table_location, const String & table_id, UInt64 version);

    MappedPtr get(const Key & key);

private:
    void onEntryRemoval(size_t weight_loss, const MappedPtr &) override;
};

using DeltaLakeMetadataCachePtr = std::shared_ptr<DeltaLakeMetadataCache>;

}

#endif
