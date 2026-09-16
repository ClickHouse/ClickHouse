#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUAggregation.h>

#include <base/UUID.h>
#include <Common/CacheBase.h>
#include <Storages/IStorage_fwd.h>

#include <memory>

namespace DB
{

class IMergeTreeDataPart;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;

struct GPUColumnCacheKey
{
    UUID table_uuid;
    String part_name;
    String column_name;

    bool operator==(const GPUColumnCacheKey & other) const = default;
};

struct GPUColumnCacheKeyHash
{
    size_t operator()(const GPUColumnCacheKey & key) const;
};

struct GPUResidentColumn
{
    GPUResidentColumn(ConstStoragePtr storage_, DataPartPtr data_part_, size_t num_rows_, size_t element_size);

    ConstStoragePtr storage;

    DataPartPtr data_part;

    size_t num_rows;

    GPU::DeviceBuffer buffer;
};

struct GPUResidentColumnWeight
{
    size_t operator()(const GPUResidentColumn & column) const { return column.buffer.size(); }
};

class GPUColumnCache : public CacheBase<GPUColumnCacheKey, GPUResidentColumn, GPUColumnCacheKeyHash, GPUResidentColumnWeight>
{
private:
    using Base = CacheBase<GPUColumnCacheKey, GPUResidentColumn, GPUColumnCacheKeyHash, GPUResidentColumnWeight>;

public:
    explicit GPUColumnCache(size_t max_size_in_bytes);

    MappedPtr get(const Key & key) = delete;

    MappedPtr getForPart(const Key & key, const DataPartPtr & data_part);

    void setForPart(const Key & key, const MappedPtr & column);

private:
    void onEntryRemoval(size_t weight_loss, const MappedPtr & mapped_ptr) override;
};

using GPUColumnCachePtr = std::shared_ptr<GPUColumnCache>;

}

#endif
