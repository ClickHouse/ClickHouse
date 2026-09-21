#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>

namespace DB::GPU
{

struct GPUBuffer;
struct GPUMarker;
struct GPUGroupBy;
struct GPUHashTableState;

struct GPUMarkerDeleter
{
    void operator()(GPUMarker * marker) const noexcept;
};

struct GPUBufferDeleter
{
    void operator()(GPUBuffer * buffer) const noexcept;
};

struct GPUGroupByDeleter
{
    void operator()(GPUGroupBy * group_by) const noexcept;
};

struct GPUHashTableDeleter
{
    void operator()(GPUHashTableState * hash_table) const noexcept;
};

using GPUMarkerPtr = std::unique_ptr<GPUMarker, GPUMarkerDeleter>;
using GPUBufferPtr = std::unique_ptr<GPUBuffer, GPUBufferDeleter>;
using GPUGroupByPtr = std::unique_ptr<GPUGroupBy, GPUGroupByDeleter>;
using GPUHashTablePtr = std::unique_ptr<GPUHashTableState, GPUHashTableDeleter>;

enum class GPUElementType : int
{
    UInt8 = 0,
    UInt16 = 1,
    UInt32 = 2,
    UInt64 = 3,
    Int8 = 4,
    Int16 = 5,
    Int32 = 6,
    Int64 = 7,
    Float32 = 8,
    Float64 = 9,
};

enum class GPUResultType : int
{
    UInt64 = 0,
    Int64 = 1,
    Float64 = 2,
};

enum class GPUAggregationKind : int
{
    Sum = 0,
    Min = 1,
    Max = 2,
};

enum class GPUCodec : int
{
    LZ4 = 0,
    ZSTD = 1,
};

constexpr bool isInteger(GPUElementType type)
{
    return type != GPUElementType::Float32 && type != GPUElementType::Float64;
}

constexpr size_t sizeOf(GPUElementType type)
{
    switch (type)
    {
        case GPUElementType::UInt8:
        case GPUElementType::Int8:
            return 1;
        case GPUElementType::UInt16:
        case GPUElementType::Int16:
            return 2;
        case GPUElementType::UInt32:
        case GPUElementType::Int32:
        case GPUElementType::Float32:
            return 4;
        case GPUElementType::UInt64:
        case GPUElementType::Int64:
        case GPUElementType::Float64:
            return 8;
    }
    return 0;
}

int reduceOnGPU(
    GPUElementType element_type,
    GPUResultType result_type,
    GPUAggregationKind aggregation,
    const GPUBuffer * values,
    size_t num_rows,
    void * result,
    char * error,
    size_t error_size);

int createGPUGroupBy(
    const GPUElementType * key_element_types,
    size_t num_keys,
    const GPUElementType * value_element_types,
    const GPUResultType * value_result_types,
    const GPUAggregationKind * value_aggregations,
    size_t num_values,
    GPUGroupBy ** handle,
    char * error,
    size_t error_size);

int addBatchToGPUGroupBy(
    GPUGroupBy * handle,
    const void * const * key_host_data,
    const void * const * value_host_data,
    size_t num_rows,
    char * error,
    size_t error_size);

int finalizeGPUGroupBy(GPUGroupBy * handle, size_t * num_groups, char * error, size_t error_size);

int copyGPUGroupsOut(
    GPUGroupBy * handle,
    void * const * key_host_data,
    void * const * value_host_data,
    char * error,
    size_t error_size);

void destroyGPUGroupBy(GPUGroupBy * handle);

int probeGPUDevice(char * error, size_t error_size);

int allocatePinnedHostMemory(size_t bytes, void ** host_ptr, char * error, size_t error_size);

void freePinnedHostMemory(void * host_ptr);

int createGPUBuffer(GPUElementType element_type, GPUBuffer ** handle, char * error, size_t error_size);

void destroyGPUBuffer(GPUBuffer * handle);

int appendToGPUBuffer(GPUBuffer * handle, const void * host_data, size_t bytes, char * error, size_t error_size);

int appendCompressedToGPUBuffer(
    GPUBuffer * handle,
    GPUCodec codec,
    const void * host_data,
    const size_t * compressed_offsets,
    const size_t * compressed_bytes,
    const size_t * decompressed_bytes,
    size_t num_blocks,
    char * error,
    size_t error_size);

int syncGPUBuffer(GPUBuffer * handle, char * error, size_t error_size);

int gpuBufferRows(GPUBuffer * handle, size_t * num_rows, char * error, size_t error_size);

void clearGPUBuffer(GPUBuffer * handle);

GPUMarker * createGPUMarker(char * error, size_t error_size);

void destroyGPUMarker(GPUMarker * marker);

int recordGPUMarker(GPUMarker * marker, char * error, size_t error_size);

int waitGPUMarker(GPUMarker * marker, char * error, size_t error_size);

int createGPUHashTable(
    GPUElementType key_element_type,
    const GPUElementType * payload_element_types,
    size_t num_payloads,
    GPUHashTableState ** handle,
    char * error,
    size_t error_size);

int buildGPUHashTable(GPUHashTableState * handle, char * error, size_t error_size);

int copyGPUMatchesOut(
    GPUHashTableState * handle,
    uint32_t * probe_row_indices,
    void * const * payload_host_data,
    char * error,
    size_t error_size);

void destroyGPUHashTable(GPUHashTableState * handle);

int setGPUHashTableBuildSide(
    GPUHashTableState * handle,
    const GPUBuffer * key_buffer,
    const GPUBuffer * const * payload_buffers,
    size_t num_payloads,
    size_t num_rows,
    char * error,
    size_t error_size);

int probeGPUHashTable(
    GPUHashTableState * handle, const GPUBuffer * key_buffer, size_t num_rows, size_t * num_matches, char * error, size_t error_size);

}
