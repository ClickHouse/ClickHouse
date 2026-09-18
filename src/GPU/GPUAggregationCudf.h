#pragma once

#include <stddef.h>
#include <stdint.h>

extern "C"
{

enum ClickHouseGPUElementType
{
    CLICKHOUSE_GPU_ELEMENT_UINT8 = 0,
    CLICKHOUSE_GPU_ELEMENT_UINT16 = 1,
    CLICKHOUSE_GPU_ELEMENT_UINT32 = 2,
    CLICKHOUSE_GPU_ELEMENT_UINT64 = 3,
    CLICKHOUSE_GPU_ELEMENT_INT8 = 4,
    CLICKHOUSE_GPU_ELEMENT_INT16 = 5,
    CLICKHOUSE_GPU_ELEMENT_INT32 = 6,
    CLICKHOUSE_GPU_ELEMENT_INT64 = 7,
    CLICKHOUSE_GPU_ELEMENT_FLOAT32 = 8,
    CLICKHOUSE_GPU_ELEMENT_FLOAT64 = 9,
};

enum ClickHouseGPUResultType
{
    CLICKHOUSE_GPU_RESULT_UINT64 = 0,
    CLICKHOUSE_GPU_RESULT_INT64 = 1,
    CLICKHOUSE_GPU_RESULT_FLOAT64 = 2,
};

enum ClickHouseGPUAggregation
{
    CLICKHOUSE_GPU_AGGREGATION_SUM = 0,
    CLICKHOUSE_GPU_AGGREGATION_MIN = 1,
    CLICKHOUSE_GPU_AGGREGATION_MAX = 2,
};

enum ClickHouseGPUCodec
{
    CLICKHOUSE_GPU_CODEC_LZ4 = 0,
    CLICKHOUSE_GPU_CODEC_ZSTD = 1,
};

int clickhouseGPUProbeDevice(char * error, size_t error_size);

int clickhouseGPUReduce(
    int element_type,
    int result_type,
    int aggregation,
    const void * host_data,
    size_t num_rows,
    void * result,
    char * error,
    size_t error_size);

int clickhouseGPUReduceCompressed(
    int codec,
    int element_type,
    int result_type,
    int aggregation,
    const void * host_data,
    const size_t * compressed_offsets,
    const size_t * compressed_bytes,
    const size_t * decompressed_bytes,
    size_t num_blocks,
    size_t num_rows,
    void * result,
    char * error,
    size_t error_size);

int clickhouseGPUGroupByCreate(
    const int * key_element_types,
    size_t num_keys,
    const int * value_element_types,
    const int * value_result_types,
    const int * value_aggregations,
    size_t num_values,
    void ** handle,
    char * error,
    size_t error_size);

int clickhouseGPUGroupByAddBatch(
    void * handle,
    const void * const * key_host_data,
    const void * const * value_host_data,
    size_t num_rows,
    char * error,
    size_t error_size);

int clickhouseGPUGroupByFinalize(void * handle, size_t * num_groups, char * error, size_t error_size);

int clickhouseGPUGroupByCopyOut(
    void * handle,
    void * const * key_host_data,
    void * const * value_host_data,
    char * error,
    size_t error_size);

void clickhouseGPUGroupByDestroy(void * handle);

int clickhouseGPUAllocPinned(size_t bytes, void ** host_ptr, char * error, size_t error_size);

void clickhouseGPUFreePinned(void * host_ptr);

}
