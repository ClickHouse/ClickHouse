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

enum ClickHouseGPUSumType
{
    CLICKHOUSE_GPU_SUM_UINT64 = 0,
    CLICKHOUSE_GPU_SUM_INT64 = 1,
    CLICKHOUSE_GPU_SUM_FLOAT64 = 2,
};

int clickhouseGPUProbeDevice(char * error, size_t error_size);

int clickhouseGPUSum(
    int element_type,
    int sum_type,
    const void * host_data,
    size_t num_rows,
    void * result,
    char * error,
    size_t error_size);

int clickhouseGPUGroupBySumCreate(
    const int * key_element_types,
    size_t num_keys,
    const int * value_element_types,
    const int * value_sum_types,
    size_t num_values,
    void ** handle,
    char * error,
    size_t error_size);

int clickhouseGPUGroupBySumAddBatch(
    void * handle,
    const void * const * key_host_data,
    const void * const * value_host_data,
    size_t num_rows,
    char * error,
    size_t error_size);

int clickhouseGPUGroupBySumFinalize(void * handle, size_t * num_groups, char * error, size_t error_size);

int clickhouseGPUGroupBySumCopyOut(
    void * handle,
    void * const * key_host_data,
    void * const * value_host_data,
    char * error,
    size_t error_size);

void clickhouseGPUGroupBySumDestroy(void * handle);


int clickhouseGPUDeviceBufferAllocate(size_t bytes, void ** handle, char * error, size_t error_size);

int clickhouseGPUDeviceBufferCopyIn(
    void * handle,
    size_t offset,
    const void * host_data,
    size_t bytes,
    char * error,
    size_t error_size);

int clickhouseGPUDeviceBufferSum(
    void * handle,
    int element_type,
    int sum_type,
    size_t num_rows,
    void * result,
    char * error,
    size_t error_size);

    void clickhouseGPUDeviceBufferFree(void * handle);

}
