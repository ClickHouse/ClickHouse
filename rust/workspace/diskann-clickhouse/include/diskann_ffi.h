#pragma once
#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    DISKANN_METRIC_L2 = 0,
    DISKANN_METRIC_COSINE = 1,
} DiskANNMetric;

int64_t diskann_create_index(
    uint32_t dim, DiskANNMetric metric,
    uint32_t pruned_degree, uint32_t max_degree,
    uint32_t l_build, float alpha);

void diskann_drop_index(int64_t handle);

int64_t diskann_insert_batch(
    int64_t handle, const float * vectors_ptr,
    uint64_t count, uint32_t dim);

int64_t diskann_search(
    int64_t handle, const float * query_ptr,
    uint32_t dim, uint64_t k,
    uint64_t * results_ptr, float * distances_ptr);

int64_t diskann_serialize(
    int64_t handle, uint8_t ** out_ptr, uint64_t * out_size);

int64_t diskann_deserialize(
    const uint8_t * data_ptr, uint64_t data_size);

void diskann_free_buffer(uint8_t * ptr);

int64_t diskann_index_size(int64_t handle);

int64_t diskann_last_error(char * buf, uint64_t buf_size);

#ifdef __cplusplus
}
#endif
