#pragma once
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    DISKANN_METRIC_L2 = 0,
    DISKANN_METRIC_COSINE = 1,
} DiskANNMetric;

int64_t diskann_create_disk_builder(
    uint32_t dim,
    DiskANNMetric metric,
    uint32_t pruned_degree,
    uint32_t max_degree,
    uint32_t l_build,
    float alpha,
    uint32_t num_threads,
    uint32_t pq_chunks,
    double build_ram_limit_gb);

void diskann_drop_builder(int64_t handle);

int64_t diskann_builder_set_data_path(
    int64_t handle,
    const char * data_path);

int64_t diskann_builder_set_index_prefix(
    int64_t handle,
    const char * index_prefix);

int64_t diskann_builder_build(int64_t handle);

int64_t diskann_open_searcher(
    const char * index_prefix,
    uint32_t dim,
    DiskANNMetric metric,
    uint32_t num_threads,
    uint32_t search_io_limit,
    uint32_t num_nodes_to_cache);

void diskann_close_searcher(int64_t handle);

int64_t diskann_searcher_num_points(int64_t handle);

int64_t diskann_searcher_dimensions(int64_t handle);

int64_t diskann_searcher_memory_usage(int64_t handle);

int64_t diskann_search_disk_index(
    int64_t handle,
    const float * query_ptr,
    uint32_t dim,
    uint32_t k,
    uint32_t search_list_size,
    uint32_t beam_width,
    uint64_t * results_ptr,
    float * distances_ptr);

int64_t diskann_compute_distances(
    DiskANNMetric metric,
    uint32_t dim,
    const float * query_ptr,
    const float * candidates_ptr,
    uint64_t n,
    float * out_ptr);

int64_t diskann_index_file_exists(const char * index_prefix);

int64_t diskann_last_error(char * buf, uint64_t buf_size);

#ifdef __cplusplus
}
#endif
