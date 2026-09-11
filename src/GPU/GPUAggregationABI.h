#pragma once

#include <stddef.h>

/// The boundary between ClickHouse and the cuDF island.
///
/// The two sides are compiled by different toolchains against different standard libraries - clang
/// with ClickHouse's own libc++ here, nvcc's gcc host pass with the system libstdc++ there (see
/// `gpu_island_target` in cmake/cuda.cmake for why they cannot be one) - so nothing with a C++ ABI
/// may cross: no `std::string`, no container, and above all no exception, whose type information
/// the other runtime does not recognize. Every function below reports failure by return code and
/// message instead, and `DB::GPU` turns that back into a ClickHouse exception.
///
/// The enumerators are part of the boundary. Append to them, never renumber.

extern "C"
{

/// The element type of a column of values to sum.
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

/// What `sum` of such a column returns, and so how to read the eight result bytes back.
enum ClickHouseGPUSumType
{
    CLICKHOUSE_GPU_SUM_UINT64 = 0,
    CLICKHOUSE_GPU_SUM_INT64 = 1,
    CLICKHOUSE_GPU_SUM_FLOAT64 = 2,
};

/// Whether this process can use a device: there is one, and its context initializes.
///
/// Returns 0 when it can, and otherwise a non-zero code with the reason in `error`. Takes the
/// hundred milliseconds the CUDA runtime needs to bring a context up, so the answer is worth
/// caching - `DB::GPU::deviceProbeError` does.
int clickhouseGPUProbeDevice(char * error, size_t error_size);

/// Sums `num_rows` values of `element_type` read from `host_data`, which holds them contiguously
/// and without nulls, and writes the sum to the eight bytes at `result` as `sum_type` says.
///
/// Integers are summed with wraparound, as `sum` has on the CPU. Floats are not summed in the
/// order they are laid out in - the device reduces them as a tree - so the last bits of a
/// `Float64` sum can differ from the CPU's. The order is fixed for a given batch, so two runs of
/// the same query agree with each other.
///
/// Returns 0 on success, and otherwise a non-zero code with a message in `error`.
int clickhouseGPUSum(
    int element_type,
    int sum_type,
    const void * host_data,
    size_t num_rows,
    void * result,
    char * error,
    size_t error_size);

/// The keyed aggregation - `GROUP BY k` - which unlike the keyless one above cannot be a single
/// stateless call.
///
/// A keyless partial result is one scalar per aggregate, small enough to come back over the
/// boundary after every batch and be added up on the host. A keyed partial result is a whole table
/// of groups, and moving it to the host after each batch and merging it there would spend more time
/// on the link and in host hash tables than the device spends grouping - and would be the CPU
/// aggregation this path exists to avoid. So the partial result stays on the device between
/// batches, and the boundary carries an opaque handle to it. Only plain C types cross, as
/// everywhere else here: the handle is a `void *` whose type the ClickHouse side never learns.
///
/// The order is: one `clickhouseGPUGroupBySumCreate`, any number of
/// `clickhouseGPUGroupBySumAddBatch`, one `clickhouseGPUGroupBySumFinalize`, one
/// `clickhouseGPUGroupBySumCopyOut`, one `clickhouseGPUGroupBySumDestroy`. Every function returns 0
/// on success and otherwise a non-zero code with a message in `error`.

/// Sets up a keyed `sum` over `num_keys` key columns of `key_element_types` and `num_values` value
/// columns of `value_element_types`, whose sums ClickHouse wants as `value_sum_types`, and writes
/// the handle to `*handle`. All three arrays are `ClickHouseGPUElementType` and
/// `ClickHouseGPUSumType` values.
int clickhouseGPUGroupBySumCreate(
    const int * key_element_types,
    size_t num_keys,
    const int * value_element_types,
    const int * value_sum_types,
    size_t num_values,
    void ** handle,
    char * error,
    size_t error_size);

/// Adds `num_rows` rows to the partial result: `key_host_data[i]` holds the `i`-th key column's
/// values contiguously and without nulls, `value_host_data[j]` the `j`-th value column's.
///
/// The batch is grouped on its own and the result merged into what the handle already holds, so
/// this call's cost is proportional to the batch rather than to everything added before it.
int clickhouseGPUGroupBySumAddBatch(
    void * handle,
    const void * const * key_host_data,
    const void * const * value_host_data,
    size_t num_rows,
    char * error,
    size_t error_size);

/// Says that no more batches are coming and writes the number of groups to `*num_groups`, which is
/// how much room `clickhouseGPUGroupBySumCopyOut` needs. Nothing leaves the device yet.
int clickhouseGPUGroupBySumFinalize(void * handle, size_t * num_groups, char * error, size_t error_size);

/// Copies the groups out, one column at a time: `key_host_data[i]` and `value_host_data[j]` each
/// have to have room for the number of elements `clickhouseGPUGroupBySumFinalize` reported, of the
/// width the corresponding element type or sum type implies.
///
/// The group order is whatever the device produced. `GROUP BY` does not promise an order on either
/// path, and the callers that need one sort afterwards.
int clickhouseGPUGroupBySumCopyOut(
    void * handle,
    void * const * key_host_data,
    void * const * value_host_data,
    char * error,
    size_t error_size);

/// Releases the partial result and the handle. Does nothing on a null handle, and cannot fail:
/// it is called from a destructor, where there is nobody left to report a failure to.
void clickhouseGPUGroupBySumDestroy(void * handle);

}
