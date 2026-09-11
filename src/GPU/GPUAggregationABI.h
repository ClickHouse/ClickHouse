#pragma once

#include <stddef.h>
#include <stdint.h>

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


/// The hash join - `INNER JOIN ... ALL` on a single fixed-width integer key column.
///
/// Like the keyed aggregation above and unlike the keyless sum, this cannot be a single stateless
/// call: the hash table is built once over the whole right table and probed once per left block, so
/// it has to live on the device between calls and the boundary carries an opaque handle to it.
///
/// A buffer of device memory that outlives the call which filled it - what one column of one
/// `MergeTree` part is held in while the GPU column cache keeps it.
///
/// This is the piece that makes the device worth using. Every other operator here takes host
/// memory, so every query pays the link: on this machine a reduction over 1.49 GiB of `UInt64`
/// takes 6 ms on the device and 334 ms to get there. A column that is already on the device costs
/// only the 6 ms, so the second query over it runs seventy times faster than the whole query does
/// on sixteen cores - and a column gets there once rather than once per query.
///
/// The order is: one `clickhouseGPUDeviceBufferAllocate`, any number of
/// `clickhouseGPUDeviceBufferCopyIn` and `clickhouseGPUDeviceBufferSum` in any order, one
/// `clickhouseGPUDeviceBufferFree`. Unlike the two stateful operators above there is no phase
/// order between filling and reducing: the caller fills a buffer once when it reads a part and
/// reduces it on every query that reaches that part afterwards.
///
/// Nothing here batches, stages or grows. The buffer is allocated at the part's full size and the
/// caller copies a block's values straight from the column's own memory into their place in it -
/// see `GPU::DeviceBuffer`, which explains why a host staging copy would cost as much as the
/// transfer itself.

/// Allocates `bytes` of device memory and writes the handle to `*handle`. `bytes` has to be
/// non-zero: a column of no values is not something to hold.
int clickhouseGPUDeviceBufferAllocate(size_t bytes, void ** handle, char * error, size_t error_size);

/// Copies `bytes` of host memory from `host_data` into the buffer at `offset`, which together have
/// to stay within it.
///
/// Returns only once the copy has run, so the caller is free to release the block it copied from -
/// which is the point: the block is the reader's, and the alternative is a second copy into a
/// staging buffer the caller owns, which on this machine costs 294 ms per 1.49 GiB against the
/// 334 ms the transfer itself takes.
int clickhouseGPUDeviceBufferCopyIn(
    void * handle,
    size_t offset,
    const void * host_data,
    size_t bytes,
    char * error,
    size_t error_size);

/// Sums the first `num_rows` values of `element_type` in the buffer and writes the sum to the eight
/// bytes at `result` as `sum_type` says, exactly as `clickhouseGPUSum` does - the difference being
/// that the values are on the device already and nothing crosses the link but the eight bytes back.
int clickhouseGPUDeviceBufferSum(
    void * handle,
    int element_type,
    int sum_type,
    size_t num_rows,
    void * result,
    char * error,
    size_t error_size);

/// Releases the device memory and the handle. Does nothing on a null handle, and cannot fail: it is
/// called from a destructor - the cache entry's - where there is nobody left to report a failure to.
void clickhouseGPUDeviceBufferFree(void * handle);

}
