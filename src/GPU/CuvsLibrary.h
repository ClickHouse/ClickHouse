#pragma once

#include "config.h"

#if USE_GPU

#include <dlpack/dlpack.h>

#include <boost/noncopyable.hpp>

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

/// The part of the C API of cuVS (https://github.com/rapidsai/cuvs) that the `cagra` method of the `vector_similarity`
/// index needs. cuVS is not linked: its prebuilt library brings its own RMM, RAFT, NCCL and CUDA math libraries, all built
/// against libstdc++, and it is loaded with `dlopen` the first time such an index is built or searched. The declarations
/// mirror `cuvs/core/c_api.h`, `cuvs/core/dataset.h` and `cuvs/neighbors/cagra.h` of cuVS 26.08, and the version of the
/// loaded library is checked against that.
namespace DB::GPU::Cuvs
{

enum Error : int
{
    CUVS_ERROR = 0,
    CUVS_SUCCESS = 1,
};

using Resources = uintptr_t;

enum DistanceType : int
{
    L2Expanded = 0,
    InnerProduct = 6,
};

enum DatasetMemType : int
{
    CUVS_DATASET_MEM_TYPE_HOST = 0,
    CUVS_DATASET_MEM_TYPE_DEVICE = 1,
};

struct Dataset;
using DatasetHandle = Dataset *;

enum CagraGraphBuildAlgo : int
{
    AUTO_SELECT = 0,
    IVF_PQ = 1,
    NN_DESCENT = 2,
};

struct CagraIndexParams
{
    DistanceType metric;
    size_t intermediate_graph_degree;
    size_t graph_degree;
    CagraGraphBuildAlgo build_algo;
    size_t nn_descent_niter;
    void * graph_build_params;
};

enum CagraSearchAlgo : int
{
    SINGLE_CTA = 0,
    MULTI_CTA = 1,
    MULTI_KERNEL = 2,
    AUTO = 100,
};

enum CagraHashMode : int
{
    HASH = 0,
    SMALL = 1,
    AUTO_HASH = 100,
};

struct CagraSearchParams
{
    size_t max_queries;
    size_t itopk_size;
    size_t max_iterations;
    CagraSearchAlgo algo;
    size_t team_size;
    size_t search_width;
    size_t min_iterations;
    size_t thread_block_size;
    CagraHashMode hashmap_mode;
    size_t hashmap_min_bitlen;
    float hashmap_max_fill_rate;
    uint32_t num_random_samplings;
    uint64_t rand_xor_mask;
    bool persistent;
    float persistent_lifetime;
    float persistent_device_usage;
};

struct CagraIndex;
using CagraIndexHandle = CagraIndex *;

enum FilterType : int
{
    NO_FILTER = 0,
};

struct Filter
{
    uintptr_t addr;
    FilterType type;
};

/// The entry points of the loaded library.
struct Api
{
    const char * (*getLastErrorText)();
    Error (*versionGet)(uint16_t *, uint16_t *, uint16_t *);
    Error (*rmmAsyncMemoryResourceEnable)();
    Error (*resourcesCreate)(Resources *);
    Error (*resourcesDestroy)(Resources);
    Error (*streamSync)(Resources);
    Error (*rmmAlloc)(Resources, void **, size_t);
    Error (*rmmFree)(Resources, void *, size_t);
    Error (*matrixCopy)(Resources, DLManagedTensor *, DLManagedTensor *);
    Error (*datasetMakeStandardView)(Resources, DLManagedTensor *, DatasetHandle *);
    Error (*datasetMakePadded)(Resources, DLManagedTensor *, DatasetMemType, DatasetHandle *);
    Error (*datasetMakePaddedView)(Resources, DLManagedTensor *, DatasetHandle *);
    Error (*datasetDestroy)(DatasetHandle);
    Error (*cagraIndexParamsCreate)(CagraIndexParams **);
    Error (*cagraIndexParamsDestroy)(CagraIndexParams *);
    Error (*cagraSearchParamsCreate)(CagraSearchParams **);
    Error (*cagraSearchParamsDestroy)(CagraSearchParams *);
    Error (*cagraIndexCreate)(CagraIndexHandle *);
    Error (*cagraIndexDestroy)(CagraIndexHandle);
    Error (*cagraBuild)(Resources, CagraIndexParams *, DatasetHandle, CagraIndexHandle);
    Error (*cagraIndexGetGraph)(CagraIndexHandle, DLManagedTensor *);
    Error (*cagraIndexFromArgs)(Resources, DistanceType, DLManagedTensor *, DLManagedTensor *, CagraIndexHandle);
    Error (*cagraUpdateDataset)(Resources, DatasetHandle, CagraIndexHandle);
    Error (*cagraSearch)(Resources, CagraSearchParams *, CagraIndexHandle, DLManagedTensor *, DLManagedTensor *, DLManagedTensor *, Filter);
};

/// Runs `task` on one of the threads reserved for cuVS, waits for it, and rethrows what it throws. Everything that calls
/// cuVS runs there. libgomp keeps a pool of OpenMP threads for each thread that starts a parallel region, and cuVS starts
/// them on the host during a build: called from the threads of the query pipeline, which come and go, it would make a new
/// pool for each of them.
void execute(std::function<void()> task);

/// Loads cuVS on the first call and returns its entry points; throws when it cannot be loaded. The library is taken
/// from the environment variable `CLICKHOUSE_CUVS_LIBRARY` (a path to `libcuvs_c.so`), or else found by the dynamic
/// loader by the name `libcuvs_c.so`.
const Api & api();

/// Throws the last error of cuVS, naming `what` failed, unless `error` is `CUVS_SUCCESS`.
void check(Error error, const char * what);

/// A cuVS handle with its own CUDA stream. It may be used by one thread at a time, and the pool gives them out.
class ResourcesPool : private boost::noncopyable
{
public:
    static ResourcesPool & instance();

    class Lease : private boost::noncopyable
    {
    public:
        Lease(ResourcesPool & pool_, Resources resources_) : pool(pool_), resources(resources_) {}
        ~Lease() { pool.release(resources); }
        Resources get() const { return resources; }
    private:
        ResourcesPool & pool;
        Resources resources;
    };

    Lease acquire();

private:
    ResourcesPool() = default;
    void release(Resources resources);

    std::mutex mutex;
    std::vector<Resources> free;
};

/// A device buffer from the workspace of `resources`. It is freed to the same `resources`: each keeps a count of what it
/// gave out, and a buffer freed to another would make one count wrap around and the other grow to its limit.
class DeviceBuffer : private boost::noncopyable
{
public:
    DeviceBuffer(Resources resources_, size_t size_);
    ~DeviceBuffer();
    void * data() const { return ptr; }
    size_t size() const { return bytes; }
private:
    Resources resources;
    void * ptr = nullptr;
    size_t bytes = 0;
};

/// A non-owning row-major DLPack matrix.
struct Matrix
{
    DLManagedTensor tensor{};
    int64_t shape[2];

    Matrix(const void * data, int64_t rows, int64_t columns, DLDataTypeCode code, bool on_device);
    /// `tensor` points to `shape`.
    Matrix(const Matrix &) = delete;
    Matrix & operator=(const Matrix &) = delete;
    DLManagedTensor * get() { return &tensor; }
};

}

#endif
