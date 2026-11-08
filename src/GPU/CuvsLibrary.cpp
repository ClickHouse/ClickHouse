#include <GPU/CuvsLibrary.h>

#if USE_GPU

#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <dlfcn.h>

#include <condition_variable>
#include <cstdlib>
#include <string_view>
#include <deque>
#include <future>
#include <thread>

namespace DB::ErrorCodes
{
    extern const int CANNOT_DLOPEN;
    extern const int GPU_ERROR;
}

namespace DB::GPU::Cuvs
{

namespace
{

/// The release whose declarations `CuvsLibrary.h` mirrors. The structs of parameters change between releases.
constexpr uint16_t expected_major = 26;
constexpr uint16_t expected_minor = 8;

template <typename F>
void bind(void * handle, F & function, const char * name)
{
    void * symbol = dlsym(handle, name);
    if (!symbol)
        throw Exception(ErrorCodes::CANNOT_DLOPEN, "cuVS has no function `{}`: {}", name, dlerror());
    function = reinterpret_cast<F>(symbol);
}

const char * libraryPath()
{
    const char * path = std::getenv("CLICKHOUSE_CUVS_LIBRARY"); // NOLINT(concurrency-mt-unsafe)
    return path ? path : "libcuvs_c.so";
}

void * openLibrary()
{
    /// RTLD_LOCAL: the RMM, RAFT and spdlog of cuVS stay out of the global namespace, where they would clash with the copies
    /// ClickHouse builds for cuDF.
    return dlopen(libraryPath(), RTLD_NOW | RTLD_LOCAL);
}

Api load()
{
    /// When a thread exits, glibc trims its cache of stacks (40 MiB by default) and frees the DTVs of the evicted ones -
    /// in the exiting thread, after jemalloc has torn down its state of that thread. jemalloc then initializes the thread
    /// anew and puts a new tcache, which lives in the TLS of the thread, into the list of an arena; the stack goes away
    /// and the next walk of that list crashes. The threads of OpenMP and CUDA that cuVS starts make the cache overflow.
    /// A large enough cache never trims.
    const char * tunables = std::getenv("GLIBC_TUNABLES"); // NOLINT(concurrency-mt-unsafe)
    if (!tunables || !std::string_view(tunables).contains("glibc.pthread.stack_cache_size="))
        throw Exception(ErrorCodes::CANNOT_DLOPEN,
            "cuVS needs the server to be started with the environment variable "
            "`GLIBC_TUNABLES=glibc.pthread.stack_cache_size=17179869184`, see the comment in GPU/CuvsLibrary.cpp");

    const char * path = libraryPath();
    void * handle = openLibrary();
    if (!handle)
        throw Exception(ErrorCodes::CANNOT_DLOPEN,
            "Cannot load cuVS from `{}`: {}. Set the environment variable `CLICKHOUSE_CUVS_LIBRARY` to the path of `libcuvs_c.so`",
            path, dlerror());

    Api api{};
    bind(handle, api.getLastErrorText, "cuvsGetLastErrorText");
    bind(handle, api.versionGet, "cuvsVersionGet");
    bind(handle, api.rmmAsyncMemoryResourceEnable, "cuvsRMMAsyncMemoryResourceEnable");
    bind(handle, api.resourcesCreate, "cuvsResourcesCreate");
    bind(handle, api.resourcesDestroy, "cuvsResourcesDestroy");
    bind(handle, api.streamSync, "cuvsStreamSync");
    bind(handle, api.rmmAlloc, "cuvsRMMAlloc");
    bind(handle, api.rmmFree, "cuvsRMMFree");
    bind(handle, api.matrixCopy, "cuvsMatrixCopy");
    bind(handle, api.datasetMakeStandardView, "cuvsDatasetMakeStandardView");
    bind(handle, api.datasetMakePadded, "cuvsDatasetMakePadded");
    bind(handle, api.datasetMakePaddedView, "cuvsDatasetMakePaddedView");
    bind(handle, api.datasetDestroy, "cuvsDatasetDestroy");
    bind(handle, api.cagraIndexParamsCreate, "cuvsCagraIndexParamsCreate");
    bind(handle, api.cagraIndexParamsDestroy, "cuvsCagraIndexParamsDestroy");
    bind(handle, api.cagraSearchParamsCreate, "cuvsCagraSearchParamsCreate");
    bind(handle, api.cagraSearchParamsDestroy, "cuvsCagraSearchParamsDestroy");
    bind(handle, api.cagraIndexCreate, "cuvsCagraIndexCreate");
    bind(handle, api.cagraIndexDestroy, "cuvsCagraIndexDestroy");
    bind(handle, api.cagraBuild, "cuvsCagraBuild");
    bind(handle, api.cagraIndexGetGraph, "cuvsCagraIndexGetGraph");
    bind(handle, api.cagraIndexFromArgs, "cuvsCagraIndexFromArgs");
    bind(handle, api.cagraUpdateDataset, "cuvsCagraUpdateDataset");
    bind(handle, api.cagraSearch, "cuvsCagraSearch");

    uint16_t major = 0;
    uint16_t minor = 0;
    uint16_t patch = 0;
    if (api.versionGet(&major, &minor, &patch) != CUVS_SUCCESS)
        throw Exception(ErrorCodes::GPU_ERROR, "Cannot get the version of cuVS: {}", api.getLastErrorText());
    if (major != expected_major || minor != expected_minor)
        throw Exception(ErrorCodes::CANNOT_DLOPEN, "cuVS {}.{}.{} at `{}` is unsupported, expected {}.{}",
            major, minor, patch, path, expected_major, expected_minor);

    /// The default resource of RMM calls `cudaMalloc` and `cudaFree` for every allocation, including the workspace of each
    /// search. The asynchronous one takes from the stream-ordered pool of the device instead.
    if (api.rmmAsyncMemoryResourceEnable() != CUVS_SUCCESS)
        throw Exception(ErrorCodes::GPU_ERROR, "Cannot enable the asynchronous memory resource of cuVS: {}", api.getLastErrorText());

    LOG_INFO(getLogger("cuVS"), "Loaded cuVS {}.{}.{} from `{}`", major, minor, patch, path);
    return api;
}

}

namespace
{

/// The threads that call cuVS, see `execute`. They are detached and never exit. CUDA serializes the work on the device
/// anyway, so a few are enough for builds and searches to overlap their copies and host work.
class Executor
{
public:
    static constexpr size_t num_threads = 8;

    static Executor & instance()
    {
        static Executor * executor = new Executor; /// Never destroyed: the threads outlive static destruction.
        return *executor;
    }

    void run(std::function<void()> task)
    {
        std::packaged_task<void()> packaged(std::move(task));
        auto future = packaged.get_future();
        {
            std::lock_guard lock(mutex);
            queue.push_back(std::move(packaged));
        }
        cv.notify_one();
        future.get();
    }

private:
    Executor()
    {
        for (size_t i = 0; i < num_threads; ++i)
            std::thread([this] { work(); }).detach();
    }

    [[noreturn]] void work()
    {
        while (true)
        {
            std::packaged_task<void()> task;
            {
                std::unique_lock lock(mutex);
                cv.wait(lock, [this] { return !queue.empty(); });
                task = std::move(queue.front());
                queue.pop_front();
            }
            task();
        }
    }

    std::mutex mutex;
    std::condition_variable cv;
    std::deque<std::packaged_task<void()>> queue;
};

}

void execute(std::function<void()> task)
{
    Executor::instance().run(std::move(task));
}

const Api & api()
{
    /// A failure to load is thrown on every call: the static is only set once loading succeeds.
    static std::mutex mutex;
    static std::unique_ptr<Api> loaded;
    std::lock_guard lock(mutex);
    if (!loaded)
        loaded = std::make_unique<Api>(load());
    return *loaded;
}

void check(Error error, const char * what)
{
    if (error != CUVS_SUCCESS)
        throw Exception(ErrorCodes::GPU_ERROR, "cuVS: {} failed: {}", what, api().getLastErrorText());
}

ResourcesPool & ResourcesPool::instance()
{
    static ResourcesPool pool;
    return pool;
}

ResourcesPool::Lease ResourcesPool::acquire()
{
    {
        std::lock_guard lock(mutex);
        if (!free.empty())
        {
            Resources resources = free.back();
            free.pop_back();
            return Lease(*this, resources);
        }
    }
    Resources resources = 0;
    check(api().resourcesCreate(&resources), "cuvsResourcesCreate");
    return Lease(*this, resources);
}

void ResourcesPool::release(Resources resources)
{
    std::lock_guard lock(mutex);
    free.push_back(resources);
}

DeviceBuffer::DeviceBuffer(Resources resources_, size_t size_)
    : resources(resources_)
    , bytes(size_)
{
    check(api().rmmAlloc(resources, &ptr, bytes), "cuvsRMMAlloc");
}

DeviceBuffer::~DeviceBuffer()
{
    if (ptr && api().rmmFree(resources, ptr, bytes) != CUVS_SUCCESS)
        LOG_ERROR(getLogger("cuVS"), "cuvsRMMFree failed: {}", api().getLastErrorText());
}

Matrix::Matrix(const void * data, int64_t rows, int64_t columns, DLDataTypeCode code, bool on_device)
{
    shape[0] = rows;
    shape[1] = columns;
    tensor.dl_tensor.data = const_cast<void *>(data);
    tensor.dl_tensor.device.device_type = on_device ? kDLCUDA : kDLCPU;
    tensor.dl_tensor.device.device_id = 0;
    tensor.dl_tensor.ndim = 2;
    tensor.dl_tensor.dtype.code = static_cast<uint8_t>(code);
    tensor.dl_tensor.dtype.bits = 32;
    tensor.dl_tensor.dtype.lanes = 1;
    tensor.dl_tensor.shape = shape;
    tensor.dl_tensor.strides = nullptr;
    tensor.dl_tensor.byte_offset = 0;
}

}

#endif
