#include <Storages/MergeTree/DiskANNIndex.h>

#if USE_DISKANN

#include <diskann_ffi.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

namespace ProfileEvents
{
    extern const Event DiskANNBuildCount;
    extern const Event DiskANNBuildMicroseconds;

    extern const Event DiskANNDistanceComputeCount;
    extern const Event DiskANNDistanceComputeRows;
    extern const Event DiskANNDistanceComputeMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace
{

::DiskANNMetric toFFIMetric(DiskANNMetric metric)
{
    switch (metric)
    {
        case DiskANNMetric::L2:
            return DISKANN_METRIC_L2;
        case DiskANNMetric::Cosine:
            return DISKANN_METRIC_COSINE;
    }
    UNREACHABLE();
}

std::string getLastFFIError()
{
    char buf[1024];
    int64_t len = diskann_last_error(buf, sizeof(buf));
    if (len <= 0)
        return "unknown error";
    return std::string(buf, static_cast<size_t>(len));
}

}

DiskANNDiskIndexBuilder::DiskANNDiskIndexBuilder(
    size_t dimensions,
    DiskANNMetric metric,
    DiskANNBuildOptions options)
    : dim(dimensions)
{
    handle = diskann_create_disk_builder(
        static_cast<uint32_t>(dim),
        toFFIMetric(metric),
        options.pruned_degree,
        options.max_degree,
        options.l_build,
        options.alpha,
        options.num_threads,
        options.pq_chunks,
        options.build_ram_limit_gb);

    if (handle < 0)
        throwFromFFIError("DiskANN create_disk_builder failed");
}

DiskANNDiskIndexBuilder::~DiskANNDiskIndexBuilder()
{
    if (handle >= 0)
        diskann_drop_builder(handle);
}

DiskANNDiskIndexBuilder::DiskANNDiskIndexBuilder(DiskANNDiskIndexBuilder && other) noexcept
    : handle(other.handle)
    , dim(other.dim)
{
    other.handle = -1;
}

DiskANNDiskIndexBuilder & DiskANNDiskIndexBuilder::operator=(DiskANNDiskIndexBuilder && other) noexcept
{
    if (this != &other)
    {
        if (handle >= 0)
            diskann_drop_builder(handle);

        handle = other.handle;
        dim = other.dim;
        other.handle = -1;
    }
    return *this;
}

void DiskANNDiskIndexBuilder::setDataPath(const std::string & path)
{
    auto rc = diskann_builder_set_data_path(handle, path.c_str());
    if (rc < 0)
        throwFromFFIError("DiskANN builder_set_data_path failed");
}

void DiskANNDiskIndexBuilder::setIndexPrefix(const std::string & prefix)
{
    auto rc = diskann_builder_set_index_prefix(handle, prefix.c_str());
    if (rc < 0)
        throwFromFFIError("DiskANN builder_set_index_prefix failed");
}

void DiskANNDiskIndexBuilder::build() const
{
    Stopwatch watch;
    auto rc = diskann_builder_build(handle);
    ProfileEvents::increment(ProfileEvents::DiskANNBuildCount);
    ProfileEvents::increment(ProfileEvents::DiskANNBuildMicroseconds, watch.elapsedMicroseconds());
    if (rc < 0)
        throwFromFFIError("DiskANN builder_build failed");
}

bool DiskANNDiskIndexBuilder::indexFilesExist(const std::string & index_prefix)
{
    auto rc = diskann_index_file_exists(index_prefix.c_str());
    if (rc < 0)
        throwFromFFIError("DiskANN index_file_exists failed");
    return rc == 1;
}

[[noreturn]] void DiskANNDiskIndexBuilder::throwFromFFIError(const std::string & context)
{
    throw Exception(ErrorCodes::INCORRECT_DATA, "{}: {}", context, getLastFFIError());
}

DiskANNDiskIndexSearcher::DiskANNDiskIndexSearcher(
    size_t dimensions,
    DiskANNMetric metric,
    const std::string & index_prefix,
    DiskANNSearchOptions options_)
    : dim(dimensions)
    , options(options_)
{
    handle = diskann_open_searcher(
        index_prefix.c_str(),
        static_cast<uint32_t>(dim),
        toFFIMetric(metric),
        options.num_threads,
        options.search_io_limit,
        options.num_nodes_to_cache);

    if (handle < 0)
        throwFromFFIError("DiskANN open_searcher failed");
}

DiskANNDiskIndexSearcher::~DiskANNDiskIndexSearcher()
{
    if (handle >= 0)
        diskann_close_searcher(handle);
}

DiskANNDiskIndexSearcher::DiskANNDiskIndexSearcher(DiskANNDiskIndexSearcher && other) noexcept
    : handle(other.handle)
    , dim(other.dim)
    , options(other.options)
{
    other.handle = -1;
}

DiskANNDiskIndexSearcher & DiskANNDiskIndexSearcher::operator=(DiskANNDiskIndexSearcher && other) noexcept
{
    if (this != &other)
    {
        if (handle >= 0)
            diskann_close_searcher(handle);

        handle = other.handle;
        dim = other.dim;
        options = other.options;
        other.handle = -1;
    }
    return *this;
}

size_t DiskANNDiskIndexSearcher::search(
    const float * query,
    size_t query_dim,
    size_t k,
    uint64_t * ids,
    float * distances,
    size_t search_list_size,
    size_t beam_width) const
{
    if (query_dim != dim)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "DiskANN search: query dimension {} does not match index dimension {}",
            query_dim,
            dim);

    auto effective_search_list_size = search_list_size > 0
        ? search_list_size
        : static_cast<size_t>(options.default_search_list_size);
    auto effective_beam_width = beam_width > 0
        ? beam_width
        : static_cast<size_t>(options.default_beam_width);

    auto rc = diskann_search_disk_index(
        handle,
        query,
        static_cast<uint32_t>(query_dim),
        static_cast<uint32_t>(k),
        static_cast<uint32_t>(effective_search_list_size),
        static_cast<uint32_t>(effective_beam_width),
        ids,
        distances);

    if (rc < 0)
        throwFromFFIError("DiskANN search_disk_index failed");

    return static_cast<size_t>(rc);
}

[[noreturn]] void DiskANNDiskIndexSearcher::throwFromFFIError(const std::string & context)
{
    throw Exception(ErrorCodes::INCORRECT_DATA, "{}: {}", context, getLastFFIError());
}

void DiskANNComputeDistances(
    DiskANNMetric metric,
    size_t dim,
    const float * query,
    const float * candidates,
    size_t n,
    float * out)
{
    if (n == 0)
        return;

    Stopwatch watch;
    auto rc = diskann_compute_distances(
        toFFIMetric(metric),
        static_cast<uint32_t>(dim),
        query,
        candidates,
        static_cast<uint64_t>(n),
        out);
    ProfileEvents::increment(ProfileEvents::DiskANNDistanceComputeCount);
    ProfileEvents::increment(ProfileEvents::DiskANNDistanceComputeRows, n);
    ProfileEvents::increment(ProfileEvents::DiskANNDistanceComputeMicroseconds, watch.elapsedMicroseconds());

    if (rc < 0)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "DiskANN compute_distances failed: {}",
            getLastFFIError());
}

}

#endif
