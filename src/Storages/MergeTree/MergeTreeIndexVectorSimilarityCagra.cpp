#include <Storages/MergeTree/MergeTreeIndexVectorSimilarityCagra.h>

#if USE_GPU

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <GPU/CuvsLibrary.h>
#include <cuda_runtime_api.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Common/Stopwatch.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Common/HashTable/HashSet.h>
#include <base/scope_guard.h>

#include <usearch/index_plugins.hpp>

#include <algorithm>
#include <atomic>
#include <thread>
#include <cmath>
#include <numeric>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 hnsw_candidate_list_size_for_search;
    extern const SettingsFloat vector_search_index_fetch_multiplier;
    extern const SettingsUInt64 max_limit_for_vector_search_queries;
    extern const SettingsBool vector_search_with_rescoring;
    extern const SettingsBool vector_search_cagra_on_cpu;
}

namespace ErrorCodes
{
    extern const int FORMAT_VERSION_TOO_OLD;
    extern const int GPU_ERROR;
    extern const int ILLEGAL_COLUMN;
    extern const int INCORRECT_DATA;
    extern const int INCORRECT_QUERY;
    extern const int INVALID_SETTING_VALUE;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace Cuvs = GPU::Cuvs;

namespace
{

/// Below this, a granule has no graph and is searched exactly: see the comment in the header.
constexpr size_t min_rows_for_graph = 10000;

/// What CAGRA marks a missing neighbour with, when fewer than `limit` were found.
constexpr UInt32 invalid_neighbour = std::numeric_limits<UInt32>::max();

Cuvs::DistanceType toCuvsMetric(CagraMetric metric)
{
    return metric == CagraMetric::L2 ? Cuvs::L2Expanded : Cuvs::InnerProduct;
}

void normalizeImpl(float * vector, size_t dimensions)
{
    double sum = 0;
    for (size_t i = 0; i < dimensions; ++i)
        sum += static_cast<double>(vector[i]) * static_cast<double>(vector[i]);
    if (sum == 0)
        return;
    const float inverse = static_cast<float>(1.0 / std::sqrt(sum));
    for (size_t i = 0; i < dimensions; ++i)
        vector[i] *= inverse;
}

/// The distance CAGRA returns, as the `hnsw` method returns it: the squared L2 distance, or one minus the inner product.
float toDistance(CagraMetric metric, float cuvs_distance)
{
    return metric == CagraMetric::L2 ? cuvs_distance : 1.0f - cuvs_distance;
}

void checkFinite(const float * vector, size_t dimensions, int error_code, std::string_view context)
{
    for (size_t i = 0; i < dimensions; ++i)
        if (!std::isfinite(vector[i]))
            throw Exception(error_code, "Vector for vector similarity index ({}) must not contain non-finite values (NaN or Inf)", context);
}

struct CagraIndexParamsHolder
{
    Cuvs::CagraIndexParams * params = nullptr;
    CagraIndexParamsHolder() { Cuvs::check(Cuvs::api().cagraIndexParamsCreate(&params), "cuvsCagraIndexParamsCreate"); }
    ~CagraIndexParamsHolder() { Cuvs::api().cagraIndexParamsDestroy(params); }
};

struct CagraSearchParamsHolder
{
    Cuvs::CagraSearchParams * params = nullptr;
    CagraSearchParamsHolder() { Cuvs::check(Cuvs::api().cagraSearchParamsCreate(&params), "cuvsCagraSearchParamsCreate"); }
    ~CagraSearchParamsHolder() { Cuvs::api().cagraSearchParamsDestroy(params); }
};

struct CagraIndexHolder
{
    Cuvs::CagraIndexHandle index = nullptr;
    CagraIndexHolder() { Cuvs::check(Cuvs::api().cagraIndexCreate(&index), "cuvsCagraIndexCreate"); }
    ~CagraIndexHolder() { Cuvs::api().cagraIndexDestroy(index); }
};

struct DatasetHolder
{
    Cuvs::DatasetHandle dataset = nullptr;
    ~DatasetHolder()
    {
        if (dataset)
            Cuvs::api().datasetDestroy(dataset);
    }
};

std::vector<UInt32> buildGraphImpl(const std::vector<float> & dataset, size_t rows, size_t dimensions, CagraMetric metric, CagraParams params)
{
    auto lease = Cuvs::ResourcesPool::instance().acquire();
    const auto & api = Cuvs::api();

    CagraIndexParamsHolder index_params;
    index_params.params->metric = toCuvsMetric(metric);
    index_params.params->graph_degree = params.graph_degree;
    index_params.params->intermediate_graph_degree = params.intermediate_graph_degree;
    index_params.params->build_algo = Cuvs::AUTO_SELECT;

    Cuvs::Matrix host_dataset(dataset.data(), rows, dimensions, kDLFloat, false);
    DatasetHolder dataset_view;
    Cuvs::check(api.datasetMakeStandardView(lease.get(), host_dataset.get(), &dataset_view.dataset), "cuvsDatasetMakeStandardView");

    CagraIndexHolder index;
    Cuvs::check(api.cagraBuild(lease.get(), index_params.params, dataset_view.dataset, index.index), "cuvsCagraBuild");

    DLManagedTensor device_graph{};
    Cuvs::check(api.cagraIndexGetGraph(index.index, &device_graph), "cuvsCagraIndexGetGraph");
    SCOPE_EXIT({
        if (device_graph.deleter)
            device_graph.deleter(&device_graph);
    });

    if (device_graph.dl_tensor.ndim != 2 || static_cast<size_t>(device_graph.dl_tensor.shape[0]) != rows
        || static_cast<size_t>(device_graph.dl_tensor.shape[1]) != params.graph_degree)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAGRA built a graph of unexpected shape");

    std::vector<UInt32> graph(rows * params.graph_degree);
    Cuvs::Matrix host_graph(graph.data(), rows, params.graph_degree, kDLUInt, false);
    Cuvs::check(api.matrixCopy(lease.get(), &device_graph, host_graph.get()), "cuvsMatrixCopy");
    Cuvs::check(api.streamSync(lease.get()), "cuvsStreamSync");
    return graph;
}

/// Builds the graph of `rows` vectors of `dataset` on the device and returns it, `rows * graph_degree` neighbours.
std::vector<UInt32> buildGraph(const std::vector<float> & dataset, size_t rows, size_t dimensions, CagraMetric metric, CagraParams params)
{
    std::vector<UInt32> graph;
    Cuvs::execute([&] { graph = buildGraphImpl(dataset, rows, dimensions, metric, params); });
    return graph;
}

}

/// The vectors of a granule on the device. Allocated with the CUDA runtime of ClickHouse, not from the workspace of cuVS,
/// which is capped at a quarter of the device memory per `cuvsResources_t`. Both runtimes are on the primary context of
/// the device, so the pointer is valid for cuVS as well.
class DeviceDataset : private boost::noncopyable
{
public:
    explicit DeviceDataset(size_t bytes)
    {
        if (cudaError_t error = cudaMalloc(&ptr, bytes); error != cudaSuccess)
            throw Exception(ErrorCodes::GPU_ERROR, "Cannot allocate {} on the device for a CAGRA index: {}", ReadableSize(bytes), cudaGetErrorString(error));
    }
    ~DeviceDataset() { cudaFree(ptr); }
    void * data() const { return ptr; }
private:
    void * ptr = nullptr;
};

/// The graph and the vectors of a granule on the device, ready to search.
class CagraDeviceIndex : private boost::noncopyable
{
public:
    explicit CagraDeviceIndex(const MergeTreeIndexGranuleCagra & granule)
    {
        const auto & api = Cuvs::api();

        const size_t bytes = granule.rows * granule.dimensions * sizeof(float);
        auto device_dataset = std::make_unique<DeviceDataset>(bytes);
        Cuvs::Matrix host_matrix(granule.dataset.data(), granule.rows, granule.dimensions, kDLFloat, false);
        Cuvs::Matrix device_matrix(device_dataset->data(), granule.rows, granule.dimensions, kDLFloat, true);
        Cuvs::check(api.matrixCopy(resources.get(), host_matrix.get(), device_matrix.get()), "cuvsMatrixCopy");

        /// The graph is copied to the device by cuVS.
        Cuvs::Matrix host_graph(granule.graph.data(), granule.rows, granule.graph_degree, kDLUInt, false);
        Cuvs::check(api.cagraIndexFromArgs(resources.get(), toCuvsMetric(granule.metric), host_graph.get(), device_matrix.get(), index.index),
            "cuvsCagraIndexFromArgs");

        /// The search needs rows aligned to 16 bytes. When they are, the dense copy is used as it is, otherwise cuVS makes an
        /// aligned copy, and the dense one is not needed after that.
        if (granule.dimensions % 4 == 0)
        {
            Cuvs::check(api.datasetMakePaddedView(resources.get(), device_matrix.get(), &padded.dataset), "cuvsDatasetMakePaddedView");
            dataset = std::move(device_dataset);
        }
        else
        {
            Cuvs::check(api.datasetMakePadded(resources.get(), device_matrix.get(), Cuvs::CUVS_DATASET_MEM_TYPE_DEVICE, &padded.dataset),
                "cuvsDatasetMakePadded");
        }
        Cuvs::check(api.cagraUpdateDataset(resources.get(), padded.dataset, index.index), "cuvsCagraUpdateDataset");
        Cuvs::check(api.streamSync(resources.get()), "cuvsStreamSync");
    }

    Cuvs::CagraIndexHandle get() const { return index.index; }

private:
    /// Its own: the memory of the index stays allocated from it for as long as the index lives, see `Cuvs::DeviceBuffer`.
    struct OwnedResources
    {
        Cuvs::Resources handle = 0;
        OwnedResources() { Cuvs::check(Cuvs::api().resourcesCreate(&handle), "cuvsResourcesCreate"); }
        ~OwnedResources() { Cuvs::api().resourcesDestroy(handle); }
        Cuvs::Resources get() const { return handle; }
    };

    /// Destroyed in the reverse order: the index, then the padded dataset, then the memory it may be a view of, then the
    /// resources all of it came from.
    OwnedResources resources;
    std::unique_ptr<DeviceDataset> dataset;
    DatasetHolder padded;
    CagraIndexHolder index;
};

MergeTreeIndexGranuleCagra::MergeTreeIndexGranuleCagra(const String & index_name_, CagraMetric metric_, size_t dimensions_)
    : index_name(index_name_)
    , metric(metric_)
    , dimensions(dimensions_)
{
}

MergeTreeIndexGranuleCagra::MergeTreeIndexGranuleCagra(
    const String & index_name_, CagraMetric metric_, size_t dimensions_,
    std::vector<float> && dataset_, std::vector<UInt32> && graph_, size_t graph_degree_)
    : index_name(index_name_)
    , metric(metric_)
    , dimensions(dimensions_)
    , rows(dataset_.size() / dimensions_)
    , graph_degree(graph_degree_)
    , dataset(std::move(dataset_))
    , graph(std::move(graph_))
{
}

MergeTreeIndexGranuleCagra::~MergeTreeIndexGranuleCagra() = default;

size_t MergeTreeIndexGranuleCagra::memoryUsageBytes() const
{
    return dataset.size() * sizeof(float) + graph.size() * sizeof(UInt32);
}

void MergeTreeIndexGranuleCagra::serializeBinary(WriteBuffer & ostr) const
{
    if (empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to write empty vector similarity index {}", backQuote(index_name));

    writeIntBinary(FILE_FORMAT_VERSION, ostr);
    writeIntBinary(static_cast<UInt64>(dimensions), ostr);
    writeIntBinary(static_cast<UInt8>(metric), ostr);
    writeIntBinary(static_cast<UInt64>(rows), ostr);
    writeIntBinary(static_cast<UInt64>(graph_degree), ostr);
    ostr.write(reinterpret_cast<const char *>(dataset.data()), dataset.size() * sizeof(float));
    ostr.write(reinterpret_cast<const char *>(graph.data()), graph.size() * sizeof(UInt32));
}

void MergeTreeIndexGranuleCagra::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion /*version*/)
{
    UInt64 file_version = 0;
    readIntBinary(file_version, istr);
    if (file_version != FILE_FORMAT_VERSION)
        throw Exception(ErrorCodes::FORMAT_VERSION_TOO_OLD,
            "Vector similarity index could not be loaded because its version is not supported (current version: {}, persisted version: {}). "
            "Please drop the index and create it again.", FILE_FORMAT_VERSION, file_version);

    UInt64 persisted_dimensions = 0;
    UInt8 persisted_metric = 0;
    UInt64 persisted_rows = 0;
    UInt64 persisted_degree = 0;
    readIntBinary(persisted_dimensions, istr);
    readIntBinary(persisted_metric, istr);
    readIntBinary(persisted_rows, istr);
    readIntBinary(persisted_degree, istr);

    if (persisted_dimensions != dimensions || persisted_metric != static_cast<UInt8>(metric))
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Vector similarity index {} was written with {} dimensions and metric {}, but the index is declared with {} dimensions and metric {}",
            backQuote(index_name), persisted_dimensions, static_cast<UInt32>(persisted_metric), dimensions, static_cast<UInt32>(metric));

    rows = persisted_rows;
    graph_degree = persisted_degree;
    dataset.resize(rows * dimensions);
    graph.resize(rows * graph_degree);
    istr.readStrict(reinterpret_cast<char *>(dataset.data()), dataset.size() * sizeof(float));
    istr.readStrict(reinterpret_cast<char *>(graph.data()), graph.size() * sizeof(UInt32));
}

std::shared_ptr<CagraDeviceIndex> MergeTreeIndexGranuleCagra::getDeviceIndex() const
{
    std::lock_guard lock(device_mutex);
    if (!device_index)
    {
        Stopwatch watch;
        /// Made and destroyed on the threads of cuVS, see `GPU::Cuvs::execute`.
        Cuvs::execute([&]
        {
            device_index = std::shared_ptr<CagraDeviceIndex>(
                new CagraDeviceIndex(*this),
                [](CagraDeviceIndex * index) { Cuvs::execute([index] { delete index; }); });
        });
        LOG_TRACE(logger, "Uploaded CAGRA index {} of {} rows ({}) to the device in {} ms",
            backQuote(index_name), rows, ReadableSize(memoryUsageBytes()), watch.elapsedMilliseconds());
    }
    return device_index;
}

NearestNeighbours MergeTreeIndexGranuleCagra::searchExact(const std::vector<float> & query, size_t limit, bool return_distances) const
{
    std::vector<float> distances(rows);
    for (size_t row = 0; row < rows; ++row)
    {
        const float * vector = dataset.data() + row * dimensions;
        float distance = 0;
        if (metric == CagraMetric::L2)
        {
            for (size_t i = 0; i < dimensions; ++i)
            {
                const float diff = vector[i] - query[i];
                distance += diff * diff;
            }
        }
        else
        {
            for (size_t i = 0; i < dimensions; ++i)
                distance += vector[i] * query[i];
            distance = 1.0f - distance;
        }
        distances[row] = distance;
    }

    std::vector<UInt64> order(rows);
    std::iota(order.begin(), order.end(), 0);
    const size_t k = std::min(limit, rows);
    std::partial_sort(order.begin(), order.begin() + k, order.end(), [&](UInt64 a, UInt64 b) { return distances[a] < distances[b]; });
    order.resize(k);

    NearestNeighbours result;
    if (return_distances)
    {
        result.distances.emplace(k);
        for (size_t i = 0; i < k; ++i)
            (*result.distances)[i] = distances[order[i]];
    }
    result.rows = std::move(order);
    return result;
}

NearestNeighbours MergeTreeIndexGranuleCagra::search(const std::vector<float> & query, size_t limit, size_t candidates, bool return_distances) const
{
    if (graph_degree == 0)
        return searchExact(query, limit, return_distances);

    const auto device = getDeviceIndex();
    const size_t k = std::min(limit, rows);
    std::vector<UInt32> neighbours(k);
    std::vector<float> distances(k);

    Cuvs::execute([&]
    {
        const auto & api = Cuvs::api();
        auto lease = Cuvs::ResourcesPool::instance().acquire();

        /// One allocation for the query and the results: the query, then `k` neighbours and `k` distances.
        Cuvs::DeviceBuffer buffer(lease.get(), (dimensions + 2 * k) * sizeof(float));
        auto * device_query = static_cast<float *>(buffer.data());
        auto * device_neighbours = reinterpret_cast<UInt32 *>(device_query + dimensions);
        auto * device_distances = reinterpret_cast<float *>(device_neighbours + k);

        Cuvs::Matrix host_query(query.data(), 1, dimensions, kDLFloat, false);
        Cuvs::Matrix query_matrix(device_query, 1, dimensions, kDLFloat, true);
        Cuvs::Matrix neighbours_matrix(device_neighbours, 1, k, kDLUInt, true);
        Cuvs::Matrix distances_matrix(device_distances, 1, k, kDLFloat, true);
        Cuvs::check(api.matrixCopy(lease.get(), host_query.get(), query_matrix.get()), "cuvsMatrixCopy");

        CagraSearchParamsHolder search_params;
        search_params.params->itopk_size = std::max(candidates, k);

        Cuvs::check(api.cagraSearch(lease.get(), search_params.params, device->get(), query_matrix.get(), neighbours_matrix.get(),
            distances_matrix.get(), Cuvs::Filter{0, Cuvs::NO_FILTER}), "cuvsCagraSearch");

        Cuvs::Matrix host_neighbours(neighbours.data(), 1, k, kDLUInt, false);
        Cuvs::Matrix host_distances(distances.data(), 1, k, kDLFloat, false);
        Cuvs::check(api.matrixCopy(lease.get(), neighbours_matrix.get(), host_neighbours.get()), "cuvsMatrixCopy");
        Cuvs::check(api.matrixCopy(lease.get(), distances_matrix.get(), host_distances.get()), "cuvsMatrixCopy");
        Cuvs::check(api.streamSync(lease.get()), "cuvsStreamSync");
    });

    NearestNeighbours result;
    result.rows.reserve(k);
    if (return_distances)
        result.distances.emplace().reserve(k);
    for (size_t i = 0; i < k; ++i)
    {
        if (neighbours[i] == invalid_neighbour)
            continue;
        result.rows.push_back(neighbours[i]);
        if (return_distances)
            result.distances->push_back(toDistance(metric, distances[i]));
    }
    return result;
}

void MergeTreeIndexGranuleCagra::normalize(float * vector, size_t dimensions_)
{
    normalizeImpl(vector, dimensions_);
}

void MergeTreeIndexGranuleCagra::searchBatch(const std::vector<float> & queries, size_t num_queries, size_t limit, size_t candidates,
    std::vector<UInt32> & neighbours, std::vector<float> & distances) const
{
    neighbours.assign(num_queries * limit, invalid_neighbour);
    distances.assign(num_queries * limit, std::numeric_limits<float>::infinity());

    if (graph_degree == 0)
    {
        for (size_t q = 0; q < num_queries; ++q)
        {
            std::vector<float> query(queries.begin() + q * dimensions, queries.begin() + (q + 1) * dimensions);
            auto result = searchExact(query, limit, /*return_distances=*/ true);
            for (size_t i = 0; i < result.rows.size(); ++i)
            {
                neighbours[q * limit + i] = static_cast<UInt32>(result.rows[i]);
                distances[q * limit + i] = (*result.distances)[i];
            }
        }
        return;
    }

    /// CAGRA returns at most as many neighbours as there are rows; the rest of each row of the result stays missing.
    const size_t k = std::min(limit, rows);
    const auto device = getDeviceIndex();
    std::vector<UInt32> found_neighbours(num_queries * k);
    std::vector<float> found_distances(num_queries * k);

    Cuvs::execute([&]
    {
        const auto & api = Cuvs::api();
        auto lease = Cuvs::ResourcesPool::instance().acquire();

        Cuvs::DeviceBuffer buffer(lease.get(), num_queries * (dimensions + 2 * k) * sizeof(float));
        auto * device_queries = static_cast<float *>(buffer.data());
        auto * device_neighbours = reinterpret_cast<UInt32 *>(device_queries + num_queries * dimensions);
        auto * device_distances = reinterpret_cast<float *>(device_neighbours + num_queries * k);

        Cuvs::Matrix host_queries(queries.data(), num_queries, dimensions, kDLFloat, false);
        Cuvs::Matrix queries_matrix(device_queries, num_queries, dimensions, kDLFloat, true);
        Cuvs::Matrix neighbours_matrix(device_neighbours, num_queries, k, kDLUInt, true);
        Cuvs::Matrix distances_matrix(device_distances, num_queries, k, kDLFloat, true);
        Cuvs::check(api.matrixCopy(lease.get(), host_queries.get(), queries_matrix.get()), "cuvsMatrixCopy");

        CagraSearchParamsHolder search_params;
        search_params.params->itopk_size = std::max(candidates, k);
        Cuvs::check(api.cagraSearch(lease.get(), search_params.params, device->get(), queries_matrix.get(), neighbours_matrix.get(),
            distances_matrix.get(), Cuvs::Filter{0, Cuvs::NO_FILTER}), "cuvsCagraSearch");

        Cuvs::Matrix host_neighbours(found_neighbours.data(), num_queries, k, kDLUInt, false);
        Cuvs::Matrix host_distances(found_distances.data(), num_queries, k, kDLFloat, false);
        Cuvs::check(api.matrixCopy(lease.get(), neighbours_matrix.get(), host_neighbours.get()), "cuvsMatrixCopy");
        Cuvs::check(api.matrixCopy(lease.get(), distances_matrix.get(), host_distances.get()), "cuvsMatrixCopy");
        Cuvs::check(api.streamSync(lease.get()), "cuvsStreamSync");
    });

    for (size_t q = 0; q < num_queries; ++q)
    {
        for (size_t i = 0; i < k; ++i)
        {
            const UInt32 row = found_neighbours[q * k + i];
            if (row == invalid_neighbour)
                continue;
            neighbours[q * limit + i] = row;
            distances[q * limit + i] = toDistance(metric, found_distances[q * k + i]);
        }
    }
}

namespace
{

/// A beam search over the graph for one query, as the single-CTA search of CAGRA does it on the device: a sorted list
/// of the best `candidates` rows seen so far, from which the best row not expanded yet is expanded - its neighbours in
/// the graph are measured and merged into the list - until all rows in the list are expanded. The list starts from rows
/// spread over the granule, as CAGRA starts from random samples.
class CpuBeamSearch
{
public:
    explicit CpuBeamSearch(const MergeTreeIndexGranuleCagra & granule_)
        : granule(granule_)
        , metric(granule.dimensions,
            granule.metric == CagraMetric::L2 ? unum::usearch::metric_kind_t::l2sq_k : unum::usearch::metric_kind_t::ip_k,
            unum::usearch::scalar_kind_t::f32_k)
    {
    }

    /// Writes up to `limit` rows and distances, best first, and returns how many.
    size_t search(const float * query, size_t limit, size_t candidates, UInt32 * rows_out, float * distances_out)
    {
        const size_t rows = granule.rows;
        const size_t list_size = std::max(candidates, limit);
        list.clear();
        visited.clear();

        /// The `hnsw` method returns `1 - dot` for the inner product and the squared L2 distance, and so does this metric.
        auto distance = [&](UInt32 row)
        {
            return static_cast<float>(metric(
                reinterpret_cast<const unum::usearch::byte_t *>(query),
                reinterpret_cast<const unum::usearch::byte_t *>(granule.dataset.data() + row * granule.dimensions)));
        };

        auto insert = [&](UInt32 row)
        {
            if (!visited.insert(row).second)
                return;
            const float d = distance(row);
            if (list.size() == list_size && d >= list.back().distance)
                return;
            Entry entry{d, row, false};
            list.insert(std::upper_bound(list.begin(), list.end(), entry), entry);
            if (list.size() > list_size)
                list.pop_back();
        };

        const size_t num_seeds = std::min(rows, list_size);
        const size_t step = rows / num_seeds;
        for (size_t i = 0; i < num_seeds; ++i)
            insert(static_cast<UInt32>(i * step + (i * 2654435761ULL) % step));

        const UInt32 * graph = granule.graph.data();
        const size_t degree = granule.graph_degree;
        while (true)
        {
            auto it = std::find_if(list.begin(), list.end(), [](const Entry & entry) { return !entry.expanded; });
            if (it == list.end())
                break;
            it->expanded = true;
            const UInt32 * neighbours = graph + static_cast<size_t>(it->row) * degree;
            for (size_t i = 0; i < degree; ++i)
                if (neighbours[i] < rows)
                    insert(neighbours[i]);
        }

        const size_t found = std::min(limit, list.size());
        for (size_t i = 0; i < found; ++i)
        {
            rows_out[i] = list[i].row;
            distances_out[i] = list[i].distance;
        }
        return found;
    }

private:
    struct Entry
    {
        float distance;
        UInt32 row;
        bool expanded;
        bool operator<(const Entry & other) const { return distance < other.distance; }
    };

    const MergeTreeIndexGranuleCagra & granule;
    unum::usearch::metric_punned_t metric;
    std::vector<Entry> list;
    HashSet<UInt32> visited;
};

}

NearestNeighbours MergeTreeIndexGranuleCagra::searchOnCpu(const std::vector<float> & query, size_t limit, size_t candidates, bool return_distances) const
{
    if (graph_degree == 0)
        return searchExact(query, limit, return_distances);

    std::vector<UInt32> rows_found(limit);
    std::vector<float> distances_found(limit);
    CpuBeamSearch beam_search(*this);
    const size_t found = beam_search.search(query.data(), limit, candidates, rows_found.data(), distances_found.data());

    NearestNeighbours result;
    result.rows.assign(rows_found.begin(), rows_found.begin() + found);
    if (return_distances)
        result.distances.emplace(distances_found.begin(), distances_found.begin() + found);
    return result;
}

void MergeTreeIndexGranuleCagra::searchBatchOnCpu(const std::vector<float> & queries, size_t num_queries, size_t limit, size_t candidates,
    std::vector<UInt32> & neighbours, std::vector<float> & distances) const
{
    if (graph_degree == 0)
    {
        searchBatch(queries, num_queries, limit, candidates, neighbours, distances);
        return;
    }

    neighbours.assign(num_queries * limit, invalid_neighbour);
    distances.assign(num_queries * limit, std::numeric_limits<float>::infinity());

    std::atomic<size_t> next_query = 0;
    auto work = [&]
    {
        CpuBeamSearch beam_search(*this);
        for (size_t q; (q = next_query.fetch_add(1)) < num_queries;)
            beam_search.search(queries.data() + q * dimensions, limit, candidates, neighbours.data() + q * limit, distances.data() + q * limit);
    };

    const size_t num_threads = std::min<size_t>(getNumberOfCPUCoresToUse(), num_queries);
    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (size_t t = 0; t < num_threads; ++t)
        threads.emplace_back(work);
    for (auto & thread : threads)
        thread.join();
}

MergeTreeIndexAggregatorCagra::MergeTreeIndexAggregatorCagra(
    const String & index_name_, const Block & index_sample_block_, size_t dimensions_, CagraMetric metric_, CagraParams params_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , dimensions(dimensions_)
    , metric(metric_)
    , params(params_)
{
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorCagra::getGranuleAndReset()
{
    const size_t rows = dataset.size() / dimensions;
    std::vector<UInt32> graph;
    size_t graph_degree = 0;

    if (rows >= min_rows_for_graph)
    {
        Stopwatch watch;
        graph = buildGraph(dataset, rows, dimensions, metric, params);
        graph_degree = params.graph_degree;
        LOG_DEBUG(logger, "Built CAGRA index {} of {} rows with {} dimensions on the device in {:.3f} s",
            backQuote(index_name), rows, dimensions, watch.elapsedSeconds());
    }

    auto granule = std::make_shared<MergeTreeIndexGranuleCagra>(index_name, metric, dimensions, std::move(dataset), std::move(graph), graph_degree);
    dataset = {};
    return granule;
}

namespace
{

template <typename Column>
void appendVectors(const IColumn & data, size_t begin, size_t end, std::vector<float> & dataset)
{
    const auto & values = typeid_cast<const Column &>(data).getData();
    for (size_t i = begin; i < end; ++i)
        dataset.push_back(static_cast<float>(values[i]));
}

}

void MergeTreeIndexAggregatorCagra::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "The provided position is not less than the number of block rows. Position: {}, Block rows: {}.", *pos, block.rows());

    const size_t rows_read = std::min(limit, block.rows() - *pos);
    if (rows_read == 0)
        return;

    if (dataset.size() / dimensions + rows_read > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Size of vector similarity index would exceed 4 billion entries");

    const auto & index_column_name = index_sample_block.getByPosition(0).name;
    const auto & column_with_type = block.getByName(index_column_name);
    const auto * column_array = typeid_cast<const ColumnArray *>(column_with_type.column.get());
    if (!column_array)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected Array(Float32|Float64|BFloat16) column");

    const auto & offsets = column_array->getOffsets();
    const size_t first = *pos;
    const size_t last = *pos + rows_read;
    for (size_t row = first; row < last; ++row)
        if (offsets[row] - offsets[row - 1] != dimensions)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Array values in column with vector similarity index have {} elements, expects {} elements", offsets[row] - offsets[row - 1], dimensions);

    /// `update` is called for every mark: no exact `reserve` here, which would copy the whole dataset each time.
    const size_t old_size = dataset.size();

    const auto & data = column_array->getData();
    const size_t begin = offsets[first - 1];
    const size_t end = offsets[last - 1];
    WhichDataType which(typeid_cast<const DataTypeArray &>(*column_with_type.type).getNestedType());
    if (which.isFloat32())
        appendVectors<ColumnFloat32>(data, begin, end, dataset);
    else if (which.isFloat64())
        appendVectors<ColumnFloat64>(data, begin, end, dataset);
    else if (which.isBFloat16())
        appendVectors<ColumnBFloat16>(data, begin, end, dataset);
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected data type Array(Float*)");

    for (size_t i = old_size; i < dataset.size(); i += dimensions)
    {
        checkFinite(dataset.data() + i, dimensions, ErrorCodes::INCORRECT_DATA, "indexed vector");
        if (metric == CagraMetric::Cosine)
            normalizeImpl(dataset.data() + i, dimensions);
    }

    *pos += rows_read;
}

MergeTreeIndexConditionCagra::MergeTreeIndexConditionCagra(
    const std::optional<VectorSearchParameters> & parameters_, const String & index_column_, CagraMetric metric_, ContextPtr context)
    : parameters(parameters_)
    , index_column(index_column_)
    , metric(metric_)
    , candidates(context->getSettingsRef()[Setting::hnsw_candidate_list_size_for_search])
    , index_fetch_multiplier(context->getSettingsRef()[Setting::vector_search_index_fetch_multiplier])
    , max_limit(context->getSettingsRef()[Setting::max_limit_for_vector_search_queries])
    , is_rescoring(context->getSettingsRef()[Setting::vector_search_with_rescoring])
    , on_cpu(context->getSettingsRef()[Setting::vector_search_cagra_on_cpu])
{
    if (candidates == 0)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Setting 'hnsw_candidate_list_size_for_search' must not be 0");
    if (!std::isfinite(index_fetch_multiplier) || index_fetch_multiplier < 1.0f || index_fetch_multiplier > 1000.0f)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
            "Setting 'vector_search_index_fetch_multiplier' must be greater or equal to 1 and less or equal to 1000");
}

bool MergeTreeIndexConditionCagra::mayBeTrueOnGranule(MergeTreeIndexGranulePtr, const UpdatePartialDisjunctionResultFn &) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "mayBeTrueOnGranule is not supported for vector similarity indexes");
}

bool MergeTreeIndexConditionCagra::alwaysUnknownOrTrue() const
{
    if (!parameters || parameters->column != index_column)
        return true;

    return !((parameters->distance_function == "L2Distance" && metric == CagraMetric::L2)
        || (parameters->distance_function == "cosineDistance" && metric == CagraMetric::Cosine)
        || (parameters->distance_function == "dotProduct" && metric == CagraMetric::Dot));
}

NearestNeighbours MergeTreeIndexConditionCagra::calculateApproximateNearestNeighbors(MergeTreeIndexGranulePtr granule_) const
{
    if (!parameters)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected vector_search_parameters to be set");

    const auto granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleCagra>(granule_);
    if (!granule)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Granule has the wrong type");

    if (parameters->reference_vector.size() != granule->dimensions)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The dimension of the reference vector in the query ({}) does not match the dimension in the index ({})",
            parameters->reference_vector.size(), granule->dimensions);

    std::vector<float> query(parameters->reference_vector.begin(), parameters->reference_vector.end());
    checkFinite(query.data(), query.size(), ErrorCodes::INCORRECT_QUERY, "reference vector in the SELECT query");
    if (metric == CagraMetric::Cosine)
        normalizeImpl(query.data(), query.size());

    size_t limit = parameters->limit;
    if (parameters->additional_filters_present || is_rescoring)
    {
        const double scaled_limit = static_cast<double>(limit) * static_cast<double>(index_fetch_multiplier);
        limit = (scaled_limit >= static_cast<double>(max_limit)) ? max_limit : static_cast<size_t>(scaled_limit);
    }

    if (on_cpu)
        return granule->searchOnCpu(query, limit, candidates, parameters->return_distances);
    return granule->search(query, limit, candidates, parameters->return_distances);
}

MergeTreeIndexCagra::MergeTreeIndexCagra(
    StorageMetadataPtr metadata_snapshot_, const IndexDescription & index_, size_t dimensions_, CagraMetric metric_, CagraParams params_)
    : IMergeTreeIndex(std::move(metadata_snapshot_), index_)
    , dimensions(dimensions_)
    , metric(metric_)
    , params(params_)
{
}

MergeTreeIndexGranulePtr MergeTreeIndexCagra::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleCagra>(index.name, metric, dimensions);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexCagra::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorCagra>(index.name, index.sample_block, dimensions, metric, params);
}

MergeTreeIndexConditionPtr MergeTreeIndexCagra::createIndexCondition(const ActionsDAG::Node *, ContextPtr) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function not supported for vector similarity index");
}

MergeTreeIndexConditionPtr MergeTreeIndexCagra::createIndexCondition(
    const ActionsDAG::Node *, ContextPtr context, const std::optional<VectorSearchParameters> & parameters) const
{
    return std::make_shared<MergeTreeIndexConditionCagra>(parameters, index.column_names[0], metric, context);
}

namespace
{

CagraMetric parseMetric(const String & distance_function)
{
    if (distance_function == "L2Distance")
        return CagraMetric::L2;
    if (distance_function == "cosineDistance")
        return CagraMetric::Cosine;
    if (distance_function == "dotProduct")
        return CagraMetric::Dot;
    throw Exception(ErrorCodes::INCORRECT_DATA,
        "Second argument (distance function) of vector similarity index is not supported. Supported distance function are: L2Distance, cosineDistance, dotProduct");
}

}

MergeTreeIndexPtr cagraIndexCreator(StorageMetadataPtr metadata_snapshot, const IndexDescription & index, const FieldVector & args)
{
    CagraParams params;
    if (args.size() == 5)
        params = {.graph_degree = args[3].safeGet<UInt64>(), .intermediate_graph_degree = args[4].safeGet<UInt64>()};
    return std::make_shared<MergeTreeIndexCagra>(
        std::move(metadata_snapshot), index, args[2].safeGet<UInt64>(), parseMetric(args[1].safeGet<String>()), params);
}

void cagraIndexValidator(const IndexDescription & index, const FieldVector & args)
{
    if (args.size() != 3 && args.size() != 5)
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "Vector similarity index with method 'cagra' must have three or five arguments: method, distance function, dimensions[, graph_degree, intermediate_graph_degree]");
    if (args[1].getType() != Field::Types::String)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Second argument of vector similarity index (metric) must be of type String");
    if (args[2].getType() != Field::Types::UInt64)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Third argument of vector similarity index (dimensions) must be of type UInt64");
    parseMetric(args[1].safeGet<String>());
    if (args[2].safeGet<UInt64>() == 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Third argument (dimensions) of vector similarity index must be > 0");

    if (args.size() == 5)
    {
        if (args[3].getType() != Field::Types::UInt64 || args[4].getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Arguments graph_degree and intermediate_graph_degree of vector similarity index must be of type UInt64");
        const UInt64 graph_degree = args[3].safeGet<UInt64>();
        const UInt64 intermediate_graph_degree = args[4].safeGet<UInt64>();
        if (graph_degree == 0 || graph_degree > 1024 || intermediate_graph_degree < graph_degree || intermediate_graph_degree > 1024)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Vector similarity index with method 'cagra' needs 0 < graph_degree <= intermediate_graph_degree <= 1024");
    }

    if (index.column_names.size() != 1 || index.data_types.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Vector similarity index must be created on a single column");

    const auto * data_type_array = typeid_cast<const DataTypeArray *>(index.sample_block.getDataTypes()[0].get());
    if (!data_type_array)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Vector similarity index can only be created on columns of type Array(Float32|Float64|BFloat16)");
    WhichDataType which(data_type_array->getNestedType());
    if (!which.isNativeFloat() && !which.isBFloat16())
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Vector similarity index can only be created on columns of type Array(Float32|Float64|BFloat16)");
}

}

#endif
