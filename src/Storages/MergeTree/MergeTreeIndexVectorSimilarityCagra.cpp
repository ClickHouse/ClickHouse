#include <Storages/MergeTree/MergeTreeIndexVectorSimilarityCagra.h>

#if USE_GPU

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <GPU/CuvsCagra.cuh>
#include <GPU/GPUDevice.h>
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

#include <usearch/index_plugins.hpp>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <string_view>
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
    extern const int ILLEGAL_COLUMN;
    extern const int INCORRECT_DATA;
    extern const int INCORRECT_QUERY;
    extern const int INVALID_SETTING_VALUE;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

/// Below this, a granule has no graph and is searched exactly: see the comment in the header.
constexpr size_t min_rows_for_graph = 10000;

/// What CAGRA marks a missing neighbour with, when fewer than `limit` were found.
constexpr UInt32 invalid_neighbour = std::numeric_limits<UInt32>::max();

/// The `pq_len` of a quantization of the `cagra` method: 0 for `f32`.
size_t parseQuantization(const String & quantization)
{
    if (quantization == "f32")
        return 0;
    if (quantization == "pq2")
        return 2;
    if (quantization == "pq4")
        return 4;
    if (quantization == "pq8")
        return 8;
    throw Exception(ErrorCodes::INCORRECT_DATA,
        "Fourth argument (quantization) of vector similarity index with method 'cagra' is not supported. Supported quantizations are: f32, pq2, pq4, pq8");
}

GPU::CagraDistance toCagraDistance(CagraMetric metric)
{
    return metric == CagraMetric::L2 ? GPU::CagraDistance::L2 : GPU::CagraDistance::InnerProduct;
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

void checkFinite(const float * vector, size_t dimensions, int error_code, std::string_view context)
{
    for (size_t i = 0; i < dimensions; ++i)
        if (!std::isfinite(vector[i]))
            throw Exception(error_code, "Vector for vector similarity index ({}) must not contain non-finite values (NaN or Inf)", context);
}

/// cuVS runs on the device of the `GPUDevice`, and allocates from its memory pool.
void checkCanUseDevice()
{
    GPU::GPUDevice::instance();

    /// When a thread exits, glibc trims its cache of stacks (40 MiB by default) and frees the DTVs of the evicted ones -
    /// in the exiting thread, after jemalloc has torn down its state of that thread. jemalloc then initializes the thread
    /// anew and puts a new tcache, which lives in the TLS of the thread, into the list of an arena; the stack goes away
    /// and the next walk of that list crashes. The threads of OpenMP that cuVS starts during a build make the cache
    /// overflow. A large enough cache never trims.
    const char * tunables = std::getenv("GLIBC_TUNABLES"); // NOLINT(concurrency-mt-unsafe)
    if (!tunables || !std::string_view(tunables).contains("glibc.pthread.stack_cache_size="))
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "The `cagra` method of the vector similarity index needs the server to be started with the environment variable "
            "`GLIBC_TUNABLES=glibc.pthread.stack_cache_size=17179869184`");
}

/// Builds the graph of `rows` vectors of `dataset` on the device and returns it, `rows * graph_degree` neighbours.
std::vector<UInt32> buildGraph(const std::vector<float> & dataset, size_t rows, size_t dimensions, CagraMetric metric, CagraParams params)
{
    checkCanUseDevice();
    std::vector<UInt32> graph(rows * params.graph_degree);
    GPU::buildCagraGraph(dataset.data(), rows, dimensions, toCagraDistance(metric),
        params.graph_degree, params.intermediate_graph_degree, graph.data());
    return graph;
}

}

/// The graph and the vectors of a granule on the device, ready to search.
class CagraDeviceIndex : public GPU::CagraIndex
{
public:
    using GPU::CagraIndex::CagraIndex;
};

MergeTreeIndexGranuleCagra::MergeTreeIndexGranuleCagra(const String & index_name_, CagraMetric metric_, size_t dimensions_)
    : index_name(index_name_)
    , metric(metric_)
    , dimensions(dimensions_)
{
}

MergeTreeIndexGranuleCagra::MergeTreeIndexGranuleCagra(
    const String & index_name_, CagraMetric metric_, size_t dimensions_, size_t rows_,
    std::vector<float> && dataset_, std::optional<CagraCompressedVectors> && compressed_,
    std::vector<UInt32> && graph_, size_t graph_degree_)
    : index_name(index_name_)
    , metric(metric_)
    , dimensions(dimensions_)
    , rows(rows_)
    , graph_degree(graph_degree_)
    , dataset(std::move(dataset_))
    , compressed(std::move(compressed_))
    , graph(std::move(graph_))
{
}

void CagraCompressedVectors::decompress(size_t row, size_t dimensions, float * vector) const
{
    const uint8_t * encoded = codes.data() + row * row_bytes;
    UInt32 center = 0;
    memcpy(&center, encoded, sizeof(center));
    const float * vq = vq_codebook.data() + static_cast<size_t>(center) * dimensions;
    const uint8_t * pq_codes = encoded + sizeof(center);
    for (size_t piece = 0; piece < dimensions / pq_len; ++piece)
    {
        const float * pq = pq_codebook.data() + static_cast<size_t>(pq_codes[piece]) * pq_len;
        for (size_t i = 0; i < pq_len; ++i)
            vector[piece * pq_len + i] = vq[piece * pq_len + i] + pq[i];
    }
}

size_t CagraCompressedVectors::memoryUsageBytes() const
{
    return (vq_codebook.size() + pq_codebook.size()) * sizeof(float) + codes.size();
}

float MergeTreeIndexGranuleCagra::toDistance(float cuvs_distance) const
{
    /// The squared L2 distance of normalized vectors is twice their cosine distance.
    if (compressed)
        return metric == CagraMetric::Cosine ? cuvs_distance / 2 : cuvs_distance;
    return metric == CagraMetric::L2 ? cuvs_distance : 1.0f - cuvs_distance;
}

MergeTreeIndexGranuleCagra::~MergeTreeIndexGranuleCagra() = default;

size_t MergeTreeIndexGranuleCagra::memoryUsageBytes() const
{
    return dataset.size() * sizeof(float) + (compressed ? compressed->memoryUsageBytes() : 0) + graph.size() * sizeof(UInt32);
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
    writeIntBinary(static_cast<UInt64>(compressed ? compressed->pq_len : 0), ostr);
    if (compressed)
    {
        writeIntBinary(static_cast<UInt64>(compressed->vq_centers), ostr);
        writeIntBinary(static_cast<UInt64>(compressed->row_bytes), ostr);
        ostr.write(reinterpret_cast<const char *>(compressed->vq_codebook.data()), compressed->vq_codebook.size() * sizeof(float));
        ostr.write(reinterpret_cast<const char *>(compressed->pq_codebook.data()), compressed->pq_codebook.size() * sizeof(float));
        ostr.write(reinterpret_cast<const char *>(compressed->codes.data()), compressed->codes.size());
    }
    else
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
    UInt64 persisted_pq_len = 0;
    readIntBinary(persisted_dimensions, istr);
    readIntBinary(persisted_metric, istr);
    readIntBinary(persisted_rows, istr);
    readIntBinary(persisted_degree, istr);
    readIntBinary(persisted_pq_len, istr);

    if (persisted_dimensions != dimensions || persisted_metric != static_cast<UInt8>(metric))
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Vector similarity index {} was written with {} dimensions and metric {}, but the index is declared with {} dimensions and metric {}",
            backQuote(index_name), persisted_dimensions, static_cast<UInt32>(persisted_metric), dimensions, static_cast<UInt32>(metric));

    rows = persisted_rows;
    graph_degree = persisted_degree;
    compressed.reset();
    dataset.clear();
    if (persisted_pq_len != 0)
    {
        auto & vectors = compressed.emplace();
        vectors.pq_len = persisted_pq_len;
        UInt64 persisted_vq_centers = 0;
        UInt64 persisted_row_bytes = 0;
        readIntBinary(persisted_vq_centers, istr);
        readIntBinary(persisted_row_bytes, istr);
        const bool supported_pq_len = vectors.pq_len == 2 || vectors.pq_len == 4 || vectors.pq_len == 8;
        if (!supported_pq_len || graph_degree == 0 || dimensions % vectors.pq_len != 0 || persisted_vq_centers == 0
            || persisted_row_bytes < sizeof(UInt32) + dimensions / vectors.pq_len)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Vector similarity index {} has compressed vectors of an invalid shape: {} components to a byte, {} VQ centers, "
                "rows of {} bytes, graph degree {}", backQuote(index_name), vectors.pq_len, persisted_vq_centers, persisted_row_bytes, graph_degree);
        vectors.vq_centers = persisted_vq_centers;
        vectors.row_bytes = persisted_row_bytes;
        vectors.vq_codebook.resize(vectors.vq_centers * dimensions);
        vectors.pq_codebook.resize(GPU::cagra_pq_codebook_rows * vectors.pq_len);
        vectors.codes.resize(rows * vectors.row_bytes);
        istr.readStrict(reinterpret_cast<char *>(vectors.vq_codebook.data()), vectors.vq_codebook.size() * sizeof(float));
        istr.readStrict(reinterpret_cast<char *>(vectors.pq_codebook.data()), vectors.pq_codebook.size() * sizeof(float));
        istr.readStrict(reinterpret_cast<char *>(vectors.codes.data()), vectors.codes.size());

        /// `decompress` and the device index rely on the VQ centers of the rows being in the codebook.
        for (size_t row = 0; row < rows; ++row)
        {
            UInt32 center = 0;
            memcpy(&center, vectors.codes.data() + row * vectors.row_bytes, sizeof(center));
            if (center >= vectors.vq_centers)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Vector similarity index {} has a row {} of the VQ center {} out of {}",
                    backQuote(index_name), row, center, vectors.vq_centers);
        }
    }
    else
    {
        dataset.resize(rows * dimensions);
        istr.readStrict(reinterpret_cast<char *>(dataset.data()), dataset.size() * sizeof(float));
    }
    graph.resize(rows * graph_degree);
    istr.readStrict(reinterpret_cast<char *>(graph.data()), graph.size() * sizeof(UInt32));
}

std::shared_ptr<CagraDeviceIndex> MergeTreeIndexGranuleCagra::getDeviceIndex() const
{
    std::lock_guard lock(device_mutex);
    if (!device_index)
    {
        Stopwatch watch;
        checkCanUseDevice();
        if (compressed)
            device_index = std::make_shared<CagraDeviceIndex>(
                compressed->vq_codebook.data(), compressed->vq_centers, compressed->pq_codebook.data(), compressed->pq_len,
                compressed->codes.data(), compressed->row_bytes, graph.data(), rows, dimensions, graph_degree);
        else
            device_index = std::make_shared<CagraDeviceIndex>(dataset.data(), graph.data(), rows, dimensions, graph_degree, toCagraDistance(metric));
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

    device->search(query.data(), 1, k, candidates, neighbours.data(), distances.data());

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
            result.distances->push_back(toDistance(distances[i]));
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

    device->search(queries.data(), num_queries, k, candidates, found_neighbours.data(), found_distances.data());

    for (size_t q = 0; q < num_queries; ++q)
    {
        for (size_t i = 0; i < k; ++i)
        {
            const UInt32 row = found_neighbours[q * k + i];
            if (row == invalid_neighbour)
                continue;
            neighbours[q * limit + i] = row;
            distances[q * limit + i] = toDistance(found_distances[q * k + i]);
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
        /// Compressed vectors are searched by the squared L2 distance, as CAGRA-Q does.
        , metric(granule.dimensions,
            granule.metric == CagraMetric::L2 || granule.compressed ? unum::usearch::metric_kind_t::l2sq_k : unum::usearch::metric_kind_t::ip_k,
            unum::usearch::scalar_kind_t::f32_k)
        , decompressed(granule.compressed ? granule.dimensions : 0)
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
            const float * vector = granule.dataset.data() + static_cast<size_t>(row) * granule.dimensions;
            if (granule.compressed)
            {
                granule.compressed->decompress(row, granule.dimensions, decompressed.data());
                vector = decompressed.data();
            }
            return static_cast<float>(metric(
                reinterpret_cast<const unum::usearch::byte_t *>(query), reinterpret_cast<const unum::usearch::byte_t *>(vector)));
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
            distances_out[i] = granule.compressed ? granule.toDistance(list[i].distance) : list[i].distance;
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
    /// The vector of the row being measured, for compressed vectors.
    std::vector<float> decompressed;
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
    std::optional<CagraCompressedVectors> compressed;

    if (rows >= min_rows_for_graph)
    {
        Stopwatch watch;
        graph = buildGraph(dataset, rows, dimensions, metric, params);
        graph_degree = params.graph_degree;
        LOG_DEBUG(logger, "Built CAGRA index {} of {} rows with {} dimensions on the device in {:.3f} s",
            backQuote(index_name), rows, dimensions, watch.elapsedSeconds());

        if (params.pq_len != 0)
        {
            watch.restart();
            GPU::CagraCompressedDataset compressed_dataset(dataset.data(), rows, dimensions, params.pq_len);
            auto & vectors = compressed.emplace();
            vectors.pq_len = params.pq_len;
            vectors.vq_centers = compressed_dataset.vqCenters();
            vectors.row_bytes = compressed_dataset.rowBytes();
            vectors.vq_codebook.resize(vectors.vq_centers * dimensions);
            vectors.pq_codebook.resize(GPU::cagra_pq_codebook_rows * vectors.pq_len);
            vectors.codes.resize(rows * vectors.row_bytes);
            compressed_dataset.copyToHost(vectors.vq_codebook.data(), vectors.pq_codebook.data(), vectors.codes.data());
            dataset = {};
            LOG_DEBUG(logger, "Compressed {} vectors of CAGRA index {} to {} bytes each ({} VQ centers) on the device in {:.3f} s",
                rows, backQuote(index_name), vectors.row_bytes, vectors.vq_centers, watch.elapsedSeconds());
        }
    }

    auto granule = std::make_shared<MergeTreeIndexGranuleCagra>(
        index_name, metric, dimensions, rows, std::move(dataset), std::move(compressed), std::move(graph), graph_degree);
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
    if (args.size() >= 4)
        params.pq_len = parseQuantization(args[3].safeGet<String>());
    if (args.size() == 6)
    {
        params.graph_degree = args[4].safeGet<UInt64>();
        params.intermediate_graph_degree = args[5].safeGet<UInt64>();
    }
    return std::make_shared<MergeTreeIndexCagra>(
        std::move(metadata_snapshot), index, args[2].safeGet<UInt64>(), parseMetric(args[1].safeGet<String>()), params);
}

void cagraIndexValidator(const IndexDescription & index, const FieldVector & args)
{
    if (args.size() != 3 && args.size() != 4 && args.size() != 6)
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "Vector similarity index with method 'cagra' must have three, four or six arguments: "
            "method, distance function, dimensions[, quantization[, graph_degree, intermediate_graph_degree]]");
    if (args[1].getType() != Field::Types::String)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Second argument of vector similarity index (metric) must be of type String");
    if (args[2].getType() != Field::Types::UInt64)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Third argument of vector similarity index (dimensions) must be of type UInt64");
    const CagraMetric metric = parseMetric(args[1].safeGet<String>());
    const UInt64 dimensions = args[2].safeGet<UInt64>();
    if (dimensions == 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Third argument (dimensions) of vector similarity index must be > 0");

    if (args.size() >= 4)
    {
        if (args[3].getType() != Field::Types::String)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Fourth argument of vector similarity index (quantization) must be of type String");
        const size_t pq_len = parseQuantization(args[3].safeGet<String>());
        if (pq_len != 0 && metric == CagraMetric::Dot)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Quantization {} of vector similarity index with method 'cagra' supports only the distance functions L2Distance and cosineDistance",
                args[3].safeGet<String>());
        if (pq_len != 0 && dimensions % pq_len != 0)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Quantization {} of vector similarity index with method 'cagra' needs the dimensions to be a multiple of {}, but they are {}",
                args[3].safeGet<String>(), pq_len, dimensions);
    }

    if (args.size() == 6)
    {
        if (args[4].getType() != Field::Types::UInt64 || args[5].getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Arguments graph_degree and intermediate_graph_degree of vector similarity index must be of type UInt64");
        const UInt64 graph_degree = args[4].safeGet<UInt64>();
        const UInt64 intermediate_graph_degree = args[5].safeGet<UInt64>();
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
