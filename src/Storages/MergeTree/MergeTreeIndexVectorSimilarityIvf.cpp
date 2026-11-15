#include <Storages/MergeTree/MergeTreeIndexVectorSimilarityIvf.h>

#if USE_GPU

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <GPU/CuvsIvf.cuh>
#include <GPU/IvfQuantization.cuh>
#include <GPU/GPUDevice.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <Interpreters/Context.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Common/Stopwatch.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdlib>
#include <mutex>
#include <optional>
#include <random>
#include <string_view>
#include <thread>

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 vector_search_ivf_probes;
    extern const SettingsFloat vector_search_index_fetch_multiplier;
    extern const SettingsUInt64 max_limit_for_vector_search_queries;
    extern const SettingsBool vector_search_with_rescoring;
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
constexpr UInt32 invalid_neighbour = std::numeric_limits<UInt32>::max();

constexpr std::string_view lists_substream_suffix = ".lst";
constexpr std::string_view vectors_substream_suffix = ".vec";

constexpr size_t rescoring_factor = 4;

constexpr size_t max_sample_bytes = 512 << 20;
constexpr size_t assign_block_bytes = 32 << 20;
constexpr size_t max_bucket_bytes = 256 << 20;

TemporaryDataOnDiskScopePtr temporaryData()
{
    auto scope = Context::getGlobalContextInstance()->getTempDataOnDisk();
    if (!scope)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The `ivf` method of the vector similarity index needs temporary data on disk to be built");
    return scope;
}

void checkFinite(const float * vector, size_t dimensions, int error_code, std::string_view context)
{
    for (size_t i = 0; i < dimensions; ++i)
        if (!std::isfinite(vector[i]))
            throw Exception(error_code, "Vector for vector similarity index ({}) must not contain non-finite values (NaN or Inf)", context);
}

void checkCanUseDevice()
{
    GPU::GPUDevice::instance();

    /// The OpenMP threads of cuVS overflow the glibc stack cache, and trimming it crashes jemalloc; a large cache never trims.
    const char * tunables = std::getenv("GLIBC_TUNABLES"); // NOLINT(concurrency-mt-unsafe)
    if (!tunables || !std::string_view(tunables).contains("glibc.pthread.stack_cache_size="))
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "The `ivf` method of the vector similarity index needs the server to be started with the environment variable "
            "`GLIBC_TUNABLES=glibc.pthread.stack_cache_size=17179869184`");
}

template <typename Work>
void runOnCores(size_t num_tasks, Work && work)
{
    const size_t num_threads = std::min<size_t>(getNumberOfCPUCoresToUse(), num_tasks);
    if (num_threads <= 1)
    {
        for (size_t task = 0; task < num_tasks; ++task)
            work(task);
        return;
    }
    std::atomic<size_t> next = 0;
    std::exception_ptr exception;
    std::mutex mutex;
    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (size_t t = 0; t < num_threads; ++t)
        threads.emplace_back([&]
        {
            try
            {
                for (size_t task; (task = next.fetch_add(1)) < num_tasks;)
                    work(task);
            }
            catch (...)
            {
                std::lock_guard lock(mutex);
                if (!exception)
                    exception = std::current_exception();
            }
        });
    for (auto & thread : threads)
        thread.join();
    if (exception)
        std::rethrow_exception(exception);
}

std::vector<float> makeRotation(size_t dimensions, UInt64 seed)
{
    pcg64 rng(seed);
    std::normal_distribution<double> gaussian;
    std::vector<double> rows(dimensions * dimensions);
    for (auto & value : rows)
        value = gaussian(rng);
    for (size_t i = 0; i < dimensions; ++i)
    {
        double * row = rows.data() + i * dimensions;
        for (size_t j = 0; j < i; ++j)
        {
            const double * other = rows.data() + j * dimensions;
            double dot = 0;
            for (size_t k = 0; k < dimensions; ++k)
                dot += row[k] * other[k];
            for (size_t k = 0; k < dimensions; ++k)
                row[k] -= dot * other[k];
        }
        double norm = 0;
        for (size_t k = 0; k < dimensions; ++k)
            norm += row[k] * row[k];
        const double inverse = 1 / std::sqrt(norm);
        for (size_t k = 0; k < dimensions; ++k)
            row[k] *= inverse;
    }
    return std::vector<float>(rows.begin(), rows.end());
}

std::vector<float> rotateCentroids(const std::vector<float> & rotation, const std::vector<float> & centroids, size_t lists, size_t dimensions)
{
    std::vector<float> rotated(lists * dimensions, 0);
    runOnCores((lists + 255) / 256, [&](size_t task)
    {
        for (size_t l = task * 256; l < std::min(lists, (task + 1) * 256); ++l)
            for (size_t i = 0; i < dimensions; ++i)
            {
                float sum = 0;
                for (size_t k = 0; k < dimensions; ++k)
                    sum += rotation[i * dimensions + k] * centroids[l * dimensions + k];
                rotated[l * dimensions + i] = sum;
            }
    });
    return rotated;
}

}

void normalizeIvfVector(float * vector, size_t dimensions)
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

MergeTreeIndexGranuleIvf::MergeTreeIndexGranuleIvf(const String & index_name_, IvfMetric metric_, size_t dimensions_)
    : index_name(index_name_)
    , metric(metric_)
    , dimensions(dimensions_)
{
}

MergeTreeIndexGranuleIvf::~MergeTreeIndexGranuleIvf() = default;

size_t MergeTreeIndexGranuleIvf::memoryUsageBytes() const
{
    return (centroids.size() + rotation.size() + rotated_centroids.size()) * sizeof(float) + list_offsets.size() * sizeof(UInt64);
}

size_t MergeTreeIndexGranuleIvf::entryBytes() const
{
    return GPU::IvfQuantization::entryBytes(dimensions);
}

void MergeTreeIndexGranuleIvf::serializeBinary(WriteBuffer &) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Vector similarity index {} of method 'ivf' is written to three substreams", backQuote(index_name));
}

void MergeTreeIndexGranuleIvf::serializeBinaryWithMultipleStreams(MergeTreeIndexOutputStreams & streams) const
{
    if (empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to write empty vector similarity index {}", backQuote(index_name));
    if (buckets.empty() || !vectors)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Vector similarity index {} is written without its lists", backQuote(index_name));

    auto & header = streams.at(MergeTreeIndexSubstream::Type::Regular)->compressed_hashing;
    auto & lists_stream = streams.at(MergeTreeIndexSubstream::Type::VectorIndexLists)->plain_hashing;
    auto & vectors_stream = streams.at(MergeTreeIndexSubstream::Type::VectorIndexVectors)->plain_hashing;

    writeIntBinary(FILE_FORMAT_VERSION, header);
    writeIntBinary(static_cast<UInt64>(dimensions), header);
    writeIntBinary(static_cast<UInt8>(metric), header);
    writeIntBinary(static_cast<UInt64>(rows), header);
    writeIntBinary(static_cast<UInt64>(lists), header);
    writeIntBinary(static_cast<UInt64>(lists_stream.count()), header);
    writeIntBinary(static_cast<UInt64>(vectors_stream.count()), header);
    header.write(reinterpret_cast<const char *>(centroids.data()), centroids.size() * sizeof(float));
    header.write(reinterpret_cast<const char *>(rotation.data()), rotation.size() * sizeof(float));
    header.write(reinterpret_cast<const char *>(list_offsets.data()), list_offsets.size() * sizeof(UInt64));

    const size_t code_bytes = GPU::IvfQuantization::codeBytes(dimensions);
    const size_t params_bytes = sizeof(GPU::IvfQuantization::CodeParams);
    const size_t record_bytes = 2 * sizeof(UInt32) + params_bytes + code_bytes;
    const size_t entry_bytes = entryBytes();
    std::vector<char> records;
    std::vector<char> sorted;
    std::vector<UInt64> cursor;
    for (const auto & bucket : buckets)
    {
        const UInt64 base = list_offsets[bucket.first_list];
        const size_t bucket_entries = list_offsets[bucket.end_list] - base;
        records.resize(bucket_entries * record_bytes);
        bucket.records->read()->readStrict(records.data(), records.size());

        sorted.resize(bucket_entries * entry_bytes);
        cursor.assign(bucket.end_list - bucket.first_list, 0);
        for (size_t i = 0; i < bucket_entries; ++i)
        {
            const char * record = records.data() + i * record_bytes;
            UInt32 list = 0;
            memcpy(&list, record, sizeof(list));
            if (list < bucket.first_list || list >= bucket.end_list)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Vector similarity index {} has an entry of the list {} in a bucket of the lists [{}, {})",
                    backQuote(index_name), list, bucket.first_list, bucket.end_list);
            const size_t size = list_offsets[list + 1] - list_offsets[list];
            const size_t index = cursor[list - bucket.first_list]++;
            char * region = sorted.data() + (list_offsets[list] - base) * entry_bytes;
            memcpy(region + index * sizeof(UInt32), record + sizeof(UInt32), sizeof(UInt32));
            memcpy(region + size * sizeof(UInt32) + index * params_bytes, record + 2 * sizeof(UInt32), params_bytes);
            memcpy(region + size * (sizeof(UInt32) + params_bytes) + index * code_bytes, record + 2 * sizeof(UInt32) + params_bytes, code_bytes);
        }
        lists_stream.write(sorted.data(), sorted.size());
    }

    auto in = vectors->read();
    copyData(*in, vectors_stream);
}

void MergeTreeIndexGranuleIvf::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion /*version*/)
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
    UInt64 persisted_lists = 0;
    readIntBinary(persisted_dimensions, istr);
    readIntBinary(persisted_metric, istr);
    readIntBinary(persisted_rows, istr);
    readIntBinary(persisted_lists, istr);
    readIntBinary(lists_byte_begin, istr);
    readIntBinary(vectors_byte_begin, istr);

    if (persisted_dimensions != dimensions || persisted_metric != static_cast<UInt8>(metric))
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Vector similarity index {} was written with {} dimensions and metric {}, but the index is declared with {} dimensions and metric {}",
            backQuote(index_name), persisted_dimensions, static_cast<UInt32>(persisted_metric), dimensions, static_cast<UInt32>(metric));
    if (persisted_lists == 0 || persisted_lists > persisted_rows || persisted_rows > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Vector similarity index {} has an invalid shape: {} rows, {} lists",
            backQuote(index_name), persisted_rows, persisted_lists);

    rows = persisted_rows;
    lists = persisted_lists;
    centroids.resize(lists * dimensions);
    rotation.resize(dimensions * dimensions);
    list_offsets.resize(lists + 1);
    istr.readStrict(reinterpret_cast<char *>(centroids.data()), centroids.size() * sizeof(float));
    istr.readStrict(reinterpret_cast<char *>(rotation.data()), rotation.size() * sizeof(float));
    istr.readStrict(reinterpret_cast<char *>(list_offsets.data()), list_offsets.size() * sizeof(UInt64));
    buckets.clear();
    vectors.reset();
    rotated_centroids = rotateCentroids(rotation, centroids, lists, dimensions);

    if (list_offsets[0] != 0 || !std::is_sorted(list_offsets.begin(), list_offsets.end()) || entries() != rows)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Vector similarity index {} has invalid list offsets", backQuote(index_name));
}

void MergeTreeIndexGranuleIvf::deserializeBinaryWithMultipleStreams(MergeTreeIndexInputStreams & streams, MergeTreeIndexDeserializationState & state)
{
    deserializeBinary(*streams.at(MergeTreeIndexSubstream::Type::Regular)->getDataBuffer(), state.version);
    data_part_storage = state.part_info.getDataPartStorage();
    index_file_name = state.index.getFileName();
}

std::shared_ptr<GPU::IvfCentroids> MergeTreeIndexGranuleIvf::getDeviceCentroids() const
{
    std::lock_guard lock(device_mutex);
    if (!device_centroids)
    {
        checkCanUseDevice();
        device_centroids = std::make_shared<GPU::IvfCentroids>(centroids.data(), lists, dimensions);
    }
    return device_centroids;
}

namespace
{
constexpr size_t list_chunk_bytes = 256 << 20;

struct ListChunk
{
    std::vector<char> bytes;
    std::vector<UInt64> begins;
    std::vector<UInt32> sizes;
    std::vector<UInt32> query_list_offsets;
    std::vector<UInt32> query_lists;
};

}

NearestNeighbours MergeTreeIndexGranuleIvf::search(const std::vector<float> & query, size_t limit, size_t probes,
    const MergeTreeReaderSettings & reader_settings, bool return_distances) const
{
    std::vector<UInt32> neighbours;
    std::vector<float> distances;
    searchBatch(query, 1, limit, probes, reader_settings, neighbours, distances);

    NearestNeighbours result;
    result.rows.reserve(limit);
    if (return_distances)
        result.distances.emplace().reserve(limit);
    for (size_t i = 0; i < limit && neighbours[i] != invalid_neighbour; ++i)
    {
        result.rows.push_back(neighbours[i]);
        if (return_distances)
            result.distances->push_back(distances[i]);
    }
    return result;
}

void MergeTreeIndexGranuleIvf::searchBatch(const std::vector<float> & queries, size_t num_queries, size_t limit, size_t probes,
    const MergeTreeReaderSettings & reader_settings, std::vector<UInt32> & neighbours, std::vector<float> & distances) const
{
    neighbours.assign(num_queries * limit, invalid_neighbour);
    distances.assign(num_queries * limit, std::numeric_limits<float>::infinity());
    if (empty() || num_queries == 0 || limit == 0)
        return;
    if (!data_part_storage)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Vector similarity index {} is searched without the storage of its lists", backQuote(index_name));

    Stopwatch watch;
    const size_t num_probes = std::clamp<size_t>(probes, 1, lists);
    if (limit > GPU::IvfListScan::maxSelect())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Vector similarity index {} of method 'ivf' returns at most {} rows per query",
            backQuote(index_name), GPU::IvfListScan::maxSelect());
    const size_t select = std::min({limit * rescoring_factor, GPU::IvfListScan::maxSelect(), rows});

    std::vector<UInt32> probed(num_queries * num_probes);
    getDeviceCentroids()->nearestLists(queries.data(), num_queries, num_probes, probed.data());

    std::vector<UInt8> needed(lists, 0);
    for (UInt32 list : probed)
        needed[list] = 1;
    std::vector<UInt32> order;
    for (size_t l = 0; l < lists; ++l)
        if (needed[l] && list_offsets[l + 1] > list_offsets[l])
            order.push_back(static_cast<UInt32>(l));

    const String lists_stream_name = index_file_name + String(lists_substream_suffix);
    const auto lists_file_name = IMergeTreeDataPart::getStreamNameOrHash(lists_stream_name, ".idx", *data_part_storage);
    if (!lists_file_name)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Vector similarity index {} has no file of lists {}.idx", backQuote(index_name), lists_stream_name);
    ReadSettings index_read_settings = reader_settings.read_settings;
    index_read_settings.local_fs_settings.method = LocalFSReadMethod::pread;
    auto lists_file = data_part_storage->readFile(*lists_file_name + ".idx", index_read_settings, std::nullopt);
    const bool read_at = lists_file->supportsReadAt();
    auto read_range = [&](char * to, size_t offset, size_t size)
    {
        if (read_at)
        {
            if (lists_file->readBigAt(to, size, offset, nullptr) != size)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Vector similarity index {} has a truncated file of lists", backQuote(index_name));
            return;
        }
        lists_file->seek(offset, SEEK_SET);
        lists_file->readStrict(to, size);
    };

    GPU::IvfListScan scan(queries.data(), num_queries, dimensions, select, rotation.data());
    std::vector<float> chunk_rotated_centroids;

    const size_t entry_bytes = entryBytes();
    std::vector<Int32> chunk_position(lists, -1);
    ListChunk chunk;
    size_t bytes_read = 0;
    for (size_t first = 0; first < order.size();)
    {
        size_t last = first;
        size_t chunk_size = 0;
        while (last < order.size())
        {
            const size_t list_bytes = (list_offsets[order[last] + 1] - list_offsets[order[last]]) * entry_bytes;
            if (last > first && chunk_size + list_bytes > list_chunk_bytes)
                break;
            chunk_size += list_bytes;
            ++last;
        }

        chunk.bytes.resize(chunk_size);
        chunk.begins.clear();
        chunk.sizes.clear();
        size_t position = 0;
        for (size_t i = first; i < last;)
        {
            size_t j = i;
            while (j + 1 < last && order[j + 1] == order[j] + 1)
                ++j;
            const size_t begin = list_offsets[order[i]] * entry_bytes;
            const size_t end = list_offsets[order[j] + 1] * entry_bytes;
            read_range(chunk.bytes.data() + position, lists_byte_begin + begin, end - begin);
            for (size_t k = i; k <= j; ++k)
            {
                chunk_position[order[k]] = static_cast<Int32>(chunk.begins.size());
                chunk.begins.push_back(position + list_offsets[order[k]] * entry_bytes - begin);
                chunk.sizes.push_back(static_cast<UInt32>(list_offsets[order[k] + 1] - list_offsets[order[k]]));
            }
            position += end - begin;
            i = j + 1;
        }
        bytes_read += chunk_size;

        chunk.query_list_offsets.assign(num_queries + 1, 0);
        chunk.query_lists.clear();
        for (size_t q = 0; q < num_queries; ++q)
        {
            for (size_t p = 0; p < num_probes; ++p)
                if (const Int32 local = chunk_position[probed[q * num_probes + p]]; local >= 0)
                    chunk.query_lists.push_back(static_cast<UInt32>(local));
            chunk.query_list_offsets[q + 1] = static_cast<UInt32>(chunk.query_lists.size());
        }

        chunk_rotated_centroids.resize(chunk.begins.size() * dimensions);
        for (size_t i = first; i < last; ++i)
            std::copy_n(rotated_centroids.begin() + order[i] * dimensions, dimensions,
                chunk_rotated_centroids.begin() + static_cast<size_t>(chunk_position[order[i]]) * dimensions);

        scan.scan(chunk.bytes.data(), chunk.bytes.size(), chunk.begins.data(), chunk.sizes.data(), chunk.begins.size(),
            chunk_rotated_centroids.data(), chunk.query_list_offsets.data(), chunk.query_lists.data());

        for (size_t i = first; i < last; ++i)
            chunk_position[order[i]] = -1;
        first = last;
    }

    std::vector<UInt32> found_rows(num_queries * select);
    std::vector<float> found_distances(num_queries * select);
    scan.result(found_rows.data(), found_distances.data());
    const UInt64 scan_ms = watch.elapsedMilliseconds();

    const String vectors_stream_name = index_file_name + String(vectors_substream_suffix);
    const auto vectors_file_name = IMergeTreeDataPart::getStreamNameOrHash(vectors_stream_name, ".idx", *data_part_storage);
    if (!vectors_file_name)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Vector similarity index {} has no file of vectors {}.idx", backQuote(index_name), vectors_stream_name);
    const size_t vector_bytes = dimensions * sizeof(float);
    ReadSettings vectors_read_settings = index_read_settings;
    vectors_read_settings.local_fs_settings.buffer_size = std::max<size_t>(vector_bytes, 4096);
    vectors_read_settings.remote_fs_settings.buffer_size = std::max<size_t>(vector_bytes, 4096);

    const bool cosine = metric == IvfMetric::Cosine;
    const size_t num_tasks = std::min<size_t>(getNumberOfCPUCoresToUse(), num_queries);
    runOnCores(num_tasks, [&](size_t task)
    {
        auto vectors_file = data_part_storage->readFile(*vectors_file_name + ".idx", vectors_read_settings, std::nullopt);
        const bool vectors_read_at = vectors_file->supportsReadAt();
        std::vector<float> vector(dimensions);
        std::vector<std::pair<float, UInt32>> exact;
        exact.reserve(select);
        for (size_t q = task; q < num_queries; q += num_tasks)
        {
            const float * query = queries.data() + q * dimensions;
            exact.clear();
            for (size_t i = 0; i < select; ++i)
            {
                const UInt32 row = found_rows[q * select + i];
                if (row == invalid_neighbour || !std::isfinite(found_distances[q * select + i]))
                    break;
                const size_t offset = vectors_byte_begin + row * vector_bytes;
                if (vectors_read_at)
                {
                    if (vectors_file->readBigAt(reinterpret_cast<char *>(vector.data()), vector_bytes, offset, nullptr) != vector_bytes)
                        throw Exception(ErrorCodes::INCORRECT_DATA, "Vector similarity index {} has a truncated file of vectors", backQuote(index_name));
                }
                else
                {
                    vectors_file->seek(offset, SEEK_SET);
                    vectors_file->readStrict(reinterpret_cast<char *>(vector.data()), vector_bytes);
                }
                float distance = 0;
                if (cosine)
                {
                    for (size_t j = 0; j < dimensions; ++j)
                        distance += vector[j] * query[j];
                    distance = 1.0f - distance;
                }
                else
                {
                    for (size_t j = 0; j < dimensions; ++j)
                    {
                        const float diff = vector[j] - query[j];
                        distance += diff * diff;
                    }
                }
                exact.emplace_back(distance, row);
            }
            const size_t found = std::min(limit, exact.size());
            std::partial_sort(exact.begin(), exact.begin() + found, exact.end());
            for (size_t i = 0; i < found; ++i)
            {
                distances[q * limit + i] = exact[i].first;
                neighbours[q * limit + i] = exact[i].second;
            }
        }
    });

    LOG_TRACE(logger, "Searched {} queries in {} of {} lists ({}) of IVF index {} in {} ms, of them the exact distances of {} candidates per query {} ms",
        num_queries, order.size(), lists, ReadableSize(bytes_read), backQuote(index_name), watch.elapsedMilliseconds(), select,
        watch.elapsedMilliseconds() - scan_ms);
}

MergeTreeIndexAggregatorIvf::MergeTreeIndexAggregatorIvf(
    const String & index_name_, const Block & index_sample_block_, size_t dimensions_, IvfMetric metric_, IvfParams params_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , dimensions(dimensions_)
    , metric(metric_)
    , params(params_)
    , rng(42)
    , converted(dimensions_)
{
    const size_t max_sample_rows = std::max<size_t>(max_sample_bytes / (dimensions * sizeof(float)), 1);
    sample_capacity = params.lists ? std::min(max_sample_rows, std::max<size_t>(params.lists * 40, 65536)) : max_sample_rows;
}

MergeTreeIndexAggregatorIvf::~MergeTreeIndexAggregatorIvf() = default;

void MergeTreeIndexAggregatorIvf::reset()
{
    rows = 0;
    vectors.reset();
    sample = {};
}

void MergeTreeIndexAggregatorIvf::add(const float * vector)
{
    if (!vectors)
        vectors = std::make_shared<TemporaryDataBuffer>(temporaryData());
    vectors->write(reinterpret_cast<const char *>(vector), dimensions * sizeof(float));

    if (sample.size() < sample_capacity * dimensions)
        sample.insert(sample.end(), vector, vector + dimensions);
    else if (const size_t slot = rng() % (rows + 1); slot < sample_capacity)
        std::copy(vector, vector + dimensions, sample.begin() + slot * dimensions);
    ++rows;
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorIvf::getGranuleAndReset()
{
    auto granule = std::make_shared<MergeTreeIndexGranuleIvf>(index_name, metric, dimensions);
    if (rows == 0)
        return granule;

    checkCanUseDevice();
    Stopwatch watch;
    const size_t sample_rows = sample.size() / dimensions;
    const size_t lists = std::clamp<size_t>(
        params.lists ? params.lists : static_cast<size_t>(std::lround(4 * std::sqrt(static_cast<double>(rows)))), 1, sample_rows);

    granule->rows = rows;
    granule->lists = lists;
    granule->centroids.assign(lists * dimensions, 0);
    if (lists == 1)
    {
        for (size_t row = 0; row < sample_rows; ++row)
            for (size_t i = 0; i < dimensions; ++i)
                granule->centroids[i] += sample[row * dimensions + i] / static_cast<float>(sample_rows);
    }
    else
        GPU::trainIvfCentroids(sample.data(), sample_rows, dimensions, lists, granule->centroids.data());
    sample = {};
    const double train_seconds = watch.elapsedSeconds();

    granule->rotation = makeRotation(dimensions, /*seed=*/ 42);
    granule->rotated_centroids = rotateCentroids(granule->rotation, granule->centroids, lists, dimensions);

    const size_t code_bytes = GPU::IvfQuantization::codeBytes(dimensions);
    const size_t record_bytes = 2 * sizeof(UInt32) + sizeof(GPU::IvfQuantization::CodeParams) + code_bytes;
    const size_t num_buckets = std::clamp<size_t>((rows * record_bytes + max_bucket_bytes - 1) / max_bucket_bytes, 1, lists);
    std::vector<IvfListBucket> buckets(num_buckets);
    std::vector<UInt32> bucket_of_list(lists);
    for (size_t b = 0; b < num_buckets; ++b)
    {
        buckets[b].first_list = b * lists / num_buckets;
        buckets[b].end_list = (b + 1) * lists / num_buckets;
        buckets[b].records = std::make_shared<TemporaryDataBuffer>(temporaryData());
        for (size_t l = buckets[b].first_list; l < buckets[b].end_list; ++l)
            bucket_of_list[l] = static_cast<UInt32>(b);
    }

    std::vector<UInt64> counts(lists, 0);
    std::vector<float> residual(dimensions);
    std::vector<uint8_t> codes(code_bytes);
    auto write_entry = [&](UInt32 list, UInt32 row, const float * rotated)
    {
        for (size_t i = 0; i < dimensions; ++i)
            residual[i] = rotated[i] - granule->rotated_centroids[list * dimensions + i];
        const auto code_params = GPU::IvfQuantization::encode(residual.data(), dimensions, codes.data());
        auto & out = *buckets[bucket_of_list[list]].records;
        out.write(reinterpret_cast<const char *>(&list), sizeof(list));
        out.write(reinterpret_cast<const char *>(&row), sizeof(row));
        out.write(reinterpret_cast<const char *>(&code_params), sizeof(code_params));
        out.write(reinterpret_cast<const char *>(codes.data()), code_bytes);
        ++counts[list];
    };

    vectors->finishWriting();
    auto in = vectors->read();
    {
        GPU::IvfCentroids device_centroids(granule->centroids.data(), lists, dimensions);
        const size_t block_rows = std::max<size_t>(assign_block_bytes / (dimensions * sizeof(float)), 1);
        std::vector<float> block(block_rows * dimensions);
        std::vector<float> rotated(block_rows * dimensions);
        std::vector<UInt32> assigned(block_rows);
        for (size_t first = 0; first < rows; first += block_rows)
        {
            const size_t n = std::min(block_rows, rows - first);
            in->readStrict(reinterpret_cast<char *>(block.data()), n * dimensions * sizeof(float));
            device_centroids.assign(block.data(), n, assigned.data(), granule->rotation.data(), rotated.data());
            for (size_t i = 0; i < n; ++i)
                write_entry(assigned[i], static_cast<UInt32>(first + i), rotated.data() + i * dimensions);
        }
    }
    in.reset();
    for (auto & bucket : buckets)
        bucket.records->finishWriting();

    granule->list_offsets.assign(lists + 1, 0);
    for (size_t l = 0; l < lists; ++l)
        granule->list_offsets[l + 1] = granule->list_offsets[l] + counts[l];
    granule->buckets = std::move(buckets);
    granule->vectors = std::move(vectors);

    LOG_DEBUG(logger, "Built IVF index {} of {} rows with {} dimensions in {} lists in {:.3f} s (k-means on {} rows {:.3f} s), {} buckets",
        backQuote(index_name), rows, dimensions, lists, watch.elapsedSeconds(), sample_rows, train_seconds, num_buckets);
    reset();
    return granule;
}

namespace
{
template <typename Column>
void appendVector(const IColumn & data, size_t begin, size_t dimensions, float * out)
{
    const auto & values = typeid_cast<const Column &>(data).getData();
    for (size_t i = 0; i < dimensions; ++i)
        out[i] = static_cast<float>(values[begin + i]);
}

}

void MergeTreeIndexAggregatorIvf::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "The provided position is not less than the number of block rows. Position: {}, Block rows: {}.", *pos, block.rows());

    const size_t rows_read = std::min(limit, block.rows() - *pos);
    if (rows_read == 0)
        return;

    if (rows + rows_read > std::numeric_limits<UInt32>::max())
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

    const auto & data = column_array->getData();
    WhichDataType which(typeid_cast<const DataTypeArray &>(*column_with_type.type).getNestedType());
    for (size_t row = first; row < last; ++row)
    {
        const size_t begin = offsets[row - 1];
        if (which.isFloat32())
            appendVector<ColumnFloat32>(data, begin, dimensions, converted.data());
        else if (which.isFloat64())
            appendVector<ColumnFloat64>(data, begin, dimensions, converted.data());
        else if (which.isBFloat16())
            appendVector<ColumnBFloat16>(data, begin, dimensions, converted.data());
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected data type Array(Float*)");

        checkFinite(converted.data(), dimensions, ErrorCodes::INCORRECT_DATA, "indexed vector");
        if (metric == IvfMetric::Cosine)
            normalizeIvfVector(converted.data(), dimensions);
        add(converted.data());
    }

    *pos += rows_read;
}

MergeTreeIndexConditionIvf::MergeTreeIndexConditionIvf(
    const std::optional<VectorSearchParameters> & parameters_, const String & index_column_, IvfMetric metric_, ContextPtr context)
    : parameters(parameters_)
    , index_column(index_column_)
    , metric(metric_)
    , probes(context->getSettingsRef()[Setting::vector_search_ivf_probes])
    , index_fetch_multiplier(context->getSettingsRef()[Setting::vector_search_index_fetch_multiplier])
    , max_limit(context->getSettingsRef()[Setting::max_limit_for_vector_search_queries])
    , is_rescoring(context->getSettingsRef()[Setting::vector_search_with_rescoring])
    , reader_settings(MergeTreeReaderSettings::createFromContext(context))
{
    if (probes == 0)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Setting 'vector_search_ivf_probes' must not be 0");
    if (!std::isfinite(index_fetch_multiplier) || index_fetch_multiplier < 1.0f || index_fetch_multiplier > 1000.0f)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
            "Setting 'vector_search_index_fetch_multiplier' must be greater or equal to 1 and less or equal to 1000");
}

bool MergeTreeIndexConditionIvf::mayBeTrueOnGranule(MergeTreeIndexGranulePtr, const UpdatePartialDisjunctionResultFn &) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "mayBeTrueOnGranule is not supported for vector similarity indexes");
}

bool MergeTreeIndexConditionIvf::alwaysUnknownOrTrue() const
{
    if (!parameters || parameters->column != index_column)
        return true;
    return !((parameters->distance_function == "L2Distance" && metric == IvfMetric::L2)
        || (parameters->distance_function == "cosineDistance" && metric == IvfMetric::Cosine));
}

NearestNeighbours MergeTreeIndexConditionIvf::calculateApproximateNearestNeighbors(MergeTreeIndexGranulePtr granule_) const
{
    if (!parameters)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected vector_search_parameters to be set");

    const auto granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleIvf>(granule_);
    if (!granule)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Granule has the wrong type");

    if (parameters->reference_vector.size() != granule->dimensions)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The dimension of the reference vector in the query ({}) does not match the dimension in the index ({})",
            parameters->reference_vector.size(), granule->dimensions);

    std::vector<float> query(parameters->reference_vector.begin(), parameters->reference_vector.end());
    checkFinite(query.data(), query.size(), ErrorCodes::INCORRECT_QUERY, "reference vector in the SELECT query");
    if (metric == IvfMetric::Cosine)
        normalizeIvfVector(query.data(), query.size());

    size_t limit = parameters->limit;
    if (parameters->additional_filters_present || is_rescoring)
    {
        const double scaled_limit = static_cast<double>(limit) * static_cast<double>(index_fetch_multiplier);
        limit = (scaled_limit >= static_cast<double>(max_limit)) ? max_limit : static_cast<size_t>(scaled_limit);
    }

    return granule->search(query, limit, probes, reader_settings, parameters->return_distances);
}

MergeTreeIndexIvf::MergeTreeIndexIvf(
    StorageMetadataPtr metadata_snapshot_, const IndexDescription & index_, size_t dimensions_, IvfMetric metric_, IvfParams params_)
    : IMergeTreeIndex(std::move(metadata_snapshot_), index_)
    , dimensions(dimensions_)
    , metric(metric_)
    , params(params_)
{
}

MergeTreeIndexGranulePtr MergeTreeIndexIvf::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleIvf>(index.name, metric, dimensions);
}

MergeTreeIndexSubstreams MergeTreeIndexIvf::getSubstreams() const
{
    return {
        {MergeTreeIndexSubstream::Type::Regular, "", ".idx"},
        {MergeTreeIndexSubstream::Type::VectorIndexLists, String(lists_substream_suffix), ".idx"},
        {MergeTreeIndexSubstream::Type::VectorIndexVectors, String(vectors_substream_suffix), ".idx"},
    };
}

MergeTreeIndexFormat MergeTreeIndexIvf::getPhysicalFormat(
    const MergeTreeDataPartChecksums & checksums, const IDataPartStorage & storage, const std::string & relative_path_prefix) const
{
    if (!indexFileExistsInChecksums(checksums, relative_path_prefix, ".idx", &storage))
        return {0, {}};
    return {1, getSubstreams()};
}

MergeTreeIndexAggregatorPtr MergeTreeIndexIvf::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorIvf>(index.name, index.sample_block, dimensions, metric, params);
}

MergeTreeIndexConditionPtr MergeTreeIndexIvf::createIndexCondition(const ActionsDAG::Node *, ContextPtr) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function not supported for vector similarity index");
}

MergeTreeIndexConditionPtr MergeTreeIndexIvf::createIndexCondition(
    const ActionsDAG::Node *, ContextPtr context, const std::optional<VectorSearchParameters> & parameters) const
{
    return std::make_shared<MergeTreeIndexConditionIvf>(parameters, index.column_names[0], metric, context);
}

namespace
{
IvfMetric parseMetric(const String & distance_function)
{
    if (distance_function == "L2Distance")
        return IvfMetric::L2;
    if (distance_function == "cosineDistance")
        return IvfMetric::Cosine;
    throw Exception(ErrorCodes::INCORRECT_DATA,
        "Second argument (distance function) of vector similarity index with method 'ivf' is not supported. "
        "Supported distance functions are: L2Distance, cosineDistance");
}

IvfParams parseParams(const FieldVector & args)
{
    IvfParams params;
    if (args.size() >= 4)
        params.lists = args[3].safeGet<UInt64>();
    return params;
}

}

MergeTreeIndexPtr ivfIndexCreator(StorageMetadataPtr metadata_snapshot, const IndexDescription & index, const FieldVector & args)
{
    return std::make_shared<MergeTreeIndexIvf>(
        std::move(metadata_snapshot), index, args[2].safeGet<UInt64>(), parseMetric(args[1].safeGet<String>()), parseParams(args));
}

void ivfIndexValidator(const IndexDescription & index, const FieldVector & args)
{
    if (args.size() < 3 || args.size() > 4)
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "Vector similarity index with method 'ivf' must have three or four arguments: method, distance function, dimensions[, lists]");
    if (args[1].getType() != Field::Types::String)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Second argument of vector similarity index (metric) must be of type String");
    if (args[2].getType() != Field::Types::UInt64)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Third argument of vector similarity index (dimensions) must be of type UInt64");
    parseMetric(args[1].safeGet<String>());
    if (args[2].safeGet<UInt64>() == 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Third argument (dimensions) of vector similarity index must be > 0");
    if (args.size() >= 4 && (args[3].getType() != Field::Types::UInt64 || args[3].safeGet<UInt64>() > (1ULL << 24)))
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "Fourth argument of vector similarity index with method 'ivf' (lists) must be a UInt64 of at most 2^24, 0 for 4 * sqrt(rows)");

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
