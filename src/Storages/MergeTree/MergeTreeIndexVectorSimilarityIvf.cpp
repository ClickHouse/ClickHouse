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
#include <Interpreters/Context.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Common/ProfileEvents.h>
#include <Common/setThreadName.h>
#include <Common/threadPoolCallbackRunner.h>
#include <IO/SharedThreadPools.h>
#include <base/scope_guard.h>
#include <Common/Stopwatch.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdlib>
#include <mutex>
#include <numeric>
#include <optional>
#include <queue>
#include <random>
#include <string_view>

namespace ProfileEvents
{
    extern const Event IvfIndexBuildMicroseconds;
    extern const Event IvfIndexTrainMicroseconds;
    extern const Event IvfIndexAssignMicroseconds;
    extern const Event IvfIndexSpillMicroseconds;
    extern const Event IvfIndexWriteMicroseconds;
    extern const Event IvfIndexSearches;
    extern const Event IvfIndexSearchMicroseconds;
    extern const Event IvfIndexRouteMicroseconds;
    extern const Event IvfIndexListReadMicroseconds;
    extern const Event IvfIndexListScanMicroseconds;
    extern const Event IvfIndexListsProbed;
    extern const Event IvfIndexListBytesRead;
    extern const Event IvfIndexListReads;
}

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

constexpr size_t rescoring_factor = 4;

constexpr size_t max_sample_bytes = 512 << 20;
constexpr size_t assign_block_bytes = 32 << 20;
constexpr size_t max_bucket_bytes = 256 << 20;

/// The rows of a list that the index aims at when the number of lists is not given.
constexpr size_t default_list_rows = 1024;
/// The children of a node of the tree of the lists, at most.
constexpr size_t tree_branching = 64;

/// The nodes that a row keeps on each level of the tree when it is assigned to its list.
constexpr size_t assign_beam = 4;


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
    runIvfTasks((lists + 255) / 256, [&](size_t task)
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

namespace
{

/// Eight independent sums, so that the compiler vectorizes the loop without reordering a single sum.
float dotOf(const float * a, const float * b, size_t dimensions)
{
    float sums[8] = {};
    size_t i = 0;
    for (; i + 8 <= dimensions; i += 8)
        for (size_t j = 0; j < 8; ++j)
            sums[j] += a[i + j] * b[i + j];
    for (; i < dimensions; ++i)
        sums[0] += a[i] * b[i];
    return ((sums[0] + sums[1]) + (sums[2] + sums[3])) + ((sums[4] + sums[5]) + (sums[6] + sums[7]));
}

void rotateRow(const std::vector<float> & rotation, const float * x, size_t dimensions, float * out)
{
    for (size_t i = 0; i < dimensions; ++i)
        out[i] = dotOf(rotation.data() + i * dimensions, x, dimensions);
}

/// The records of `n` rows, as `GPU::IvfTreeRouter::assignAndEncode` makes them on the device.
void assignAndEncodeOnCpu(const IvfTree & tree, const std::vector<float> & rotation, const std::vector<float> & rotated_centroids,
    const float * vectors, size_t n, size_t dimensions, size_t beam, UInt32 first_row, char * records)
{
    const size_t code_bytes = GPU::IvfQuantization::codeBytes(dimensions);
    const size_t record_bytes = 2 * sizeof(UInt32) + sizeof(GPU::IvfQuantization::CodeParams) + code_bytes;
    constexpr size_t rows_per_task = 2048;
    runIvfTasks((n + rows_per_task - 1) / rows_per_task, [&](size_t task)
    {
        IvfTree::Scratch scratch;
        std::vector<UInt32> nearest;
        std::vector<float> residual(dimensions);
        for (size_t i = task * rows_per_task; i < std::min(n, (task + 1) * rows_per_task); ++i)
        {
            const float * x = vectors + i * dimensions;
            tree.route(x, beam, 1, nearest, scratch);
            const UInt32 list = nearest.front();
            rotateRow(rotation, x, dimensions, residual.data());
            for (size_t k = 0; k < dimensions; ++k)
                residual[k] -= rotated_centroids[size_t(list) * dimensions + k];
            char * record = records + i * record_bytes;
            const auto code_params = GPU::IvfQuantization::encode(residual.data(), dimensions,
                reinterpret_cast<uint8_t *>(record + 2 * sizeof(UInt32) + sizeof(GPU::IvfQuantization::CodeParams)));
            const UInt32 row = first_row + static_cast<UInt32>(i);
            memcpy(record, &list, sizeof(list));
            memcpy(record + sizeof(list), &row, sizeof(row));
            memcpy(record + 2 * sizeof(UInt32), &code_params, sizeof(code_params));
        }
    });
}

/// The scan of lists of `IvfQuantization` codes on the CPU, as `GPU::IvfListScan` does it on the device: the `select` rows
/// nearest to the query by their codes.
class CpuListScan
{
public:
    CpuListScan(const float * query, size_t dimensions_, size_t select_, const std::vector<float> & rotation)
        : dimensions(dimensions_)
        , words(GPU::IvfQuantization::codeWords(dimensions_))
        , select(select_)
        , rotated_query(dimensions_)
        , u(words * 32, 0.0f)
    {
        rotateRow(rotation, query, dimensions, rotated_query.data());
    }

    void scan(const char * chunk, const UInt64 * list_begins, const UInt32 * list_sizes, size_t num_lists, const float * rotated_centroids)
    {
        const size_t params_bytes = sizeof(GPU::IvfQuantization::CodeParams);
        const size_t code_bytes = GPU::IvfQuantization::codeBytes(dimensions);
        for (size_t l = 0; l < num_lists; ++l)
        {
            /// u = R q - R c, and its squared length and sum.
            float squared_length = 0;
            float sum = 0;
            for (size_t i = 0; i < dimensions; ++i)
            {
                u[i] = rotated_query[i] - rotated_centroids[l * dimensions + i];
                squared_length += u[i] * u[i];
                sum += u[i];
            }

            const size_t size = list_sizes[l];
            const char * rows = chunk + list_begins[l];
            const char * params = rows + size * sizeof(UInt32);
            const char * codes = params + size * params_bytes;
            for (size_t r = 0; r < size; ++r)
            {
                GPU::IvfQuantization::CodeParams code_params;
                memcpy(&code_params, params + r * params_bytes, params_bytes);
                const float dot = dotWithCodes(codes + r * code_bytes);
                const float distance = squared_length - 2.0f * (code_params.lo * sum + code_params.step * dot) + code_params.squared_length;
                if (best.size() < select)
                {
                    UInt32 row;
                    memcpy(&row, rows + r * sizeof(UInt32), sizeof(row));
                    best.emplace(distance, row);
                }
                else if (distance < best.top().first)
                {
                    UInt32 row;
                    memcpy(&row, rows + r * sizeof(UInt32), sizeof(row));
                    best.pop();
                    best.emplace(distance, row);
                }
            }
        }
    }

    /// Nearest first; `invalid_neighbour` and an infinite distance where there are fewer than `select` rows.
    void result(UInt32 * found_rows, float * found_distances)
    {
        for (size_t i = 0; i < select; ++i)
        {
            found_rows[i] = invalid_neighbour;
            found_distances[i] = std::numeric_limits<float>::infinity();
        }
        for (size_t i = best.size(); i > 0; --i)
        {
            found_rows[i - 1] = best.top().second;
            found_distances[i - 1] = best.top().first;
            best.pop();
        }
    }

private:
    /// dot(u, code) of a row, see `IvfQuantization::codeByte`.
    float dotWithCodes(const char * code) const
    {
        float dot = 0;
        for (size_t w = 0; w < words; ++w)
            for (size_t part = 0; part < 4; ++part)
            {
                UInt32 bits;
                memcpy(&bits, code + w * 16 + part * 4, sizeof(bits));
                const float * at = u.data() + w * 32 + part * 8;
                for (size_t nibble = 0; nibble < 8; ++nibble)
                    dot += at[nibble] * static_cast<float>((bits >> (4 * nibble)) & 15u);
            }
        return dot;
    }

    const size_t dimensions;
    const size_t words;
    const size_t select;
    std::vector<float> rotated_query;
    /// Of the list being scanned, padded with zeros to the components of the codes.
    std::vector<float> u;
    /// A max-heap of the best rows so far.
    std::priority_queue<std::pair<float, UInt32>> best;
};

}

MergeTreeIndexGranuleIvf::MergeTreeIndexGranuleIvf(const String & index_name_, IvfMetric metric_, size_t dimensions_, bool on_gpu_)
    : index_name(index_name_)
    , metric(metric_)
    , dimensions(dimensions_)
    , on_gpu(on_gpu_)
{
}

MergeTreeIndexGranuleIvf::~MergeTreeIndexGranuleIvf() = default;

size_t MergeTreeIndexGranuleIvf::memoryUsageBytes() const
{
    return tree.memoryUsageBytes() + (rotation.size() + rotated_centroids.size()) * sizeof(float) + list_offsets.size() * sizeof(UInt64);
}

size_t MergeTreeIndexGranuleIvf::entryBytes() const
{
    return GPU::IvfQuantization::entryBytes(dimensions);
}

void MergeTreeIndexGranuleIvf::serializeBinary(WriteBuffer &) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Vector similarity index {} of method 'ivf' is written to two substreams", backQuote(index_name));
}

void MergeTreeIndexGranuleIvf::serializeBinaryWithMultipleStreams(MergeTreeIndexOutputStreams & streams) const
{
    if (empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to write empty vector similarity index {}", backQuote(index_name));
    if (buckets.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Vector similarity index {} is written without its lists", backQuote(index_name));

    Stopwatch watch;
    SCOPE_EXIT({ ProfileEvents::increment(ProfileEvents::IvfIndexWriteMicroseconds, watch.elapsedMicroseconds()); });
    auto & header = streams.at(MergeTreeIndexSubstream::Type::Regular)->compressed_hashing;
    auto & lists_stream = streams.at(MergeTreeIndexSubstream::Type::VectorIndexLists)->plain_hashing;

    writeIntBinary(FILE_FORMAT_VERSION, header);
    writeIntBinary(static_cast<UInt64>(dimensions), header);
    writeIntBinary(static_cast<UInt8>(metric), header);
    writeIntBinary(static_cast<UInt64>(rows), header);
    writeIntBinary(static_cast<UInt64>(lists), header);
    writeIntBinary(static_cast<UInt64>(lists_stream.count()), header);
    tree.serialize(header);
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

    if (persisted_dimensions != dimensions || persisted_metric != static_cast<UInt8>(metric))
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Vector similarity index {} was written with {} dimensions and metric {}, but the index is declared with {} dimensions and metric {}",
            backQuote(index_name), persisted_dimensions, static_cast<UInt32>(persisted_metric), dimensions, static_cast<UInt32>(metric));
    if (persisted_lists == 0 || persisted_lists > persisted_rows || persisted_rows > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Vector similarity index {} has an invalid shape: {} rows, {} lists",
            backQuote(index_name), persisted_rows, persisted_lists);

    rows = persisted_rows;
    lists = persisted_lists;
    tree = IvfTree(dimensions);
    tree.deserialize(istr);
    if (tree.leaves() != lists)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Vector similarity index {} has {} lists, but its tree has {} leaves",
            backQuote(index_name), lists, tree.leaves());
    rotation.resize(dimensions * dimensions);
    list_offsets.resize(lists + 1);
    istr.readStrict(reinterpret_cast<char *>(rotation.data()), rotation.size() * sizeof(float));
    istr.readStrict(reinterpret_cast<char *>(list_offsets.data()), list_offsets.size() * sizeof(UInt64));
    buckets.clear();
    rotated_centroids = rotateCentroids(rotation, tree.leafCentroids(), lists, dimensions);

    if (list_offsets[0] != 0 || !std::is_sorted(list_offsets.begin(), list_offsets.end()) || entries() != rows)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Vector similarity index {} has invalid list offsets", backQuote(index_name));
}

void MergeTreeIndexGranuleIvf::deserializeBinaryWithMultipleStreams(MergeTreeIndexInputStreams & streams, MergeTreeIndexDeserializationState & state)
{
    deserializeBinary(*streams.at(MergeTreeIndexSubstream::Type::Regular)->getDataBuffer(), state.version);
    data_part_storage = state.part_info.getDataPartStorage();
    index_file_name = state.index.getFileName();
}

namespace
{
constexpr size_t list_chunk_bytes = 256 << 20;

/// Probed lists whose bytes are this close in the substream are read at once, with the bytes between them.
constexpr size_t max_read_gap = 64 << 10;

/// Probed lists [first, last) of the order of a search, read as one range of the substream.
struct ListRange
{
    size_t first = 0;
    size_t last = 0;
    UInt64 begin = 0;
    UInt64 end = 0;
};

struct ListChunk
{
    std::vector<char> bytes;
    std::vector<UInt64> begins;
    std::vector<UInt32> sizes;
    std::vector<float> rotated_centroids;
};

}

NearestNeighbours MergeTreeIndexGranuleIvf::search(const std::vector<float> & query, size_t limit, size_t probes,
    const MergeTreeReaderSettings & reader_settings, bool return_distances) const
{
    NearestNeighbours result;
    if (return_distances)
        result.distances.emplace();
    if (empty() || limit == 0)
        return result;
    if (!data_part_storage)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Vector similarity index {} is searched without the storage of its lists", backQuote(index_name));

    Stopwatch watch;
    const size_t num_probes = std::clamp<size_t>(probes, 1, lists);
    if (limit > GPU::IvfListScan::maxSelect())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Vector similarity index {} of method 'ivf' returns at most {} rows per query",
            backQuote(index_name), GPU::IvfListScan::maxSelect());
    const size_t select = std::min(limit, rows);

    /// The probed lists in the order of the substream.
    std::vector<UInt32> order;
    IvfTree::Scratch scratch;
    /// A query keeps as many nodes on each level above the lists as it probes lists.
    tree.route(query.data(), num_probes, num_probes, order, scratch);
    const double route_ms = watch.elapsedSeconds() * 1000;
    ProfileEvents::increment(ProfileEvents::IvfIndexRouteMicroseconds, watch.elapsedMicroseconds());
    ProfileEvents::increment(ProfileEvents::IvfIndexSearches);
    SCOPE_EXIT({ ProfileEvents::increment(ProfileEvents::IvfIndexSearchMicroseconds, watch.elapsedMicroseconds()); });
    std::sort(order.begin(), order.end());
    std::erase_if(order, [&](UInt32 list) { return list_offsets[list + 1] == list_offsets[list]; });

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

    std::optional<GPU::IvfListScan> scan;
    std::optional<CpuListScan> cpu_scan;
    if (on_gpu)
        scan.emplace(query.data(), 1, dimensions, select, rotation.data());
    else
        cpu_scan.emplace(query.data(), dimensions, select, rotation);

    const size_t entry_bytes = entryBytes();
    auto list_begin = [&](UInt32 list) { return list_offsets[list] * entry_bytes; };
    auto list_end = [&](UInt32 list) { return list_offsets[list + 1] * entry_bytes; };

    std::vector<ListRange> ranges;
    for (size_t i = 0; i < order.size(); ++i)
    {
        if (!ranges.empty() && list_begin(order[i]) <= ranges.back().end + max_read_gap
            && list_end(order[i]) - ranges.back().begin <= list_chunk_bytes)
        {
            ranges.back().last = i + 1;
            ranges.back().end = list_end(order[i]);
        }
        else
            ranges.push_back({i, i + 1, list_begin(order[i]), list_end(order[i])});
    }

    ListChunk chunk;
    std::vector<UInt32> query_lists;
    std::vector<size_t> positions;
    size_t bytes_read = 0;
    for (size_t first_range = 0; first_range < ranges.size();)
    {
        size_t last_range = first_range;
        size_t chunk_size = 0;
        positions.clear();
        while (last_range < ranges.size())
        {
            const size_t range_bytes = ranges[last_range].end - ranges[last_range].begin;
            if (last_range > first_range && chunk_size + range_bytes > list_chunk_bytes)
                break;
            positions.push_back(chunk_size);
            chunk_size += range_bytes;
            ++last_range;
        }

        Stopwatch read_watch;
        chunk.bytes.resize(chunk_size);
        const size_t num_reads = last_range - first_range;
        if (num_reads > 1 && read_at && getIOThreadPool().isInitialized())
        {
            /// The tasks take only values: the runner, declared after the file and the chunk, waits for them first.
            ThreadPoolCallbackRunnerLocal<void> runner(getIOThreadPool().get(), ThreadName::DEFAULT_THREAD_POOL);
            ReadBufferFromFileBase * file = lists_file.get();
            for (size_t r = 0; r < num_reads; ++r)
            {
                const auto & range = ranges[first_range + r];
                runner.enqueueAndKeepTrack([file, to = chunk.bytes.data() + positions[r], offset = lists_byte_begin + range.begin,
                    size = range.end - range.begin, name = index_name]
                {
                    if (file->readBigAt(to, size, offset, nullptr) != size)
                        throw Exception(ErrorCodes::INCORRECT_DATA, "Vector similarity index {} has a truncated file of lists", backQuote(name));
                });
            }
            runner.waitForAllToFinishAndRethrowFirstError();
        }
        else
            for (size_t r = 0; r < num_reads; ++r)
            {
                const auto & range = ranges[first_range + r];
                read_range(chunk.bytes.data() + positions[r], lists_byte_begin + range.begin, range.end - range.begin);
            }

        chunk.begins.clear();
        chunk.sizes.clear();
        chunk.rotated_centroids.clear();
        for (size_t r = 0; r < num_reads; ++r)
        {
            const auto & range = ranges[first_range + r];
            for (size_t k = range.first; k < range.last; ++k)
            {
                chunk.begins.push_back(positions[r] + list_begin(order[k]) - range.begin);
                chunk.sizes.push_back(static_cast<UInt32>(list_offsets[order[k] + 1] - list_offsets[order[k]]));
                chunk.rotated_centroids.insert(chunk.rotated_centroids.end(),
                    rotated_centroids.begin() + order[k] * dimensions, rotated_centroids.begin() + (order[k] + 1) * dimensions);
            }
        }
        bytes_read += chunk_size;
        /// Here, not in the reads, which may run on threads of no query.
        ProfileEvents::increment(ProfileEvents::IvfIndexListReads, num_reads);
        ProfileEvents::increment(ProfileEvents::IvfIndexListBytesRead, chunk_size);
        ProfileEvents::increment(ProfileEvents::IvfIndexListReadMicroseconds, read_watch.elapsedMicroseconds());
        ProfileEvents::increment(ProfileEvents::IvfIndexListsProbed, chunk.begins.size());

        /// The query scans all lists of the chunk.
        query_lists.resize(chunk.begins.size());
        std::iota(query_lists.begin(), query_lists.end(), 0);
        const std::array<UInt32, 2> query_list_offsets = {0, static_cast<UInt32>(query_lists.size())};
        Stopwatch scan_watch;
        if (cpu_scan)
            cpu_scan->scan(chunk.bytes.data(), chunk.begins.data(), chunk.sizes.data(), chunk.begins.size(), chunk.rotated_centroids.data());
        else
            scan->scan(chunk.bytes.data(), chunk.bytes.size(), chunk.begins.data(), chunk.sizes.data(), chunk.begins.size(),
            chunk.rotated_centroids.data(), query_list_offsets.data(), query_lists.data());
        ProfileEvents::increment(ProfileEvents::IvfIndexListScanMicroseconds, scan_watch.elapsedMicroseconds());
        first_range = last_range;
    }

    std::vector<UInt32> found_rows(select);
    std::vector<float> found_distances(select);
    if (cpu_scan)
        cpu_scan->result(found_rows.data(), found_distances.data());
    else
        scan->result(found_rows.data(), found_distances.data());

    /// The codes give |q - x|^2, which for normalized vectors is 2 (1 - cos).
    const float distance_scale = metric == IvfMetric::Cosine ? 0.5f : 1.0f;
    for (size_t i = 0; i < select && found_rows[i] != invalid_neighbour && std::isfinite(found_distances[i]); ++i)
    {
        result.rows.push_back(found_rows[i]);
        if (return_distances)
            result.distances->push_back(found_distances[i] * distance_scale);
    }

    LOG_TRACE(logger, "Searched {} of {} lists ({}) of IVF index {} in {:.3f} ms (the tree of {} levels {:.3f} ms)",
        order.size(), lists, ReadableSize(bytes_read), backQuote(index_name), watch.elapsedSeconds() * 1000, tree.depth(), route_ms);
    return result;
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
    if (sample.size() < sample_capacity * dimensions)
        sample.insert(sample.end(), vector, vector + dimensions);
    else if (const size_t slot = rng() % (rows + 1); slot < sample_capacity)
        std::copy(vector, vector + dimensions, sample.begin() + slot * dimensions);
    ++rows;
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorIvf::getGranuleAndReset()
{
    auto granule = std::make_shared<MergeTreeIndexGranuleIvf>(index_name, metric, dimensions, params.on_gpu);
    if (rows == 0)
        return granule;

    if (params.on_gpu)
        checkCanUseDevice();
    Stopwatch watch;
    SCOPE_EXIT({ ProfileEvents::increment(ProfileEvents::IvfIndexBuildMicroseconds, watch.elapsedMicroseconds()); });
    const size_t sample_rows = sample.size() / dimensions;
    const size_t wanted_lists = params.lists ? params.lists : (rows + default_list_rows - 1) / default_list_rows;
    granule->tree = IvfTree::build(sample.data(), sample_rows, dimensions, wanted_lists, tree_branching, /*seed=*/ 42);
    const size_t lists = granule->tree.leaves();
    sample = {};
    const double train_seconds = watch.elapsedSeconds();
    ProfileEvents::increment(ProfileEvents::IvfIndexTrainMicroseconds, watch.elapsedMicroseconds());

    granule->rows = rows;
    granule->lists = lists;

    granule->rotation = makeRotation(dimensions, /*seed=*/ 42);
    granule->rotated_centroids = rotateCentroids(granule->rotation, granule->tree.leafCentroids(), lists, dimensions);

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
    /// The records of a block, `record_bytes` each: the list, the row, the parameters and the codes.
    std::vector<char> records;
    if (GPU::IvfTreeRouter::recordBytes(dimensions) != record_bytes)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The records of the IVF index {} differ between the host and the device", backQuote(index_name));

    vectors->finishWriting();
    auto in = vectors->read();
    double assign_seconds = 0;
    {
        const size_t block_rows = std::max<size_t>(assign_block_bytes / (dimensions * sizeof(float)), 1);
        const size_t beam = std::min(assign_beam, GPU::IvfTreeRouter::maxBeam());
        const auto & tree = granule->tree;
        std::vector<size_t> level_nodes(tree.depth());
        std::vector<const float *> level_centroids(tree.depth());
        std::vector<const UInt32 *> level_child_offsets(tree.depth());
        for (size_t l = 0; l < tree.depth(); ++l)
        {
            level_nodes[l] = tree.centroidsOf(l).size() / dimensions;
            level_centroids[l] = tree.centroidsOf(l).data();
            level_child_offsets[l] = tree.childOffsetsOf(l).data();
        }
        std::optional<GPU::IvfTreeRouter> router;
        if (params.on_gpu)
            router.emplace(dimensions, tree.depth(), level_nodes.data(), level_centroids.data(), level_child_offsets.data(),
                granule->rotation.data(), granule->rotated_centroids.data());

        std::vector<float> block(block_rows * dimensions);
        for (size_t first = 0; first < rows; first += block_rows)
        {
            const size_t n = std::min(block_rows, rows - first);
            Stopwatch spill_watch;
            in->readStrict(reinterpret_cast<char *>(block.data()), n * dimensions * sizeof(float));
            ProfileEvents::increment(ProfileEvents::IvfIndexSpillMicroseconds, spill_watch.elapsedMicroseconds());

            Stopwatch assign_watch;
            records.resize(n * record_bytes);
            if (router)
                router->assignAndEncode(block.data(), n, beam, static_cast<UInt32>(first), records.data());
            else
                assignAndEncodeOnCpu(tree, granule->rotation, granule->rotated_centroids, block.data(), n, dimensions, beam,
                    static_cast<UInt32>(first), records.data());
            assign_seconds += assign_watch.elapsedSeconds();
            ProfileEvents::increment(ProfileEvents::IvfIndexAssignMicroseconds, assign_watch.elapsedMicroseconds());

            spill_watch.restart();
            for (size_t i = 0; i < n; ++i)
            {
                UInt32 list = 0;
                memcpy(&list, records.data() + i * record_bytes, sizeof(list));
                if (list >= lists)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Vector similarity index {} assigned a row to the list {} of {}",
                        backQuote(index_name), list, lists);
                buckets[bucket_of_list[list]].records->write(records.data() + i * record_bytes, record_bytes);
                ++counts[list];
            }
            ProfileEvents::increment(ProfileEvents::IvfIndexSpillMicroseconds, spill_watch.elapsedMicroseconds());
        }
    }
    in.reset();
    for (auto & bucket : buckets)
        bucket.records->finishWriting();

    granule->list_offsets.assign(lists + 1, 0);
    for (size_t l = 0; l < lists; ++l)
        granule->list_offsets[l + 1] = granule->list_offsets[l] + counts[l];
    granule->buckets = std::move(buckets);

    LOG_DEBUG(logger, "Built IVF index {} of {} rows with {} dimensions in {} lists, a tree of {} levels, in {:.3f} s "
        "(the tree on {} rows {:.3f} s, routing the rows {:.3f} s), {} buckets",
        backQuote(index_name), rows, dimensions, lists, granule->tree.depth(), watch.elapsedSeconds(), sample_rows, train_seconds,
        assign_seconds, num_buckets);
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
    /// The rows of the block, written to the temporary data at once.
    converted.resize(rows_read * dimensions);
    for (size_t row = first; row < last; ++row)
    {
        const size_t begin = offsets[row - 1];
        float * vector = converted.data() + (row - first) * dimensions;
        if (which.isFloat32())
            appendVector<ColumnFloat32>(data, begin, dimensions, vector);
        else if (which.isFloat64())
            appendVector<ColumnFloat64>(data, begin, dimensions, vector);
        else if (which.isBFloat16())
            appendVector<ColumnBFloat16>(data, begin, dimensions, vector);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected data type Array(Float*)");

        checkFinite(vector, dimensions, ErrorCodes::INCORRECT_DATA, "indexed vector");
        if (metric == IvfMetric::Cosine)
            normalizeIvfVector(vector, dimensions);
        add(vector);
    }

    Stopwatch spill_watch;
    if (!vectors)
        vectors = std::make_shared<TemporaryDataBuffer>(temporaryData());
    vectors->write(reinterpret_cast<const char *>(converted.data()), converted.size() * sizeof(float));
    ProfileEvents::increment(ProfileEvents::IvfIndexSpillMicroseconds, spill_watch.elapsedMicroseconds());

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

    if (limit > GPU::IvfListScan::maxSelect())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Vector similarity index of method 'ivf' returns at most {} rows per query",
            GPU::IvfListScan::maxSelect());
    /// The candidates of the exact distances that ClickHouse computes from the column, see `useVectorSearch`.
    const size_t candidates = std::min(limit * rescoring_factor, GPU::IvfListScan::maxSelect());
    return granule->search(query, candidates, probes, reader_settings, parameters->return_distances);
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
    return std::make_shared<MergeTreeIndexGranuleIvf>(index.name, metric, dimensions, params.on_gpu);
}

MergeTreeIndexSubstreams MergeTreeIndexIvf::getSubstreams() const
{
    return {
        {MergeTreeIndexSubstream::Type::Regular, "", ".idx"},
        {MergeTreeIndexSubstream::Type::VectorIndexLists, String(lists_substream_suffix), ".idx"},
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
    if (args.size() >= 5)
        params.on_gpu = args[4].safeGet<String>() == "gpu";
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
    if (args.size() < 3 || args.size() > 5)
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "Vector similarity index with method 'ivf' must have three to five arguments: method, distance function, dimensions[, lists[, device]]");
    if (args[1].getType() != Field::Types::String)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Second argument of vector similarity index (metric) must be of type String");
    if (args[2].getType() != Field::Types::UInt64)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Third argument of vector similarity index (dimensions) must be of type UInt64");
    parseMetric(args[1].safeGet<String>());
    if (args[2].safeGet<UInt64>() == 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Third argument (dimensions) of vector similarity index must be > 0");
    if (args.size() >= 4 && (args[3].getType() != Field::Types::UInt64 || args[3].safeGet<UInt64>() > (1ULL << 24)))
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "Fourth argument of vector similarity index with method 'ivf' (lists) must be a UInt64 of at most 2^24, 0 for one list per 1024 rows");
    if (args.size() >= 5 && (args[4].getType() != Field::Types::String || (args[4].safeGet<String>() != "gpu" && args[4].safeGet<String>() != "cpu")))
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Fifth argument of vector similarity index with method 'ivf' (device) must be 'gpu' or 'cpu'");

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
