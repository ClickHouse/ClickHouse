#include <Storages/MergeTree/MergeTreeIndexVectorSimilarityIvf.h>

#if USE_GPU

#include <GPU/CuvsIvf.cuh>
#include <IO/ReadHelpers.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Common/Stopwatch.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <string_view>

/// The batch search of the `ivf` index for `vectorSearch`: `searchBatch` of `MergeTreeIndexGranuleIvf`, which
/// https://github.com/ClickHouse/ClickHouse/pull/131127 removed. It needs the index of that pull request and its declaration:
///
///     void searchBatch(const std::vector<float> & queries, size_t num_queries, size_t limit, size_t probes,
///         const MergeTreeReaderSettings & reader_settings, std::vector<UInt32> & neighbours, std::vector<float> & distances) const;

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
}

namespace
{
constexpr UInt32 invalid_neighbour = std::numeric_limits<UInt32>::max();
constexpr std::string_view lists_substream_suffix = ".lst";
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
    const size_t select = std::min(limit, rows);

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

    /// The codes give |q - x|^2, which for normalized vectors is 2 (1 - cos).
    const float distance_scale = metric == IvfMetric::Cosine ? 0.5f : 1.0f;
    for (size_t q = 0; q < num_queries; ++q)
        for (size_t i = 0; i < select; ++i)
        {
            const UInt32 row = found_rows[q * select + i];
            const float distance = found_distances[q * select + i];
            if (row == invalid_neighbour || !std::isfinite(distance))
                break;
            neighbours[q * limit + i] = row;
            distances[q * limit + i] = distance * distance_scale;
        }

    LOG_TRACE(logger, "Searched {} queries in {} of {} lists ({}) of IVF index {} in {} ms",
        num_queries, order.size(), lists, ReadableSize(bytes_read), backQuote(index_name), watch.elapsedMilliseconds());
}

}

#endif
