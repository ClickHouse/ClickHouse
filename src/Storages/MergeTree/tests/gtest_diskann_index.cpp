#include "config.h"
#if USE_DISKANN

#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Storages/MergeTree/DiskANNIndex.h>

#include <array>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

using namespace DB;

namespace
{
namespace fs = std::filesystem;

fs::path makeUniqueTestDir(const std::string & name)
{
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    auto dir = fs::temp_directory_path() / ("clickhouse-diskann-" + name + "-" + std::to_string(now));
    fs::create_directories(dir);
    return dir;
}

class TempDirScope
{
public:
    explicit TempDirScope(const std::string & name)
        : path(makeUniqueTestDir(name))
    {
    }

    ~TempDirScope()
    {
        std::error_code ec;
        fs::remove_all(path, ec);
    }

    fs::path path;
};

void writeFbinFile(const fs::path & path, size_t rows, size_t dim, const std::vector<float> & data)
{
    ASSERT_EQ(data.size(), rows * dim);

    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(out.is_open()) << path;

    const auto rows_u32 = static_cast<uint32_t>(rows);
    const auto dim_u32 = static_cast<uint32_t>(dim);
    out.write(reinterpret_cast<const char *>(&rows_u32), sizeof(rows_u32));
    out.write(reinterpret_cast<const char *>(&dim_u32), sizeof(dim_u32));
    out.write(reinterpret_cast<const char *>(data.data()), static_cast<std::streamsize>(data.size() * sizeof(float)));
    out.close();

    ASSERT_TRUE(out.good()) << path;
}

void writeSequentialFbinFile(const fs::path & path, size_t rows, size_t dim)
{
    std::vector<float> data(rows * dim);
    for (size_t row = 0; row < rows; ++row)
    {
        for (size_t col = 0; col < dim; ++col)
            data[row * dim + col] = static_cast<float>(row * 100 + col + 1);
    }

    writeFbinFile(path, rows, dim, data);
}

void writeTruncatedFbinFile(const fs::path & path, size_t rows, size_t dim)
{
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(out.is_open()) << path;

    const auto rows_u32 = static_cast<uint32_t>(rows);
    const auto dim_u32 = static_cast<uint32_t>(dim);
    const float partial_value = 1.0f;
    out.write(reinterpret_cast<const char *>(&rows_u32), sizeof(rows_u32));
    out.write(reinterpret_cast<const char *>(&dim_u32), sizeof(dim_u32));
    out.write(reinterpret_cast<const char *>(&partial_value), sizeof(partial_value));
    out.close();

    ASSERT_TRUE(out.good()) << path;
}

std::string toString(const fs::path & path)
{
    return path.string();
}

DiskANNBuildOptions createBuildOptions()
{
    DiskANNBuildOptions options;
    options.pruned_degree = 16;
    options.max_degree = 32;
    options.l_build = 64;
    options.alpha = 1.2f;
    options.num_threads = 1;
    options.pq_chunks = 4;
    options.build_ram_limit_gb = 0.25;
    return options;
}

DiskANNSearchOptions createSearchOptions()
{
    DiskANNSearchOptions options;
    options.num_threads = 1;
    options.search_io_limit = 4;
    options.num_nodes_to_cache = 0;
    options.default_search_list_size = 16;
    options.default_beam_width = 4;
    return options;
}

void buildSequentialIndex(const fs::path & data_path, const fs::path & index_prefix, size_t rows, size_t dim)
{
    writeSequentialFbinFile(data_path, rows, dim);

    DiskANNDiskIndexBuilder builder(dim, DiskANNMetric::L2, createBuildOptions());
    builder.setDataPath(toString(data_path));
    builder.setIndexPrefix(toString(index_prefix));
    builder.build();
}

}

TEST(DiskANNIndex, BuildAndSearchRealDiskIndex)
{
    TempDirScope dir("build-search");
    const auto data_path = dir.path / "vectors.fbin";
    const auto index_prefix = dir.path / "diskann_index";
    writeSequentialFbinFile(data_path, 32, 8);

    DiskANNDiskIndexBuilder builder(8, DiskANNMetric::L2, createBuildOptions());
    builder.setDataPath(toString(data_path));
    builder.setIndexPrefix(toString(index_prefix));
    builder.build();

    ASSERT_TRUE(DiskANNDiskIndexBuilder::indexFilesExist(toString(index_prefix)));

    DiskANNDiskIndexSearcher searcher(8, DiskANNMetric::L2, toString(index_prefix), createSearchOptions());
    const std::array<float, 8> query = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f};
    std::vector<uint64_t> ids(5);
    std::vector<float> distances(5);

    const auto found = searcher.search(query.data(), query.size(), ids.size(), ids.data(), distances.data());
    ASSERT_EQ(found, ids.size());
    EXPECT_EQ(ids[0], 0u);
    EXPECT_GE(distances[0], 0.0f);
}

TEST(DiskANNIndex, BuildAndSearchCosineDiskIndex)
{
    TempDirScope dir("build-search-cosine");
    const auto data_path = dir.path / "vectors.fbin";
    const auto index_prefix = dir.path / "diskann_index";
    const std::vector<float> data = {
        1.0f, 0.0f, 0.0f, 0.0f,
        0.9f, 0.1f, 0.0f, 0.0f,
        0.0f, 1.0f, 0.0f, 0.0f,
        -1.0f, 0.0f, 0.0f, 0.0f,
        0.0f, 0.0f, 1.0f, 0.0f,
        0.0f, 0.0f, 0.0f, 1.0f,
        0.5f, 0.5f, 0.5f, 0.5f,
        -0.5f, 0.5f, 0.0f, 0.0f,
    };
    writeFbinFile(data_path, 8, 4, data);

    DiskANNDiskIndexBuilder builder(4, DiskANNMetric::Cosine, createBuildOptions());
    builder.setDataPath(toString(data_path));
    builder.setIndexPrefix(toString(index_prefix));
    builder.build();

    DiskANNDiskIndexSearcher searcher(4, DiskANNMetric::Cosine, toString(index_prefix), createSearchOptions());
    const std::array<float, 4> query = {1.0f, 0.0f, 0.0f, 0.0f};
    uint64_t id = 0;
    float distance = 0.0f;

    const auto found = searcher.search(query.data(), query.size(), 1, &id, &distance);
    ASSERT_EQ(found, 1u);
    EXPECT_EQ(id, 0u);
    EXPECT_GE(distance, 0.0f);
}

TEST(DiskANNIndex, BuildRejectsMissingDataset)
{
    TempDirScope dir("missing-dataset");
    const auto index_prefix = dir.path / "diskann_index";

    DiskANNDiskIndexBuilder builder(8, DiskANNMetric::L2, createBuildOptions());
    builder.setDataPath(toString(dir.path / "missing.fbin"));
    builder.setIndexPrefix(toString(index_prefix));

    EXPECT_THROW(builder.build(), Exception);
    EXPECT_FALSE(DiskANNDiskIndexBuilder::indexFilesExist(toString(index_prefix)));
}

TEST(DiskANNIndex, BuildRejectsUnsetDataPath)
{
    TempDirScope dir("unset-data-path");

    DiskANNDiskIndexBuilder builder(8, DiskANNMetric::L2, createBuildOptions());
    builder.setIndexPrefix(toString(dir.path / "diskann_index"));

    EXPECT_THROW(builder.build(), Exception);
}

TEST(DiskANNIndex, BuildRejectsUnsetIndexPrefix)
{
    TempDirScope dir("unset-index-prefix");
    const auto data_path = dir.path / "vectors.fbin";
    writeSequentialFbinFile(data_path, 16, 8);

    DiskANNDiskIndexBuilder builder(8, DiskANNMetric::L2, createBuildOptions());
    builder.setDataPath(toString(data_path));

    EXPECT_THROW(builder.build(), Exception);
}

TEST(DiskANNIndex, BuildRejectsDimensionMismatch)
{
    TempDirScope dir("build-dim-mismatch");
    const auto data_path = dir.path / "vectors.fbin";
    const auto index_prefix = dir.path / "diskann_index";
    writeSequentialFbinFile(data_path, 16, 6);

    DiskANNDiskIndexBuilder builder(8, DiskANNMetric::L2, createBuildOptions());
    builder.setDataPath(toString(data_path));
    builder.setIndexPrefix(toString(index_prefix));

    EXPECT_THROW(builder.build(), Exception);
}

TEST(DiskANNIndex, BuildRejectsZeroRowDataset)
{
    TempDirScope dir("zero-rows");
    const auto data_path = dir.path / "vectors.fbin";
    const auto index_prefix = dir.path / "diskann_index";
    writeFbinFile(data_path, 0, 8, {});

    DiskANNDiskIndexBuilder builder(8, DiskANNMetric::L2, createBuildOptions());
    builder.setDataPath(toString(data_path));
    builder.setIndexPrefix(toString(index_prefix));

    EXPECT_THROW(builder.build(), Exception);
}

TEST(DiskANNIndex, BuildRejectsTruncatedDataset)
{
    TempDirScope dir("truncated-dataset");
    const auto data_path = dir.path / "vectors.fbin";
    const auto index_prefix = dir.path / "diskann_index";
    writeTruncatedFbinFile(data_path, 8, 8);

    DiskANNDiskIndexBuilder builder(8, DiskANNMetric::L2, createBuildOptions());
    builder.setDataPath(toString(data_path));
    builder.setIndexPrefix(toString(index_prefix));

    EXPECT_THROW(builder.build(), Exception);
    EXPECT_FALSE(DiskANNDiskIndexBuilder::indexFilesExist(toString(index_prefix)));
}

TEST(DiskANNIndex, BuilderRejectsInvalidConfiguration)
{
    EXPECT_THROW(DiskANNDiskIndexBuilder(0, DiskANNMetric::L2, createBuildOptions()), Exception);

    auto invalid_options = createBuildOptions();
    invalid_options.pq_chunks = 0;
    EXPECT_THROW(DiskANNDiskIndexBuilder(8, DiskANNMetric::L2, invalid_options), Exception);
}

TEST(DiskANNIndex, OpenSearcherRejectsMissingIndexFiles)
{
    TempDirScope dir("missing-index");
    EXPECT_THROW(
        DiskANNDiskIndexSearcher(8, DiskANNMetric::L2, toString(dir.path / "diskann_index"), createSearchOptions()),
        Exception);
}

TEST(DiskANNIndex, OpenSearcherRejectsRequestedDimensionMismatch)
{
    TempDirScope dir("open-dim-mismatch");
    const auto data_path = dir.path / "vectors.fbin";
    const auto index_prefix = dir.path / "diskann_index";
    buildSequentialIndex(data_path, index_prefix, 32, 8);

    EXPECT_THROW(
        DiskANNDiskIndexSearcher(7, DiskANNMetric::L2, toString(index_prefix), createSearchOptions()),
        Exception);
}

TEST(DiskANNIndex, OpenSearcherRejectsInvalidSearchOptions)
{
    TempDirScope dir("invalid-search-options");
    const auto data_path = dir.path / "vectors.fbin";
    const auto index_prefix = dir.path / "diskann_index";
    buildSequentialIndex(data_path, index_prefix, 32, 8);

    auto options = createSearchOptions();
    options.search_io_limit = 0;

    EXPECT_THROW(
        DiskANNDiskIndexSearcher(8, DiskANNMetric::L2, toString(index_prefix), options),
        Exception);
}

TEST(DiskANNIndex, SearchRejectsDimensionMismatch)
{
    TempDirScope dir("search-dim-mismatch");
    const auto data_path = dir.path / "vectors.fbin";
    const auto index_prefix = dir.path / "diskann_index";
    buildSequentialIndex(data_path, index_prefix, 32, 8);

    DiskANNDiskIndexSearcher searcher(8, DiskANNMetric::L2, toString(index_prefix), createSearchOptions());
    const std::array<float, 7> wrong_query = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f};
    std::vector<uint64_t> ids(3);
    std::vector<float> distances(3);

    EXPECT_THROW(
        searcher.search(wrong_query.data(), wrong_query.size(), ids.size(), ids.data(), distances.data()),
        Exception);
}

TEST(DiskANNIndex, SearchReturnsZeroForZeroK)
{
    TempDirScope dir("search-zero-k");
    const auto data_path = dir.path / "vectors.fbin";
    const auto index_prefix = dir.path / "diskann_index";
    buildSequentialIndex(data_path, index_prefix, 32, 8);

    DiskANNDiskIndexSearcher searcher(8, DiskANNMetric::L2, toString(index_prefix), createSearchOptions());
    const std::array<float, 8> query = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f};

    EXPECT_EQ(searcher.search(query.data(), query.size(), 0, nullptr, nullptr), 0u);
}

TEST(DiskANNIndex, SearchSupportsExplicitOverridesAfterReopen)
{
    TempDirScope dir("search-reopen-overrides");
    const auto data_path = dir.path / "vectors.fbin";
    const auto index_prefix = dir.path / "diskann_index";
    buildSequentialIndex(data_path, index_prefix, 32, 8);

    const std::array<float, 8> query = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f};
    std::vector<uint64_t> ids_first(4);
    std::vector<float> distances_first(4);
    std::vector<uint64_t> ids_second(4);
    std::vector<float> distances_second(4);

    {
        DiskANNDiskIndexSearcher searcher(8, DiskANNMetric::L2, toString(index_prefix), createSearchOptions());
        const auto found = searcher.search(query.data(), query.size(), ids_first.size(), ids_first.data(), distances_first.data(), 24, 8);
        ASSERT_EQ(found, ids_first.size());
        EXPECT_EQ(ids_first[0], 0u);
    }

    {
        DiskANNDiskIndexSearcher searcher(8, DiskANNMetric::L2, toString(index_prefix), createSearchOptions());
        const auto found = searcher.search(query.data(), query.size(), ids_second.size(), ids_second.data(), distances_second.data(), 24, 8);
        ASSERT_EQ(found, ids_second.size());
        EXPECT_EQ(ids_second[0], 0u);
    }

    EXPECT_EQ(ids_first, ids_second);
    EXPECT_EQ(distances_first.size(), distances_second.size());
}

TEST(DiskANNIndex, OpenSearcherRejectsMissingAnyRequiredIndexFile)
{
    const std::vector<std::string> required_suffixes = {
        "_disk.index",
        "_pq_pivots.bin",
        "_pq_compressed.bin",
    };

    for (const auto & suffix : required_suffixes)
    {
        TempDirScope dir("missing-required-file" + suffix);
        const auto data_path = dir.path / "vectors.fbin";
        const auto index_prefix = dir.path / "diskann_index";
        buildSequentialIndex(data_path, index_prefix, 32, 8);

        ASSERT_TRUE(DiskANNDiskIndexBuilder::indexFilesExist(toString(index_prefix)));
        fs::remove(index_prefix.string() + suffix);
        ASSERT_FALSE(DiskANNDiskIndexBuilder::indexFilesExist(toString(index_prefix)));

        EXPECT_THROW(
            DiskANNDiskIndexSearcher(8, DiskANNMetric::L2, toString(index_prefix), createSearchOptions()),
            Exception);
    }
}

#endif
