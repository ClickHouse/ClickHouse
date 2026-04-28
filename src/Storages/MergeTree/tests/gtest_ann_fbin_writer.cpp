#include <gtest/gtest.h>

#include <Storages/MergeTree/ANNIndex/DiskANNFbinWriter.h>

#include <Common/Exception.h>
#include <IO/WriteBufferFromFile.h>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

using namespace DB;

namespace
{
namespace fs = std::filesystem;

fs::path makeTempFile(const std::string & tag)
{
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto dir = fs::temp_directory_path() / ("clickhouse-ann-fbin-" + tag + "-" + std::to_string(now));
    fs::create_directories(dir);
    return dir / "out.fbin";
}

struct FbinHeader
{
    uint32_t npoints;
    uint32_t ndim;
};

FbinHeader readHeader(const fs::path & path)
{
    std::ifstream in(path, std::ios::binary);
    FbinHeader header{};
    in.read(reinterpret_cast<char *>(&header), sizeof(header));
    return header;
}

std::vector<float> readBody(const fs::path & path, size_t npoints, size_t ndim)
{
    std::ifstream in(path, std::ios::binary);
    in.seekg(sizeof(FbinHeader));
    std::vector<float> body(npoints * ndim);
    if (npoints * ndim > 0)
        in.read(reinterpret_cast<char *>(body.data()),
                static_cast<std::streamsize>(body.size() * sizeof(float)));
    return body;
}

}

TEST(DiskANNFbinWriterTest, WriteRowsRoundTrip)
{
    const fs::path path = makeTempFile("roundtrip");
    const UInt32 dim = 4;
    const size_t rows = 3;

    std::vector<float> expected(rows * dim);
    for (size_t r = 0; r < rows; ++r)
        for (size_t c = 0; c < dim; ++c)
            expected[r * dim + c] = static_cast<float>(r * 10 + c);

    {
        WriteBufferFromFile out(path.string());
        DiskANNFbinWriter writer(out, dim);
        for (size_t r = 0; r < rows; ++r)
            writer.appendRow(expected.data() + r * dim, dim);
        writer.finalize();
        out.finalize();
    }

    const auto header = readHeader(path);
    EXPECT_EQ(header.npoints, rows);
    EXPECT_EQ(header.ndim, dim);

    const auto body = readBody(path, rows, dim);
    ASSERT_EQ(body.size(), expected.size());
    EXPECT_EQ(std::memcmp(body.data(), expected.data(), body.size() * sizeof(float)), 0);

    std::error_code ec;
    fs::remove_all(path.parent_path(), ec);
}

TEST(DiskANNFbinWriterTest, EmptyWriterHasZeroNpoints)
{
    const fs::path path = makeTempFile("empty");
    const UInt32 dim = 8;

    {
        WriteBufferFromFile out(path.string());
        DiskANNFbinWriter writer(out, dim);
        writer.finalize();
        out.finalize();
    }

    const auto header = readHeader(path);
    EXPECT_EQ(header.npoints, 0u);
    EXPECT_EQ(header.ndim, dim);
    EXPECT_EQ(fs::file_size(path), sizeof(FbinHeader));

    std::error_code ec;
    fs::remove_all(path.parent_path(), ec);
}

TEST(DiskANNFbinWriterTest, DimMismatchThrows)
{
    const fs::path path = makeTempFile("mismatch");
    const UInt32 dim = 5;

    WriteBufferFromFile out(path.string());
    DiskANNFbinWriter writer(out, dim);

    std::vector<float> good(dim, 1.0f);
    writer.appendRow(good.data(), dim);

    std::vector<float> bad(dim + 1, 2.0f);
    EXPECT_THROW(writer.appendRow(bad.data(), dim + 1), DB::Exception);

    writer.finalize();
    out.finalize();

    std::error_code ec;
    fs::remove_all(path.parent_path(), ec);
}

TEST(DiskANNFbinWriterTest, ZeroDimRejected)
{
    const fs::path path = makeTempFile("zerodim");
    WriteBufferFromFile out(path.string());
    EXPECT_THROW(DiskANNFbinWriter(out, 0), DB::Exception);
    out.finalize();

    std::error_code ec;
    fs::remove_all(path.parent_path(), ec);
}

TEST(DiskANNFbinWriterTest, AppendAfterFinalizeRejected)
{
    const fs::path path = makeTempFile("afterfinalize");
    const UInt32 dim = 3;

    WriteBufferFromFile out(path.string());
    DiskANNFbinWriter writer(out, dim);

    std::vector<float> row(dim, 0.5f);
    writer.appendRow(row.data(), dim);
    writer.finalize();

    EXPECT_THROW(writer.appendRow(row.data(), dim), DB::Exception);

    out.finalize();

    std::error_code ec;
    fs::remove_all(path.parent_path(), ec);
}

TEST(DiskANNFbinWriterTest, DestructWithoutFinalizeLeavesZeroNpoints)
{
    /// When `finalize` is not called, the header placeholder remains and `npoints` stays 0.
    /// The destructor logs a warning but must not throw.
    const fs::path path = makeTempFile("nofinalize");
    const UInt32 dim = 6;

    {
        WriteBufferFromFile out(path.string());
        {
            DiskANNFbinWriter writer(out, dim);
            std::vector<float> row(dim, 1.0f);
            writer.appendRow(row.data(), dim);
            /// No `finalize` — destructor runs here.
        }
        out.finalize();
    }

    const auto header = readHeader(path);
    EXPECT_EQ(header.npoints, 0u);
    EXPECT_EQ(header.ndim, dim);

    std::error_code ec;
    fs::remove_all(path.parent_path(), ec);
}
