#include <gtest/gtest.h>

#include <IO/ReadPipeline.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>

#include <cstring>
#include <map>


using namespace DB;

namespace
{

/// A ReadBufferFromFileBase that reads from a string, chunk by chunk in nextImpl.
/// Compatible with the ReadBufferFromRemoteFSGather / SwapHelper pattern,
/// unlike ReadBufferFromOwnMemoryFile which pre-loads data at construction.
class TestReadBuffer : public ReadBufferFromFileBase
{
public:
    explicit TestReadBuffer(String data_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE)
        : ReadBufferFromFileBase(buf_size, nullptr, 0)
        , data(std::move(data_))
    {
    }

    String getFileName() const override { return "test"; }

    off_t seek(off_t off, int whence) override
    {
        if (whence == SEEK_SET)
            file_offset = off;
        else if (whence == SEEK_CUR)
            file_offset += off;
        resetWorkingBuffer();
        return file_offset;
    }

    off_t getPosition() override { return file_offset; }
    size_t getFileOffsetOfBufferEnd() const override { return file_offset; }

private:
    bool nextImpl() override
    {
        if (file_offset >= data.size())
            return false;
        size_t to_read = std::min(data.size() - file_offset, internal_buffer.size());
        memcpy(internal_buffer.begin(), data.data() + file_offset, to_read);
        working_buffer = Buffer(internal_buffer.begin(), internal_buffer.begin() + to_read);
        file_offset += to_read;
        return true;
    }

    String data;
    size_t file_offset = 0;
};

/// Helper: creates a BufferCreator that returns a TestReadBuffer with the given data.
ReadPipeline::BufferCreator memoryCreator(const std::string & data)
{
    return [data](const StoredObject & /* object */, const ReadSettings & /* settings */,
        bool /* use_external_buffer */, bool /* restrict_seek */)
        -> std::unique_ptr<ReadBufferFromFileBase>
    {
        return std::make_unique<TestReadBuffer>(data);
    };
}

/// Per-object memory creator: each object's remote_path maps to its data chunk.
ReadPipeline::BufferCreator perObjectCreator(std::map<String, String> object_data)
{
    return [data = std::move(object_data)](const StoredObject & object, const ReadSettings & /* settings */,
        bool /* use_external_buffer */, bool /* restrict_seek */)
        -> std::unique_ptr<ReadBufferFromFileBase>
    {
        auto it = data.find(object.remote_path);
        if (it == data.end())
            throw std::runtime_error("No data for object " + object.remote_path);
        return std::make_unique<TestReadBuffer>(it->second);
    };
}

StoredObject testObject(const String & path, size_t size)
{
    return StoredObject(path, path, size);
}

StoredObject testObject(size_t size = 100)
{
    return StoredObject("test/object", "local/object", size);
}

}


/// -- Source only (no gather): single object --

TEST(ReadPipeline, BuildSingleObjectNoGather)
try
{
    std::string data = "hello world";

    ReadPipeline pipeline;
    pipeline.setSource(memoryCreator(data), StoredObjects{testObject(data.size())}, ReadSettings{});

    auto buf = pipeline.build();
    ASSERT_TRUE(buf != nullptr);

    String result;
    readStringUntilEOF(result, *buf);
    EXPECT_EQ(result, "hello world");
}
catch (...)
{
    FAIL() << getCurrentExceptionMessage(true);
}


/// -- Source + Gather: single object --

TEST(ReadPipeline, BuildSingleObjectWithGather)
try
{
    std::string data = "hello world";

    ReadPipeline pipeline;
    pipeline.setSource(memoryCreator(data), StoredObjects{testObject(data.size())}, ReadSettings{});
    pipeline.needGather();
    auto buf = pipeline.build();
    ASSERT_TRUE(buf != nullptr);

    String result;
    readStringUntilEOF(result, *buf);
    EXPECT_EQ(result, "hello world");
}
catch (...)
{
    FAIL() << getCurrentExceptionMessage(true);
}


/// -- Source + Gather: multiple objects --

TEST(ReadPipeline, BuildMultipleObjects)
try
{
    /// Two objects: "AAAA" (4 bytes) and "BBBBBB" (6 bytes).
    String data_a = "AAAA";
    String data_b = "BBBBBB";

    auto creator = perObjectCreator({
        {"obj/a", data_a},
        {"obj/b", data_b},
    });

    ReadPipeline pipeline;
    pipeline.setSource(
        std::move(creator),
        StoredObjects{testObject("obj/a", data_a.size()), testObject("obj/b", data_b.size())},
        ReadSettings{});
    pipeline.needGather();
    auto buf = pipeline.build();
    ASSERT_TRUE(buf != nullptr);

    String result;
    readStringUntilEOF(result, *buf);
    EXPECT_EQ(result, "AAAABBBBBB");
}
catch (...)
{
    FAIL() << getCurrentExceptionMessage(true);
}


TEST(ReadPipeline, BuildMultipleObjectsSeek)
try
{
    /// Three objects: "AAA" (3), "BBB" (3), "CCC" (3) = 9 bytes total.
    auto creator = perObjectCreator({
        {"obj/a", "AAA"},
        {"obj/b", "BBB"},
        {"obj/c", "CCC"},
    });

    ReadPipeline pipeline;
    pipeline.setSource(
        std::move(creator),
        StoredObjects{testObject("obj/a", 3), testObject("obj/b", 3), testObject("obj/c", 3)},
        ReadSettings{});
    pipeline.needGather();
    auto buf = pipeline.build();

    /// Seek to offset 4 (middle of second object) and read the rest.
    buf->seek(4, SEEK_SET);
    String result;
    readStringUntilEOF(result, *buf);
    EXPECT_EQ(result, "BBCCC");
}
catch (...)
{
    FAIL() << getCurrentExceptionMessage(true);
}


/// -- Validation --

TEST(ReadPipeline, BuildWithoutSourceThrows)
try
{
    ReadPipeline pipeline;
    EXPECT_THROW(pipeline.build(), Exception);
}
catch (...)
{
    FAIL() << getCurrentExceptionMessage(true);
}


TEST(ReadPipeline, BuildWithEmptyObjectsThrows)
try
{
    ReadPipeline pipeline;
    pipeline.setSource(memoryCreator("data"), StoredObjects{}, ReadSettings{});
    EXPECT_THROW(pipeline.build(), Exception);
}
catch (...)
{
    FAIL() << getCurrentExceptionMessage(true);
}


/// -- describe --

TEST(ReadPipeline, DescribeEmpty)
{
    ReadPipeline pipeline;
    EXPECT_EQ(pipeline.describe(), "(empty)");
}


TEST(ReadPipeline, DescribeSourceOnly)
{
    ReadPipeline pipeline;
    pipeline.setSource(memoryCreator(""), StoredObjects{testObject()}, ReadSettings{});
    EXPECT_EQ(pipeline.describe(), "Source(Custom)");
}


TEST(ReadPipeline, DescribeMultipleStages)
{
    ReadPipeline pipeline;
    pipeline.setSource(memoryCreator(""), StoredObjects{testObject()}, ReadSettings{});
    pipeline.needDiskCache(nullptr, FilesystemCacheSettings{});
    pipeline.needGather();
    EXPECT_EQ(pipeline.describe(), "Source(Custom) -> DiskCache -> Gather");
}


/// -- hasSource / getStoredObjects --

TEST(ReadPipeline, HasSource)
{
    ReadPipeline pipeline;
    EXPECT_FALSE(pipeline.hasSource());

    pipeline.setSource(memoryCreator(""), StoredObjects{testObject()}, ReadSettings{});
    EXPECT_TRUE(pipeline.hasSource());
}


TEST(ReadPipeline, GetStoredObjects)
try
{
    ReadPipeline pipeline;
    auto obj = testObject(42);
    pipeline.setSource(memoryCreator(""), StoredObjects{obj}, ReadSettings{});

    const auto & objects = pipeline.getStoredObjects();
    ASSERT_EQ(objects.size(), 1u);
    EXPECT_EQ(objects[0].bytes_size, 42u);
    EXPECT_EQ(objects[0].remote_path, "test/object");
}
catch (...)
{
    FAIL() << getCurrentExceptionMessage(true);
}


TEST(ReadPipeline, GetStoredObjectsWithoutSourceThrows)
try
{
    ReadPipeline pipeline;
    EXPECT_THROW(pipeline.getStoredObjects(), Exception);
}
catch (...)
{
    FAIL() << getCurrentExceptionMessage(true);
}


/// -- clone --

TEST(ReadPipeline, ClonePreservesState)
try
{
    std::string data = "clone test data";

    ReadPipeline original;
    original.setSource(memoryCreator(data), StoredObjects{testObject(data.size())}, ReadSettings{});

    ReadPipeline cloned = original.clone();

    EXPECT_TRUE(cloned.hasSource());
    EXPECT_EQ(cloned.describe(), "Source(Custom)");

    auto buf = cloned.build();
    String result;
    readStringUntilEOF(result, *buf);
    EXPECT_EQ(result, "clone test data");
}
catch (...)
{
    FAIL() << getCurrentExceptionMessage(true);
}


TEST(ReadPipeline, CloneIsIndependent)
try
{
    ReadPipeline original;
    original.setSource(memoryCreator("original"), StoredObjects{testObject()}, ReadSettings{});

    ReadPipeline cloned = original.clone();
    cloned.setSource(memoryCreator("cloned"), StoredObjects{testObject()}, ReadSettings{});

    auto buf1 = original.build();
    String result1;
    readStringUntilEOF(result1, *buf1);
    EXPECT_EQ(result1, "original");

    auto buf2 = cloned.build();
    String result2;
    readStringUntilEOF(result2, *buf2);
    EXPECT_EQ(result2, "cloned");
}
catch (...)
{
    FAIL() << getCurrentExceptionMessage(true);
}
