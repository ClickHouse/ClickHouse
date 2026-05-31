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


TEST(ReadPipeline, BuildWithEmptyObjectsReturnsEmptyBuffer)
try
{
    ReadPipeline pipeline;
    pipeline.setSource(memoryCreator("data"), StoredObjects{}, ReadSettings{});

    /// Empty objects = zero-blob file. build() returns ReadBufferFromEmptyFile.
    auto buf = pipeline.build();
    ASSERT_TRUE(buf != nullptr);

    String result;
    readStringUntilEOF(result, *buf);
    EXPECT_EQ(result, "");
}
catch (...)
{
    FAIL() << getCurrentExceptionMessage(true);
}


TEST(ReadPipeline, UnknownSizeReadsToEofViaExecutor)
try
{
    /// `StoredObject::UnknownSize` callers (S3 `HEAD` without
    /// `Content-Length`, local `stat()` failure) put the sentinel into
    /// `bytes_size` instead of `0` so consumers can distinguish from a
    /// truly empty file. The executor recognises the sentinel via
    /// `OffsetMap::hasUnknownSize` and switches to streaming-until-EOF
    /// — it reads window-sized chunks from the source and detects EOF
    /// when the source returns short. No fall back to the legacy
    /// pipeline.
    std::string data = "actually-not-empty";

    ReadSettings rs;
    rs.use_reader_executor = true;

    ReadPipeline pipeline;
    pipeline.setSource(
        memoryCreator(data),
        StoredObjects{testObject(StoredObject::UnknownSize)},
        rs);

    auto buf = pipeline.build();
    ASSERT_TRUE(buf != nullptr);

    String result;
    readStringUntilEOF(result, *buf);
    EXPECT_EQ(result, data)
        << "ReaderExecutor must stream unknown-size sources to EOF without "
           "needing bytes_size up front";
}
catch (...)
{
    FAIL() << getCurrentExceptionMessage(true);
}


TEST(ReadPipeline, KnownSizeUsesReaderExecutor)
try
{
    /// Sanity check the other side: when the size IS known (default), the
    /// executor path runs and produces the same bytes.
    std::string data = "executor-path-ok";

    ReadSettings rs;
    rs.use_reader_executor = true;

    ReadPipeline pipeline;
    pipeline.setSource(
        memoryCreator(data),
        StoredObjects{testObject(data.size())},
        rs);

    auto buf = pipeline.build();
    ASSERT_TRUE(buf != nullptr);

    String result;
    readStringUntilEOF(result, *buf);
    EXPECT_EQ(result, data);
}
catch (...)
{
    FAIL() << getCurrentExceptionMessage(true);
}


TEST(ReadPipeline, UnknownSizeDeniesRandomReadProbes)
try
{
    /// An unknown-size source must NOT advertise random reads or seekability.
    /// Random-read formats (Parquet/ORC/Arrow) react to a positive answer by
    /// calling `getFileSizeFromReadBuffer`, which throws `UNKNOWN_FILE_SIZE`
    /// because `tryGetFileSize` is `nullopt` here — making the object unreadable
    /// even though the executor can stream it to EOF. So both probes (and the
    /// file size) must be denied, and the format falls back to streaming.
    ReadSettings rs;
    rs.use_reader_executor = true;

    ReadPipeline pipeline;
    pipeline.setSource(
        memoryCreator("actually-not-empty"),
        StoredObjects{testObject(StoredObject::UnknownSize)},
        rs);

    auto buf = pipeline.build();
    ASSERT_TRUE(buf != nullptr);

    EXPECT_FALSE(buf->supportsReadAt());
    EXPECT_FALSE(buf->checkIfActuallySeekable());
    EXPECT_FALSE(buf->tryGetFileSize().has_value());
}
catch (...)
{
    FAIL() << getCurrentExceptionMessage(true);
}


TEST(ReadPipeline, KnownSizeAllowsRandomReadProbes)
try
{
    /// With a known size the executor advertises random reads and seekability,
    /// so formats can take the fast `readBigAt` path, and reports the real size.
    std::string data = "known-size-data";

    ReadSettings rs;
    rs.use_reader_executor = true;

    ReadPipeline pipeline;
    pipeline.setSource(
        memoryCreator(data),
        StoredObjects{testObject(data.size())},
        rs);

    auto buf = pipeline.build();
    ASSERT_TRUE(buf != nullptr);

    EXPECT_TRUE(buf->supportsReadAt());
    EXPECT_TRUE(buf->checkIfActuallySeekable());
    ASSERT_TRUE(buf->tryGetFileSize().has_value());
    EXPECT_EQ(*buf->tryGetFileSize(), data.size());
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
    pipeline.needFilesystemCache(nullptr, FilesystemCacheSettings{});
    pipeline.needGather();
    EXPECT_EQ(pipeline.describe(), "Source(Custom) -> FilesystemCache -> Gather");
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
    original.setSource(memoryCreator("data"), StoredObjects{testObject()}, ReadSettings{});

    ReadPipeline cloned = original.clone();

    /// Mutating `cloned` (adding a stage) must not affect `original`.
    /// `setSource` cannot be called twice on the same pipeline, so we exercise
    /// independence by adding a different stage on each side.
    cloned.needGather();

    EXPECT_EQ(original.describe(), "Source(Custom)");
    EXPECT_EQ(cloned.describe(), "Source(Custom) -> Gather");
}
catch (...)
{
    FAIL() << getCurrentExceptionMessage(true);
}
