#include <gtest/gtest.h>

#include <IO/ReadPipeline.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <IO/AsynchronousReadBufferFromFileDescriptor.h>
#include <IO/AsynchronousReader.h>
#include <IO/MMapReadBufferFromFileWithCache.h>
#include <IO/MMappedFileCache.h>
#include <Common/Exception.h>
#include <Common/filesystemHelpers.h>
#include <Common/ProfileEvents.h>
#include <Interpreters/FileCache/FileCache.h>
#include <Interpreters/FileCache/FileCacheFactory.h>
#include <Common/PageCache.h>
#include <base/scope_guard.h>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <future>
#include <map>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <vector>


using namespace DB;
namespace fs = std::filesystem;

namespace DB::FileCacheSetting
{
    extern const FileCacheSettingsString path;
    extern const FileCacheSettingsUInt64 max_size;
    extern const FileCacheSettingsUInt64 max_elements;
    extern const FileCacheSettingsUInt64 max_file_segment_size;
    extern const FileCacheSettingsUInt64 boundary_alignment;
    extern const FileCacheSettingsUInt64 background_download_threads;
}

namespace ProfileEvents
{
    extern const Event CreatedReadBufferDirectIO;
    extern const Event CreatedReadBufferDirectIOFailed;
}

namespace
{

/// A ReadBufferFromFileBase that reads from a string, chunk by chunk in nextImpl.
/// Compatible with the ReadBufferFromRemoteFSGather / SwapHelper pattern,
/// unlike ReadBufferFromOwnMemoryFile which pre-loads data at construction.
class TestReadBuffer : public ReadBufferFromFileBase
{
public:
    /// `bound_out`, when set, receives the right bound that the gather stage forwards to this buffer.
    explicit TestReadBuffer(
        String data_,
        std::shared_ptr<std::optional<size_t>> bound_out_ = nullptr,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE)
        : ReadBufferFromFileBase(buf_size, nullptr, 0)
        , data(std::move(data_))
        , bound_out(std::move(bound_out_))
    {
    }

    String getFileName() const override { return "test"; }

    void setReadUntilPosition(size_t position) override
    {
        read_until_position = position;
        if (bound_out)
            *bound_out = position;
    }

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
        const size_t limit = std::min(data.size(), read_until_position.value_or(data.size()));
        if (file_offset >= limit)
            return false;
        size_t to_read = std::min(limit - file_offset, internal_buffer.size());
        memcpy(internal_buffer.begin(), data.data() + file_offset, to_read);
        working_buffer = Buffer(internal_buffer.begin(), internal_buffer.begin() + to_read);
        file_offset += to_read;
        return true;
    }

    String data;
    std::shared_ptr<std::optional<size_t>> bound_out;
    std::optional<size_t> read_until_position;
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

/// Helper: like `memoryCreator`, but the buffer reports the right bound the gather stage forwards.
ReadPipeline::BufferCreator boundRecordingCreator(const std::string & data, const std::shared_ptr<std::optional<size_t>> & bound_out)
{
    return [data, bound_out](const StoredObject & /* object */, const ReadSettings & /* settings */,
        bool /* use_external_buffer */, bool /* restrict_seek */)
        -> std::unique_ptr<ReadBufferFromFileBase>
    {
        return std::make_unique<TestReadBuffer>(data, bound_out);
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

std::unique_ptr<Poco::TemporaryFile> createLocalFileWithContent(const String & content)
{
    auto tmp_file = createTemporaryFile((fs::current_path() / "tmp").string());
    {
        WriteBufferFromFile out(tmp_file->path());
        writeString(content, out);
        out.finalize();
    }
    return tmp_file;
}

/// Build a legacy local read pipeline. The reader executor and Direct I/O are
/// disabled so the test exercises the shared EOF guard on any kernel.
std::unique_ptr<ReadBufferFromFileBase> buildLocalPipeline(const String & path, size_t reported_size)
{
    ReadSettings settings;
    settings.reader_executor.enabled = false;
    settings.local_fs_settings.method = LocalFSReadMethod::pread;
    settings.local_fs_settings.direct_io_threshold = 0;

    ReadPipeline pipeline;
    pipeline.setLocalFileSource(
        path,
        StoredObjects{testObject(path, reported_size)},
        settings);
    return pipeline.build();
}

FileCachePtr createTestFileCache(const String & name)
{
    const auto cache_path = fs::current_path() / (name + "_cache");
    fs::remove_all(cache_path);
    fs::create_directories(cache_path);

    FileCacheSettings settings;
    settings[FileCacheSetting::path] = cache_path / "";
    settings[FileCacheSetting::max_size] = 1024 * 1024;
    settings[FileCacheSetting::max_elements] = 100;
    settings[FileCacheSetting::boundary_alignment] = 0;
    settings[FileCacheSetting::max_file_segment_size] = 1024 * 1024;
    settings[FileCacheSetting::background_download_threads] = 0;

    auto cache = FileCacheFactory::instance().getOrCreate(name, settings, "");
    cache->initialize();
    return cache;
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


/// -- Local source: preserve the known size and its EOF guard --

TEST(ReadPipeline, LocalFileSourceKnownSizeBoundsEOF)
{
    auto tmp_file = createLocalFileWithContent("hello");

    /// The file is 5 bytes, but the reported size is 3. Using the real size would
    /// make the fixed and unfixed paths indistinguishable on modern kernels, where
    /// a read at EOF returns 0 instead of EINVAL.
    auto buf = buildLocalPipeline(tmp_file->path(), 3);
    ASSERT_NE(buf, nullptr);

    buf->seek(3, SEEK_SET);
    EXPECT_TRUE(buf->eof());
}

TEST(ReadPipeline, LocalFileSourceUnknownSizeDoesNotBoundEOF)
{
    /// An unknown size must not impose an EOF boundary at the reported offset.
    auto tmp_file = createLocalFileWithContent("hello");
    auto buf = buildLocalPipeline(tmp_file->path(), StoredObject::UnknownSize);
    ASSERT_NE(buf, nullptr);

    buf->seek(3, SEEK_SET);
    EXPECT_FALSE(buf->eof());
}

TEST(ReadPipeline, LocalFileSourceZeroSizeStillOpensPath)
{
    auto tmp_file = createTemporaryFile((fs::current_path() / "tmp").string());
    const String path = tmp_file->path();

    /// Zero is treated as unknown, so the missing file is still opened and reports
    /// an open-time error instead of being silently treated as an empty file.
    fs::remove(path);
    ASSERT_FALSE(fs::exists(path));

    EXPECT_THROW(buildLocalPipeline(path, 0), DB::Exception);
}

/// The known size is forwarded for the EOF guard only. It must not change reader selection.
TEST(ReadPipeline, LocalFileSourceKeepsReaderSelection)
try
{
    auto tmp_file = createLocalFileWithContent("hello");

    ReadSettings settings;
    settings.reader_executor.enabled = false;
    settings.local_fs_settings.method = LocalFSReadMethod::mmap;
    settings.local_fs_settings.mmap_threshold = 1;
    settings.local_fs_settings.direct_io_threshold = 0;

    MMappedFileCache mmap_cache(1024);
    settings.local_fs_settings.mmap_cache = &mmap_cache;

    auto build = [&](std::optional<size_t> read_hint)
    {
        ReadPipeline pipeline;
        pipeline.setLocalFileSource(
            tmp_file->path(),
            StoredObjects{testObject(tmp_file->path(), 5)},
            settings,
            read_hint);
        return pipeline.build();
    };

    /// No hint and a size above the mmap threshold: selection must stay on the old buffered
    /// reader, otherwise forwarding the size would silently change cache behavior.
    auto without_hint = build(std::nullopt);
    ASSERT_NE(without_hint, nullptr);
    EXPECT_EQ(dynamic_cast<MMapReadBufferFromFileWithCache *>(without_hint.get()), nullptr);

    /// An explicit hint must still select the mmap reader, exactly as before.
    auto with_hint = build(5);
    ASSERT_NE(with_hint, nullptr);
    EXPECT_NE(dynamic_cast<MMapReadBufferFromFileWithCache *>(with_hint.get()), nullptr);
}
catch (...)
{
    FAIL() << getCurrentExceptionMessage(true);
}

/// The same rule must hold for the Direct I/O branch: a forwarded size without a hint must not
/// attempt O_DIRECT; an explicit hint keeps the old attempt.
TEST(ReadPipeline, LocalFileSourceKeepsDirectIOSelection)
try
{
#if defined(OS_LINUX) || defined(OS_FREEBSD)
    auto tmp_file = createLocalFileWithContent("hello");

    ReadSettings settings;
    settings.reader_executor.enabled = false;
    settings.local_fs_settings.method = LocalFSReadMethod::pread;
    settings.local_fs_settings.mmap_threshold = 0;
    settings.local_fs_settings.direct_io_threshold = 1;

    auto build = [&](std::optional<size_t> read_hint)
    {
        ReadPipeline pipeline;
        pipeline.setLocalFileSource(
            tmp_file->path(),
            StoredObjects{testObject(tmp_file->path(), 5)},
            settings,
            read_hint);
        return pipeline.build();
    };

    /// Count both successful O_DIRECT opens and rejected attempts: the test must distinguish
    /// "not attempted" from "attempted but the filesystem refused it".
    auto direct_io_attempts = []
    {
        return ProfileEvents::global_counters[ProfileEvents::CreatedReadBufferDirectIO]
            + ProfileEvents::global_counters[ProfileEvents::CreatedReadBufferDirectIOFailed];
    };

    auto before = direct_io_attempts();
    auto without_hint = build(std::nullopt);
    ASSERT_NE(without_hint, nullptr);
    EXPECT_EQ(direct_io_attempts(), before);

    before = direct_io_attempts();
    auto with_zero_hint = build(0);
    ASSERT_NE(with_zero_hint, nullptr);
    EXPECT_EQ(direct_io_attempts(), before);

    before = direct_io_attempts();
    auto with_hint = build(5);
    ASSERT_NE(with_hint, nullptr);
    EXPECT_GT(direct_io_attempts(), before);
#endif
}
catch (...)
{
    FAIL() << getCurrentExceptionMessage(true);
}


namespace
{

/// Serves the given data in memory and records the offsets of submitted requests.
/// With `enforce_known_eof`, a request at or past the end is a test failure: that is exactly
/// what the EOF guard must prevent for a reader with a known file size.
class RecordingAsyncReader : public IAsynchronousReader
{
public:
    explicit RecordingAsyncReader(String data_, bool enforce_known_eof_ = true)
        : data(std::move(data_)), enforce_known_eof(enforce_known_eof_)
    {
    }

    std::future<Result> submit(Request request) override
    {
        std::lock_guard lock(mutex);
        request_offsets.push_back(request.offset);
        request_ignores.push_back(request.ignore);
        if (enforce_known_eof)
            EXPECT_LT(request.offset, data.size()) << "read submitted at or past the known EOF";
        EXPECT_LE(request.offset, data.size());

        size_t available = request.offset < data.size()
            ? std::min(request.size, data.size() - request.offset)
            : 0;
        if (available > 0)
            memcpy(request.buf, data.data() + request.offset, available);

        /// Match the real reader contract: the ignored prefix is included in `size`, and
        /// `offset` tells the buffer where the usable data starts.
        EXPECT_LE(request.ignore, available);

        std::promise<Result> promise;
        Result result;
        result.buf = request.buf;
        result.size = available;
        result.offset = request.ignore;
        result.file_offset_of_buffer_end = request.offset + available;
        promise.set_value(std::move(result));
        return promise.get_future();
    }

    Result execute(Request) override
    {
        throw std::runtime_error("execute is not supported by the test reader");
    }

    void wait() override {}

    String data;
    bool enforce_known_eof;
    std::mutex mutex;
    std::vector<size_t> request_offsets;
    std::vector<size_t> request_ignores;
};

std::unique_ptr<AsynchronousReadBufferFromFileDescriptor> makeAsyncBuffer(
    RecordingAsyncReader & reader,
    size_t file_size,
    size_t alignment = 0)
{
    return std::make_unique<AsynchronousReadBufferFromFileDescriptor>(
        reader,
        Priority{},
        /* fd = */ -1,
        /* buf_size = */ 4096,
        /* flags = */ -1,
        /* existing_memory = */ nullptr,
        alignment,
        file_size);
}

}


TEST(ReadPipeline, AsynchronousReaderDoesNotSubmitAtKnownEOF)
try
{
    const String data(58, 'x');
    RecordingAsyncReader reader(data);

    auto buf = makeAsyncBuffer(reader, data.size());

    String result;
    while (buf->next())
    {
        result.append(buf->position(), buf->available());
        buf->position() += buf->available();
    }

    EXPECT_EQ(result, data);

    /// Only the data read was submitted; reaching EOF must not submit another request.
    ASSERT_EQ(reader.request_offsets.size(), 1u);
    EXPECT_EQ(reader.request_offsets.front(), 0u);

    /// Prefetch must also stay silent at a known EOF.
    buf->prefetch(Priority{});
    EXPECT_EQ(reader.request_offsets.size(), 1u);
}
catch (...)
{
    FAIL() << getCurrentExceptionMessage(true);
}

TEST(ReadPipeline, AsynchronousReaderConsumesPendingPrefetchAtEOF)
try
{
    const String data(58, 'x');
    RecordingAsyncReader reader(data);

    auto buf = makeAsyncBuffer(reader, data.size());

    /// The prefetch is submitted before EOF and must be consumed normally.
    buf->prefetch(Priority{});
    ASSERT_EQ(reader.request_offsets.size(), 1u);

    String result;
    while (buf->next())
    {
        result.append(buf->position(), buf->available());
        buf->position() += buf->available();
    }

    EXPECT_EQ(result, data);
    EXPECT_EQ(reader.request_offsets.size(), 1u);
}
catch (...)
{
    FAIL() << getCurrentExceptionMessage(true);
}

TEST(ReadPipeline, AsynchronousReaderDoesNotSubmitAfterAlignedSeekToEOF)
try
{
    const String data(58, 'x');
    RecordingAsyncReader reader(data);

    /// The seek rounds the physical offset down to the alignment and records the difference in
    /// bytes_to_ignore. The logical position is EOF, so no physical read is needed.
    auto buf = makeAsyncBuffer(reader, data.size(), /* alignment = */ 4096);

    EXPECT_EQ(buf->seek(58, SEEK_SET), 58);
    EXPECT_TRUE(buf->eof());
    EXPECT_TRUE(reader.request_offsets.empty());

    /// Prefetch at the logical EOF must stay silent as well.
    buf->prefetch(Priority{});
    EXPECT_TRUE(reader.request_offsets.empty());
}
catch (...)
{
    FAIL() << getCurrentExceptionMessage(true);
}

TEST(ReadPipeline, AsynchronousReaderReadsAfterAlignedSeekWithIgnore)
try
{
    String data;
    for (size_t i = 0; i < 58; ++i)
        data.push_back(static_cast<char>('a' + i % 26));

    RecordingAsyncReader reader(data);
    auto buf = makeAsyncBuffer(reader, data.size(), /* alignment = */ 4096);

    /// The physical offset is rounded down to 0; the first 17 bytes must be ignored.
    EXPECT_EQ(buf->seek(17, SEEK_SET), 17);

    /// The logical position is before EOF, so prefetch must submit with ignore = 17.
    buf->prefetch(Priority{});
    ASSERT_EQ(reader.request_offsets.size(), 1u);
    ASSERT_EQ(reader.request_ignores.size(), 1u);
    EXPECT_EQ(reader.request_offsets.front(), 0u);
    EXPECT_EQ(reader.request_ignores.front(), 17u);

    String result;
    while (buf->next())
    {
        result.append(buf->position(), buf->available());
        buf->position() += buf->available();
    }

    EXPECT_EQ(result, data.substr(17));

    /// Reaching EOF must not submit another request.
    EXPECT_EQ(reader.request_offsets.size(), 1u);
}
catch (...)
{
    FAIL() << getCurrentExceptionMessage(true);
}

TEST(ReadPipeline, AsynchronousReaderWithoutKnownSizeReliesOnRealEOF)
try
{
    const String data(58, 'x');
    RecordingAsyncReader reader(data, /* enforce_known_eof = */ false);

    /// file_size is unknown: the reader cannot guard, so the read at offset 58 is submitted and
    /// the real zero-length result reports EOF.
    AsynchronousReadBufferFromFileDescriptor buf(
        reader,
        Priority{},
        /* fd = */ -1,
        /* buf_size = */ 4096,
        /* flags = */ -1,
        /* existing_memory = */ nullptr,
        /* alignment = */ 0,
        /* file_size_ = */ std::nullopt);

    String result;
    while (buf.next())
    {
        result.append(buf.position(), buf.available());
        buf.position() += buf.available();
    }

    EXPECT_EQ(result, data);
    ASSERT_EQ(reader.request_offsets.size(), 2u);
    EXPECT_EQ(reader.request_offsets[0], 0u);
    EXPECT_EQ(reader.request_offsets[1], data.size());
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


/// -- Gather right bound --

TEST(ReadPipeline, GatherPassesRightBoundAtObjectEnd)
try
{
    /// A read up to the end of the object must get the bound as well. Without it the request to object
    /// storage is open-ended, and `ReadBufferFromS3` returns the HTTP connection to the pool only after
    /// a read observes EOF, which never happens when the caller reads exactly the remaining bytes.
    auto bound = std::make_shared<std::optional<size_t>>();

    ReadPipeline pipeline;
    pipeline.setSource(boundRecordingCreator("ABCDEFGHIJ", bound), StoredObjects{testObject("obj/a", 10)}, ReadSettings{});
    pipeline.needGather();
    auto buf = pipeline.build();

    buf->setReadUntilPosition(10);
    buf->next();

    ASSERT_TRUE(bound->has_value());
    EXPECT_EQ(**bound, 10);
}
catch (...)
{
    FAIL() << getCurrentExceptionMessage(true);
}


TEST(ReadPipeline, GatherPassesRightBoundForUnknownObjectSize)
try
{
    /// The size of the object is unknown, as it is for an HTTP server that answers without
    /// `Content-Length`. The end of such an object does not bound the position, so the bound has to be
    /// forwarded unconditionally.
    auto bound = std::make_shared<std::optional<size_t>>();

    ReadPipeline pipeline;
    pipeline.setSource(
        boundRecordingCreator("ABCDEFGHIJ", bound),
        StoredObjects{testObject("obj/a", StoredObject::UnknownSize)},
        ReadSettings{});
    pipeline.needGather();
    auto buf = pipeline.build();

    buf->setReadUntilPosition(4);

    String result;
    readStringUntilEOF(result, *buf);
    EXPECT_EQ(result, "ABCD");

    ASSERT_TRUE(bound->has_value());
    EXPECT_EQ(**bound, 4);
}
catch (...)
{
    FAIL() << getCurrentExceptionMessage(true);
}


TEST(ReadPipeline, GatherRejectsUnknownSizeObjectWhenAnotherFollows)
try
{
    /// An object of an unknown size must be the only one of its file: the offsets of the other objects
    /// could not be computed, so neither the right bound nor a seek could be translated into their
    /// coordinates. Reject such a layout explicitly instead of reading it at wrong offsets - the same
    /// invariant `OffsetMap::build` enforces.
    ReadPipeline pipeline;
    pipeline.setSource(
        perObjectCreator({
            {"obj/a", "ABCDEFGHIJ"},
            {"obj/b", "XYZ"},
        }),
        StoredObjects{testObject("obj/a", StoredObject::UnknownSize), testObject("obj/b", 3)},
        ReadSettings{});
    pipeline.needGather();

    EXPECT_THROW(pipeline.build(), Exception);
}
catch (...)
{
    FAIL() << getCurrentExceptionMessage(true);
}


TEST(ReadPipeline, GatherRejectsUnknownSizeObjectAfterAnother)
try
{
    /// The same layout with the object of an unknown size at the end. It is rejected as well: the size
    /// is `UINT64_MAX`, so `initialize` wraps `start_offset` around when it walks past it, and a `seek`
    /// to the start of that object reports EOF instead of reading it.
    ReadPipeline pipeline;
    pipeline.setSource(
        perObjectCreator({
            {"obj/a", "ABC"},
            {"obj/b", "XYZ"},
        }),
        StoredObjects{testObject("obj/a", 3), testObject("obj/b", StoredObject::UnknownSize)},
        ReadSettings{});
    pipeline.needGather();

    EXPECT_THROW(pipeline.build(), Exception);
}
catch (...)
{
    FAIL() << getCurrentExceptionMessage(true);
}


TEST(ReadPipeline, GatherEmptyRangeAtFileStart)
try
{
    /// An empty range at offset zero. A `0` right bound is indistinguishable from "no bound" when the
    /// bound is stored in a plain `size_t`, and the read then returns the contents of the first object
    /// instead of EOF.
    auto bound = std::make_shared<std::optional<size_t>>();

    ReadPipeline pipeline;
    pipeline.setSource(boundRecordingCreator("ABCDEFGHIJ", bound), StoredObjects{testObject("obj/a", 10)}, ReadSettings{});
    pipeline.needGather();
    auto buf = pipeline.build();

    buf->seek(0, SEEK_SET);
    buf->setReadUntilPosition(0);

    String result;
    readStringUntilEOF(result, *buf);
    EXPECT_EQ(result, "");

    /// Nothing was requested from object storage at all.
    EXPECT_FALSE(bound->has_value());
}
catch (...)
{
    FAIL() << getCurrentExceptionMessage(true);
}


TEST(ReadPipeline, GatherEmptyRangeAtObjectBoundary)
try
{
    /// An empty range at the boundary between two objects: the position is already at the right bound,
    /// so nothing must be read from the object that starts exactly there.
    auto creator = perObjectCreator({
        {"obj/a", "AAA"},
        {"obj/b", "BBB"},
    });

    ReadPipeline pipeline;
    pipeline.setSource(
        std::move(creator),
        StoredObjects{testObject("obj/a", 3), testObject("obj/b", 3)},
        ReadSettings{});
    pipeline.needGather();
    auto buf = pipeline.build();

    buf->seek(3, SEEK_SET);
    buf->setReadUntilPosition(3);

    String result;
    readStringUntilEOF(result, *buf);
    EXPECT_EQ(result, "");
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


TEST(ReadPipeline, FilesystemCacheSkipsUnknownSizeObject)
try
{
    const String data = "unknown size data";
    const String cache_name = "read_pipeline_unknown_size";
    auto cache = createTestFileCache(cache_name);
    SCOPE_EXIT({
        FileCacheFactory::instance().clear();
        fs::remove_all(fs::current_path() / (cache_name + "_cache"));
    });

    ReadPipeline pipeline;
    pipeline.setSource(memoryCreator(data), StoredObjects{testObject(StoredObject::UnknownSize)}, ReadSettings{});
    pipeline.needFilesystemCache(cache, FilesystemCacheSettings{});

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


TEST(ReadPipeline, MemoryCacheSkipsUnknownSizeObject)
try
{
    /// Regression: an object served without `Content-Length` arrives with `UnknownSize`.
    /// The page cache addresses the file by absolute offset and reads `getFileSize()` up front
    /// (`CachedInMemoryReadBufferFromFile`), so wrapping such an object would throw
    /// `UNKNOWN_FILE_SIZE` before any bytes are read. `wrapMemoryCache` must skip the page-cache
    /// stage for unknown-size objects and stream them directly. This mirrors the `DiskObjectStorage`
    /// read path (e.g. a `web_index` disk entry whose origin sent no `Content-Length`) reading with
    /// `use_page_cache_for_disks_without_file_cache = 1`, which enables the page cache without
    /// checking object size; the object storage source already disables it for unknown sizes.
    const String data = "unknown size data routed through the page cache stage";

    auto page_cache = std::make_shared<PageCache>(
        std::chrono::milliseconds(0),
        "SLRU",
        /*size_ratio=*/ 0.5,
        /*min_size_in_bytes=*/ 0,
        /*max_size_in_bytes=*/ 1 << 20,
        /*free_memory_ratio=*/ 0.0,
        /*num_shards=*/ 1);

    PageCacheSettings page_cache_settings;
    page_cache_settings.cache = page_cache;

    ReadPipeline pipeline;
    pipeline.setSource(memoryCreator(data), StoredObjects{testObject(StoredObject::UnknownSize)}, ReadSettings{});
    pipeline.needGather();
    pipeline.needMemoryCache("test:", page_cache_settings);

    /// Without the guard, build() throws `UNKNOWN_FILE_SIZE` while constructing the page-cache wrapper.
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
