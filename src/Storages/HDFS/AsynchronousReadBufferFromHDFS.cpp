#include "AsynchronousReadBufferFromHDFS.h"

#if USE_HDFS
#include <mutex>
#include <Storages/HDFS/HDFSCommon.h>
#include <Storages/HDFS/ReadBufferFromHDFS.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>

namespace ProfileEvents
{
    extern const Event AsynchronousReadWaitMicroseconds;
}

namespace CurrentMetrics
{
    extern const Metric AsynchronousReadWait;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
    extern const int LOGICAL_ERROR;
}

AsynchronousReadBufferFromHDFS::~AsynchronousReadBufferFromHDFS() = default;

class AsynchronousReadBufferFromHDFS::AsynchronousReadBufferFromHDFSImpl : public BufferWithOwnMemory<SeekableReadBuffer>
{
public:
    explicit AsynchronousReadBufferFromHDFSImpl(
        std::shared_ptr<ReadBufferFromHDFS> in_,
        const std::string & hdfs_uri_,
        const std::string & hdfs_file_path_,
        const Poco::Util::AbstractConfiguration & config_,
        size_t buf_size_, size_t read_until_position_)
        : BufferWithOwnMemory<SeekableReadBuffer>(buf_size_)
        , in(in_)
        , hdfs_uri(hdfs_uri_)
        , hdfs_file_path(hdfs_file_path_)
        , builder(createHDFSBuilder(hdfs_uri_, config_))
        , read_until_position(read_until_position_)
    {
        fs = createHDFSFS(builder.get());
        fin = hdfsOpenFile(fs.get(), hdfs_file_path.c_str(), O_RDONLY, 0, 0, 0);

        if (fin == nullptr)
            throw Exception(ErrorCodes::CANNOT_OPEN_FILE,
                "Unable to open HDFS file: {}. Error: {}",
                hdfs_uri + hdfs_file_path, std::string(hdfsGetLastError()));
    }

    ~AsynchronousReadBufferFromHDFSImpl() override
    {
        hdfsCloseFile(fs.get(), fin);
    }

    std::optional<size_t> getTotalSize() const
    {
        auto * file_info = hdfsGetPathInfo(fs.get(), hdfs_file_path.c_str());
        if (!file_info)
            return std::nullopt;
        return file_info->mSize;
    }

    bool nextImpl() override
    {
        if (prefetch_future.valid())
        {
            /// Read request already in flight. Wait for its completion.
            size_t size = 0;
            size_t offset = 0;
            {
                Stopwatch watch;
                CurrentMetrics::Increment metric_increment{CurrentMetrics::AsynchronousReadWait};
                auto result = prefetch_future.get();
                size = result.size;
                offset = result.offset;
                assert(offset < size || size == 0);
                ProfileEvents::increment(ProfileEvents::AsynchronousReadWaitMicroseconds, watch.elapsedMicroseconds());
            }
            prefetch_future = {};

            if (size)
            {
                prefetch_buffer.swap(memory);
                /// Adjust the working buffer so that it ignores `offset` bytes.
                setWithBytesToIgnore(memory.data(), size, offset);
                return true;
            }
            return false;
        }
        else
        {
            /// No pending request. Do synchronous read.
            auto [size, offset] = readInto(memory.data(), memory.size()).get();
            file_offset_of_buffer_end += size;

            if (size)
            {
                /// Adjust the working buffer so that it ignores `offset` bytes.
                setWithBytesToIgnore(memory.data(), size, offset);
                return true;
            }

            return false;
        }
    }

    off_t seek(off_t file_offset_, int whence) override
    {
        if (whence != SEEK_SET)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Only SEEK_SET is supported");

        file_offset_of_buffer_end = file_offset_;
        int seek_status = hdfsSeek(fs.get(), fin, file_offset_of_buffer_end);
        if (seek_status != 0)
            throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Fail to seek HDFS file: {}, error: {}", hdfs_uri, std::string(hdfsGetLastError()));
        return file_offset_of_buffer_end;
    }

    off_t getPosition() override
    {
        return file_offset_of_buffer_end;
    }

    void prefetch() override
    {
        if (prefetch_future.valid())
            return;

        /// Will request the same amount of data that is read in nextImpl.
        prefetch_buffer.resize(internal_buffer.size());
        prefetch_future = readInto(prefetch_buffer.data(), prefetch_buffer.size());
    }

    void finalize()
    {
        if (prefetch_future.valid())
        {
            prefetch_future.wait();
            prefetch_future = {};
        }
    }

    static AsynchronousReaderPtr getThreadPoolReader()
    {
        constexpr size_t pool_size = 50;
        constexpr size_t queue_size = 1000000;
        static AsynchronousReaderPtr reader = std::make_shared<ThreadPoolRemoteFSReader<ReadBufferFromHDFS>>(pool_size, queue_size);
        return reader;
    }

private:
    std::future<IAsynchronousReader::Result> readInto(char * data, size_t size)
    {
        IAsynchronousReader::Request request;
        request.descriptor = std::make_shared<RemoteFSFileDescriptor<ReadBufferFromHDFS>>(in);
        request.buf = data;
        request.size = size;
        request.offset = file_offset_of_buffer_end;
        request.priority = priority;
        request.ignore = 0;
        return getThreadPoolReader()->submit(request);
    }

    std::shared_ptr<ReadBufferFromHDFS> in;
    Int32 priority;
    Memory<> prefetch_buffer;
    std::future<IAsynchronousReader::Result> prefetch_future;
};

AsynchronousReadBufferFromHDFS::AsynchronousReadBufferFromHDFS(
        const String & hdfs_uri_,
        const String & hdfs_file_path_,
        const Poco::Util::AbstractConfiguration & config_,
        size_t buf_size_, size_t read_until_position_)
    : SeekableReadBufferWithSize(nullptr, 0)
    , impl(std::make_unique<AsynchronousReadBufferFromHDFSImpl>(hdfs_uri_, hdfs_file_path_, config_, buf_size_, read_until_position_))
{
}

std::optional<size_t> AsynchronousReadBufferFromHDFS::getTotalSize()
{
    return impl->getTotalSize();
}

bool AsynchronousReadBufferFromHDFS::nextImpl()
{
    impl->position() = impl->buffer().begin() + offset();
    auto result = impl->next();

    if (result)
        BufferBase::set(impl->buffer().begin(), impl->buffer().size(), impl->offset()); /// use the buffer returned by `impl`

    return result;
}


off_t AsynchronousReadBufferFromHDFS::seek(off_t offset_, int whence)
{
    if (whence != SEEK_SET)
        throw Exception("Only SEEK_SET mode is allowed.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

    if (offset_ < 0)
        throw Exception("Seek position is out of bounds. Offset: " + std::to_string(offset_), ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);

    if (!working_buffer.empty()
        && size_t(offset_) >= impl->getPosition() - working_buffer.size()
        && offset_ < impl->getPosition())
    {
        pos = working_buffer.end() - (impl->getPosition() - offset_);
        assert(pos >= working_buffer.begin());
        assert(pos <= working_buffer.end());

        return getPosition();
    }

    resetWorkingBuffer();
    impl->seek(offset_, whence);
    return impl->getPosition();
}


off_t AsynchronousReadBufferFromHDFS::getPosition()
{
    return impl->getPosition() - available();
}

size_t AsynchronousReadBufferFromHDFS::getFileOffsetOfBufferEnd() const
{
    return impl->getPosition();
}

}

#endif
