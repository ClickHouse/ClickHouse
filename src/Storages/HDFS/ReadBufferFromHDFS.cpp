#include "ReadBufferFromHDFS.h"

#if USE_HDFS
#include <Storages/HDFS/HDFSCommon.h>
#include <hdfs/hdfs.h>
#include <mutex>


namespace ProfileEvents
{
    extern const Event HDFSReadInitializeMicroseconds;
    extern const Event HDFSReadInitialize;
    extern const Event HDFSReadExecuteMicroseconds;
    extern const Event HDFSReadExecute;
    extern const Event HDFSReadSeekMicroseconds;
    extern const Event HDFSReadSeek;
    extern const Event HDFSReadCloseMicroseconds;
    extern const Event HDFSReadClose;
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

ReadBufferFromHDFS::~ReadBufferFromHDFS() = default;

struct ReadBufferFromHDFS::ReadBufferFromHDFSImpl : public BufferWithOwnMemory<SeekableReadBuffer>
{

    String hdfs_uri;
    String hdfs_file_path;

    const HDFSBuilderFSFactory & factory;
    HDFSBuilderWrapperPtr builder;
    HDFSFSSharedPtr fs;
    hdfsFile fin;

    off_t file_offset = 0;
    off_t read_until_position = 0;

    explicit ReadBufferFromHDFSImpl(
        const std::string & hdfs_uri_,
        const std::string & hdfs_file_path_,
        const Poco::Util::AbstractConfiguration & config_,
        size_t buf_size_,
        size_t file_offset_,
        size_t read_until_position_)
        : BufferWithOwnMemory<SeekableReadBuffer>(buf_size_)
        , hdfs_uri(hdfs_uri_)
        , hdfs_file_path(hdfs_file_path_)
        , factory(HDFSBuilderFSFactory::instance())
        , builder(factory.getBuilder(hdfs_uri_, config_))
        , file_offset(file_offset_)
        , read_until_position(read_until_position_)
    {
        Stopwatch watch;
        assert(builder && builder->get());

        fs = factory.getFS(builder);
        if (!factory.tryCallFS(
                builder,
                fs,
                [&](HDFSFSSharedPtr & fs_) -> bool
                {
                    fin = hdfsOpenFile(fs_.get(), hdfs_file_path.c_str(), O_RDONLY, 0, 0, 0);
                    return fin != nullptr;
                }))
            throw Exception(
                ErrorCodes::CANNOT_OPEN_FILE,
                "Unable to open HDFS file: {}. Error: {}",
                hdfs_uri + hdfs_file_path,
                std::string(hdfsGetLastError()));

        if (file_offset)
        {
            Stopwatch seek_watch;
            int seek_status = hdfsSeek(fs.get(), fin, file_offset);
            if (seek_status != 0)
                throw Exception(
                    ErrorCodes::CANNOT_SEEK_THROUGH_FILE,
                    "Fail to seek HDFS file: {}, error: {}",
                    hdfs_uri,
                    std::string(hdfsGetLastError()));

            ProfileEvents::increment(ProfileEvents::HDFSReadSeekMicroseconds, watch.elapsedMicroseconds());
            ProfileEvents::increment(ProfileEvents::HDFSReadSeek, 1);
        }

        ProfileEvents::increment(ProfileEvents::HDFSReadInitializeMicroseconds, watch.elapsedMicroseconds());
        ProfileEvents::increment(ProfileEvents::HDFSReadInitialize, 1);
    }

    ~ReadBufferFromHDFSImpl() override
    {
        Stopwatch watch;
        hdfsCloseFile(fs.get(), fin);

        ProfileEvents::increment(ProfileEvents::HDFSReadCloseMicroseconds, watch.elapsedMicroseconds());
        ProfileEvents::increment(ProfileEvents::HDFSReadClose, 1);
    }

    std::optional<size_t> getFileSize() const
    {
        auto * file_info = hdfsGetPathInfo(fs.get(), hdfs_file_path.c_str());
        if (!file_info)
            return std::nullopt;
        return file_info->mSize;
    }

    bool nextImpl() override
    {
        size_t num_bytes_to_read;
        if (read_until_position)
        {
            if (read_until_position == file_offset)
                return false;

            if (read_until_position < file_offset)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to read beyond right offset ({} > {})", file_offset, read_until_position - 1);

            num_bytes_to_read = read_until_position - file_offset;
        }
        else
        {
            num_bytes_to_read = internal_buffer.size();
        }

        Stopwatch watch;
        int bytes_read = hdfsRead(fs.get(), fin, internal_buffer.begin(), num_bytes_to_read);
        if (bytes_read < 0)
            throw Exception(ErrorCodes::NETWORK_ERROR,
                "Fail to read from HDFS: {}, file path: {}. Error: {}",
                hdfs_uri, hdfs_file_path, std::string(hdfsGetLastError()));

        ProfileEvents::increment(ProfileEvents::HDFSReadExecuteMicroseconds, watch.elapsedMicroseconds());
        ProfileEvents::increment(ProfileEvents::HDFSReadExecute, 1);

        if (bytes_read)
        {
            working_buffer = internal_buffer;
            working_buffer.resize(bytes_read);
            file_offset += bytes_read;
            return true;
        }

        return false;
    }

    off_t seek(off_t file_offset_, int whence) override
    {
        if (whence != SEEK_SET)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Only SEEK_SET is supported");

        Stopwatch watch;
        int seek_status = hdfsSeek(fs.get(), fin, file_offset_);
        if (seek_status != 0)
            throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Fail to seek HDFS file: {}, error: {}", hdfs_uri, std::string(hdfsGetLastError()));

        ProfileEvents::increment(ProfileEvents::HDFSReadSeekMicroseconds, watch.elapsedMicroseconds());
        ProfileEvents::increment(ProfileEvents::HDFSReadSeek, 1);

        file_offset = file_offset_;
        resetWorkingBuffer();
        return file_offset;
    }

    off_t getPosition() override
    {
        return file_offset;
    }

    Range getRemainingReadRange() const override
    {
        return Range{
            .left = static_cast<size_t>(file_offset), .right = read_until_position ? std::optional{read_until_position - 1} : std::nullopt};
    }
};

ReadBufferFromHDFS::ReadBufferFromHDFS(
    const String & hdfs_uri_,
    const String & hdfs_file_path_,
    const Poco::Util::AbstractConfiguration & config_,
    size_t buf_size_,
    size_t file_offset_,
    size_t read_until_position_)
    : SeekableReadBuffer(nullptr, 0)
    , impl(std::make_unique<ReadBufferFromHDFSImpl>(hdfs_uri_, hdfs_file_path_, config_, buf_size_, file_offset_, read_until_position_))
{
}

std::optional<size_t> ReadBufferFromHDFS::getFileSize()
{
    return impl->getFileSize();
}

bool ReadBufferFromHDFS::nextImpl()
{
    impl->position() = impl->buffer().begin() + offset();
    auto result = impl->next();

    if (result)
        BufferBase::set(impl->buffer().begin(), impl->buffer().size(), impl->offset()); /// use the buffer returned by `impl`

    return result;
}


off_t ReadBufferFromHDFS::seek(off_t offset_, int whence)
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


off_t ReadBufferFromHDFS::getPosition()
{
    return impl->getPosition() - available();
}

size_t ReadBufferFromHDFS::getFileOffsetOfBufferEnd() const
{
    return impl->getPosition();
}

String ReadBufferFromHDFS::getFileName() const
{
    return impl->hdfs_file_path;
}

ParallelReadBuffer::Range ReadBufferFromHDFS::getRemainingReadRange() const
{
    return impl->getRemainingReadRange();
}


SeekableReadBufferPtr ReadBufferHDFSFactory::getReader()
{
    const auto next_range = range_generator.nextRange();
    if (!next_range)
        return nullptr;

    auto reader = std::make_shared<ReadBufferFromHDFS>(
        hdfs_uri,
        hdfs_file_path,
        config,
        read_settings.remote_fs_buffer_size,
        next_range->first,
        next_range->second);
    return std::move(reader);
}

off_t ReadBufferHDFSFactory::seek(off_t off, [[maybe_unused]] int whence)
{
    range_generator = RangeGenerator{file_size, range_step, static_cast<size_t>(off)};
    return off;
}

std::optional<size_t> ReadBufferHDFSFactory::getFileSize()
{
    return file_size;
}

}

#endif
