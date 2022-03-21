#include "ReadBufferFromHDFS.h"

#if USE_HDFS
#include <Storages/HDFS/HDFSCommon.h>
#include <hdfs/hdfs.h>
#include <mutex>


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
    /// HDFS create/open functions are not thread safe
    static std::mutex hdfs_init_mutex;

    String hdfs_uri;
    String hdfs_file_path;

    hdfsFile fin;
    HDFSBuilderWrapper builder;
    HDFSFSPtr fs;

    off_t offset = 0;
    off_t read_until_position = 0;

    explicit ReadBufferFromHDFSImpl(
        const std::string & hdfs_uri_,
        const std::string & hdfs_file_path_,
        const Poco::Util::AbstractConfiguration & config_,
        size_t buf_size_, size_t read_until_position_)
        : BufferWithOwnMemory<SeekableReadBuffer>(buf_size_)
        , hdfs_uri(hdfs_uri_)
        , hdfs_file_path(hdfs_file_path_)
        , builder(createHDFSBuilder(hdfs_uri_, config_))
        , read_until_position(read_until_position_)
    {
        std::lock_guard lock(hdfs_init_mutex);

        fs = createHDFSFS(builder.get());
        fin = hdfsOpenFile(fs.get(), hdfs_file_path.c_str(), O_RDONLY, 0, 0, 0);

        if (fin == nullptr)
            throw Exception(ErrorCodes::CANNOT_OPEN_FILE,
                "Unable to open HDFS file: {}. Error: {}",
                hdfs_uri + hdfs_file_path, std::string(hdfsGetLastError()));
    }

    ~ReadBufferFromHDFSImpl() override
    {
        std::lock_guard lock(hdfs_init_mutex);
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
        size_t num_bytes_to_read;
        if (read_until_position)
        {
            if (read_until_position == offset)
                return false;

            if (read_until_position < offset)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to read beyond right offset ({} > {})", offset, read_until_position - 1);

            num_bytes_to_read = read_until_position - offset;
        }
        else
        {
            num_bytes_to_read = internal_buffer.size();
        }

        int bytes_read = hdfsRead(fs.get(), fin, internal_buffer.begin(), num_bytes_to_read);
        if (bytes_read < 0)
            throw Exception(ErrorCodes::NETWORK_ERROR,
                "Fail to read from HDFS: {}, file path: {}. Error: {}",
                hdfs_uri, hdfs_file_path, std::string(hdfsGetLastError()));

        if (bytes_read)
        {
            working_buffer = internal_buffer;
            working_buffer.resize(bytes_read);
            offset += bytes_read;
            return true;
        }

        return false;
    }

    off_t seek(off_t offset_, int whence) override
    {
        if (whence != SEEK_SET)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Only SEEK_SET is supported");

        offset = offset_;
        int seek_status = hdfsSeek(fs.get(), fin, offset);
        if (seek_status != 0)
            throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Fail to seek HDFS file: {}, error: {}", hdfs_uri, std::string(hdfsGetLastError()));
        return offset;
    }

    off_t getPosition() override
    {
        return offset;
    }
};


std::mutex ReadBufferFromHDFS::ReadBufferFromHDFSImpl::hdfs_init_mutex;

ReadBufferFromHDFS::ReadBufferFromHDFS(
        const String & hdfs_uri_,
        const String & hdfs_file_path_,
        const Poco::Util::AbstractConfiguration & config_,
        size_t buf_size_, size_t read_until_position_)
    : SeekableReadBufferWithSize(nullptr, 0)
    , impl(std::make_unique<ReadBufferFromHDFSImpl>(hdfs_uri_, hdfs_file_path_, config_, buf_size_, read_until_position_))
{
}

std::optional<size_t> ReadBufferFromHDFS::getTotalSize()
{
    return impl->getTotalSize();
}

bool ReadBufferFromHDFS::nextImpl()
{
    impl->position() = impl->buffer().begin() + offset();
    auto result = impl->next();

    if (result)
        BufferBase::set(impl->buffer().begin(), impl->buffer().size(), impl->offset); /// use the buffer returned by `impl`

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

}

#endif
