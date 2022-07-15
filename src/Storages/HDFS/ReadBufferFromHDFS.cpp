#include "ReadBufferFromHDFS.h"

#if USE_HDFS
#include <Storages/HDFS/HDFSCommon.h>
#include <Common/Throttler.h>
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
    extern const int UNKNOWN_FILE_SIZE;
}


ReadBufferFromHDFS::~ReadBufferFromHDFS() = default;

struct ReadBufferFromHDFS::ReadBufferFromHDFSImpl : public BufferWithOwnMemory<SeekableReadBuffer>
{
    String hdfs_uri;
    String hdfs_file_path;

    hdfsFile fin;
    HDFSBuilderWrapper builder;
    HDFSFSPtr fs;
    ReadSettings read_settings;

    off_t file_offset = 0;
    off_t read_until_position = 0;

    explicit ReadBufferFromHDFSImpl(
        const std::string & hdfs_uri_,
        const std::string & hdfs_file_path_,
        const Poco::Util::AbstractConfiguration & config_,
        const ReadSettings & read_settings_,
        size_t read_until_position_)
        : BufferWithOwnMemory<SeekableReadBuffer>(read_settings_.remote_fs_buffer_size)
        , hdfs_uri(hdfs_uri_)
        , hdfs_file_path(hdfs_file_path_)
        , builder(createHDFSBuilder(hdfs_uri_, config_))
        , read_settings(read_settings_)
        , read_until_position(read_until_position_)
    {
        fs = createHDFSFS(builder.get());
        fin = hdfsOpenFile(fs.get(), hdfs_file_path.c_str(), O_RDONLY, 0, 0, 0);

        if (fin == nullptr)
            throw Exception(ErrorCodes::CANNOT_OPEN_FILE,
                "Unable to open HDFS file: {}. Error: {}",
                hdfs_uri + hdfs_file_path, std::string(hdfsGetLastError()));
    }

    ~ReadBufferFromHDFSImpl() override
    {
        hdfsCloseFile(fs.get(), fin);
    }

    size_t getFileSize() const
    {
        auto * file_info = hdfsGetPathInfo(fs.get(), hdfs_file_path.c_str());
        if (!file_info)
            throw Exception(ErrorCodes::UNKNOWN_FILE_SIZE, "Cannot find out file size for: {}", hdfs_file_path);
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

        int bytes_read = hdfsRead(fs.get(), fin, internal_buffer.begin(), num_bytes_to_read);
        if (bytes_read < 0)
            throw Exception(ErrorCodes::NETWORK_ERROR,
                "Fail to read from HDFS: {}, file path: {}. Error: {}",
                hdfs_uri, hdfs_file_path, std::string(hdfsGetLastError()));

        if (bytes_read)
        {
            working_buffer = internal_buffer;
            working_buffer.resize(bytes_read);
            file_offset += bytes_read;
            if (read_settings.remote_throttler)
                read_settings.remote_throttler->add(bytes_read);
            return true;
        }

        return false;
    }

    off_t seek(off_t file_offset_, int whence) override
    {
        if (whence != SEEK_SET)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Only SEEK_SET is supported");

        int seek_status = hdfsSeek(fs.get(), fin, file_offset_);
        if (seek_status != 0)
            throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Fail to seek HDFS file: {}, error: {}", hdfs_uri, std::string(hdfsGetLastError()));
        file_offset = file_offset_;
        resetWorkingBuffer();
        return file_offset;
    }

    off_t getPosition() override
    {
        return file_offset;
    }
};

ReadBufferFromHDFS::ReadBufferFromHDFS(
        const String & hdfs_uri_,
        const String & hdfs_file_path_,
        const Poco::Util::AbstractConfiguration & config_,
        const ReadSettings & read_settings_,
        size_t read_until_position_)
    : ReadBufferFromFileBase(read_settings_.remote_fs_buffer_size, nullptr, 0)
    , impl(std::make_unique<ReadBufferFromHDFSImpl>(
               hdfs_uri_, hdfs_file_path_, config_, read_settings_, read_until_position_))
{
}

size_t ReadBufferFromHDFS::getFileSize()
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

IAsynchronousReader::Result ReadBufferFromHDFS::readInto(char * data, size_t size, size_t offset, size_t /*ignore*/)
{
    /// TODO: we don't need to copy if there is no pending data
    seek(offset, SEEK_SET);
    if (eof())
        return {0, 0};

    /// Make sure returned size no greater than available bytes in working_buffer
    size_t count = std::min(size, available());
    memcpy(data, position(), count);
    position() += count;
    return {count, 0};
}

String ReadBufferFromHDFS::getFileName() const
{
    return impl->hdfs_file_path;
}

}

#endif
