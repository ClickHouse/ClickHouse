#include "ReadBufferFromHDFS.h"

#if USE_HDFS
#include "HDFSCommon.h"
#include "HDFSErrorWrapper.h"
#include <Common/Scheduler/ResourceGuard.h>
#include <IO/Progress.h>
#include <Common/Throttler.h>
#include <Common/safe_cast.h>
#include <Common/logger_useful.h>
#include <IO/ReadSettings.h>
#include <hdfs/hdfs.h>


namespace DB
{

namespace ErrorCodes
{
extern const int HDFS_ERROR;
extern const int CANNOT_OPEN_FILE;
extern const int CANNOT_SEEK_THROUGH_FILE;
extern const int SEEK_POSITION_OUT_OF_BOUND;
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_FILE_SIZE;
}


struct ReadBufferFromHDFS::ReadBufferFromHDFSImpl : public BufferWithOwnMemory<SeekableReadBuffer>, public WithFileSize, public HDFSErrorWrapper
{
    String hdfs_uri;
    String hdfs_file_path;

    hdfsFile fin;
    HDFSFSPtr fs;
    ReadSettings read_settings;

    off_t file_offset = 0;
    off_t read_until_position = 0;
    off_t file_size;
    bool enable_pread = true;

    explicit ReadBufferFromHDFSImpl(
        const std::string & hdfs_uri_,
        const std::string & hdfs_file_path_,
        const Poco::Util::AbstractConfiguration & config_,
        const ReadSettings & read_settings_,
        size_t read_until_position_,
        bool use_external_buffer_,
        std::optional<size_t> file_size_)
        : BufferWithOwnMemory<SeekableReadBuffer>(use_external_buffer_ ? 0 : read_settings_.remote_fs_buffer_size)
        , HDFSErrorWrapper(hdfs_uri_, config_)
        , hdfs_uri(hdfs_uri_)
        , hdfs_file_path(hdfs_file_path_)
        , read_settings(read_settings_)
        , read_until_position(read_until_position_)
        , enable_pread(read_settings_.enable_hdfs_pread)
    {
        fs = createHDFSFS(builder.get());
        fin = wrapErr<hdfsFile>(hdfsOpenFile, fs.get(), hdfs_file_path.c_str(), O_RDONLY, 0, 0, 0);

        if (fin == nullptr)
            throw Exception(ErrorCodes::CANNOT_OPEN_FILE,
                "Unable to open HDFS file: {}. Error: {}",
                hdfs_uri + hdfs_file_path, std::string(hdfsGetLastError()));

        if (file_size_.has_value())
        {
            file_size = file_size_.value();
        }
        else
        {
            auto * file_info = wrapErr<hdfsFileInfo *>(hdfsGetPathInfo, fs.get(), hdfs_file_path.c_str());
            if (!file_info)
            {
                hdfsCloseFile(fs.get(), fin);
                throw Exception(ErrorCodes::UNKNOWN_FILE_SIZE, "Cannot find out file size for: {}", hdfs_file_path);
            }
            file_size = static_cast<size_t>(file_info->mSize);
            hdfsFreeFileInfo(file_info, 1);
        }
    }

    ~ReadBufferFromHDFSImpl() override
    {
        hdfsCloseFile(fs.get(), fin);
    }

    std::optional<size_t> tryGetFileSize() override
    {
        return file_size;
    }

    bool nextImpl() override
    {
        size_t num_bytes_to_read;
        if (read_until_position)
        {
            if (read_until_position == file_offset)
            {
                return false;
            }

            if (read_until_position < file_offset)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to read beyond right offset ({} > {})", file_offset, read_until_position - 1);

            num_bytes_to_read = std::min<size_t>(read_until_position - file_offset, internal_buffer.size());
        }
        else
        {
            num_bytes_to_read = internal_buffer.size();
        }
        if (file_size != 0 && file_offset >= file_size)
        {
            return false;
        }

        ResourceGuard rlock(ResourceGuard::Metrics::getIORead(), read_settings.io_scheduling.read_resource_link, num_bytes_to_read);
        int bytes_read = wrapErr<tSize>(hdfsRead, fs.get(), fin, internal_buffer.begin(), safe_cast<int>(num_bytes_to_read));
        rlock.unlock(std::max(0, bytes_read));

        if (bytes_read < 0)
        {
            throw Exception(
                ErrorCodes::HDFS_ERROR,
                "Fail to read from HDFS: {}, file path: {}. Error: {}",
                hdfs_uri,
                hdfs_file_path,
                std::string(hdfsGetLastError()));
        }

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

        int seek_status = wrapErr<int>(hdfsSeek, fs.get(), fin, file_offset_);
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

    size_t pread(char * buffer, size_t size, size_t offset)
    {
        ResourceGuard rlock(ResourceGuard::Metrics::getIORead(), read_settings.io_scheduling.read_resource_link, size);
        auto bytes_read = wrapErr<tSize>(hdfsPread, fs.get(), fin, buffer, safe_cast<int>(size), offset);
        rlock.unlock(std::max(0, bytes_read));

        if (bytes_read < 0)
        {
            throw Exception(
                ErrorCodes::HDFS_ERROR,
                "Fail to read from HDFS: {}, file path: {}. Error: {}",
                hdfs_uri,
                hdfs_file_path,
                std::string(hdfsGetLastError()));
        }
        if (bytes_read && read_settings.remote_throttler)
        {
            read_settings.remote_throttler->add(bytes_read);
        }
        return bytes_read;
    }
};


ReadBufferFromHDFS::ReadBufferFromHDFS(
        const String & hdfs_uri_,
        const String & hdfs_file_path_,
        const Poco::Util::AbstractConfiguration & config_,
        const ReadSettings & read_settings_,
        size_t read_until_position_,
        bool use_external_buffer_,
        std::optional<size_t> file_size_)
    : ReadBufferFromFileBase()
    , impl(std::make_unique<ReadBufferFromHDFSImpl>(
               hdfs_uri_, hdfs_file_path_, config_, read_settings_, read_until_position_, use_external_buffer_, file_size_))
    , use_external_buffer(use_external_buffer_)
{
}

ReadBufferFromHDFS::~ReadBufferFromHDFS() = default;

std::optional<size_t> ReadBufferFromHDFS::tryGetFileSize()
{
    return impl->tryGetFileSize();
}

bool ReadBufferFromHDFS::nextImpl()
{
    if (use_external_buffer)
    {
        impl->set(internal_buffer.begin(), internal_buffer.size());
        assert(working_buffer.begin() != nullptr);
        assert(!internal_buffer.empty());
    }
    else
    {
        impl->position() = impl->buffer().begin() + offset();
        assert(!impl->hasPendingData());
    }

    auto result = impl->next();

    if (result)
        BufferBase::set(impl->buffer().begin(), impl->buffer().size(), impl->offset()); /// use the buffer returned by `impl`

    return result;
}


off_t ReadBufferFromHDFS::seek(off_t offset_, int whence)
{
    if (whence != SEEK_SET)
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Only SEEK_SET mode is allowed.");

    if (offset_ < 0)
        throw Exception(ErrorCodes::SEEK_POSITION_OUT_OF_BOUND, "Seek position is out of bounds. Offset: {}", offset_);

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

size_t ReadBufferFromHDFS::readBigAt(char * buffer, size_t size, size_t offset, const std::function<bool(size_t)> &) const
{
    return impl->pread(buffer, size, offset);
}

bool ReadBufferFromHDFS::supportsReadAt()
{
    return impl->enable_pread;
}

}

#endif
