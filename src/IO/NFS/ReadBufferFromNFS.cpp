#include <IO/NFS/ReadBufferFromNFS.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <Common/Stopwatch.h>
#include <Common/hex.h>
#include <Common/logger_useful.h>
#include <base/errnoToString.h>
#include <base/sleep.h>

#include <sys/stat.h>
#include <mutex>
#include <unistd.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
    extern const int LOGICAL_ERROR;
    extern const int FILE_DOESNT_EXIST;
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
}

struct ReadBufferFromNFS::ReadBufferFromNFSImpl : public BufferWithOwnMemory<SeekableReadBuffer>
{
    String nfs_file_path;
    ReadSettings read_settings;
    UInt64 max_single_read_retries;
    off_t read_until_position = 0;
    off_t file_offset_of_buffer_end = 0;
    int fd;
    bool use_pread = false;
    String uuid;

    explicit ReadBufferFromNFSImpl(
        const String & nfs_file_path_,
        const ReadSettings & settings_,
        UInt64 max_single_read_retries_,
        size_t buf_size_,
        size_t read_until_position_)
        : BufferWithOwnMemory<SeekableReadBuffer>(buf_size_)
        , nfs_file_path(nfs_file_path_)
        , read_settings(settings_)
        , max_single_read_retries(max_single_read_retries_)
        , read_until_position(read_until_position_)
    {
#ifdef __APPLE__
        bool o_direct = (flags != -1) && (flags & O_DIRECT);
        if (o_direct)
            flags = flags & ~O_DIRECT;
#endif
        int flags = O_RDONLY | O_CLOEXEC;
        fd = ::open(nfs_file_path.c_str(), flags);
        if (-1 == fd)
            throwFromErrnoWithPath("Cannot open file " + nfs_file_path, nfs_file_path,
                                    errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);

#ifdef __APPLE__
        if (o_direct)
        {
            if (fcntl(fd, F_NOCACHE, 1) == -1)
                throwFromErrnoWithPath("Cannot set F_NOCACHE on file " + nfs_file_path, nfs_file_path, ErrorCodes::CANNOT_OPEN_FILE);
        }
#endif
    }

    ~ReadBufferFromNFSImpl() override
    {
        if (fd < 0)
            return;
        ::close(fd);
    }

    bool nextImpl() override
    {
        size_t num_bytes_to_read = internal_buffer.size();
        if (read_until_position)
        {
            /// File EOF
            if (file_offset_of_buffer_end == read_until_position)
                return false;

            if (read_until_position < file_offset_of_buffer_end)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to read beyond right file_offset_of_buffer_end ({} > {})",
                    file_offset_of_buffer_end, read_until_position - 1);

            if (read_until_position - getPosition() < static_cast<off_t>(internal_buffer.size()))
                num_bytes_to_read = read_until_position - getPosition();
        }

        ssize_t bytes_read = 0;

        if (use_pread)
            bytes_read = ::pread(fd, internal_buffer.begin(), num_bytes_to_read, file_offset_of_buffer_end);
        else
            bytes_read = ::read(fd, internal_buffer.begin(), num_bytes_to_read);

        if (bytes_read < 0 && errno != EINTR)
        {
            throw Exception(ErrorCodes::NETWORK_ERROR,
                "Fail to read from NFS file path {}, fd {}, bytes_read {}."
                " offset {}, internal_buffer size {}, num_bytes_to_read {}, file size {}."
                " Error: {}",
                nfs_file_path, fd, bytes_read,
                file_offset_of_buffer_end, internal_buffer.size(), num_bytes_to_read, getTotalSize().value(),
                errnoToString(errno));
        }

        if (bytes_read > 0)
        {
            file_offset_of_buffer_end += bytes_read;
            working_buffer = internal_buffer;
            working_buffer.resize(bytes_read);
            return true;
        }
        else
            return false;
    }

    /// If 'offset_' is small enough to stay in buffer after seek, then true seek in file does not happen.
    /// The 'offset_' is the position of remote file, if not the position(offset) of buffer end.
    off_t seek(off_t offset_, int) override
    {
        resetWorkingBuffer();
        /// In case of using 'pread' we just update the info about the next position in file.
        /// In case of using 'read' we call 'lseek'.
        /// We account both cases as seek event as it leads to non-contiguous reads from file.
        if (!use_pread)
        {
            off_t res = ::lseek(fd, offset_, SEEK_SET);
            if (res == -1)
                throwFromErrnoWithPath("Cannot seek through file " + nfs_file_path, nfs_file_path,
                    ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

            /// Also note that seeking past the file size is not allowed.
            if (res != offset_)
                throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE,
                    "The 'lseek' syscall returned value ({}) that is not expected ({})", res, offset_);
        }
        file_offset_of_buffer_end = offset_;
        return file_offset_of_buffer_end;
    }

    std::optional<size_t> getTotalSize()
    {
        struct stat statbuff;
        if(::stat(nfs_file_path.c_str(), &statbuff) >= 0)
            return statbuff.st_size;
        else
            return std::nullopt;
    }

    size_t getFileOffsetOfBufferEnd() const override
    {
        return file_offset_of_buffer_end;
    }

    off_t getPosition() override
    {
        return file_offset_of_buffer_end;
    }

    /// [left, right) range
    void setReadUntilPosition(size_t position) override
    {
        if (position != static_cast<size_t>(read_until_position))
        {
            if (position <= getTotalSize().value())
            {
                read_until_position = position;
                resetWorkingBuffer();
            }
        }
    }

    SeekableReadBuffer::Range getRemainingReadRange() const override
    {
        auto range = Range{ .left = static_cast<size_t>(file_offset_of_buffer_end), .right = read_until_position };
        return range;
    }

    String getInfoForLog() override
    {
        auto info = String("NFSImplBufferInfo path ") + nfs_file_path
            + ", class address " + getHexUIntLowercase(static_cast<void*>(this));

        info += ", file size " + toString(getTotalSize().value())
        + ", file position " + toString(getPosition() - available())
        + ", readUntilPosition " + toString(getRemainingReadRange().right.value())
        + ", offset " + toString(offset())
        + ", available " + toString(available())
        + ", buffer size " + toString(internalBuffer().size())
        + ", buffer address " + getHexUIntLowercase(static_cast<void*>(internalBuffer().begin()));
        return info;
    }
};

ReadBufferFromNFS::ReadBufferFromNFS(
    const String & nfs_file_path_,
    const ReadSettings & settings_,
    UInt64 max_single_read_retries_,
    size_t buf_size_,
    size_t read_until_position_)
    : ReadBufferFromFileBase(settings_.remote_fs_buffer_size, nullptr, 0)
    , impl(std::make_unique<ReadBufferFromNFSImpl>(nfs_file_path_, settings_, max_single_read_retries_, buf_size_, read_until_position_))
    , log(&Poco::Logger::get("ReadBufferFromNFS"))
{
    file_size = 0;
    LOG_TEST(log, "Constructor {}", getInfoForLog());
}

ReadBufferFromNFS::~ReadBufferFromNFS()
{
    LOG_TEST(log, "~Constructor {}", getInfoForLog());
};

bool ReadBufferFromNFS::nextImpl()
{
    // The calling function directly sets Position(), so you need to set the position of impl
    impl->position() = impl->buffer().begin() + offset();
    if (impl->position() - impl->buffer().end() > 0)
        impl->position() = impl->buffer().end();

    auto result = impl->next();
    if (result)
        BufferBase::set(impl->buffer().begin(), impl->buffer().size(), impl->file_offset_of_buffer_end); /// use the buffer returned by `impl`

    //LOG_TEST(log, "NextImpl {}", getInfoForLog());
    return result;
}

/// If 'offset_' is small enough to stay in buffer after seek, then true seek in file does not happen.
/// The 'offset_' is the position of remote file, if not the position(offset) of buffer end.
off_t ReadBufferFromNFS::seek(off_t offset_, int whence)
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
    }
    else
    {
        resetWorkingBuffer();
        impl->seek(offset_, whence);
    }
    //LOG_TEST(log, "Seek offset {}, {}", offset_, getInfoForLog());
    return impl->getPosition();
}

/// [left, right) range
void ReadBufferFromNFS::setReadUntilPosition(size_t position)
{
    impl->setReadUntilPosition(position);
    //LOG_TEST(log, "setReadUntilPosition {}. {}", position, getInfoForLog());
}

std::optional<size_t> ReadBufferFromNFS::getTotalSize()
{
    if (file_size > 0)
        return file_size;
    auto result = impl->getTotalSize();
    if (result.has_value())
    {
        file_size = result.value();
    }
    return result;
}

size_t ReadBufferFromNFS::getFileOffsetOfBufferEnd() const
{
    return impl->getFileOffsetOfBufferEnd();
}

off_t ReadBufferFromNFS::getPosition()
{
    auto position = impl->getPosition() - available();
    //LOG_TEST(log, "getPosition {}. {}", position, getInfoForLog());
    return position;
}

SeekableReadBuffer::Range ReadBufferFromNFS::getRemainingReadRange() const
{
    return impl->getRemainingReadRange();
}

String ReadBufferFromNFS::getFileName() const
{
    return impl->nfs_file_path;
}

String ReadBufferFromNFS::getInfoForLog()
{
    auto info = String("NFSBufferInfo path ") + impl->nfs_file_path
        + ", class address " + getHexUIntLowercase(static_cast<void*>(this));
    if (impl != nullptr)
    {
        info += ", file size " + toString(getTotalSize().value())
        + ", file position " + toString(impl->getPosition() - available())
        + ", readUntilPosition " + toString(impl->getRemainingReadRange().right.value())
        + ", offset " + toString(offset())
        + ", available " + toString(available())
        + ", buffer size " + toString(impl->internalBuffer().size())
        + ", buffer address " + getHexUIntLowercase(static_cast<void*>(impl->internalBuffer().begin()))
        + ". " + impl->getInfoForLog();
    }

    return info;
}

}

