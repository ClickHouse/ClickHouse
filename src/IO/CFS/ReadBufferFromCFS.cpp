#include <IO/CFS/ReadBufferFromCFS.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <base/errnoToString.h>
#include <base/sleep.h>
#include <base/hex.h>

#include <sys/stat.h>
#include <mutex>
#include <unistd.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_OPEN_FILE;
    extern const int FILE_DOESNT_EXIST;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
    extern const int CANNOT_GET_SIZE_OF_FIELD;
}

#define POINT_TO_STRING(p) getHexUIntLowercase(reinterpret_cast<uint64_t>(static_cast<void*>(p)))

struct ReadBufferFromCFS::ReadBufferFromCFSImpl : public BufferWithOwnMemory<SeekableReadBuffer>
{
    String cfs_file_path;
    ReadSettings read_settings;
    UInt64 max_single_read_retries;
    std::atomic<off_t> file_offset_of_init = 0;
    std::atomic<off_t> file_offset_of_buffer_end = 0;
    std::atomic<off_t> read_until_position = 0;
    int fd;
    bool use_pread = false;
    String uuid;

    Poco::Logger* log = &Poco::Logger::get("ReadBufferFromCFSImpl");

    explicit ReadBufferFromCFSImpl(
        const String & cfs_file_path_,
        const ReadSettings & settings_,
        UInt64 max_single_read_retries_,
        size_t offset_,
        size_t read_until_position_,
        bool use_external_buffer_)
        : BufferWithOwnMemory<SeekableReadBuffer>(use_external_buffer_ ? 0 : settings_.remote_fs_buffer_size)
        , cfs_file_path(cfs_file_path_)
        , read_settings(settings_)
        , max_single_read_retries(max_single_read_retries_)
        , file_offset_of_init(offset_)
        , file_offset_of_buffer_end(offset_)
        , read_until_position(read_until_position_)
    {
#ifdef __APPLE__
        bool o_direct = (flags != -1) && (flags & O_DIRECT);
        if (o_direct)
            flags = flags & ~O_DIRECT;
#endif
        int flags = O_RDONLY | O_CLOEXEC;
        fd = ::open(cfs_file_path.c_str(), flags);
        if (-1 == fd)
            throwFromErrnoWithPath("Cannot open file " + cfs_file_path, cfs_file_path,
                                    errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);

#ifdef __APPLE__
        if (o_direct)
        {
            if (fcntl(fd, F_NOCACHE, 1) == -1)
                throwFromErrnoWithPath("Cannot set F_NOCACHE on file " + cfs_file_path, cfs_file_path, ErrorCodes::CANNOT_OPEN_FILE);
        }
#endif
    }

    ~ReadBufferFromCFSImpl() override
    {
        if (fd < 0)
            return;
        int err = ::close(fd);
        chassert(!err || errno == EINTR);
        fd = -1;
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
                "Fail to read from CFS file path {}, fd {}, bytes_read {}."
                " offset {}, internal_buffer size {}, num_bytes_to_read {}, file size {}."
                " Error: {}",
                cfs_file_path, fd, bytes_read,
                file_offset_of_buffer_end, internal_buffer.size(), num_bytes_to_read, getFileSize(),
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
                throwFromErrnoWithPath("Cannot seek through file " + cfs_file_path, cfs_file_path,
                    ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

            /// Also note that seeking past the file size is not allowed.
            if (res != offset_)
                throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE,
                    "The 'lseek' syscall returned value ({}) that is not expected ({})", res, offset_);
        }
        file_offset_of_init = offset_;
        file_offset_of_buffer_end = offset_;
        return file_offset_of_buffer_end;
    }

    size_t getFileSize()
    {
        struct stat statbuff;
        if (::stat(cfs_file_path.c_str(), &statbuff) >= 0)
            return statbuff.st_size;
        else
        {
            if (errno != EINTR)
                throw Exception(ErrorCodes::CANNOT_GET_SIZE_OF_FIELD,
                    "Can't get file size by path {}, error {}:{}", cfs_file_path, errno, errnoToString(errno));
            return 0;
        }
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
            if (position <= getFileSize())
            {
                read_until_position = position;
                resetWorkingBuffer();
                LOG_TEST(log, "setReadUntilPosition {}. {}", position, getInfoForLog());
            }
        }
    }

    String getInfoForLog() override
    {
        auto info = String("CFSImplBufferInfo path ") + cfs_file_path
            + ", class address " + POINT_TO_STRING(this)
            + ", file size " + toString(getFileSize())
            + ", file offset " + toString(file_offset_of_init.load())
            + ", readUntilPosition " + toString(read_until_position.load())
            + ", file position " + toString(getFileOffsetOfBufferEnd() - available())
            + ", offset " + toString(offset())
            + ", available " + toString(available())
            + ", buffer size " + toString(internalBuffer().size())
            + ", buffer address " + POINT_TO_STRING(internalBuffer().begin());
        return info;
    }
};

ReadBufferFromCFS::ReadBufferFromCFS(
    const String & cfs_file_path_,
    const ReadSettings & settings_,
    UInt64 max_single_read_retries_,
    size_t offset_,
    size_t read_until_position_,
    bool use_external_buffer_)
    : ReadBufferFromFileBase(use_external_buffer_ ? settings_.remote_fs_buffer_size : 0, nullptr, 0)
    , use_external_buffer(use_external_buffer_)
    , impl(std::make_unique<ReadBufferFromCFSImpl>(cfs_file_path_, settings_, max_single_read_retries_,
        offset_, read_until_position_, use_external_buffer_))
    , log(&Poco::Logger::get("ReadBufferFromCFS"))
{
}

ReadBufferFromCFS::~ReadBufferFromCFS()
{
};

bool ReadBufferFromCFS::nextImpl()
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

    LOG_TEST(log, "nextImpl {}", getInfoForLog());
    return result;
}

/// If 'offset_' is small enough to stay in buffer after seek, then true seek in file does not happen.
/// The 'offset_' is the position of remote file, if not the position(offset) of buffer end.
off_t ReadBufferFromCFS::seek(off_t offset_, int whence)
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
    }
    else
    {
        resetWorkingBuffer();
        impl->seek(offset_, whence);
    }
    LOG_TEST(log, "seek offset {}, {}", offset_, getInfoForLog());
    return impl->getPosition();
}

/// [left, right) range
void ReadBufferFromCFS::setReadUntilPosition(size_t position)
{
    impl->setReadUntilPosition(position);
}

size_t ReadBufferFromCFS::getFileSize()
{
    if (!file_size)
    {
        auto result = impl->getFileSize();
        file_size = result;
    }
    return file_size.value();
}

size_t ReadBufferFromCFS::getFileOffsetOfBufferEnd() const
{
    return impl->getFileOffsetOfBufferEnd();
}

off_t ReadBufferFromCFS::getPosition()
{
    auto position = impl->getPosition() - available();
    LOG_TEST(log, "getPosition {}. {}", position, getInfoForLog());
    return position;
}

String ReadBufferFromCFS::getFileName() const
{
    return impl->cfs_file_path;
}

String ReadBufferFromCFS::getInfoForLog()
{
    auto info = String("CFSBufferInfo path ") + impl->cfs_file_path
        + ", class address " + POINT_TO_STRING(this);
    if (impl != nullptr)
    {
        info += ", file size " + toString(getFileSize())
        + ", file offset " + toString(impl->file_offset_of_init.load())
        + ", file position " + toString(impl->getPosition() - available())
        + ", buffer offset " + toString(offset())
        + ", buffer available " + toString(available())
        + ", buffer size " + toString(impl->internalBuffer().size())
        + ", buffer address " + POINT_TO_STRING(impl->internalBuffer().begin())
        + ". " + impl->getInfoForLog();
    }

    return info;
}

}
