#include "config.h"
#include <IO/WriteHelpers.h>
#include <IO/CFS/WriteBufferFromCFS.h>
#include <sys/uio.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_FSYNC;
    extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
    extern const int FILE_DOESNT_EXIST;
}

struct WriteBufferFromCFS::WriteBufferFromCFSImpl
{
    std::string cfs_file_path;
    const Poco::Util::AbstractConfiguration & config;
    WriteSettings write_settings;
    int fd;

    WriteBufferFromCFSImpl(
            const std::string & cfs_file_path_,
            const Poco::Util::AbstractConfiguration & config_,
            const WriteSettings write_settings_,
            int flags)
        : cfs_file_path(cfs_file_path_), config(config_), write_settings(write_settings_)
    {
#if defined(OS_DARWIN)
        bool o_direct = (flags != -1) && (flags & O_DIRECT);
        if (o_direct)
            flags = flags & ~O_DIRECT;
#endif

        flags = flags == -1 ? O_WRONLY | O_TRUNC | O_CREAT | O_CLOEXEC : flags | O_CLOEXEC;

#if defined(OS_LINUX)
        mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
        fd = ::open(cfs_file_path.c_str(), flags, mode);
#else
        fd = ::open(cfs_file_path.c_str(), flags);
#endif

        if (-1 == fd)
            throwFromErrnoWithPath("Cannot open file " + cfs_file_path, cfs_file_path,
                                errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);

#if defined(OS_DARWIN)
        if (o_direct)
        {
            if (fcntl(fd, F_NOCACHE, 1) == -1)
                throwFromErrnoWithPath("Cannot set F_NOCACHE on file " + cfs_file_path, cfs_file_path, ErrorCodes::CANNOT_OPEN_FILE);
        }
#endif
    }

    ~WriteBufferFromCFSImpl()
    {
        if (fd < 0)
            return;
        int err = ::close(fd);
        chassert(!err || errno == EINTR);
        fd = -1;
    }

    size_t write(const char * start, size_t size) const
    {
        size_t bytes_written = 0;
        ssize_t res = 0;
        {
            struct iovec vec[1];
            vec[0].iov_base = const_cast<char*>(start);
            vec[0].iov_len = size;
            res = ::writev(fd, vec, 1);
        }
        if ((-1 == res || 0 == res) && errno != EINTR)
        {
            String error_file_name = cfs_file_path;
            if (error_file_name.empty())
                error_file_name = "(fd = " + toString(fd) + ")";
            throwFromErrnoWithPath("Cannot write to CFS file " + error_file_name, error_file_name,
                                   ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR);
        }
        if (res > 0)
            bytes_written += res;
       return bytes_written;
    }

    void sync() const
    {
        /// Request OS to sync data with storage medium.
#if defined(OS_DARWIN)
        int res = ::fsync(fd);
#else
        int res = ::fdatasync(fd);
#endif
        if (-1 == res)
            throwFromErrnoWithPath("Cannot CFS fsync " + cfs_file_path, cfs_file_path, ErrorCodes::CANNOT_FSYNC);
    }
};


WriteBufferFromCFS::WriteBufferFromCFS(
        const std::string & cfs_file_path_,
        const Poco::Util::AbstractConfiguration & config_,
        const WriteSettings write_settings_,
        size_t buf_size_,
        int flags)
    : WriteBufferFromFileBase(buf_size_, nullptr, 0)
    , impl(std::make_unique<WriteBufferFromCFSImpl>(cfs_file_path_, config_, write_settings_, flags))
    , file_name(cfs_file_path_)
{
}

void WriteBufferFromCFS::nextImpl()
{
    if (!offset())
        return;
    size_t bytes_written = 0;
    while (bytes_written != offset())
        bytes_written += impl->write(working_buffer.begin() + bytes_written, offset() - bytes_written);
}

void WriteBufferFromCFS::sync()
{
    impl->sync();
}

void WriteBufferFromCFS::finalizeImpl()
{
    try
    {
        next();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

WriteBufferFromCFS::~WriteBufferFromCFS()
{
    finalize();
}

}
