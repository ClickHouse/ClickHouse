#include <Common/StatusFile.h>

#include <sys/file.h>
#include <fcntl.h>
#include <cerrno>

#include <Common/logger_useful.h>
#include <Common/ClickHouseRevision.h>
#include <Common/LocalDateTime.h>
#include <base/errnoToString.h>
#include <base/defines.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/LimitReadBuffer.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/Operators.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_TRUNCATE_FILE;
    extern const int CANNOT_SEEK_THROUGH_FILE;
}


StatusFile::FillFunction StatusFile::write_pid = [](WriteBuffer & out)
{
    out << getpid();
};

StatusFile::FillFunction StatusFile::write_full_info = [](WriteBuffer & out)
{
    out << "PID: " << getpid() << "\n"
        << "Started at: " << LocalDateTime(time(nullptr)) << "\n"
        << "Revision: " << ClickHouseRevision::getVersionRevision() << "\n";
};


StatusFile::StatusFile(std::string path_, FillFunction fill_)
    : path(std::move(path_)), fill(std::move(fill_))
{
    /// If file already exists. NOTE Minor race condition.
    if (fs::exists(path))
    {
        std::string contents;
        {
            ReadBufferFromFile in(path, 1024);
            LimitReadBuffer limit_in(in, {.read_no_more = 1024});
            readStringUntilEOF(contents, limit_in);
        }

        if (!contents.empty())
            LOG_INFO(getLogger("StatusFile"), "Status file {} already exists - unclean restart. Contents:\n{}", path, contents);
        else
            LOG_INFO(getLogger("StatusFile"), "Status file {} already exists and is empty - probably unclean hardware restart.", path);
    }

    fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_CLOEXEC, 0666);

    if (-1 == fd)
        ErrnoException::throwFromPath(ErrorCodes::CANNOT_OPEN_FILE, path, "Cannot open file {}", path);

    try
    {
        int flock_ret = flock(fd, LOCK_EX | LOCK_NB);
        if (-1 == flock_ret)
        {
            if (errno == EWOULDBLOCK)
                throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "Cannot lock file {}. Another server instance in same directory is already running.", path);
            ErrnoException::throwFromPath(ErrorCodes::CANNOT_OPEN_FILE, path, "Cannot lock file {}", path);
        }

        if (0 != ftruncate(fd, 0))
            ErrnoException::throwFromPath(ErrorCodes::CANNOT_TRUNCATE_FILE, path, "Cannot ftruncate file {}", path);

        if (0 != lseek(fd, 0, SEEK_SET))
            ErrnoException::throwFromPath(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, path, "Cannot lseek file {}", path);

        /// Write information about current server instance to the file.
        WriteBufferFromFileDescriptor out(fd, 1024);
        try
        {
            fill(out);
            out.finalize();
        }
        catch (...)
        {
            out.cancel();
            throw;
        }
    }
    catch (...)
    {
        [[maybe_unused]] int err = close(fd);
        chassert(!err || errno == EINTR);
        throw;
    }
}


StatusFile::~StatusFile()
{
    if (0 != close(fd))
        LOG_ERROR(getLogger("StatusFile"), "Cannot close file {}, {}", path, errnoToString());

    if (0 != unlink(path.c_str()))
        LOG_ERROR(getLogger("StatusFile"), "Cannot unlink file {}, {}", path, errnoToString());
}

}
