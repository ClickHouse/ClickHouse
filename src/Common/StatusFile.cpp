#include <Common/StatusFile.h>
#include <fcntl.h>
#include <cerrno>

#include <Common/logger_useful.h>
#include <Common/ClickHouseRevision.h>
#include <Common/LocalDateTime.h>
#include <Common/ErrnoException.h>
#include <base/errnoToString.h>
#include <base/defines.h>
#include <base/pathToString.h>

#include <IO/PlatformFileIO.h>
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


StatusFile::StatusFile(std::string path_, FillFunction fill)
    : path(std::move(path_))
{
    /// If file already exists. NOTE Minor race condition.
    /// `path` is UTF-8, so every filesystem operation on it here has to go through either
    /// `pathFromString` or one of the `platform*` wrappers - the narrow `std::filesystem` and CRT
    /// entrypoints convert through the Windows active code page and lose non-ASCII path components.
    if (fs::exists(pathFromString(path)))
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

    fd = platformOpenFile(path, O_WRONLY | O_CREAT | O_CLOEXEC, 0666);

    if (-1 == fd)
        ErrnoException::throwFromPath(ErrorCodes::CANNOT_OPEN_FILE, path, "Cannot open file {}", path);

    try
    {
        if (-1 == platformLockFileExclusive(fd, /*blocking*/ false))
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
            LOG_INFO(getLogger("StatusFile"), "Writing pid {} to {}", getpid(), path);
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

    if (0 != platformUnlink(path))
        LOG_ERROR(getLogger("StatusFile"), "Cannot unlink file {}, {}", path, errnoToString());
}

}
