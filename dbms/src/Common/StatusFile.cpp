#include "StatusFile.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <fcntl.h>
#include <errno.h>

#include <Poco/File.h>
#include <common/logger_useful.h>
#include <Common/ClickHouseRevision.h>
#include <common/LocalDateTime.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/LimitReadBuffer.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_CLOSE_FILE;
    extern const int CANNOT_TRUNCATE_FILE;
    extern const int CANNOT_SEEK_THROUGH_FILE;
}


StatusFile::StatusFile(const std::string & path_)
    : path(path_)
{
    /// If file already exists. NOTE Minor race condition.
    if (Poco::File(path).exists())
    {
        std::string contents;
        {
            ReadBufferFromFile in(path, 1024);
            LimitReadBuffer limit_in(in, 1024, false);
            readStringUntilEOF(contents, limit_in);
        }

        if (!contents.empty())
            LOG_INFO(&Logger::get("StatusFile"), "Status file " << path << " already exists - unclean restart. Contents:\n" << contents);
        else
            LOG_INFO(&Logger::get("StatusFile"), "Status file " << path << " already exists and is empty - probably unclean hardware restart.");
    }

    fd = ::open(path.c_str(), O_WRONLY | O_CREAT, 0666);

    if (-1 == fd)
        throwFromErrno("Cannot open file " + path, ErrorCodes::CANNOT_OPEN_FILE);

    try
    {
        int flock_ret = flock(fd, LOCK_EX | LOCK_NB);
        if (-1 == flock_ret)
        {
            if (errno == EWOULDBLOCK)
                throw Exception("Cannot lock file " + path + ". Another server instance in same directory is already running.", ErrorCodes::CANNOT_OPEN_FILE);
            else
                throwFromErrno("Cannot lock file " + path, ErrorCodes::CANNOT_OPEN_FILE);
        }

        if (0 != ftruncate(fd, 0))
            throwFromErrno("Cannot ftruncate " + path, ErrorCodes::CANNOT_TRUNCATE_FILE);

        if (0 != lseek(fd, 0, SEEK_SET))
            throwFromErrno("Cannot lseek " + path, ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

        /// Write information about current server instance to the file.
        {
            WriteBufferFromFileDescriptor out(fd, 1024);
            out
                << "PID: " << getpid() << "\n"
                << "Started at: " << LocalDateTime(time(nullptr)) << "\n"
                << "Revision: " << ClickHouseRevision::get() << "\n";
        }
    }
    catch (...)
    {
        close(fd);
        throw;
    }
}


StatusFile::~StatusFile()
{
    if (0 != close(fd))
        LOG_ERROR(&Logger::get("StatusFile"), "Cannot close file " << path << ", " << errnoToString(ErrorCodes::CANNOT_CLOSE_FILE));

    if (0 != unlink(path.c_str()))
        LOG_ERROR(&Logger::get("StatusFile"), "Cannot unlink file " << path << ", " << errnoToString(ErrorCodes::CANNOT_CLOSE_FILE));
}

}
