#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <utime.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Poco/Timestamp.h>
#include <Common/FileSystemHelpers.h>

namespace DB
{
namespace ErrorCodes
{
extern const int PATH_ACCESS_DENIED;
extern const int CANNOT_CREATE_FILE;
}
}

namespace FS
{

/// Copy from Poco::createFile
bool createFile(const std::string & path)
{
    int n = open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
    if (n != -1)
    {
        close(n);
        return true;
    }
    DB::throwFromErrnoWithPath("Cannot create file: " + path, path, DB::ErrorCodes::CANNOT_CREATE_FILE);
}

bool canRead(const std::string & path)
{
    struct stat st;
    if (stat(path.c_str(), &st) == 0)
    {
        if (st.st_uid == geteuid())
            return (st.st_mode & S_IRUSR) != 0;
        else if (st.st_gid == getegid())
            return (st.st_mode & S_IRGRP) != 0;
        else
            return (st.st_mode & S_IROTH) != 0 || geteuid() == 0;
    }
    DB::throwFromErrnoWithPath("Cannot check read access to file: " + path, path, DB::ErrorCodes::PATH_ACCESS_DENIED);
}


bool canWrite(const std::string & path)
{
    struct stat st;
    if (stat(path.c_str(), &st) == 0)
    {
        if (st.st_uid == geteuid())
            return (st.st_mode & S_IWUSR) != 0;
        else if (st.st_gid == getegid())
            return (st.st_mode & S_IWGRP) != 0;
        else
            return (st.st_mode & S_IWOTH) != 0 || geteuid() == 0;
    }
    DB::throwFromErrnoWithPath("Cannot check write access to file: " + path, path, DB::ErrorCodes::PATH_ACCESS_DENIED);
}

time_t getModificationTime(const std::string & path)
{
    struct stat st;
    if (stat(path.c_str(), &st) == 0)
        return st.st_mtime;
    DB::throwFromErrnoWithPath("Cannot check modification time for file: " + path, path, DB::ErrorCodes::PATH_ACCESS_DENIED);
}

Poco::Timestamp getModificationTimestamp(const std::string & path)
{
    return Poco::Timestamp::fromEpochTime(getModificationTime(path));
}

void setModificationTime(const std::string & path, time_t time)
{
    struct utimbuf tb;
    tb.actime  = time;
    tb.modtime = time;
    if (utime(path.c_str(), &tb) != 0)
        DB::throwFromErrnoWithPath("Cannot set modification time for file: " + path, path, DB::ErrorCodes::PATH_ACCESS_DENIED);
}
}
