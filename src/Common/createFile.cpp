#include "createFile.h"
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

namespace DB
{
namespace ErrorCodes
{
extern const int FILE_ALREADY_EXISTS;
extern const int OPERATION_NOT_PERMITTED;
extern const int NOT_ENOUGH_SPACE;
extern const int CANNOT_CREATE_FILE;
}
}

namespace std::filesystem
{
[[noreturn]] void handleLastError(const std::string & path)
{
    switch (errno)
    {
        case EEXIST:
            throw DB::Exception(DB::ErrorCodes::FILE_ALREADY_EXISTS, "File {} already exist", path);
        case EPERM:
            throw DB::Exception(DB::ErrorCodes::OPERATION_NOT_PERMITTED, "Not enough permissions to create file {}", path);
        case ENOSPC:
            throw DB::Exception(DB::ErrorCodes::NOT_ENOUGH_SPACE, "Not enough space to create file {}", path);
        case ENAMETOOLONG:
            throw DB::Exception(DB::ErrorCodes::CANNOT_CREATE_FILE, "File name {} is too long");
        default:
            throw DB::Exception(DB::ErrorCodes::CANNOT_CREATE_FILE, "Cannot create file {}. Error: {}", path, strerror(errno));
    }
}

/// Copy from Poco::createFile
bool createFile(const path & path)
{
    int n = open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
    if (n != -1)
    {
        close(n);
        return true;
    }
    if (n == -1 && errno == EEXIST)
        return false;
    handleLastError(path);
}
}
