#include <unistd.h>
#include <fcntl.h>

#include <Common/ProfileEvents.h>
#include <Common/Exception.h>
#include <IO/OpenedFile.h>


namespace ProfileEvents
{
    extern const Event FileOpen;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_CLOSE_FILE;
}


void OpenedFile::open(int flags)
{
    ProfileEvents::increment(ProfileEvents::FileOpen);

    fd = ::open(file_name.c_str(), (flags == -1 ? 0 : flags) | O_RDONLY | O_CLOEXEC);

    if (-1 == fd)
        throwFromErrnoWithPath("Cannot open file " + file_name, file_name,
            errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);
}


std::string OpenedFile::getFileName() const
{
    return file_name;
}


OpenedFile::OpenedFile(const std::string & file_name_, int flags)
    : file_name(file_name_)
{
    open(flags);
}


OpenedFile::~OpenedFile()
{
    if (fd != -1)
        close();    /// Exceptions will lead to std::terminate and that's Ok.
}


void OpenedFile::close()
{
    if (0 != ::close(fd))
        throw Exception("Cannot close file", ErrorCodes::CANNOT_CLOSE_FILE);

    fd = -1;
    metric_increment.destroy();
}

}

