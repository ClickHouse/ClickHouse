#include <mutex>
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


void OpenedFile::open() const
{
    ProfileEvents::increment(ProfileEvents::FileOpen);

    fd = ::open(file_name.c_str(), (flags == -1 ? 0 : flags) | O_RDONLY | O_CLOEXEC);

    if (-1 == fd)
        DB::ErrnoException::throwFromPath(
            errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE, file_name, "Cannot open file {}", file_name);
}

int OpenedFile::getFD() const
{
    std::lock_guard l(mutex);
    if (fd == -1)
        open();
    return fd;
}

std::string OpenedFile::getFileName() const
{
    return file_name;
}


OpenedFile::OpenedFile(const std::string & file_name_, int flags_)
    : file_name(file_name_), flags(flags_)
{
}


OpenedFile::~OpenedFile()
{
    close();    /// Exceptions will lead to std::terminate and that's Ok.
}


void OpenedFile::close()
{
    std::lock_guard l(mutex);
    if (fd == -1)
        return;

    if (0 != ::close(fd))
    {
        fd = -1;
        throw Exception(ErrorCodes::CANNOT_CLOSE_FILE, "Cannot close file");
    }

    fd = -1;
    metric_increment.destroy();
}

}
