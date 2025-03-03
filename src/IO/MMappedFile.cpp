#include <unistd.h>
#include <fcntl.h>

#include <Common/ProfileEvents.h>
#include <Common/formatReadable.h>
#include <Common/Exception.h>
#include <IO/MMappedFile.h>


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


void MMappedFile::open()
{
    ProfileEvents::increment(ProfileEvents::FileOpen);

    fd = ::open(file_name.c_str(), O_RDONLY | O_CLOEXEC);

    if (-1 == fd)
        ErrnoException::throwFromPath(
            errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE, file_name, "Cannot open file {}", file_name);
}


std::string MMappedFile::getFileName() const
{
    return file_name;
}


MMappedFile::MMappedFile(const std::string & file_name_, size_t offset_, size_t length_)
    : file_name(file_name_)
{
    open();
    set(fd, offset_, length_);
}


MMappedFile::MMappedFile(const std::string & file_name_, size_t offset_)
    : file_name(file_name_)
{
    open();
    set(fd, offset_);
}


MMappedFile::~MMappedFile()
{
    if (fd != -1)
        close();    /// Exceptions will lead to std::terminate and that's Ok.
}


void MMappedFile::close()
{
    finish();

    if (0 != ::close(fd))
    {
        fd = -1;
        throw Exception(ErrorCodes::CANNOT_CLOSE_FILE, "Cannot close file");
    }

    fd = -1;
    metric_increment.destroy();
}

}
