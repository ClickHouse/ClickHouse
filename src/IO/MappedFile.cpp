#include <unistd.h>
#include <fcntl.h>

#include <Common/ProfileEvents.h>
#include <Common/formatReadable.h>
#include <Common/Exception.h>
#include <IO/MappedFile.h>


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


void MappedFile::open()
{
    ProfileEvents::increment(ProfileEvents::FileOpen);

    fd = ::open(file_name.c_str(), O_RDONLY | O_CLOEXEC);

    if (-1 == fd)
        throwFromErrnoWithPath("Cannot open file " + file_name, file_name,
                               errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);
}


std::string MappedFile::getFileName() const
{
    return file_name;
}


MappedFile::MappedFile(const std::string & file_name_, size_t offset_, size_t length_)
    : file_name(file_name_)
{
    open();
    set(fd, offset_, length_);
}


MappedFile::MappedFile(const std::string & file_name_, size_t offset_)
    : file_name(file_name_)
{
    open();
    set(fd, offset_);
}


MappedFile::~MappedFile()
{
    if (fd != -1)
        close();    /// Exceptions will lead to std::terminate and that's Ok.
}


void MappedFile::close()
{
    finish();

    if (0 != ::close(fd))
        throw Exception("Cannot close file", ErrorCodes::CANNOT_CLOSE_FILE);

    fd = -1;
    metric_increment.destroy();
}

}
