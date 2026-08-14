#include <unistd.h>
#include <fcntl.h>

#include <Common/ProfileEvents.h>
#include <Common/formatReadable.h>
#include <IO/MMapReadBufferFromFile.h>


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


void MMapReadBufferFromFile::open()
{
    ProfileEvents::increment(ProfileEvents::FileOpen);

    fd = ::open(file_name.c_str(), O_RDONLY | O_CLOEXEC);

    if (-1 == fd)
        ErrnoException::throwFromPath(
            errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE, file_name, "Cannot open file {}", file_name);
}


std::string MMapReadBufferFromFile::getFileName() const
{
    return file_name;
}


bool MMapReadBufferFromFile::isRegularLocalFile(size_t * out_view_offset)
{
    *out_view_offset = mapped.getOffset();
    return true;
}


MMapReadBufferFromFile::MMapReadBufferFromFile(const std::string & file_name_, size_t offset, size_t length_)
    : file_name(file_name_)
{
    open();
    mapped.set(fd, offset, length_);
    init();
}


MMapReadBufferFromFile::MMapReadBufferFromFile(const std::string & file_name_, size_t offset)
    : file_name(file_name_)
{
    open();
    mapped.set(fd, offset);
    init();
}


MMapReadBufferFromFile::~MMapReadBufferFromFile()
{
    if (fd != -1)
        close();    /// Exceptions will lead to std::terminate and that's Ok.
}


void MMapReadBufferFromFile::close()
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
