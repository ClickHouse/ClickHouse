#include <Common/createHardLink.h>
#include <Common/Exception.h>
#include <cerrno>
#include <unistd.h>
#include <sys/stat.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_STAT;
    extern const int CANNOT_LINK;
}

void createHardLink(const String & source_path, const String & destination_path)
{
    if (0 != link(source_path.c_str(), destination_path.c_str()))
    {
        if (errno == EEXIST)
        {
            auto link_errno = errno;

            struct stat source_descr;
            struct stat destination_descr;

            if (0 != lstat(source_path.c_str(), &source_descr))
                ErrnoException::throwFromPath(ErrorCodes::CANNOT_STAT, source_path, "Cannot stat {}", source_path);

            if (0 != lstat(destination_path.c_str(), &destination_descr))
                ErrnoException::throwFromPath(ErrorCodes::CANNOT_STAT, destination_path, "Cannot stat {}", destination_path);

            if (source_descr.st_ino != destination_descr.st_ino)
                ErrnoException::throwFromPathWithErrno(
                    ErrorCodes::CANNOT_LINK,
                    destination_path,
                    link_errno,
                    "Destination file {} already exists and has a different inode",
                    destination_path);
        }
        else
            ErrnoException::throwFromPath(ErrorCodes::CANNOT_LINK, destination_path, "Cannot link {} to {}", source_path, destination_path);
    }
}

}
