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
                throwFromErrnoWithPath("Cannot stat " + source_path, source_path, ErrorCodes::CANNOT_STAT);

            if (0 != lstat(destination_path.c_str(), &destination_descr))
                throwFromErrnoWithPath("Cannot stat " + destination_path, destination_path, ErrorCodes::CANNOT_STAT);

            if (source_descr.st_ino != destination_descr.st_ino)
                throwFromErrnoWithPath(
                        "Destination file " + destination_path + " is already exist and have different inode.",
                        destination_path, ErrorCodes::CANNOT_LINK, link_errno);
        }
        else
            throwFromErrnoWithPath("Cannot link " + source_path + " to " + destination_path, destination_path,
                                   ErrorCodes::CANNOT_LINK);
    }
}

}
