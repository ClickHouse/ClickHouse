#include <Common/createHardLink.h>
#include <Common/Exception.h>
#include <errno.h>
#include <unistd.h>
#include <sys/stat.h>


namespace DB
{

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
                throwFromErrno("Cannot stat " + source_path);

            if (0 != lstat(destination_path.c_str(), &destination_descr))
                throwFromErrno("Cannot stat " + destination_path);

            if (source_descr.st_ino != destination_descr.st_ino)
                throwFromErrno("Destination file " + destination_path + " is already exist and have different inode.", 0, link_errno);
        }
        else
            throwFromErrno("Cannot link " + source_path + " to " + destination_path);
    }
}

}
