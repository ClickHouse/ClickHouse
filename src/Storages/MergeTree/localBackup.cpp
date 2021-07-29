#include "localBackup.h"

#include <Common/Exception.h>
#include <string>
#include <cerrno>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_DEEP_RECURSION;
    extern const int DIRECTORY_ALREADY_EXISTS;
}


static void localBackupImpl(const DiskPtr & disk, const String & source_path, const String & destination_path, size_t level,
                            std::optional<size_t> max_level)
{
    if (max_level && level > *max_level)
        return;

    if (level >= 1000)
        throw DB::Exception("Too deep recursion", DB::ErrorCodes::TOO_DEEP_RECURSION);

    disk->createDirectories(destination_path);

    for (auto it = disk->iterateDirectory(source_path); it->isValid(); it->next())
    {
        auto source = it->path();
        auto destination = fs::path(destination_path) / it->name();

        if (!disk->isDirectory(source))
        {
            disk->setReadOnly(source);
            disk->createHardLink(source, destination);
        }
        else
        {
            localBackupImpl(disk, source, destination, level + 1, max_level);
        }
    }
}

void localBackup(const DiskPtr & disk, const String & source_path, const String & destination_path, std::optional<size_t> max_level)
{
    if (disk->exists(destination_path) && !disk->isDirectoryEmpty(destination_path))
    {
        throw DB::Exception("Directory " + fullPath(disk, destination_path) + " already exists and is not empty.", DB::ErrorCodes::DIRECTORY_ALREADY_EXISTS);
    }

    size_t try_no = 0;
    const size_t max_tries = 10;

    /** Files in the directory can be permanently added and deleted.
      * If some file is deleted during an attempt to make a backup, then try again,
      *  because it's important to take into account any new files that might appear.
      */
    while (true)
    {
        try
        {
            localBackupImpl(disk, source_path, destination_path, 0, max_level);
        }
        catch (const DB::ErrnoException & e)
        {
            if (e.getErrno() != ENOENT)
                throw;

            ++try_no;
            if (try_no == max_tries)
                throw;

            continue;
        }
        catch (const fs::filesystem_error & e)
        {
            if (e.code() == std::errc::no_such_file_or_directory)
            {
                ++try_no;
                if (try_no == max_tries)
                    throw;
                continue;
            }
            throw;
        }

        break;
    }
}

}
