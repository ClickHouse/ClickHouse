#include <Common/localBackup.h>
#include <Common/createHardLink.h>
#include <Common/Exception.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/File.h>
#include <string>
#include <iostream>
#include <errno.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_DEEP_RECURSION;
    extern const int DIRECTORY_ALREADY_EXISTS;
}


static void localBackupImpl(const Poco::Path & source_path, const Poco::Path & destination_path, size_t level,
                            std::optional<size_t> max_level)
{
    if (max_level && level > *max_level)
        return;

    if (level >= 1000)
        throw DB::Exception("Too deep recursion", DB::ErrorCodes::TOO_DEEP_RECURSION);

    Poco::File(destination_path).createDirectories();

    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator dir_it(source_path); dir_it != dir_end; ++dir_it)
    {
        Poco::Path source = dir_it.path();
        Poco::Path destination = destination_path;
        destination.append(dir_it.name());

        if (!dir_it->isDirectory())
        {
            dir_it->setReadOnly();

            createHardLink(source.toString(), destination.toString());
        }
        else
        {
            localBackupImpl(source, destination, level + 1, max_level);
        }
    }
}

void localBackup(const Poco::Path & source_path, const Poco::Path & destination_path, std::optional<size_t> max_level)
{
    if (Poco::File(destination_path).exists()
        && Poco::DirectoryIterator(destination_path) != Poco::DirectoryIterator())
    {
        throw DB::Exception("Directory " + destination_path.toString() + " already exists and is not empty.", DB::ErrorCodes::DIRECTORY_ALREADY_EXISTS);
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
            localBackupImpl(source_path, destination_path, 0, max_level);
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
        catch (const Poco::FileNotFoundException &)
        {
            ++try_no;
            if (try_no == max_tries)
                throw;

            continue;
        }

        break;
    }
}

}
