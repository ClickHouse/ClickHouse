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

namespace
{

void localBackupImpl(
    const DiskPtr & disk, const String & source_path,
    const String & destination_path, bool make_source_readonly, size_t level,
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
            if (make_source_readonly)
                disk->setReadOnly(source);
            disk->createHardLink(source, destination);
        }
        else
        {
            localBackupImpl(disk, source, destination, make_source_readonly, level + 1, max_level);
        }
    }
}

class CleanupOnFail
{
public:
    explicit CleanupOnFail(std::function<void()> && cleaner_)
        : cleaner(cleaner_)
    {}

    ~CleanupOnFail()
    {
        if (!is_success)
        {
            /// We are trying to handle race condition here. So if we was not
            /// able to backup directory try to remove garbage, but it's ok if
            /// it doesn't exist.
            try
            {
                cleaner();
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
    }

    void success()
    {
        is_success = true;
    }

private:
    std::function<void()> cleaner;
    bool is_success{false};
};
}

void localBackup(
    const DiskPtr & disk, const String & source_path,
    const String & destination_path, bool make_source_readonly,
    std::optional<size_t> max_level, bool copy_instead_of_hardlinks)
{
    if (disk->exists(destination_path) && !disk->isDirectoryEmpty(destination_path))
    {
        throw DB::Exception("Directory " + fullPath(disk, destination_path) + " already exists and is not empty.", DB::ErrorCodes::DIRECTORY_ALREADY_EXISTS);
    }

    size_t try_no = 0;
    const size_t max_tries = 10;

    CleanupOnFail cleanup([disk, destination_path]() { disk->removeRecursive(destination_path); });

    /** Files in the directory can be permanently added and deleted.
      * If some file is deleted during an attempt to make a backup, then try again,
      * because it's important to take into account any new files that might appear.
      */
    while (true)
    {
        try
        {
            if (copy_instead_of_hardlinks)
                disk->copyDirectoryContent(source_path, disk, destination_path);
            else
                localBackupImpl(disk, source_path, destination_path, make_source_readonly, 0, max_level);
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

    cleanup.success();
}

}
