#include "Backup.h"

#include <Common/Exception.h>
#include <Disks/IDiskTransaction.h>
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

void BackupImpl(
    const DiskPtr & src_disk,
    const DiskPtr & dst_disk,
    IDiskTransaction * transaction,
    const String & source_path,
    const String & destination_path,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    bool make_source_readonly,
    size_t level,
    std::optional<size_t> max_level,
    bool copy_instead_of_hardlinks,
    const NameSet & files_to_copy_instead_of_hardlinks)
{
    if (max_level && level > *max_level)
        return;

    if (level >= 1000)
        throw DB::Exception(DB::ErrorCodes::TOO_DEEP_RECURSION, "Too deep recursion");

    if (transaction)
        transaction->createDirectories(destination_path);
    else
        dst_disk->createDirectories(destination_path);

    for (auto it = src_disk->iterateDirectory(source_path); it->isValid(); it->next())
    {
        auto source = it->path();
        auto destination = fs::path(destination_path) / it->name();

        if (!src_disk->existsDirectory(source))
        {
            if (make_source_readonly)
            {
                if (transaction)
                    transaction->setReadOnly(source);
                else
                    src_disk->setReadOnly(source);
            }
            if (copy_instead_of_hardlinks || files_to_copy_instead_of_hardlinks.contains(it->name()))
            {
                if (transaction)
                    transaction->copyFile(source, destination, read_settings, write_settings);
                else
                    src_disk->copyFile(source, *dst_disk, destination, read_settings, write_settings);
            }
            else
            {
                if (transaction)
                    transaction->createHardLink(source, destination);
                else
                    src_disk->createHardLink(source, destination);
            }
        }
        else
        {
            BackupImpl(
                src_disk,
                dst_disk,
                transaction,
                source,
                destination,
                read_settings,
                write_settings,
                make_source_readonly,
                level + 1,
                max_level,
                copy_instead_of_hardlinks,
                files_to_copy_instead_of_hardlinks);
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

/// src_disk and dst_disk can be the same disk when local backup.
/// copy_instead_of_hardlinks must be true when remote backup.
void Backup(
    const DiskPtr & src_disk,
    const DiskPtr & dst_disk,
    const String & source_path,
    const String & destination_path,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    bool make_source_readonly,
    std::optional<size_t> max_level,
    bool copy_instead_of_hardlinks,
    const NameSet & files_to_copy_intead_of_hardlinks,
    DiskTransactionPtr disk_transaction)
{
    if (dst_disk->existsFileOrDirectory(destination_path) && !dst_disk->isDirectoryEmpty(destination_path))
    {
        throw DB::Exception(ErrorCodes::DIRECTORY_ALREADY_EXISTS, "Directory {} already exists and is not empty.",
                            DB::fullPath(dst_disk, destination_path));
    }

    size_t try_no = 0;
    const size_t max_tries = 10;

    /** Files in the directory can be permanently added and deleted.
      * If some file is deleted during an attempt to make a backup, then try again,
      * because it's important to take into account any new files that might appear.
      */
    while (true)
    {
        try
        {
            if (disk_transaction)
            {
                BackupImpl(
                    src_disk,
                    dst_disk,
                    disk_transaction.get(),
                    source_path,
                    destination_path,
                    read_settings,
                    write_settings,
                    make_source_readonly,
                    /* level= */ 0,
                    max_level,
                    copy_instead_of_hardlinks,
                    files_to_copy_intead_of_hardlinks
                    );
            }
            else if (copy_instead_of_hardlinks)
            {
                CleanupOnFail cleanup([dst_disk, destination_path]() { dst_disk->removeRecursive(destination_path); });
                src_disk->copyDirectoryContent(source_path, dst_disk, destination_path, read_settings, write_settings, /*cancellation_hook=*/{});
                cleanup.success();
            }
            else
            {
                std::function<void()> cleaner;
                if (dst_disk->supportZeroCopyReplication())
                    /// Note: this code will create garbage on s3. We should always remove `copy_instead_of_hardlinks` files.
                    /// The third argument should be a list of exceptions, but (looks like) it is ignored for keep_all_shared_data = true.
                    cleaner = [dst_disk, destination_path]() { dst_disk->removeSharedRecursive(destination_path, /*keep_all_shared_data*/ true, {}); };
                else
                    cleaner = [dst_disk, destination_path]() { dst_disk->removeRecursive(destination_path); };

                CleanupOnFail cleanup(std::move(cleaner));
                BackupImpl(
                    src_disk,
                    dst_disk,
                    disk_transaction.get(),
                    source_path,
                    destination_path,
                    read_settings,
                    write_settings,
                    make_source_readonly,
                    /* level= */ 0,
                    max_level,
                    /* copy_instead_of_hardlinks= */ false,
                    files_to_copy_intead_of_hardlinks);
                cleanup.success();
            }
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
