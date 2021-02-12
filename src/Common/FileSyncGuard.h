#pragma once

#include <Disks/IDisk.h>

namespace DB
{

/// Helper class, that receives file descriptor and does fsync for it in destructor.
/// It's used to keep descriptor open, while doing some operations with it, and do fsync at the end.
/// Guaranties of sequence 'close-reopen-fsync' may depend on kernel version.
/// Source: linux-fsdevel mailing-list https://marc.info/?l=linux-fsdevel&m=152535409207496
class FileSyncGuard
{
public:
    /// NOTE: If you have already opened descriptor, it's preferred to use
    /// this constructor instead of constructor with path.
    FileSyncGuard(const DiskPtr & disk_, int fd_) : disk(disk_), fd(fd_) {}

    FileSyncGuard(const DiskPtr & disk_, const String & path)
        : disk(disk_), fd(disk_->open(path, O_RDWR)) {}

    ~FileSyncGuard()
    {
        try
        {
            disk->sync(fd);
            disk->close(fd);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

private:
    DiskPtr disk;
    int fd = -1;
};

}

