#pragma once

#include <string>
#include <memory>

namespace DB
{

class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

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
    FileSyncGuard(const DiskPtr & disk_, const std::string & path);
    ~FileSyncGuard();

private:
    DiskPtr disk;
    int fd = -1;
};

}

