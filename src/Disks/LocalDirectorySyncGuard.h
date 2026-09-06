#pragma once

#include <Disks/IDisk.h>

#include <system_error>

namespace DB
{

class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

/// Helper class, that receives file descriptor and does fsync for it in destructor.
/// It's used to keep descriptor open, while doing some operations with it, and do fsync at the end.
/// Guaranties of sequence 'close-reopen-fsync' may depend on kernel version.
/// Source: linux-fsdevel mailing-list https://marc.info/?l=linux-fsdevel&m=152535409207496
class LocalDirectorySyncGuard final : public ISyncGuard
{
public:
    /// NOTE: If you have already opened descriptor, it's preferred to use
    /// this constructor instead of constructor with path.
    explicit LocalDirectorySyncGuard(int fd_) : fd(fd_) {}
    explicit LocalDirectorySyncGuard(const String & full_path);
    ~LocalDirectorySyncGuard() override;

private:
    int fd = -1;
};

/// Keeps a directory open so a mutation inside it can be persisted afterwards.
/// Unlike LocalDirectorySyncGuard, the sync is explicit and its failure propagates, so a caller
/// can refuse to acknowledge a change whose directory entry it could not persist. The
/// constructor opens the directory, so it can be held before the mutation it will persist.
class CheckedDirectorySync
{
public:
    explicit CheckedDirectorySync(const String & full_path);
    ~CheckedDirectorySync();

    CheckedDirectorySync(const CheckedDirectorySync &) = delete;
    CheckedDirectorySync & operator=(const CheckedDirectorySync &) = delete;

    /// Persists the directory and closes it, throwing if it cannot be persisted.
    /// A second call does nothing. The destructor never syncs: after an exception the operation
    /// has already failed, and a best-effort sync there would only blur what is durable.
    void sync();

private:
    int fd = -1;
    String path;
};

/// Creates `dir` and any missing ancestor of it. When `fsync` is set, each directory this call
/// creates is persisted in its own parent, and failure to persist one throws.
/// A file's own fsync does not persist its directory entry, which is why the directory holding
/// it is synced separately after any create, rename or remove inside it.
void createDirectoriesAndSync(const String & dir, bool fsync, std::error_code & ec);

/// Same, but reports a failure to create the directory by throwing instead of through `ec`.
void createDirectoriesAndSync(const String & dir, bool fsync);

}

