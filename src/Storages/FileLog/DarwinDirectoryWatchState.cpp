#include <Storages/FileLog/DarwinDirectoryWatchState.h>

#if defined(OS_DARWIN)

#include <cerrno>
#include <unistd.h>

namespace DB
{

void closeFileDescriptor(int fd)
{
    [[maybe_unused]] int err = ::close(fd);
    chassert(!err || errno == EINTR);
}

DarwinDirectoryWatchState::~DarwinDirectoryWatchState()
{
    /// Closing a descriptor removes it from the kqueue of the reactor.
    for (const auto & [name, watched] : watched_fds)
        closeFileDescriptor(watched.fd);
    if (dir_fd != -1)
        closeFileDescriptor(dir_fd);
}

void DarwinDirectoryWatchState::watchFile(const std::string & name, int fd, UInt64 inode)
{
    const auto it = watched_fds.emplace(name, WatchedFd{fd, inode}).first;
    try
    {
        watched_names.emplace(fd, name);
    }
    catch (...)
    {
        watched_fds.erase(it);
        throw;
    }
}

std::map<std::string, DarwinDirectoryWatchState::WatchedFd>::iterator
DarwinDirectoryWatchState::unwatchFile(std::map<std::string, WatchedFd>::iterator it)
{
    closeFileDescriptor(it->second.fd);
    watched_names.erase(it->second.fd);
    return watched_fds.erase(it);
}

void DarwinDirectoryWatchState::setSnapshot(std::map<std::string, UInt64> && new_snapshot)
{
    snapshot = std::move(new_snapshot);
    followed_files.clear();
    for (const auto & [name, inode] : snapshot)
        followed_files.emplace(inode, name);
    has_snapshot = true;
}

}

#endif
