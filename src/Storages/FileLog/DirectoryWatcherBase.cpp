#include <Interpreters/Context.h>
#include <Storages/FileLog/DirectoryWatcherBase.h>
#include <Storages/FileLog/FileLogDirectoryWatcher.h>
#include <Storages/FileLog/FileLogSettings.h>
#include <Storages/FileLog/StorageFileLog.h>
#include <base/defines.h>

#include <filesystem>
#include <unistd.h>
#include <sys/inotify.h>
#include <poll.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int BAD_FILE_TYPE;
    extern const int IO_SETUP_ERROR;
}

namespace FileLogSetting
{
    extern const FileLogSettingsUInt64 poll_directory_watch_events_backoff_factor;
    extern const FileLogSettingsMilliseconds poll_directory_watch_events_backoff_init;
    extern const FileLogSettingsMilliseconds poll_directory_watch_events_backoff_max;
}

static constexpr int buffer_size = 4096;

DirectoryWatcherBase::DirectoryWatcherBase(
    FileLogDirectoryWatcher & owner_, const std::string & path_, ContextPtr context_, int event_mask_)
    : WithContext(context_)
    , owner(owner_)
    , path(path_)
    , event_mask(event_mask_)
    , milliseconds_to_wait((*owner.storage.getFileLogSettings())[FileLogSetting::poll_directory_watch_events_backoff_init].totalMilliseconds())
{
    if (!std::filesystem::exists(path))
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Path {} does not exist", path);

    if (!std::filesystem::is_directory(path))
        throw Exception(ErrorCodes::BAD_FILE_TYPE, "Path {} is not a directory", path);

    inotify_fd = inotify_init();
    if (inotify_fd == -1)
        throw ErrnoException(ErrorCodes::IO_SETUP_ERROR, "Cannot initialize inotify");

    watch_task = getContext()->getSchedulePool().createTask("directory_watch", [this] { watchFunc(); });
    start();
}

void DirectoryWatcherBase::watchFunc()
{
    int mask = 0;
    if (eventMask() & DirectoryWatcherBase::DW_ITEM_ADDED)
        mask |= IN_CREATE;
    if (eventMask() & DirectoryWatcherBase::DW_ITEM_REMOVED)
        mask |= IN_DELETE;
    if (eventMask() & DirectoryWatcherBase::DW_ITEM_MODIFIED)
        mask |= IN_MODIFY;
    if (eventMask() & DirectoryWatcherBase::DW_ITEM_MOVED_FROM)
        mask |= IN_MOVED_FROM;
    if (eventMask() & DirectoryWatcherBase::DW_ITEM_MOVED_TO)
        mask |= IN_MOVED_TO;

    int wd = inotify_add_watch(inotify_fd, path.c_str(), mask);
    if (wd == -1)
    {
        owner.onError(Exception(ErrorCodes::IO_SETUP_ERROR, "Watch directory {} failed", path));
        ErrnoException::throwFromPath(ErrorCodes::IO_SETUP_ERROR, path, "Watch directory {} failed", path);
    }

    std::string buffer;
    buffer.resize(buffer_size);
    pollfd pfds[2];
    /// inotify descriptor
    pfds[0].fd = inotify_fd;
    pfds[0].events = POLLIN;
    // notifier
    pfds[1].fd = event_pipe.fds_rw[0];
    pfds[1].events = POLLIN;
    while (!stopped)
    {
        const auto & settings = owner.storage.getFileLogSettings();
        if (poll(pfds, 2, static_cast<int>(milliseconds_to_wait)) > 0 && pfds[0].revents & POLLIN)
        {
            milliseconds_to_wait = (*settings)[FileLogSetting::poll_directory_watch_events_backoff_init].totalMilliseconds();
            ssize_t n = read(inotify_fd, buffer.data(), buffer.size());
            int i = 0;
            if (n > 0)
            {
                while (n > 0)
                {
                    struct inotify_event * p_event = reinterpret_cast<struct inotify_event *>(buffer.data() + i);

                    if (p_event->len > 0)
                    {
                        if ((p_event->mask & IN_CREATE) && (eventMask() & DirectoryWatcherBase::DW_ITEM_ADDED))
                        {
                            DirectoryWatcherBase::DirectoryEvent ev(p_event->name, DirectoryWatcherBase::DW_ITEM_ADDED);
                            owner.onItemAdded(ev);
                        }
                        if ((p_event->mask & IN_DELETE) && (eventMask() & DirectoryWatcherBase::DW_ITEM_REMOVED))
                        {
                            DirectoryWatcherBase::DirectoryEvent ev(p_event->name, DirectoryWatcherBase::DW_ITEM_REMOVED);
                            owner.onItemRemoved(ev);
                        }
                        if ((p_event->mask & IN_MODIFY) && (eventMask() & DirectoryWatcherBase::DW_ITEM_MODIFIED))
                        {
                            DirectoryWatcherBase::DirectoryEvent ev(p_event->name, DirectoryWatcherBase::DW_ITEM_MODIFIED);
                            owner.onItemModified(ev);
                        }
                        if ((p_event->mask & IN_MOVED_FROM) && (eventMask() & DirectoryWatcherBase::DW_ITEM_MOVED_FROM))
                        {
                            DirectoryWatcherBase::DirectoryEvent ev(p_event->name, DirectoryWatcherBase::DW_ITEM_MOVED_FROM);
                            owner.onItemMovedFrom(ev);
                        }
                        if ((p_event->mask & IN_MOVED_TO) && (eventMask() & DirectoryWatcherBase::DW_ITEM_MOVED_TO))
                        {
                            DirectoryWatcherBase::DirectoryEvent ev(p_event->name, DirectoryWatcherBase::DW_ITEM_MOVED_TO);
                            owner.onItemMovedTo(ev);
                        }
                    }

                    i += sizeof(inotify_event) + p_event->len;
                    n -= sizeof(inotify_event) + p_event->len;
                }
            }

            /// Wake up reader thread
            owner.storage.wakeUp();
        }
        else
        {
            if (milliseconds_to_wait < static_cast<uint64_t>((*settings)[FileLogSetting::poll_directory_watch_events_backoff_max].totalMilliseconds()))
                milliseconds_to_wait *= (*settings)[FileLogSetting::poll_directory_watch_events_backoff_factor].value;
        }
    }
}

DirectoryWatcherBase::~DirectoryWatcherBase()
{
    stop();
    [[maybe_unused]] int err = ::close(inotify_fd);
    chassert(!err || errno == EINTR);
}

void DirectoryWatcherBase::start()
{
    if (watch_task)
        watch_task->activateAndSchedule();
}

void DirectoryWatcherBase::stop()
{
    stopped = true;
    ::write(event_pipe.fds_rw[1], "\0", 1);
    if (watch_task)
        watch_task->deactivate();
}

}
