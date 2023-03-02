#include <Interpreters/Context.h>
#include <Storages/FileLog/DirectoryWatcherBase.h>
#include <Storages/FileLog/FileLogDirectoryWatcher.h>
#include <Storages/FileLog/StorageFileLog.h>

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

static constexpr int buffer_size = 4096;

DirectoryWatcherBase::DirectoryWatcherBase(
    FileLogDirectoryWatcher & owner_, const std::string & path_, ContextPtr context_, int event_mask_)
    : WithContext(context_)
    , owner(owner_)
    , path(path_)
    , event_mask(event_mask_)
    , milliseconds_to_wait(owner.storage.getFileLogSettings()->poll_directory_watch_events_backoff_init.totalMilliseconds())
{
    if (!std::filesystem::exists(path))
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Path {} does not exist", path);

    if (!std::filesystem::is_directory(path))
        throw Exception(ErrorCodes::BAD_FILE_TYPE, "Path {} is not a directory", path);

    fd = inotify_init();
    if (fd == -1)
        throwFromErrno("Cannot initialize inotify", ErrorCodes::IO_SETUP_ERROR);

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

    int wd = inotify_add_watch(fd, path.c_str(), mask);
    if (wd == -1)
    {
        owner.onError(Exception(ErrorCodes::IO_SETUP_ERROR, "Watch directory {} failed", path));
        throwFromErrnoWithPath("Watch directory {} failed", path, ErrorCodes::IO_SETUP_ERROR);
    }

    std::string buffer;
    buffer.resize(buffer_size);
    pollfd pfd;
    pfd.fd = fd;
    pfd.events = POLLIN;
    while (!stopped)
    {
        const auto & settings = owner.storage.getFileLogSettings();
        if (poll(&pfd, 1, milliseconds_to_wait) > 0 && pfd.revents & POLLIN)
        {
            milliseconds_to_wait = settings->poll_directory_watch_events_backoff_init.totalMilliseconds();
            int n = read(fd, buffer.data(), buffer.size());
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
            if (milliseconds_to_wait < static_cast<uint64_t>(settings->poll_directory_watch_events_backoff_max.totalMilliseconds()))
                milliseconds_to_wait *= settings->poll_directory_watch_events_backoff_factor.value;
        }
    }
}

DirectoryWatcherBase::~DirectoryWatcherBase()
{
    stop();
    close(fd);
}

void DirectoryWatcherBase::start()
{
    if (watch_task)
        watch_task->activateAndSchedule();
}

void DirectoryWatcherBase::stop()
{
    stopped = true;
    if (watch_task)
        watch_task->deactivate();
}

}
