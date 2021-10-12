#include <Interpreters/Context.h>
#include <Storages/FileLog/DirectoryWatcherBase.h>
#include <Storages/FileLog/FileLogDirectoryWatcher.h>

#include <filesystem>
#include <unistd.h>
#include <sys/inotify.h>
#include <sys/select.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int DIRECTORY_DOESNT_EXIST;
    extern const int IO_SETUP_ERROR;
}

static constexpr int event_size = sizeof(struct inotify_event);
static constexpr int buffer_size = 1024 * (NAME_MAX + event_size + 1);

DirectoryWatcherBase::DirectoryWatcherBase(
    FileLogDirectoryWatcher & owner_, const std::string & path_, ContextPtr context_, int event_mask_)
    : WithContext(context_->getGlobalContext()), owner(owner_), path(path_), event_mask(event_mask_)
{
    if (!std::filesystem::exists(path))
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "The path {} does not exist.", path);

    if (!std::filesystem::is_directory(path))
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "The path {} does not a directory.", path);

    fd = inotify_init();
    if (fd == -1)
        throw Exception("cannot initialize inotify", ErrorCodes::IO_SETUP_ERROR);

    watch_task = getContext()->getMessageBrokerSchedulePool().createTask("directory_watch", [this] { watchFunc(); });
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
        owner.onError(Exception(ErrorCodes::IO_SETUP_ERROR, "Watch directory {} failed.", path));
    }

    std::string buffer;
    buffer.resize(buffer_size);
    fd_set fds;
    while (!stopped)
    {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreserved-identifier"
        FD_ZERO(&fds);
        FD_SET(fd, &fds);
#pragma clang diagnostic pop

        struct timeval tv;
        tv.tv_sec = 0;
        tv.tv_usec = 200000;

        if (select(fd + 1, &fds, nullptr, nullptr, &tv) == 1)
        {
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
