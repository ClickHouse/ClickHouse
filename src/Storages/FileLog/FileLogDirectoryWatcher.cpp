#include <Storages/FileLog/FileLogDirectoryWatcher.h>
#include <Common/logger_useful.h>

namespace DB
{
FileLogDirectoryWatcher::FileLogDirectoryWatcher(const std::string & path_, StorageFileLog & storage_, ContextPtr context_)
    : path(path_)
    , storage(storage_)
    , log(getLogger("FileLogDirectoryWatcher(" + path + ")"))
    , dw(std::make_unique<DirectoryWatcherBase>(*this, path, context_))
{
}

FileLogDirectoryWatcher::Events FileLogDirectoryWatcher::getEventsAndReset()
{
    std::lock_guard lock(mutex);
    Events res;
    res.swap(events);
    modified_seen.clear();
    return res;
}

FileLogDirectoryWatcher::Error FileLogDirectoryWatcher::getErrorAndReset()
{
    std::lock_guard lock(mutex);
    Error old_error = error;
    error = {};
    return old_error;
}

const std::string & FileLogDirectoryWatcher::getPath() const
{
    return path;
}

void FileLogDirectoryWatcher::onItemAdded(DirectoryWatcherBase::DirectoryEvent ev)
{
    std::lock_guard lock(mutex);
    events.emplace_back(ev.path, EventInfo{ev.event, "onItemAdded"});
}


void FileLogDirectoryWatcher::onItemRemoved(DirectoryWatcherBase::DirectoryEvent ev)
{
    std::lock_guard lock(mutex);
    events.emplace_back(ev.path, EventInfo{ev.event, "onItemRemoved"});
}

/// Optimize for MODIFY event, during a streamToViews period, since the log files
/// are append only, there are may a lots of MODIFY events produced for one file.
/// For example, appending 10000 logs into one file will result in 10000 MODIFY event.
/// So, if we record all of these events, it will use a lot of memory, and then we
/// need to handle it one by one in StorageFileLog::updateFileInfos, this is unnecessary
/// because it is equal to just record and handle one MODIY event
void FileLogDirectoryWatcher::onItemModified(DirectoryWatcherBase::DirectoryEvent ev)
{
    std::lock_guard lock(mutex);
    if (!modified_seen.insert(ev.path).second)
        return;
    events.emplace_back(ev.path, EventInfo{ev.event, "onItemModified"});
}

void FileLogDirectoryWatcher::onItemMovedFrom(DirectoryWatcherBase::DirectoryEvent ev)
{
    std::lock_guard lock(mutex);
    events.emplace_back(ev.path, EventInfo{ev.event, "onItemMovedFrom"});
}

void FileLogDirectoryWatcher::onItemMovedTo(DirectoryWatcherBase::DirectoryEvent ev)
{
    std::lock_guard lock(mutex);
    events.emplace_back(ev.path, EventInfo{ev.event, "onItemMovedTo"});
}

void FileLogDirectoryWatcher::onError(Exception e)
{
    std::lock_guard lock(mutex);
    LOG_ERROR(log, "Error happened during watching directory: {}", error.error_msg);
    error.has_error = true;
    error.error_msg = e.message();
}
}
