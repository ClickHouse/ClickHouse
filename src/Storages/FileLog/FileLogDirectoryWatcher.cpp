#include <Storages/FileLog/FileLogDirectoryWatcher.h>
#include <Poco/Delegate.h>
#include <Poco/DirectoryWatcher.h>

FileLogDirectoryWatcher::FileLogDirectoryWatcher(const std::string & path_)
    : path(path_), dw(std::make_shared<Poco::DirectoryWatcher>(path)), log(&Poco::Logger::get("DirectoryIterator (" + path + ")"))
{
    /// DW_ITEM_MOVED_FROM and DW_ITEM_MOVED_TO events will only be reported on Linux.
    /// On other platforms, a file rename or move operation will be reported via a
    /// DW_ITEM_REMOVED and a DW_ITEM_ADDED event. The order of these two events is not defined.
    dw->itemAdded += Poco::delegate(this, &FileLogDirectoryWatcher::onItemAdded);
    dw->itemRemoved += Poco::delegate(this, &FileLogDirectoryWatcher::onItemRemoved);
    dw->itemModified += Poco::delegate(this, &FileLogDirectoryWatcher::onItemModified);
    dw->itemMovedFrom += Poco::delegate(this, &FileLogDirectoryWatcher::onItemMovedFrom);
    dw->itemMovedTo += Poco::delegate(this, &FileLogDirectoryWatcher::onItemMovedTo);
}

FileLogDirectoryWatcher::Events FileLogDirectoryWatcher::getEventsAndReset()
{
    std::lock_guard<std::mutex> lock(mutex);
    Events res;
    res.swap(events);
    return res;
}

FileLogDirectoryWatcher::Error FileLogDirectoryWatcher::getErrorAndReset()
{
    std::lock_guard<std::mutex> lock(mutex);
    Error old_error = error;
    error = {};
    return old_error;
}

const std::string & FileLogDirectoryWatcher::getPath() const
{
    return path;
}

void FileLogDirectoryWatcher::onItemAdded(const Poco::DirectoryWatcher::DirectoryEvent& ev)
{
    std::lock_guard<std::mutex> lock(mutex);
    EventInfo info{ev.event, "onItemAdded"};
    events[ev.item.path()].emplace_back(info);
}


void FileLogDirectoryWatcher::onItemRemoved(const Poco::DirectoryWatcher::DirectoryEvent& ev)
{
    std::lock_guard<std::mutex> lock(mutex);
    EventInfo info{ev.event, "onItemRemoved"};
    events[ev.item.path()].emplace_back(info);
}

/// Optimize for MODIFY event, during a streamToViews period, since the log files
/// are append only, there are may a lots of MODIFY events produced for one file.
/// For example, appending 10000 logs into one file will result in 10000 MODIFY event.
/// So, if we record all of these events, it will use a lot of memory, and then we
/// need to handle it one by one in StorageFileLog::updateFileInfos, this is unnecessary
/// because it is equal to just record and handle one MODIY event
void FileLogDirectoryWatcher::onItemModified(const Poco::DirectoryWatcher::DirectoryEvent& ev)
{
    std::lock_guard<std::mutex> lock(mutex);
    auto event_path = ev.item.path();
    /// Already have MODIFY event for this file
    if (auto it = events.find(event_path); it != events.end() && it->second.back().type == ev.event)
        return;
    EventInfo info{ev.event, "onItemModified"};
    events[event_path].emplace_back(info);
}

void FileLogDirectoryWatcher::onItemMovedFrom(const Poco::DirectoryWatcher::DirectoryEvent& ev)
{
    std::lock_guard<std::mutex> lock(mutex);
    EventInfo info{ev.event, "onItemMovedFrom"};
    events[ev.item.path()].emplace_back(info);
}

void FileLogDirectoryWatcher::onItemMovedTo(const Poco::DirectoryWatcher::DirectoryEvent& ev)
{
    std::lock_guard<std::mutex> lock(mutex);
    EventInfo info{ev.event, "onItemMovedTo"};
    events[ev.item.path()].emplace_back(info);
}

void FileLogDirectoryWatcher::onError(const Poco::Exception & e)
{
    std::lock_guard<std::mutex> lock(mutex);
    LOG_ERROR(log, "Error happened during watching directory {}: {}", path, error.error_msg);
    error.has_error = true;
    error.error_msg = e.message();
}
