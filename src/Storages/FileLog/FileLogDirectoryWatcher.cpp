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
    DirEvent de;
    de.callback = "onItemAdded";
    de.path = ev.item.path();
    de.type = ev.event;
    events.push_back(de);
}


void FileLogDirectoryWatcher::onItemRemoved(const Poco::DirectoryWatcher::DirectoryEvent& ev)
{
    std::lock_guard<std::mutex> lock(mutex);
    DirEvent de;
    de.callback = "onItemRemoved";
    de.path = ev.item.path();
    de.type = ev.event;
    events.push_back(de);
}


void FileLogDirectoryWatcher::onItemModified(const Poco::DirectoryWatcher::DirectoryEvent& ev)
{
    std::lock_guard<std::mutex> lock(mutex);
    DirEvent de;
    de.callback = "onItemModified";
    de.path = ev.item.path();
    de.type = ev.event;
    events.push_back(de);
}

void FileLogDirectoryWatcher::onItemMovedFrom(const Poco::DirectoryWatcher::DirectoryEvent& ev)
{
    std::lock_guard<std::mutex> lock(mutex);
    DirEvent de;
    de.callback = "onItemMovedFrom";
    de.path = ev.item.path();
    de.type = ev.event;
    events.push_back(de);
}

void FileLogDirectoryWatcher::onItemMovedTo(const Poco::DirectoryWatcher::DirectoryEvent& ev)
{
    std::lock_guard<std::mutex> lock(mutex);
    DirEvent de;
    de.callback = "onItemMovedTo";
    de.path = ev.item.path();
    de.type = ev.event;
    events.push_back(de);
}

void FileLogDirectoryWatcher::onError(const Poco::Exception & e)
{
    std::lock_guard<std::mutex> lock(mutex);
    LOG_ERROR(log, "Error happened during watching directory {}: {}", path, error.error_msg);
    error.has_error = true;
    error.error_msg = e.message();
}
