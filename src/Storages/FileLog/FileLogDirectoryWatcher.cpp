#include <Storages/FileLog/FileLogDirectoryWatcher.h>
#include <Poco/Delegate.h>
#include <Poco/DirectoryWatcher.h>

FileLogDirectoryWatcher::FileLogDirectoryWatcher(const std::string & path_)
    : path(path_), dw(std::make_shared<Poco::DirectoryWatcher>(path))
{
    dw->itemAdded += Poco::delegate(this, &FileLogDirectoryWatcher::onItemAdded);
    dw->itemRemoved += Poco::delegate(this, &FileLogDirectoryWatcher::onItemRemoved);
    dw->itemModified += Poco::delegate(this, &FileLogDirectoryWatcher::onItemModified);
    dw->itemMovedFrom += Poco::delegate(this, &FileLogDirectoryWatcher::onItemMovedFrom);
    dw->itemMovedTo += Poco::delegate(this, &FileLogDirectoryWatcher::onItemMovedTo);
}

FileLogDirectoryWatcher::Events FileLogDirectoryWatcher::getEvents()
{
    std::lock_guard<std::mutex> lock(mutex);
    Events res;
    res.swap(events);
    return res;
}

bool FileLogDirectoryWatcher::getError() const
{
    return error;
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


void FileLogDirectoryWatcher::onError(const Poco::Exception &)
{
	error = true;
}
