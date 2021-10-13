#include <Storages/FileLog/FileLogDirectoryWatcher.h>

namespace DB
{
FileLogDirectoryWatcher::FileLogDirectoryWatcher(const std::string & path_, ContextPtr context_)
    : path(path_)
    , log(&Poco::Logger::get("FileLogDirectoryWatcher(" + path + ")"))
    , dw(std::make_unique<DirectoryWatcherBase>(*this, path, context_))
{
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

void FileLogDirectoryWatcher::onItemAdded(const DirectoryWatcherBase::DirectoryEvent & ev)
{
    std::lock_guard<std::mutex> lock(mutex);

    EventInfo info{ev.event, "onItemAdded"};
    std::string event_path = ev.path;

    if (auto it = events.find(event_path); it != events.end())
    {
        it->second.emplace_back(info);
    }
    else
    {
        events.emplace(event_path, std::vector<EventInfo>{info});
    }
}


void FileLogDirectoryWatcher::onItemRemoved(const DirectoryWatcherBase::DirectoryEvent & ev)
{
    std::lock_guard<std::mutex> lock(mutex);

    EventInfo info{ev.event, "onItemRemoved"};
    std::string event_path = ev.path;

    if (auto it = events.find(event_path); it != events.end())
    {
        it->second.emplace_back(info);
    }
    else
    {
        events.emplace(event_path, std::vector<EventInfo>{info});
    }
}

/// Optimize for MODIFY event, during a streamToViews period, since the log files
/// are append only, there are may a lots of MODIFY events produced for one file.
/// For example, appending 10000 logs into one file will result in 10000 MODIFY event.
/// So, if we record all of these events, it will use a lot of memory, and then we
/// need to handle it one by one in StorageFileLog::updateFileInfos, this is unnecessary
/// because it is equal to just record and handle one MODIY event
void FileLogDirectoryWatcher::onItemModified(const DirectoryWatcherBase::DirectoryEvent & ev)
{
    std::lock_guard<std::mutex> lock(mutex);

    auto event_path = ev.path;
    EventInfo info{ev.event, "onItemModified"};
    /// Already have MODIFY event for this file
    if (auto it = events.find(event_path); it != events.end())
    {
        if (it->second.back().type == ev.event)
            return;
        else
            it->second.emplace_back(info);
    }
    else
    {
        events.emplace(event_path, std::vector<EventInfo>{info});
    }
}

void FileLogDirectoryWatcher::onItemMovedFrom(const DirectoryWatcherBase::DirectoryEvent & ev)
{
    std::lock_guard<std::mutex> lock(mutex);

    EventInfo info{ev.event, "onItemMovedFrom"};
    std::string event_path = ev.path;

    if (auto it = events.find(event_path); it != events.end())
    {
        it->second.emplace_back(info);
    }
    else
    {
        events.emplace(event_path, std::vector<EventInfo>{info});
    }
}

void FileLogDirectoryWatcher::onItemMovedTo(const DirectoryWatcherBase::DirectoryEvent & ev)
{
    std::lock_guard<std::mutex> lock(mutex);

    EventInfo info{ev.event, "onItemMovedTo"};
    std::string event_path = ev.path;

    if (auto it = events.find(event_path); it != events.end())
    {
        it->second.emplace_back(info);
    }
    else
    {
        events.emplace(event_path, std::vector<EventInfo>{info});
    }
}

void FileLogDirectoryWatcher::onError(const Exception & e)
{
    std::lock_guard<std::mutex> lock(mutex);
    LOG_ERROR(log, "Error happened during watching directory {}: {}", path, error.error_msg);
    error.has_error = true;
    error.error_msg = e.message();
}
}
