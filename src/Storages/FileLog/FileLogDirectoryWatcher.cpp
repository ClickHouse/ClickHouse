#include <Storages/FileLog/FileLogDirectoryWatcher.h>
#include <Common/logger_useful.h>

namespace DB
{
FileLogDirectoryWatcher::FileLogDirectoryWatcher(
    const std::string & path_, StorageFileLog & storage_, std::unordered_map<uint64_t, std::string> read_files_, ContextPtr context_)
    : path(path_)
    , storage(storage_)
    , read_files(std::move(read_files_))
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

void FileLogDirectoryWatcher::onItemAdded(const DirectoryWatcherBase::DirectoryEvent & ev)
{
    pending.emplace_back(ev.path, EventInfo{ev.event, "onItemAdded"});
}


void FileLogDirectoryWatcher::onItemRemoved(const DirectoryWatcherBase::DirectoryEvent & ev)
{
    pending.emplace_back(ev.path, EventInfo{ev.event, "onItemRemoved"});
}

/// Optimize for MODIFY event, during a streamToViews period, since the log files
/// are append only, there are may a lots of MODIFY events produced for one file.
/// For example, appending 10000 logs into one file will result in 10000 MODIFY event.
/// So, if we record all of these events, it will use a lot of memory, and then we
/// need to handle it one by one in StorageFileLog::updateFileInfos, this is unnecessary
/// because it is equal to just record and handle one MODIY event
void FileLogDirectoryWatcher::onItemModified(const DirectoryWatcherBase::DirectoryEvent & ev)
{
    pending.emplace_back(ev.path, EventInfo{ev.event, "onItemModified"});
}

void FileLogDirectoryWatcher::onItemMovedFrom(const DirectoryWatcherBase::DirectoryEvent & ev)
{
    pending.emplace_back(ev.path, EventInfo{ev.event, "onItemMovedFrom", ev.cookie});
}

void FileLogDirectoryWatcher::onItemMovedTo(const DirectoryWatcherBase::DirectoryEvent & ev)
{
    pending.emplace_back(ev.path, EventInfo{ev.event, "onItemMovedTo", ev.cookie});
}

void FileLogDirectoryWatcher::commitEvents()
{
    std::lock_guard lock(mutex);
    for (auto & event : pending)
    {
        if (event.second.type == DirectoryWatcherBase::DW_ITEM_MODIFIED && !modified_seen.insert(event.first).second)
            continue;
        events.push_back(std::move(event));
    }
    pending.clear();
}

void FileLogDirectoryWatcher::onError(const Exception & e)
{
    std::lock_guard lock(mutex);
    LOG_ERROR(log, "Error happened during watching directory: {}", error.error_msg);
    error.has_error = true;
    error.error_msg = e.message();
}
}
