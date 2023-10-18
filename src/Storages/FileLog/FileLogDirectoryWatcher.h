#pragma once

#include <Storages/FileLog/DirectoryWatcherBase.h>

#include <Common/logger_useful.h>

#include <memory>
#include <mutex>

namespace DB
{
class StorageFileLog;

class FileLogDirectoryWatcher
{
public:
    struct EventInfo
    {
        DirectoryWatcherBase::DirectoryEventType type;
        std::string callback;
    };

    struct FileEvents
    {
        bool received_modification_event = false;
        std::vector<EventInfo> file_events;
    };

    using Events = std::unordered_map<std::string, FileEvents>;

    struct Error
    {
        bool has_error = false;
        std::string error_msg = {};
    };

    FileLogDirectoryWatcher(const std::string & path_, StorageFileLog & storage_, ContextPtr context_);
    ~FileLogDirectoryWatcher() = default;

    Events getEventsAndReset();

    Error getErrorAndReset();

    const std::string & getPath() const;

private:
    friend class DirectoryWatcherBase;
    /// Here must pass by value, otherwise will lead to stack-use-of-scope.
    void onItemAdded(DirectoryWatcherBase::DirectoryEvent ev);
    void onItemRemoved(DirectoryWatcherBase::DirectoryEvent ev);
    void onItemModified(DirectoryWatcherBase::DirectoryEvent ev);
    void onItemMovedFrom(DirectoryWatcherBase::DirectoryEvent ev);
    void onItemMovedTo(DirectoryWatcherBase::DirectoryEvent ev);
    void onError(Exception);

    const std::string path;

    StorageFileLog & storage;

    /// Note, in order to avoid data race found by fuzzer, put events before dw,
    /// such that when this class destruction, dw will be destructed before events.
    /// The data race is because dw create a separate thread to monitor file events
    /// and put into events, then if we destruct events first, the monitor thread still
    /// running, it may access events during events destruction, leads to data race.
    /// And we should put other members before dw as well, because all of them can be
    /// accessed in thread created by dw.
    Events events;

    Poco::Logger * log;

    std::mutex mutex;

    Error error;

    std::unique_ptr<DirectoryWatcherBase> dw;
};
}
