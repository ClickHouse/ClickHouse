#pragma once

#include <Storages/FileLog/DirectoryWatcherBase.h>

#include <base/logger_useful.h>

#include <memory>
#include <mutex>

namespace DB
{

class FileLogDirectoryWatcher
{
public:
    struct EventInfo
    {
        DirectoryWatcherBase::DirectoryEventType type;
        std::string callback;
    };

    using Events = std::unordered_map<std::string, std::vector<EventInfo>>;

    struct Error
    {
        bool has_error = false;
        std::string error_msg = {};
    };

    explicit FileLogDirectoryWatcher(const std::string & path_, ContextPtr context_);
    ~FileLogDirectoryWatcher() = default;

    Events getEventsAndReset();

    Error getErrorAndReset();

    const std::string & getPath() const;

    void onItemAdded(const DirectoryWatcherBase::DirectoryEvent & ev);
    void onItemRemoved(const DirectoryWatcherBase::DirectoryEvent & ev);
    void onItemModified(const DirectoryWatcherBase::DirectoryEvent & ev);
    void onItemMovedFrom(const DirectoryWatcherBase::DirectoryEvent & ev);
    void onItemMovedTo(const DirectoryWatcherBase::DirectoryEvent & ev);
    void onError(const Exception &);

private:
    const std::string path;

    /// Note, in order to avoid data race found by fuzzer, put events before dw,
    /// such that when this class destruction, dw will be destructed before events.
    /// The data race is because dw create a separate thread to monitor file events
    /// and put into events, then if we destruct events first, the monitor thread still
    /// running, it may access events during events destruction, leads to data race.
    Events events;

    std::unique_ptr<DirectoryWatcherBase> dw;

    Poco::Logger * log;

    std::mutex mutex;

    Error error;
};
}
