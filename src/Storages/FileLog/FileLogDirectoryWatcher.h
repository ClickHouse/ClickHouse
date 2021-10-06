#pragma once

#include <Poco/DirectoryWatcher.h>
#include <Poco/Foundation.h>
#include <Poco/Path.h>

#include <base/logger_useful.h>

#include <memory>
#include <mutex>

class FileLogDirectoryWatcher
{
public:
    struct EventInfo
    {
        Poco::DirectoryWatcher::DirectoryEventType type;
        std::string callback;
    };

    using Events = std::unordered_map<std::string, std::vector<EventInfo>>;

    struct Error
    {
        bool has_error = false;
        std::string error_msg = {};
    };

    explicit FileLogDirectoryWatcher(const std::string & path_);
    ~FileLogDirectoryWatcher() = default;

    Events getEventsAndReset();

    Error getErrorAndReset();

    const std::string & getPath() const;

protected:
    void onItemAdded(const Poco::DirectoryWatcher::DirectoryEvent& ev);
    void onItemRemoved(const Poco::DirectoryWatcher::DirectoryEvent & ev);
    void onItemModified(const Poco::DirectoryWatcher::DirectoryEvent& ev);
    void onItemMovedFrom(const Poco::DirectoryWatcher::DirectoryEvent & ev);
    void onItemMovedTo(const Poco::DirectoryWatcher::DirectoryEvent & ev);
    void onError(const Poco::Exception &);

private:
    const std::string path;

    /// Note, in order to avoid data race found by fuzzer, put events before dw,
    /// such that when this class destruction, dw will be destructed before events.
    /// The data race is because dw create a separate thread to monitor file events
    /// and put into events, then if we destruct events first, the monitor thread still
    /// running, it may access events during events destruction, leads to data race.
    Events events;

    std::unique_ptr<Poco::DirectoryWatcher> dw;

    Poco::Logger * log;

    std::mutex mutex;

    Error error;
};
