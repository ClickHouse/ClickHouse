#pragma once

#include <Poco/DirectoryWatcher.h>
#include <Poco/Foundation.h>
#include <Poco/Path.h>

#include <common/logger_useful.h>

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
    std::shared_ptr<Poco::DirectoryWatcher> dw;

    Poco::Logger * log;

    std::mutex mutex;

    Events events;

    Error error;
};
