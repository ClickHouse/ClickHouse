#pragma once

#include <Poco/DirectoryWatcher.h>
#include <Poco/Foundation.h>
#include <Poco/Path.h>

#include <deque>
#include <mutex>

class FileLogDirectoryWatcher
{
public:
    struct DirEvent
	{
		Poco::DirectoryWatcher::DirectoryEventType type;
		std::string callback;
		std::string path;
	};

    using Events = std::deque<DirEvent>;

    explicit FileLogDirectoryWatcher(const std::string & path_);
    ~FileLogDirectoryWatcher() = default;

    Events getEvents();

    bool getError() const;

    const std::string & getPath() const;

protected:
    void onItemAdded(const Poco::DirectoryWatcher::DirectoryEvent& ev);
	void onItemRemoved(const Poco::DirectoryWatcher::DirectoryEvent& ev);
	void onItemModified(const Poco::DirectoryWatcher::DirectoryEvent& ev);
	void onItemMovedFrom(const Poco::DirectoryWatcher::DirectoryEvent& ev);
	void onItemMovedTo(const Poco::DirectoryWatcher::DirectoryEvent& ev);
    void onError(const Poco::Exception &);

private:
    const std::string path;
    std::shared_ptr<Poco::DirectoryWatcher> dw;

    std::mutex mutex;

    Events events;

    bool error = false;
};
