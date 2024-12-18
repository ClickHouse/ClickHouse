#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Common/PipeFDs.h>
#include <Interpreters/Context_fwd.h>

#include <atomic>
#include <memory>
#include <string>

namespace DB
{
class FileLogDirectoryWatcher;

class DirectoryWatcherBase : WithContext
{
    /// Most of code in this class is copy from the Poco project:
    /// https://github.com/ClickHouse-Extras/poco/blob/clickhouse/Foundation/src/DirectoryWatcher.cpp
    /// This class is used to get notifications about changes
    /// to the filesystem, more precisely, to a specific
    /// directory. Changes to a directory are reported via
    /// events.
    ///
    /// A thread will be created that watches the specified
    /// directory for changes. Events are reported in the context
    /// of this thread.
    ///
    /// Note that changes to files in subdirectories of the watched
    /// directory are not reported. Separate DirectoryWatcher objects
    /// must be created for these directories if they should be watched.
public:
    enum DirectoryEventType
    {
        DW_ITEM_ADDED = 1,
        /// A new item has been created and added to the directory.

        DW_ITEM_REMOVED = 2,
        /// An item has been removed from the directory.

        DW_ITEM_MODIFIED = 4,
        /// An item has been modified.

        DW_ITEM_MOVED_FROM = 8,
        /// An item has been renamed or moved. This event delivers the old name.

        DW_ITEM_MOVED_TO = 16
        /// An item has been renamed or moved. This event delivers the new name.
    };

    enum DirectoryEventMask
    {
        /// Enables all event types.
        DW_FILTER_ENABLE_ALL = 31,

        /// Disables all event types.
        DW_FILTER_DISABLE_ALL = 0
    };

    struct DirectoryEvent
    {
        DirectoryEvent(const std::string & f, DirectoryEventType ev) : path(f), event(ev) { }

        /// The directory or file that has been changed.
        const std::string path;
        /// The kind of event.
        DirectoryEventType event;
    };


    DirectoryWatcherBase() = delete;
    DirectoryWatcherBase(const DirectoryWatcherBase &) = delete;
    DirectoryWatcherBase & operator=(const DirectoryWatcherBase &) = delete;

    /// Creates a DirectoryWatcher for the directory given in path.
    /// To enable only specific events, an eventMask can be specified by
    /// OR-ing the desired event IDs (e.g., DW_ITEM_ADDED | DW_ITEM_MODIFIED).
    explicit DirectoryWatcherBase(
        FileLogDirectoryWatcher & owner_, const std::string & path_, ContextPtr context_, int event_mask_ = DW_FILTER_ENABLE_ALL);

    ~DirectoryWatcherBase();

    /// Returns the value of the eventMask passed to the constructor.
    int eventMask() const { return event_mask; }

    /// Returns the directory being watched.
    const std::string & directory() const;

    void watchFunc();

private:
    FileLogDirectoryWatcher & owner;

    using TaskThread = BackgroundSchedulePool::TaskHolder;
    TaskThread watch_task;

    std::atomic<bool> stopped{false};


    const std::string path;
    int event_mask;
    uint64_t milliseconds_to_wait;

    int inotify_fd;
    PipeFDs event_pipe;

    void start();
    void stop();
};

}
