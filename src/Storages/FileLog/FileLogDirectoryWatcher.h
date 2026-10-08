#pragma once

#include <Storages/FileLog/DirectoryWatcherBase.h>

#include <Common/Logger_fwd.h>

#include <memory>
#include <mutex>
#include <unordered_set>
#include <utility>
#include <vector>

namespace DB
{
class Exception;
class StorageFileLog;

class FileLogDirectoryWatcher
{
public:
    struct EventInfo
    {
        DirectoryWatcherBase::DirectoryEventType type;
        std::string callback;
    };

    /// Events are accumulated as a flat chronologically-ordered sequence so that
    /// consumers see the cross-name order matching the kernel-emitted order
    /// (within a single batch). The previous per-name aggregation was an
    /// `unordered_map`, which dropped global ordering and made rename pairs
    /// (`DW_ITEM_MOVED_FROM` on one name + `DW_ITEM_MOVED_TO` on another)
    /// race against unrelated events on a third name.
    using Events = std::vector<std::pair<std::string, EventInfo>>;

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

    /// Dedup state for repeated `DW_ITEM_MODIFIED` events within a single
    /// batch. The original aggregation stored this per-name in `FileEvents`;
    /// with a flat event list we keep it as a side set, cleared together with
    /// `events` on `getEventsAndReset`.
    std::unordered_set<std::string> modified_seen;

    LoggerPtr log;

    std::mutex mutex;

    Error error;

    std::unique_ptr<DirectoryWatcherBase> dw;
};
}
