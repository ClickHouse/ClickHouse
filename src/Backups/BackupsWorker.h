#pragma once

#include <Backups/BackupStatus.h>
#include <Common/ThreadPool.h>
#include <Core/UUID.h>
#include <unordered_map>


namespace DB
{

class BackupsWorker
{
public:
    static BackupsWorker & instance();

    UInt64 add(const String & backup_name, BackupStatus status, const String & error = {});
    void update(UInt64 task_id, BackupStatus status, const String & error = {});

    struct Entry
    {
        String backup_name;
        UInt64 task_id;
        BackupStatus status;
        String error;
        time_t timestamp;
    };

    Entry getEntry(UInt64 task_id) const;
    std::vector<Entry> getEntries() const;

    /// Schedules a new task and perfoms it in the background thread.
    void run(std::function<void()> && task);

    /// Waits until all tasks have been completed.
    void shutdown();

private:
    BackupsWorker();

    mutable std::mutex mutex;
    std::vector<Entry> entries;
    std::unordered_map<String, UInt64> entries_by_name;
    std::unordered_map<UInt64, size_t> entries_by_task_id;
    UInt64 current_task_id = 0;
    ThreadPool thread_pool;
};

}
